use repr::{RelationDesc, RelationType, ScalarType};
use tokio_postgres::types::Type;
use tokio_postgres::{connect_replication, NoTls, ReplicationMode, SimpleQueryMessage};

pub async fn get_remote_postgres_schema(
    conninfo: &str,
    _publication: &str,
    table: &str,
) -> Result<RelationDesc, anyhow::Error> {
    println!("getting schema for {} with conninfo {}", table, conninfo);
    let (rclient, rconnection) =
        connect_replication(conninfo, NoTls, ReplicationMode::Logical).await?;

    // spawn connection to run on its own
    tokio::spawn(async move {
        if let Err(e) = rconnection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let namespace = "public";

    let rel_id_query = format!(
        "SELECT c.oid
         FROM pg_catalog.pg_class c
         INNER JOIN pg_catalog.pg_namespace n
               ON (c.relnamespace = n.oid)
         WHERE n.nspname = '{}'
           AND c.relname = '{}';",
        namespace, table
    );
    let rel_id = rclient
        .simple_query(&rel_id_query)
        .await?
        .into_iter()
        .filter_map(|msg| {
            if let SimpleQueryMessage::Row(row) = msg {
                Some(row.get(0).unwrap().parse::<u32>().unwrap())
            } else {
                None
            }
        })
        .next()
        .unwrap();

    // Get the column type info
    let col_info_query = format!(
        "SELECT a.attname,
            a.atttypid,
            a.atttypmod,
            a.attnotnull,
            a.attnum = ANY(i.indkey)
         FROM pg_catalog.pg_attribute a
         LEFT JOIN pg_catalog.pg_index i
              ON (i.indexrelid = pg_get_replica_identity_index({}))
         WHERE a.attnum > 0::pg_catalog.int2
           AND NOT a.attisdropped
           AND a.attrelid = {}
         ORDER BY a.attnum",
        rel_id, rel_id
    );

    let col_info = rclient
        .simple_query(&col_info_query)
        .await?
        .into_iter()
        .filter_map(|msg| {
            if let SimpleQueryMessage::Row(row) = msg {
                let name = row.get(0).unwrap().to_owned();
                let ty = Type::from_oid(row.get(1).unwrap().parse().unwrap()).unwrap();
                let nullable = row.get(3).unwrap() == "f";

                let scalar_type = match ty {
                    Type::BOOL => ScalarType::Bool,
                    Type::INT4 => ScalarType::Int32,
                    Type::INT8 => ScalarType::Int64,
                    Type::FLOAT4 => ScalarType::Float32,
                    Type::FLOAT8 => ScalarType::Float64,
                    Type::DATE => ScalarType::Date,
                    Type::TIME => ScalarType::Time,
                    Type::TIMESTAMP => ScalarType::Timestamp,
                    Type::TIMESTAMPTZ => ScalarType::TimestampTz,
                    Type::TEXT => ScalarType::String,
                    Type::UUID => ScalarType::Uuid,
                    Type::JSONB => ScalarType::Jsonb,
                    _ => panic!("unsuported column type"),
                };
                let col_ty = scalar_type.nullable(nullable);
                Some((Some(name), col_ty))
            } else {
                None
            }
        });

    let mut col_types = vec![];
    let mut col_names = vec![];

    for (name, col_ty) in col_info {
        col_names.push(name);
        col_types.push(col_ty);
    }
    let rel_ty = RelationType::new(col_types);

    Ok(dbg!(RelationDesc::new(rel_ty, col_names)))
}
