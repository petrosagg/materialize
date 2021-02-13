use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{pin_mut, Stream, StreamExt};
use itertools::Itertools;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_protocol::replication::pgoutput::{PgOutput, LogicalReplicationMessage, Tuple, TupleData};
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_postgres::replication_client::ReplicationClient;
use tokio_postgres::replication_client::SnapshotMode;
use tokio_postgres::types::{PgLsn, Type as PgType};
use tokio_postgres::{connect_replication, NoTls, ReplicationMode};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use crate::source::{SimpleSource, StreamItem};
use dataflow_types::PostgresSourceConnector;
use expr::parse_datum;
use futures::stream::BoxStream;
use genawaiter::sync::{Co, Gen};
use lazy_static::lazy_static;
use repr::{Datum, Row, RowPacker, ScalarType};
use timely::dataflow::operators::Event;
use uuid::Uuid;

lazy_static! {
    /// Postgres epoch starts at 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

/// foobar
pub struct PostgresSimpleSource {
    connector: PostgresSourceConnector,
    packer: RowPacker,
    plugin: PgOutput,
}

impl Clone for PostgresSimpleSource {
    fn clone(&self) -> Self {
        Self {
            connector: self.connector.clone(),
            packer: RowPacker::new(),
            plugin: self.plugin.clone(),
        }
    }
}

struct PgColumn {
    scalar_type: PgType,
    nullable: bool,
}

impl PostgresSimpleSource {
    /// Constructs a new instance
    pub fn new(connector: PostgresSourceConnector) -> Self {
        let publication = connector.publication.clone();
        Self {
            connector,
            packer: RowPacker::new(),
            plugin: PgOutput::new(vec![publication]),
        }
    }

    async fn table_info(&self) -> (u32, Vec<PgColumn>) {
        let conninfo = &self.connector.conn;
        let table = &self.connector.table;
        let namespace = "public";

        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls).await.unwrap();

        // TODO communicate errors back into the stream
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let rel_id: u32 = client
            .query(
                "SELECT c.oid
                    FROM pg_catalog.pg_class c
                    INNER JOIN pg_catalog.pg_namespace n
                          ON (c.relnamespace = n.oid)
                    WHERE n.nspname = $1
                      AND c.relname = $2;",
                &[&namespace, &table],
            )
            .await
            .unwrap()
            .get(0)
            .unwrap()
            .get(0);

        // Get the column type info
        let col_types = client
            .query(
                "SELECT a.atttypid, a.attnotnull
                    FROM pg_catalog.pg_attribute a
                    WHERE a.attnum > 0::pg_catalog.int2
                      AND NOT a.attisdropped
                      AND a.attrelid = $1
                    ORDER BY a.attnum",
                &[&rel_id],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| PgColumn {
                scalar_type: PgType::from_oid(row.get(0)).unwrap(),
                nullable: !row.get::<_, bool>(1),
            })
            .collect_vec();

        (rel_id, col_types)
    }

    async fn initial_snapshot_stream<'a>(
        &self,
        client: &mut ReplicationClient,
        col_types: &'a [PgColumn],
    ) -> (String, PgLsn, impl Stream<Item = StreamItem> + 'a) {
        let table = &self.connector.table;
        let namespace = "public";

        // TODO: find better naming scheme. Postgres doesn't like dashes
        let slot_name = Uuid::new_v4().to_string().replace('-', "");

        // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        //
        // TODO: The slot should be permanent and destroyed on Drop. This way we can support
        // reconnects
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await
            .unwrap();
        let slot = client
            .create_logical_replication_slot(
                &slot_name,
                true,
                &self.plugin,
                Some(SnapshotMode::UseSnapshot),
            )
            .await
            .unwrap();

        let cur_lsn = slot.consistent_point();

        // Initial data copy
        let copy_query = format!(
            r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
            namespace, table
        );

        let stream = client.copy_out(&copy_query).await.unwrap();

        let scalar_types = col_types
            .iter()
            .map(|c| c.scalar_type.clone())
            .collect_vec();
        let rows = BinaryCopyOutStream::new(stream, &scalar_types);

        let mut packer = RowPacker::new();

        let snapshot = rows.map(Result::unwrap).map(move |row| {
            for (i, ty) in col_types.iter().enumerate() {
                let datum: Datum = match ty.scalar_type {
                    PgType::BOOL => row.get::<Option<bool>>(i).into(),
                    PgType::INT4 => row.get::<Option<i32>>(i).into(),
                    PgType::INT8 => row.get::<Option<i64>>(i).into(),
                    PgType::FLOAT4 => row.get::<Option<f32>>(i).into(),
                    PgType::FLOAT8 => row.get::<Option<f64>>(i).into(),
                    PgType::DATE => row.get::<Option<NaiveDate>>(i).into(),
                    PgType::TIME => row.get::<Option<NaiveTime>>(i).into(),
                    PgType::TIMESTAMP => row.get::<Option<NaiveDateTime>>(i).into(),
                    PgType::TEXT => row.get::<Option<&str>>(i).into(),
                    PgType::UUID => row.get::<Option<Uuid>>(i).into(),
                    _ => todo!(),
                };

                if !ty.nullable && datum.is_null() {
                    panic!("null value in non nullable column");
                }
                packer.push(datum);
            }

            let row = packer.finish_and_reuse();

            Event::Message(cur_lsn.into(), Ok((row, 1)))
        });

        client.simple_query("COMMIT;").await.unwrap();

        (slot_name, cur_lsn, snapshot)
    }

    fn row_from_tuple(&mut self, tuple: &Tuple, col_types: &[PgColumn]) -> Row {
        self.packer.clear();

        for (val, ty) in tuple.tuple_data().iter().zip(col_types.iter()) {
            let datum = match val {
                TupleData::Null => Datum::Null,
                TupleData::Toast => Datum::Null, // FIXME
                TupleData::Text(b) => {
                    let s = std::str::from_utf8(&b).unwrap();
                    match ty.scalar_type {
                        PgType::BOOL => parse_datum(s, ScalarType::Bool).unwrap(),
                        PgType::INT4 => parse_datum(s, ScalarType::Int32).unwrap(),
                        PgType::INT8 => parse_datum(s, ScalarType::Int64).unwrap(),
                        PgType::FLOAT4 => parse_datum(s, ScalarType::Float32).unwrap(),
                        PgType::FLOAT8 => parse_datum(s, ScalarType::Float64).unwrap(),
                        PgType::DATE => parse_datum(s, ScalarType::Date).unwrap(),
                        PgType::TIME => parse_datum(s, ScalarType::Time).unwrap(),
                        PgType::TIMESTAMP => parse_datum(s, ScalarType::Timestamp).unwrap(),
                        PgType::TEXT => parse_datum(s, ScalarType::String).unwrap(),
                        PgType::UUID => parse_datum(s, ScalarType::Uuid).unwrap(),
                        _ => panic!(),
                    }
                }
            };
            self.packer.push(datum);
        }

        self.packer.finish_and_reuse()
    }

    async fn into_generator(mut self, co: Co<StreamItem>) {
        let conninfo = &self.connector.conn;
        let publication = &self.connector.publication;

        let (rel_id, col_types) = self.table_info().await;

        let (mut client, conn) = connect_replication(&conninfo, NoTls, ReplicationMode::Logical)
            .await
            .unwrap();

        // TODO communicate errors back into the stream
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("connection error: {}", e);
            }
        });

        let (slot_name, mut cur_lsn, snapshot) =
            self.initial_snapshot_stream(&mut client, &col_types).await;

        pin_mut!(snapshot);

        while let Some(event) = snapshot.next().await {
            co.yield_(event).await;
        }

        // We now switch to consuming the stream
        let mut stream = client
            .start_logical_replication(&slot_name, cur_lsn, &self.plugin)
            .await
            .unwrap();

        cur_lsn = (u64::from(cur_lsn) + 1u64).into();
        co.yield_(Event::Progress(Some(cur_lsn.into()))).await;

        let mut tx_rows = vec![];
        while let Some(replication_message) = stream.next().await {
            match replication_message.unwrap() {
                ReplicationMessage::XLogData(xlog_data) => {
                    use LogicalReplicationMessage::*;

                    match xlog_data.data() {
                        Insert(insert) if insert.rel_id() == rel_id => {
                            let row = self.row_from_tuple(insert.tuple(), &col_types);
                            tx_rows.push((row, 1));
                        }
                        Update(update) if update.rel_id() == rel_id => {
                            let row = self.row_from_tuple(update.old_tuple().unwrap(), &col_types);
                            tx_rows.push((row, -1));

                            let row = self.row_from_tuple(update.new_tuple(), &col_types);
                            tx_rows.push((row, 1));
                        }
                        Delete(delete) if delete.rel_id() == rel_id => {
                            let row = self.row_from_tuple(delete.old_tuple().unwrap(), &col_types);
                            tx_rows.push((row, -1));
                        }
                        Commit(commit) => {
                            cur_lsn = (commit.commit_lsn() as u64).into();

                            for (row, diff) in tx_rows.drain(..) {
                                let event = Event::Message(cur_lsn.into(), Ok((row, diff)));
                                co.yield_(event).await;
                            }
                            cur_lsn = (u64::from(cur_lsn) + 1u64).into();
                            co.yield_(Event::Progress(Some(cur_lsn.into()))).await;
                        }
                        _ => (),
                    }
                }
                ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                    if keepalive.reply() == 1 {
                        let ts = PG_EPOCH
                            .elapsed()
                            .expect("system clock set earlier than year 2000!")
                            .as_micros() as i64;
                        stream
                            .as_mut()
                            .standby_status_update(cur_lsn, cur_lsn, cur_lsn, ts, 0)
                            .await
                            .unwrap();
                    }
                }
                _ => panic!("Unexpected replication message"),
            }
        }
    }
}

impl SimpleSource for PostgresSimpleSource {
    fn build_stream<'a>(&mut self) -> BoxStream<'a, StreamItem> {
        Box::pin(Gen::new(|co| self.clone().into_generator(co)))
    }
}
