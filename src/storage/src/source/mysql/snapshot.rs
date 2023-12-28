// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::str::FromStr;

use differential_dataflow::{AsCollection, Collection};
use futures::TryStreamExt;
use mysql_async::prelude::Queryable;
use mysql_async::{IsolationLevel, Row as MySqlRow, TxOpts};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Broadcast, CapabilitySet, ConnectLoop, Feedback};
use timely::dataflow::{Scope, Stream};
use timely::progress::{Antichain, Timestamp};
use tracing::trace;

use mz_expr::MirScalarExpr;
use mz_mysql_util::{
    ensure_full_row_binlog_format, ensure_gtid_consistency, ensure_replication_commit_order,
    query_sys_var, GtidSet,
};
use mz_mysql_util::{schema_info, MySqlTableDesc};
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};

use crate::source::{RawSourceCreationConfig, SourceReaderError};

use super::{RewindRequest, TransactionId, TransientError};

/// Renders the snapshot dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = TransactionId>>(
    mut scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<TransactionId>>,
    table_info: BTreeMap<String, (usize, MySqlTableDesc, Vec<MirScalarExpr>)>,
) -> (
    Collection<G, (usize, Result<Row, SourceReaderError>), Diff>,
    Stream<G, RewindRequest>,
    Stream<G, Rc<TransientError>>,
    PressOnDropButton,
) {
    let mut builder =
        AsyncOperatorBuilder::new(format!("MySQLSnapshot({})", config.id), scope.clone());

    let (mut raw_handle, raw_data) = builder.new_output();
    let (mut rewinds_handle, rewinds) = builder.new_output();

    // Broadcast a signal from the snapshot leader worker to the other workers when the table
    // lock is in place. Upon receiving the first message the workers should start a transaction
    // that guarantees they all see a consistent view from the same GTID.
    let (mut lock_start_handle, lock_start) = builder.new_output();
    let (ls_feedback_handle, ls_feedback_data) = scope.feedback(Default::default());
    let mut lock_start_input =
        builder.new_input_connection(&ls_feedback_data, Pipeline, vec![Antichain::new(); 3]);
    lock_start.broadcast().connect_loop(ls_feedback_handle);

    // Broadcast a signal from the all workers that they have begun a transaction and that the
    // table lock held by the snapshot leader can be released
    let (mut transaction_start_handle, transaction_start) = builder.new_output();
    let (ts_feedback_handle, ts_feedback_data) = scope.feedback(Default::default());
    let mut transaction_start_input =
        builder.new_input_connection(&ts_feedback_data, Pipeline, vec![Antichain::new(); 4]);
    transaction_start
        .broadcast()
        .connect_loop(ts_feedback_handle);

    let is_snapshot_leader = config.responsible_for("snapshot_leader");

    // A global view of all exports that need to be snapshot by all workers. Note that this affects
    // `reader_snapshot_table_info` but must be kept separate from it because each worker needs to
    // understand if any worker is snapshotting any subsource.
    let export_indexes_to_snapshot: BTreeSet<_> = subsource_resume_uppers
        .into_iter()
        .filter_map(|(id, upper)| {
            // Determined which collections need to be snapshot and which already have been.
            if id != config.id && *upper == [TransactionId::minimum()] {
                // Convert from `GlobalId` to output index.
                Some(config.source_exports[&id].output_index)
            } else {
                None
            }
        })
        .collect();

    let mut all_table_names = vec![];
    // A map containing only the table infos that this worker should snapshot.
    let mut reader_snapshot_table_info = BTreeMap::new();

    for (table, val) in table_info {
        mz_ore::soft_assert_or_log!(
            val.0 != 0,
            "primary collection should not be represented in table info"
        );
        if !export_indexes_to_snapshot.contains(&val.0) {
            continue;
        }
        all_table_names.push(table.clone());
        if config.responsible_for(&table) {
            reader_snapshot_table_info.insert(table, val);
        }
    }

    let (button, errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let id = config.id;
            let worker_id = config.worker_id;

            let [data_cap_set, rewind_cap_set, lock_start_cap_set, transaction_start_cap_set]: &mut [_; 4] =
                caps.try_into().unwrap();
            trace!(
                %id,
                "timely-{worker_id} initializing table reader with {} tables to snapshot",
                reader_snapshot_table_info.len()
            );

            // Nothing needs to be snapshot.
            if all_table_names.is_empty() {
                trace!(%id, "no exports to snapshot");
                return Ok(());
            }

            let connection_config = connection
                .connection
                .config(
                    &*config.config.connection_context.secrets_reader,
                    &config.config,
                )
                .await?;
            let task_name = format!("timely-{worker_id} MySQL snapshotter");

            // The snapshot leader is responsible for ensuring a consistent snapshot of
            // all tables that need to be snapshot. The leader will create a new 'lock connection'
            // that takes a table lock on all tables to be snapshot and then read the active GTID
            // at that time.
            //
            // Once this lock is acquired, the leader is responsible for sending a signal to all
            // workers that they should start their own transactions. The workers will then start a
            // transaction with REPEATABLE READ and 'CONSISTENT SNAPSHOT' semantics.
            // Once each worker has started their transaction, they will send a signal to the
            // leader that the table lock can be released. This will allow further writes to the
            // tables by other clients to occur, while ensuring that the snapshot workers all see
            // a consistent view of the tables starting from the same GTID.
            let mut lock_conn = None;
            if is_snapshot_leader {
                let lock_clauses = all_table_names
                    .iter()
                    .map(|t| format!("{} READ", t.as_str()))
                    .collect::<Vec<String>>()
                    .join(", ");
                let leader_task_name = format!("{} leader", task_name);
                lock_conn = Some(Box::new(connection_config.connect(&leader_task_name, &config.config.connection_context.ssh_tunnel_manager).await?));

                trace!(%id, "timely-{worker_id} acquiring table locks: {lock_clauses}");
                lock_conn.as_mut().expect("lock_conn just created")
                    .query_drop(format!("LOCK TABLES {lock_clauses}"))
                    .await?;

                // Record the GTID set at the start of the snapshot
                let snapshot_gtid_set = query_sys_var(lock_conn.as_mut().expect("lock_conn just created"), "global.gtid_executed").await?;
                let snapshot_gtid_set = GtidSet::from_str(snapshot_gtid_set.as_str())?;
                trace!(%id, "timely-{worker_id} acquired table locks at start gtid set: {snapshot_gtid_set:?}");
                // TODO(roshan): Insert metric for how long it took to acquire the locks

                // Send a signal to all workers that they should start their transactions
                lock_start_handle.give(&lock_start_cap_set[0], snapshot_gtid_set).await;
            }
            *lock_start_cap_set = CapabilitySet::new();

            // This worker has no tables to snapshot.
            if reader_snapshot_table_info.is_empty() {
                // Send a signal to the leader that this worker is okay with releasing the table locks
                transaction_start_handle.give(&transaction_start_cap_set[0], ()).await;
                *transaction_start_cap_set = CapabilitySet::new();
                return Ok(());
            }

            let mut conn = connection_config.connect(&task_name, &config.config.connection_context.ssh_tunnel_manager).await?;

            // Verify the MySQL system settings are correct for consistent row-based replication using GTIDs
            // TODO: Should these return DefiniteError instead of TransientError?
            ensure_gtid_consistency(&mut conn).await?;
            ensure_full_row_binlog_format(&mut conn).await?;
            ensure_replication_commit_order(&mut conn).await?;

            // Wait for the start_gtids from the leader, which indicate that the table locks have
            // been acquired and we should start a transaction.
            let snapshot_gtid_set = loop {
                match lock_start_input.next_mut().await {
                    Some(AsyncEvent::Data(_, data)) => break data.pop().expect("Sent above"),
                    Some(AsyncEvent::Progress(_)) => (),
                    None => panic!("lock_start_input closed unexpectedly"),
                }
            };

            trace!(%id, "timely-{worker_id} starting transaction with consistent snapshot at gtid set: {snapshot_gtid_set:?}");

            // Start a transaction with REPEATABLE READ and 'CONSISTENT SNAPSHOT' semantics
            // so we can read a consistent snapshot of the table at the specific GTID we read.
            let mut tx_opts = TxOpts::default();
            tx_opts
                .with_isolation_level(IsolationLevel::RepeatableRead)
                .with_consistent_snapshot(true)
                .with_readonly(true);
            conn.start_transaction(tx_opts).await?;
            conn.query_drop("set @@session.time_zone = '+00:00'")
                .await?;

            // Read the schemas of the tables we are snapshotting
            // TODO(roshan): Only request the schemas of the tables we are snapshotting
            let _ = schema_info(&mut conn, vec!["dummyschema"]).await?;
            // TODO: verify the schema matches the expected schema

            // Send a signal to the leader that we have started our transaction
            transaction_start_handle.give(&transaction_start_cap_set[0], ()).await;
            *transaction_start_cap_set = CapabilitySet::new();

            trace!(%id, "timely-{worker_id} started transaction");

            if is_snapshot_leader {
                // Wait for all workers to start their transactions so we can release the
                // table locks
                let mut received = 0;
                while received < config.worker_count {
                    match transaction_start_input.next().await {
                        Some(AsyncEvent::Data(_, _)) => received += 1,
                        Some(AsyncEvent::Progress(_)) => (),
                        None => panic!("transaction_start_input closed unexpectedly"),
                    }
                }

                trace!(%id, "timely-{worker_id} releasing table locks");
                if let Some(mut lock_conn) = lock_conn.take() {
                    lock_conn.query_drop("UNLOCK TABLES").await?;
                    lock_conn.disconnect().await?;
                } else {
                    panic!("lock_conn should have been created for the snapshot leader");
                }
                drop(lock_conn);
            }

            // We have established a snapshot GTID set so we can broadcast the rewind requests
            for table_qualified_name in reader_snapshot_table_info.keys() {
                trace!(%id, "timely-{worker_id} producing rewind request for {table_qualified_name}");
                let req = RewindRequest { table_qualified_name: table_qualified_name.clone(), snapshot_gtid_set: snapshot_gtid_set.clone() };
                rewinds_handle.give(&rewind_cap_set[0], req).await;
            }
            *rewind_cap_set = CapabilitySet::new();

            // Read the snapshot data from the tables
            let mut final_row = Row::default();
            let mut temp_storage = Vec::new();
            for (table_qualified_name, (output_index, table_desc, _casts)) in reader_snapshot_table_info {
                let query = format!("SELECT * FROM {}", table_qualified_name);
                trace!(%id, "timely-{worker_id} reading snapshot from table: {table_qualified_name}\n{table_desc:?}");
                let mut results = conn.query_stream(query).await?;
                while let Some(row) = results.try_next().await? {
                    let mut row: MySqlRow = row;
                    let mut packer = final_row.packer();

                    for col in table_desc.columns.iter() {
                        // TODO: Properly decode MySql column into MZ datum
                        // TODO: Implement cast expressions
                        let s: String = row.take::<String, _>(col.name.as_str()).unwrap();
                        temp_storage.push(s);
                    }
                    let joined = temp_storage.join(" ");
                    let datum = Datum::String(joined.as_str());
                    packer.push(datum);
                    // TODO(roshan): Should these updates be published at TransactionId::minimum()?
                    raw_handle.give(&data_cap_set[0], ((output_index, Ok(final_row.clone())), TransactionId::minimum(), 1)).await;
                    temp_storage.clear();
                }
            }

            Ok(())
        })
    });

    // TODO: Split row decoding into a separate operator that can be distributed across all workers
    let snapshot_updates = raw_data.as_collection();

    (snapshot_updates, rewinds, errors, button.press_on_drop())
}
