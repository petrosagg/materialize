// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::pin::pin;
use std::rc::Rc;
use std::str::FromStr;

use anyhow::anyhow;
use bytes::Bytes;
use differential_dataflow::{AsCollection, Collection};
use futures::{FutureExt, StreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, GnoInterval, Sid};
use mysql_common::binlog::value::BinlogValue;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp;
use tracing::trace;
use uuid::Uuid;

use mz_expr::MirScalarExpr;
use mz_mysql_util::GtidSet;
use mz_mysql_util::{
    ensure_full_row_binlog_format, ensure_gtid_consistency, ensure_replication_commit_order,
    MySqlTableDesc,
};
use mz_ore::cast::CastFrom;
use mz_ore::result::ResultExt;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};

use crate::source::types::SourceReaderError;
use crate::source::RawSourceCreationConfig;

use super::{DefiniteError, RewindRequest, TransactionId, TransientError};

// Used as a partition id to determine if the worker is responsible for reading from the
// MySQL replication stream
static REPL_READER: &str = "reader";

/// Renders the replication dataflow. See the module documentation for more information.
pub(crate) fn render<G: Scope<Timestamp = TransactionId>>(
    scope: G,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    subsource_resume_uppers: BTreeMap<GlobalId, Antichain<TransactionId>>,
    table_info: BTreeMap<String, (usize, MySqlTableDesc, Vec<MirScalarExpr>)>,
    rewind_stream: &Stream<G, RewindRequest>,
    _committed_uppers: impl futures::Stream<Item = Antichain<TransactionId>> + 'static,
) -> (
    Collection<G, (usize, Result<Row, SourceReaderError>), Diff>,
    Stream<G, Infallible>,
    Stream<G, Rc<TransientError>>,
    PressOnDropButton,
) {
    let op_name = format!("ReplicationReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(op_name, scope);

    let repl_reader_id = u64::cast_from(config.responsible_worker(REPL_READER));
    // TODO(roshan): Why do we need to use an Exchange pact in this case?
    let mut rewind_input = builder.new_input(rewind_stream, Exchange::new(move |_| repl_reader_id));
    let (mut data_output, data_stream) = builder.new_output();
    let (_upper_output, upper_stream) = builder.new_output();

    // TODO: Add metrics

    let (button, errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let (id, worker_id) = (config.id, config.worker_id);
            let [data_cap_set, upper_cap_set]: &mut [_; 2] = caps.try_into().unwrap();

            // Only run the replication reader on the worker responsible for it.
            if !config.responsible_for(REPL_READER) {
                return Ok(());
            }

            let connection_config = connection
                .connection
                .config(&*config.config.connection_context.secrets_reader, &config.config)
                .await?;

            let mut conn = connection_config.connect(
                &format!("timely-{worker_id} MySQL replication reader"),
                &config.config.connection_context.ssh_tunnel_manager,
            ).await?;

            // Get the set of GTIDs currently available in the binlogs (GTIDs that we can safely start replication from)
            let binlog_gtid_set: String = conn.query_first("SELECT GTID_SUBTRACT(@@global.gtid_executed, @@global.gtid_purged)").await?.unwrap();
            // NOTE: The assumption here for the MVP is that there is only one source-id in the GTID set, so we can just 
            // take the transaction_id from the first one. Fix this once we move to a partitioned timestamp.
            let binlog_start_gtid = GtidSet::from_str(&binlog_gtid_set)?.gtids.first().expect("At least one gtid").to_owned();
            let binlog_start_transaction_id = TransactionId::new(binlog_start_gtid.earliest_transaction_id());

            let resume_upper = Antichain::from_iter(
                subsource_resume_uppers
                    .values()
                    .flat_map(|f| f.elements())
                    // Advance any upper as far as the start of the binlog.
                    .map(|t| std::cmp::max(*t, binlog_start_transaction_id))
            );

            let Some(resume_transaction_id) = resume_upper.into_option() else {
                return Ok(());
            };
            data_cap_set.downgrade([&resume_transaction_id]);
            upper_cap_set.downgrade([&resume_transaction_id]);
            trace!(%id, "timely-{worker_id} replication reader started transaction_id={}", resume_transaction_id);

            let mut rewinds = BTreeMap::new();
            while let Some(event) = rewind_input.next_mut().await {
                if let AsyncEvent::Data(cap, data) = event {
                    let cap = cap.retain_for_output(0);
                    for req in data.drain(..) {
                        let snapshot_transaction_id = TransactionId::new(req.snapshot_gtid_set.gtids[0].latest_transaction_id());
                        // TODO: Should this be an assert? Or should we raise a DefiniteError?
                        assert!(
                            resume_transaction_id <= snapshot_transaction_id + TransactionId::new(1),
                            "binlogs compacted past snapshot point. snapshot consistent point={} resume_lsn={resume_transaction_id}", snapshot_transaction_id + TransactionId::new(1)
                        );
                        rewinds.insert(req.table_qualified_name.clone(), (cap.clone(), req));
                    }
                }
            }
            trace!(%id, "timely-{worker_id} pending rewinds {rewinds:?}");

            let stream_result = raw_stream(
                &config,
                conn,
                binlog_start_gtid.uuid,
                *data_cap_set[0].time(),
            )
            .await?;

            let binlog_stream = match stream_result {
                Ok(stream) => stream,
                Err(err) => {
                    // If the replication stream cannot be obtained in a definite way there is
                    // nothing else to do. These errors are not retractable.
                    for (output_index, _, _) in table_info.values() {
                        // We pick `u64::MAX` as the TransactionId which will (in practice) never conflict
                        // any previously revealed portions of the TVC.
                        let update = ((*output_index, Err(err.clone())), TransactionId::new(u64::MAX), 1);
                        data_output.give(&data_cap_set[0], update).await;
                    }
                    return Ok(());
                }
            };
            let mut stream = pin!(binlog_stream.peekable());

            let mut container = Vec::new();
            let max_capacity = timely::container::buffer::default_capacity::<((u32, Result<Vec<Option<Bytes>>, DefiniteError>), TransactionId, Diff)>();

            // Binlog Table Id -> Table Qualified Name (its key in the `table_info` map)
            let mut table_id_map = BTreeMap::<u64, String>::new();
            let mut skipped_table_ids = BTreeSet::<u64>::new();

            let mut final_row = Row::default();
            let mut temp_storage = Vec::new();

            let mut new_upper = *data_cap_set[0].time();

            trace!(%id, "timely-{worker_id} starting replication at {new_upper:?}");
            while let Some(event) = stream.as_mut().next().await {
                use mysql_async::binlog::events::*;
                let event = event?;

                match event.read_data()? {
                    // We receive a GtidEvent that tells us the transaction-id of proceeding
                    // RowsEvents (and other events)
                    Some(EventData::GtidEvent(event)) => {
                        // TODO: Use this when we use a partitioned timestamp
                        let _source_id = Uuid::from_bytes(event.sid());
                        let received_upper = TransactionId::new(event.gno());
                        // TODO: Should we assert this?
                        assert!(new_upper <= received_upper, "GTID went backwards");
                        new_upper = received_upper;
                    }
                    // A row event is a write/update/delete event
                    Some(EventData::RowsEvent(data)) => {
                        // Find the relevant table
                        let binlog_table_id = data.table_id();
                        let table_map_event = stream.get_ref().get_tme(binlog_table_id).ok_or_else(|| {
                            TransientError::Generic(anyhow::anyhow!("Table map event not found"))
                        })?;
                        let table_qualified_name = match (
                            table_id_map.get(&binlog_table_id),
                            skipped_table_ids.get(&binlog_table_id),
                        ) {
                            (Some(table_qualified_name), None) => table_qualified_name,
                            (None, Some(_)) => {
                                // We've seen this table ID before and it was skipped
                                continue;
                            }
                            (None, None) => {
                                let table_qualified_name = format!("{}.{}", table_map_event.database_name(), table_map_event.table_name());
                                if table_info.contains_key(&table_qualified_name) {
                                    table_id_map.insert(binlog_table_id, table_qualified_name);
                                    &table_id_map[&binlog_table_id]
                                } else {
                                    skipped_table_ids.insert(binlog_table_id);
                                    // We don't know about this table, so skip this event
                                    continue;
                                }
                            }
                            _ => unreachable!(),
                        };

                        let (output_index, table_desc, _casts) = &table_info[table_qualified_name];

                        // Iterate over the rows in this RowsEvent. Each row is a pair of 'before_row', 'after_row',
                        // to accomodate for updates and deletes (which include a before_row),
                        // and updates and inserts (which inclued an after row).
                        let mut rows_iter = data.rows(table_map_event);
                        while let Some(Ok((before_row, after_row))) = rows_iter.next() {
                            for (row, diff) in [(before_row, -1), (after_row, 1)].iter_mut() {
                                // TODO: Map columns from table schema to indexes in each row
                                if let Some(row) = row {
                                    let mut packer = final_row.packer();
                                    // TODO: Verify that the ordinal_position (col_index) can be safely used as the column
                                    // BinlogRow index
                                    for (col_index, _col_desc) in table_desc.columns.iter().enumerate() {
                                        // TODO: Properly decode MySql column into MZ datum
                                        // TODO: Implement cast expressions
                                        let s: String = match row.take(col_index).unwrap() {
                                            BinlogValue::Value(value) => match value {
                                                mysql_async::Value::NULL => "NULL".to_string(),
                                                mysql_async::Value::Bytes(bytes) => String::from_utf8_lossy(bytes.as_ref()).to_string(),
                                                mysql_async::Value::Int(int) => int.to_string(),
                                                mysql_async::Value::UInt(uint) => uint.to_string(),
                                                mysql_async::Value::Float(float) => float.to_string(),
                                                mysql_async::Value::Double(double) => double.to_string(),
                                                _ => "".to_string(),
                                            },
                                            BinlogValue::Jsonb(_) => todo!(),
                                            BinlogValue::JsonDiff(_) => todo!(),
                                        };
                                        temp_storage.push(s);
                                    }
                                    let joined = temp_storage.join(" ");
                                    let datum = Datum::String(joined.as_str());
                                    packer.push(datum);
                                    temp_storage.clear();

                                    let data = (*output_index, Ok(final_row.clone()));

                                    // Rewind this update if it was already present in the snapshot
                                    if let Some((rewind_cap, req)) = rewinds.get(table_qualified_name) {
                                        if new_upper <= TransactionId::new(req.snapshot_gtid_set.gtids[0].latest_transaction_id()) {
                                            data_output.give(rewind_cap, (data.clone(), TransactionId::minimum(), -*diff)).await;
                                        }
                                    }
                                    container.push((data, new_upper, *diff));
                                }
                            }
                        }

                        // flush any pending records and advance our frontier
                        if container.len() > max_capacity {
                            data_output.give_container(&data_cap_set[0], &mut container).await;
                            upper_cap_set.downgrade([&new_upper]);
                            data_cap_set.downgrade([&new_upper]);
                        }
                    }
                    _ => {
                        // TODO: Handle other event types
                        // We definitely need to handle 'query' events and parse them for DDL changes
                        // that might affect the schema of the tables we care about.
                    }
                }

                let will_yield = stream.as_mut().peek().now_or_never().is_none();
                if will_yield {
                    data_output.give_container(&data_cap_set[0], &mut container).await;
                    // TODO: Confirm that it is acceptable to advance the frontier at this point
                    // We are assuming that since the stream is yielding, there are no more updates for the last presented
                    // GTID. This is not necessarily true, but if we don't do this then we will prevent the last written
                    // rows from being available downstream.
                    new_upper = new_upper + TransactionId::new(1);
                    upper_cap_set.downgrade([&new_upper]);
                    data_cap_set.downgrade([&new_upper]);
                    rewinds.retain(|_, (_, req)| data_cap_set[0].time() <= &TransactionId::new(req.snapshot_gtid_set.gtids[0].latest_transaction_id()));
                }
            }
            // We never expect the replication stream to gracefully end
            Err(TransientError::ReplicationEOF)
        })
    });

    // TODO: Split row decoding into a separate operator that can be distributed across all workers
    let replication_updates = data_stream
        .as_collection()
        .map(|(output_index, row)| (output_index, row.err_into()));

    (
        replication_updates,
        upper_stream,
        errors,
        button.press_on_drop(),
    )
}

/// Produces the logical replication stream while taking care of regularly sending standby
/// keepalive messages with the provided `uppers` stream.
///
/// The returned stream will contain all transactions that whose commit LSN is beyond `resume_lsn`.
async fn raw_stream<'a>(
    config: &'a RawSourceCreationConfig,
    mut conn: Conn,
    // TODO: this is only needed while we use a single source-id for the timestamp; remove once we move to a partitioned timestamp
    gtid_source_id: Uuid,
    resume_transaction_id: TransactionId,
) -> Result<Result<BinlogStream, DefiniteError>, TransientError> {
    // Verify the MySQL system settings are correct for consistent row-based replication using GTIDs
    // TODO: Should these return DefiniteError instead of TransientError?
    ensure_gtid_consistency(&mut conn).await?;
    ensure_full_row_binlog_format(&mut conn).await?;
    ensure_replication_commit_order(&mut conn).await?;

    // To start the stream we need to provide a GTID set of the transactions that we've 'seen'
    // that the server will use to determine the starting point of the replication stream.
    // The resume_transaction_id points to the transaction_id we want to resume 'at', so we need
    // to end the interval at the transaction_id before that.
    let seen_gtids = match resume_transaction_id.into() {
        0 | 1 => {
            // If we're starting from the beginning of the binlog or the first transaction id
            // we haven't seen 'anything'
            Vec::new()
        }
        ref a => {
            // NOTE: Since we enforce replica_preserve_commit_order=ON we can start the interval at 1
            // since we know that all transactions with a lower transaction id were monotonic
            vec![Sid::new(*gtid_source_id.as_bytes()).with_interval(GnoInterval::new(1, a - 1))]
        }
    };

    let repl_stream = match conn
        .get_binlog_stream(
            // TODO: The server-id should probably be something more static than the worker-id?
            BinlogStreamRequest::new(
                config
                    .worker_id
                    .try_into()
                    .map_err(|_| TransientError::Generic(anyhow!("invalid worker-id")))?,
            )
            .with_gtid()
            .with_gtid_set(seen_gtids),
        )
        .await
    {
        Ok(stream) => stream,
        Err(mysql_async::Error::Server(ref server_err)) if server_err.code == 1236 => {
            // The GTID set we requested is no longer available
            return Ok(Err(DefiniteError::BinlogNotAvailable));
        }
        // TODO: handle other error types. Some may require a re-snapshot and some may be transient
        Err(err) => return Err(err.into()),
    };

    Ok(Ok(repl_stream))
}
