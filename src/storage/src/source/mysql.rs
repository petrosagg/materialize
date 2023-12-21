// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`MySqlSourceConnection`].

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io;

use differential_dataflow::Collection;
use mz_mysql_util::MySqlError;
use mz_mysql_util::{MySqlColumnDesc, MySqlDataType, MySqlTableDesc};
use mz_ore::error::ErrorExt;
use mz_repr::{Diff, Row};
use mz_storage_types::sources::MySqlSourceConnection;
use mz_storage_types::sources::SourceTimestamp;
use mz_timely_util::builder_async::PressOnDropButton;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::SourceRender;
use crate::source::{RawSourceCreationConfig, SourceMessage, SourceReaderError};

mod snapshot;
mod timestamp;

use timestamp::TransactionId;

impl SourceRender for MySqlSourceConnection {
    type Key = ();
    type Value = Row;
    // TODO: Eventually replace with a Partitioned<Uuid, TransactionId> timestamp
    type Time = TransactionId;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Postgres;

    /// Render the ingestion dataflow. This function only connects things together and contains no
    /// actual processing logic.
    fn render<G: Scope<Timestamp = TransactionId>>(
        self,
        scope: &mut G,
        config: RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<TransactionId>> + 'static,
        _start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        Collection<G, (usize, Result<SourceMessage<(), Row>, SourceReaderError>), Diff>,
        Option<Stream<G, Infallible>>,
        Stream<G, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        // Determined which collections need to be snapshot and which already have been.
        let subsource_resume_uppers: BTreeMap<_, _> = config
            .source_resume_uppers
            .iter()
            .map(|(id, upper)| {
                assert!(
                    config.source_exports.contains_key(id),
                    "all source resume uppers must be present in source exports"
                );

                (
                    *id,
                    Antichain::from_iter(upper.iter().map(TransactionId::decode_row)),
                )
            })
            .collect();

        // Collect the tables that we will be ingesting.
        // TODO(roshan): Get this from self.details.tables (MySqlSourceDetails)
        let tables = vec![MySqlTableDesc {
            schema_name: "dummyschema".to_string(),
            name: "dummy".to_string(),
            columns: vec![
                MySqlColumnDesc {
                    name: "f1".to_string(),
                    data_type: MySqlDataType::Int,
                    nullable: false,
                },
                MySqlColumnDesc {
                    name: "id".to_string(),
                    data_type: MySqlDataType::Varchar(128),
                    nullable: false,
                },
            ],
        }];
        let mut table_info = BTreeMap::new();
        for (i, desc) in tables.iter().enumerate() {
            table_info.insert(
                desc.qualified_name(),
                (
                    // Index zero maps to the main source
                    i + 1,
                    desc.clone(),
                    // TODO: implement table casts
                    vec![],
                ),
            );
        }

        let (snapshot_updates, rewinds, snapshot_err, snapshot_token) = snapshot::render(
            scope.clone(),
            config.clone(),
            self.clone(),
            subsource_resume_uppers.clone(),
            table_info.clone(),
        );

        // let (repl_updates, uppers, repl_err, repl_token) = replication::render(
        //     scope.clone(),
        //     config,
        //     self,
        //     subsource_resume_uppers,
        //     table_info,
        //     &rewinds,
        //     resume_uppers,
        // );
        let uppers = None;

        let updates = snapshot_updates
            //.concat(&repl_updates)
            .map(|(output, res)| {
                let res = res.map(|row| SourceMessage {
                    key: (),
                    value: row,
                    metadata: Row::default(),
                });
                (output, res)
            });

        // TODO: concat repl_err
        let health = snapshot_err.flat_map(move |err| {
            // This update will cause the dataflow to restart
            let err_string = err.display_with_causes().to_string();
            let update = HealthStatusUpdate::halting(err_string.clone(), None);
            // TODO: change namespace for SSH errors
            let namespace = Self::STATUS_NAMESPACE.clone();
            let mut statuses = vec![HealthStatusMessage {
                index: 0,
                namespace: namespace.clone(),
                update,
            }];

            // But we still want to report the transient error for all subsources
            statuses.extend(tables.iter().enumerate().map(|(index, _)| {
                let status = HealthStatusUpdate::stalled(err_string.clone(), None);
                HealthStatusMessage {
                    index: index + 1,
                    namespace,
                    update: status,
                }
            }));
            statuses
        });

        (
            updates,
            uppers,
            health,
            vec![
                snapshot_token,
                // repl_token
            ],
        )
    }
}

/// A transient error that never ends up in the collection of a specific table.
#[derive(Debug, thiserror::Error)]
pub enum TransientError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("sql client error")]
    SQLClient(#[from] mysql_async::Error),
    #[error(transparent)]
    MySqlError(#[from] MySqlError),
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
}

/// A definite error that always ends up in the collection of a specific table.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum DefiniteError {
    #[error("server gtid error: {0}")]
    ServerGTIDError(String),
}

/// TODO: This should use a partitioned timestamp implementation instead of the snapshot gtid string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RewindRequest {
    /// The table FQN that should be rewound.
    pub(crate) table_qualified_name: String,
    /// The GTID set at the start of the snapshot, returned by the server's `gtid_executed` system variable.
    pub(crate) snapshot_gtid_set: String,
}

impl RewindRequest {
    // For now we're assuming there is only one source-id in the GTID set.
    /// We can assume that we only need the highest transaction-id per source-id from
    /// the GTID set because we are ensuring the server has replica_preserve_commit_order=1
    /// set, which means that the GTID's per source-id are guaranteed to be monotonically increasing.
    /// When the replication streams start they can present the GTID set as source_id:1-highest_transaction_id
    pub(crate) fn highest_transaction_id(&self) -> Result<TransactionId, TransientError> {
        let start_gtid_set: Vec<&str> = self.snapshot_gtid_set.split(", ").collect();
        // TODO: Once we switch to a partitioned timestamp format we should support more than one source-id in the GTID set
        if start_gtid_set.len() != 1 {
            return Err(TransientError::Generic(anyhow::anyhow!(
                "expected a single source-id in the gtid_executed system variable, got: {:?}",
                start_gtid_set
            )));
        }
        // Parse the highest transaction-id from the intervals in the GTID range for the source-id
        // e.g. extract '9' from 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:4:5-9
        // NOTE: There might be discrete intervals if they have not been compacted yet but they are
        // guaranteed to be monotonically increasing since we earlier verified replica_preserve_commit_order=1
        // which is why we only care about the highest value
        let highest_seen_transaction_id: i64 = start_gtid_set[0]
            .rsplit_once(":")
            .expect("at least one interval")
            .1
            .rsplit("-")
            .next()
            .expect("at least one")
            .parse()
            .expect("valid number");
        Ok(TransactionId::new(highest_seen_transaction_id))
    }
}
