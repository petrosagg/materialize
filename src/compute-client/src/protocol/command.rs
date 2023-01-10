// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute protocol commands.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroI64;
use std::time::Duration;

use proptest::prelude::{any, Arbitrary};
use proptest::strategy::{BoxedStrategy, Strategy, Union};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use uuid::Uuid;

use mz_expr::RowSetFinishing;
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_proto::{any_uuid, IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, Row};
use mz_storage_client::client::ProtoAllowCompaction;
use mz_storage_client::controller::CollectionMetadata;

use crate::logging::LoggingConfig;
use crate::types::dataflows::DataflowDescription;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_compute_client.protocol.command.rs"
));

/// Compute protocol commands, sent by the compute controller to replicas.
///
/// Command sequences sent by the compute controller must be valid according to the [Protocol
/// Stages].
///
/// [Protocol Stages]: super#protocol-stages
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeCommand<T = mz_repr::Timestamp> {
    /// `CreateTimely` is the first command sent to a replica after a connection was established.
    /// It instructs the replica to initialize the timely dataflow runtime using the given
    /// `config`.
    ///
    /// This command is special in that it is the only one that is broadcast to all processes of a
    /// multi-process replica. All subsequent commands are only sent to the first process, which
    /// then distributes them to the other processes using a dataflow. This method of command
    /// distribution requires the timely dataflow runtime to be initialized, which is why the
    /// `CreateTimely` command exists.
    ///
    /// The `epoch` value imposes an ordering on iterations of the compute protocol. When the
    /// compute controller connects to a replica, it must send an `epoch` that is greater than all
    /// epochs it sent to the same replica on previous connections. Multi-process replicas should
    /// use the `epoch` to ensure that their individual processes agree on which protocol iteration
    /// they are in.
    CreateTimely {
        config: TimelyConfig,
        epoch: ComputeStartupEpoch,
    },

    /// `CreateInstance` must be sent after `CreateTimely` to complete the [Creation Stage] of the
    /// compute protocol. Unlike `CreateTimely`, and like all other commands, it is only sent to
    /// the first process of the replica, and then distributed through the timely runtime.
    /// `CreateInstance` instructs the replica to initialize its state to a point where it is ready
    /// to start maintaining dataflows.
    ///
    /// Upon receiving a `CreateInstance` command, the replica must further initialize logging
    /// dataflows according to the given [`LoggingConfig`].
    ///
    /// [Creation Stage]: super#creation-stage
    CreateInstance(LoggingConfig),

    /// `InitializationComplete` informs the replica about the end of the [Initialization Stage].
    /// Upon receiving this command, the replica should perform a reconciliation process, to ensure
    /// its dataflow state matches the state requested by the computation commands it received
    /// previously. The replica must now start sending responses to commands received previously,
    /// if it opted to defer them during the [Initialization Stage].
    ///
    /// [Initialization Stage]: super#initialization-stage
    InitializationComplete,

    /// `UpdateConfiguration` instructs the replica to update its configuration, according to the
    /// given [`ComputeParameter`]s.
    ///
    /// Parameter updates transmitted through this command must be applied by the replica as soon
    /// as it receives the command, and they must be apply globally to all replica state, even
    /// dataflows and pending peeks that were created before the parameter update. This property
    /// allows the replica to hoist `UpdateConfiguration` commands during reconciliation.
    ///
    /// Configuration parameters that should not be applied globally, but only to specific
    /// dataflows or peeks, should be added to the [`DataflowDescription`] or [`Peek`] types,
    /// rather than as [`ComputeParameter`]s.
    UpdateConfiguration(BTreeSet<ComputeParameter>),

    /// `CreateDataflows` instructs the replica to create and start maintaining dataflows according
    /// to the given [`DataflowDescription`]s.
    ///
    /// If a `CreateDataflows` command defines multiple dataflows, the list of
    /// [`DataflowDescription`]s must be topologically ordered according to the dependency
    /// relation.
    ///
    /// Each [`DataflowDescription`] must have the following properties:
    ///
    ///   * Dataflow imports are valid:
    ///     * Imported storage collections specified in [`source_imports`] exist and are readable by
    ///       the compute replica.
    ///     * Imported indexes specified in [`index_imports`] have been created on the replica
    ///       previously, either by previous `CreateDataflows` commands, or by the same
    ///       `CreateDataflows` command.
    ///   * Dataflow imports are readable at the specified [`as_of`]. In other words: The `since`s of
    ///     imported collections are not beyond the dataflow [`as_of`].
    ///   * Dataflow exports have unique IDs, i.e., the IDs of exports from dataflows a replica is
    ///     instructed to create do not repeat (within a single protocol iteration).
    ///   * The dataflow objects defined in [`objects_to_build`] are topologically ordered according
    ///     to the dependency relation.
    ///
    /// A dataflow description that violates any of the above properties can cause the replica to
    /// exhibit undefined behavior, such as panicking or production of incorrect results. A replica
    /// should prefer panicking over producing incorrect results.
    ///
    /// After receiving a `CreateDataflows` command, for created dataflows that export indexes or
    /// storage sinks, the replica must produce [`FrontierUppers`] responses that report the
    /// advancement of the `upper` frontiers of these compute collections.
    ///
    /// After receiving a `CreateDataflows` command, for created dataflows that export subscribes,
    /// the replica must produce [`SubscribeResponse`]s that report the progress and results of the
    /// subscribes.
    ///
    /// During the [Initialization Stage], the controller must not send `CreateDataflows` commands
    /// that instruct the creation of dataflows exporting subscribes. This is a limitation of our
    /// current implementation that we indend to remove ([#16247]).
    ///
    /// [`objects_to_build`]: DataflowDescription::objects_to_build
    /// [`source_imports`]: DataflowDescription::source_imports
    /// [`index_imports`]: DataflowDescription::index_imports
    /// [`as_of`]: DataflowDescription::as_of
    /// [`FrontierUppers`]: super::response::ComputeResponse::FrontierUppers
    /// [`SubscribeResponse`]: super::response::ComputeResponse::SubscribeResponse
    /// [Initialization Stage]: super#initialization-stage
    /// [#16247]: https://github.com/MaterializeInc/materialize/issues/16247
    CreateDataflows(Vec<DataflowDescription<crate::plan::Plan<T>, CollectionMetadata, T>>),

    /// `AllowCompaction` informs the replica about the relaxation of external read capabilities on
    /// the compute collections exported by the replica’s dataflow.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct. The replica gains the liberty of compacting the
    /// corresponding maintained traces up through that frontier.
    ///
    /// It is invalid to send an `AllowCompaction` command that references compute collections that
    /// were not created by a corresponding `CreateDataflows` command before. Doing so may cause
    /// the replica to exhibit undefined behavior.
    ///
    /// The `AllowCompaction` command only informs about external read requirements, not internal
    /// ones. The replica is responsible for ensuring that internal requirements are fulfilled at
    /// all times, so local dataflow inputs are not compacted beyond times at which they are still
    /// being read from.
    ///
    /// The read frontiers transmitted through `AllowCompactions` may be beyond the corresponding
    /// collections' current `upper` frontiers. This signals that external readers are not
    /// interested in times up to the specified new read frontiers. Consequently, an empty read
    /// frontier signals that external readers are not interested in updates from the corresponding
    /// collection ever again, so the collection is not required anymore.
    ///
    /// Sending an `AllowCompaction` command with the empty frontier is the canonical way to drop
    /// compute collections.
    ///
    /// A replica that receives an `AllowCompaction` command with the empty frontier must
    /// eventually respond with a [`FrontierUppers`] response reporting the empty frontier for the
    /// same collection. ([#16275])
    ///
    /// [`FrontierUppers`]: super::response::ComputeResponse::FrontierUppers
    /// [#16275]: https://github.com/MaterializeInc/materialize/issues/16275
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),

    /// `Peek` instructs the replica to perform a peek at an index.
    ///
    /// The [`Peek`] description must have the following properties:
    ///
    ///   * The target index has previously been created by a corresponding `CreateDataflows`
    ///     command.
    ///   * The [`Peek::uuid`] is unique, i.e., the UUIDs of peeks a replica gets instructed to
    ///     perform do not repeat (within a single protocol iteration).
    ///
    /// A [`Peek`] description that violates any of the above properties can cause the replica to
    /// exhibit undefined behavior.
    ///
    /// Specifying a [`Peek::timestamp`] that is less than the target index’s `since` frontier does
    /// not provoke undefined behavior. Instead, the replica must produce a [`PeekResponse::Error`]
    /// in response.
    ///
    /// After receiving a `Peek` command, the replica must eventually produce a single
    /// [`PeekResponse`]:
    ///
    ///    * For peeks that were not cancelled: either [`Rows`] or [`Error`].
    ///    * For peeks that were cancelled: either [`Rows`], or [`Error`], or [`Canceled`].
    ///
    /// [`PeekResponse`]: super::response::PeekResponse
    /// [`PeekResponse::Error`]: super::response::PeekResponse::Error
    /// [`Rows`]: super::response::PeekResponse::Rows
    /// [`Error`]: super::response::PeekResponse::Error
    /// [`Canceled`]: super::response::PeekResponse::Canceled
    Peek(Peek<T>),

    /// `CancelPeeks` instructs the replica to cancel the identified pending peeks.
    ///
    /// It is invalid to send a `CancelPeeks` command that references peeks that were not created
    /// by a corresponding `Peek` command before. Doing so may cause the replica to exhibit
    /// undefined behavior.
    ///
    /// If a replica cancels a peek in response to a `CancelPeeks` command, it must respond with a
    /// [`PeekResponse::Canceled`]. The replica may also decide to fulfill the peek instead and
    /// return a different [`PeekResponse`], or it may already have returned a response to the
    /// specified peek. In these cases it must *not* return another [`PeekResponse`].
    ///
    /// [`PeekResponse`]: super::response::PeekResponse
    /// [`PeekResponse::Canceled`]: super::response::PeekResponse::Canceled
    CancelPeeks {
        /// The identifiers of the peek requests to cancel.
        ///
        /// Values in this set must match [`Peek::uuid`] values transmitted in previous `Peek`
        /// commands.
        uuids: BTreeSet<Uuid>,
    },
}

impl RustType<ProtoComputeCommand> for ComputeCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoComputeCommand {
        use proto_compute_command::Kind::*;
        use proto_compute_command::*;
        ProtoComputeCommand {
            kind: Some(match self {
                ComputeCommand::CreateTimely { config, epoch } => CreateTimely(ProtoCreateTimely {
                    config: Some(config.into_proto()),
                    epoch: Some(epoch.into_proto()),
                }),
                ComputeCommand::CreateInstance(logging) => CreateInstance(logging.into_proto()),
                ComputeCommand::InitializationComplete => InitializationComplete(()),
                ComputeCommand::UpdateConfiguration(params) => {
                    UpdateConfiguration(ProtoUpdateConfiguration {
                        params: params.into_proto(),
                    })
                }
                ComputeCommand::CreateDataflows(dataflows) => {
                    CreateDataflows(ProtoCreateDataflows {
                        dataflows: dataflows.into_proto(),
                    })
                }
                ComputeCommand::AllowCompaction(collections) => {
                    AllowCompaction(ProtoAllowCompaction {
                        collections: collections.into_proto(),
                    })
                }
                ComputeCommand::Peek(peek) => Peek(peek.into_proto()),
                ComputeCommand::CancelPeeks { uuids } => CancelPeeks(ProtoCancelPeeks {
                    uuids: uuids.into_proto(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeCommand) -> Result<Self, TryFromProtoError> {
        use proto_compute_command::Kind::*;
        use proto_compute_command::*;
        match proto.kind {
            Some(CreateTimely(ProtoCreateTimely { config, epoch })) => {
                Ok(ComputeCommand::CreateTimely {
                    config: config.into_rust_if_some("ProtoCreateTimely::config")?,
                    epoch: epoch.into_rust_if_some("ProtoCreateTimely::epoch")?,
                })
            }
            Some(CreateInstance(logging)) => {
                Ok(ComputeCommand::CreateInstance(logging.into_rust()?))
            }
            Some(InitializationComplete(())) => Ok(ComputeCommand::InitializationComplete),
            Some(UpdateConfiguration(ProtoUpdateConfiguration { params })) => {
                Ok(ComputeCommand::UpdateConfiguration(params.into_rust()?))
            }
            Some(CreateDataflows(ProtoCreateDataflows { dataflows })) => {
                Ok(ComputeCommand::CreateDataflows(dataflows.into_rust()?))
            }
            Some(AllowCompaction(ProtoAllowCompaction { collections })) => {
                Ok(ComputeCommand::AllowCompaction(collections.into_rust()?))
            }
            Some(Peek(peek)) => Ok(ComputeCommand::Peek(peek.into_rust()?)),
            Some(CancelPeeks(ProtoCancelPeeks { uuids })) => Ok(ComputeCommand::CancelPeeks {
                uuids: uuids.into_rust()?,
            }),
            None => Err(TryFromProtoError::missing_field(
                "ProtoComputeCommand::kind",
            )),
        }
    }
}

impl Arbitrary for ComputeCommand<mz_repr::Timestamp> {
    type Strategy = Union<BoxedStrategy<Self>>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
                any::<LoggingConfig>()
                    .prop_map(ComputeCommand::CreateInstance)
                    .boxed(),
                proptest::collection::btree_set(any::<ComputeParameter>(), 1..4)
                    .prop_map(ComputeCommand::UpdateConfiguration)
                    .boxed(),
                proptest::collection::vec(
                    any::<
                        DataflowDescription<
                            crate::plan::Plan,
                            CollectionMetadata,
                            mz_repr::Timestamp,
                        >,
                    >(),
                    1..4,
                )
                .prop_map(ComputeCommand::CreateDataflows)
                .boxed(),
                proptest::collection::vec(
                    (
                        any::<GlobalId>(),
                        proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4),
                    ),
                    1..4,
                )
                .prop_map(|collections| {
                    ComputeCommand::AllowCompaction(
                        collections
                            .into_iter()
                            .map(|(id, frontier_vec)| (id, Antichain::from(frontier_vec)))
                            .collect(),
                    )
                })
                .boxed(),
                any::<Peek>().prop_map(ComputeCommand::Peek).boxed(),
                proptest::collection::vec(any_uuid(), 1..6)
                    .prop_map(|uuids| ComputeCommand::CancelPeeks {
                        uuids: BTreeSet::from_iter(uuids.into_iter()),
                    })
                    .boxed(),
            ])
    }
}

/// A value generated by environmentd and passed to the clusterd processes
/// to help them disambiguate different `CreateTimely` commands.
///
/// The semantics of this value are not important, except that they
/// must be totally ordered, and any value (for a given replica) must
/// be greater than any that were generated before (for that replica).
/// This is the reason for having two
/// components (one from the stash that increases on every environmentd restart,
/// another in-memory and local to the current incarnation of environmentd)
#[derive(PartialEq, Eq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct ComputeStartupEpoch {
    envd: NonZeroI64,
    replica: u64,
}

impl RustType<ProtoComputeStartupEpoch> for ComputeStartupEpoch {
    fn into_proto(&self) -> ProtoComputeStartupEpoch {
        let Self { envd, replica } = self;
        ProtoComputeStartupEpoch {
            envd: envd.get(),
            replica: *replica,
        }
    }

    fn from_proto(proto: ProtoComputeStartupEpoch) -> Result<Self, TryFromProtoError> {
        let ProtoComputeStartupEpoch { envd, replica } = proto;
        Ok(Self {
            envd: envd.try_into().unwrap(),
            replica,
        })
    }
}

impl ComputeStartupEpoch {
    pub fn new(envd: NonZeroI64, replica: u64) -> Self {
        Self { envd, replica }
    }

    /// Serialize for transfer over the network
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut ret = [0; 16];
        let mut p = &mut ret[..];
        use std::io::Write;
        p.write_all(&self.envd.get().to_be_bytes()[..]).unwrap();
        p.write_all(&self.replica.to_be_bytes()[..]).unwrap();
        ret
    }

    /// Inverse of `to_bytes`
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let envd = i64::from_be_bytes((&bytes[0..8]).try_into().unwrap());
        let replica = u64::from_be_bytes((&bytes[8..16]).try_into().unwrap());
        Self {
            envd: envd.try_into().unwrap(),
            replica,
        }
    }
}

impl std::fmt::Display for ComputeStartupEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { envd, replica } = self;
        write!(f, "({envd}, {replica})")
    }
}

impl PartialOrd for ComputeStartupEpoch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ComputeStartupEpoch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let Self { envd, replica } = self;
        let Self {
            envd: other_envd,
            replica: other_replica,
        } = other;
        (envd, replica).cmp(&(other_envd, other_replica))
    }
}

/// Configuration of the cluster we will spin up
#[derive(Arbitrary, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TimelyConfig {
    /// Number of per-process worker threads
    pub workers: usize,
    /// Identity of this process
    pub process: usize,
    /// Addresses of all processes
    pub addresses: Vec<String>,
    /// The amount of effort to be spent on arrangement compaction during idle times.
    ///
    /// See [`differential_dataflow::Config::idle_merge_effort`].
    pub idle_arrangement_merge_effort: u32,
}

impl RustType<ProtoTimelyConfig> for TimelyConfig {
    fn into_proto(&self) -> ProtoTimelyConfig {
        ProtoTimelyConfig {
            workers: self.workers.into_proto(),
            addresses: self.addresses.into_proto(),
            process: self.process.into_proto(),
            idle_arrangement_merge_effort: self.idle_arrangement_merge_effort,
        }
    }

    fn from_proto(proto: ProtoTimelyConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            process: proto.process.into_rust()?,
            workers: proto.workers.into_rust()?,
            addresses: proto.addresses.into_rust()?,
            idle_arrangement_merge_effort: proto.idle_arrangement_merge_effort,
        })
    }
}

/// Compute instance configuration parameters.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ComputeParameter {
    /// The maximum allowed size in bytes for results of peeks and subscribes.
    ///
    /// Peeks and subscribes that would return results larger than this maximum return the
    /// respective error responses instead:
    ///   * [`PeekResponse::Rows`] is replaced by [`PeekResponse::Error`].
    ///   * The [`SubscribeBatch::updates`] field is populated with an [`Err`] value.
    ///
    /// [`PeekResponse::Rows`]: super::response::PeekResponse::Rows
    /// [`PeekResponse::Error`]: super::response::PeekResponse::Error
    /// [`SubscribeBatch::updates`]: super::response::SubscribeBatch::updates
    MaxResultSize(u32),

    /// Configures `PersistConfig::blob_target_size`.
    PersistBlobTargetSize(usize),
    /// Configures `PersistConfig::compaction_minimum_timeout`.
    PersistCompactionMinimumTimeout(Duration),
}

impl ComputeParameter {
    pub fn key(&self) -> &'static str {
        match self {
            Self::MaxResultSize(_) => "max_result_size",
            Self::PersistBlobTargetSize(_) => "persist_blob_target_size",
            Self::PersistCompactionMinimumTimeout(_) => "persist_compaction_minimum_timeout",
        }
    }
}

impl fmt::Display for ComputeParameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::MaxResultSize(v) => v.to_string(),
            Self::PersistBlobTargetSize(v) => v.to_string(),
            Self::PersistCompactionMinimumTimeout(v) => format!("{v:?}"),
        };
        write!(f, "{}={}", self.key(), value)
    }
}

impl RustType<ProtoComputeParameter> for ComputeParameter {
    fn into_proto(&self) -> ProtoComputeParameter {
        use proto_compute_parameter::*;

        ProtoComputeParameter {
            kind: Some(match self {
                ComputeParameter::MaxResultSize(v) => Kind::MaxResultSize(*v),
                ComputeParameter::PersistBlobTargetSize(v) => {
                    Kind::PersistBlobTargetSize(v.into_proto())
                }
                ComputeParameter::PersistCompactionMinimumTimeout(v) => {
                    Kind::PersistCompactionMinimumTimeout(v.into_proto())
                }
            }),
        }
    }

    fn from_proto(proto: ProtoComputeParameter) -> Result<Self, TryFromProtoError> {
        use proto_compute_parameter::*;

        match proto.kind {
            Some(Kind::MaxResultSize(v)) => Ok(ComputeParameter::MaxResultSize(v)),
            Some(Kind::PersistBlobTargetSize(v)) => {
                Ok(ComputeParameter::PersistBlobTargetSize(usize::cast_from(v)))
            }
            Some(Kind::PersistCompactionMinimumTimeout(v)) => Ok(
                ComputeParameter::PersistCompactionMinimumTimeout(v.into_rust()?),
            ),
            None => Err(TryFromProtoError::missing_field(
                "ProtoComputeParameter::kind",
            )),
        }
    }
}

/// Peek at an arrangement.
///
/// This request elicits data from the worker, by naming an
/// arrangement and some actions to apply to the results before
/// returning them.
///
/// The `timestamp` member must be valid for the arrangement that
/// is referenced by `id`. This means that `AllowCompaction` for
/// this arrangement should not pass `timestamp` before this command.
/// Subsequent commands may arbitrarily compact the arrangements;
/// the dataflow runners are responsible for ensuring that they can
/// correctly answer the `Peek`.
#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Peek<T = mz_repr::Timestamp> {
    /// The identifier of the arrangement.
    pub id: GlobalId,
    /// If `Some`, then look up only the given keys from the arrangement (instead of a full scan).
    /// The vector is never empty.
    #[proptest(strategy = "proptest::option::of(proptest::collection::vec(any::<Row>(), 1..5))")]
    pub literal_constraints: Option<Vec<Row>>,
    /// The identifier of this peek request.
    ///
    /// Used in responses and cancellation requests.
    #[proptest(strategy = "any_uuid()")]
    pub uuid: Uuid,
    /// The logical timestamp at which the arrangement is queried.
    pub timestamp: T,
    /// Actions to apply to the result set before returning them.
    pub finishing: RowSetFinishing,
    /// Linear operation to apply in-line on each result.
    pub map_filter_project: mz_expr::SafeMfpPlan,
    /// An `OpenTelemetryContext` to forward trace information along
    /// to the compute worker to allow associating traces between
    /// the compute controller and the compute worker.
    #[proptest(strategy = "empty_otel_ctx()")]
    pub otel_ctx: OpenTelemetryContext,
}

impl RustType<ProtoPeek> for Peek {
    fn into_proto(&self) -> ProtoPeek {
        ProtoPeek {
            id: Some(self.id.into_proto()),
            key: match &self.literal_constraints {
                // In the Some case, the vector is never empty, so it's safe to encode None as an
                // empty vector, and Some(vector) as just the vector.
                Some(vec) => {
                    assert!(!vec.is_empty());
                    vec.into_proto()
                }
                None => Vec::<Row>::new().into_proto(),
            },
            uuid: Some(self.uuid.into_proto()),
            timestamp: self.timestamp.into(),
            finishing: Some(self.finishing.into_proto()),
            map_filter_project: Some(self.map_filter_project.into_proto()),
            otel_ctx: self.otel_ctx.clone().into(),
        }
    }

    fn from_proto(x: ProtoPeek) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            id: x.id.into_rust_if_some("ProtoPeek::id")?,
            literal_constraints: {
                let vec: Vec<Row> = x.key.into_rust()?;
                if vec.is_empty() {
                    None
                } else {
                    Some(vec)
                }
            },
            uuid: x.uuid.into_rust_if_some("ProtoPeek::uuid")?,
            timestamp: x.timestamp.into(),
            finishing: x.finishing.into_rust_if_some("ProtoPeek::finishing")?,
            map_filter_project: x
                .map_filter_project
                .into_rust_if_some("ProtoPeek::map_filter_project")?,
            otel_ctx: x.otel_ctx.into(),
        })
    }
}

fn empty_otel_ctx() -> impl Strategy<Value = OpenTelemetryContext> {
    (0..1).prop_map(|_| OpenTelemetryContext::empty())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::ProptestConfig;
    use proptest::proptest;

    use mz_proto::protobuf_roundtrip;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn peek_protobuf_roundtrip(expect in any::<Peek>() ) {
            let actual = protobuf_roundtrip::<_, ProtoPeek>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn compute_command_protobuf_roundtrip(expect in any::<ComputeCommand<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoComputeCommand>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
