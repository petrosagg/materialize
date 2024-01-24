// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `COPY TO` statements.

use std::marker::PhantomData;
use std::sync::Arc;

use mz_adapter_types::connection::ConnectionId;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, CopyToSinkConnection};
use mz_compute_types::ComputeInstanceId;
use mz_ore::soft_assert_or_log;
use mz_repr::explain::trace_plan;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use mz_sql::plan::CopyToFrom;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::Optimizer as TransformOptimizer;
use timely::progress::Antichain;
use tracing::{span, warn, Level};

use crate::catalog::Catalog;
use crate::optimize::dataflows::{
    dataflow_import_id_bundle, ComputeInstanceSnapshot, DataflowBuilder,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerConfig, OptimizerError,
};
use crate::CollectionIdBundle;

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A transient GlobalId to be used when constructing the dataflow.
    transient_id: GlobalId,
    /// The id of the session connection in which the optimizer will run.
    conn_id: ConnectionId,
    // Optimizer config.
    config: OptimizerConfig,
}

// A bogey `Debug` implementation that hides fields. This is needed to make the
// `event!` call in `sequence_peek_stage` not emit a lot of data.
//
// For now, we skip almost all fields, but we might revisit that bit if it turns
// out that we really need those for debugging purposes.
impl std::fmt::Debug for Optimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Optimizer")
            .field("config", &self.config)
            .finish()
    }
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        transient_id: GlobalId,
        conn_id: ConnectionId,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            transient_id,
            conn_id,
            config,
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
    }
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`CopyToFrom`] plan into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone, Debug)]
pub struct GlobalMirPlan<T: Clone> {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
    phantom: PhantomData<T>,
}

impl<T: Clone> GlobalMirPlan<T> {
    /// Computes the [`CollectionIdBundle`] of the wrapped dataflow.
    pub fn id_bundle(&self, compute_instance_id: ComputeInstanceId) -> CollectionIdBundle {
        dataflow_import_id_bundle(&self.df_desc, compute_instance_id)
    }
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Clone, Debug)]
pub struct GlobalLirPlan {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalLirPlan {
    pub fn sink_id(&self) -> GlobalId {
        let sink_exports = &self.df_desc.sink_exports;
        let sink_id = sink_exports.keys().next().expect("valid sink");
        *sink_id
    }
}

/// Marker type for [`GlobalMirPlan`] structs representing an optimization
/// result without a resolved timestamp.
#[derive(Clone, Debug)]
pub struct Unresolved;

/// Marker type for [`GlobalMirPlan`] structs representing an optimization
/// result with a resolved timestamp.
///
/// The actual timestamp value is set in the [`MirDataflowDescription`] of the
/// surrounding [`GlobalMirPlan`] when we call `resolve()`.
#[derive(Clone, Debug)]
pub struct Resolved;

impl Optimize<CopyToFrom> for Optimizer {
    type To = GlobalMirPlan<Unresolved>;

    fn optimize(&mut self, plan: CopyToFrom) -> Result<Self::To, OptimizerError> {
        let sink_name = format!("copy-to-{}", self.transient_id);

        let (df_desc, df_meta) = match plan {
            CopyToFrom::Id { id, .. } => {
                let from = self.catalog.get_entry(&id);
                let from_desc = from
                    .desc(
                        &self
                            .catalog
                            .state()
                            .resolve_full_name(from.name(), Some(&self.conn_id)),
                    )
                    .expect("copy-tos can only be run on items with descs")
                    .into_owned();

                // Make SinkDesc
                let sink_id = self.transient_id;
                let sink_desc = ComputeSinkDesc {
                    from: id,
                    from_desc,
                    connection: ComputeSinkConnection::CopyTo(CopyToSinkConnection::default()),
                    with_snapshot: true,
                    up_to: Default::default(),
                    // No `FORCE NOT NULL` for copy to
                    non_null_assertions: vec![],
                    // No `REFRESH` for copy tp
                    refresh_schedule: None,
                };

                let mut df_builder =
                    DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());

                let (df_desc, df_meta) =
                    df_builder.build_sink_dataflow(sink_name, sink_id, sink_desc)?;

                (df_desc, df_meta)
            }
            CopyToFrom::Query {
                expr,
                desc,
                finishing,
            } => {
                // MIR ⇒ MIR optimization (local)
                let expr = span!(target: "optimizer", Level::DEBUG, "local").in_scope(|| {
                    #[allow(deprecated)]
                    let optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
                    let expr = optimizer.optimize(expr)?;

                    // Trace the result of this phase.
                    trace_plan(&expr);

                    Ok::<_, OptimizerError>(expr)
                })?;

                let from_desc = RelationDesc::new(expr.typ(), desc.iter_names());
                let from_id = self.transient_id;

                // Make SinkDesc
                let sink_desc = ComputeSinkDesc {
                    from: from_id,
                    from_desc,
                    connection: ComputeSinkConnection::CopyTo(CopyToSinkConnection::default()),
                    with_snapshot: true,
                    up_to: Default::default(),
                    // No `FORCE NOT NULL` for copy to
                    non_null_assertions: vec![],
                    // No `REFRESH` for copy to
                    refresh_schedule: None,
                };

                let mut df_builder =
                    DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());

                let mut df_desc = MirDataflowDescription::new(sink_name);

                df_builder.import_view_into_dataflow(&from_id, &expr, &mut df_desc)?;
                df_builder.reoptimize_imported_views(&mut df_desc, &self.config)?;

                let df_meta =
                    df_builder.build_sink_dataflow_into(&mut df_desc, from_id, sink_desc)?;

                (df_desc, df_meta)
            }
        };

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan {
            df_desc,
            df_meta,
            phantom: PhantomData::<Unresolved>,
        })
    }
}

impl GlobalMirPlan<Unresolved> {
    /// Produces the [`GlobalMirPlan`] with [`Resolved`] timestamp.
    ///
    /// We need to resolve timestamps before the `GlobalMirPlan ⇒ GlobalLirPlan`
    /// optimization stage in order to profit from possible single-time
    /// optimizations in the `Plan::finalize_dataflow` call.
    pub fn resolve(mut self, as_of: Antichain<Timestamp>) -> GlobalMirPlan<Resolved> {
        // A datalfow description for a `COPY TO` statement should not have
        // index exports.
        soft_assert_or_log!(
            self.df_desc.index_exports.is_empty(),
            "unexpectedly setting until for a DataflowDescription with an index",
        );

        // Set the `as_of` timestamp for the dataflow.
        self.df_desc.set_as_of(as_of.clone());

        if let Some(ts) = as_of.into_option() {
            if let Some(until) = ts.checked_add(1) {
                let up_to = Antichain::from_elem(until);
                self.df_desc.until = up_to.clone();
                // TODO(mouli): clean this up
                // Setting the sink up_to to as_of + 1
                for (_, sink) in &mut self.df_desc.sink_exports {
                    sink.up_to = up_to.clone();
                }
            } else {
                warn!(ts = %ts, "as_of + 1 overflow");
            }
        }

        GlobalMirPlan {
            df_desc: self.df_desc,
            df_meta: self.df_meta,
            phantom: PhantomData::<Resolved>,
        }
    }
}

impl Optimize<GlobalMirPlan<Resolved>> for Optimizer {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: GlobalMirPlan<Resolved>) -> Result<Self::To, OptimizerError> {
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
            phantom: _,
        } = plan;

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0)?
        }

        // Finalize the dataflow. This includes:
        // - MIR ⇒ LIR lowering
        // - LIR ⇒ LIR transforms
        let df_desc = Plan::finalize_dataflow(
            df_desc,
            self.config.enable_consolidate_after_union_negate,
            self.config.enable_specialized_arrangements,
            self.config.enable_reduce_mfp_fusion,
        )
        .map_err(OptimizerError::Internal)?;

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}

impl GlobalLirPlan {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}
