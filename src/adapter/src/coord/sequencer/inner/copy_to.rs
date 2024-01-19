// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::str::FromStr;

use http::Uri;

use mz_repr::{Datum, RowArena};
use mz_sql::plan::{self, CopyToTarget};
use timely::progress::Antichain;

use crate::coord::sequencer::inner::{check_log_reads, return_if_err};
use crate::coord::{
    Coordinator, CopyToFinish, CopyToOptimizeLir, CopyToOptimizeMir, CopyToStage, CopyToTimestamp,
    CopyToValidate, PlanValidity, TargetCluster,
};
use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::optimize::Optimize;
use crate::session::Session;
use crate::{optimize, AdapterError, ExecuteContext, ExecuteResponse};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_copy_to(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CopyToPlan,
        target_cluster: TargetCluster,
    ) {
        self.sequence_copy_to_stage(
            ctx,
            CopyToStage::Validate(CopyToValidate {
                plan,
                target_cluster,
            }),
        )
        .await;
    }

    /// Processes as many `copy_to` stages as possible.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_copy_to_stage(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: CopyToStage,
    ) {
        use CopyToStage::*;

        // Process the current stage and allow for processing the next.
        loop {
            // Always verify plan validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                return_if_err!(validity.check(self.catalog()), ctx);
            }

            (ctx, stage) = match stage {
                Validate(stage) => {
                    let next = return_if_err!(self.copy_to_validate(ctx.session_mut(), stage), ctx);
                    (ctx, CopyToStage::OptimizeMir(next))
                }
                OptimizeMir(stage) => {
                    let next = return_if_err!(self.copy_to_optimize_mir(&mut ctx, stage), ctx);
                    (ctx, CopyToStage::Timestamp(next))
                }
                Timestamp(stage) => {
                    let next = return_if_err!(self.copy_to_timestamp(&mut ctx, stage).await, ctx);
                    (ctx, CopyToStage::OptimizeLir(next))
                }
                OptimizeLir(stage) => {
                    let next = return_if_err!(self.copy_to_optimize_lir(&mut ctx, stage), ctx);
                    (ctx, CopyToStage::Finish(next))
                }
                Finish(stage) => {
                    let result = self.copy_to_finish(&mut ctx, stage).await;
                    ctx.retire(result);
                    return;
                }
            }
        }
    }

    fn copy_to_validate(
        &mut self,
        session: &mut Session,
        CopyToValidate {
            mut plan,
            target_cluster,
        }: CopyToValidate,
    ) -> Result<CopyToOptimizeMir, AdapterError> {
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::NotAvailable,
            session,
            catalog_state: self.catalog().state(),
        };
        let CopyToTarget::Unresolved(mut to_expr) = plan.to else {
            // TODO refactor and fix
            todo!()
        };
        prep_scalar_expr(&mut to_expr, style)?;
        let temp_storage = RowArena::new();
        let evaled = to_expr.eval(&[], &temp_storage)?;
        if evaled == Datum::Null {
            coord_bail!("COPY TO target value can not be null");
        }
        let to_url = match Uri::from_str(evaled.unwrap_str()) {
            Ok(url) => {
                if url.scheme_str() != Some("s3") {
                    coord_bail!("only 's3://...' urls are supported as COPY TO target");
                }
                url
            }
            Err(e) => coord_bail!("could not parse COPY TO target url: {}", e),
        };
        plan.to = CopyToTarget::Resolved(to_url);

        let cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, session)?;
        let cluster_id = cluster.id;

        let mut replica_id = session
            .vars()
            .cluster_replica()
            .map(|name| {
                cluster
                    .replica_id(name)
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;
        // TODO (mouli): not sure what to do here
        // let determination = session.get_transaction_timestamp_determination();
        // session.add_transaction_ops(TransactionOps::Peeks {
        //     determination: todo!(),
        //     cluster_id: cluster_id,
        // })?;

        let depends_on = &plan.from.depends_on();

        // Run `check_log_reads` and emit notices.
        let notices = check_log_reads(
            self.catalog(),
            cluster,
            depends_on,
            &mut replica_id,
            session.vars(),
        )?;
        session.add_notices(notices);

        // Determine timeline.
        let mut timeline = self.validate_timeline_context(depends_on.clone())?;

        let validity = PlanValidity {
            transient_revision: self.catalog().transient_revision(),
            dependency_ids: depends_on.clone(),
            cluster_id: Some(cluster_id),
            replica_id,
            role_metadata: session.role_metadata().clone(),
        };

        Ok(CopyToOptimizeMir {
            validity,
            plan,
            timeline,
        })
    }

    fn copy_to_optimize_mir(
        &mut self,
        ctx: &mut ExecuteContext,
        CopyToOptimizeMir {
            validity,
            plan,
            timeline,
        }: CopyToOptimizeMir,
    ) -> Result<CopyToTimestamp, AdapterError> {
        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(validity.cluster_id.expect("cluser_id"))
            .expect("compute instance does not exist");
        let id = self.allocate_transient_id()?;
        let conn_id = ctx.session().conn_id().clone();

        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this COPY TO.
        let mut optimizer = optimize::copy_to::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            id,
            conn_id,
            optimizer_config,
        );

        let global_mir_plan = optimizer
            .catch_unwind_optimize(plan.from.clone())
            .map_err(|e| AdapterError::Optimizer(e))?;

        Ok(CopyToTimestamp {
            validity,
            plan,
            timeline,
            optimizer,
            global_mir_plan,
        })
    }

    async fn copy_to_timestamp(
        &mut self,
        ctx: &mut ExecuteContext,
        CopyToTimestamp {
            validity,
            plan,
            timeline,
            optimizer,
            global_mir_plan,
        }: CopyToTimestamp,
    ) -> Result<CopyToOptimizeLir, AdapterError> {
        let when = plan::QueryWhen::Immediately;
        // Timestamp selection
        let oracle_read_ts = self.oracle_read_ts(&ctx.session, &timeline, &when).await;
        let as_of = match self
            .determine_timestamp(
                ctx.session(),
                &global_mir_plan.id_bundle(optimizer.cluster_id()),
                &when,
                optimizer.cluster_id(),
                &timeline,
                oracle_read_ts,
                None,
            )
            .await
        {
            Ok(v) => v.timestamp_context.timestamp_or_default(),
            Err(e) => return Err(e),
        };
        if let Some(id) = ctx.extra().contents() {
            self.set_statement_execution_timestamp(id, as_of);
        }

        Ok(CopyToOptimizeLir {
            validity,
            plan,
            optimizer,
            global_mir_plan: global_mir_plan.resolve(Antichain::from_elem(as_of)),
        })
    }

    fn copy_to_optimize_lir(
        &mut self,
        ctx: &mut ExecuteContext,
        CopyToOptimizeLir {
            validity,
            plan,
            mut optimizer,
            global_mir_plan,
        }: CopyToOptimizeLir,
    ) -> Result<CopyToFinish, AdapterError> {
        let global_lir_plan = optimizer
            .catch_unwind_optimize(global_mir_plan.clone())
            .map_err(|e| AdapterError::Optimizer(e))?;

        Ok(CopyToFinish {
            validity,
            cluster_id: optimizer.cluster_id(),
            plan,
            global_lir_plan,
        })
    }

    async fn copy_to_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        CopyToFinish {
            validity,
            cluster_id,
            plan: plan::CopyToPlan { to, .. },
            global_lir_plan,
        }: CopyToFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let sink_id = global_lir_plan.sink_id();

        Err(AdapterError::Internal(format!(
            "COPY TO '{:?}' is not yet implemented",
            to,
        )))
    }
}
