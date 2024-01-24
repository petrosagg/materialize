// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::rc::Rc;

use differential_dataflow::Collection;
use mz_compute_types::sinks::{ComputeSinkDesc, CopyToSinkConnection};
use mz_ore::collections::CollectionExt;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for CopyToSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_continuous_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let name = format!("copy_to-{}", sink_id);
        let mut op = OperatorBuilder::new(name, sinked_collection.scope());
        let mut ok_input = op.new_input(&sinked_collection.inner, Pipeline);
        let mut err_input = op.new_input(&err_collection.inner, Pipeline);

        let up_to = sink.up_to.clone();
        // TODO(mouli): implement!

        op.build(|_cap| {
            let mut finished = false;
            let mut ok_buf = Default::default();

            move |frontiers| {
                if finished {
                    // Drain the inputs, to avoid the operator being constantly rescheduled
                    ok_input.for_each(|_, _| {});
                    err_input.for_each(|_, _| {});
                    return;
                }
                let mut frontier = Antichain::new();
                for input_frontier in frontiers {
                    frontier.extend(input_frontier.frontier().iter().copied());
                }
                ok_input.for_each(|_, data| {
                    data.swap(&mut ok_buf);
                    for (row, _, _) in ok_buf.drain(..) {
                        row.iter().for_each(|a| println!("************ {:?}", a))
                    }
                });
                if PartialOrder::less_equal(&up_to, &frontier) {
                    finished = true;
                }
            }
        });

        Some(Rc::new(""))
    }
}
