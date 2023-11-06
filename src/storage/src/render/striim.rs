// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::{AsCollection, Collection};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::StriimEnvelope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{OkErr, Operator};
use timely::dataflow::{Scope, ScopeParent};

use crate::source::types::DecodeResult;

pub(crate) fn render<G: Scope>(
    envelope: &StriimEnvelope,
    input: &Collection<G, DecodeResult, Diff>,
) -> (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>)
where
    G: ScopeParent<Timestamp = Timestamp>,
{
    let (before_idx, after_idx) = (envelope.before_idx, envelope.after_idx);
    let (oks, errs) = input
        .inner
        .unary(Pipeline, "envelope-striim", move |_, _| {
            let mut data = vec![];
            move |input, output| {
                while let Some((cap, refmut_data)) = input.next() {
                    let mut session = output.session(&cap);
                    refmut_data.swap(&mut data);
                    for (result, time, diff) in data.drain(..) {
                        let value = match result.value {
                            Some(Ok(value)) => value,
                            Some(Err(err)) => {
                                session.give((Err(err.into()), time, diff));
                                continue;
                            }
                            None => continue,
                        };

                        match value.iter().nth(before_idx).unwrap() {
                            Datum::List(l) => session.give((Ok(Row::pack(&l)), time, -diff)),
                            Datum::Null => {}
                            d => panic!("type error: expected record, found {:?}", d),
                        }
                        match value.iter().nth(after_idx).unwrap() {
                            Datum::List(l) => session.give((Ok(Row::pack(&l)), time, diff)),
                            Datum::Null => {}
                            d => panic!("type error: expected record, found {:?}", d),
                        }
                    }
                }
            }
        })
        .ok_err(|(res, time, diff)| match res {
            Ok(v) => Ok((v, time, diff)),
            Err(e) => Err((e, time, diff)),
        });
    (oks.as_collection(), errs.as_collection())
}
