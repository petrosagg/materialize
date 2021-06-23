// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::{File, OpenOptions};

use differential_dataflow::{Collection, Hashable};

use itertools::repeat_n;
use log::error;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::Scope;

use dataflow_types::AvroOcfSinkConnector;
use expr::GlobalId;
use interchange::avro::{encode_datums_as_avro, Encoder};
use mz_avro::{self, Writer};
use repr::{RelationDesc, Row, Timestamp};

pub fn avro_ocf<G>(
    collection: Collection<G, (Option<Row>, Option<Row>)>,
    id: GlobalId,
    connector: AvroOcfSinkConnector,
    desc: RelationDesc,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let collection = collection.map(|(k, v)| {
        assert!(k.is_none(), "Avro OCF sinks must not have keys");
        let v = v.expect("Avro OCF sinks must have values");
        v
    });
    let (schema, columns) = {
        let encoder = Encoder::new(None, desc, false);
        let schema = encoder.value_writer_schema().clone();
        let columns = encoder.value_columns().to_vec();
        (schema, columns)
    };

    let create_avro_writer = move || {
        let res = OpenOptions::new().append(true).open(&connector.path);
        match res {
            Ok(f) => Ok(mz_avro::Writer::new(schema.clone(), f)),
            Err(e) => Err(format!(
                "creating avro ocf file writer for sink failed: {}",
                e
            )),
        }
    };

    let mut vector = vec![];
    let mut avro_writer: Option<Result<Writer<File>, _>> = None;

    // We want exactly one worker to write to the single output file
    let hashed_id = id.hashed();

    collection.inner.sink(
        Exchange::new(move |_| hashed_id),
        &format!("avro-ocf-{}", id),
        move |input| {
            input.for_each(|_, rows| {
                rows.swap(&mut vector);

                let avro_writer = avro_writer.get_or_insert_with(|| create_avro_writer());

                let avro_writer = match avro_writer.as_mut() {
                    Ok(avro_writer) => avro_writer,
                    Err(e) => {
                        error!("{}", e);
                        return;
                    }
                };

                for (v, _time, diff) in vector.drain(..) {
                    let value = encode_datums_as_avro(v.iter(), &columns);
                    assert!(diff > 0, "can't sink negative multiplicities");
                    for value in repeat_n(value, diff as usize) {
                        if let Err(e) = avro_writer.append(value) {
                            error!("appending to avro ocf failed: {}", e)
                        };
                    }
                }
                let res = avro_writer.flush();
                match res {
                    Ok(_) => (),
                    Err(e) => error!("flushing bytes to avro ocf failed: {}", e),
                }
            })
        },
    )
}
