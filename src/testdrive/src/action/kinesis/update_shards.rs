// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::time::Duration;

use anyhow::{bail, Context};

use aws_sdk_kinesis::types::{ScalingType, StreamStatus};
use mz_ore::retry::Retry;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_update_shards(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let stream_name = format!("testdrive-{}", cmd.args.string("stream")?);
    let target_shard_count = cmd.args.parse("shards")?;
    cmd.args.done()?;

    let stream_name = format!("{}-{}", stream_name, state.seed);
    println!(
        "Updating Kinesis stream {} to have {} shards",
        stream_name, target_shard_count
    );

    state
        .kinesis_client
        .update_shard_count()
        .scaling_type(ScalingType::UniformScaling)
        .stream_name(&stream_name)
        .target_shard_count(target_shard_count)
        .send()
        .await
        .with_context(|| format!("adding shards to stream {}", &stream_name))?;

    // Verify the current shard count.
    Retry::default()
        .max_duration(cmp::max(state.default_timeout, Duration::from_secs(60)))
        .retry_async_canceling(|_| async {
            // Wait for shards to stop updating.
            let description = state
                .kinesis_client
                .describe_stream()
                .stream_name(&stream_name)
                .send()
                .await
                .context("getting current shard count")?
                .stream_description
                .unwrap();
            if description.stream_status != Some(StreamStatus::Active) {
                bail!(
                    "stream {} is not active, is {:?}",
                    stream_name,
                    description.stream_status
                );
            }

            let active_shards_len = i32::try_from(
                description
                    .shards
                    .unwrap()
                    .iter()
                    .filter(|shard| {
                        shard
                            .sequence_number_range
                            .as_ref()
                            .unwrap()
                            .ending_sequence_number
                            .is_none()
                    })
                    .count(),
            )
            .context("converting shard length to i32: {}")?;
            if active_shards_len != target_shard_count {
                bail!(
                    "expected {} shards, found {}",
                    target_shard_count,
                    active_shards_len
                );
            }
            Ok(())
        })
        .await?;
    Ok(ControlFlow::Continue)
}
