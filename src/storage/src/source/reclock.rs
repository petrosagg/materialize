// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timestamper using persistent collection
use std::collections::{hash_map, vec_deque, HashMap, VecDeque};
use std::time::Duration;
use std::vec;

use anyhow::Context;
use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use futures::StreamExt;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::progress::Timestamp as _;
use timely::PartialOrder;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_dataflow_types::sources::MzOffset;
use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_persist_client::read::{ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::{Diff, Timestamp};

pub struct ReclockOperator {
    /// A dTVC of the remap collection containing updates at `t` such
    /// that `since_ts <= t < upper_ts` indexed by partition id
    remap_trace: HashMap<PartitionId, VecDeque<(Timestamp, Diff)>>,
    /// The since frontier of the partial remap trace in terms of IntoTime
    into_since: Antichain<Timestamp>,
    /// The upper frontier of the partial remap trace in terms of IntoTime
    into_upper: Antichain<Timestamp>,

    /// The since frontier of partial remap trace in terms of FromTime
    from_since: HashMap<PartitionId, MzOffset>,
    /// The upper frontier of partial remap trace in terms of FromTime
    from_upper: HashMap<PartitionId, MzOffset>,

    write_handle: WriteHandle<(), PartitionId, Timestamp, Diff>,
    read_handle: ReadHandle<(), PartitionId, Timestamp, Diff>,
    now: NowFn,
    update_interval: u64,
}

impl ReclockOperator {
    pub async fn new(
        CollectionMetadata {
            persist_location,
            timestamp_shard_id,
        }: CollectionMetadata,
        now: NowFn,
        update_interval: Duration,
        as_of: Antichain<Timestamp>,
    ) -> anyhow::Result<Self> {
        let persist_client = persist_location
            .open()
            .await
            .with_context(|| "error creating persist client")?;

        let (write_handle, read_handle) = persist_client.open(timestamp_shard_id).await.unwrap();

        assert!(
            PartialOrder::less_equal(read_handle.since(), &as_of),
            "Source AS OF must not be less than since: {:?} vs {:?}",
            as_of,
            read_handle.since(),
        );

        let mut operator = Self {
            remap_trace: HashMap::new(),
            from_since: HashMap::new(),
            from_upper: HashMap::new(),
            into_since: read_handle.since().clone(),
            into_upper: Antichain::from_elem(Timestamp::minimum()),
            write_handle,
            read_handle,
            now,
            update_interval: update_interval.as_millis().try_into().unwrap(),
        };

        // Load the initial state
        let global_upper = operator.write_handle.upper().clone();
        operator.sync_through(global_upper.borrow()).await;

        Ok(operator)
    }

    async fn get_time(&mut self) -> Timestamp {
        loop {
            let mut new_ts = (self.now)();
            new_ts -= new_ts % self.update_interval;
            if self.into_upper.less_equal(&new_ts) {
                break new_ts;
            }
            tokio::time::sleep(Duration::from_millis(self.update_interval)).await;
        }
    }

    /// Syncs the local state of this operator to match the state of the persist shard
    async fn sync_through(&mut self, target: AntichainRef<'_, Timestamp>) {
        let listener = self
            .read_handle
            .listen(self.into_upper.clone())
            .await
            .unwrap()
            .into_stream();
        tokio::pin!(listener);
        while PartialOrder::less_than(&self.into_upper.borrow(), &target) {
            match listener.next().await {
                Some(ListenEvent::Progress(progress)) => {
                    self.into_upper = progress;
                }
                Some(ListenEvent::Updates(updates)) => {
                    for ((_, pid), ts, diff) in updates {
                        let pid = pid.expect("Unable to decode partition id");
                        self.remap_trace
                            .entry(pid.clone())
                            .or_default()
                            .push_back((ts, diff));
                    }
                }
                None => unreachable!("ListenStream doesn't end"),
            }
        }
        self.read_handle
            .downgrade_since(self.into_upper.clone())
            .await;
    }

    /// Ensures that the persist shard backing this reclock operator contains bindings that cover
    /// the provided gauge frontier.
    ///
    /// When this function returns the local dTVC view of the remap collection will contain
    /// definite timestamp bindings that can be used to timestamp data.
    async fn mint_through(
        &mut self,
        target: &HashMap<PartitionId, MzOffset>,
    ) -> Result<(), Upper<Timestamp>> {
        let new_ts = self.get_time().await;

        let mut updates = vec![];
        for (pid, upper) in target {
            let prev = self.from_upper.get(pid).cloned().unwrap_or_default();
            if prev < *upper {
                updates.push((((), pid), new_ts, *upper - prev));
            }
        }

        if !updates.is_empty() {
            let new_into_upper = Antichain::from_elem(new_ts + 1);
            self.write_handle
                .compare_and_append(&updates, self.into_upper.clone(), new_into_upper.clone())
                .await
                .unwrap()
                .unwrap()?;
            self.into_upper = new_into_upper;

            for ((_, pid), ts, diff) in updates {
                self.remap_trace
                    .entry(pid.clone())
                    .or_default()
                    .push_back((ts, diff));
                *self.from_upper.entry(pid.clone()).or_default() += diff;
            }
        }
        Ok(())
    }

    /// Calling this method gives permission to the relock operator to compact its locally cached
    /// bindings.
    ///
    /// If there is a change in the minimum timestamp it returns it to the caller who can use it to downgrade its capability.
    pub fn compact(
        &mut self,
        target: &HashMap<PartitionId, MzOffset>,
    ) -> Option<Antichain<Timestamp>> {
        // First check frontier invariants
        for (pid, new_since) in target {
            let since = self.from_since.get(pid).cloned().unwrap_or_default();
            assert!(since <= *new_since, "attempt to move since backwards");
            let upper = self.from_upper.get(pid).cloned().unwrap_or_default();
            assert!(*new_since <= upper, "attempt to compact past upper");
        }

        self.from_since = target.clone();

        // Translate the FromTime compaction frontier into an IntoTime compaction frontier
        // The upper is the maximum frontier we can ever compact to
        let mut new_into_since = self.into_upper.clone();
        for (pid, target_offset) in target {
            let bindings = self.remap_trace.get(pid).expect("checked at the beginning");

            let mut partition_since = None;
            let mut offset = MzOffset::default();
            for (ts, diff) in bindings {
                offset += *diff;
                if offset > *target_offset {
                    break;
                }
                partition_since = Some(*ts);
            }
            new_into_since.insert(partition_since.expect("offset must exist"));
        }

        if !PartialOrder::less_than(&self.into_since, &new_into_since) {
            return None;
        }

        // Now compact and consolidate the remap trace according to the computed frontier
        for (pid, bindings) in self.remap_trace.iter_mut() {
            for binding in bindings.iter_mut() {
                if new_into_since.less_than(&binding.0) {
                    break;
                }
                binding.0.advance_by(new_into_since.borrow());
            }
            let (head, tail) = bindings.as_mut_slices();
            consolidation::consolidate_slice(head);
            consolidation::consolidate_slice(tail);
            // XXX: suspicious logic
            self.from_since.insert(
                pid.clone(),
                MzOffset {
                    offset: bindings.front().unwrap().1,
                },
            );
        }
        self.into_since = new_into_since.clone();
        Some(new_into_since)
    }

    pub async fn reclock_messages<'a, M>(
        &'a mut self,
        messages: &'a mut HashMap<PartitionId, Vec<(MzOffset, M)>>,
    ) -> ReclockIter<'a, M> {
        // Assert that we're not asked to reclock data that is not beyond our compaction frontier
        for (pid, messages) in messages.iter() {
            for (offset, _) in messages {
                let since = self.from_since.get(pid).cloned().unwrap_or_default();
                assert!(
                    since <= *offset,
                    "offset not beyond compaction frontier: {offset}"
                );
            }
        }

        // Calculate the upper frontier of this batch of messages
        let mut messages_upper = HashMap::new();
        for (pid, messages) in messages.iter() {
            if let Some((offset, _)) = messages.last() {
                messages_upper.insert(pid.clone(), *offset);
            }
        }

        while let Err(Upper(actual_upper)) = self.mint_through(&messages_upper).await {
            self.sync_through(actual_upper.borrow()).await;
        }

        // At this point our local view of definite bindings has enough data to reclock all
        // messages requested.
        ReclockIter {
            remap_trace: &self.remap_trace,
            messages: messages.iter_mut(),
        }
    }
}

pub struct ReclockIter<'a, M> {
    remap_trace: &'a HashMap<PartitionId, VecDeque<(Timestamp, Diff)>>,
    messages: hash_map::IterMut<'a, PartitionId, Vec<(MzOffset, M)>>,
}

impl<'a, M> Iterator for ReclockIter<'a, M> {
    type Item = ReclockPartIter<'a, M>;

    fn next(&mut self) -> Option<Self::Item> {
        let (partition, messages) = self.messages.next()?;
        Some(ReclockPartIter {
            updates: self.remap_trace.get(partition).unwrap().iter(),
            state: Default::default(),
            messages: messages.drain(..),
        })
    }
}

pub struct ReclockPartIter<'a, M> {
    updates: vec_deque::Iter<'a, (Timestamp, i64)>,
    state: (Timestamp, MzOffset),
    messages: vec::Drain<'a, (MzOffset, M)>,
}

impl<'a, M> Iterator for ReclockPartIter<'a, M> {
    type Item = (Timestamp, M);

    fn next(&mut self) -> Option<Self::Item> {
        let (offset, message) = self.messages.next()?;
        // Integrate bindings until find the one that covers this message's offset
        while self.state.1 < offset {
            let (new_ts, diff) = self.updates.next().expect("mint_through guarantees this");
            self.state.0 = *new_ts;
            self.state.1 += *diff;
        }

        Some((self.state.0, message))
    }
}
