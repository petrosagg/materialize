// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for the storage controller components

use std::sync::Arc;

use mz_cluster_client::ReplicaId;
use mz_ore::cast::{CastFrom, TryCastFrom};
use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropGauge, DeleteOnDropHistogram, GaugeVecExt, HistogramVecExt, MetricsRegistry,
    UIntGaugeVec,
};
use mz_ore::stats::HISTOGRAM_BYTE_BUCKETS;
use mz_repr::GlobalId;
use mz_service::codec::StatsCollector;
use mz_storage_types::instances::StorageInstanceId;
use prometheus::core::AtomicU64;

use crate::client::{ProtoStorageCommand, ProtoStorageResponse};

/// Storage controller metrics
#[derive(Debug, Clone)]
pub struct StorageControllerMetricDefs {
    messages_sent_bytes: prometheus::HistogramVec,
    messages_received_bytes: prometheus::HistogramVec,
    objects: UIntGaugeVec,
    instances: UIntGaugeVec,
    instance_replicas: UIntGaugeVec,
    instance_objects: UIntGaugeVec,
    replica_objects: UIntGaugeVec,
}

impl StorageControllerMetricDefs {
    pub fn new(metrics_registry: MetricsRegistry) -> Self {
        Self {
            messages_sent_bytes: metrics_registry.register(metric!(
                name: "mz_storage_messages_sent_bytes",
                help: "size of storage messages sent",
                var_labels: ["instance"],
                buckets: HISTOGRAM_BYTE_BUCKETS.to_vec()
            )),
            messages_received_bytes: metrics_registry.register(metric!(
                name: "mz_storage_messages_received_bytes",
                help: "size of storage messages received",
                var_labels: ["instance"],
                buckets: HISTOGRAM_BYTE_BUCKETS.to_vec()
            )),
            objects: metrics_registry.register(metric!(
                name: "mz_storage_objects",
                help: "the lifetime of a storage object",
                var_labels: ["object_id"],
            )),
            instances: metrics_registry.register(metric!(
                name: "mz_storage_instances",
                help: "the lifetime of a storage instance",
                var_labels: ["instance_id"],
            )),
            instance_replicas: metrics_registry.register(metric!(
                name: "mz_storage_instance_replicas",
                help: "the lifetime of a storage instance replica",
                var_labels: ["instance_id", "replica_id"],
            )),
            instance_objects: metrics_registry.register(metric!(
                name: "mz_storage_instance_objects",
                help: "the lifetime of a storage instance object",
                var_labels: ["instance_id", "object_id"],
            )),
            replica_objects: metrics_registry.register(metric!(
                name: "mz_storage_replica_objects",
                help: "the lifetime of an instanciation of an object on a replica",
                var_labels: ["instance_id", "replica_id", "object_id"],
            )),
        }
    }
    pub fn for_object(
        &mut self,
        object_id: GlobalId,
    ) -> DeleteOnDropGauge<'static, AtomicU64, Vec<String>> {
        let gauge = self
            .objects
            .get_delete_on_drop_gauge(vec![object_id.to_string()]);
        gauge.set(1);
        gauge
    }
    pub fn for_instance(
        &mut self,
        instance_id: StorageInstanceId,
    ) -> DeleteOnDropGauge<'static, AtomicU64, Vec<String>> {
        let gauge = self
            .instances
            .get_delete_on_drop_gauge(vec![instance_id.to_string()]);
        gauge.set(1);
        gauge
    }
    pub fn for_instance_client(&self, id: StorageInstanceId) -> RehydratingStorageClientMetrics {
        let labels = vec![id.to_string()];
        RehydratingStorageClientMetrics {
            inner: Arc::new(RehydratingStorageClientMetricsInner {
                messages_sent_bytes: self
                    .messages_sent_bytes
                    .get_delete_on_drop_histogram(labels.clone()),
                messages_received_bytes: self
                    .messages_received_bytes
                    .get_delete_on_drop_histogram(labels),
            }),
        }
    }

    pub fn for_replica(
        &mut self,
        instance_id: StorageInstanceId,
        replica_id: ReplicaId,
    ) -> DeleteOnDropGauge<'static, AtomicU64, Vec<String>> {
        let labels = vec![instance_id.to_string(), replica_id.to_string()];
        let gauge = self.instance_replicas.get_delete_on_drop_gauge(labels);
        gauge.set(1);
        gauge
    }
    pub fn for_object_in_cluster(
        &mut self,
        instance_id: StorageInstanceId,
        source_id: GlobalId,
    ) -> DeleteOnDropGauge<'static, AtomicU64, Vec<String>> {
        let labels = vec![instance_id.to_string(), source_id.to_string()];
        let gauge = self.instance_objects.get_delete_on_drop_gauge(labels);
        gauge.set(1);
        gauge
    }
    pub fn for_object_in_replica(
        &mut self,
        instance_id: StorageInstanceId,
        replica_id: ReplicaId,
        source_id: GlobalId,
    ) -> DeleteOnDropGauge<'static, AtomicU64, Vec<String>> {
        let labels = vec![
            instance_id.to_string(),
            replica_id.to_string(),
            source_id.to_string(),
        ];
        let gauge = self.replica_objects.get_delete_on_drop_gauge(labels);
        gauge.set(1);
        gauge
    }
}

#[derive(Debug)]
struct RehydratingStorageClientMetricsInner {
    messages_sent_bytes: DeleteOnDropHistogram<'static, Vec<String>>,
    messages_received_bytes: DeleteOnDropHistogram<'static, Vec<String>>,
}

/// Per-instance metrics
#[derive(Debug, Clone)]
pub struct RehydratingStorageClientMetrics {
    inner: Arc<RehydratingStorageClientMetricsInner>,
}

/// Make ReplicaConnectionMetric pluggable into the gRPC connection.
impl StatsCollector<ProtoStorageCommand, ProtoStorageResponse> for RehydratingStorageClientMetrics {
    fn send_event(&self, _item: &ProtoStorageCommand, size: usize) {
        match f64::try_cast_from(u64::cast_from(size)) {
            Some(x) => self.inner.messages_sent_bytes.observe(x),
            None => tracing::warn!(
                "{} has no precise representation as f64, ignoring message",
                size
            ),
        }
    }

    fn receive_event(&self, _item: &ProtoStorageResponse, size: usize) {
        match f64::try_cast_from(u64::cast_from(size)) {
            Some(x) => self.inner.messages_received_bytes.observe(x),
            None => tracing::warn!(
                "{} has no precise representation as f64, ignoring message",
                size
            ),
        }
    }
}
