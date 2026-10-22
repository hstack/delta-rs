//! Codec for planning-time count metrics attached to Delta scan plans.
//!
//! Only [`MetricValue::Count`] metrics are serialized. Runtime metrics are collected by the
//! execution plan wherever it runs and are never copied from the encoded plan.
// TODO: Move this codec into `table_provider::next::scan::codec` after removing the legacy table
// provider.

use std::borrow::Cow;
use std::sync::Arc;

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Metric, MetricValue, MetricsSet,
};
use serde::{Deserialize, Serialize};

/// Serialization options shared by the Delta physical codecs
/// ([`DeltaNextPhysicalCodec`](super::DeltaNextPhysicalCodec) and the legacy
/// `DeltaPhysicalCodec`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaPhysicalCodecConfig {
    /// Write planning-time scan count metrics (for example the number of scanned or pruned
    /// files) with an encoded scan, and restore them when decoding. Enabled by default.
    ///
    /// Disable it for plans shipped to execution-only workers, such as distributed tasks whose
    /// runtime metrics are merged back into the coordinator's plan: the coordinator already owns
    /// the planning metrics, so a copy in every task would be counted once per task. When
    /// disabled, planning metrics are neither written nor restored from a received plan.
    pub serialize_planning_metrics: bool,
}

impl Default for DeltaPhysicalCodecConfig {
    fn default() -> Self {
        Self {
            serialize_planning_metrics: true,
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct PlanningCountMetricsWire(Vec<PlanningCountMetricWire>);

impl PlanningCountMetricsWire {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Wire form of a scan's planning metrics; empty when `config` disables them.
    pub(super) fn encode(metrics: &MetricsSet, config: DeltaPhysicalCodecConfig) -> Self {
        if config.serialize_planning_metrics {
            Self::from(metrics)
        } else {
            Self::default()
        }
    }

    /// Metrics for a decoded scan; empty when `config` disables planning metrics.
    pub(super) fn decode(self, config: DeltaPhysicalCodecConfig) -> ExecutionPlanMetricsSet {
        if config.serialize_planning_metrics {
            self.into()
        } else {
            ExecutionPlanMetricsSet::new()
        }
    }
}

impl From<&MetricsSet> for PlanningCountMetricsWire {
    fn from(metrics: &MetricsSet) -> Self {
        Self(
            metrics
                .iter()
                .filter_map(|metric| match metric.value() {
                    MetricValue::Count { name, count } => Some(PlanningCountMetricWire {
                        name: name.to_string(),
                        value: count.value(),
                    }),
                    _ => None,
                })
                .collect(),
        )
    }
}

impl From<PlanningCountMetricsWire> for ExecutionPlanMetricsSet {
    fn from(wire: PlanningCountMetricsWire) -> Self {
        wire.0
            .into_iter()
            .map(|metric| {
                let count = Count::new();
                count.add(metric.value);
                Arc::new(Metric::new(
                    MetricValue::Count {
                        name: Cow::Owned(metric.name),
                        count,
                    },
                    None,
                ))
            })
            .collect::<MetricsSet>()
            .into()
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PlanningCountMetricWire {
    name: String,
    value: usize,
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::metrics::{Label, MetricType};

    use super::*;

    fn count(value: usize) -> Count {
        let count = Count::new();
        count.add(value);
        count
    }

    #[test]
    fn planning_count_metrics_wire_roundtrips_named_counts_only() {
        let metrics = vec![
            Arc::new(Metric::new_with_labels(
                MetricValue::Count {
                    name: Cow::Borrowed("files_scanned"),
                    count: count(4),
                },
                Some(7),
                vec![Label::new("source", "planning")],
            )),
            Arc::new(Metric::new(
                MetricValue::Count {
                    name: Cow::Borrowed("files_pruned"),
                    count: count(2),
                },
                None,
            )),
            Arc::new(Metric::new(MetricValue::OutputRows(count(80)), None)),
        ]
        .into_iter()
        .collect::<MetricsSet>();
        let config = DeltaPhysicalCodecConfig::default();
        let wire = PlanningCountMetricsWire::encode(&metrics, config);
        let encoded = serde_json::to_vec(&wire).unwrap();
        let decoded: PlanningCountMetricsWire = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, wire);

        let decoded = decoded.decode(config).clone_inner();
        assert_eq!(decoded.iter().count(), 2);
        assert_eq!(decoded.sum_by_name("files_scanned").unwrap().as_usize(), 4);
        assert_eq!(decoded.sum_by_name("files_pruned").unwrap().as_usize(), 2);
        assert!(decoded.iter().all(|metric| {
            metric.partition().is_none()
                && metric.labels().is_empty()
                && metric.metric_type() == MetricType::Dev
                && metric.metric_category().is_none()
        }));
    }
}
