//! Codec for planning-time count metrics attached to Delta scan plans.
//!
//! Only [`MetricValue::Count`] metrics are serialized. Runtime metrics are collected by the
//! execution plan on the worker and are not copied from the coordinator.
// TODO: Move this codec into `table_provider::next::scan::codec` after removing the legacy table
// provider.

use std::borrow::Cow;
use std::sync::Arc;

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Label, Metric, MetricValue, MetricsSet,
};
use serde::{Deserialize, Serialize};

// Consumed by datafusion-distributed's MetricsWrapperExec before task aggregation.
const CREATED_AT_LABEL: &str = "created_at";
const CREATED_AT_PLANNING: &str = "planning";

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct PlanningCountMetricsWire(Vec<PlanningCountMetricWire>);

impl PlanningCountMetricsWire {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
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
                Arc::new(Metric::new_with_labels(
                    MetricValue::Count {
                        name: Cow::Owned(metric.name),
                        count,
                    },
                    None,
                    vec![Label::new(CREATED_AT_LABEL, CREATED_AT_PLANNING)],
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

        let wire = PlanningCountMetricsWire::from(&metrics);
        let encoded = serde_json::to_vec(&wire).unwrap();
        let decoded: PlanningCountMetricsWire = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, wire);

        let decoded = ExecutionPlanMetricsSet::from(decoded).clone_inner();
        assert_eq!(decoded.iter().count(), 2);
        assert_eq!(decoded.sum_by_name("files_scanned").unwrap().as_usize(), 4);
        assert_eq!(decoded.sum_by_name("files_pruned").unwrap().as_usize(), 2);
        assert!(decoded.iter().all(|metric| {
            let labels = metric.labels();
            metric.partition().is_none()
                && labels.len() == 1
                && labels[0].name() == CREATED_AT_LABEL
                && labels[0].value() == CREATED_AT_PLANNING
                && metric.metric_type() == MetricType::Dev
                && metric.metric_category().is_none()
        }));
    }
}
