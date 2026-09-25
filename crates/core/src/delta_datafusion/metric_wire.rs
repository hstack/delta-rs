//! Serializable representation of the DataFusion metrics attached to Delta scan plans.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, Label, Metric, MetricCategory, MetricType, MetricValue,
    MetricsSet, PruningMetrics, RatioMergeStrategy, RatioMetrics, Time, Timestamp,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct MetricsSetWire(Vec<MetricWire>);

impl TryFrom<&MetricsSet> for MetricsSetWire {
    type Error = DataFusionError;

    fn try_from(metrics: &MetricsSet) -> Result<Self, Self::Error> {
        metrics
            .iter()
            .map(|metric| MetricWire::try_from(metric.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }
}

impl TryFrom<MetricsSetWire> for ExecutionPlanMetricsSet {
    type Error = DataFusionError;

    fn try_from(wire: MetricsSetWire) -> Result<Self, Self::Error> {
        let metrics = wire
            .0
            .into_iter()
            .map(MetricWire::try_into_metric)
            .collect::<Result<MetricsSet, _>>()?;
        Ok(metrics.into())
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MetricWire {
    value: MetricValueWire,
    partition: Option<u64>,
    labels: Vec<LabelWire>,
    metric_type: MetricTypeWire,
    category: Option<MetricCategoryWire>,
}

impl TryFrom<&Metric> for MetricWire {
    type Error = DataFusionError;

    fn try_from(metric: &Metric) -> Result<Self, Self::Error> {
        Ok(Self {
            value: MetricValueWire::try_from(metric.value())?,
            partition: metric.partition().map(usize_to_wire).transpose()?,
            labels: metric.labels().iter().map(LabelWire::from).collect(),
            metric_type: metric.metric_type().into(),
            category: metric.metric_category().map(Into::into),
        })
    }
}

impl MetricWire {
    fn try_into_metric(self) -> Result<Arc<Metric>, DataFusionError> {
        let partition = self.partition.map(wire_to_usize).transpose()?;
        let labels = self.labels.into_iter().map(Into::into).collect();
        let mut metric = Metric::new_with_labels(self.value.try_into()?, partition, labels)
            .with_type(self.metric_type.into());
        if let Some(category) = self.category {
            metric = metric.with_category(category.into());
        }
        Ok(Arc::new(metric))
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LabelWire {
    name: String,
    value: String,
}

impl From<&Label> for LabelWire {
    fn from(label: &Label) -> Self {
        Self {
            name: label.name().to_owned(),
            value: label.value().to_owned(),
        }
    }
}

impl From<LabelWire> for Label {
    fn from(label: LabelWire) -> Self {
        Self::new(label.name, label.value)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MetricTypeWire {
    Summary,
    Dev,
}

impl From<MetricType> for MetricTypeWire {
    fn from(metric_type: MetricType) -> Self {
        match metric_type {
            MetricType::Summary => Self::Summary,
            MetricType::Dev => Self::Dev,
        }
    }
}

impl From<MetricTypeWire> for MetricType {
    fn from(metric_type: MetricTypeWire) -> Self {
        match metric_type {
            MetricTypeWire::Summary => Self::Summary,
            MetricTypeWire::Dev => Self::Dev,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MetricCategoryWire {
    Rows,
    Bytes,
    Timing,
    Uncategorized,
}

impl From<MetricCategory> for MetricCategoryWire {
    fn from(category: MetricCategory) -> Self {
        match category {
            MetricCategory::Rows => Self::Rows,
            MetricCategory::Bytes => Self::Bytes,
            MetricCategory::Timing => Self::Timing,
            MetricCategory::Uncategorized => Self::Uncategorized,
        }
    }
}

impl From<MetricCategoryWire> for MetricCategory {
    fn from(category: MetricCategoryWire) -> Self {
        match category {
            MetricCategoryWire::Rows => Self::Rows,
            MetricCategoryWire::Bytes => Self::Bytes,
            MetricCategoryWire::Timing => Self::Timing,
            MetricCategoryWire::Uncategorized => Self::Uncategorized,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RatioMergeStrategyWire {
    Add,
    SetTotal,
    SetPart,
}

impl From<&RatioMergeStrategy> for RatioMergeStrategyWire {
    fn from(strategy: &RatioMergeStrategy) -> Self {
        match strategy {
            RatioMergeStrategy::AddPartAddTotal => Self::Add,
            RatioMergeStrategy::AddPartSetTotal => Self::SetTotal,
            RatioMergeStrategy::SetPartAddTotal => Self::SetPart,
        }
    }
}

impl From<RatioMergeStrategyWire> for RatioMergeStrategy {
    fn from(strategy: RatioMergeStrategyWire) -> Self {
        match strategy {
            RatioMergeStrategyWire::Add => Self::AddPartAddTotal,
            RatioMergeStrategyWire::SetTotal => Self::AddPartSetTotal,
            RatioMergeStrategyWire::SetPart => Self::SetPartAddTotal,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MetricValueWire {
    OutputRows {
        value: u64,
    },
    ElapsedCompute {
        nanos: u64,
    },
    SpillCount {
        value: u64,
    },
    SpilledBytes {
        value: u64,
    },
    OutputBytes {
        value: u64,
    },
    OutputBatches {
        value: u64,
    },
    SpilledRows {
        value: u64,
    },
    CurrentMemoryUsage {
        value: u64,
    },
    Count {
        name: String,
        value: u64,
    },
    Gauge {
        name: String,
        value: u64,
    },
    Time {
        name: String,
        nanos: u64,
    },
    StartTimestamp {
        nanos: Option<i64>,
    },
    EndTimestamp {
        nanos: Option<i64>,
    },
    PruningMetrics {
        name: String,
        pruned: u64,
        matched: u64,
        fully_matched: u64,
    },
    Ratio {
        name: String,
        part: u64,
        total: u64,
        merge_strategy: RatioMergeStrategyWire,
        display_raw_values: bool,
    },
}

impl TryFrom<&MetricValue> for MetricValueWire {
    type Error = DataFusionError;

    fn try_from(value: &MetricValue) -> Result<Self, Self::Error> {
        match value {
            MetricValue::OutputRows(count) => Ok(Self::OutputRows {
                value: usize_to_wire(count.value())?,
            }),
            MetricValue::ElapsedCompute(time) => Ok(Self::ElapsedCompute {
                nanos: usize_to_wire(time.value())?,
            }),
            MetricValue::SpillCount(count) => Ok(Self::SpillCount {
                value: usize_to_wire(count.value())?,
            }),
            MetricValue::SpilledBytes(count) => Ok(Self::SpilledBytes {
                value: usize_to_wire(count.value())?,
            }),
            MetricValue::OutputBytes(count) => Ok(Self::OutputBytes {
                value: usize_to_wire(count.value())?,
            }),
            MetricValue::OutputBatches(count) => Ok(Self::OutputBatches {
                value: usize_to_wire(count.value())?,
            }),
            MetricValue::SpilledRows(count) => Ok(Self::SpilledRows {
                value: usize_to_wire(count.value())?,
            }),
            MetricValue::CurrentMemoryUsage(gauge) => Ok(Self::CurrentMemoryUsage {
                value: usize_to_wire(gauge.value())?,
            }),
            MetricValue::Count { name, count } => Ok(Self::Count {
                name: name.to_string(),
                value: usize_to_wire(count.value())?,
            }),
            MetricValue::Gauge { name, gauge } => Ok(Self::Gauge {
                name: name.to_string(),
                value: usize_to_wire(gauge.value())?,
            }),
            MetricValue::Time { name, time } => Ok(Self::Time {
                name: name.to_string(),
                nanos: usize_to_wire(time.value())?,
            }),
            MetricValue::StartTimestamp(timestamp) => Ok(Self::StartTimestamp {
                nanos: timestamp_to_wire(timestamp)?,
            }),
            MetricValue::EndTimestamp(timestamp) => Ok(Self::EndTimestamp {
                nanos: timestamp_to_wire(timestamp)?,
            }),
            MetricValue::PruningMetrics {
                name,
                pruning_metrics,
            } => Ok(Self::PruningMetrics {
                name: name.to_string(),
                pruned: usize_to_wire(pruning_metrics.pruned())?,
                matched: usize_to_wire(pruning_metrics.matched())?,
                fully_matched: usize_to_wire(pruning_metrics.fully_matched())?,
            }),
            MetricValue::Ratio {
                name,
                ratio_metrics,
            } => Ok(Self::Ratio {
                name: name.to_string(),
                part: usize_to_wire(ratio_metrics.part())?,
                total: usize_to_wire(ratio_metrics.total())?,
                merge_strategy: ratio_metrics.merge_strategy().into(),
                display_raw_values: ratio_metrics.display_raw_values(),
            }),
            MetricValue::Custom { name, .. } => Err(DataFusionError::NotImplemented(format!(
                "Cannot serialize custom metric '{name}' without its concrete type"
            ))),
        }
    }
}

impl TryFrom<MetricValueWire> for MetricValue {
    type Error = DataFusionError;

    fn try_from(value: MetricValueWire) -> Result<Self, Self::Error> {
        match value {
            MetricValueWire::OutputRows { value } => Ok(Self::OutputRows(count_from_wire(value)?)),
            MetricValueWire::ElapsedCompute { nanos } => {
                Ok(Self::ElapsedCompute(time_from_wire(nanos)?))
            }
            MetricValueWire::SpillCount { value } => Ok(Self::SpillCount(count_from_wire(value)?)),
            MetricValueWire::SpilledBytes { value } => {
                Ok(Self::SpilledBytes(count_from_wire(value)?))
            }
            MetricValueWire::OutputBytes { value } => {
                Ok(Self::OutputBytes(count_from_wire(value)?))
            }
            MetricValueWire::OutputBatches { value } => {
                Ok(Self::OutputBatches(count_from_wire(value)?))
            }
            MetricValueWire::SpilledRows { value } => {
                Ok(Self::SpilledRows(count_from_wire(value)?))
            }
            MetricValueWire::CurrentMemoryUsage { value } => {
                Ok(Self::CurrentMemoryUsage(gauge_from_wire(value)?))
            }
            MetricValueWire::Count { name, value } => Ok(Self::Count {
                name: Cow::Owned(name),
                count: count_from_wire(value)?,
            }),
            MetricValueWire::Gauge { name, value } => Ok(Self::Gauge {
                name: Cow::Owned(name),
                gauge: gauge_from_wire(value)?,
            }),
            MetricValueWire::Time { name, nanos } => Ok(Self::Time {
                name: Cow::Owned(name),
                time: time_from_wire(nanos)?,
            }),
            MetricValueWire::StartTimestamp { nanos } => {
                Ok(Self::StartTimestamp(timestamp_from_wire(nanos)))
            }
            MetricValueWire::EndTimestamp { nanos } => {
                Ok(Self::EndTimestamp(timestamp_from_wire(nanos)))
            }
            MetricValueWire::PruningMetrics {
                name,
                pruned,
                matched,
                fully_matched,
            } => {
                let metrics = PruningMetrics::new();
                metrics.add_pruned(wire_to_usize(pruned)?);
                metrics.add_matched(wire_to_usize(matched)?);
                metrics.add_fully_matched(wire_to_usize(fully_matched)?);
                Ok(Self::PruningMetrics {
                    name: Cow::Owned(name),
                    pruning_metrics: metrics,
                })
            }
            MetricValueWire::Ratio {
                name,
                part,
                total,
                merge_strategy,
                display_raw_values,
            } => {
                let metrics = RatioMetrics::new()
                    .with_merge_strategy(merge_strategy.into())
                    .with_display_raw_values(display_raw_values);
                metrics.set_part(wire_to_usize(part)?);
                metrics.set_total(wire_to_usize(total)?);
                Ok(Self::Ratio {
                    name: Cow::Owned(name),
                    ratio_metrics: metrics,
                })
            }
        }
    }
}

fn usize_to_wire(value: usize) -> Result<u64, DataFusionError> {
    u64::try_from(value).map_err(|_| {
        DataFusionError::Internal(format!(
            "Metric value {value} does not fit in the wire format"
        ))
    })
}

fn wire_to_usize(value: u64) -> Result<usize, DataFusionError> {
    usize::try_from(value).map_err(|_| {
        DataFusionError::Internal(format!("Metric value {value} does not fit in usize"))
    })
}

fn count_from_wire(value: u64) -> Result<Count, DataFusionError> {
    let count = Count::new();
    count.add(wire_to_usize(value)?);
    Ok(count)
}

fn gauge_from_wire(value: u64) -> Result<Gauge, DataFusionError> {
    let gauge = Gauge::new();
    gauge.set(wire_to_usize(value)?);
    Ok(gauge)
}

fn time_from_wire(nanos: u64) -> Result<Time, DataFusionError> {
    wire_to_usize(nanos)?;
    let time = Time::new();
    if nanos > 0 {
        time.add_duration(Duration::from_nanos(nanos));
    }
    Ok(time)
}

fn timestamp_to_wire(timestamp: &Timestamp) -> Result<Option<i64>, DataFusionError> {
    timestamp
        .value()
        .map(|value| {
            value.timestamp_nanos_opt().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Metric timestamp {value} cannot be represented in nanoseconds"
                ))
            })
        })
        .transpose()
}

fn timestamp_from_wire(nanos: Option<i64>) -> Timestamp {
    let timestamp = Timestamp::new();
    if let Some(nanos) = nanos {
        timestamp.set(DateTime::from_timestamp_nanos(nanos));
    }
    timestamp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(value: usize) -> Count {
        let count = Count::new();
        count.add(value);
        count
    }

    fn gauge(value: usize) -> Gauge {
        let gauge = Gauge::new();
        gauge.set(value);
        gauge
    }

    fn time(nanos: u64) -> Time {
        let time = Time::new();
        time.add_duration(Duration::from_nanos(nanos));
        time
    }

    #[test]
    fn metrics_set_wire_preserves_all_builtin_metric_values() {
        let start = Timestamp::new();
        start.set(DateTime::from_timestamp_nanos(123));
        let end = Timestamp::new();
        end.set(DateTime::from_timestamp_nanos(456));

        let pruning = PruningMetrics::new();
        pruning.add_pruned(2);
        pruning.add_matched(3);
        pruning.add_fully_matched(1);

        let ratio = RatioMetrics::new()
            .with_merge_strategy(RatioMergeStrategy::SetPartAddTotal)
            .with_display_raw_values(false);
        ratio.set_part(4);
        ratio.set_total(5);

        let values = vec![
            MetricValue::OutputRows(count(1)),
            MetricValue::ElapsedCompute(Time::new()),
            MetricValue::SpillCount(count(3)),
            MetricValue::SpilledBytes(count(4)),
            MetricValue::OutputBytes(count(5)),
            MetricValue::OutputBatches(count(6)),
            MetricValue::SpilledRows(count(7)),
            MetricValue::CurrentMemoryUsage(gauge(8)),
            MetricValue::Count {
                name: Cow::Borrowed("count"),
                count: count(9),
            },
            MetricValue::Gauge {
                name: Cow::Borrowed("gauge"),
                gauge: gauge(10),
            },
            MetricValue::Time {
                name: Cow::Borrowed("time"),
                time: time(11),
            },
            MetricValue::StartTimestamp(start),
            MetricValue::EndTimestamp(end),
            MetricValue::PruningMetrics {
                name: Cow::Borrowed("pruning"),
                pruning_metrics: pruning,
            },
            MetricValue::Ratio {
                name: Cow::Borrowed("ratio"),
                ratio_metrics: ratio,
            },
        ];

        let metrics = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let category = match index % 4 {
                    0 => MetricCategory::Rows,
                    1 => MetricCategory::Bytes,
                    2 => MetricCategory::Timing,
                    _ => MetricCategory::Uncategorized,
                };
                Arc::new(
                    Metric::new_with_labels(
                        value,
                        Some(index),
                        vec![Label::new("index", index.to_string())],
                    )
                    .with_type(if index % 2 == 0 {
                        MetricType::Summary
                    } else {
                        MetricType::Dev
                    })
                    .with_category(category),
                )
            })
            .collect::<MetricsSet>();

        let wire = MetricsSetWire::try_from(&metrics).unwrap();
        let encoded = serde_json::to_vec(&wire).unwrap();
        let decoded: MetricsSetWire = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, wire);

        let decoded_metrics = ExecutionPlanMetricsSet::try_from(decoded).unwrap();
        let roundtrip = MetricsSetWire::try_from(&decoded_metrics.clone_inner()).unwrap();
        assert_eq!(roundtrip, wire);
    }
}
