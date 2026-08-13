use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::CastOptions;
use arrow_schema::{DataType as ArrowDataType, Field, Fields, Schema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::Scalar;
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::{DataType, SchemaRef as KernelSchemaRef, StructField, StructType};
use delta_kernel::{EvaluationHandler, Expression, ExpressionEvaluator};

use crate::kernel::arrow::engine_ext::ExpressionEvaluatorExt;
use crate::kernel::schema::cast::cast_struct;
use crate::kernel::snapshot::stats_projection::{FIELD_PARTITION_VALUES_PARSED, FIELD_STATS};
use crate::kernel::{ARROW_HANDLER, FIELD_STATS_PARSED};
use crate::{DeltaResult, DeltaTableError};

struct CachedStructProjection {
    path: String,
    input_fields: Fields,
    output_fields: Fields,
}

impl CachedStructProjection {
    fn try_new(input_fields: Fields, output_fields: Fields, path: &str) -> DeltaResult<Self> {
        Self::validate_fields(&input_fields, &output_fields, path)?;
        Ok(Self {
            path: path.to_string(),
            input_fields,
            output_fields,
        })
    }

    fn validate_fields(
        input_fields: &Fields,
        output_fields: &Fields,
        path: &str,
    ) -> DeltaResult<()> {
        for output_field in output_fields {
            let field_path = format!("{path}.{}", output_field.name());
            let Some(input_field) = input_fields
                .iter()
                .find(|field| field.name() == output_field.name())
            else {
                if output_field.is_nullable() {
                    continue;
                }
                return Err(DeltaTableError::SchemaMismatch {
                    msg: format!("cached {field_path} is not available"),
                });
            };

            match (input_field.data_type(), output_field.data_type()) {
                (ArrowDataType::Struct(input_child), ArrowDataType::Struct(output_child)) => {
                    Self::validate_fields(input_child, output_child, &field_path)?;
                }
                (input_type, output_type)
                    if input_type == output_type
                        || Self::is_legal_primitive_widening(input_type, output_type) => {}
                (input_type, output_type) => {
                    return Err(DeltaTableError::SchemaMismatch {
                        msg: format!(
                            "cached {field_path} has type {input_type:?} but requested {output_type:?}"
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    fn is_legal_primitive_widening(
        input_type: &ArrowDataType,
        output_type: &ArrowDataType,
    ) -> bool {
        matches!(
            (input_type, output_type),
            (
                ArrowDataType::Int8,
                ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
            ) | (
                ArrowDataType::Int16,
                ArrowDataType::Int32 | ArrowDataType::Int64
            ) | (ArrowDataType::Int32, ArrowDataType::Int64)
                | (ArrowDataType::Float32, ArrowDataType::Float64)
        )
    }

    /// Reshape a seed `stats_parsed` array to the requested schema with a targeted arrow cast:
    /// reorder / drop / add-nullable / widen / nested, preserving null buffers.
    fn cast_to_requested(&self, array: &StructArray) -> DeltaResult<StructArray> {
        let expected_type = ArrowDataType::Struct(self.input_fields.clone());
        if array.data_type() != &expected_type {
            return Err(DeltaTableError::SchemaMismatch {
                msg: format!(
                    "cached {} type {:?} does not match declared type {:?}",
                    self.path,
                    array.data_type(),
                    expected_type
                ),
            });
        }

        Ok(cast_struct(
            array,
            &self.output_fields,
            &CastOptions {
                safe: true,
                ..Default::default()
            },
            true,
        )?)
    }
}

/// How the requested `stats_parsed` column is produced from a materialized-file seed batch.
enum StatsParsedSource {
    /// The seed's typed `stats_parsed` column is compatible with the request: reshape it with a
    /// targeted arrow cast ([`CachedStructProjection`]). A kernel expression cannot do this
    /// because its evaluator never widens primitives, so this one step stays imperative.
    Reshape(CachedStructProjection),
    /// The seed's typed stats are missing or incompatible: parse the raw `stats` JSON instead.
    /// This path is expressed entirely as a kernel `parse_json` in the assembly expression.
    ParseJson,
}

impl StatsParsedSource {
    /// The assembly sub-expression that yields `stats_parsed` for this source. The reshape path
    /// references the column that [`CachedScanRowEvaluator::reshape_cached_stats`] casts into
    /// place; the JSON path parses raw `stats` directly, fully inside the kernel expression.
    fn stats_expression(&self, requested_schema: &KernelSchemaRef) -> Expression {
        match self {
            StatsParsedSource::Reshape(_) => Expression::column([FIELD_STATS_PARSED]),
            StatsParsedSource::ParseJson => {
                Expression::parse_json(Expression::column([FIELD_STATS]), requested_schema.clone())
            }
        }
    }
}

/// The chosen `stats_parsed` production strategy plus the exact schema it emits.
struct StatsPlan {
    source: StatsParsedSource,
    schema: KernelSchemaRef,
}

/// One fixed transformation for every batch in a materialized-file seed.
///
/// Assembly is a single kernel [`ExpressionEvaluator`]: base scan-row columns pass through by
/// name, a missing raw `stats` column becomes a typed NULL, `stats_parsed` is produced per the
/// [`StatsPlan`] (a by-name column reference on the reshape path, or `parse_json` on the raw-JSON
/// path), and `partitionValues_parsed` passes through by name. Columns resolve by name, so the
/// cached batch may carry extra columns or a different field order.
///
/// The only step the expression cannot perform is the reshape cast (kernel expressions never
/// widen primitives); [`Self::reshape_cached_stats`] applies it as a per-batch pre-pass, and only
/// on the reshape path. `partition_fields`, when set, guards that the seed carries a
/// `partitionValues_parsed` column with exactly the expected fields, surfacing a typed
/// [`DeltaTableError::SchemaMismatch`] rather than a kernel column-not-found error.
pub(crate) struct CachedScanRowEvaluator {
    evaluator: Arc<dyn ExpressionEvaluator>,
    stats_plan: Option<StatsPlan>,
    partition_fields: Option<Fields>,
}

impl CachedScanRowEvaluator {
    pub(crate) fn try_new(
        available_stats_schema: Option<&KernelSchemaRef>,
        raw_stats_available: bool,
        effective_replay_stats_schema: Option<KernelSchemaRef>,
        partition_schema: Option<&KernelSchemaRef>,
    ) -> DeltaResult<Self> {
        let base_schema = scan_row_schema();

        let available_stats_fields = available_stats_schema
            .map(|available| {
                let available_arrow: Schema = available.as_ref().try_into_arrow()?;
                Ok::<_, DeltaTableError>(available_arrow.fields().clone())
            })
            .transpose()?;
        let partition_fields = partition_schema
            .map(|schema| {
                let partition_arrow: Schema = schema.as_ref().try_into_arrow()?;
                Ok::<_, DeltaTableError>(partition_arrow.fields().clone())
            })
            .transpose()?;

        // Decide once how `stats_parsed` is produced (reshape cached typed stats, or parse raw
        // JSON). The choice is fixed for every batch of this seed.
        let stats_plan = Self::plan_stats(
            available_stats_fields.as_ref(),
            raw_stats_available,
            effective_replay_stats_schema.as_ref(),
        )?;

        // Assemble the scan-row output expression: base columns (or a typed NULL for a missing
        // raw `stats`), then `stats_parsed`, then `partitionValues_parsed`. Column references are
        // resolved by name, so a positional reordering of the seed cannot silently mis-map.
        let mut expr_fields: Vec<Expression> = Vec::with_capacity(base_schema.num_fields() + 2);
        let mut output_fields: Vec<StructField> =
            Vec::with_capacity(base_schema.num_fields() + 2);
        for field in base_schema.fields() {
            if field.name().as_str() == FIELD_STATS && !raw_stats_available {
                expr_fields.push(Expression::literal(Scalar::Null(field.data_type().clone())));
            } else {
                expr_fields.push(Expression::column([field.name().clone()]));
            }
            output_fields.push(field.clone());
        }
        if let Some(plan) = &stats_plan {
            expr_fields.push(plan.source.stats_expression(&plan.schema));
            output_fields.push(StructField::nullable(
                FIELD_STATS_PARSED,
                DataType::Struct(Box::new(plan.schema.as_ref().clone())),
            ));
        }
        if let Some(partition_schema) = partition_schema {
            expr_fields.push(Expression::column([FIELD_PARTITION_VALUES_PARSED]));
            output_fields.push(StructField::not_null(
                FIELD_PARTITION_VALUES_PARSED,
                DataType::Struct(Box::new(partition_schema.as_ref().clone())),
            ));
        }

        let output_type = DataType::Struct(Box::new(StructType::try_new(output_fields)?));
        let expression = Expression::struct_from(expr_fields);
        // The evaluator ignores its declared input schema at runtime (it resolves columns by name
        // from each batch), so the base scan-row schema is a sufficient placeholder here.
        let evaluator = ARROW_HANDLER.new_expression_evaluator(
            base_schema.clone(),
            Arc::new(expression),
            output_type,
        )?;

        Ok(Self {
            evaluator,
            stats_plan,
            partition_fields,
        })
    }

    /// Chooses the `stats_parsed` strategy: reshape the seed's typed stats when they are present
    /// and compatible (cheapest — no JSON parsing), otherwise parse the raw `stats` JSON, and
    /// error when neither is available. The decision is made here rather than at evaluation time
    /// because a `cast`/parse can silently produce nulls instead of failing, so a runtime failure
    /// cannot be used to trigger the fallback.
    fn plan_stats(
        available_stats_fields: Option<&Fields>,
        raw_stats_available: bool,
        effective_replay_stats_schema: Option<&KernelSchemaRef>,
    ) -> DeltaResult<Option<StatsPlan>> {
        let Some(requested_schema) = effective_replay_stats_schema else {
            return Ok(None);
        };

        // Can the seed's typed `stats_parsed` be reshaped to the request? `Ok(None)` means there
        // were no typed stats at all; `Err` means they exist but are incompatible.
        let reshape = available_stats_fields
            .map(|available_fields| {
                let requested_arrow: Schema = requested_schema.as_ref().try_into_arrow()?;
                CachedStructProjection::try_new(
                    available_fields.clone(),
                    requested_arrow.fields().clone(),
                    FIELD_STATS_PARSED,
                )
            })
            .transpose();

        let source = match reshape {
            Ok(Some(projection)) => StatsParsedSource::Reshape(projection),
            Ok(None) | Err(_) if raw_stats_available => StatsParsedSource::ParseJson,
            Err(err) => return Err(err),
            Ok(None) => {
                return Err(DeltaTableError::SchemaMismatch {
                    msg: "cached seed has neither parsed nor raw statistics required by the scan"
                        .to_string(),
                });
            }
        };

        Ok(Some(StatsPlan {
            source,
            schema: requested_schema.clone(),
        }))
    }

    /// Exact `stats_parsed` schema emitted by this evaluator.
    pub(crate) fn stats_parsed_schema(&self) -> Option<KernelSchemaRef> {
        self.stats_plan.as_ref().map(|plan| plan.schema.clone())
    }

    pub(crate) fn evaluate(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        self.validate_partition_layout(&batch)?;
        let batch = self.reshape_cached_stats(batch)?;
        Ok(self.evaluator.evaluate_arrow(batch)?)
    }

    /// When a partition schema was declared, require the seed to carry a `partitionValues_parsed`
    /// column with exactly the expected fields. This turns a missing / mismatched partition column
    /// into a typed [`DeltaTableError::SchemaMismatch`] instead of the kernel's generic
    /// column-not-found error. Base and stats columns are intentionally not checked here — they are
    /// resolved by name by the expression, so extra or reordered columns are harmless.
    fn validate_partition_layout(&self, batch: &RecordBatch) -> DeltaResult<()> {
        let Some(expected_fields) = &self.partition_fields else {
            return Ok(());
        };
        let column = batch.column_by_name(FIELD_PARTITION_VALUES_PARSED).ok_or_else(|| {
            DeltaTableError::SchemaMismatch {
                msg: format!("cached batch is missing required {FIELD_PARTITION_VALUES_PARSED} column"),
            }
        })?;
        let ArrowDataType::Struct(actual_fields) = column.data_type() else {
            return Err(DeltaTableError::SchemaMismatch {
                msg: format!("cached {FIELD_PARTITION_VALUES_PARSED} column is not a struct"),
            });
        };
        if actual_fields != expected_fields {
            return Err(DeltaTableError::SchemaMismatch {
                msg: format!(
                    "cached {FIELD_PARTITION_VALUES_PARSED} has fields {actual_fields:?} but expected {expected_fields:?}"
                ),
            });
        }
        Ok(())
    }

    /// On the reshape path, cast the seed's `stats_parsed` column to the requested schema
    /// (reorder / drop / add-nullable / widen / nested, preserving null buffers) so the assembly
    /// expression can reference it by name. Every other path (raw-JSON parse, no stats) is handled
    /// entirely inside the assembly expression and passes through untouched here.
    fn reshape_cached_stats(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let Some(StatsPlan {
            source: StatsParsedSource::Reshape(projection),
            ..
        }) = &self.stats_plan
        else {
            return Ok(batch);
        };
        let index = batch.schema().index_of(FIELD_STATS_PARSED)?;
        let parsed = batch
            .column(index)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DeltaTableError::SchemaMismatch {
                msg: "stats_parsed column is not a struct".to_string(),
            })?;
        let reshaped: ArrayRef = Arc::new(projection.cast_to_requested(parsed)?);

        let mut fields = batch.schema().fields().to_vec();
        fields[index] = Arc::new(Field::new(
            FIELD_STATS_PARSED,
            reshaped.data_type().clone(),
            true,
        ));
        let mut columns = batch.columns().to_vec();
        columns[index] = reshaped;
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, new_null_array};
    use arrow::datatypes::DataType as ArrowDataType;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::Field;
    use delta_kernel::expressions::Scalar;
    use delta_kernel::scan::scan_row_schema;
    use delta_kernel::schema::{SchemaRef, StructField, StructType};
    use delta_kernel::table_features::ColumnMappingMode;
    use pretty_assertions::assert_eq;

    use crate::kernel::arrow::engine_ext::SnapshotExt;
    use crate::kernel::snapshot::iterators::scan_row::parse_stats_column_with_schema;
    use crate::kernel::scalars::ScalarExt;
    use crate::kernel::snapshot::Snapshot;
    use crate::kernel::snapshot::iterators::scan_row::parse_stats_column_impl;
    use crate::kernel::snapshot::iterators::scan_row::tests::{append_stats_parsed, num_records_stats_parsed, num_records_stats_schema, raw_stats_string, scan_row_batch_with_stats, stats_parsed_field_names, value_stats_parsed, value_stats_schema};
    use crate::kernel::snapshot::stats_projection::{FileStatsMaterialization, StatsProjection};
    use crate::test_utils::TestTables;

    fn empty_num_records_stats_parsed() -> StructArray {
        StructArray::from(vec![(
            Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Option::<i64>::None])) as ArrayRef,
        )])
    }

    fn scan_row_batch_with_optional_stats(raw_stats: Option<&str>) -> RecordBatch {
        let schema: ArrowSchema = scan_row_schema().as_ref().try_into_arrow().unwrap();
        let mut columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), 1))
            .collect();
        columns[schema.index_of("path").unwrap()] =
            Arc::new(StringArray::from(vec![Some("part-000.parquet")]));
        columns[schema.index_of("size").unwrap()] = Arc::new(Int64Array::from(vec![1]));
        columns[schema.index_of("modificationTime").unwrap()] = Arc::new(Int64Array::from(vec![1]));
        columns[schema.index_of("stats").unwrap()] = Arc::new(StringArray::from(vec![raw_stats]));

        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    fn wide_min_values_stats_parsed(
        num_records: i64,
        min_value: i32,
        min_values_valid: bool,
    ) -> StructArray {
        let min_values = StructArray::new(
            Fields::from(vec![
                Field::new("value", ArrowDataType::Int32, true),
                Field::new("other", ArrowDataType::Int32, true),
            ]),
            vec![
                Arc::new(Int32Array::from(vec![Some(min_value)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(99)])) as ArrayRef,
            ],
            Some(NullBuffer::from_iter([min_values_valid])),
        );

        StructArray::from(vec![
            (
                Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(num_records)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "minValues",
                    min_values.data_type().clone(),
                    true,
                )),
                Arc::new(min_values) as ArrayRef,
            ),
        ])
    }

    fn wide_min_values_stats_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new([
                StructField::nullable("numRecords", DataType::LONG),
                StructField::nullable(
                    "minValues",
                    StructType::try_new([
                        StructField::nullable("value", DataType::INTEGER),
                        StructField::nullable("other", DataType::INTEGER),
                    ])
                    .unwrap(),
                ),
            ])
            .unwrap(),
        )
    }

    fn selected_min_values_stats_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new([
                StructField::nullable("numRecords", DataType::LONG),
                StructField::nullable(
                    "minValues",
                    StructType::try_new([StructField::nullable("value", DataType::INTEGER)])
                        .unwrap(),
                ),
            ])
            .unwrap(),
        )
    }

    pub(crate) fn append_partition_values_parsed(
        batch: &RecordBatch,
        partition_values: StructArray,
    ) -> RecordBatch {
        let mut fields = batch.schema().fields().to_vec();
        let mut columns = batch.columns().to_vec();
        fields.push(Arc::new(Field::new(
            FIELD_PARTITION_VALUES_PARSED,
            partition_values.data_type().clone(),
            false,
        )));
        columns.push(Arc::new(partition_values));
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
    }

    #[test]
    fn parse_stats_column_impl_regenerates_raw_stats_when_raw_policy_preserves() -> DeltaResult<()>
    {
        let batch = scan_row_batch_with_stats("stale raw stats");
        let batch = append_stats_parsed(&batch, value_stats_parsed());
        let projected = parse_stats_column_impl(
            &batch,
            value_stats_schema(),
            None,
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::compatibility(StatsProjection::full()),
        )?;

        let raw_stats = raw_stats_string(projected, 0)
            .expect("full typed stats should generate compatibility JSON");
        let raw_stats: serde_json::Value = serde_json::from_str(&raw_stats)?;
        assert_eq!(raw_stats["numRecords"], serde_json::json!(11));
        assert_eq!(raw_stats["minValues"]["value"], serde_json::json!(1));
        assert_eq!(raw_stats["maxValues"]["value"], serde_json::json!(9));
        assert_eq!(raw_stats["nullCount"]["value"], serde_json::json!(0));

        Ok(())
    }

    #[test]
    fn cached_uniform_evaluator_maps_reordered_base_fields_by_name() -> DeltaResult<()> {
        // Give `size` and `modificationTime` distinct values so a positional (rather than by-name)
        // read would route the wrong value to the wrong output field.
        let batch = scan_row_batch_with_optional_stats(Some(r#"{"numRecords":11}"#));
        let schema = batch.schema();
        let size_index = schema.index_of("size").unwrap();
        let modification_time_index = schema.index_of("modificationTime").unwrap();
        let mut columns = batch.columns().to_vec();
        columns[size_index] = Arc::new(Int64Array::from(vec![10]));
        columns[modification_time_index] = Arc::new(Int64Array::from(vec![20]));

        // Physically swap the two same-typed columns (field + data together). By-name column
        // resolution must still route each value back to its correctly named output field.
        let mut fields = schema.fields().to_vec();
        fields.swap(size_index, modification_time_index);
        columns.swap(size_index, modification_time_index);
        let reordered = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();

        let evaluator = CachedScanRowEvaluator::try_new(None, true, None, None)?;
        let projected = evaluator.evaluate(reordered)?;

        let size = projected
            .column_by_name("size")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .expect("size should be an Int64 column");
        let modification_time = projected
            .column_by_name("modificationTime")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .expect("modificationTime should be an Int64 column");
        assert_eq!(size.value(0), 10);
        assert_eq!(modification_time.value(0), 20);
        Ok(())
    }

    #[test]
    fn cached_uniform_evaluator_requires_exact_declared_partition_layout() -> DeltaResult<()> {
        let partition_schema = Arc::new(StructType::try_new([StructField::nullable(
            "part",
            DataType::STRING,
        )])?);
        let evaluator = CachedScanRowEvaluator::try_new(None, true, None, Some(&partition_schema))?;
        let wrong_partition = StructArray::from(vec![(
            Arc::new(Field::new("wrong", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("value")])) as ArrayRef,
        )]);

        for batch in [
            scan_row_batch_with_optional_stats(Some(r#"{"numRecords":11}"#)),
            append_partition_values_parsed(
                &scan_row_batch_with_optional_stats(Some(r#"{"numRecords":11}"#)),
                wrong_partition,
            ),
        ] {
            let error = evaluator
                .evaluate(batch)
                .expect_err("missing or wrong partition layout must be rejected");
            assert!(matches!(error, DeltaTableError::SchemaMismatch { .. }));
        }

        let partition_values = StructArray::from(vec![(
            Arc::new(Field::new("part", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("value")])) as ArrayRef,
        )]);
        let projected = evaluator.evaluate(append_partition_values_parsed(
            &scan_row_batch_with_optional_stats(Some(r#"{"numRecords":11}"#)),
            partition_values,
        ))?;
        assert!(
            projected
                .schema()
                .field_with_name(FIELD_PARTITION_VALUES_PARSED)
                .is_ok()
        );

        Ok(())
    }

    #[test]
    fn seeded_input_wider_parsed_stats_project_to_selected_nested_columns() -> DeltaResult<()> {
        let available_schema = wide_min_values_stats_schema();
        let requested_schema = selected_min_values_stats_schema();
        let evaluator = CachedScanRowEvaluator::try_new(
            Some(&available_schema),
            true,
            Some(requested_schema.clone()),
            None,
        )?;

        assert_eq!(evaluator.stats_parsed_schema(), Some(requested_schema));
        for (num_records, min_value, min_values_valid) in [(11, 1, true), (12, 2, false)] {
            let raw_stats = format!(
                r#"{{"numRecords":{num_records},"minValues":{{"value":{min_value},"other":99}}}}"#
            );
            let batch = append_stats_parsed(
                &scan_row_batch_with_optional_stats(Some(&raw_stats)),
                wide_min_values_stats_parsed(num_records, min_value, min_values_valid),
            );
            let projected = evaluator.evaluate(batch)?;

            assert_eq!(
                stats_parsed_field_names(&projected),
                vec!["numRecords", "minValues"]
            );
            assert_eq!(
                raw_stats_string(projected.clone(), 0),
                Some(raw_stats.clone())
            );
            let stats = projected
                .column_by_name(FIELD_STATS_PARSED)
                .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                .expect("stats_parsed should be a struct");
            let min_values = stats
                .column_by_name("minValues")
                .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                .expect("minValues should be a struct");
            let ArrowDataType::Struct(fields) = min_values.data_type() else {
                panic!("minValues should have a struct type");
            };
            assert_eq!(
                fields
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["value"]
            );
            assert_eq!(min_values.is_valid(0), min_values_valid);
            if min_values_valid {
                assert_eq!(
                    Scalar::from_array(min_values.column_by_name("value").unwrap().as_ref(), 0),
                    Some(Scalar::Integer(min_value))
                );
            }
        }

        Ok(())
    }

    #[test]
    fn seeded_input_stats_schema_change_between_batches_returns_typed_error() {
        let available_schema = wide_min_values_stats_schema();
        let evaluator = CachedScanRowEvaluator::try_new(
            Some(&available_schema),
            true,
            Some(available_schema.clone()),
            None,
        )
        .unwrap();

        let matching_batch = append_stats_parsed(
            &scan_row_batch_with_optional_stats(Some(
                r#"{"numRecords":11,"minValues":{"value":1,"other":99}}"#,
            )),
            wide_min_values_stats_parsed(11, 1, true),
        );
        evaluator
            .evaluate(matching_batch)
            .expect("matching declared stats_parsed schema should be accepted");

        let changed_batch = append_stats_parsed(
            &scan_row_batch_with_optional_stats(Some(r#"{"numRecords":12}"#)),
            num_records_stats_parsed(12),
        );
        let error = evaluator
            .evaluate(changed_batch)
            .expect_err("changed stats_parsed schema should return an error");

        assert!(matches!(error, DeltaTableError::SchemaMismatch { .. }));
    }

    #[test]
    fn cached_uniform_evaluator_falls_back_to_exact_raw_json() -> DeltaResult<()> {
        let raw_stats = r#"{ "minValues": {"value": 1}, "numRecords": 11 }"#;
        let available_schema = Arc::new(StructType::try_new([StructField::nullable(
            "minValues",
            DataType::STRING,
        )])?);
        let requested_schema = selected_min_values_stats_schema();
        let incompatible_parsed = StructArray::from(vec![(
            Arc::new(Field::new("minValues", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("incompatible")])) as ArrayRef,
        )]);
        let batch = append_stats_parsed(
            &scan_row_batch_with_optional_stats(Some(raw_stats)),
            incompatible_parsed,
        );
        let evaluator = CachedScanRowEvaluator::try_new(
            Some(&available_schema),
            true,
            Some(requested_schema.clone()),
            None,
        )?;
        let projected = evaluator.evaluate(batch)?;

        assert_eq!(evaluator.stats_parsed_schema(), Some(requested_schema));
        assert_eq!(
            raw_stats_string(projected.clone(), 0),
            Some(raw_stats.to_string())
        );
        assert_eq!(
            stats_parsed_field_names(&projected),
            vec!["numRecords", "minValues"]
        );
        let min_values = projected
            .column_by_name(FIELD_STATS_PARSED)
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .and_then(|stats| stats.column_by_name("minValues"))
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .expect("raw fallback should produce minValues");
        assert_eq!(
            Scalar::from_array(min_values.column_by_name("value").unwrap().as_ref(), 0),
            Some(Scalar::Integer(1))
        );

        Ok(())
    }

    #[test]
    fn cached_uniform_evaluator_rejects_incompatible_seed_without_raw() {
        let available_schema = Arc::new(
            StructType::try_new([StructField::nullable("minValues", DataType::STRING)]).unwrap(),
        );
        let error = match CachedScanRowEvaluator::try_new(
            Some(&available_schema),
            false,
            Some(selected_min_values_stats_schema()),
            None,
        ) {
            Ok(_) => panic!("incompatible parsed seed should fail without raw statistics"),
            Err(error) => error,
        };

        assert!(matches!(error, DeltaTableError::SchemaMismatch { .. }));
        assert!(
            error.to_string().contains("stats_parsed.minValues"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_stats_column_impl_generates_full_json_before_narrowing_parsed() -> DeltaResult<()> {
        let batch = append_stats_parsed(
            &scan_row_batch_with_optional_stats(None),
            value_stats_parsed(),
        );
        let projected = parse_stats_column_impl(
            &batch,
            num_records_stats_schema(),
            None,
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::compatibility(StatsProjection::NumRecordsOnly),
        )?;

        let raw_stats = raw_stats_string(projected.clone(), 0)
            .expect("full typed stats should generate compatibility JSON");
        let raw_stats: serde_json::Value = serde_json::from_str(&raw_stats)?;
        assert_eq!(
            raw_stats,
            serde_json::json!({
                "numRecords": 11,
                "minValues": {"value": 1},
                "maxValues": {"value": 9},
                "nullCount": {"value": 0}
            })
        );
        assert_eq!(stats_parsed_field_names(&projected), vec!["numRecords"]);

        Ok(())
    }

    #[test]
    fn parse_stats_column_impl_keeps_raw_null_when_typed_stats_are_empty() -> DeltaResult<()> {
        let batch = append_stats_parsed(
            &scan_row_batch_with_optional_stats(None),
            empty_num_records_stats_parsed(),
        );
        let projected = parse_stats_column_impl(
            &batch,
            num_records_stats_schema(),
            None,
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::compatibility(StatsProjection::NumRecordsOnly),
        )?;

        assert_eq!(raw_stats_string(projected, 0), None);

        let raw_stats = r#"{"numRecords":11}"#;
        let batch = append_stats_parsed(
            &scan_row_batch_with_optional_stats(Some(raw_stats)),
            empty_num_records_stats_parsed(),
        );
        let projected = parse_stats_column_impl(
            &batch,
            num_records_stats_schema(),
            None,
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::compatibility(StatsProjection::NumRecordsOnly),
        )?;
        assert_eq!(raw_stats_string(projected, 0), Some(raw_stats.to_string()));

        Ok(())
    }

    fn stats_schema(fields: impl IntoIterator<Item = StructField>) -> SchemaRef {
        Arc::new(StructType::try_new(fields).unwrap())
    }

    fn cached_batch(
        rows: usize,
        raw_stats: Option<&[Option<&str>]>,
        stats_parsed: StructArray,
    ) -> RecordBatch {
        assert_eq!(stats_parsed.len(), rows);
        let base_schema: ArrowSchema = scan_row_schema().as_ref().try_into_arrow().unwrap();
        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for field in base_schema.fields() {
            let column: ArrayRef = match field.name().as_str() {
                FIELD_STATS if raw_stats.is_none() => continue,
                "path" => Arc::new(StringArray::from(vec!["file.parquet"; rows])),
                "size" | "modificationTime" => Arc::new(Int64Array::from(vec![1; rows])),
                FIELD_STATS => Arc::new(StringArray::from(raw_stats.unwrap().to_vec())),
                _ => new_null_array(field.data_type(), rows),
            };
            fields.push(field.clone());
            columns.push(column);
        }

        fields.push(Arc::new(Field::new(
            FIELD_STATS_PARSED,
            stats_parsed.data_type().clone(),
            true,
        )));
        columns.push(Arc::new(stats_parsed));
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
    }

    fn projected_stats(batch: &RecordBatch) -> &StructArray {
        batch
            .column_by_name(FIELD_STATS_PARSED)
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .expect("stats_parsed should be a struct")
    }

    #[tokio::test]
    async fn cached_batches_preserve_raw_and_parsed_stats_for_kernel_roundtrip() -> DeltaResult<()>
    {
        let raw_stats =
            r#"{"maxValues":{"id":9},"numRecords":11,"nullCount":{"id":0},"minValues":{"id":1}}"#;
        let batch = scan_row_batch_with_optional_stats(Some(raw_stats));
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let projected = parse_stats_column_with_schema(
            snapshot.inner.as_ref(),
            &batch,
            snapshot.inner.stats_schema()?,
        )?;
        // Keep this guard so cached batches still pass through the uniform evaluator
        // without rebuilding raw `stats` from `ToJson`.
        let available_stats_schema = snapshot.inner.stats_schema()?;
        let partition_schema = snapshot.inner.partitions_schema()?;
        let evaluator = CachedScanRowEvaluator::try_new(
            Some(&available_stats_schema),
            true,
            Some(available_stats_schema.clone()),
            partition_schema.as_ref(),
        )?;
        let reparsed = evaluator.evaluate(projected)?;

        assert_eq!(
            raw_stats_string(reparsed.clone(), 0),
            Some(raw_stats.to_string())
        );
        let stats = reparsed
            .column_by_name(FIELD_STATS_PARSED)
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .expect("stats_parsed should be preserved");
        let num_records = stats
            .column_by_name("numRecords")
            .expect("numRecords should be preserved");
        let min_values = stats
            .column_by_name("minValues")
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .expect("minValues should be a struct");
        let max_values = stats
            .column_by_name("maxValues")
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .expect("maxValues should be a struct");
        let null_count = stats
            .column_by_name("nullCount")
            .and_then(|column| column.as_any().downcast_ref::<StructArray>())
            .expect("nullCount should be a struct");
        let min_value = min_values
            .column_by_name("id")
            .expect("min value should be preserved");
        let max_value = max_values
            .column_by_name("id")
            .expect("max value should be preserved");
        let null_value = null_count
            .column_by_name("id")
            .expect("null count should be preserved");

        assert_eq!(
            Scalar::from_array(num_records.as_ref(), 0),
            Some(Scalar::Long(11))
        );
        assert_eq!(
            Scalar::from_array(min_value.as_ref(), 0),
            Some(Scalar::Long(1))
        );
        assert_eq!(
            Scalar::from_array(max_value.as_ref(), 0),
            Some(Scalar::Long(9))
        );
        assert_eq!(
            Scalar::from_array(null_value.as_ref(), 0),
            Some(Scalar::Long(0))
        );

        Ok(())
    }

    #[tokio::test]
    async fn cached_batches_without_stats_parsed_use_uniform_evaluator() -> DeltaResult<()> {
        let raw_stats = r#"{"numRecords":11}"#;
        let batch = scan_row_batch_with_optional_stats(Some(raw_stats));
        let evaluator = CachedScanRowEvaluator::try_new(None, true, None, None)?;
        let projected = evaluator.evaluate(batch)?;

        assert!(projected.column_by_name(FIELD_STATS_PARSED).is_none());
        assert_eq!(raw_stats_string(projected, 0), Some(raw_stats.to_string()));

        Ok(())
    }

    #[test]
    fn cached_stats_add_missing_nullable_field_without_raw() -> DeltaResult<()> {
        let available = stats_schema([StructField::nullable("numRecords", DataType::LONG)]);
        let requested = stats_schema([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("added", DataType::INTEGER),
        ]);
        let parsed = StructArray::from(vec![(
            Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(11)])) as ArrayRef,
        )]);

        let evaluator =
            CachedScanRowEvaluator::try_new(Some(&available), false, Some(requested), None)?;
        let projected = evaluator.evaluate(cached_batch(1, None, parsed))?;
        let stats = projected_stats(&projected);

        assert_eq!(
            stats
                .column_by_name("numRecords")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            11
        );
        let added = stats.column_by_name("added").unwrap();
        assert_eq!(added.data_type(), &ArrowDataType::Int32);
        assert!(added.is_null(0));
        Ok(())
    }

    #[test]
    fn cached_stats_drop_extra_field_without_raw() -> DeltaResult<()> {
        let available = stats_schema([
            StructField::nullable("keep", DataType::INTEGER),
            StructField::nullable("dropped", DataType::STRING),
        ]);
        let requested = stats_schema([StructField::nullable("keep", DataType::INTEGER)]);
        let parsed = StructArray::from(vec![
            (
                Arc::new(Field::new("keep", ArrowDataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("dropped", ArrowDataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("unused")])) as ArrayRef,
            ),
        ]);

        let evaluator =
            CachedScanRowEvaluator::try_new(Some(&available), false, Some(requested), None)?;
        let projected = evaluator.evaluate(cached_batch(1, None, parsed))?;
        let stats = projected_stats(&projected);
        let ArrowDataType::Struct(fields) = stats.data_type() else {
            unreachable!()
        };
        assert_eq!(
            fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
            ["keep"]
        );
        Ok(())
    }

    #[test]
    fn cached_stats_reorder_fields_by_name_without_raw() -> DeltaResult<()> {
        let available = stats_schema([
            StructField::nullable("first", DataType::INTEGER),
            StructField::nullable("second", DataType::LONG),
        ]);
        let requested = stats_schema([
            StructField::nullable("second", DataType::LONG),
            StructField::nullable("first", DataType::INTEGER),
        ]);
        let parsed = StructArray::from(vec![
            (
                Arc::new(Field::new("first", ArrowDataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("second", ArrowDataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(2)])) as ArrayRef,
            ),
        ]);

        let evaluator =
            CachedScanRowEvaluator::try_new(Some(&available), false, Some(requested), None)?;
        let projected = evaluator.evaluate(cached_batch(1, None, parsed))?;
        let stats = projected_stats(&projected);
        let ArrowDataType::Struct(fields) = stats.data_type() else {
            unreachable!()
        };
        assert_eq!(
            fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
            ["second", "first"]
        );
        assert_eq!(
            stats
                .column_by_name("second")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            stats
                .column_by_name("first")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        Ok(())
    }

    #[test]
    fn cached_stats_evolve_nested_fields_without_raw() -> DeltaResult<()> {
        let available_nested = StructType::try_new([
            StructField::nullable("dropped", DataType::INTEGER),
            StructField::nullable("keep", DataType::INTEGER),
        ])?;
        let requested_nested = StructType::try_new([
            StructField::nullable("added", DataType::INTEGER),
            StructField::nullable("keep", DataType::INTEGER),
        ])?;
        let available = stats_schema([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("minValues", available_nested),
        ]);
        let requested = stats_schema([
            StructField::nullable("minValues", requested_nested),
            StructField::nullable("numRecords", DataType::LONG),
        ]);

        let min_values = StructArray::new(
            Fields::from(vec![
                Field::new("dropped", ArrowDataType::Int32, true),
                Field::new("keep", ArrowDataType::Int32, true),
            ]),
            vec![
                Arc::new(Int32Array::from(vec![Some(9), Some(8), Some(7)])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
            ],
            Some(NullBuffer::from_iter([true, true, false])),
        );
        let parsed = StructArray::new(
            Fields::from(vec![
                Field::new("numRecords", ArrowDataType::Int64, true),
                Field::new("minValues", min_values.data_type().clone(), true),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)])),
                Arc::new(min_values),
            ],
            Some(NullBuffer::from_iter([true, false, true])),
        );

        let evaluator =
            CachedScanRowEvaluator::try_new(Some(&available), false, Some(requested), None)?;
        let projected = evaluator.evaluate(cached_batch(3, None, parsed))?;
        let stats = projected_stats(&projected);
        assert_eq!(
            stats.nulls().unwrap(),
            &NullBuffer::from_iter([true, false, true])
        );

        let min_values = stats
            .column_by_name("minValues")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            min_values.nulls().unwrap(),
            &NullBuffer::from_iter([true, true, false])
        );
        let ArrowDataType::Struct(fields) = min_values.data_type() else {
            unreachable!()
        };
        assert_eq!(
            fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
            ["added", "keep"]
        );
        assert_eq!(min_values.column_by_name("added").unwrap().null_count(), 3);
        assert_eq!(
            min_values
                .column_by_name("keep")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 3]
        );
        Ok(())
    }

    #[test]
    fn cached_stats_widen_primitive_without_raw() -> DeltaResult<()> {
        let available = stats_schema([StructField::nullable("value", DataType::INTEGER)]);
        let requested = stats_schema([StructField::nullable("value", DataType::LONG)]);
        let parsed = StructArray::from(vec![(
            Arc::new(Field::new("value", ArrowDataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef,
        )]);

        let evaluator =
            CachedScanRowEvaluator::try_new(Some(&available), false, Some(requested), None)?;
        let projected = evaluator.evaluate(cached_batch(2, None, parsed))?;
        let values = projected_stats(&projected)
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 7);
        assert!(values.is_null(1));
        Ok(())
    }

    #[test]
    fn cached_stats_normal_evolution_does_not_parse_raw() -> DeltaResult<()> {
        let available = stats_schema([StructField::nullable("numRecords", DataType::LONG)]);
        let requested = stats_schema([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("added", DataType::INTEGER),
        ]);
        let parsed = StructArray::from(vec![(
            Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(11)])) as ArrayRef,
        )]);
        let invalid_raw = [Some("not valid JSON")];

        let evaluator = CachedScanRowEvaluator::try_new(Some(&available), true, Some(requested), None)?;
        let projected = evaluator.evaluate(cached_batch(1, Some(&invalid_raw), parsed))?;
        assert!(
            projected_stats(&projected)
                .column_by_name("added")
                .unwrap()
                .is_null(0)
        );
        assert_eq!(
            raw_stats_string(projected, 0),
            Some("not valid JSON".to_string())
        );
        Ok(())
    }

    #[test]
    fn cached_stats_reject_truly_incompatible_present_type() {
        let available = stats_schema([StructField::nullable("value", DataType::STRING)]);
        let requested = stats_schema([StructField::nullable("value", DataType::INTEGER)]);
        let error =
            match CachedScanRowEvaluator::try_new(Some(&available), false, Some(requested), None) {
                Ok(_) => panic!("string statistics should not cast to integer"),
                Err(error) => error,
            };

        assert!(matches!(error, DeltaTableError::SchemaMismatch { .. }));
        assert!(error.to_string().contains("Utf8"));
        assert!(error.to_string().contains("Int32"));
    }
}
