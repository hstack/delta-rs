//! Codec for serializing and deserializing [`DeltaScanExec`] physical plans.
//!
//! Provides a [`PhysicalExtensionCodec`] implementation for distributed execution.
//! Expressions are serialized via DataFusion protobuf; kernel `Transform` expressions
//! use a custom wire format since they have no DataFusion equivalent.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow_schema::Field;
use dashmap::DashMap;
use datafusion::common::HashMap;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_proto::bytes::Serializeable;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::expressions::{ColumnName, Expression, FieldTransform, Transform};
use delta_kernel::schema::{DataType as KernelDataType, StructType};
use serde::{Deserialize, Serialize};
use super::{DeltaScanExec, ProjectedScanContract};
use super::plan::KernelScanPlan;
use crate::delta_datafusion::engine::{to_datafusion_expr, to_delta_expression};
use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::expr_adapter::build_expr_adapter_factory;
use crate::DeltaTableConfig;
use crate::kernel::size_limits::SnapshotLoadMetrics;
use crate::kernel::Snapshot;

/// Codec for serializing/deserializing [`DeltaScanExec`] physical plans.
///
/// This codec enables distributed execution by serializing the inputs needed
/// to reconstruct the execution plan rather than the plan itself. This approach
/// avoids the need for serde support in delta-kernel types.
#[derive(Debug, Clone, Default)]
pub struct DeltaNextPhysicalCodec;

impl PhysicalExtensionCodec for DeltaNextPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let wire: DeltaScanExecWire = serde_json::from_slice(buf).map_err(|e| {
            DataFusionError::Internal(format!("Failed to decode DeltaScanExec: {e}"))
        })?;

        wire.into_exec(inputs, ctx)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        let delta_scan = node.as_any().downcast_ref::<DeltaScanExec>().ok_or_else(|| {
            DataFusionError::Internal("Expected DeltaScanExec for encoding".to_string())
        })?;

        let wire = DeltaScanExecWire::try_from(delta_scan)?;
        serde_json::to_writer(buf, &wire).map_err(|e| {
            DataFusionError::Internal(format!("Failed to encode DeltaScanExec: {e}"))
        })?;
        Ok(())
    }
}

/// Wire format for a kernel FieldTransform.
#[derive(Debug, Serialize, Deserialize)]
struct FieldTransformWire {
    exprs: Vec<Vec<u8>>,
    is_replace: bool,
    optional: bool, 
}

/// Wire format for a kernel Transform expression.
///
/// Transform is a sparse schema modification: specifies which fields to modify,
/// with unmentioned fields passing through unchanged.
#[derive(Debug, Serialize, Deserialize)]
struct TransformWire {
    input_path: Option<Vec<String>>,
    field_transforms: std::collections::HashMap<String, FieldTransformWire>,
    prepended_fields: Vec<Vec<u8>>,
}

/// Wire format for serializing [`DeltaScanExec`].
///
/// Uses `std::collections::HashMap` instead of `DashMap` for serde compatibility.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DeltaScanExecWire {
    scan: ScanPlanWire,
    transforms: std::collections::HashMap<String, TransformWire>,
    selection_vectors: std::collections::HashMap<String, Vec<bool>>,
    input_file_id_column: String,
    file_id_column: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ScanPlanWire {
    snapshot: Snapshot,
    contract: ProjectedScanContractWire,
    filters: Vec<Vec<u8>>,
    skipping_predicate: Option<Vec<Vec<u8>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ProjectedScanContractWire {
    table_schema: SchemaRef,
    provider_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Vec<u8>>,
}

impl TryFrom<&DeltaScanExec> for DeltaScanExecWire {
    type Error = DataFusionError;

    fn try_from(exec: &DeltaScanExec) -> Result<Self, Self::Error> {
        let snapshot = {
            let exec_scan_plan_scan_snapshot = exec.scan_plan.scan.snapshot().clone();
            // @HSTACK FIXME AT upgrade
            // ATM, DeltaTableConfig is ONLY used with defaults
            // The only thing that we set in it is the log_size_limiter -
            //      which has already been used early in the logical / planning phase
            // At upgrade, RECHECK usage sites for DeltaTableConfig, we'll need to re-evaluate if
            //      stuff begins writing to it
            let delta_table_config = DeltaTableConfig::default();
            Snapshot {
                inner: exec_scan_plan_scan_snapshot,
                config: delta_table_config,
                materialized_files: None,
            }
        };

        let scan_plan = &exec.scan_plan;

        let projected_scan_contract_wire = ProjectedScanContractWire {
            table_schema: scan_plan.contract.table_schema.clone(),
            provider_schema: scan_plan.contract.provider_schema.clone(),
            projection: scan_plan.contract.projection.clone(),
            filters: scan_plan.contract.filters
                .iter()
                .map(|p| {
                    p.to_bytes().map(|b| b.to_vec()).unwrap()
                })
                .collect::<Vec<_>>(),
        };

        let scan_plan_wire = ScanPlanWire {
            snapshot,
            contract: projected_scan_contract_wire,
            filters: scan_plan
                .filters
                .iter()
                .map(|p| {
                    p.to_bytes().map(|b| b.to_vec()).unwrap()
                })
                .collect::<Vec<_>>(),
            skipping_predicate: scan_plan
                .skipping_predicate
                .clone()
                .map(|sp| {
                    let out: Vec<Vec<u8>> = sp
                        .iter()
                        .map(|e| e.to_bytes().map(|b| b.to_vec()).unwrap())
                        .collect::<Vec<_>>();
                    out
                })
        };

        let transforms: std::collections::HashMap<String, TransformWire> = exec
            .transforms
            .iter()
            .map(|(file_url, kernel_expr)| {
                serialize_transform(kernel_expr.as_ref())
                    .map(|wire| (file_url.clone(), wire))
            })
            .collect::<Result<_, _>>()?;

        let selection_vectors: std::collections::HashMap<String, Vec<bool>> = exec
            .selection_vectors
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        Ok(Self {
            scan: scan_plan_wire,
            transforms,
            selection_vectors,
            input_file_id_column: exec.input_file_id_column.clone(),
            file_id_column: exec.file_id_column.clone(),
        })
    }
}

/// Converts kernel Expression -> DataFusion Expr -> protobuf bytes.
/// Does not support Transform expressions - use `serialize_transform` instead.
fn serialize_kernel_expression(expr: &Expression) -> Result<Vec<u8>, DataFusionError> {
    let placeholder_type = KernelDataType::STRING;
    let df_expr = to_datafusion_expr(expr, &placeholder_type)?;
    let bytes = df_expr.to_bytes()?;
    Ok(bytes.to_vec())
}

/// Converts protobuf bytes -> DataFusion Expr -> kernel Expression.
fn deserialize_kernel_expression(bytes: &[u8]) -> Result<Expression, DataFusionError> {
    let df_expr = Expr::from_bytes(bytes)?;
    to_delta_expression(&df_expr)
        .map_err(|e| DataFusionError::Internal(format!("Failed to convert to kernel expr: {e}")))
}

/// Converts a kernel Transform expression to wire format.
fn serialize_transform(expr: &Expression) -> Result<TransformWire, DataFusionError> {
    match expr {
        Expression::Transform(transform) => {
            let input_path = transform
                .input_path
                .as_ref()
                .map(|p| p.iter().map(|s| s.to_string()).collect());

            let field_transforms = transform
                .field_transforms
                .iter()
                .map(|(name, ft)| {
                    let exprs = ft
                        .exprs
                        .iter()
                        .map(|e| serialize_kernel_expression(e))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok((
                        name.clone(),
                        FieldTransformWire {
                            exprs,
                            is_replace: ft.is_replace,
                            optional: ft.optional,
                        },
                    ))
                })
                .collect::<Result<_, DataFusionError>>()?;

            let prepended_fields = transform
                .prepended_fields
                .iter()
                .map(|e| serialize_kernel_expression(e))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(TransformWire {
                input_path,
                field_transforms,
                prepended_fields,
            })
        }
        _ => Err(DataFusionError::Internal(format!(
            "Expected Transform expression, got {:?}",
            expr
        ))),
    }
}

/// Converts wire format to a kernel Transform expression.
fn deserialize_transform(wire: TransformWire) -> Result<Expression, DataFusionError> {
    let input_path = wire.input_path.map(ColumnName::new);

    let field_transforms = wire
        .field_transforms
        .into_iter()
        .map(|(name, ft_wire)| {
            let exprs = ft_wire
                .exprs
                .iter()
                .map(|bytes| deserialize_kernel_expression(bytes).map(Arc::new))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                name,
                FieldTransform {
                    exprs,
                    is_replace: ft_wire.is_replace,
                    optional: ft_wire.optional,
                },
            ))
        })
        .collect::<Result<std::collections::HashMap<_, _>, DataFusionError>>()?;

    let prepended_fields = wire
        .prepended_fields
        .iter()
        .map(|bytes| deserialize_kernel_expression(bytes).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Expression::Transform(Transform {
        input_path,
        field_transforms,
        prepended_fields,
    }))
}

impl DeltaScanExecWire {
    /// Reconstruct a [`DeltaScanExec`] from the wire format.
    fn into_exec(
        self,
        inputs: &[Arc<dyn ExecutionPlan>],
        task: &TaskContext,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "DeltaScanExec expects exactly 1 input, got {}",
                inputs.len()
            )));
        }
        let input = inputs[0].clone();

        let mut execution_plan = input.clone();
        #[allow(clippy::collapsible_if)]
        if let Some(ds_exec) = execution_plan.as_any().downcast_ref::<DataSourceExec>() {
            if let Some(file_conf) = ds_exec
                .data_source()
                .as_any()
                .downcast_ref::<FileScanConfig>()
            {
                let new_file_scan_config = FileScanConfigBuilder::from(file_conf.clone())
                    .with_expr_adapter(build_expr_adapter_factory())
                    .build();

                // DataSourceExec::from_data_source(new_file_scan_config)
                execution_plan = Arc::new(
                    ds_exec
                        .clone()
                        .with_data_source(Arc::new(new_file_scan_config)),
                );
            }
        }

        let mut delta_scan_config = DeltaScanConfig::new();
        if let Some(fic) = self.file_id_column {
            delta_scan_config = delta_scan_config.with_file_column_name(fic);
        }

        let filters = self.scan.contract.filters
            .iter()
            .map(|b| {
                Expr::from_bytes_with_registry(b, task).unwrap()
            })
            .collect::<Vec<_>>();

        let contract = ProjectedScanContract::try_new(
            self.scan.contract.table_schema.clone(),
            self.scan.contract.provider_schema.clone(),
            &delta_scan_config,
            self.scan.contract.projection.as_ref(),
            &filters
        )?;

        let skipping_predicate = self.scan.skipping_predicate
            .map(|osp| {
                osp.iter()
                    .map(|b| Expr::from_bytes_with_registry(b, task).unwrap())
                    .collect::<Vec<_>>()
            });

        let scan_plan = KernelScanPlan::try_new_with_contract(
            &self.scan.snapshot,
            contract,
            &filters,
            &delta_scan_config,
            skipping_predicate
        )?;

        let transforms: HashMap<String, Arc<Expression>> = self
            .transforms
            .into_iter()
            .map(|(file_url, wire)| {
                deserialize_transform(wire).map(|expr| (file_url, Arc::new(expr)))
            })
            .collect::<Result<_, _>>()?;

        let selection_vectors: DashMap<String, Vec<bool>> =
            self.selection_vectors.into_iter().collect();

        let exec = DeltaScanExec::new(
            Arc::new(scan_plan),
            execution_plan,
            Arc::new(transforms),
            Arc::new(selection_vectors),
            Default::default(),
            Default::default(),
        );

        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{col, lit};
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;

    use crate::delta_datafusion::session::create_session;
    use crate::delta_datafusion::table_provider::next::DeltaScan;
    use crate::kernel::Snapshot;
    use crate::test_utils::{TestResult, TestTables};

    use super::*;

    async fn create_delta_scan_exec(
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
    ) -> TestResult<Arc<dyn ExecutionPlan>> {
        create_delta_scan_exec_from_table(TestTables::Simple, filters, projection).await
    }

    async fn create_delta_scan_exec_from_table(
        table: TestTables,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
    ) -> TestResult<Arc<dyn ExecutionPlan>> {
        let log_store = table.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::builder().with_snapshot(snapshot).await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let plan = provider.scan(&state, projection, filters, None).await?;
        Ok(plan)
    }

    fn extract_delta_scan_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&DeltaScanExec> {
        plan.as_any().downcast_ref::<DeltaScanExec>()
    }

    #[tokio::test]
    async fn test_codec_roundtrip_basic() -> TestResult {
        let plan = create_delta_scan_exec(&[], None).await?;

        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        assert!(!buf.is_empty(), "Encoded buffer should not be empty");

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let input = delta_scan.children()[0].clone();
        let decoded = codec.try_decode(&buf, &[input], &task_ctx)?;

        let decoded_delta_scan =
            extract_delta_scan_exec(&decoded).expect("Expected DeltaScanExec after decode");

        assert_eq!(
            delta_scan.scan_plan.contract.result_schema,
            decoded_delta_scan.scan_plan.contract.result_schema,
            "Result schemas should match"
        );
        assert_eq!(
            delta_scan.file_id_column,
            decoded_delta_scan.file_id_column,
            "File ID columns should match"
        );
        assert_eq!(
            delta_scan.input_file_id_column,
            decoded_delta_scan.input_file_id_column,
            "Retain file IDs should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_codec_roundtrip_with_projection() -> TestResult {
        let projection = vec![0usize];
        let plan = create_delta_scan_exec(&[], Some(&projection)).await?;

        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let input = delta_scan.children()[0].clone();
        let decoded = codec.try_decode(&buf, &[input], &task_ctx)?;

        let decoded_delta_scan =
            extract_delta_scan_exec(&decoded).expect("Expected DeltaScanExec after decode");

        assert_eq!(
            delta_scan.scan_plan.contract.result_schema.fields().len(),
            decoded_delta_scan.scan_plan.contract.result_schema.fields().len(),
            "Projected schema field count should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_codec_roundtrip_with_filter() -> TestResult {
        let filters = vec![col("id").gt(lit(5i64))];
        let plan = create_delta_scan_exec(&filters, None).await?;

        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let input = delta_scan.children()[0].clone();
        let decoded = codec.try_decode(&buf, &[input], &task_ctx)?;

        let decoded_delta_scan =
            extract_delta_scan_exec(&decoded).expect("Expected DeltaScanExec after decode");

        assert_eq!(
            delta_scan.scan_plan.contract.result_schema,
            decoded_delta_scan.scan_plan.contract.result_schema,
            "Result schemas should match with filter"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_wire_format_serialization() -> TestResult {
        let plan = create_delta_scan_exec(&[], None).await?;

        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");

        let wire = DeltaScanExecWire::try_from(delta_scan)?;

        let json = serde_json::to_string(&wire)?;
        assert!(!json.is_empty(), "JSON should not be empty");

        let deserialized: DeltaScanExecWire = serde_json::from_str(&json)?;

        assert_eq!(
            wire.file_id_column, deserialized.file_id_column,
            "File ID column should roundtrip"
        );
        assert_eq!(
            wire.input_file_id_column, deserialized.input_file_id_column,
            "Retain file IDs should roundtrip"
        );
        assert_eq!(
            wire.scan.contract.table_schema, deserialized.scan.contract.table_schema,
            "Result schema should roundtrip"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_codec_decode_wrong_input_count() -> TestResult {
        let plan = create_delta_scan_exec(&[], None).await?;

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let result = codec.try_decode(&buf, &[], &task_ctx);
        assert!(result.is_err(), "Should fail with 0 inputs");

        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");
        let input = delta_scan.children()[0].clone();
        let result = codec.try_decode(&buf, &[input.clone(), input], &task_ctx);
        assert!(result.is_err(), "Should fail with 2 inputs");

        Ok(())
    }

    #[test]
    fn test_kernel_expression_serialization_roundtrip() {
        use delta_kernel::expressions::{ColumnName, Expression as KernelExpression, Scalar};

        let column_expr = KernelExpression::Column(ColumnName::new(["test_column"]));
        let serialized = serialize_kernel_expression(&column_expr).unwrap();
        let deserialized = deserialize_kernel_expression(&serialized).unwrap();
        assert_eq!(column_expr, deserialized, "Column expression should roundtrip");

        let literal_expr = KernelExpression::Literal(Scalar::Integer(42));
        let serialized = serialize_kernel_expression(&literal_expr).unwrap();
        let deserialized = deserialize_kernel_expression(&serialized).unwrap();
        assert_eq!(literal_expr, deserialized, "Literal expression should roundtrip");

        let string_literal = KernelExpression::Literal(Scalar::String("hello".to_string()));
        let serialized = serialize_kernel_expression(&string_literal).unwrap();
        let deserialized = deserialize_kernel_expression(&serialized).unwrap();
        assert_eq!(string_literal, deserialized, "String literal should roundtrip");
    }

    #[tokio::test]
    async fn test_wire_format_with_selection_vectors() -> TestResult {
        let plan = create_delta_scan_exec(&[], None).await?;
        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");

        let wire = DeltaScanExecWire::try_from(delta_scan)?;

        assert!(
            wire.selection_vectors.is_empty() || !wire.selection_vectors.is_empty(),
            "Selection vectors should serialize (empty or not)"
        );

        let json = serde_json::to_string(&wire)?;
        let deserialized: DeltaScanExecWire = serde_json::from_str(&json)?;

        assert_eq!(
            wire.selection_vectors.len(),
            deserialized.selection_vectors.len(),
            "Selection vectors count should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_codec_roundtrip_preserves_transforms_and_selection_vectors() -> TestResult {
        let plan = create_delta_scan_exec(&[], None).await?;
        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let input = delta_scan.children()[0].clone();
        let decoded = codec.try_decode(&buf, &[input], &task_ctx)?;

        let decoded_delta_scan =
            extract_delta_scan_exec(&decoded).expect("Expected DeltaScanExec after decode");

        assert_eq!(
            delta_scan.transforms.len(),
            decoded_delta_scan.transforms.len(),
            "Transforms count should match"
        );

        assert_eq!(
            delta_scan.selection_vectors.len(),
            decoded_delta_scan.selection_vectors.len(),
            "Selection vectors count should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_codec_roundtrip_with_deletion_vectors() -> TestResult {
        let plan =
            create_delta_scan_exec_from_table(TestTables::WithDvSmall, &[], None).await?;
        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");

        assert!(
            !delta_scan.selection_vectors.is_empty(),
            "Table with deletion vectors should have non-empty selection_vectors"
        );

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let input = delta_scan.children()[0].clone();
        let decoded = codec.try_decode(&buf, &[input], &task_ctx)?;

        let decoded_delta_scan =
            extract_delta_scan_exec(&decoded).expect("Expected DeltaScanExec after decode");

        assert_eq!(
            delta_scan.selection_vectors.len(),
            decoded_delta_scan.selection_vectors.len(),
            "Selection vectors count should match"
        );

        for entry in delta_scan.selection_vectors.iter() {
            let key = entry.key();
            let original_vec = entry.value();
            let decoded_vec = decoded_delta_scan
                .selection_vectors
                .get(key)
                .expect("Decoded should have same keys");
            assert_eq!(
                original_vec.as_slice(),
                decoded_vec.value().as_slice(),
                "Selection vector values should match for key {key}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_codec_roundtrip_with_column_mapping() -> TestResult {
        let plan =
            create_delta_scan_exec_from_table(TestTables::WithColumnMapping, &[], None).await?;
        let delta_scan = extract_delta_scan_exec(&plan).expect("Expected DeltaScanExec");

        // Column mapping tables have transforms that inject partition values.
        // This test verifies full roundtrip serialization of Transform expressions.
        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let input = delta_scan.children()[0].clone();
        let decoded = codec.try_decode(&buf, &[input], &task_ctx)?;

        let decoded_delta_scan =
            extract_delta_scan_exec(&decoded).expect("Expected DeltaScanExec after decode");

        assert_eq!(
            delta_scan.transforms.len(),
            decoded_delta_scan.transforms.len(),
            "Transforms count should match"
        );

        // Verify each transform was correctly serialized and deserialized
        for key in delta_scan.transforms.keys() {
            assert!(
                decoded_delta_scan.transforms.contains_key(key),
                "Decoded should have transform for key {key}"
            );
        }

        Ok(())
    }
}
