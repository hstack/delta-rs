//! Codec for serializing and deserializing [`DeltaScanExec`] physical plans.
//!
//! Provides a [`PhysicalExtensionCodec`] implementation for distributed execution.
//! Expressions are serialized via DataFusion protobuf; kernel `Transform` expressions
//! use a custom wire format since they have no DataFusion equivalent.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use super::plan::KernelScanPlan;
use super::{DeltaScanExec, DeltaScanMetaExec, ProjectedScanContract, PublicFileIdMap};
use crate::DeltaTableConfig;
use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::engine::{to_datafusion_expr, to_delta_expression};
use crate::kernel::Snapshot;
use crate::kernel::size_limits::SnapshotLoadMetrics;
use arrow::datatypes::SchemaRef;
use dashmap::DashMap;
use datafusion::common::HashMap;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::prelude::Expr;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_proto::bytes::Serializeable;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use delta_kernel::expressions::{
    ColumnName, Expression, ExpressionFieldPatch, ExpressionStructPatch,
};
use delta_kernel::schema::DataType as KernelDataType;
use serde::{Deserialize, Serialize};

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
        let wire: ScanExecWire = serde_json::from_slice(buf).map_err(|e| {
            DataFusionError::Internal(format!("Failed to decode Delta scan exec: {e}"))
        })?;

        match wire {
            ScanExecWire::Scan(wire) => wire.into_exec(inputs, ctx),
            ScanExecWire::Meta(wire) => wire.into_exec(inputs, ctx),
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        let wire = if let Some(delta_scan) = node.downcast_ref::<DeltaScanExec>() {
            ScanExecWire::Scan(DeltaScanExecWire::try_from(delta_scan)?)
        } else if let Some(meta_scan) = node.downcast_ref::<DeltaScanMetaExec>() {
            ScanExecWire::Meta(DeltaScanMetaExecWire::try_from(meta_scan)?)
        } else {
            return Err(DataFusionError::Internal(
                "Expected DeltaScanExec or DeltaScanMetaExec for encoding".to_string(),
            ));
        };

        serde_json::to_writer(buf, &wire).map_err(|e| {
            DataFusionError::Internal(format!("Failed to encode Delta scan exec: {e}"))
        })?;
        Ok(())
    }
}

/// Tagged wire wrapper dispatching between the two scan exec variants.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ScanExecWire {
    Scan(DeltaScanExecWire),
    Meta(DeltaScanMetaExecWire),
}

/// Wire format for a kernel FieldTransform.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct ExpressionFieldPatchWire {
    keep_input: bool,
    insertions: Vec<Vec<u8>>,
    optional: bool,
}

/// Wire format for a kernel StructPatch expression.
///
/// StructPatch is a sparse schema modification: specifies which fields to modify,
/// with unmentioned fields passing through unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct StructPatchWire {
    input_path: Option<Vec<String>>,
    field_patches: BTreeMap<String, ExpressionFieldPatchWire>,
    prepended_fields: Vec<Vec<u8>>,
    appended_fields: Vec<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TransformsWire {
    dedupped_expressions: Vec<StructPatchWire>,
    transforms: BTreeMap<String, usize>,
}

/// Wire format for serializing [`DeltaScanExec`].
///
/// Uses standard collections instead of `DashMap` for serde compatibility.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DeltaScanExecWire {
    scan: ScanPlanWire,
    transforms: TransformsWire,
    selection_vectors: std::collections::HashMap<String, Vec<bool>>,
    input_file_id_column: String,
    file_id_column: Option<String>,
    public_file_ids: std::collections::HashMap<String, String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_schema: Option<SchemaRef>,
    projection: Option<Vec<usize>>,
    filters: Vec<Vec<u8>>,
    row_index_column: Option<String>,
}

/// Build the [`ScanPlanWire`] shared by both scan codecs.
fn scan_plan_wire(scan_plan: &KernelScanPlan) -> Result<ScanPlanWire, DataFusionError> {
    let snapshot = {
        let exec_scan_plan_scan_snapshot = scan_plan.scan.snapshot().clone();
        // @HSTACK FIXME AT upgrade
        // ATM, DeltaTableConfig is ONLY used with defaults
        // The only thing that we set in it is the log_size_limiter -
        //      which has already been used early in the logical / planning phase
        // At upgrade, RECHECK usage sites for DeltaTableConfig, we'll need to re-evaluate if
        //      stuff begins writing to it
        let delta_table_config = DeltaTableConfig::default();
        let load_metrics = SnapshotLoadMetrics::from_snapshot(&exec_scan_plan_scan_snapshot);
        Snapshot {
            inner: exec_scan_plan_scan_snapshot,
            config: delta_table_config,
            materialized_files: None,
        }
    };

    let row_index_column = scan_plan
        .contract
        .row_index_field
        .as_ref()
        .map(|f| f.name().clone());

    // Arrow schema equality can ignore dictionary properties that the wire format preserves.
    let table_schema_json = serde_json::to_value(&scan_plan.contract.table_schema)
        .map_err(|e| DataFusionError::Internal(format!("Failed to encode table schema: {e}")))?;
    let provider_schema_json = serde_json::to_value(&scan_plan.contract.provider_schema)
        .map_err(|e| DataFusionError::Internal(format!("Failed to encode provider schema: {e}")))?;

    let projected_scan_contract_wire = ProjectedScanContractWire {
        table_schema: scan_plan.contract.table_schema.clone(),
        provider_schema: (provider_schema_json != table_schema_json)
            .then(|| scan_plan.contract.provider_schema.clone()),
        projection: scan_plan.contract.projection.clone(),
        filters: scan_plan
            .contract
            .filters
            .iter()
            .map(|p| p.to_bytes().map(|b| b.to_vec()).unwrap())
            .collect::<Vec<_>>(),
        row_index_column,
    };

    Ok(ScanPlanWire {
        snapshot,
        contract: projected_scan_contract_wire,
        filters: scan_plan
            .filters
            .iter()
            .map(|p| p.to_bytes().map(|b| b.to_vec()).unwrap())
            .collect::<Vec<_>>(),
        skipping_predicate: scan_plan.skipping_predicate.clone().map(|sp| {
            sp.iter()
                .map(|e| e.to_bytes().map(|b| b.to_vec()).unwrap())
                .collect::<Vec<_>>()
        }),
    })
}

impl TryFrom<&DeltaScanExec> for DeltaScanExecWire {
    type Error = DataFusionError;

    fn try_from(exec: &DeltaScanExec) -> Result<Self, Self::Error> {
        let scan_plan_wire = scan_plan_wire(&exec.scan_plan)?;

        let transforms = serialize_transforms(&exec.transforms)?;

        let selection_vectors: std::collections::HashMap<String, Vec<bool>> = exec
            .selection_vectors
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let public_file_ids = exec
            .public_file_ids
            .as_ref()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<std::collections::HashMap<String, String>>();

        Ok(Self {
            scan: scan_plan_wire,
            transforms,
            selection_vectors,
            input_file_id_column: exec.input_file_id_column.clone(),
            file_id_column: exec.file_id_column.clone(),
            public_file_ids,
        })
    }
}

fn serialize_transforms(
    transforms: &HashMap<String, Arc<Expression>>,
) -> Result<TransformsWire, DataFusionError> {
    let mut entries = transforms.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

    let mut dedupped_expressions = Vec::new();
    let mut temporary_unique_expressions: HashMap<StructPatchWire, usize> = HashMap::default();
    let mut output_transforms = BTreeMap::new();
    for (file_id, expression) in entries {
        let wire = serialize_transform(expression)?;
        let index = *temporary_unique_expressions
            .entry(wire)
            .or_insert_with_key(|wire| {
                let index = dedupped_expressions.len();
                dedupped_expressions.push(wire.clone());
                index
            });
        output_transforms.insert(file_id.clone(), index);
    }
    Ok(TransformsWire {
        dedupped_expressions,
        transforms: output_transforms,
    })
}

fn deserialize_transforms(
    transforms: TransformsWire,
) -> Result<HashMap<String, Arc<Expression>>, DataFusionError> {
    let expressions = transforms
        .dedupped_expressions
        .into_iter()
        .map(|wire| deserialize_transform(wire).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    transforms
        .transforms
        .into_iter()
        .map(|(file_id, index)| {
            let expression = expressions.get(index).cloned().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Invalid transform index {index} for file {file_id:?}: only {} transforms",
                    expressions.len()
                ))
            })?;
            Ok((file_id, expression))
        })
        .collect()
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
fn serialize_transform(expr: &Expression) -> Result<StructPatchWire, DataFusionError> {
    match expr {
        Expression::StructPatch(struct_patch) => {
            let input_path = struct_patch
                .input_path
                .as_ref()
                .map(|p| p.iter().map(|s| s.to_string()).collect());

            let field_patches = struct_patch
                .field_patches
                .iter()
                .map(|(name, ft)| {
                    let insertions = ft
                        .insertions
                        .iter()
                        .map(|e| serialize_kernel_expression(e))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok((
                        name.clone(),
                        ExpressionFieldPatchWire {
                            keep_input: ft.keep_input,
                            insertions,
                            optional: ft.optional,
                        },
                    ))
                })
                .collect::<Result<_, DataFusionError>>()?;

            let prepended_fields = struct_patch
                .prepended_fields
                .iter()
                .map(|e| serialize_kernel_expression(e))
                .collect::<Result<Vec<_>, _>>()?;

            let appended_fields = struct_patch
                .appended_fields
                .iter()
                .map(|e| serialize_kernel_expression(e))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(StructPatchWire {
                input_path,
                field_patches,
                prepended_fields,
                appended_fields,
            })
        }
        _ => Err(DataFusionError::Internal(format!(
            "Expected Transform expression, got {:?}",
            expr
        ))),
    }
}

/// Converts wire format to a kernel Transform expression.
fn deserialize_transform(wire: StructPatchWire) -> Result<Expression, DataFusionError> {
    let input_path = wire.input_path.map(ColumnName::new);

    let field_patches = wire
        .field_patches
        .into_iter()
        .map(|(name, ft_wire)| {
            let exprs = ft_wire
                .insertions
                .iter()
                .map(|bytes| deserialize_kernel_expression(bytes).map(Arc::new))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                name,
                ExpressionFieldPatch {
                    keep_input: ft_wire.keep_input,
                    insertions: exprs,
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

    let appended_fields = wire
        .appended_fields
        .iter()
        .map(|bytes| deserialize_kernel_expression(bytes).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Expression::StructPatch(ExpressionStructPatch {
        input_path,
        field_patches,
        prepended_fields,
        appended_fields,
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
        if let Some(ds_exec) = execution_plan.downcast_ref::<DataSourceExec>() {
            if let Some(file_conf) = ds_exec.data_source().downcast_ref::<FileScanConfig>() {
                let new_file_scan_config = FileScanConfigBuilder::from(file_conf.clone()).build();

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

        let filters = self
            .scan
            .contract
            .filters
            .iter()
            .map(|b| Expr::from_bytes_with_ctx(b, task).unwrap())
            .collect::<Vec<_>>();

        let row_index_column = self.scan.contract.row_index_column.clone();

        let contract = ProjectedScanContract::try_new(
            self.scan.contract.table_schema.clone(),
            self.scan
                .contract
                .provider_schema
                .as_ref()
                .unwrap_or(&self.scan.contract.table_schema)
                .clone(),
            &delta_scan_config,
            row_index_column.as_deref(),
            self.scan.contract.projection.as_ref(),
            &filters,
        )?;

        let skipping_predicate = self.scan.skipping_predicate.map(|osp| {
            osp.iter()
                .map(|b| Expr::from_bytes_with_ctx(b, task).unwrap())
                .collect::<Vec<_>>()
        });

        let scan_plan = KernelScanPlan::try_new_with_contract(
            &self.scan.snapshot,
            contract,
            &filters,
            &delta_scan_config,
            skipping_predicate,
        )?;

        let transforms = deserialize_transforms(self.transforms)?;

        let selection_vectors: DashMap<String, Vec<bool>> =
            self.selection_vectors.into_iter().collect();

        let mut public_file_ids = PublicFileIdMap::default();
        if scan_plan.contract.retain_file_id {
            for (k, v) in self.public_file_ids.iter() {
                public_file_ids.insert(k.clone(), v.clone());
            }
        }

        let exec = DeltaScanExec::new(
            Arc::new(scan_plan),
            execution_plan,
            Arc::new(transforms),
            Arc::new(selection_vectors),
            Arc::new(public_file_ids),
            Default::default(),
            Default::default(),
        );

        Ok(Arc::new(exec))
    }
}

/// Wire format for serializing [`DeltaScanMetaExec`].
///
/// Unlike [`DeltaScanExecWire`], there is no child plan: the per-partition
/// `(file_id, row_count)` inputs are serialized directly.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DeltaScanMetaExecWire {
    scan: ScanPlanWire,
    input: Vec<VecDeque<(String, usize)>>,
    transforms: TransformsWire,
    selection_vectors: std::collections::HashMap<String, Vec<bool>>,
    file_id_column: Option<String>,
    public_file_ids: std::collections::HashMap<String, String>,
}

impl TryFrom<&DeltaScanMetaExec> for DeltaScanMetaExecWire {
    type Error = DataFusionError;

    fn try_from(exec: &DeltaScanMetaExec) -> Result<Self, Self::Error> {
        let scan_plan_wire = scan_plan_wire(&exec.scan_plan)?;

        let transforms = serialize_transforms(&exec.transforms)?;

        let selection_vectors: std::collections::HashMap<String, Vec<bool>> = exec
            .selection_vectors
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let public_file_ids = exec
            .public_file_ids
            .as_ref()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<std::collections::HashMap<String, String>>();

        Ok(Self {
            scan: scan_plan_wire,
            input: exec.input.clone(),
            transforms,
            selection_vectors,
            file_id_column: exec.file_id_field.as_ref().map(|f| f.name().clone()),
            public_file_ids,
        })
    }
}

impl DeltaScanMetaExecWire {
    /// Reconstruct a [`DeltaScanMetaExec`] from the wire format.
    fn into_exec(
        self,
        inputs: &[Arc<dyn ExecutionPlan>],
        task: &TaskContext,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "DeltaScanMetaExec expects 0 inputs, got {}",
                inputs.len()
            )));
        }

        let mut delta_scan_config = DeltaScanConfig::new();
        if let Some(fic) = self.file_id_column {
            delta_scan_config = delta_scan_config.with_file_column_name(fic);
        }

        let filters = self
            .scan
            .contract
            .filters
            .iter()
            .map(|b| Expr::from_bytes_with_ctx(b, task).unwrap())
            .collect::<Vec<_>>();

        let row_index_column = self.scan.contract.row_index_column.clone();

        let contract = ProjectedScanContract::try_new(
            self.scan.contract.table_schema.clone(),
            self.scan
                .contract
                .provider_schema
                .as_ref()
                .unwrap_or(&self.scan.contract.table_schema)
                .clone(),
            &delta_scan_config,
            row_index_column.as_deref(),
            self.scan.contract.projection.as_ref(),
            &filters,
        )?;

        let file_id_field = contract
            .retain_file_id
            .then(|| contract.file_id_field.clone());

        let skipping_predicate = self.scan.skipping_predicate.map(|osp| {
            osp.iter()
                .map(|b| Expr::from_bytes_with_ctx(b, task).unwrap())
                .collect::<Vec<_>>()
        });

        let scan_plan = KernelScanPlan::try_new_with_contract(
            &self.scan.snapshot,
            contract,
            &filters,
            &delta_scan_config,
            skipping_predicate,
        )?;

        let transforms = deserialize_transforms(self.transforms)?;

        let selection_vectors: DashMap<String, Vec<bool>> =
            self.selection_vectors.into_iter().collect();

        let mut public_file_ids = PublicFileIdMap::default();
        if scan_plan.contract.retain_file_id {
            for (k, v) in self.public_file_ids.iter() {
                public_file_ids.insert(k.clone(), v.clone());
            }
        }

        let exec = DeltaScanMetaExec::new(
            Arc::new(scan_plan),
            self.input,
            Arc::new(transforms),
            Arc::new(selection_vectors),
            Arc::new(public_file_ids),
            file_id_field,
            ExecutionPlanMetricsSet::new(),
        );

        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::joins::CrossJoinExec;
    use datafusion::prelude::{col, lit};
    use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
    use datafusion_proto::protobuf;

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
        plan.downcast_ref::<DeltaScanExec>()
    }

    /// Build a metadata-only scan by projecting no data columns.
    async fn create_delta_scan_meta_exec_from_table(
        table: TestTables,
        filters: &[Expr],
    ) -> TestResult<Arc<dyn ExecutionPlan>> {
        let log_store = table.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::builder().with_snapshot(snapshot).await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let empty_projection = vec![];
        let plan = provider
            .scan(&state, Some(&empty_projection), filters, None)
            .await?;
        Ok(plan)
    }

    fn extract_delta_scan_meta_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&DeltaScanMetaExec> {
        plan.downcast_ref::<DeltaScanMetaExec>()
    }

    fn test_transform(value: i32, reverse_fields: bool) -> Arc<Expression> {
        use delta_kernel::expressions::Scalar;

        let mut fields = [
            (
                "a",
                ExpressionFieldPatch {
                    keep_input: false,
                    insertions: vec![Arc::new(Expression::Literal(Scalar::Integer(value)))],
                    optional: true,
                },
            ),
            (
                "b",
                ExpressionFieldPatch {
                    keep_input: true,
                    insertions: vec![],
                    optional: false,
                },
            ),
        ];
        if reverse_fields {
            fields.reverse();
        }
        Arc::new(Expression::StructPatch(ExpressionStructPatch {
            input_path: Some(ColumnName::new(["nested"])),
            field_patches: fields
                .into_iter()
                .map(|(name, patch)| (name.to_string(), patch))
                .collect(),
            prepended_fields: vec![Arc::new(Expression::Column(ColumnName::new(["first"])))],
            appended_fields: vec![Arc::new(Expression::Literal(Scalar::Integer(42)))],
        }))
    }

    #[test]
    fn test_transforms_dedup_and_deterministic_order() -> TestResult {
        let entries = [
            ("a".to_string(), test_transform(1, false)),
            ("b".to_string(), test_transform(1, true)),
            ("c".to_string(), test_transform(2, false)),
        ];
        let transforms = entries.clone().into_iter().collect();
        let wire = serialize_transforms(&transforms)?;
        assert_eq!(wire.dedupped_expressions.len(), 2);
        assert_eq!(
            wire.transforms,
            BTreeMap::from([
                ("a".to_string(), 0),
                ("b".to_string(), 0),
                ("c".to_string(), 1),
            ])
        );
        let reversed = entries.into_iter().rev().collect();
        assert_eq!(
            serde_json::to_vec(&wire)?,
            serde_json::to_vec(&serialize_transforms(&reversed)?)?,
        );
        let decoded = deserialize_transforms(serde_json::from_slice(&serde_json::to_vec(&wire)?)?)?;
        assert_eq!(decoded, transforms);
        assert!(Arc::ptr_eq(&decoded["a"], &decoded["b"]));
        assert!(!Arc::ptr_eq(&decoded["a"], &decoded["c"]));
        Ok(())
    }

    #[test]
    fn test_transforms_invalid_references() -> TestResult {
        let value = serialize_transform(&test_transform(1, false))?;
        for (values, index) in [
            (vec![], 0),
            (vec![value.clone()], 1),
            (vec![value], usize::MAX),
        ] {
            let wire = TransformsWire {
                dedupped_expressions: values,
                transforms: BTreeMap::from([("bad-file".to_string(), index)]),
            };
            let wire = serde_json::from_slice(&serde_json::to_vec(&wire)?)?;
            let error = deserialize_transforms(wire).unwrap_err().to_string();
            assert!(error.contains(&format!("Invalid transform index {index}")));
            assert!(error.contains("bad-file"));
        }
        for json in [r#"[[], {"bad-file": -1}]"#, r#"[[], {"bad-file": 0.5}]"#] {
            assert!(serde_json::from_str::<TransformsWire>(json).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_schema_omission_scan_meta_roundtrip() -> TestResult {
        let plans = [
            create_delta_scan_exec(&[], None).await?,
            create_delta_scan_meta_exec_from_table(TestTables::WithColumnMapping, &[]).await?,
        ];
        let session = create_session().into_inner();
        let task = session.task_ctx();
        let codec = DeltaNextPhysicalCodec;
        let transforms: HashMap<_, _> = [
            ("a".to_string(), test_transform(1, false)),
            ("b".to_string(), test_transform(1, true)),
            ("c".to_string(), test_transform(2, false)),
        ]
        .into_iter()
        .collect();

        for plan in plans {
            let (mut wire, tag) = if let Some(scan) = extract_delta_scan_exec(&plan) {
                (
                    ScanExecWire::Scan(DeltaScanExecWire::try_from(scan)?),
                    "Scan",
                )
            } else {
                let meta = extract_delta_scan_meta_exec(&plan).expect("Expected metadata scan");
                (
                    ScanExecWire::Meta(DeltaScanMetaExecWire::try_from(meta)?),
                    "Meta",
                )
            };
            assert!(serde_json::to_value(&wire)?[tag]["transforms"].is_object());
            let original_scan_plan = if let Some(scan) = extract_delta_scan_exec(&plan) {
                &scan.scan_plan
            } else {
                &extract_delta_scan_meta_exec(&plan).unwrap().scan_plan
            };
            let mut scan_plan = original_scan_plan.as_ref().clone();
            scan_plan.contract.provider_schema =
                Arc::new(scan_plan.contract.table_schema.as_ref().clone());
            assert!(!Arc::ptr_eq(
                &scan_plan.contract.table_schema,
                &scan_plan.contract.provider_schema
            ));
            let scan_wire = scan_plan_wire(&scan_plan)?;
            assert!(scan_wire.contract.provider_schema.is_none());
            match &mut wire {
                ScanExecWire::Scan(wire) => {
                    wire.scan = scan_wire;
                    wire.transforms = serialize_transforms(&transforms)?;
                }
                ScanExecWire::Meta(wire) => {
                    wire.scan = scan_wire;
                    wire.transforms = serialize_transforms(&transforms)?;
                }
            }
            let mut json = serde_json::to_value(&wire)?;
            assert!(
                json[tag]["scan"]["contract"]
                    .get("provider_schema")
                    .is_none()
            );
            let inputs = plan.children().into_iter().cloned().collect::<Vec<_>>();
            let decoded = codec.try_decode(&serde_json::to_vec(&json)?, &inputs, &task)?;
            let (contract, decoded_transforms) =
                if let Some(scan) = extract_delta_scan_exec(&decoded) {
                    (&scan.scan_plan.contract, &scan.transforms)
                } else {
                    let meta = extract_delta_scan_meta_exec(&decoded).unwrap();
                    (&meta.scan_plan.contract, &meta.transforms)
                };
            assert_eq!(
                serde_json::to_value(&contract.provider_schema)?,
                serde_json::to_value(&scan_plan.contract.provider_schema)?
            );
            assert_eq!(decoded_transforms.as_ref(), &transforms);
                assert!(Arc::ptr_eq(
                    &decoded_transforms["a"],
                    &decoded_transforms["b"]
                ));
        }
        Ok(())
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_provider_schema_differences_are_retained() -> TestResult {
        use arrow::datatypes::{DataType, Field, Schema};

        let plan = create_delta_scan_exec(&[], None).await?;
        let mut scan_plan = extract_delta_scan_exec(&plan)
            .unwrap()
            .scan_plan
            .as_ref()
            .clone();
        let nested_field = Field::new("child", DataType::Int32, true);
        let nested_schema = |field: Field| {
            Arc::new(Schema::new(vec![Field::new(
                "parent",
                DataType::Struct(vec![field].into()),
                true,
            )]))
        };
        let dictionary_schema = |id, ordered| {
            Arc::new(Schema::new(vec![Field::new_dict(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
                id,
                ordered,
            )]))
        };
        let cases = [
            (
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
            ),
            (
                nested_schema(nested_field.clone()),
                nested_schema(
                    nested_field.with_metadata(std::collections::HashMap::from([(
                        "key".to_string(),
                        "value".to_string(),
                    )])),
                ),
            ),
            (dictionary_schema(0, false), dictionary_schema(1, false)),
            (dictionary_schema(0, false), dictionary_schema(0, true)),
        ];
        for (table_schema, provider_schema) in cases {
            scan_plan.contract.table_schema = table_schema;
            scan_plan.contract.provider_schema = provider_schema.clone();
            let wire = scan_plan_wire(&scan_plan)?;
            assert!(wire.contract.provider_schema.is_some());
            let json = serde_json::to_value(&wire.contract)?;
            assert_eq!(
                json["provider_schema"],
                serde_json::to_value(&provider_schema)?
            );
            let decoded: ProjectedScanContractWire = serde_json::from_value(json)?;
            assert_eq!(
                serde_json::to_value(decoded.provider_schema.unwrap())?,
                serde_json::to_value(provider_schema)?
            );
        }
        Ok(())
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
            delta_scan.file_id_column, decoded_delta_scan.file_id_column,
            "File ID columns should match"
        );
        assert_eq!(
            delta_scan.input_file_id_column, decoded_delta_scan.input_file_id_column,
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
            decoded_delta_scan
                .scan_plan
                .contract
                .result_schema
                .fields()
                .len(),
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

        let meta_plan =
            create_delta_scan_meta_exec_from_table(TestTables::WithColumnMapping, &[]).await?;
        let mut meta_buf = Vec::new();
        codec.try_encode(meta_plan.clone(), &mut meta_buf)?;

        let result = codec.try_decode(&meta_buf, &[meta_plan], &task_ctx);
        assert!(
            result.is_err(),
            "DeltaScanMetaExec should fail with 1 input"
        );

        Ok(())
    }

    #[test]
    fn test_kernel_expression_serialization_roundtrip() {
        use delta_kernel::expressions::{ColumnName, Expression as KernelExpression, Scalar};

        let column_expr = KernelExpression::Column(ColumnName::new(["test_column"]));
        let serialized = serialize_kernel_expression(&column_expr).unwrap();
        let deserialized = deserialize_kernel_expression(&serialized).unwrap();
        assert_eq!(
            column_expr, deserialized,
            "Column expression should roundtrip"
        );

        let literal_expr = KernelExpression::Literal(Scalar::Integer(42));
        let serialized = serialize_kernel_expression(&literal_expr).unwrap();
        let deserialized = deserialize_kernel_expression(&serialized).unwrap();
        assert_eq!(
            literal_expr, deserialized,
            "Literal expression should roundtrip"
        );

        let string_literal = KernelExpression::Literal(Scalar::String("hello".to_string()));
        let serialized = serialize_kernel_expression(&string_literal).unwrap();
        let deserialized = deserialize_kernel_expression(&serialized).unwrap();
        assert_eq!(
            string_literal, deserialized,
            "String literal should roundtrip"
        );
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
            delta_scan.transforms, decoded_delta_scan.transforms,
            "Transform values must match"
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
        let plan = create_delta_scan_exec_from_table(TestTables::WithDvSmall, &[], None).await?;
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
            delta_scan.transforms, decoded_delta_scan.transforms,
            "Transform values must match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_codec_roundtrip_basic() -> TestResult {
        let plan =
            create_delta_scan_meta_exec_from_table(TestTables::WithColumnMapping, &[]).await?;
        let meta_scan = extract_delta_scan_meta_exec(&plan).expect("Expected DeltaScanMetaExec");

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;
        assert!(!buf.is_empty(), "Encoded buffer should not be empty");

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let decoded = codec.try_decode(&buf, &[], &task_ctx)?;
        let decoded_meta = extract_delta_scan_meta_exec(&decoded)
            .expect("Expected DeltaScanMetaExec after decode");

        assert_eq!(
            meta_scan.scan_plan.contract.result_schema,
            decoded_meta.scan_plan.contract.result_schema,
            "Result schemas should match"
        );
        assert_eq!(
            meta_scan.input, decoded_meta.input,
            "Per-file (file_id, row_count) inputs should match"
        );
        assert_eq!(
            meta_scan.partition_statistics(None)?.num_rows,
            decoded_meta.partition_statistics(None)?.num_rows,
            "Exact row counts should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_codec_roundtrip_with_filter() -> TestResult {
        // Filter on the partition column so pushdown is Exact and the scan stays
        // metadata-only (otherwise data must be read, yielding a DeltaScanExec).
        let filters = vec![col("Company Very Short").eq(lit("BMS"))];
        let plan =
            create_delta_scan_meta_exec_from_table(TestTables::WithColumnMapping, &filters).await?;
        let meta_scan = extract_delta_scan_meta_exec(&plan).expect("Expected DeltaScanMetaExec");

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let decoded = codec.try_decode(&buf, &[], &task_ctx)?;
        let decoded_meta = extract_delta_scan_meta_exec(&decoded)
            .expect("Expected DeltaScanMetaExec after decode");

        assert_eq!(
            meta_scan.scan_plan.contract.result_schema,
            decoded_meta.scan_plan.contract.result_schema,
            "Result schemas should match with filter"
        );
        assert_eq!(
            meta_scan.partition_statistics(None)?.num_rows,
            decoded_meta.partition_statistics(None)?.num_rows,
            "Exact row counts should match with filter"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_codec_roundtrip_with_deletion_vectors() -> TestResult {
        let plan = create_delta_scan_meta_exec_from_table(TestTables::WithDvSmall, &[]).await?;
        let meta_scan = extract_delta_scan_meta_exec(&plan).expect("Expected DeltaScanMetaExec");
        assert!(
            !meta_scan.selection_vectors.is_empty(),
            "Table with deletion vectors should have non-empty selection_vectors"
        );

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let decoded = codec.try_decode(&buf, &[], &task_ctx)?;
        let decoded_meta = extract_delta_scan_meta_exec(&decoded)
            .expect("Expected DeltaScanMetaExec after decode");

        assert_eq!(
            meta_scan.selection_vectors.len(),
            decoded_meta.selection_vectors.len(),
            "Selection vectors count should match"
        );
        for entry in meta_scan.selection_vectors.iter() {
            let decoded_vec = decoded_meta
                .selection_vectors
                .get(entry.key())
                .expect("Decoded should have same keys");
            assert_eq!(
                entry.value().as_slice(),
                decoded_vec.value().as_slice(),
                "Selection vector values should match for key {}",
                entry.key()
            );
        }
        assert_eq!(
            meta_scan.partition_statistics(None)?.num_rows,
            decoded_meta.partition_statistics(None)?.num_rows,
            "Exact row counts should match with deletion vectors"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_codec_roundtrip_with_transforms() -> TestResult {
        let plan =
            create_delta_scan_meta_exec_from_table(TestTables::WithColumnMapping, &[]).await?;
        let meta_scan = extract_delta_scan_meta_exec(&plan).expect("Expected DeltaScanMetaExec");

        let codec = DeltaNextPhysicalCodec;

        let mut buf = Vec::new();
        codec.try_encode(plan.clone(), &mut buf)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();

        let decoded = codec.try_decode(&buf, &[], &task_ctx)?;
        let decoded_meta = extract_delta_scan_meta_exec(&decoded)
            .expect("Expected DeltaScanMetaExec after decode");

        assert_eq!(
            meta_scan.transforms, decoded_meta.transforms,
            "Transform values must match"
        );

        Ok(())
    }

    /// Test with a plan that has both DeltaScanMetaExec and DeltaScanExec
    async fn join_roundtrip(meta_first: bool) -> TestResult {
        let scan_plan = create_delta_scan_exec_from_table(TestTables::Simple, &[], None).await?;
        let meta_plan =
            create_delta_scan_meta_exec_from_table(TestTables::WithColumnMapping, &[]).await?;

        let join: Arc<dyn ExecutionPlan> = if meta_first {
            Arc::new(CrossJoinExec::new(meta_plan.clone(), scan_plan.clone()))
        } else {
            Arc::new(CrossJoinExec::new(scan_plan.clone(), meta_plan.clone()))
        };

        let codec = DeltaNextPhysicalCodec;
        let proto = protobuf::PhysicalPlanNode::try_from_physical_plan(join.clone(), &codec)?;

        let session = create_session().into_inner();
        let task_ctx = session.task_ctx();
        let decoded = proto.try_into_physical_plan(&task_ctx, &codec)?;

        let decoded_children = decoded.children();
        assert_eq!(decoded_children.len(), 2, "Join should have two children");

        let (meta_idx, scan_idx) = if meta_first { (0, 1) } else { (1, 0) };
        assert!(
            decoded_children[meta_idx]
                .downcast_ref::<DeltaScanMetaExec>()
                .is_some(),
            "Child {meta_idx} should decode to DeltaScanMetaExec"
        );
        assert!(
            decoded_children[scan_idx]
                .downcast_ref::<DeltaScanExec>()
                .is_some(),
            "Child {scan_idx} should decode to DeltaScanExec"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_codec_roundtrip_join_meta_first() -> TestResult {
        join_roundtrip(true).await
    }

    #[tokio::test]
    async fn test_codec_roundtrip_join_meta_last() -> TestResult {
        join_roundtrip(false).await
    }
}
