use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use chrono::{DateTime, TimeZone, Utc};
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::{Column, Result, ScalarValue, Statistics, ToDFSchema};
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::TableType;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, TableProviderFilterPushDown};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::{FileExtensions, PartitionedFile, TableSchema};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use object_store::ObjectMeta;
use url::Url;
use crate::delta_datafusion::{get_null_of_arrow_type, to_correct_scalar_value, DataFusionMixins, DeltaScanConfig, DeltaScanConfigBuilder};
use crate::delta_datafusion::table_provider::{simplify_expr, DeltaScan};
use crate::{DeltaResult, DeltaTable, DeltaTableConfig, DeltaTableError};
use crate::delta_datafusion::file_id::{file_id_data_type, wrap_file_id_value};
use crate::kernel::{Add, EagerSnapshot};
use crate::kernel::transaction::PROTOCOL;
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;


pub(crate) fn get_pushdown_filters(
    filter: &[&Expr],
    partition_cols: &[String],
) -> Vec<TableProviderFilterPushDown> {
    filter
        .iter()
        .cloned()
        .map(|expr| {
            let applicable = expr_is_exact_predicate_for_cols(partition_cols, expr);
            if !expr.column_refs().is_empty() && applicable {
                TableProviderFilterPushDown::Exact
            } else {
                TableProviderFilterPushDown::Inexact
            }
        })
        .collect()
}

// inspired from datafusion::listing::helpers, but adapted to only stats based pruning
fn expr_is_exact_predicate_for_cols(partition_cols: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.apply(|expr| match expr {
        Expr::Column(Column { name, .. }) => {
            is_applicable &= partition_cols.contains(name);

            // TODO: decide if we should constrain this to Utf8 columns (including views, dicts etc)

            if is_applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::BinaryExpr(BinaryExpr { op, .. }) => {
            is_applicable &= matches!(
                op,
                Operator::And
                    | Operator::Or
                    | Operator::NotEq
                    | Operator::Eq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
            );
            if is_applicable {
                Ok(TreeNodeRecursion::Continue)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::Literal(_, _)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::Between(_)
        | Expr::InList(_) => Ok(TreeNodeRecursion::Continue),
        _ => {
            is_applicable = false;
            Ok(TreeNodeRecursion::Stop)
        }
    })
        .unwrap();
    is_applicable
}

fn partitioned_file_from_action(
    action: &Add,
    partition_columns: &[String],
    schema: &Schema,
) -> PartitionedFile {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            action
                .partition_values
                .get(part)
                .map(|val| {
                    schema
                        .field_with_name(part)
                        .map(|field| match val {
                            Some(value) => to_correct_scalar_value(
                                &serde_json::Value::String(value.to_string()),
                                field.data_type(),
                            )
                                .unwrap_or(Some(ScalarValue::Null))
                                .unwrap_or(ScalarValue::Null),
                            None => get_null_of_arrow_type(field.data_type())
                                .unwrap_or(ScalarValue::Null),
                        })
                        .unwrap_or(ScalarValue::Null)
                })
                .unwrap_or(ScalarValue::Null)
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    let last_modified = Utc.from_utc_datetime(
        &DateTime::from_timestamp(ts_secs, ts_ns as u32)
            .unwrap()
            .naive_utc(),
    );
    PartitionedFile {
        object_meta: ObjectMeta {
            last_modified,
            ..action.try_into().unwrap()
        },
        partition_values,
        range: None,
        statistics: None,
        ordering: None,
        extensions: FileExtensions::new(),
        metadata_size_hint: None,
        table_reference: None,
    }
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
pub(crate) fn df_logical_schema(
    snapshot: &EagerSnapshot,
    file_column_name: &Option<String>,
    schema: Option<SchemaRef>,
) -> DeltaResult<SchemaRef> {
    let input_schema = match schema {
        Some(schema) => schema,
        None => snapshot.input_schema(),
    };
    let table_partition_cols = snapshot.metadata().partition_columns();

    let mut fields: Vec<Arc<Field>> = input_schema
        .fields()
        .iter()
        .filter(|f| !table_partition_cols.contains(f.name()))
        .cloned()
        .collect();

    for partition_col in table_partition_cols.iter() {
        fields.push(Arc::new(
            input_schema
                .field_with_name(partition_col)?
                .to_owned(),
        ));
    }

    if let Some(file_column_name) = file_column_name {
        fields.push(Arc::new(Field::new(file_column_name, DataType::Utf8, true)));
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) struct DeltaScanBuilder<'a> {
    snapshot: &'a EagerSnapshot,
    log_store: LogStoreRef,
    filter: Option<Expr>,
    session: &'a dyn Session,
    projection: Option<&'a Vec<usize>>,
    limit: Option<usize>,
    files: Option<&'a [Add]>,
    config: Option<DeltaScanConfig>,
}

impl<'a> DeltaScanBuilder<'a> {
    pub fn new(
        snapshot: &'a EagerSnapshot,
        log_store: LogStoreRef,
        session: &'a dyn Session,
    ) -> Self {
        DeltaScanBuilder {
            snapshot,
            log_store,
            filter: None,
            session,
            projection: None,
            limit: None,
            files: None,
            config: None,
        }
    }

    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_files(mut self, files: &'a [Add]) -> Self {
        self.files = Some(files);
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub async fn build(self) -> DeltaResult<DeltaScan> {
        PROTOCOL.can_read_from(self.snapshot)?;
        let config = match self.config {
            Some(config) => config,
            None => DeltaScanConfigBuilder::new().build(self.snapshot)?,
        };

        let schema = match config.schema.clone() {
            Some(value) => value,
            None => self.snapshot.read_schema(),
        };

        let logical_schema = df_logical_schema(
            self.snapshot,
            &config.file_column_name,
            Some(schema.clone()),
        )?;

        let logical_schema = if let Some(used_columns) = self.projection {
            let mut fields = Vec::with_capacity(used_columns.len());
            for idx in used_columns {
                fields.push(logical_schema.field(*idx).to_owned());
            }
            // partition filters with Exact pushdown were removed from projection by DF optimizer,
            // we need to add them back for the predicate pruning to work
            if let Some(expr) = &self.filter {
                // Avoid non-determinism of HashSet (affects the expressions in pushed-down filters)
                let column_refs = expr.column_refs().into_iter().collect::<BTreeSet<_>>();
                for c in column_refs {
                    let idx = logical_schema.index_of(c.name.as_str())?;
                    if !used_columns.contains(&idx) {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            Arc::new(Schema::new(fields))
        } else {
            logical_schema
        };

        let df_schema = Arc::new(logical_schema.clone().to_dfschema()?);

        let logical_filter = self
            .filter
            .clone()
            .map(|expr| simplify_expr(self.session, df_schema.clone(), expr))
            .transpose()?;
        // only inexact filters should be pushed down to the data source, doing otherwise
        // will make stats inexact and disable datafusion optimizations like AggregateStatistics
        let pushdown_filter = self
            .filter
            .and_then(|expr| {
                let predicates = split_conjunction(&expr);
                let pushdown_filters =
                    get_pushdown_filters(&predicates, self.snapshot.metadata().partition_columns());

                let filtered_predicates = predicates
                    .into_iter()
                    .zip(pushdown_filters.into_iter())
                    .filter_map(|(filter, pushdown)| {
                        if pushdown == TableProviderFilterPushDown::Inexact {
                            Some(filter.clone())
                        } else {
                            None
                        }
                    });
                conjunction(filtered_predicates)
            })
            .map(|expr| simplify_expr(self.session, df_schema.clone(), expr))
            .transpose()?;

        // Perform Pruning of files to scan
        let (files, files_scanned, files_pruned, _) = match self.files {
            Some(files) => {
                let files = files.to_owned();
                let files_scanned = files.len();
                (files, files_scanned, 0, None)
            }
            None => {
                // early return in case we have no push down filters or limit
                if logical_filter.is_none() && self.limit.is_none() {
                    // let files = self
                    //     .snapshot
                    //     .file_views(&self.log_store, None)
                    //     .map_ok(|f| f.to_add())
                    //     .try_collect::<Vec<_>>()
                    //     .await?;
                    let files = self
                        .snapshot
                        .log_data()
                        .iter()
                        .map(|f| f.to_add())
                        .collect::<Vec<_>>();

                    let files_scanned = files.len();
                    (files, files_scanned, 0, None)
                } else {
                    let num_containers = self.snapshot.num_containers();

                    let files_to_prune = if let Some(predicate) = &logical_filter {
                        let pruning_predicate =
                            PruningPredicate::try_new(predicate.clone(), logical_schema.clone())?;
                        pruning_predicate.prune(self.snapshot)?
                    } else {
                        vec![true; num_containers]
                    };

                    // needed to enforce limit and deal with missing statistics
                    // rust port of https://github.com/delta-io/delta/pull/1495
                    let mut pruned_without_stats = Vec::new();
                    let mut rows_collected = 0;
                    let mut files = Vec::with_capacity(num_containers);

                    // let file_actions: Vec<_> = self
                    //     .snapshot
                    //     .file_views(&self.log_store, None)
                    //     .map_ok(|f| f.to_add())
                    //     .try_collect::<Vec<_>>()
                    //     .await?;

                    use rand::seq::SliceRandom;

                    let mut indices = (0..num_containers).collect::<Vec<_>>();
                    if self.limit.is_some() && std::env::var("DELTA_RS_SHUFFLE_FILES").is_ok() {
                        let mut rng = rand::rng();
                        indices.shuffle(&mut rng);
                    }

                    let log_data_handler = self.snapshot.log_data();
                    for i in indices {
                        let file_view = log_data_handler.get(i).unwrap();
                        let keep = files_to_prune[i];

                        // prune file based on predicate pushdown
                        let action = file_view.add_action_no_stats();
                        let num_records = file_view.num_records();
                        if keep {
                            // prune file based on limit pushdown
                            if let Some(limit) = self.limit {
                                if let Some(num_records) = num_records {
                                    if rows_collected < limit as i64 {
                                        rows_collected += num_records as i64;
                                        files.push(action.to_owned());
                                    } else {
                                        break;
                                    }
                                } else {
                                    // some files are missing stats; skipping but storing them
                                    // in a list in case we can't reach the target limit
                                    pruned_without_stats.push(action.to_owned());
                                }
                            } else {
                                files.push(action.to_owned());
                            }
                        }
                    }

                    if let Some(limit) = self.limit
                        && rows_collected < limit as i64
                    {
                        files.extend(pruned_without_stats);
                    }

                    let files_scanned = files.len();
                    let files_pruned = num_containers - files_scanned;
                    (files, files_scanned, files_pruned, Some(files_to_prune))
                }
            }
        };

        // TODO we group files together by their partition values. If the table is partitioned
        // and partitions are somewhat evenly distributed, probably not the worst choice ...
        // However we may want to do some additional balancing in case we are far off from the above.
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

        let table_partition_cols = &self.snapshot.metadata().partition_columns();

        for action in files.iter() {
            let mut part = partitioned_file_from_action(action, table_partition_cols, &schema);

            if config.file_column_name.is_some() {
                let partition_value = if config.wrap_partition_values {
                    wrap_file_id_value(action.path.clone())
                } else {
                    ScalarValue::Utf8(Some(action.path.clone()))
                };
                part.partition_values.push(partition_value);
            }

            file_groups
                .entry(part.partition_values.clone())
                .or_default()
                .push(part);
        }

        let file_schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<arrow::datatypes::FieldRef>>(),
        ));

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|name| schema.field_with_name(name).map(|f| f.to_owned()))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        if let Some(file_column_name) = &config.file_column_name {
            let field_name_datatype = if config.wrap_partition_values {
                file_id_data_type()
            } else {
                DataType::Utf8
            };
            table_partition_cols.push(Field::new(
                file_column_name.clone(),
                field_name_datatype,
                false,
            ));
        }

        let parquet_options = TableParquetOptions {
            global: self.session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let partition_fields: Vec<Arc<Field>> =
            table_partition_cols.into_iter().map(Arc::new).collect();
        let table_schema = TableSchema::new(file_schema, partition_fields);

        let mut file_source =
            ParquetSource::new(table_schema).with_table_parquet_options(parquet_options);

        // Sometimes (i.e Merge) we want to prune files that don't make the
        // filter and read the entire contents for files that do match the
        // filter
        if let Some(predicate) = pushdown_filter
            && config.enable_parquet_pushdown
        {
            file_source = file_source.with_predicate(predicate);
        };

        let file_scan_config =
            FileScanConfigBuilder::new(self.log_store.object_store_url(), Arc::new(file_source))
                .with_file_groups(
                    // If all files were filtered out, we still need to emit at least one partition to
                    // pass datafusion sanity checks.
                    //
                    // See https://github.com/apache/datafusion/issues/11322
                    if file_groups.is_empty() {
                        vec![FileGroup::from(vec![])]
                    } else {
                        file_groups.into_values().map(FileGroup::from).collect()
                    },
                )
                .with_projection_indices(self.projection.cloned())?
                .with_limit(self.limit)
                .build();

        let metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics)
            .global_counter("files_scanned")
            .add(files_scanned);
        MetricBuilder::new(&metrics)
            .global_counter("files_pruned")
            .add(files_pruned);

        Ok(DeltaScan {
            table_url: self.log_store.root_url().clone(),
            config,
            parquet_scan: DataSourceExec::from_data_source(file_scan_config),
            logical_schema,
            metrics,
        })
    }
}



impl DeltaTable {
    pub fn table_provider_old(&self) -> DeltaTableOldProvider {
        self.clone().into()
    }
}

// each delta table must register a specific object store, since paths are internally
// handled relative to the table root.
pub(crate) fn register_store(store: LogStoreRef, env: &RuntimeEnv) {
    let object_store_url = store.object_store_url();
    let url: &Url = object_store_url.as_ref();
    env.register_object_store(url, store.object_store(None));
}

#[derive(Debug, Clone)]
pub struct DeltaTableOldProvider {
    /// The state of the table as of the most recent loaded Delta log entry.
    pub state: Option<DeltaTableState>,
    /// the load options used during load
    pub config: DeltaTableConfig,
    /// log store
    pub(crate) log_store: LogStoreRef,
    /// Optional schema override for scanning
    pub(crate) schema: Option<SchemaRef>,
}

impl DeltaTableOldProvider {
    pub fn snapshot(&self) -> DeltaResult<&DeltaTableState> {
        self.state.as_ref().ok_or(DeltaTableError::NotInitialized)
    }
    pub fn log_store(&self) -> LogStoreRef {
        self.log_store.clone()
    }
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

impl From<DeltaTable> for DeltaTableOldProvider {
    fn from(value: DeltaTable) -> Self {
        Self {
            state: value.state.clone(),
            config: value.config.clone(),
            log_store: value.log_store.clone(),
            schema: None,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaTableOldProvider {
    fn schema(&self) -> Arc<Schema> {
        match &self.schema {
            Some(s) => df_logical_schema(
                self.snapshot().unwrap().snapshot(),
                &None,
                Some(s.clone()),
            )
            .unwrap_or_else(|_| s.clone()),
            None => self.snapshot().unwrap().snapshot().read_schema(),
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        _session: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("scan is not available for this table provider; use scan_with_args")
    }

    async fn scan_with_args<'a>(&self, state: &dyn Session, args: ScanArgs<'a>) -> Result<ScanResult> {
        register_store(self.log_store(), state.runtime_env().as_ref());
        let filters = args.filters().unwrap_or(&[]);
        let filter_expr = conjunction(filters.iter().cloned());

        let config = DeltaScanConfigBuilder {
            include_file_column: false,
            file_column_name: None,
            wrap_partition_values: None,
            enable_parquet_pushdown: true,
            schema: self.schema.clone(),
        };

        let config = config
            .build(self.snapshot()?.snapshot())?;

        let projection = args.projection().map(|p| p.to_vec());
        let scan = DeltaScanBuilder::new(self.snapshot()?.snapshot(), self.log_store(), state)
            .with_projection(projection.as_ref())
            .with_limit(args.limit())
            .with_filter(filter_expr)
            .with_scan_config(config)
            .build()
            .await?;

        Ok(ScanResult::new(Arc::new(scan)))
    }
    
    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot()?.metadata().partition_columns();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        // HSTACK: removed in upstream as well
        None
    }
}
