use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::scan::{
    Scan as KernelScan, ScanBuilder as KernelScanBuilder, ScanMetadata,
    StatsOptions,
};
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::{Engine, EngineData, PredicateRef, SnapshotRef, Version};
use futures::Stream;
use futures::future::ready;
use futures::stream::once;
use url::Url;

#[cfg(feature = "datafusion")]
use super::MaterializedFiles;
use super::stats_projection::{
    FileStatsMaterialization, RawStatsPolicy, StatsProjection, StatsSourcePolicy,
};
use crate::DeltaResult;
use crate::kernel::arrow::engine_ext::SnapshotExt;
use crate::kernel::{CachedScanRowEvaluator, ReceiverStreamBuilder};

/// A boxed, `Send`able stream of [`ScanMetadata`] results produced while scanning a snapshot.
pub type SendableScanMetadataStream = Pin<Box<dyn Stream<Item = DeltaResult<ScanMetadata>> + Send>>;

/// Builder to scan a snapshot of a table.
#[derive(Debug)]
pub struct ScanBuilder {
    snapshot: Arc<KernelSnapshot>,
    schema: Option<SchemaRef>,
    predicate: Option<PredicateRef>,
    stats_materialization: Option<FileStatsMaterialization>,
    checkpoint_stats_json_fallback: bool,
}

impl ScanBuilder {
    /// Create a new [`ScanBuilder`] instance.
    pub fn new(snapshot: impl Into<Arc<KernelSnapshot>>) -> Self {
        Self {
            snapshot: snapshot.into(),
            schema: None,
            predicate: None,
            stats_materialization: None,
            checkpoint_stats_json_fallback: false,
        }
    }

    pub(super) fn with_checkpoint_stats_json_fallback(mut self, enabled: bool) -> Self {
        self.checkpoint_stats_json_fallback = enabled;
        self
    }

    /// Provide [`Schema`] for columns to select from the [`Snapshot`].
    ///
    /// A table with columns `[a, b, c]` could have a scan which reads only the first
    /// two columns by using the schema `[a, b]`.
    ///
    /// [`Schema`]: crate::schema::Schema
    /// [`Snapshot`]: crate::snapshot::Snapshot
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Optionally provide a [`SchemaRef`] for columns to select from the [`Snapshot`]. See
    /// [`ScanBuilder::with_schema`] for details. If `schema_opt` is `None` this is a no-op.
    ///
    /// [`Snapshot`]: crate::Snapshot
    pub fn with_schema_opt(mut self, schema_opt: Option<SchemaRef>) -> Self {
        if let Some(schema) = schema_opt {
            self.schema = Some(schema);
        }
        self
    }

    /// Optionally provide an expression to filter rows. For example, using the predicate `x <
    /// 4` to return a subset of the rows in the scan which satisfy the filter. If `predicate_opt`
    /// is `None`, this is a no-op.
    ///
    /// NOTE: The filtering is best-effort and can produce false positives (rows that should should
    /// have been filtered out but were kept).
    pub fn with_predicate(mut self, predicate: impl Into<Option<PredicateRef>>) -> Self {
        self.predicate = predicate.into();
        self
    }

    /// Skip file statistics during kernel log replay.
    ///
    /// When `true`, min/max/null stats are not parsed and `stats_parsed` in scan output may
    /// be null. Partition filtering still applies. With a predicate, stats based data skipping
    /// is disabled. Use `false` when file pruning from statistics is required. Passing `false`
    /// clears any previous stats materialization override and restores default inference.
    pub fn with_skip_stats(mut self, skip_stats: bool) -> Self {
        if skip_stats {
            self.stats_materialization = Some(FileStatsMaterialization::without_stats());
        } else {
            self.stats_materialization = None;
        }
        self
    }

    /// Override the file statistics emitted from scan metadata.
    ///
    /// The policy controls parsed stats projection, parsed stats source, and raw JSON retention.
    pub(crate) fn with_stats_materialization(
        mut self,
        stats_materialization: FileStatsMaterialization,
    ) -> Self {
        self.stats_materialization = Some(stats_materialization);
        self
    }

    /// Finalize the builder into a [`Scan`], validating the configured schema and predicate.
    pub fn build(self) -> DeltaResult<Scan> {
        let Self {
            snapshot,
            schema,
            predicate,
            stats_materialization,
            checkpoint_stats_json_fallback,
        } = self;

        let stats_materialization = match stats_materialization {
            Some(stats_materialization) => stats_materialization,
            None => FileStatsMaterialization::query(StatsProjection::for_scan_inputs(
                snapshot.as_ref(),
                schema.as_ref(),
                predicate.as_ref(),
            )?),
        };

        // Modernization: Use kernel's StatsOptions when available
        // TODO: Add condition to use kernel StatsOptions API once fully integrated

        let inner = build_kernel_scan(
            snapshot,
            schema,
            predicate,
            Some(&stats_materialization),
            checkpoint_stats_json_fallback,
        )?;

        Ok(Scan::new(Arc::new(inner), stats_materialization))
    }

    /// Experimental: Use kernel's all_struct statistics mode (AFFECTS ULP).
    /// This is a preview of the modernization - use for experimentation.
    pub fn with_kernel_all_struct_stats(mut self) -> Self {
        // Conceptually: self.kernel_stats = StatsOptions::all_struct();
        // Placeholder implementation - sets appropriate legacy materialization
        self.stats_materialization = Some(FileStatsMaterialization::query(StatsProjection::Full));
        self
    }
}

fn build_kernel_scan(
    snapshot: Arc<KernelSnapshot>,
    schema: Option<SchemaRef>,
    predicate: Option<PredicateRef>,
    stats_materialization: Option<&FileStatsMaterialization>,
    checkpoint_stats_json_fallback: bool,
) -> DeltaResult<KernelScan> {
    let mut builder = KernelScanBuilder::new(snapshot)
        .with_schema_opt(schema)
        .with_predicate(predicate);

    if let Some(stats_materialization) = stats_materialization {
        builder = with_kernel_stats_output(
            builder,
            stats_materialization,
            checkpoint_stats_json_fallback,
        );
    }

    Ok(builder.build()?)
}

fn kernel_stats_options(
    materialization: &FileStatsMaterialization,
    checkpoint_stats_json_fallback: bool,
) -> StatsOptions {
    match (
        materialization.stats_source_policy(),
        materialization.raw_stats_policy(),
        materialization.stats_projection(),
    ) {
        (StatsSourcePolicy::None, _, _) | (_, _, StatsProjection::None) => StatsOptions::none(),
        (StatsSourcePolicy::ParsedWithJsonFallback, RawStatsPolicy::Preserve, _) => {
            StatsOptions::all_struct()
                .with_checkpoint_stats_json_fallback(checkpoint_stats_json_fallback)
        }
        (
            StatsSourcePolicy::ParsedWithJsonFallback,
            RawStatsPolicy::Omit,
            StatsProjection::Full,
        ) => StatsOptions::all_struct()
            .with_checkpoint_stats_json_fallback(checkpoint_stats_json_fallback),
        (
            StatsSourcePolicy::ParsedWithJsonFallback,
            RawStatsPolicy::Omit,
            StatsProjection::PredicateColumns(columns),
        ) => StatsOptions::struct_columns(columns.iter().cloned().collect())
            .with_checkpoint_stats_json_fallback(checkpoint_stats_json_fallback),
        (
            StatsSourcePolicy::ParsedWithJsonFallback,
            RawStatsPolicy::Omit,
            StatsProjection::NumRecordsOnly,
        ) => StatsOptions::struct_columns(vec![])
            .with_checkpoint_stats_json_fallback(checkpoint_stats_json_fallback),
    }
}

fn with_kernel_stats_output(
    builder: KernelScanBuilder,
    materialization: &FileStatsMaterialization,
    checkpoint_stats_json_fallback: bool,
) -> KernelScanBuilder {
    builder.with_stats(kernel_stats_options(
        materialization,
        checkpoint_stats_json_fallback,
    ))
}

#[cfg(test)]
#[path = "scan/stats_options_tests.rs"]
mod stats_options_tests;

#[cfg(test)]
mod tests {
    use delta_kernel::expressions::{ColumnName, Scalar};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::{Expression, PredicateRef};

    use super::super::stats_projection::{
        FileStatsMaterialization, StatsProjection, StatsSourcePolicy,
    };
    use super::*;
    use crate::DeltaTable;

    async fn synthetic_snapshot() -> DeltaResult<super::super::Snapshot> {
        let nested = StructType::try_new([
            StructField::nullable("leaf", DataType::INTEGER),
            StructField::nullable("other_leaf", DataType::STRING),
        ])?;
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([
                StructField::nullable("value", DataType::INTEGER),
                StructField::nullable("unreferenced_col", DataType::STRING),
                StructField::nullable("part", DataType::STRING),
                StructField::nullable("nested", DataType::Struct(Box::new(nested))),
            ])
            .with_partition_columns(["part"])
            .await?;
        super::super::Snapshot::try_new(table.log_store().as_ref(), Default::default(), None).await
    }

    fn assert_stats_options_eq(actual: StatsOptions, expected: StatsOptions) {
        assert_eq!(
            serde_json::to_value(actual).expect("actual StatsOptions should serialize"),
            serde_json::to_value(expected).expect("expected StatsOptions should serialize")
        );
    }

    #[tokio::test]
    async fn scan_builder_infers_num_records_only_for_default_query_scan() -> DeltaResult<()> {
        let snapshot = synthetic_snapshot().await?;
        let scan = snapshot.scan_builder().build()?;

        assert_eq!(
            scan.stats_materialization().stats_projection(),
            &StatsProjection::NumRecordsOnly
        );
        assert!(!scan.stats_materialization().preserves_raw_stats());
        assert_eq!(
            scan.stats_materialization().stats_source_policy(),
            StatsSourcePolicy::ParsedWithJsonFallback
        );

        Ok(())
    }

    #[tokio::test]
    async fn scan_builder_infers_predicate_columns_for_data_predicate() -> DeltaResult<()> {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef =
            Arc::new(Expression::column(["value"]).gt(Scalar::Integer(10)));
        let scan = snapshot.scan_builder().with_predicate(predicate).build()?;

        assert_eq!(
            scan.stats_materialization().stats_projection(),
            &StatsProjection::PredicateColumns([ColumnName::new(["value"])].into())
        );

        Ok(())
    }

    #[tokio::test]
    async fn scan_builder_rejects_predicate_on_unknown_column() -> DeltaResult<()> {
        let snapshot = synthetic_snapshot().await?;
        let predicate: PredicateRef =
            Arc::new(Expression::column(["missing"]).gt(Scalar::Integer(10)));

        let err = snapshot
            .scan_builder()
            .with_predicate(predicate)
            .build()
            .expect_err("predicate on an unknown column should fail scan planning");

        assert!(
            err.to_string().to_lowercase().contains("missing")
                || err.to_string().to_lowercase().contains("unknown"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn scan_builder_preserves_explicit_compatibility_materialization() -> DeltaResult<()> {
        let snapshot = synthetic_snapshot().await?;
        let materialization = FileStatsMaterialization::compatibility(StatsProjection::full());
        let scan = snapshot
            .scan_builder()
            .with_stats_materialization(materialization.clone())
            .build()?;

        assert_eq!(scan.stats_materialization(), &materialization);
        assert!(scan.stats_materialization().preserves_raw_stats());

        Ok(())
    }

    #[test]
    fn scan_builder_compatibility_materialization_maps_to_all_struct_stats() {
        let materialization = FileStatsMaterialization::compatibility(StatsProjection::full());

        assert_stats_options_eq(
            kernel_stats_options(&materialization, false),
            StatsOptions::all_struct().with_checkpoint_stats_json_fallback(false),
        );
    }

    #[tokio::test]
    async fn scan_builder_preserves_explicit_without_stats_materialization() -> DeltaResult<()> {
        let snapshot = synthetic_snapshot().await?;
        let scan = snapshot
            .scan_builder()
            .with_stats_materialization(FileStatsMaterialization::without_stats())
            .build()?;

        assert_eq!(
            scan.stats_materialization().stats_projection(),
            &StatsProjection::none()
        );
        assert_eq!(
            scan.stats_materialization().stats_source_policy(),
            StatsSourcePolicy::None
        );

        Ok(())
    }

    #[tokio::test]
    async fn scan_builder_with_skip_stats_false_clears_prior_skip_stats() -> DeltaResult<()> {
        let snapshot = synthetic_snapshot().await?;
        let scan = snapshot
            .scan_builder()
            .with_stats_materialization(FileStatsMaterialization::without_stats())
            .with_skip_stats(false)
            .build()?;

        assert_eq!(
            scan.stats_materialization().stats_projection(),
            &StatsProjection::NumRecordsOnly
        );
        assert_eq!(
            scan.stats_materialization().stats_source_policy(),
            StatsSourcePolicy::ParsedWithJsonFallback
        );
        assert!(!scan.stats_materialization().preserves_raw_stats());

        Ok(())
    }
}

/// A configured, executable scan over a table snapshot.
///
/// Produced by [`ScanBuilder::build`]; drives log replay to enumerate the data files (and the
/// statistics materialization strategy) that satisfy the scan's schema and predicate.
#[derive(Debug)]
pub struct Scan {
    inner: Arc<KernelScan>,
    stats_materialization: FileStatsMaterialization,
}

impl From<KernelScan> for Scan {
    fn from(inner: KernelScan) -> Self {
        Self::new(
            Arc::new(inner),
            FileStatsMaterialization::compatibility(StatsProjection::full()),
        )
    }
}

impl From<Arc<KernelScan>> for Scan {
    fn from(inner: Arc<KernelScan>) -> Self {
        Self::new(
            inner,
            FileStatsMaterialization::compatibility(StatsProjection::full()),
        )
    }
}

impl Scan {
    fn new(inner: Arc<KernelScan>, stats_materialization: FileStatsMaterialization) -> Self {
        Self {
            inner,
            stats_materialization,
        }
    }

    /// Get a shared reference to the inner [`KernelScan`].
    #[cfg(feature = "datafusion")]
    pub(crate) fn inner(&self) -> &Arc<KernelScan> {
        &self.inner
    }

    /// Get the stats materialization policy attached to this scan.
    ///
    /// The policy is used when converting kernel scan rows into output rows.
    pub(crate) fn stats_materialization(&self) -> &FileStatsMaterialization {
        &self.stats_materialization
    }

    /// The table's root URL. Any relative paths returned from `scan_data` (or in a callback from
    /// [`ScanMetadata::visit_scan_files`]) must be resolved against this root to get the actual path to
    /// the file.
    ///
    /// [`ScanMetadata::visit_scan_files`]: crate::scan::ScanMetadata::visit_scan_files
    // NOTE: this is obviously included in the snapshot, just re-exposed here for convenience.
    pub fn table_root(&self) -> &Url {
        self.inner.table_root()
    }

    /// Get a shared reference to the [`Snapshot`] of this scan.
    ///
    /// [`Snapshot`]: crate::Snapshot
    pub fn snapshot(&self) -> &SnapshotRef {
        self.inner.snapshot()
    }

    /// Get a shared reference to the logical [`Schema`] of the scan (i.e. the output schema of the
    /// scan). Note that the logical schema can differ from the physical schema due to e.g.
    /// partition columns which are present in the logical schema but not in the physical schema.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn logical_schema(&self) -> &SchemaRef {
        self.inner.logical_schema()
    }

    /// Get a shared reference to the physical [`Schema`] of the scan. This represents the schema
    /// of the underlying data files which must be read from storage.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn physical_schema(&self) -> &SchemaRef {
        self.inner.physical_schema()
    }

    /// Get the predicate [`PredicateRef`] of the scan.
    /// Returns the predicate pushed down to the physical scan, if any was derived.
    pub fn physical_predicate(&self) -> Option<PredicateRef> {
        self.inner.physical_predicate()
    }

    /// Stream the per-file [`ScanMetadata`] for this scan, driving log replay on `engine`.
    pub fn scan_metadata(&self, engine: Arc<dyn Engine>) -> SendableScanMetadataStream {
        // TODO: which capacity to choose?
        let mut builder = ReceiverStreamBuilder::<ScanMetadata>::new(100);
        let tx = builder.tx();

        let inner = self.inner.clone();
        let blocking_iter = move || {
            for res in inner.scan_metadata(engine.as_ref())? {
                if tx.blocking_send(Ok(res?)).is_err() {
                    break;
                }
            }
            Ok(())
        };

        builder.spawn_blocking(blocking_iter);
        builder.build()
    }

    #[cfg(feature = "datafusion")]
    pub(crate) fn scan_metadata_seeded(
        &self,
        engine: Arc<dyn Engine>,
        materialized_files: Option<&Arc<MaterializedFiles>>,
    ) -> SendableScanMetadataStream {
        let Some(materialized_seed) =
            materialized_files.and_then(|materialized_files| materialized_files.full_table_seed())
        else {
            return self.scan_metadata(engine);
        };
        let (
            existing_version,
            available_stats_schema,
            raw_stats_available,
            existing_data,
            existing_predicate,
        ) = materialized_seed.into_parts();
        self.scan_metadata_from(
            engine,
            existing_version,
            available_stats_schema,
            raw_stats_available,
            Box::new(existing_data),
            existing_predicate,
        )
    }

    /// Stream [`ScanMetadata`] incrementally starting from a previously observed state.
    ///
    /// Given the data already known at `existing_version` (and the predicate used to produce it),
    /// only the log changes since that version are replayed, avoiding a full scan when refreshing
    /// an already-loaded snapshot.
    pub fn scan_metadata_from<T: Iterator<Item = RecordBatch> + Send + 'static>(
        &self,
        engine: Arc<dyn Engine>,
        existing_version: Version,
        available_stats_schema: Option<SchemaRef>,
        raw_stats_available: bool,
        existing_data: Box<T>,
        existing_predicate: Option<PredicateRef>,
    ) -> SendableScanMetadataStream {
        let inner = self.inner.clone();
        let effective_replay_stats_schema = inner.effective_replay_stats_schema().cloned();
        let partition_schema = match inner.snapshot().partitions_schema() {
            Ok(partition_schema) => partition_schema,
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };
        let evaluator = match CachedScanRowEvaluator::try_new(
            available_stats_schema.as_ref(),
            raw_stats_available,
            effective_replay_stats_schema,
            partition_schema.as_ref(),
        ) {
            Ok(evaluator) => evaluator,
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };
        let stats_parsed_schema = evaluator.stats_parsed_schema();
        let scan_row_iter = existing_data.map(move |batch| {
            evaluator
                .evaluate(batch)
                .map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
                .map_err(|err| delta_kernel::Error::Generic(err.to_string()))
        });

        // TODO: which capacity to choose?
        let mut builder = ReceiverStreamBuilder::<ScanMetadata>::new(100);
        let tx = builder.tx();
        let scan_inner = move || {
            for res in inner.scan_metadata_from(
                engine.as_ref(),
                existing_version,
                stats_parsed_schema,
                Box::new(scan_row_iter),
                existing_predicate,
            )? {
                if tx.blocking_send(Ok(res?)).is_err() {
                    break;
                }
            }
            Ok(())
        };

        builder.spawn_blocking(scan_inner);
        builder.build()
    }
}
