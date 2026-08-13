use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::assert_batches_sorted_eq;
use datafusion::common::{ScalarValue, stats::Precision};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionContext, col, lit};
use datafusion_datasource::source::DataSourceExec;
use delta_kernel::actions::STATS_PARSED;
use delta_kernel::schema::{DataType, SchemaRef, StructField};
use delta_kernel::{
    DeltaResult as KernelResult, DeltaResultIteratorStatic, Engine, EngineData, EvaluationHandler,
    FileDataReadResultIterator, FileMeta, JsonHandler, ParquetFooter, ParquetHandler, PredicateRef,
    StorageHandler,
};
use deltalake_core::checkpoints::create_checkpoint;
use deltalake_core::delta_datafusion::DeltaScanExec;
use deltalake_core::operations::write::SchemaMode;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaTable, DeltaTableBuilder, TableProperty};
use deltalake_test::TestResult;
use futures::TryStreamExt;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use url::Url;

struct StatsParsedParquetHandler {
    inner: Arc<dyn ParquetHandler>,
    checkpoint_add_reads: AtomicUsize,
}

impl ParquetHandler for StatsParsedParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let reads_checkpoint_adds = physical_schema.field("add").is_some()
            && files
                .iter()
                .any(|file| file.location.path().contains(".checkpoint."));
        if reads_checkpoint_adds {
            let add = physical_schema
                .field("add")
                .and_then(|field| match field.data_type() {
                    DataType::Struct(add) => Some(add),
                    _ => None,
                })
                .expect("projected checkpoint Add action must be a struct");
            assert!(
                add.field("stats").is_none(),
                "compatibility reads must not project add.stats when add.stats_parsed is available"
            );
            assert!(
                add.field(STATS_PARSED).is_some(),
                "compatibility reads must project add.stats_parsed"
            );
            self.checkpoint_add_reads.fetch_add(1, Ordering::SeqCst);
        }

        self.inner
            .read_parquet_files(files, physical_schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> KernelResult<()> {
        self.inner.write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }
}

struct RecordingEngine {
    inner: Arc<dyn Engine>,
    parquet_handler: Arc<StatsParsedParquetHandler>,
}

impl Engine for RecordingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.inner.storage_handler()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.inner.json_handler()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }
}

#[tokio::test]
#[ignore = "Missing / broken fixture"]
async fn table_scan_prefers_checkpoint_stats_parsed() -> TestResult {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../delta-kernel-rs/kernel/tests/data/stats-parsed-partitioned");
    assert!(fixture.is_dir(), "missing fixture at {}", fixture.display());

    let temp_dir = tempfile::tempdir()?;
    let copy_options = fs_extra::dir::CopyOptions {
        content_only: true,
        ..Default::default()
    };
    fs_extra::dir::copy(&fixture, temp_dir.path(), &copy_options)?;

    let table_url = Url::from_directory_path(temp_dir.path())
        .map_err(|_| "temporary table path is not a directory")?;

    // Checkpoint version 2 so the latest scan combines checkpoint stats with version 3 JSON stats.
    let checkpoint_table = DeltaTableBuilder::from_url(table_url.clone())?
        .with_version(2)
        .load()
        .await?;
    create_checkpoint(&checkpoint_table, None).await?;

    let base_engine = checkpoint_table.log_store().engine(None);
    let checkpoint_url = table_url.join("_delta_log/00000000000000000002.checkpoint.parquet")?;
    let checkpoint_meta = base_engine.storage_handler().head(&checkpoint_url)?;
    let checkpoint_footer = base_engine
        .parquet_handler()
        .read_parquet_footer(&checkpoint_meta)?;
    let checkpoint_add = checkpoint_footer
        .schema
        .field("add")
        .and_then(|field| match field.data_type() {
            DataType::Struct(add) => Some(add),
            _ => None,
        })
        .expect("checkpoint Add action must be a struct");
    assert!(
        checkpoint_add.field("stats").is_some(),
        "checkpoint must physically contain stats so the projection assertion is meaningful"
    );
    assert!(
        checkpoint_add.field(STATS_PARSED).is_some(),
        "checkpoint must physically contain stats_parsed"
    );

    let parquet_handler = Arc::new(StatsParsedParquetHandler {
        inner: base_engine.parquet_handler(),
        checkpoint_add_reads: AtomicUsize::new(0),
    });
    let engine: Arc<dyn Engine> = Arc::new(RecordingEngine {
        inner: base_engine,
        parquet_handler: parquet_handler.clone(),
    });

    // This is the builder equivalent of open_table, with an injected engine for observing the
    // checkpoint projection. The DataFusion scan replays the opened table's materialized files.
    let table = DeltaTableBuilder::from_url(table_url)?
        .with_engine(engine)
        .load()
        .await?;
    let checkpoint_add_reads_after_load =
        parquet_handler.checkpoint_add_reads.load(Ordering::SeqCst);
    assert_eq!(
        checkpoint_add_reads_after_load, 1,
        "the eager compatibility-cache read must read checkpoint Add actions exactly once"
    );
    assert_eq!(table.version(), Some(3));

    let context = SessionContext::new();
    let provider = table.table_provider().await?;

    // Keep all fixture files in the plan so their exact statistics remain observable. The
    // narrower row predicate below correctly prunes the first file during kernel replay.
    let stats_filter = col("user_id")
        .gt_eq(lit(1_i64))
        .and(col("user_id").lt_eq(lit(9_i64)));
    let stats_scan = provider
        .scan(&context.state(), None, &[stats_filter], None)
        .await?;

    let delta_scan = stats_scan
        .downcast_ref::<DeltaScanExec>()
        .expect("expected DeltaScanExec");
    let children = delta_scan.children();
    let data_source = children[0]
        .downcast_ref::<DataSourceExec>()
        .expect("expected DataSourceExec child");
    let (file_scan, _) = data_source
        .downcast_to_file_source::<ParquetSource>()
        .expect("expected Parquet file source");
    let user_id_index = file_scan
        .file_source
        .table_schema()
        .table_schema()
        .index_of("user_id")?;
    let mut parsed_file_stats = file_scan
        .file_groups
        .iter()
        .flat_map(|group| group.iter())
        .map(|file| {
            let stats = file
                .statistics
                .as_ref()
                .expect("scan file must have parsed statistics");
            let user_id = &stats.column_statistics[user_id_index];
            let value = |precision: &Precision<ScalarValue>| match precision {
                Precision::Exact(ScalarValue::Int64(Some(value))) => *value,
                other => panic!("expected exact Int64 statistic, got {other:?}"),
            };
            (value(&user_id.min_value), value(&user_id.max_value))
        })
        .collect::<Vec<_>>();
    parsed_file_stats.sort_unstable();
    assert_eq!(parsed_file_stats, vec![(1, 3), (4, 6), (7, 9)]);

    let row_filter = col("user_id")
        .gt_eq(lit(4_i64))
        .and(col("user_id").lt_eq(lit(8_i64)));
    let row_scan = provider
        .scan(&context.state(), None, &[row_filter], None)
        .await?;
    let batches = collect(row_scan, context.task_ctx()).await?;

    let expected = [
        "+------------+---------------+---------+",
        "| _ACP_DATE  | _ACP_BATCH_ID | user_id |",
        "+------------+---------------+---------+",
        "| 2024-01-01 | batch-002     | 4       |",
        "| 2024-01-01 | batch-002     | 5       |",
        "| 2024-01-01 | batch-002     | 6       |",
        "| 2024-01-02 | batch-001     | 7       |",
        "| 2024-01-02 | batch-001     | 8       |",
        "+------------+---------------+---------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
    assert_eq!(
        parquet_handler.checkpoint_add_reads.load(Ordering::SeqCst),
        checkpoint_add_reads_after_load,
        "broad and narrow query scans must replay from the materialized seed instead of re-reading checkpoint Add actions"
    );

    Ok(())
}

#[tokio::test]
async fn table_scan_skips_old_physical_schema_file_from_checkpoint_stats_parsed() -> TestResult {
    let temp_dir = tempfile::tempdir()?;
    let table_url = Url::from_directory_path(temp_dir.path())
        .map_err(|_| "temporary table path is not a directory")?;

    let table = DeltaTable::try_from_url(table_url.clone())
        .await?
        .create()
        .with_columns([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("part", DataType::STRING),
        ])
        .with_partition_columns(["part"])
        .with_configuration_property(TableProperty::CheckpointWriteStatsAsStruct, Some("true"))
        .with_configuration_property(TableProperty::CheckpointWriteStatsAsJson, Some("true"))
        .await?;

    let old_batch = RecordBatch::try_from_iter_with_nullable(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            false,
        ),
        (
            "part",
            Arc::new(StringArray::from(vec!["old", "old"])) as ArrayRef,
            false,
        ),
    ])?;
    let table = table
        .write(vec![old_batch])
        .with_save_mode(SaveMode::Append)
        .await?;

    let new_batch = RecordBatch::try_from_iter_with_nullable(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![100, 101])) as ArrayRef,
            false,
        ),
        (
            "part",
            Arc::new(StringArray::from(vec!["new", "new"])) as ArrayRef,
            false,
        ),
        (
            "added",
            Arc::new(StringArray::from(vec![Some("present"), None])) as ArrayRef,
            true,
        ),
    ])?;
    let table = table
        .write(vec![new_batch])
        .with_save_mode(SaveMode::Append)
        .with_schema_mode(SchemaMode::Merge)
        .await?;
    assert_eq!(table.version(), Some(2));

    let adds = table
        .get_active_add_actions_by_partitions(&[])
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(adds.len(), 2, "expected one data file per partition");

    let mut physical_files = Vec::with_capacity(2);
    for add in adds {
        let path = add.path().into_owned();
        let partition_values = add.partition_values_map();
        assert_eq!(partition_values.len(), 1);
        let part = partition_values
            .get("part")
            .cloned()
            .flatten()
            .expect("data file must have a non-null part value");

        let reader = ParquetObjectReader::new(
            table.object_store(),
            deltalake_core::Path::from(path.as_str()),
        );
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let physical_fields = builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        physical_files.push((part, path, physical_fields));
    }
    physical_files.sort_by(|left, right| left.0.cmp(&right.0));

    let new_file = &physical_files[0];
    assert_eq!(new_file.0, "new");
    assert!(new_file.1.contains("part=new/"), "{:?}", new_file.1);
    assert_eq!(new_file.2, vec!["id", "added"]);

    let old_file = &physical_files[1];
    assert_eq!(old_file.0, "old");
    assert!(old_file.1.contains("part=old/"), "{:?}", old_file.1);
    assert_eq!(old_file.2, vec!["id"]);

    create_checkpoint(&table, None).await?;

    let base_engine = table.log_store().engine(None);
    let checkpoint_url = table_url.join("_delta_log/00000000000000000002.checkpoint.parquet")?;
    let checkpoint_meta = base_engine.storage_handler().head(&checkpoint_url)?;
    let checkpoint_footer = base_engine
        .parquet_handler()
        .read_parquet_footer(&checkpoint_meta)?;
    let checkpoint_add = checkpoint_footer
        .schema
        .field("add")
        .and_then(|field| match field.data_type() {
            DataType::Struct(add) => Some(add),
            _ => None,
        })
        .expect("checkpoint Add action must be a struct");
    assert!(
        checkpoint_add.field("stats").is_some(),
        "checkpoint must physically contain JSON stats"
    );
    assert!(
        checkpoint_add.field(STATS_PARSED).is_some(),
        "checkpoint must physically contain stats_parsed"
    );

    let parquet_handler = Arc::new(StatsParsedParquetHandler {
        inner: base_engine.parquet_handler(),
        checkpoint_add_reads: AtomicUsize::new(0),
    });
    let engine: Arc<dyn Engine> = Arc::new(RecordingEngine {
        inner: base_engine,
        parquet_handler: parquet_handler.clone(),
    });
    let reopened = DeltaTableBuilder::from_url(table_url)?
        .with_engine(engine)
        .load()
        .await?;
    let checkpoint_add_reads_after_load =
        parquet_handler.checkpoint_add_reads.load(Ordering::SeqCst);
    assert_eq!(checkpoint_add_reads_after_load, 1);

    let context = SessionContext::new();
    let provider = reopened.table_provider().await?;
    let id_filter = col("id").gt(lit(50_i32));
    let scan = provider
        .scan(&context.state(), None, &[id_filter], None)
        .await?;

    let delta_scan = scan
        .downcast_ref::<DeltaScanExec>()
        .expect("expected DeltaScanExec");
    let children = delta_scan.children();
    let data_source = children[0]
        .downcast_ref::<DataSourceExec>()
        .expect("expected DataSourceExec child");
    let (file_scan, _) = data_source
        .downcast_to_file_source::<ParquetSource>()
        .expect("expected Parquet file source");
    let scanned_paths = file_scan
        .file_groups
        .iter()
        .flat_map(|group| group.iter())
        .map(|file| file.object_meta.location.to_string())
        .collect::<Vec<_>>();
    assert_eq!(scanned_paths.len(), 1, "{scanned_paths:?}");
    assert!(scanned_paths[0].ends_with(&new_file.1), "{scanned_paths:?}");
    assert!(
        !scanned_paths[0].ends_with(&old_file.1),
        "{scanned_paths:?}"
    );

    let batches = collect(scan.clone(), context.task_ctx()).await?;
    assert_eq!(
        delta_scan
            .metrics()
            .and_then(|metrics| metrics.sum_by_name("count_files_scanned"))
            .map(|value| value.as_usize()),
        Some(1)
    );

    let expected = [
        "+-----+------+---------+",
        "| id  | part | added   |",
        "+-----+------+---------+",
        "| 100 | new  | present |",
        "| 101 | new  |         |",
        "+-----+------+---------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
    assert_eq!(
        parquet_handler.checkpoint_add_reads.load(Ordering::SeqCst),
        checkpoint_add_reads_after_load,
        "query scan must not re-read checkpoint Add actions"
    );

    Ok(())
}
