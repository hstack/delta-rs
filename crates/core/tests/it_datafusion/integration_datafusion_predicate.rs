use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::{
    collect, visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_kernel::expressions::ColumnName;
use delta_kernel::{
    DeltaResult as KernelResult, DeltaResultIterator, DeltaResultIteratorStatic, Engine,
    EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, FileSlice,
    FilteredEngineData, JsonHandler, ParquetFooter, ParquetHandler, PredicateRef, StorageHandler,
};
use deltalake_core::delta_datafusion::DeltaScanExec;
use deltalake_core::kernel::DeltaResult;
use deltalake_core::{DeltaTableBuilder, SchemaRef};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tracing::info;
use url::Url;

/// Records every bounded byte-range read made against the inner object store, keyed by path.
///
/// Intercepts `get_opts` (for single ranged reads) and `get_ranges` (for multi-range reads,
/// where the `ObjectStore` default impl coalesces ranges into one `get_opts` call — recording
/// at the `get_opts` level alone would hide the caller's per-column intent). `get_range` is
/// not overridden because its default impl routes through `get_opts` without coalescing.
#[derive(Debug)]
pub struct RangeRecordingObjectStore {
    inner: Arc<dyn ObjectStore>,
    reads: Arc<Mutex<HashMap<String, Vec<Range<u64>>>>>,
}

impl Display for RangeRecordingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RangeRecording({})", self.inner)
    }
}

#[allow(dead_code)]
impl RangeRecordingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            reads: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns all byte ranges recorded for any path whose string representation ends with `suffix`.
    pub fn reads_for_path_suffix(&self, suffix: &str) -> Vec<Range<u64>> {
        self.reads
            .lock()
            .expect("lock not poisoned")
            .iter()
            .filter(|(k, _)| k.ends_with(suffix))
            .flat_map(|(_, v)| v.clone())
            .collect()
    }

    fn clear_reads(&self) {
        self.reads.lock().expect("lock not poisoned").clear();
    }

    fn record(&self, path: &Path, range: Range<u64>) {
        self.reads
            .lock()
            .expect("lock not poisoned")
            .entry(path.to_string())
            .or_default()
            .push(range);
    }

}

#[async_trait]
impl ObjectStore for RangeRecordingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotImplemented {
            operation: "put_multipart_opts".to_string(),
            implementer: "RangeRecordingObjectStore".to_string(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Only record bounded ranges. Parquet readers issue suffix reads for the footer
        // (not column data) and don't issue unranged full reads for column access, so
        // skipping those keeps the recorder focused on column-chunk traffic.
        if let Some(GetRange::Bounded(r)) = &options.range {
            self.record(location, r.clone());
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        for r in ranges {
            self.record(location, r.clone());
        }
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Returns the byte ranges of parquet column chunks whose dotted path matches `predicate`.
fn parquet_column_ranges(parquet_path: &str, predicate: impl Fn(&str) -> bool) -> Vec<Range<u64>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    #[allow(clippy::unwrap_used)]
    let reader = SerializedFileReader::new(std::fs::File::open(parquet_path).unwrap()).unwrap();
    reader
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|rg| rg.columns())
        .filter(|col| predicate(&col.column_descr().path().parts().join(".")))
        .map(|col| {
            // `file_offset` is deprecated in the Parquet spec and inconsistent across
            // writers (often `0`); `data_page_offset` is the reliable fallback when
            // there is no dictionary page.
            let start = col
                .dictionary_page_offset()
                .unwrap_or(col.data_page_offset()) as u64;
            start..start + col.compressed_size() as u64
        })
        .collect()
}

fn ranges_overlap(a: &Range<u64>, b: &Range<u64>) -> bool {
    a.start < b.end && b.start < a.end
}

#[derive(Clone)]
struct ParquetReadCall {
    files: Vec<FileMeta>,
    physical_schema: SchemaRef,
    predicate: Option<PredicateRef>,
}

struct TracingParquetHandler {
    inner: Arc<dyn ParquetHandler>,
    calls: Arc<Mutex<Vec<ParquetReadCall>>>,
}

impl ParquetHandler for TracingParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        info!(
            "TracingEngine::TracingParquetHandler::read_parquet_files\n\tfiles: {:?}\n\tpredicate: {:?}",
            &files, &predicate
        );
        self.calls
            .lock()
            .expect("lock not poisoned")
            .push(ParquetReadCall {
                files: files.to_vec(),
                physical_schema: physical_schema.clone(),
                predicate: predicate.clone(),
            });
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

struct TracingJsonHandler {
    inner: Arc<dyn JsonHandler>,
    predicates: Arc<Mutex<Vec<Option<PredicateRef>>>>,
}

impl JsonHandler for TracingJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.inner.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        info!(
            "TracingEngine::TracingJsonHandler::read_json_files\n\tfiles: {:?}\n\tpredicate: {:?}",
            &files, predicate
        );
        self.predicates
            .lock()
            .expect("lock not poisoned")
            .push(predicate.clone());
        self.inner
            .read_json_files(files, physical_schema, predicate)
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: DeltaResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> KernelResult<()> {
        self.inner.write_json_file(path, data, overwrite)
    }
}

struct TracingStorageHandler {
    inner: Arc<dyn StorageHandler>,
}

impl StorageHandler for TracingStorageHandler {
    fn list_from(
        &self,
        path: &Url,
    ) -> KernelResult<Box<dyn Iterator<Item = KernelResult<FileMeta>>>> {
        self.inner.list_from(path)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> KernelResult<Box<dyn Iterator<Item = KernelResult<Bytes>>>> {
        self.inner.read_files(files)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        self.inner.copy_atomic(src, dest)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        self.inner.put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        self.inner.head(path)
    }

    fn delete(&self, path: &Url) -> KernelResult<()> {
        self.inner.delete(path)
    }
}

pub struct TracingEngine {
    inner: Arc<dyn Engine>,
    parquet_handler: Arc<TracingParquetHandler>,
    storage_handler: Arc<TracingStorageHandler>,
    json_handler: Arc<TracingJsonHandler>,
    parquet_calls: Arc<Mutex<Vec<ParquetReadCall>>>,
    json_predicates: Arc<Mutex<Vec<Option<PredicateRef>>>>,
}

impl TracingEngine {
    fn new(inner: Arc<dyn Engine>) -> Self {
        let parquet_calls = Arc::new(Mutex::new(Vec::new()));
        let json_predicates = Arc::new(Mutex::new(Vec::new()));
        Self {
            inner: inner.clone(),
            parquet_handler: Arc::new(TracingParquetHandler {
                inner: inner.parquet_handler(),
                calls: parquet_calls.clone(),
            }),
            storage_handler: Arc::new(TracingStorageHandler {
                inner: inner.storage_handler(),
            }),
            json_handler: Arc::new(TracingJsonHandler {
                inner: inner.json_handler(),
                predicates: json_predicates.clone(),
            }),
            parquet_calls,
            json_predicates,
        }
    }

    pub fn wrap(inner: Arc<dyn Engine>) -> Arc<Self> {
        Arc::new(Self::new(inner))
    }

    fn take_parquet_calls(&self) -> Vec<ParquetReadCall> {
        std::mem::take(&mut *self.parquet_calls.lock().expect("lock not poisoned"))
    }

    fn take_json_predicates(&self) -> Vec<Option<PredicateRef>> {
        std::mem::take(&mut *self.json_predicates.lock().expect("lock not poisoned"))
    }
}

impl Engine for TracingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage_handler.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }
}

#[derive(Default)]
struct DeltaScanVisitor {
    num_scanned: Option<usize>,
}

impl ExecutionPlanVisitor for DeltaScanVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> DataFusionResult<bool> {
        if let Some(delta_scan) = plan.downcast_ref::<DeltaScanExec>() {
            self.num_scanned = delta_scan
                .metrics()
                .and_then(|metrics| metrics.sum_by_name("count_files_scanned"))
                .map(|value| value.as_usize());
        }
        Ok(true)
    }
}

#[derive(Clone, Copy)]
enum QueryColumn {
    BatchId,
    ConversationId,
}

struct PredicateQuery {
    sql: &'static str,
    selected_files: usize,
    output_rows: usize,
    column: QueryColumn,
}

struct CheckpointColumnRanges {
    stats_parsed: Vec<Range<u64>>,
    stats: Vec<Range<u64>>,
    partition_values: Vec<Range<u64>>,
    partition_values_parsed: Vec<Range<u64>>,
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_predicate_stats_parsed_without_files() -> DeltaResult<()> {
    run_engine_predicate("conversations", true, false).await
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_predicate_stats_parsed_with_files() -> DeltaResult<()> {
    run_engine_predicate("conversations", true, true).await
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_predicate_no_stats_parsed_without_files() -> DeltaResult<()> {
    run_engine_predicate("conversations_no_stats_parsed", false, false).await
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_predicate_no_stats_parsed_with_files() -> DeltaResult<()> {
    run_engine_predicate("conversations_no_stats_parsed", false, true).await
}

async fn run_engine_predicate(
    fixture: &str,
    expect_parsed_checkpoint_fields: bool,
    require_files: bool,
) -> DeltaResult<()> {
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let subscriber = tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber).ok();
    pretty_env_logger::try_init().ok();

    let config =
        SessionConfig::new().set_bool("datafusion.sql_parser.enable_ident_normalization", false);

    let ctx = SessionContext::new_with_config(config);

    let recording_object_store = Arc::new(RangeRecordingObjectStore::new(Arc::new(
        object_store::local::LocalFileSystem::new(),
    )));

    let path = format!(
        "file://{}/tests/data/{fixture}/delta/",
        env!("CARGO_MANIFEST_DIR")
    );
    let url = Url::parse(&path)?;
    let mut builder = DeltaTableBuilder::from_url(url.clone())
        .unwrap()
        .with_storage_backend(recording_object_store.clone(), url)
        .with_checkpoint_stats_json_fallback(true);
    let engine = TracingEngine::wrap(builder.build_storage().unwrap().engine(None));
    builder = builder.with_engine(engine.clone());
    if !require_files {
        builder = builder.without_files();
    }

    let table = builder.load().await.unwrap();
    table.update_datafusion_session(&ctx.state()).unwrap();
    let provider = table.table_provider().await.unwrap();

    let construction_parquet_calls = engine.take_parquet_calls();
    let construction_json_predicates = engine.take_json_predicates();
    if require_files {
        assert!(
            construction_parquet_calls.iter().any(|call| {
                call.files.iter().any(|file| {
                    let path = file.location.path();
                    path.contains("/_delta_log/") && path.contains(".checkpoint.")
                })
            }),
            "eager table construction must read transaction-log parquet"
        );
        assert!(
            !construction_json_predicates.is_empty(),
            "eager table construction must read transaction-log JSON"
        );
    }

    ctx.register_table("conversations", provider).unwrap();
    engine.take_parquet_calls();
    engine.take_json_predicates();
    recording_object_store.clear_reads();

    let checkpoint_name = "00000000000000000007.checkpoint.parquet";
    let checkpoint_path = format!(
        "{}/tests/data/{fixture}/delta/_delta_log/{checkpoint_name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let checkpoint_ranges = CheckpointColumnRanges {
        stats_parsed: parquet_column_ranges(&checkpoint_path, |path| {
            path.starts_with("add.stats_parsed.")
        }),
        stats: parquet_column_ranges(&checkpoint_path, |path| path == "add.stats"),
        partition_values: parquet_column_ranges(&checkpoint_path, |path| {
            path.starts_with("add.partitionValues.")
        }),
        partition_values_parsed: parquet_column_ranges(&checkpoint_path, |path| {
            path.starts_with("add.partitionValues_parsed.")
        }),
    };

    let queries = [
        PredicateQuery {
            sql: "SELECT conversation['conversationID'] FROM conversations WHERE _ACP_BATCHID = 'batch-1-2024-01-01'",
            selected_files: 2,
            output_rows: 4,
            column: QueryColumn::BatchId,
        },
        PredicateQuery {
            sql: "SELECT conversation['conversationID'] FROM conversations WHERE _ACP_BATCHID != 'batch-1-2024-01-01'",
            selected_files: 26,
            output_rows: 52,
            column: QueryColumn::BatchId,
        },
        PredicateQuery {
            sql: "SELECT conversation['conversationID'] FROM conversations WHERE conversation['conversationID'] = 'conversation-2024-01-01-1'",
            selected_files: 2,
            output_rows: 4,
            column: QueryColumn::ConversationId,
        },
    ];

    for query in queries {
        run_predicate_query(
            &ctx,
            &engine,
            &recording_object_store,
            checkpoint_name,
            &checkpoint_ranges,
            expect_parsed_checkpoint_fields,
            require_files,
            &query,
        )
        .await;
    }

    Ok(())
}

async fn run_predicate_query(
    ctx: &SessionContext,
    engine: &TracingEngine,
    recording_object_store: &RangeRecordingObjectStore,
    checkpoint_name: &str,
    checkpoint_ranges: &CheckpointColumnRanges,
    expect_parsed_checkpoint_fields: bool,
    require_files: bool,
    query: &PredicateQuery,
) {
    engine.take_parquet_calls();
    engine.take_json_predicates();
    recording_object_store.clear_reads();

    let dataframe = ctx.sql(query.sql).await.unwrap();
    let physical_plan = dataframe.create_physical_plan().await.unwrap();
    let mut visitor = DeltaScanVisitor::default();
    visit_execution_plan(physical_plan.as_ref(), &mut visitor).unwrap();
    assert_eq!(
        visitor.num_scanned,
        Some(query.selected_files),
        "unexpected selected file count for {}",
        query.sql
    );

    let result = collect(physical_plan, ctx.task_ctx()).await.unwrap();
    let output_rows = result.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(
        output_rows, query.output_rows,
        "unexpected output row count for {}",
        query.sql
    );

    let parquet_calls = engine.take_parquet_calls();
    let json_predicates = engine.take_json_predicates();
    if require_files {
        assert_eq!(
            parquet_calls.len(),
            0,
            "eager query must not make kernel parquet calls: {}",
            query.sql
        );
        assert_eq!(
            json_predicates.len(),
            0,
            "eager query must not make kernel JSON calls: {}",
            query.sql
        );
    } else {
        // check we read parquet + json
        {
            assert_eq!(
                json_predicates.len(),
                1,
                "expected one kernel JSON call for {}",
                query.sql
            );
            let predicate = json_predicates[0]
                .as_ref()
                .expect("query JSON call must have a predicate");
            let query_column = match query.column {
                QueryColumn::BatchId => ColumnName::new(["_ACP_BATCHID"]),
                QueryColumn::ConversationId => ColumnName::new(["conversation", "conversationID"]),
            };
            assert!(
                predicate.references().contains(&query_column),
                "JSON predicate must reference the query column for {}",
                query.sql
            );
        }
        {
            assert_eq!(
                parquet_calls.len(),
                2,
                "expected sidecar discovery then checkpoint action read for {}",
                query.sql
            );
            for call in &parquet_calls {
                assert_eq!(
                    call.files.len(),
                    1,
                    "each kernel parquet call must read one checkpoint file"
                );
                assert!(
                    call.files[0].location.path().ends_with(checkpoint_name),
                    "kernel parquet call must read the version-7 checkpoint"
                );
            }

            let sidecar_call = &parquet_calls[0];
            assert!(sidecar_call.physical_schema.field("sidecar").is_some());
            assert!(sidecar_call.physical_schema.field("add").is_none());
            assert!(sidecar_call.predicate.is_none());

            let action_call = &parquet_calls[1];
            assert!(action_call.physical_schema.field("add").is_some());
            if !expect_parsed_checkpoint_fields {
                assert!(
                    action_call.predicate.is_none(),
                    "checkpoint without parsed stats must not have an action predicate"
                );
                return;
            }

            let predicate = action_call
                .predicate
                .as_ref()
                .expect("parsed checkpoint action read must have a predicate");
            let references = predicate.references();
            let partition_column = ColumnName::new(["add", "partitionValues_parsed", "_ACP_BATCHID"]);
            match query.column {
                QueryColumn::BatchId => assert!(
                    references.contains(&partition_column),
                    "checkpoint predicate must reference the parsed partition column"
                ),
                QueryColumn::ConversationId => {
                    assert!(
                        references.contains(&ColumnName::new([
                            "add",
                            "stats_parsed",
                            "minValues",
                            "conversation",
                            "conversationID",
                        ])),
                        "checkpoint predicate must reference the nested minimum"
                    );
                    assert!(
                        references.contains(&ColumnName::new([
                            "add",
                            "stats_parsed",
                            "maxValues",
                            "conversation",
                            "conversationID",
                        ])),
                        "checkpoint predicate must reference the nested maximum"
                    );
                    assert!(
                        !references.contains(&partition_column),
                        "nested checkpoint predicate must not retain the partition reference"
                    );
                }
            }
        }
    }

    {
        let checkpoint_reads = recording_object_store.reads_for_path_suffix(checkpoint_name);
        if require_files {
            assert!(
                checkpoint_reads.is_empty(),
                "eager query must not read checkpoint byte ranges"
            );
            return;
        }

        let ranges_were_read = |column_ranges: &[Range<u64>]| {
            checkpoint_reads.iter().any(|read| {
                column_ranges
                    .iter()
                    .any(|range| ranges_overlap(read, range))
            })
        };
        assert!(
            !checkpoint_reads.is_empty(),
            "expected checkpoint column reads"
        );
        assert!(
            !checkpoint_ranges.stats.is_empty(),
            "checkpoint must contain add.stats"
        );
        assert!(
            ranges_were_read(&checkpoint_ranges.partition_values),
            "expected add.partitionValues to be read"
        );

        if expect_parsed_checkpoint_fields {
            assert!(
                !checkpoint_ranges.stats_parsed.is_empty(),
                "checkpoint must contain add.stats_parsed"
            );
            assert!(
                !checkpoint_ranges.partition_values_parsed.is_empty(),
                "checkpoint must contain add.partitionValues_parsed"
            );
            assert!(
                ranges_were_read(&checkpoint_ranges.stats_parsed),
                "expected add.stats_parsed to be read"
            );
            assert!(
                !ranges_were_read(&checkpoint_ranges.stats),
                "expected add.stats not to be read"
            );
            assert_eq!(
                ranges_were_read(&checkpoint_ranges.partition_values_parsed),
                matches!(query.column, QueryColumn::BatchId),
                "parsed partition-value reads must be query-specific"
            );
        } else {
            assert!(
                checkpoint_ranges.stats_parsed.is_empty(),
                "checkpoint must not contain add.stats_parsed"
            );
            assert!(
                checkpoint_ranges.partition_values_parsed.is_empty(),
                "checkpoint must not contain add.partitionValues_parsed"
            );
            assert!(
                ranges_were_read(&checkpoint_ranges.stats),
                "expected add.stats to be read"
            );
        }
    }
}
