use super::storage::{group_by_store, AsObjectStoreUrl};
use crate::delta_datafusion::engine::predicate_conversion::{
    partition_filter_to_datafusion_expr, predicate_to_datafusion_expr,
};
use crate::PartitionFilter;
use dashmap::{mapref::one::Ref, DashMap};
use datafusion::execution::{
    object_store::{ObjectStoreRegistry, ObjectStoreUrl},
    TaskContext,
};
use datafusion::prelude::*;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::parse_json as arrow_parse_json;
use delta_kernel::{
    engine::default::{
        executor::tokio::{TokioBackgroundExecutor, TokioMultiThreadExecutor},
        json::DefaultJsonHandler,
        parquet::DefaultParquetHandler,
        stream_future_to_iter,
    },
    error::DeltaResult as KernelResult,
    schema::SchemaRef,
    EngineData, FileDataReadResultIterator, FileMeta, FilteredEngineData, JsonHandler,
    ParquetHandler, PredicateRef,
};
use futures::stream::BoxStream;
use futures::StreamExt;
use itertools::Itertools;
use std::sync::Arc;
use tokio::runtime::{Handle, RuntimeFlavor};

#[derive(Clone)]
pub struct DataFusionFileFormatHandler {
    ctx: Arc<TaskContext>,
    pq_registry: Arc<DashMap<ObjectStoreUrl, Arc<dyn ParquetHandler>>>,
    json_registry: Arc<DashMap<ObjectStoreUrl, Arc<dyn JsonHandler>>>,
    handle: Handle,
}

impl DataFusionFileFormatHandler {
    /// Create a new [`DatafusionParquetHandler`] instance.
    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        Self {
            ctx,
            pq_registry: DashMap::new().into(),
            json_registry: DashMap::new().into(),
            handle,
        }
    }

    fn registry(&self) -> Arc<dyn ObjectStoreRegistry> {
        self.ctx.runtime_env().object_store_registry.clone()
    }

    fn get_or_create_pq(
        &self,
        url: ObjectStoreUrl,
    ) -> KernelResult<Ref<'_, ObjectStoreUrl, Arc<dyn ParquetHandler>>> {
        if let Some(handler) = self.pq_registry.get(&url) {
            return Ok(handler);
        }
        let store = self
            .registry()
            .get_store(url.as_ref())
            .map_err(delta_kernel::Error::generic_err)?;

        let handler: Arc<dyn ParquetHandler> = match self.handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => Arc::new(DefaultParquetHandler::new(
                store,
                Arc::new(TokioMultiThreadExecutor::new(self.handle.clone())),
            )),
            RuntimeFlavor::CurrentThread => Arc::new(DefaultParquetHandler::new(
                store,
                Arc::new(TokioBackgroundExecutor::new()),
            )),
            _ => panic!("unsupported runtime flavor"),
        };

        self.pq_registry.insert(url.clone(), handler);
        Ok(self.pq_registry.get(&url).unwrap())
    }

    fn get_or_create_json(
        &self,
        url: ObjectStoreUrl,
    ) -> KernelResult<Ref<'_, ObjectStoreUrl, Arc<dyn JsonHandler>>> {
        if let Some(handler) = self.json_registry.get(&url) {
            return Ok(handler);
        }
        let store = self
            .registry()
            .get_store(url.as_ref())
            .map_err(delta_kernel::Error::generic_err)?;

        let handler: Arc<dyn JsonHandler> = match self.handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => Arc::new(DefaultJsonHandler::new(
                store,
                Arc::new(TokioMultiThreadExecutor::new(self.handle.clone())),
            )),
            RuntimeFlavor::CurrentThread => Arc::new(DefaultJsonHandler::new(
                store,
                Arc::new(TokioBackgroundExecutor::new()),
            )),
            _ => panic!("unsupported runtime flavor"),
        };

        self.json_registry.insert(url.clone(), handler);
        Ok(self.json_registry.get(&url).unwrap())
    }
}

impl ParquetHandler for DataFusionFileFormatHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        println!("read_parquet_files FILES: {files:#?}");
        // println!("read_parquet_files SCHEMA: {physical_schema:#?}");
        println!("read_parquet_files PREDICATE: {predicate:#?}");

        let grouped_files = group_by_store(files.to_vec());
        Ok(Box::new(
            grouped_files
                .into_iter()
                .map(|(url, files)| {
                    self.get_or_create_pq(url)?.read_parquet_files(
                        &files.to_vec(),
                        physical_schema.clone(),
                        predicate.clone(),
                    )
                })
                // TODO: this should not do any blocking operations, since this should
                // happen when the iterators are polled and we are just creating a vec of iterators.
                // Is this correct?
                .try_collect::<_, Vec<_>, _>()?
                .into_iter()
                .flatten(),
        ))
    }
}

impl JsonHandler for DataFusionFileFormatHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        println!("read_json_files FILES: {files:#?}");
        // println!("read_json_files SCHEMA: {physical_schema:#?}");
        println!("read_json_files PREDICATE: {predicate:#?}");

        let grouped_files = group_by_store(files.to_vec());
        Ok(Box::new(
            grouped_files
                .into_iter()
                .map(|(url, files)| {
                    self.get_or_create_json(url)?.read_json_files(
                        &files.to_vec(),
                        physical_schema.clone(),
                        predicate.clone(),
                    )
                })
                // TODO: this should not do any blocking operations, since this should
                // happen when the iterators are polled and we are just creating a vec of iterators.
                // Is this correct?
                .try_collect::<_, Vec<_>, _>()?
                .into_iter()
                .flatten(),
        ))
    }

    fn write_json_file(
        &self,
        path: &url::Url,
        data: Box<dyn Iterator<Item = KernelResult<FilteredEngineData>> + Send + '_>,
        overwrite: bool,
    ) -> KernelResult<()> {
        self.get_or_create_json(path.as_object_store_url())?
            .write_json_file(path, data, overwrite)
    }
}

// ============================================================================
// V2 Implementation using DataFusion for distributed execution
// ============================================================================

/// V2 Parquet handler that uses DataFusion's execution engine
#[derive(Clone)]
pub struct DataFusionFileFormatHandlerV2 {
    ctx: Arc<TaskContext>,
    handle: Handle,
    partition_filter: Option<PartitionFilter>,
}

impl DataFusionFileFormatHandlerV2 {
    pub fn new(
        ctx: Arc<TaskContext>,
        handle: Handle,
        partition_filter: Option<PartitionFilter>,
    ) -> Self {
        Self {
            ctx,
            handle,
            partition_filter,
        }
    }
}

impl ParquetHandler for DataFusionFileFormatHandlerV2 {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        println!(
            "DataFusionParquetHandlerV2::read_parquet_files called with {} files",
            files.len()
        );

        println!("read_parquet_files FILES: {files:#?}");
        // println!("read_parquet_files SCHEMA: {physical_schema:#?}");
        println!("read_parquet_files PREDICATE: {predicate:#?}");

        // Create the async future
        let future = read_files_with_datafusion(
            self.ctx.clone(),
            files.to_vec(),
            FileFormat::Parquet,
            physical_schema,
            predicate,
            self.partition_filter.clone(),
        );

        // Convert async stream to sync iterator using delta-kernel-rs utility
        match self.handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => stream_future_to_iter(
                Arc::new(TokioMultiThreadExecutor::new(self.handle.clone())),
                future,
            ),
            RuntimeFlavor::CurrentThread => {
                stream_future_to_iter(Arc::new(TokioBackgroundExecutor::new()), future)
            }
            _ => panic!("unsupported runtime flavor"),
        }
    }
}

impl JsonHandler for DataFusionFileFormatHandlerV2 {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        println!(
            "DataFusionParquetHandlerV2::read_json_files called with {} files",
            files.len()
        );

        println!("read_json_files FILES: {files:#?}");
        // println!("read_json_files SCHEMA: {physical_schema:#?}");
        println!("read_json_files PREDICATE: {predicate:#?}");

        // Create the async future
        let future = read_files_with_datafusion(
            self.ctx.clone(),
            files.to_vec(),
            FileFormat::Json,
            physical_schema,
            predicate,
            self.partition_filter.clone(),
        );

        // Convert async stream to sync iterator using delta-kernel-rs utility
        match self.handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => stream_future_to_iter(
                Arc::new(TokioMultiThreadExecutor::new(self.handle.clone())),
                future,
            ),
            RuntimeFlavor::CurrentThread => {
                stream_future_to_iter(Arc::new(TokioBackgroundExecutor::new()), future)
            }
            _ => panic!("unsupported runtime flavor"),
        }
    }

    fn write_json_file(
        &self,
        _path: &url::Url,
        _data: Box<dyn Iterator<Item = KernelResult<FilteredEngineData>> + Send + '_>,
        _overwrite: bool,
    ) -> KernelResult<()> {
        todo!()
    }
}

enum FileFormat {
    Parquet,
    Json,
}

/// Async function to read json files using DataFusion's SessionContext
async fn read_files_with_datafusion(
    task_ctx: Arc<TaskContext>,
    files: Vec<FileMeta>,
    file_format: FileFormat,
    physical_schema: SchemaRef,
    predicate: Option<PredicateRef>,
    partition_filter: Option<PartitionFilter>,
) -> KernelResult<BoxStream<'static, KernelResult<Box<dyn EngineData>>>> {
    if files.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    use delta_kernel::engine::arrow_conversion::TryIntoArrow;

    // Convert kernel schema to Arrow schema
    let schema = TryIntoArrow::<arrow_schema::Schema>::try_into_arrow(physical_schema.as_ref())
        .map_err(delta_kernel::Error::generic_err)?;

    // Create a SessionContext - we'll use the runtime from TaskContext
    let config = SessionConfig::new();
    // config = config.set_bool("datafusion.sql_parser.enable_ident_normalization", false);

    let session_ctx = SessionContext::new_with_config_rt(config, task_ctx.runtime_env().clone());

    // Get object store URL from first file (assuming single object store for milestone 1)
    // Convert FileMeta to file paths
    let file_paths: Vec<String> = files.iter().map(|f| f.location.to_string()).collect();

    let df = match file_format {
        FileFormat::Parquet => {
            let options = ParquetReadOptions::default().schema(&schema);
            session_ctx
                .read_parquet(file_paths, options)
                .await
                .map_err(|e| {
                    delta_kernel::Error::generic(format!("DataFusion read_parquet error: {}", e))
                })?
        }
        FileFormat::Json => {
            let options = NdJsonReadOptions::default().schema(&schema);

            // FIXME: sort by add.modificationTime DESC
            session_ctx
                .read_json(file_paths, options)
                .await
                .map_err(|e| {
                    delta_kernel::Error::generic(format!("DataFusion read_json error: {}", e))
                })?
        }
    };

    let df = if let Some(predicate) = predicate {
        let predicate_expr = predicate_to_datafusion_expr(predicate.as_ref()).map_err(|e| {
            delta_kernel::Error::generic(format!("DataFusion predicate error: {}", e))
        })?;

        // println!("DF PREDICATE: {predicate_expr:#?}");
        df.filter(predicate_expr)
            .map_err(|e| delta_kernel::Error::generic(format!("DataFusion filter error: {}", e)))?
    } else {
        df
    };

    // TODO: better schema check
    let df = if let (Some(pf), Ok(_)) = (partition_filter, schema.field_with_name("add")) {
        // FIXME don't apply partition pruning to removed files
        let predicate_expr = partition_filter_to_datafusion_expr(&pf).map_err(|e| {
            delta_kernel::Error::generic(format!("DataFusion partition filter error: {}", e))
        })?;

        println!("PARTITION FILTER: {pf:#?}");
        // println!("PREDICATE_EXPR: {predicate_expr:#?}");

        df.filter(predicate_expr)
            .map_err(|e| delta_kernel::Error::generic(format!("DataFusion filter error: {}", e)))?
    } else {
        df
    };

    // println!(
    //     "Physical plan: {:#?}",
    //     df.clone().create_physical_plan().await.map_err(|e| {
    //         delta_kernel::Error::generic(format!("DataFusion create plan error: {}", e))
    //     })?
    // );

    // Execute and get stream
    let stream = df.execute_stream().await.map_err(|e| {
        delta_kernel::Error::generic(format!("DataFusion execute_stream error: {}", e))
    })?;

    // Convert RecordBatch stream to EngineData stream
    let engine_data_stream = stream.map(|batch_result| {
        batch_result
            .map_err(|e| delta_kernel::Error::generic(format!("DataFusion stream error: {}", e)))
            .map(|batch| {
                // let _ = print_batches(&[batch.clone()]).unwrap();
                Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
            })
    });

    Ok(Box::pin(engine_data_stream))
}
