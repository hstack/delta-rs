use std::sync::Arc;

use self::file_formats::{DataFusionFileFormatHandler, DataFusionFileFormatHandlerV2};
use self::storage::DataFusionStorageHandler;
use crate::kernel::ARROW_HANDLER;
use crate::PartitionFilter;
use datafusion::catalog::Session;
use datafusion::execution::TaskContext;
use delta_kernel::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};
use tokio::runtime::Handle;

mod file_formats;
mod predicate_conversion;
pub mod storage;

/// A Datafusion based Kernel Engine
#[derive(Clone)]
pub struct DataFusionEngine {
    storage: Arc<DataFusionStorageHandler>,
    formats: Arc<DataFusionFileFormatHandler>,
}

impl DataFusionEngine {
    pub fn new_from_session(session: &dyn Session) -> Arc<Self> {
        Self::new(session.task_ctx(), Handle::current()).into()
    }

    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        let storage = Arc::new(DataFusionStorageHandler::new(ctx.clone(), handle.clone()));
        let formats = Arc::new(DataFusionFileFormatHandler::new(ctx, handle));
        Self { storage, formats }
    }
}

impl Engine for DataFusionEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        ARROW_HANDLER.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.formats.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.formats.clone()
    }
}

// ============================================================================
// V2 Implementation using DataFusion for distributed execution
// ============================================================================

/// V2 Datafusion based Kernel Engine that uses DataFusion's execution engine
/// for reading parquet files instead of delegating to the default kernel implementation
#[derive(Clone)]
pub struct DataFusionEngineV2 {
    ctx: Arc<TaskContext>,
    handle: Handle,
    storage: Arc<DataFusionStorageHandler>,
    partition_filter: Option<PartitionFilter>,
}

impl DataFusionEngineV2 {
    pub fn new_from_session(session: &dyn Session) -> Arc<Self> {
        Self::new(session.task_ctx(), Handle::current()).into()
    }

    pub fn new(ctx: Arc<TaskContext>, handle: Handle) -> Self {
        let storage = Arc::new(DataFusionStorageHandler::new(ctx.clone(), handle.clone()));
        Self {
            ctx,
            handle,
            storage,
            partition_filter: None,
        }
    }

    pub fn with_partition_filter(mut self, filter: PartitionFilter) -> Self {
        self.partition_filter = Some(filter);
        self
    }

    fn formats(&self) -> Arc<DataFusionFileFormatHandlerV2> {
        Arc::new(DataFusionFileFormatHandlerV2::new(
            self.ctx.clone(),
            self.handle.clone(),
            self.partition_filter.clone(),
        ))
    }
}

impl Engine for DataFusionEngineV2 {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        ARROW_HANDLER.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.formats()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.formats()
    }
}
