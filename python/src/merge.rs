use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::arrow::ffi_stream::ArrowArrayStreamReader;
use deltalake::arrow::pyarrow::IntoPyArrow;
use deltalake::datafusion::catalog::TableProvider;
use deltalake::datafusion::datasource::MemTable;
use deltalake::datafusion::physical_plan::memory::LazyBatchGenerator;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::LazyTableProvider;
use deltalake::logstore::LogStoreRef;
use deltalake::operations::merge::MergeBuilder;
use deltalake::operations::CustomExecuteHandler;
use deltalake::table::state::DeltaTableState;
use deltalake::{DeltaResult, DeltaTable};
use parking_lot::RwLock;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fmt::{self};
use std::future::IntoFuture;
use std::sync::{Arc, Mutex};

use crate::error::PythonError;
use crate::utils::rt;
use crate::{
    maybe_create_commit_properties, set_writer_properties, PyCommitProperties,
    PyPostCommitHookProperties, PyWriterProperties,
};

#[pyclass(module = "deltalake._internal")]
pub(crate) struct PyMergeBuilder {
    _builder: Option<MergeBuilder>,
    #[pyo3(get)]
    source_alias: Option<String>,
    #[pyo3(get)]
    target_alias: Option<String>,
    #[pyo3(get)]
    merge_schema: bool,
    arrow_schema: Arc<ArrowSchema>,
}
#[derive(Debug)]
struct ArrowStreamBatchGenerator {
    pub array_stream: Arc<Mutex<ArrowArrayStreamReader>>,
}

impl fmt::Display for ArrowStreamBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArrowStreamBatchGenerator {{ array_stream: {:?} }}",
            self.array_stream
        )
    }
}

impl ArrowStreamBatchGenerator {
    fn new(array_stream: Arc<Mutex<ArrowArrayStreamReader>>) -> Self {
        Self { array_stream }
    }
}

impl LazyBatchGenerator for ArrowStreamBatchGenerator {
    fn generate_next_batch(
        &mut self,
    ) -> deltalake::datafusion::error::Result<Option<deltalake::arrow::array::RecordBatch>> {
        let mut stream_reader = self.array_stream.lock().map_err(|_| {
            deltalake::datafusion::error::DataFusionError::Execution(
                "Failed to lock the ArrowArrayStreamReader".to_string(),
            )
        })?;

        match stream_reader.next() {
            Some(Ok(record_batch)) => Ok(Some(record_batch)),
            Some(Err(err)) => Err(deltalake::datafusion::error::DataFusionError::ArrowError(
                err, None,
            )),
            None => Ok(None), // End of stream
        }
    }
}

impl PyMergeBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_store: LogStoreRef,
        snapshot: DeltaTableState,
        source: ArrowArrayStreamReader,
        predicate: String,
        source_alias: Option<String>,
        target_alias: Option<String>,
        merge_schema: bool,
        safe_cast: bool,
        streamed_exec: bool,
        writer_properties: Option<PyWriterProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
        commit_properties: Option<PyCommitProperties>,
        custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
    ) -> DeltaResult<Self> {
        let ctx = SessionContext::new();
        let schema = source.schema();

        let source_df = if streamed_exec {
            let arrow_stream: Arc<Mutex<ArrowArrayStreamReader>> = Arc::new(Mutex::new(source));
            let arrow_stream_batch_generator: Arc<RwLock<dyn LazyBatchGenerator>> =
                Arc::new(RwLock::new(ArrowStreamBatchGenerator::new(arrow_stream)));

            let table_provider: Arc<dyn TableProvider> = Arc::new(LazyTableProvider::try_new(
                schema.clone(),
                vec![arrow_stream_batch_generator],
            )?);
            ctx.read_table(table_provider).unwrap()
        } else {
            let batches = vec![source.map(|batch| batch.unwrap()).collect::<Vec<_>>()];
            let table_provider: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(schema.clone(), batches).unwrap());
            ctx.read_table(table_provider).unwrap()
        };

        let mut cmd = MergeBuilder::new(log_store, snapshot, predicate, source_df)
            .with_safe_cast(safe_cast)
            .with_streaming(streamed_exec);

        if let Some(src_alias) = &source_alias {
            cmd = cmd.with_source_alias(src_alias);
        }

        if let Some(trgt_alias) = &target_alias {
            cmd = cmd.with_target_alias(trgt_alias);
        }

        cmd = cmd.with_merge_schema(merge_schema);

        if let Some(writer_props) = writer_properties {
            cmd = cmd.with_writer_properties(set_writer_properties(writer_props)?);
        }

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        if let Some(handler) = custom_execute_handler {
            cmd = cmd.with_custom_execute_handler(handler);
        }

        Ok(Self {
            _builder: Some(cmd),
            source_alias,
            target_alias,
            merge_schema,
            arrow_schema: schema,
        })
    }

    pub fn execute(&mut self) -> DeltaResult<(DeltaTable, String)> {
        let (table, metrics) = rt().block_on(self._builder.take().unwrap().into_future())?;
        Ok((table, serde_json::to_string(&metrics).unwrap()))
    }
}

#[pymethods]
impl PyMergeBuilder {
    #[getter]
    fn get_arrow_schema(&self, py: Python) -> PyResult<PyObject> {
        <arrow_schema::Schema as Clone>::clone(&self.arrow_schema).into_pyarrow(py)
    }

    #[pyo3(signature=(
        updates,
        predicate = None,
    ))]
    fn when_matched_update(
        &mut self,
        updates: HashMap<String, String>,
        predicate: Option<String>,
    ) -> PyResult<()> {
        self._builder = match self._builder.take() {
            Some(cmd) => Some(
                cmd.when_matched_update(|mut update| {
                    for (column, expression) in updates {
                        update = update.update(column, expression)
                    }
                    if let Some(predicate) = predicate {
                        update = update.predicate(predicate)
                    };
                    update
                })
                .map_err(PythonError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }

    #[pyo3(signature=(
        predicate = None,
    ))]
    fn when_matched_delete(&mut self, predicate: Option<String>) -> PyResult<()> {
        self._builder = match self._builder.take() {
            Some(cmd) => Some(
                cmd.when_matched_delete(|mut delete| {
                    if let Some(predicate) = predicate {
                        delete = delete.predicate(predicate)
                    };
                    delete
                })
                .map_err(PythonError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }

    #[pyo3(signature=(
        updates,
        predicate = None,
    ))]
    fn when_not_matched_insert(
        &mut self,
        updates: HashMap<String, String>,
        predicate: Option<String>,
    ) -> PyResult<()> {
        self._builder = match self._builder.take() {
            Some(cmd) => Some(
                cmd.when_not_matched_insert(|mut insert| {
                    for (column, expression) in updates {
                        insert = insert.set(column, expression)
                    }
                    if let Some(predicate) = predicate {
                        insert = insert.predicate(predicate)
                    };
                    insert
                })
                .map_err(PythonError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }

    #[pyo3(signature=(
        updates,
        predicate = None,
    ))]
    fn when_not_matched_by_source_update(
        &mut self,
        updates: HashMap<String, String>,
        predicate: Option<String>,
    ) -> PyResult<()> {
        self._builder = match self._builder.take() {
            Some(cmd) => Some(
                cmd.when_not_matched_by_source_update(|mut update| {
                    for (column, expression) in updates {
                        update = update.update(column, expression)
                    }
                    if let Some(predicate) = predicate {
                        update = update.predicate(predicate)
                    };
                    update
                })
                .map_err(PythonError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }

    #[pyo3(signature=(
        predicate = None,
    ))]
    fn when_not_matched_by_source_delete(&mut self, predicate: Option<String>) -> PyResult<()> {
        self._builder = match self._builder.take() {
            Some(cmd) => Some(
                cmd.when_not_matched_by_source_delete(|mut delete| {
                    if let Some(predicate) = predicate {
                        delete = delete.predicate(predicate)
                    };
                    delete
                })
                .map_err(PythonError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }
}
