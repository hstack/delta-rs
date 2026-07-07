use std::sync::Arc;
use crate::kernel::{EagerSnapshot, Snapshot};
use delta_kernel::snapshot::Snapshot as KernelSnapshot;

pub fn new_delta_eager_snapshot(snapshot: Snapshot) -> EagerSnapshot {
    EagerSnapshot {
        snapshot: Arc::new(snapshot),
    }
}

pub fn new_delta_snapshot(input: &Snapshot, kernel_snapshot: KernelSnapshot) -> Snapshot {
    Snapshot {
        inner: Arc::new(kernel_snapshot),
        config: input.config.clone(),
        materialized_files: input.materialized_files.clone(),
        load_metrics: input.load_metrics.clone(),
    }
}