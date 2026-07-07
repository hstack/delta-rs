//! Helpers for constructing Delta snapshots from existing snapshot state.

use crate::kernel::{EagerSnapshot, Snapshot};
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use std::sync::Arc;

/// Wrap an existing snapshot without replaying the transaction log.
pub fn new_delta_eager_snapshot(snapshot: Snapshot) -> EagerSnapshot {
    EagerSnapshot {
        snapshot: Arc::new(snapshot),
    }
}

/// Replace the kernel snapshot while preserving materialized state and load metrics.
pub fn new_delta_snapshot(input: &Snapshot, kernel_snapshot: KernelSnapshot) -> Snapshot {
    let mut snapshot = input.clone();
    snapshot.inner = Arc::new(kernel_snapshot);
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{TestResult, TestTables};

    #[tokio::test]
    async fn manual_snapshot_construction() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let input = Snapshot::try_new(log_store.as_ref(), Some(0)).await?;
        let kernel_snapshot = KernelSnapshot::builder_for(log_store.root_url().clone())
            .build(log_store.engine(None).as_ref())?;
        let kernel_snapshot =
            Arc::try_unwrap(kernel_snapshot).expect("kernel snapshot is not shared");

        let snapshot = new_delta_snapshot(&input, kernel_snapshot);
        assert_eq!(input.version(), 0);
        assert_eq!(snapshot.inner.version(), 4);
        assert_eq!(snapshot.load_metrics(), input.load_metrics());

        let eager = new_delta_eager_snapshot(snapshot.clone());
        assert_eq!(eager.version(), 4);
        assert_eq!(eager.snapshot.as_ref(), &snapshot);
        Ok(())
    }
}
