//! File-level row-count accumulator for `LIMIT` pushdown.
//!
//! This module owns the correctness logic that decides — given a stream of
//! [`ScanFileContext`]s and a row cap — which files to keep and which to drop.
//! It is intentionally pure (no async, no I/O) so it can be unit-tested in
//! isolation from `replay_files` and the kernel scan-metadata stream.
//!
//! Rules:
//! - Files with a deletion vector (`has_dv == true`) go into a set-aside pool,
//!   because a DV may delete an arbitrary number of rows and `num_records`
//!   would overstate the file's contribution to the limit.
//! - Files missing `num_records` stats go into the same set-aside pool for the
//!   same reason.
//! - Files with exact stats and no DV accumulate; once `rows_collected >=
//!   limit` the outer loop should break.
//! - At finalize time, the set-aside pool is drained into the kept list iff
//!   the accumulator is still short of the limit; otherwise its entries are
//!   counted as pruned.

use datafusion::common::stats::Precision;
use tracing::info;
use super::replay::ScanFileContext;

/// Per-scan accumulator for file-level limit pruning. See module docs.
#[derive(Debug)]
pub(crate) struct LimitPruneState {
    limit: usize,
    rows_collected: usize,
    files_with_exact_num_records: Vec<ScanFileContext>,
    files_with_unknown_num_records: Vec<ScanFileContext>,
}

impl LimitPruneState {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            rows_collected: 0,
            files_with_exact_num_records: Vec::new(),
            files_with_unknown_num_records: Vec::new(),
        }
    }

    /// Try to accept one file. Returns `true` when the row cap has been reached
    /// and the outer scan-metadata loop should break.
    pub(crate) fn accept(&mut self, file: ScanFileContext) -> bool {
        if file.has_deletion_vector {
            self.files_with_unknown_num_records.push(file);
            return false;
        }
        info!("ACCEPT {} = {:?}", file.file_url.as_str(), file.stats);
        match file.stats.num_rows {
            Precision::Exact(n) => {
                self.rows_collected = self.rows_collected.saturating_add(n);
                self.files_with_exact_num_records.push(file);
                self.rows_collected >= self.limit
            }
            _ => {
                self.files_with_unknown_num_records.push(file);
                false
            }
        }
    }

    /// Consume the state, returning `(kept_files, count_files_pruned_by_limit)`.
    ///
    /// The set-aside pool is drained into `kept_files` only when the accumulator
    /// is still short of the row cap; otherwise its entries are counted as pruned.
    pub(crate) fn finalize(mut self) -> (Vec<ScanFileContext>, usize) {
        if self.rows_collected < self.limit {
            self.files_with_exact_num_records.extend(self.files_with_unknown_num_records.drain(..));
            (self.files_with_exact_num_records, 0)
        } else {
            let pruned = self.files_with_unknown_num_records.len();
            (self.files_with_exact_num_records, pruned)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::common::Statistics;
    use url::Url;

    use crate::delta_datafusion::table_provider::next::scan::replay::ScanFileContext;

    fn stub_file(url: &str, num_rows: Precision<usize>, has_deletion_vector: bool) -> ScanFileContext {
        let stats = Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics: Vec::new(),
        };
        ScanFileContext {
            file_url: Url::parse(url).unwrap(),
            size: 0,
            transform: None,
            stats,
            partitions: None,
            has_deletion_vector,
        }
    }

    #[test]
    fn accept_exact_stats_accumulates_until_cap_reached() {
        let mut state = LimitPruneState::new(10);

        let stop = state.accept(stub_file("file:///a", Precision::Exact(4), false));
        assert!(
            !stop,
            "accumulator at 4 rows should not signal stop with cap=10"
        );

        let stop = state.accept(stub_file("file:///b", Precision::Exact(7), false));
        assert!(
            stop,
            "accumulator at 11 rows should signal stop with cap=10"
        );

        let (files, pruned) = state.finalize();
        assert_eq!(files.len(), 2);
        assert_eq!(pruned, 0);
    }

    #[test]
    fn dv_files_go_to_unknown_pool() {
        let mut state = LimitPruneState::new(5);

        let stop = state.accept(stub_file("file:///dv", Precision::Exact(1000), true));
        assert!(
            !stop,
            "DV files must not contribute to row count regardless of stats"
        );

        let (files, pruned) = state.finalize();
        assert_eq!(
            files.len(),
            1,
            "DV file should be drained from unknown pool when limit unmet"
        );
        assert_eq!(pruned, 0);
    }

    #[test]
    fn absent_stats_go_to_unknown_pool() {
        let mut state = LimitPruneState::new(5);

        let stop = state.accept(stub_file("file:///stats-less", Precision::Absent, false));
        assert!(!stop);

        let (files, pruned) = state.finalize();
        assert_eq!(files.len(), 1);
        assert_eq!(pruned, 0);
    }

    #[test]
    fn unknown_pool_is_dropped_when_cap_already_satisfied() {
        let mut state = LimitPruneState::new(3);

        // DV file goes into unknown pool (does not contribute to rows_collected).
        let _ = state.accept(stub_file("file:///dv", Precision::Exact(10), true));
        // Exact-stats file saturates the cap.
        let stop = state.accept(stub_file("file:///exact", Precision::Exact(5), false));
        assert!(stop, "exact 5 rows should saturate cap=3");

        let (files, pruned) = state.finalize();
        assert_eq!(files.len(), 1, "only the exact-stats file should be kept");
        assert_eq!(pruned, 1, "the unknown-pool DV file should count as pruned");
    }

    #[test]
    fn unknown_pool_is_drained_when_cap_unmet() {
        let mut state = LimitPruneState::new(100);

        let _ = state.accept(stub_file("file:///exact", Precision::Exact(5), false));
        let _ = state.accept(stub_file("file:///dv", Precision::Exact(10), true));
        let _ = state.accept(stub_file("file:///no-stats", Precision::Absent, false));

        let (files, pruned) = state.finalize();
        assert_eq!(
            files.len(),
            3,
            "all files should be returned when cap is not reached"
        );
        assert_eq!(pruned, 0);
    }

    #[test]
    fn inexact_stats_go_to_unknown_pool() {
        let mut state = LimitPruneState::new(5);

        let stop = state.accept(stub_file("file:///inexact", Precision::Inexact(3), false));
        assert!(!stop);

        let (files, pruned) = state.finalize();
        assert_eq!(files.len(), 1);
        assert_eq!(pruned, 0);
    }

    #[test]
    fn saturating_add_handles_pathological_num_records() {
        let mut state = LimitPruneState::new(10);
        let stop = state.accept(stub_file(
            "file:///big",
            Precision::Exact(usize::MAX),
            false,
        ));
        assert!(
            stop,
            "even saturated accumulator must signal stop once cap is reached"
        );

        let (files, _) = state.finalize();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn limit_greater_than_total_keeps_everything() {
        let mut state = LimitPruneState::new(1_000_000);

        for i in 0..3 {
            let url = format!("file:///f{i}");
            let stop = state.accept(stub_file(&url, Precision::Exact(10), false));
            assert!(!stop, "small files should never trip a generous cap");
        }

        let (files, pruned) = state.finalize();
        assert_eq!(files.len(), 3);
        assert_eq!(pruned, 0);
    }
}

