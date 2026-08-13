use delta_kernel::expressions::ColumnName;
use serde_json::{Value, json};

use super::*;

fn serialized(options: StatsOptions) -> Value {
    serde_json::to_value(options).expect("StatsOptions should serialize")
}

fn assert_typed_options(materialization: FileStatsMaterialization, expected: StatsOptions) {
    let actual = serialized(kernel_stats_options(&materialization, false));
    assert_eq!(actual, serialized(expected));
    assert_eq!(
        actual.get("checkpoint_stats_json_fallback"),
        Some(&json!(false))
    );
}

#[test]
fn compatibility_full_disables_checkpoint_json_fallback() {
    assert_typed_options(
        FileStatsMaterialization::compatibility(StatsProjection::Full),
        StatsOptions::all_struct().with_checkpoint_stats_json_fallback(false),
    );
}

#[test]
fn query_full_disables_checkpoint_json_fallback() {
    assert_typed_options(
        FileStatsMaterialization::query(StatsProjection::Full),
        StatsOptions::all_struct().with_checkpoint_stats_json_fallback(false),
    );
}

#[test]
fn predicate_columns_disable_checkpoint_json_fallback() {
    let columns = [ColumnName::new(["value"])].into();
    assert_typed_options(
        FileStatsMaterialization::query(StatsProjection::PredicateColumns(columns)),
        StatsOptions::struct_columns(vec![ColumnName::new(["value"])])
            .with_checkpoint_stats_json_fallback(false),
    );
}

#[test]
fn row_count_only_disables_checkpoint_json_fallback() {
    assert_typed_options(
        FileStatsMaterialization::query(StatsProjection::NumRecordsOnly),
        StatsOptions::struct_columns(vec![]).with_checkpoint_stats_json_fallback(false),
    );
}

#[test]
fn disabled_stats_map_to_none() {
    let none = serialized(StatsOptions::none());
    assert_eq!(
        serialized(kernel_stats_options(
            &FileStatsMaterialization::query(StatsProjection::None),
            false,
        )),
        none
    );
    assert_eq!(
        serialized(kernel_stats_options(
            &FileStatsMaterialization::compatibility(StatsProjection::None),
            false,
        )),
        none
    );
    assert_eq!(
        serialized(kernel_stats_options(
            &FileStatsMaterialization::without_stats(),
            false,
        )),
        none
    );
}
