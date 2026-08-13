use std::error::Error;

use arrow_array::{Array, Int32Array, Int64Array, StringArray};
use deltalake_core::kernel::transaction::CommitBuilder;
use deltalake_core::kernel::{Action, Add, DataType, MetadataExt, StructField, StructType};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::{DeltaTable, DeltaTableBuilder};
use url::Url;

fn add(path: &str, stats: &str) -> Add {
    Add {
        path: path.to_string(),
        size: 1,
        modification_time: 1,
        data_change: true,
        stats: Some(stats.to_string()),
        ..Default::default()
    }
}

fn write_operation() -> DeltaOperation {
    DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    }
}

#[tokio::test]
async fn eager_cache_schema_evolution() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let table_url = Url::from_directory_path(temp_dir.path())
        .map_err(|_| "temporary table path is not a directory")?;
    let mut table = DeltaTable::try_from_url(table_url.clone())
        .await?
        .create()
        .with_columns([StructField::nullable("id", DataType::INTEGER)])
        .await?;

    CommitBuilder::default()
        .with_actions(vec![Action::Add(add(
            "old.parquet",
            r#"{"numRecords":3,"nullCount":{"id":0},"minValues":{"id":1,"added":"not-an-integer"},"maxValues":{"id":3}}"#,
        ))])
        .build(
            Some(table.snapshot()?),
            table.log_store(),
            write_operation(),
        )
        .await?;
    table.update_state().await?;

    // Reopen version 1 so its active Add batch is eagerly materialized with the old stats schema.
    let mut cached = DeltaTableBuilder::from_url(table_url)?.load().await?;
    assert_eq!(cached.version(), Some(1));

    let evolved_schema = StructType::try_new([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("added", DataType::INTEGER),
    ])?;
    let metadata = cached
        .snapshot()?
        .metadata()
        .clone()
        .with_schema(&evolved_schema)?;
    CommitBuilder::default()
        .with_actions(vec![
            Action::Metadata(metadata),
            Action::Add(add(
                "new.parquet",
                r#"{"numRecords":2,"nullCount":{"id":0,"added":0},"minValues":{"id":10,"added":100},"maxValues":{"id":20,"added":200}}"#,
            )),
        ])
        .build(Some(cached.snapshot()?), cached.log_store(), write_operation())
        .await?;

    // No checkpoint is created. The update must evolve the cached parsed struct and replay the
    // new JSON commit rather than reparsing raw statistics for the old file.
    cached.update_state().await?;
    assert_eq!(cached.version(), Some(2));
    let actions = cached.snapshot()?.add_actions_table(true)?;
    assert_eq!(actions.num_rows(), 2);

    let paths = actions
        .column_by_name("path")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let num_records = actions
        .column_by_name("num_records")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let null_id = actions
        .column_by_name("null_count.id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let min_id = actions
        .column_by_name("min.id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let max_id = actions
        .column_by_name("max.id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let null_added = actions
        .column_by_name("null_count.added")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let min_added = actions
        .column_by_name("min.added")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let max_added = actions
        .column_by_name("max.added")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    for row in 0..actions.num_rows() {
        match paths.value(row) {
            "old.parquet" => {
                assert_eq!(num_records.value(row), 3);
                assert_eq!(null_id.value(row), 0);
                assert_eq!(min_id.value(row), 1);
                assert_eq!(max_id.value(row), 3);
                assert!(null_added.is_null(row));
                assert!(min_added.is_null(row));
                assert!(max_added.is_null(row));
            }
            "new.parquet" => {
                assert_eq!(num_records.value(row), 2);
                assert_eq!(null_id.value(row), 0);
                assert_eq!(min_id.value(row), 10);
                assert_eq!(max_id.value(row), 20);
                assert_eq!(null_added.value(row), 0);
                assert_eq!(min_added.value(row), 100);
                assert_eq!(max_added.value(row), 200);
            }
            path => panic!("unexpected Add path {path}"),
        }
    }

    Ok(())
}
