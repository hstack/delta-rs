#![cfg(feature = "datafusion")]
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Once;
use arrow_cast::display::FormatOptions;
use arrow_cast::pretty;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::parquet::push_all_projection_hints::PushAllProjectionHints;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::source::DataSourceExec;
use datafusion_proto::physical_plan::{AsExecutionPlan, ComposedPhysicalExtensionCodec, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use tracing::info;
use deltalake_core::delta_datafusion::{DeltaNextPhysicalCodec, DeltaPhysicalCodec, DeltaScanExec};
use deltalake_core::delta_datafusion::udtf::register_delta_table_udtf;

/// DF 53 stores deep-projection state on `ParquetSource` as
/// `projection_hints: ProjectionExprs` + `projection_hints_indices: Vec<usize>`,
/// populated post-planning by the `PushAllProjectionHints` physical optimizer rule.
/// `ProjectionExprs` derives `PartialEq`, sufficient for serde round-trip equality.
fn extract_projection_deep_from_plan(
    plan: Arc<dyn ExecutionPlan>,
) -> Vec<(ProjectionExprs, Vec<usize>)> {
    let mut deep_projections: Vec<(ProjectionExprs, Vec<usize>)> = vec![];
    let _ = plan.apply(|pp| {
        if let Some(dse) = pp.as_any().downcast_ref::<DataSourceExec>() {
            if let Some((_file_scan_conf, parquet_source)) =
                dse.downcast_to_file_source::<ParquetSource>()
            {
                deep_projections.push((
                    parquet_source.projection_hints.clone(),
                    parquet_source.projection_hints_indices.clone(),
                ));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    deep_projections
}

/// Setting `tracing`'s global default subscriber is a one-shot per process,
/// so multiple tests in the same binary must share a single init.
fn init_tracing() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        let subscriber = tracing_subscriber::fmt()
            .pretty()
            .with_env_filter(filter)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
        let _ = pretty_env_logger::try_init();
    });
}

/// Build a `SessionContext` with the DF 53 `PushAllProjectionHints` physical
/// optimizer rule registered. Without this rule, `ParquetSource.projection_hints`
/// stays empty and the test's serde assertion is trivially true.
fn build_context(config: SessionConfig) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::new(RuntimeEnv::default()))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(PushAllProjectionHints {}))
        .build();
    SessionContext::new_with_state(state)
}

#[tokio::test]
async fn test_hstack_deep_column_pruning() -> datafusion::common::Result<()> {
    unsafe { std::env::set_var("DELTA_USE_EXPR_ADAPTER", "1"); }
    init_tracing();

    let config = SessionConfig::new()
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false);

    let ctx = build_context(config);

    register_delta_table_udtf(&ctx, None, None);

    let delta_path = format!(
        "{}/tests/data/deep",
        env!("CARGO_MANIFEST_DIR")
    );

    let query = format!(r#"
        select
            t1._id, t1.productListItems['SKU'], _ACP_DATE
        from
            delta_table('file://{}') as t1
        "#, delta_path);

    let plan = ctx.state().create_logical_plan(&query).await.expect("Error creating logical plan");
    let optimized_plan = ctx.state().optimize(&plan).expect("Error optimizing plan");
    let state = ctx.state();
    let query_planner = state.query_planner().clone();
    let physical_plan = query_planner
        .create_physical_plan(&optimized_plan, &state)
        .await.expect("Error creating physical plan");
    info!(
            "Physical plan: {}",
            displayable(physical_plan.deref()).set_show_schema(true).indent(true)
        );
    let proj1 = extract_projection_deep_from_plan(physical_plan.clone());
    let batches1 = collect(physical_plan.clone(), ctx.state().task_ctx()).await?;
    let results1 = pretty::pretty_format_batches_with_options(&batches1, &FormatOptions::default())?.to_string();
    println!("{}", results1);

    // codec
    let codec = ComposedPhysicalExtensionCodec::new(
        vec![
            Arc::new(DefaultPhysicalExtensionCodec {}),
            Arc::new(DeltaPhysicalCodec{})
        ]
    );
    let proto = PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)
        .unwrap();
    let bytes = proto.encode_to_vec();
    let plan_after_serde = PhysicalPlanNode::try_decode(&bytes)
        .expect("Error try_decode")
        .try_into_physical_plan(&ctx.task_ctx(), &codec)
        .expect("try_into_physical_plan");
    info!(
            "Physical plan after serde: {}",
            displayable(plan_after_serde.deref()).set_show_schema(true).indent(true)
        );
    let _ = plan_after_serde.apply(|plan| {
        if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
            if let Some((file_scan, other)) = exec.downcast_to_file_source::<ParquetSource>() {
                if let Some(expr_adapter) = file_scan.expr_adapter_factory.clone() {
                    let debug_format = format!("{:?}", expr_adapter);
                    info!("FOUND IT: {}", debug_format);
                    // can't downcast here, no as_any for PhysicalExprAdapter
                    assert!(debug_format.contains("DeltaPhysicalExprAdapter"), "FileScanConfig does not have DeltaPhysicalExprAdapter after serde !");
                } else {
                    assert_eq!(true, false, "FileScanConfig does not have an expr_adapter !");
                }
            } else {
                assert_eq!(true, false, "DataSourceExec is not a file source !");
            }
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });

    let proj2 = extract_projection_deep_from_plan(plan_after_serde.clone());
    let batches2 = collect(plan_after_serde.clone(), ctx.state().task_ctx()).await?;
    let results2 = pretty::pretty_format_batches_with_options(&batches2, &FormatOptions::default())?.to_string();
    println!("{}", results2);

    assert_eq!(results1, results2, "Batches not equal !");
    println!("proj1: {:?}", proj1);
    println!("proj2: {:?}", proj2);

    assert_eq!(proj1, proj2, "Deep Projection not equal !");
    Ok(())
}


#[tokio::test]
async fn test_hstack_nullable_new() -> datafusion::common::Result<()> {
    unsafe { std::env::set_var("DELTA_USE_EXPR_ADAPTER", "1"); }
    init_tracing();

    let config = SessionConfig::new()
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false)
        .set_bool("datafusion.execution.parquet.schema_force_view_types", false);

    let ctx = build_context(config);

    register_delta_table_udtf(&ctx, None, None);

    let delta_path = format!(
        "{}/tests/data/hstack_nullable_difference",
        env!("CARGO_MANIFEST_DIR")
    );

    let query = format!(r#"
        select
            *
        from
            delta_table('file://{}') as t1
        "#, delta_path);

    let plan = ctx.state().create_logical_plan(&query).await.expect("Error creating logical plan");
    let optimized_plan = ctx.state().optimize(&plan).expect("Error optimizing plan");
    let state = ctx.state();
    let query_planner = state.query_planner().clone();
    let physical_plan = query_planner
        .create_physical_plan(&optimized_plan, &state)
        .await.expect("Error creating physical plan");
    info!(
            "Physical plan: {}",
            displayable(physical_plan.deref()).set_show_schema(true).indent(true)
        );
    let proj1 = extract_projection_deep_from_plan(physical_plan.clone());
    let batches1 = collect(physical_plan.clone(), ctx.state().task_ctx()).await?;
    let results1 = pretty::pretty_format_batches_with_options(&batches1, &FormatOptions::default())?.to_string();
    println!("{}", results1);

    Ok(())
}

#[tokio::test]
async fn test_hstack_deep_column_pruning_next_codec() -> datafusion::common::Result<()> {
    unsafe { std::env::set_var("DELTA_USE_EXPR_ADAPTER", "1"); }

    let filter = tracing_subscriber::EnvFilter::from_default_env();
    let subscriber = tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber).ok();
    pretty_env_logger::try_init().ok();

    let config = SessionConfig::new()
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false);

    let ctx = SessionContext::new_with_config(config);

    register_delta_table_udtf(&ctx, None, None);

    let delta_path = format!(
        "{}/tests/data/deep",
        env!("CARGO_MANIFEST_DIR")
    );

    let query = format!(r#"
        select
            t1._id, t1.productListItems['SKU'], _ACP_DATE
        from
            delta_table_next('file://{}') as t1
        "#, delta_path);

    let plan = ctx.state().create_logical_plan(&query).await.expect("Error creating logical plan");
    let optimized_plan = ctx.state().optimize(&plan).expect("Error optimizing plan");
    let state = ctx.state();
    let query_planner = state.query_planner().clone();
    let physical_plan = query_planner
        .create_physical_plan(&optimized_plan, &state)
        .await.expect("Error creating physical plan");
    info!(
            "Physical plan: {}",
            displayable(physical_plan.deref()).set_show_schema(true).indent(true)
        );
    let proj1 = extract_projection_deep_from_plan(physical_plan.clone());
    let batches1 = collect(physical_plan.clone(), ctx.state().task_ctx()).await?;
    let results1 = pretty::pretty_format_batches_with_options(&batches1, &FormatOptions::default())?.to_string();
    println!("{}", results1);

    // codec
    let codec = ComposedPhysicalExtensionCodec::new(
        vec![
            Arc::new(DefaultPhysicalExtensionCodec {}),
            Arc::new(DeltaNextPhysicalCodec{})
        ]
    );

    let proto = PhysicalPlanNode::try_from_physical_plan(physical_plan.clone(), &codec)
        .unwrap();
    let bytes = proto.encode_to_vec();
    let plan_after_serde = PhysicalPlanNode::try_decode(&bytes)
        .expect("Error try_decode")
        .try_into_physical_plan(&ctx.task_ctx(), &codec)
        .expect("try_into_physical_plan");
    info!(
            "Physical plan after serde: {}",
            displayable(plan_after_serde.deref()).set_show_schema(true).indent(true)
        );

    let delta_scan = find_exec_node::<DeltaScanExec>(&plan_after_serde)
        .expect("Error finding DeltaScanExec");
    let data_source_exec = find_exec_node::<DataSourceExec>(&plan_after_serde)
        .expect("Error finding DataSourceExec");
    let (file_scan, other) = data_source_exec
        .downcast_to_file_source::<ParquetSource>()
        .expect("DataSourceExec is not a file source");
    if let Some(expr_adapter) = file_scan.expr_adapter_factory.clone() {
        let debug_format = format!("{:?}", expr_adapter);
        // can't downcast here, no as_any for PhysicalExprAdapter
        assert!(debug_format.contains("DeltaPhysicalExprAdapter"), "FileScanConfig does not have DeltaPhysicalExprAdapter after serde !");
    } else {
        assert_eq!(true, false, "FileScanConfig does not have an expr_adapter !");
    }

    let proj2 = extract_projection_deep_from_plan(plan_after_serde.clone());
    let batches2 = collect(plan_after_serde.clone(), ctx.state().task_ctx()).await?;
    let results2 = pretty::pretty_format_batches_with_options(&batches2, &FormatOptions::default())?.to_string();
    println!("{}", results2);

    assert_eq!(results1, results2, "Batches not equal !");
    println!("proj1: {:?}", proj1);
    println!("proj2: {:?}", proj2);

    assert_eq!(proj1, proj2, "Deep Projection not equal !");

    Ok(())
}

fn find_exec_node<T: ExecutionPlan + 'static>(input: &Arc<dyn ExecutionPlan>) -> Option<&T> {
    if let Some(found) = input.as_any().downcast_ref::<T>() {
        Some(found)
    } else {
        input.children().iter()
            .find_map(|child| find_exec_node(child))
    }
}
