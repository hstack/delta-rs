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
use deltalake_core::delta_datafusion::DeltaPhysicalCodec;
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
        if let Some(dse) = pp.downcast_ref::<DataSourceExec>() {
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
