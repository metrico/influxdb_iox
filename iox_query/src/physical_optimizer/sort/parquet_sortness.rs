use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_expr::{PhysicalSortExpr, PhysicalSortRequirement},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        file_format::{FileScanConfig, ParquetExec},
        sorts::sort::SortExec,
        ExecutionPlan,
    },
};
use observability_deps::tracing::{warn, info};

use crate::config::IoxConfigExt;

/// Trade wider fan-out of not having to sort parquet files.
///
/// This will fan-out [`ParquetExec`] nodes beyond [`target_partitions`] if it is under a node that desires sorting, e.g.:
///
/// - [`SortExec`] itself
/// - any other node that requires sorting, e.g. [`DeduplicateExec`]
///
/// [`DeduplicateExec`]: crate::provider::DeduplicateExec
/// [`target_partitions`]: datafusion::common::config::ExecutionOptions::target_partitions
#[derive(Debug, Default)]
pub struct ParquetSortness;

impl PhysicalOptimizerRule for ParquetSortness {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let Some(children_with_sort) = detect_children_with_desired_ordering(plan.as_ref()) else {
                return Ok(Transformed::No(plan));
            };

            let mut children_new = Vec::with_capacity(children_with_sort.len());
            let mut transformed_any = false;
            for (mut child, input_ordering) in children_with_sort {
                let transformed_child = Arc::clone(&child).transform_down(&|plan| {
                    if detect_children_with_desired_ordering(plan.as_ref()).is_some() {
                        // another sort or sort-desiring node
                        return Ok(Transformed::No(plan));
                    }

                    let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() else {
                        // not a parquet exec
                        return Ok(Transformed::No(plan));
                    };

                    let base_config = parquet_exec.base_config();
                    info!(output_ordering=?base_config.output_ordering, "AAL initial ordering input");
                    if base_config.output_ordering.is_none() {
                        // no output ordering requested
                        return Ok(Transformed::No(plan));
                    }

                    if base_config.file_groups.iter().all(|g| g.len() < 2) {
                        // already flat
                        return Ok(Transformed::No(plan));
                    }

                    // Protect against degenerative plans
                    let n_files = base_config.file_groups.iter().map(Vec::len).sum::<usize>();
                    let max_parquet_fanout = config
                        .extensions
                        .get::<IoxConfigExt>()
                        .cloned()
                        .unwrap_or_default()
                        .max_parquet_fanout;
                    if n_files > max_parquet_fanout {
                        warn!(
                            n_files,
                            max_parquet_fanout, "cannot use pre-sorted parquet files, fan-out too wide"
                        );
                        return Ok(Transformed::No(plan));
                    }

                    let base_config = FileScanConfig {
                        file_groups: base_config
                            .file_groups
                            .iter()
                            .flat_map(|g| g.iter())
                            .map(|f| vec![f.clone()])
                            .collect(),
                        ..base_config.clone()
                    };

                    info!(output_ordering=?base_config.output_ordering, "new ordering input");
                    let new_parquet_exec =
                        ParquetExec::new(base_config, parquet_exec.predicate().cloned(), None);
                    Ok(Transformed::Yes(Arc::new(new_parquet_exec)))
                })?;

                // did this help?
                if transformed_child.output_ordering() == Some(&input_ordering) {
                    child = transformed_child;
                    transformed_any = true;
                }
                children_new.push(child);
            }

            if transformed_any {
                Ok(Transformed::Yes(
                    plan.with_new_children(children_new)?
                ))
            } else {
                Ok(Transformed::No(plan))
            }
        })
    }

    fn name(&self) -> &str {
        "parquet_sortness"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

type ChildWithSorting<'a> = (Arc<dyn ExecutionPlan>, Vec<PhysicalSortExpr>);

fn detect_children_with_desired_ordering(
    plan: &dyn ExecutionPlan,
) -> Option<Vec<ChildWithSorting<'_>>> {
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        return Some(vec![(
            Arc::clone(sort_exec.input()),
            sort_exec.expr().to_vec(),
        )]);
    }

    let required_input_ordering = plan.required_input_ordering();
    if !required_input_ordering.iter().all(|expr| expr.is_some()) {
        // not all inputs require sorting, ignore it
        return None;
    }

    let children = plan.children();
    if children.len() != required_input_ordering.len() {
        // this should normally not happen, but we ignore it
        return None;
    }
    if children.is_empty() {
        // leaf node
        return None;
    }

    Some(
        children
            .into_iter()
            .zip(
                required_input_ordering
                    .into_iter()
                    .map(|requirement| requirement.expect("just checked"))
                    .map(PhysicalSortRequirement::to_sort_exprs),
            )
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
    use datafusion::{
        datasource::{listing::PartitionedFile, object_store::ObjectStoreUrl},
        physical_expr::PhysicalSortExpr,
        physical_plan::{empty::EmptyExec, expressions::Column, sorts::sort::SortExec, Statistics},
    };
    use object_store::{path::Path, ObjectMeta};

    use crate::{
        chunk_order_field,
        physical_optimizer::test_util::{assert_unknown_partitioning, OptimizationTest},
        provider::DeduplicateExec,
        CHUNK_ORDER_COLUMN_NAME,
    };

    use super::*;

    #[test]
    fn test_happy_path_sort() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::new(ordering(["col2", "col1"], &schema), Arc::new(inner))
                .with_fetch(Some(42)),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet], [2.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC], projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_happy_path_dedup() {
        let schema = schema_with_chunk_order();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1", CHUNK_ORDER_COLUMN_NAME], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(DeduplicateExec::new(
            Arc::new(inner),
            ordering(["col2", "col1"], &schema),
            true,
        ));
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3, __chunk_order]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet], [2.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC, __chunk_order@3 ASC], projection=[col1, col2, col3, __chunk_order]"
        "###
        );
    }

    #[test]
    fn test_sort_partitioning() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)], vec![file(3)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::new(ordering(["col2", "col1"], &schema), Arc::new(inner))
                .with_preserve_partitioning(true)
                .with_fetch(Some(42)),
        );

        assert_unknown_partitioning(plan.output_partitioning(), 2);

        let opt = ParquetSortness::default();
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet, 2.parquet], [3.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={3 groups: [[1.parquet], [2.parquet], [3.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC], projection=[col1, col2, col3]"
        "###
        );

        assert_unknown_partitioning(test.output_plan().unwrap().output_partitioning(), 3);
    }

    #[test]
    fn test_parquet_already_flat() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1)], vec![file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::new(ordering(["col2", "col1"], &schema), Arc::new(inner))
                .with_fetch(Some(42)),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet], [2.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC], projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet], [2.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC], projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_parquet_has_different_ordering() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col1", "col2"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::new(ordering(["col2", "col1"], &schema), Arc::new(inner))
                .with_fetch(Some(42)),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_parquet_has_no_ordering() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::new(ordering(["col2", "col1"], &schema), Arc::new(inner))
                .with_fetch(Some(42)),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_fanout_limit() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2), file(3)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::new(ordering(["col2", "col1"], &schema), Arc::new(inner))
                .with_fetch(Some(42)),
        );
        let opt = ParquetSortness::default();
        let mut config = ConfigOptions::default();
        config.extensions.insert(IoxConfigExt {
            max_parquet_fanout: 2,
            ..Default::default()
        });
        insta::assert_yaml_snapshot!(
            OptimizationTest::new_with_config(plan, opt, &config),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet, 3.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet, 3.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_other_node() {
        let schema = schema();
        let inner = EmptyExec::new(true, Arc::clone(&schema));
        let plan = Arc::new(
            SortExec::new(ordering(["col2", "col1"], &schema), Arc::new(inner))
                .with_fetch(Some(42)),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   EmptyExec: produce_one_row=true"
        "###
        );
    }

    #[test]
    fn test_does_not_touch_freestanding_parquet_exec() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let plan = Arc::new(ParquetExec::new(base_config, None, None));
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_ignore_outer_sort_if_inner_preform_resort() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col1", "col2"], &schema)),
            infinite_source: false,
        };
        let plan = Arc::new(ParquetExec::new(base_config, None, None));
        let plan =
            Arc::new(SortExec::new(ordering(["col2", "col1"], &schema), plan).with_fetch(Some(42)));
        let plan =
            Arc::new(SortExec::new(ordering(["col1", "col2"], &schema), plan).with_fetch(Some(42)));
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col1@0 ASC,col2@1 ASC]"
          - "   SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "     ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col1@0 ASC,col2@1 ASC]"
            - "   SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "     ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, true),
            Field::new("col2", DataType::Utf8, true),
            Field::new("col3", DataType::Utf8, true),
        ]))
    }

    fn schema_with_chunk_order() -> SchemaRef {
        Arc::new(Schema::new(
            schema()
                .fields()
                .iter()
                .cloned()
                .chain(std::iter::once(chunk_order_field()))
                .collect::<Fields>(),
        ))
    }

    fn file(n: u128) -> PartitionedFile {
        PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::parse(format!("{n}.parquet")).unwrap(),
                last_modified: Default::default(),
                size: 0,
            },
            partition_values: vec![],
            range: None,
            extensions: None,
        }
    }

    fn ordering<const N: usize>(cols: [&str; N], schema: &SchemaRef) -> Vec<PhysicalSortExpr> {
        cols.into_iter()
            .map(|col| PhysicalSortExpr {
                expr: Arc::new(Column::new_with_schema(col, schema.as_ref()).unwrap()),
                options: Default::default(),
            })
            .collect()
    }
}
