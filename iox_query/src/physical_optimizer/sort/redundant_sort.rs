use std::{collections::HashSet, sync::Arc};

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_expr::PhysicalSortExpr,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{expressions::Column, sorts::sort::SortExec, ExecutionPlan},
};

/// Removes [`SortExec`] if it is no longer needed.
#[derive(Debug, Default)]
pub struct RedundantSort;

impl PhysicalOptimizerRule for RedundantSort {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let plan_any = plan.as_any();

            if let Some(sort_exec) = plan_any.downcast_ref::<SortExec>() {
                let child = sort_exec.input();

                let schema = child.schema();
                let stats = child.statistics();
                let sorted_cols = stats
                    .column_statistics
                    .unwrap_or_default()
                    .into_iter()
                    .zip(schema.fields())
                    .filter(|(col_stats, _field)| {
                        (stats.is_exact)
                            && (col_stats.null_count == Some(0))
                            && (col_stats
                                .distinct_count
                                .map(|count| count < 2)
                                .unwrap_or_default()
                                || (col_stats.min_value.is_some()
                                    && (col_stats.min_value == col_stats.max_value)))
                    })
                    .map(|(_stats, field)| field.name().as_str())
                    .collect::<HashSet<_>>();

                let expr = remove_sort_cols(sort_exec.expr(), &sorted_cols);
                let child_expr = child
                    .output_ordering()
                    .map(|expr| remove_sort_cols(expr, &sorted_cols));

                if child_expr.as_ref() == Some(&expr) {
                    return Ok(Transformed::Yes(Arc::clone(child)));
                }

                if expr.len() != sort_exec.expr().len() {
                    return Ok(Transformed::Yes(Arc::new(SortExec::try_new(
                        expr,
                        Arc::clone(child),
                        sort_exec.fetch(),
                    )?)));
                }
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "redundant_sort"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn remove_sort_cols(expr: &[PhysicalSortExpr], cols: &HashSet<&str>) -> Vec<PhysicalSortExpr> {
    expr.iter()
        .filter(|expr| {
            if let Some(col) = expr.expr.as_any().downcast_ref::<Column>() {
                !cols.contains(col.name())
            } else {
                true
            }
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::{
        datasource::object_store::ObjectStoreUrl,
        physical_expr::PhysicalSortExpr,
        physical_plan::{
            expressions::Column,
            file_format::{FileScanConfig, ParquetExec},
            ColumnStatistics, Statistics,
        },
        scalar::ScalarValue,
    };

    use crate::physical_optimizer::test_util::OptimizationTest;

    use super::*;

    #[test]
    fn test_not_redundant() {
        let schema = schema();
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan =
            Arc::new(SortExec::try_new(full_sort_expr(schema.as_ref()), input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_redundant_exactly_sorted() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: Some(sort_expr.clone()),
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, output_ordering=[col1@0 ASC, col2@1 ASC], projection=[col1, col2]"
        output:
          Ok:
            - " ParquetExec: limit=None, partitions={0 groups: []}, output_ordering=[col1@0 ASC, col2@1 ASC], projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_stats_not_exact() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            min_value: None,
                            max_value: None,
                            distinct_count: Some(0),
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: false,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_stats_null_none() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: None,
                            min_value: None,
                            max_value: None,
                            distinct_count: Some(0),
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_stats_null_greater_zero() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(1),
                            min_value: None,
                            max_value: None,
                            distinct_count: Some(0),
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_stats_no_col_boundaries_or_minmax() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            min_value: None,
                            max_value: None,
                            distinct_count: None,
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_redundant_one_col_zero_distinct() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            min_value: None,
                            max_value: None,
                            distinct_count: Some(0),
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_redundant_one_col_one_distinct() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            min_value: None,
                            max_value: None,
                            distinct_count: Some(1),
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_redundant_one_col_min_max_same() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            min_value: Some(ScalarValue::from(42i64)),
                            max_value: Some(ScalarValue::from(42i64)),
                            distinct_count: None,
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_min_max_different() {
        let schema = schema();
        let sort_expr = full_sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            min_value: Some(ScalarValue::from(42i64)),
                            max_value: Some(ScalarValue::from(43i64)),
                            distinct_count: None,
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_redundant_sorted_by_multivalue_cols() {
        let schema = schema();
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            min_value: None,
                            max_value: None,
                            distinct_count: Some(1),
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: Some(vec![col_sort_expr("col2", &schema)]),
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan =
            Arc::new(SortExec::try_new(full_sort_expr(&input.schema()), input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, output_ordering=[col2@1 ASC], projection=[col1, col2]"
        output:
          Ok:
            - " ParquetExec: limit=None, partitions={0 groups: []}, output_ordering=[col2@1 ASC], projection=[col1, col2]"
        "###
        );
    }

    #[test]
    fn test_redundant_child_is_stronger_sorted() {
        let schema = schema();
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics {
                    num_rows: None,
                    total_byte_size: None,
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            min_value: None,
                            max_value: None,
                            distinct_count: Some(1),
                        },
                        ColumnStatistics::default(),
                    ]),
                    is_exact: true,
                },
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: Some(full_sort_expr(&schema)),
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan =
            Arc::new(SortExec::try_new(full_sort_expr(&input.schema()), input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col1@0 ASC,col2@1 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, output_ordering=[col1@0 ASC, col2@1 ASC], projection=[col1, col2]"
        output:
          Ok:
            - " ParquetExec: limit=None, partitions={0 groups: []}, output_ordering=[col1@0 ASC, col2@1 ASC], projection=[col1, col2]"
        "###
        );
    }

    fn full_sort_expr(schema: &Schema) -> Vec<PhysicalSortExpr> {
        vec![col_sort_expr("col1", schema), col_sort_expr("col2", schema)]
    }

    fn col_sort_expr(col: &str, schema: &Schema) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: Arc::new(Column::new_with_schema(col, schema).unwrap()),
            options: Default::default(),
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Int64, false),
        ]))
    }
}
