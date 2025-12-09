//! Conversion from delta-kernel predicates to DataFusion expressions for predicate pushdown.

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::schema::partitions::{PartitionFilter, PartitionValue};
use datafusion::common::Column;
use datafusion::logical_expr::{binary_expr, col, lit, Expr, Operator};
use datafusion::prelude::{array_element, get_field, in_list, map_extract};
use delta_kernel::expressions::{
    BinaryPredicateOp, Expression, JunctionPredicateOp, Predicate, Scalar, UnaryPredicateOp,
};
use itertools::Itertools;

/// Convert a delta-kernel Predicate to a DataFusion Expr for predicate pushdown.
///
/// This function supports common partition pruning predicates including:
/// - Binary comparisons: =, !=, <, >, <=, >=
/// - Logical junctions: AND, OR
/// - Unary predicates: IS NULL
/// - NOT predicates
///
/// Unsupported predicates will return an error.
pub fn predicate_to_datafusion_expr(predicate: &Predicate) -> DeltaResult<Expr> {
    match predicate {
        Predicate::BooleanExpression(expr) => expression_to_datafusion_expr(expr),
        Predicate::Not(pred) => {
            // Delta-kernel represents several operators using NOT:
            // - <= as NOT(a > b)
            // - >= as NOT(a < b)
            // - != as NOT(a = b)
            // - IS NOT NULL as NOT(IS NULL)

            // Handle NOT(IS NULL) => IS NOT NULL
            if let Predicate::Unary(unary) = pred.as_ref() {
                if matches!(unary.op, UnaryPredicateOp::IsNull) {
                    let expr = expression_to_datafusion_expr(&unary.expr)?;
                    return Ok(expr.is_not_null());
                }
            }

            // Handle NOT(binary) for <=, >=, !=
            if let Predicate::Binary(binary) = pred.as_ref() {
                let left = expression_to_datafusion_expr(&binary.left)?;
                let right = expression_to_datafusion_expr(&binary.right)?;

                match binary.op {
                    // NOT(a > b) => a <= b
                    BinaryPredicateOp::GreaterThan => {
                        return Ok(binary_expr(left, Operator::LtEq, right));
                    }
                    // NOT(a < b) => a >= b
                    BinaryPredicateOp::LessThan => {
                        return Ok(binary_expr(left, Operator::GtEq, right));
                    }
                    // NOT(a = b) => a != b
                    BinaryPredicateOp::Equal => {
                        return Ok(binary_expr(left, Operator::NotEq, right));
                    }
                    _ => {
                        // For other cases, fall through to general NOT handling
                    }
                }
            }

            // General NOT handling for other predicates
            let inner = predicate_to_datafusion_expr(pred)?;
            Ok(Expr::Not(Box::new(inner)))
        }
        Predicate::Unary(unary) => {
            let expr = expression_to_datafusion_expr(&unary.expr)?;
            match unary.op {
                UnaryPredicateOp::IsNull => Ok(expr.is_null()),
            }
        }
        Predicate::Binary(binary) => {
            let left = expression_to_datafusion_expr(&binary.left)?;
            let right = expression_to_datafusion_expr(&binary.right)?;

            let op = match binary.op {
                BinaryPredicateOp::Equal => Operator::Eq,
                BinaryPredicateOp::LessThan => Operator::Lt,
                BinaryPredicateOp::GreaterThan => Operator::Gt,
                BinaryPredicateOp::Distinct => Operator::NotEq,
                BinaryPredicateOp::In => {
                    // For IN operator, we need special handling
                    // Convert to OR of equality checks
                    return Err(DeltaTableError::Generic(
                        "IN operator requires special handling - not yet implemented".to_string(),
                    ));
                }
            };

            Ok(binary_expr(left, op, right))
        }
        Predicate::Junction(junction) => {
            if junction.preds.is_empty() {
                return Err(DeltaTableError::Generic(
                    "Empty junction predicate".to_string(),
                ));
            }

            let mut exprs = junction
                .preds
                .iter()
                .map(predicate_to_datafusion_expr)
                .collect::<DeltaResult<Vec<_>>>()?;

            let first = exprs.remove(0);
            let result = exprs
                .into_iter()
                .fold(first, |acc, expr| match junction.op {
                    JunctionPredicateOp::And => acc.and(expr),
                    JunctionPredicateOp::Or => acc.or(expr),
                });

            Ok(result)
        }
        Predicate::Opaque(_) => Err(DeltaTableError::Generic(
            "Opaque predicates are not supported for DataFusion conversion".to_string(),
        )),
        Predicate::Unknown(name) => Err(DeltaTableError::Generic(format!(
            "Unknown predicate '{}' cannot be converted to DataFusion",
            name
        ))),
    }
}

/// Convert a delta-kernel Expression to a DataFusion Expr.
fn expression_to_datafusion_expr(expr: &Expression) -> DeltaResult<Expr> {
    match expr {
        Expression::Literal(scalar) => scalar_to_datafusion_lit(scalar),
        Expression::Column(col_name) => {
            // Column names in delta-kernel are Vec<String> (e.g., ["struct", "field"])
            // For nested columns, we need to use get_field to properly handle dots in field names
            let path = col_name.path();
            if path.is_empty() {
                return Err(DeltaTableError::Generic(
                    "Column name cannot be empty".to_string(),
                ));
            }

            // Start with the first field
            let mut expr = col(Column::from_name(&path[0]));

            // Chain get_field calls for nested fields
            for field_name in &path[1..] {
                expr = get_field(expr, field_name);
            }

            Ok(expr)
        }
        Expression::Predicate(pred) => predicate_to_datafusion_expr(pred),
        _ => Err(DeltaTableError::Generic(format!(
            "Unsupported expression type for DataFusion conversion: {:?}",
            expr
        ))),
    }
}

/// Convert a delta-kernel Scalar to a DataFusion literal Expr.
fn scalar_to_datafusion_lit(scalar: &Scalar) -> DeltaResult<Expr> {
    match scalar {
        Scalar::String(s) => Ok(lit(s.clone())),
        Scalar::Integer(i) => Ok(lit(*i)),
        Scalar::Long(l) => Ok(lit(*l)),
        Scalar::Short(s) => Ok(lit(*s)),
        Scalar::Byte(b) => Ok(lit(*b)),
        Scalar::Float(f) => Ok(lit(*f)),
        Scalar::Double(d) => Ok(lit(*d)),
        Scalar::Boolean(b) => Ok(lit(*b)),
        Scalar::Null(_) => Ok(lit(datafusion::scalar::ScalarValue::Null)),
        _ => Err(DeltaTableError::Generic(format!(
            "Unsupported scalar type for DataFusion conversion: {:?}",
            scalar
        ))),
    }
}

/// Extract a partition value from the `add.partitionValues` map structure.
///
/// This creates an expression that accesses `add.partitionValues[partition_key]`.
///
/// # Arguments
/// * `partition_key` - The partition column name to extract from the map
///
/// # Returns
/// A DataFusion expression that extracts the value: `map_extract(add.partitionValues, 'partition_key')`
pub fn extract_partition_value(partition_key: &str) -> Expr {
    // Build the expression: add.partitionValues[partition_key]
    // This is equivalent to: map_extract(get_field(col("add"), "partitionValues"), partition_key)
    let add_col = col("add");
    let partition_values_map = get_field(add_col, "partitionValues");

    // map_extract returns a list, so we need to extract the first element
    array_element(
        map_extract(partition_values_map, lit(partition_key)),
        lit(1),
    )
}

/// Convert a PartitionFilter to a DataFusion Expr for predicate pushdown.
///
/// This function supports all PartitionFilter operations:
/// - Equal, NotEqual
/// - LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual
/// - In, NotIn
///
/// Note: PartitionFilter values are always strings, so they are converted to string literals.
pub fn partition_filter_to_datafusion_expr(filter: &PartitionFilter) -> DeltaResult<Expr> {
    let column = extract_partition_value(&filter.key);

    match &filter.value {
        PartitionValue::Equal(value) => {
            // Handle NULL_PARTITION_VALUE_DATA_PATH as IS NULL
            if value == crate::kernel::schema::partitions::NULL_PARTITION_VALUE_DATA_PATH {
                Ok(column.is_null())
            } else {
                Ok(binary_expr(column, Operator::Eq, lit(value.clone())))
            }
        }
        PartitionValue::NotEqual(value) => {
            // Handle NULL_PARTITION_VALUE_DATA_PATH as IS NOT NULL
            if value == crate::kernel::schema::partitions::NULL_PARTITION_VALUE_DATA_PATH {
                Ok(column.is_not_null())
            } else {
                Ok(binary_expr(column, Operator::NotEq, lit(value.clone())))
            }
        }
        PartitionValue::LessThan(value) => {
            Ok(binary_expr(column, Operator::Lt, lit(value.clone())))
        }
        PartitionValue::LessThanOrEqual(value) => {
            Ok(binary_expr(column, Operator::LtEq, lit(value.clone())))
        }
        PartitionValue::GreaterThan(value) => {
            Ok(binary_expr(column, Operator::Gt, lit(value.clone())))
        }
        PartitionValue::GreaterThanOrEqual(value) => {
            Ok(binary_expr(column, Operator::GtEq, lit(value.clone())))
        }
        PartitionValue::In(values) => {
            let lit_values: Vec<Expr> = values.iter().map(|v| lit(v.clone())).collect();
            Ok(in_list(column, lit_values, false))
        }
        PartitionValue::NotIn(values) => {
            let lit_values: Vec<Expr> = values.iter().map(|v| lit(v.clone())).collect();
            Ok(in_list(column, lit_values, true)) // true = negated
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delta_kernel::expressions::Expression as KernelExpr;

    #[test]
    fn test_extract_partition_value() {
        // Test extracting a partition value from add.partitionValues map
        let expr = extract_partition_value("date");

        // Expected: array_element(map_extract(get_field(col("add"), "partitionValues"), "date"), 1)
        let expected = array_element(
            map_extract(get_field(col("add"), "partitionValues"), lit("date")),
            lit(1),
        );

        assert_eq!(expr, expected);
    }

    #[test]
    fn test_simple_equality() {
        // name = 'Alice'
        let pred = KernelExpr::column(["name"]).eq(Scalar::String("Alice".into()));
        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = binary_expr(col("name"), Operator::Eq, lit("Alice"));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_not_equal() {
        // status != 'inactive'
        let pred = KernelExpr::column(["status"]).ne(Scalar::String("inactive".into()));
        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = binary_expr(col("status"), Operator::NotEq, lit("inactive"));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_less_than() {
        // score < 100
        let pred = KernelExpr::column(["score"]).lt(Scalar::Integer(100));
        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = binary_expr(col("score"), Operator::Lt, lit(100));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_greater_than() {
        // price > 50
        let pred = KernelExpr::column(["price"]).gt(Scalar::Long(50));
        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = binary_expr(col("price"), Operator::Gt, lit(50i64));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_less_than_or_equal() {
        // age <= 65
        // Delta-kernel represents this as NOT(age > 65)
        let pred = KernelExpr::column(["age"]).le(Scalar::Integer(65));
        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = binary_expr(col("age"), Operator::LtEq, lit(65));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_greater_than_or_equal() {
        // salary >= 50000
        // Delta-kernel represents this as NOT(salary < 50000)
        let pred = KernelExpr::column(["salary"]).ge(Scalar::Long(50000));
        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = binary_expr(col("salary"), Operator::GtEq, lit(50000i64));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_is_null() {
        // name IS NULL
        let pred = KernelExpr::column(["name"]).is_null();
        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = col("name").is_null();
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_is_not_null() {
        // name IS NOT NULL
        let pred = KernelExpr::column(["name"]).is_not_null();
        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = col("name").is_not_null();
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_and_junction() {
        // age > 18 AND status = 'active'
        let pred1 = KernelExpr::column(["age"]).gt(Scalar::Integer(18));
        let pred2 = KernelExpr::column(["status"]).eq(Scalar::String("active".into()));
        let pred = Predicate::junction(JunctionPredicateOp::And, vec![pred1, pred2]);

        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = binary_expr(col("age"), Operator::Gt, lit(18)).and(binary_expr(
            col("status"),
            Operator::Eq,
            lit("active"),
        ));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_or_junction() {
        // category = 'books' OR category = 'electronics'
        let pred1 = KernelExpr::column(["category"]).eq(Scalar::String("books".into()));
        let pred2 = KernelExpr::column(["category"]).eq(Scalar::String("electronics".into()));
        let pred = Predicate::junction(JunctionPredicateOp::Or, vec![pred1, pred2]);

        let df_expr = predicate_to_datafusion_expr(&pred).unwrap();

        let expected = binary_expr(col("category"), Operator::Eq, lit("books")).or(binary_expr(
            col("category"),
            Operator::Eq,
            lit("electronics"),
        ));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_complex_junction() {
        // (age > 18 AND status = 'active') OR (vip = true)
        let pred1 = KernelExpr::column(["age"]).gt(Scalar::Integer(18));
        let pred2 = KernelExpr::column(["status"]).eq(Scalar::String("active".into()));
        let and_pred = Predicate::junction(JunctionPredicateOp::And, vec![pred1, pred2]);

        let pred3 = KernelExpr::column(["vip"]).eq(Scalar::Boolean(true));
        let or_pred = Predicate::junction(JunctionPredicateOp::Or, vec![and_pred, pred3]);

        let df_expr = predicate_to_datafusion_expr(&or_pred).unwrap();

        let expected = binary_expr(col("age"), Operator::Gt, lit(18))
            .and(binary_expr(col("status"), Operator::Eq, lit("active")))
            .or(binary_expr(col("vip"), Operator::Eq, lit(true)));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_not_predicate() {
        // NOT (age < 18)
        let pred = KernelExpr::column(["age"]).lt(Scalar::Integer(18));
        let not_pred = Predicate::Not(Box::new(pred));

        let df_expr = predicate_to_datafusion_expr(&not_pred).unwrap();

        let expected = binary_expr(col("age"), Operator::GtEq, lit(18));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_different_scalar_types() {
        // Test various scalar types
        let test_cases = vec![
            (Scalar::String("test".into()), lit("test")),
            (Scalar::Integer(42), lit(42)),
            (Scalar::Long(1000i64), lit(1000i64)),
            (Scalar::Boolean(true), lit(true)),
            (Scalar::Float(3.14f32), lit(3.14f32)),
            (Scalar::Double(2.718f64), lit(2.718f64)),
        ];

        for (scalar, expected_lit) in test_cases {
            let pred = KernelExpr::column(["col"]).eq(scalar);
            let df_expr = predicate_to_datafusion_expr(&pred).unwrap();
            let expected = binary_expr(col("col"), Operator::Eq, expected_lit);
            assert_eq!(df_expr, expected);
        }
    }

    // PartitionFilter conversion tests
    #[test]
    fn test_partition_filter_equal() {
        let filter = PartitionFilter {
            key: "date".to_string(),
            value: PartitionValue::Equal("2023-01-01".to_string()),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = binary_expr(
            extract_partition_value("date"),
            Operator::Eq,
            lit("2023-01-01"),
        );
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_partition_filter_not_equal() {
        let filter = PartitionFilter {
            key: "region".to_string(),
            value: PartitionValue::NotEqual("us-west".to_string()),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = binary_expr(
            extract_partition_value("region"),
            Operator::NotEq,
            lit("us-west"),
        );
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_partition_filter_less_than() {
        let filter = PartitionFilter {
            key: "year".to_string(),
            value: PartitionValue::LessThan("2023".to_string()),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = binary_expr(extract_partition_value("year"), Operator::Lt, lit("2023"));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_partition_filter_less_than_or_equal() {
        let filter = PartitionFilter {
            key: "month".to_string(),
            value: PartitionValue::LessThanOrEqual("06".to_string()),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = binary_expr(extract_partition_value("month"), Operator::LtEq, lit("06"));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_partition_filter_greater_than() {
        let filter = PartitionFilter {
            key: "day".to_string(),
            value: PartitionValue::GreaterThan("15".to_string()),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = binary_expr(extract_partition_value("day"), Operator::Gt, lit("15"));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_partition_filter_greater_than_or_equal() {
        let filter = PartitionFilter {
            key: "hour".to_string(),
            value: PartitionValue::GreaterThanOrEqual("12".to_string()),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = binary_expr(extract_partition_value("hour"), Operator::GtEq, lit("12"));
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_partition_filter_in() {
        let filter = PartitionFilter {
            key: "status".to_string(),
            value: PartitionValue::In(vec![
                "active".to_string(),
                "pending".to_string(),
                "processing".to_string(),
            ]),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = in_list(
            extract_partition_value("status"),
            vec![lit("active"), lit("pending"), lit("processing")],
            false,
        );
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_partition_filter_not_in() {
        let filter = PartitionFilter {
            key: "country".to_string(),
            value: PartitionValue::NotIn(vec!["US".to_string(), "CA".to_string()]),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = in_list(
            extract_partition_value("country"),
            vec![lit("US"), lit("CA")],
            true,
        );
        assert_eq!(df_expr, expected);
    }

    #[test]
    fn test_partition_filter_null_value() {
        use crate::kernel::schema::partitions::NULL_PARTITION_VALUE_DATA_PATH;

        // Equal to NULL should become IS NULL
        let filter = PartitionFilter {
            key: "optional_field".to_string(),
            value: PartitionValue::Equal(NULL_PARTITION_VALUE_DATA_PATH.to_string()),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = extract_partition_value("optional_field").is_null();
        assert_eq!(df_expr, expected);

        // NotEqual to NULL should become IS NOT NULL
        let filter = PartitionFilter {
            key: "optional_field".to_string(),
            value: PartitionValue::NotEqual(NULL_PARTITION_VALUE_DATA_PATH.to_string()),
        };
        let df_expr = partition_filter_to_datafusion_expr(&filter).unwrap();
        let expected = extract_partition_value("optional_field").is_not_null();
        assert_eq!(df_expr, expected);
    }
}
