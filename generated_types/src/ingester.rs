use crate::influxdata::iox::ingester::v1 as proto;
use data_types::delete_predicate::{DeleteExpr, DeletePredicate, Op, Scalar};

impl From<&DeletePredicate> for proto::DeletePredicate {
    fn from(delete_predicate: &DeletePredicate) -> Self {
        Self {
            range: Some(proto::TimestampRange {
                start: delete_predicate.range.start(),
                end: delete_predicate.range.end(),
            }),
            exprs: delete_predicate.exprs.iter().map(Into::into).collect(),
        }
    }
}

impl From<&DeleteExpr> for proto::DeleteExpr {
    fn from(delete_expr: &DeleteExpr) -> Self {
        Self {
            column: delete_expr.column.clone(),
            op: proto::DeleteOp::from(delete_expr.op).into(),
            scalar: Some((&delete_expr.scalar).into()),
        }
    }
}

impl From<Op> for proto::DeleteOp {
    fn from(value: Op) -> Self {
        match value {
            Op::Eq => Self::Eq,
            Op::Ne => Self::Ne,
        }
    }
}

impl From<&Scalar> for proto::DeleteScalar {
    fn from(value: &Scalar) -> Self {
        use crate::influxdata::iox::ingester::v1::delete_scalar::Value;

        let value = match value {
            Scalar::Bool(v) => Value::ValueBool(*v),
            Scalar::I64(v) => Value::ValueI64(*v),
            Scalar::F64(v) => Value::ValueF64(v.0),
            Scalar::String(v) => Value::ValueString(v.into()),
        };

        Self { value: Some(value) }
    }
}
