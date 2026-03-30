#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::{Value, json};
use twinning::{
    ir::{
        AggregateKind, ConflictTarget, MutationKind, MutationOp, Operation, PredicateOperator,
        ReadShape, RefusalOp, RefusalScope, ScalarValue, SessionOpKind,
    },
    result::{KernelResult, MutationResult, ReadResult, RefusalResult, ResultTag},
};

#[test]
fn controlled_vocab_values_stay_exact_for_public_core_types() {
    assert_eq!(
        serialize_vocab([
            SessionOpKind::SetParam,
            SessionOpKind::Begin,
            SessionOpKind::Commit,
            SessionOpKind::Rollback,
            SessionOpKind::Sync,
        ]),
        vec!["set_param", "begin", "commit", "rollback", "sync"]
    );
    assert_eq!(
        serialize_vocab([
            MutationKind::Insert,
            MutationKind::Upsert,
            MutationKind::Update,
            MutationKind::Delete,
        ]),
        vec!["insert", "upsert", "update", "delete"]
    );
    assert_eq!(
        serialize_vocab([
            ReadShape::PointLookup,
            ReadShape::FilteredScan,
            ReadShape::AggregateScan
        ]),
        vec!["point_lookup", "filtered_scan", "aggregate_scan"]
    );
    assert_eq!(
        serialize_vocab([
            AggregateKind::None,
            AggregateKind::Count,
            AggregateKind::Sum,
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
        ]),
        vec!["none", "count", "sum", "avg", "min", "max"]
    );
    assert_eq!(
        serialize_vocab([
            PredicateOperator::Eq,
            PredicateOperator::Neq,
            PredicateOperator::Lt,
            PredicateOperator::Lte,
            PredicateOperator::Gt,
            PredicateOperator::Gte,
            PredicateOperator::IsNull,
            PredicateOperator::InList,
            PredicateOperator::Between,
        ]),
        vec![
            "eq", "neq", "lt", "lte", "gt", "gte", "is_null", "in_list", "between",
        ]
    );
    assert_eq!(
        serialize_vocab([
            RefusalScope::Session,
            RefusalScope::Prepare,
            RefusalScope::Mutation,
            RefusalScope::Read,
        ]),
        vec!["session", "prepare", "mutation", "read"]
    );
    assert_eq!(
        serialize_vocab([
            ResultTag::SetParam,
            ResultTag::Begin,
            ResultTag::Commit,
            ResultTag::Rollback,
            ResultTag::Sync,
            ResultTag::Insert,
            ResultTag::Upsert,
            ResultTag::Update,
            ResultTag::Delete,
        ]),
        vec![
            "set_param",
            "begin",
            "commit",
            "rollback",
            "sync",
            "insert",
            "upsert",
            "update",
            "delete",
        ]
    );

    assert_eq!(
        serde_json::to_value(ConflictTarget::PrimaryKey).expect("serialize pk"),
        json!("primary_key")
    );
    assert_eq!(
        serde_json::to_value(ConflictTarget::Columns(vec![
            String::from("tenant_id"),
            String::from("deal_id"),
        ]))
        .expect("serialize columns target"),
        json!({ "columns": ["tenant_id", "deal_id"] })
    );
    assert_eq!(
        serde_json::to_value(ConflictTarget::NamedConstraint(String::from(
            "deals_name_idx"
        )))
        .expect("serialize named target"),
        json!({ "named_constraint": "deals_name_idx" })
    );
}

#[test]
fn unknown_controlled_vocab_values_are_rejected() {
    assert_unknown_variant::<SessionOpKind>(json!("savepoint"), "unknown variant");
    assert_unknown_variant::<MutationKind>(json!("merge"), "unknown variant");
    assert_unknown_variant::<ReadShape>(json!("join_scan"), "unknown variant");
    assert_unknown_variant::<PredicateOperator>(json!("like"), "unknown variant");
    assert_unknown_variant::<ResultTag>(json!("merge"), "unknown variant");
    assert_unknown_variant::<RefusalScope>(json!("protocol"), "unknown variant");
}

#[test]
fn required_fields_stay_required_for_core_contract_types() {
    assert_missing_field::<MutationOp>(
        json!({
            "session_id": "session-1",
            "table": "public.deals",
            "kind": "insert",
            "rows": []
        }),
        "columns",
    );
    assert_missing_field::<Operation>(
        json!({
            "op_type": "read",
            "session_id": "session-1",
            "table": "public.deals",
            "shape": "point_lookup",
            "projection": []
        }),
        "aggregate",
    );
    assert_missing_field::<ReadResult>(
        json!({
            "columns": ["deal_id"]
        }),
        "rows",
    );
    assert_missing_field::<RefusalResult>(
        json!({
            "code": "unsupported_shape",
            "message": "unsupported live query shape",
            "detail": { "shape": "window_function" }
        }),
        "sqlstate",
    );
}

#[test]
fn refusal_contract_keeps_operation_and_result_surfaces_distinct() {
    let refusal_op = RefusalOp {
        scope: RefusalScope::Read,
        code: String::from("unsupported_shape"),
        detail: BTreeMap::from([(String::from("shape"), String::from("window_function"))]),
    };
    let refusal_result = RefusalResult {
        code: String::from("unsupported_shape"),
        message: String::from("window functions are outside the declared subset"),
        sqlstate: String::from("0A000"),
        detail: BTreeMap::from([(String::from("shape"), String::from("window_function"))]),
    };

    let op_json = serde_json::to_value(refusal_op).expect("serialize refusal op");
    let result_json =
        serde_json::to_value(refusal_result.clone()).expect("serialize refusal result");
    let kernel_json = serde_json::to_value(KernelResult::Refusal(refusal_result))
        .expect("serialize kernel refusal");

    assert_eq!(
        op_json,
        json!({
            "scope": "read",
            "code": "unsupported_shape",
            "detail": { "shape": "window_function" }
        })
    );
    assert!(op_json.get("message").is_none());
    assert!(op_json.get("sqlstate").is_none());

    assert_eq!(
        result_json,
        json!({
            "code": "unsupported_shape",
            "message": "window functions are outside the declared subset",
            "sqlstate": "0A000",
            "detail": { "shape": "window_function" }
        })
    );
    assert!(result_json.get("scope").is_none());

    assert_eq!(kernel_json["result_kind"], "refusal");
    assert_eq!(kernel_json["sqlstate"], "0A000");
    assert!(kernel_json.get("scope").is_none());
}

#[test]
fn result_and_scalar_shapes_remain_protocol_agnostic() {
    let read = ReadResult {
        columns: vec![String::from("deal_id"), String::from("is_active")],
        rows: vec![vec![
            ScalarValue::Text(String::from("deal-1")),
            ScalarValue::Boolean(true),
        ]],
    };
    let mutation = MutationResult {
        tag: ResultTag::Upsert,
        rows_affected: 1,
        returning_rows: vec![vec![ScalarValue::Integer(7), ScalarValue::Null]],
    };

    let read_json = serde_json::to_value(read).expect("serialize read");
    let mutation_json = serde_json::to_value(mutation).expect("serialize mutation");

    assert_eq!(
        read_json,
        json!({
            "columns": ["deal_id", "is_active"],
            "rows": [[
                { "text": "deal-1" },
                { "boolean": true }
            ]]
        })
    );
    assert_eq!(
        mutation_json,
        json!({
            "tag": "upsert",
            "rows_affected": 1,
            "returning_rows": [[
                { "integer": 7 },
                "null"
            ]]
        })
    );
}

fn serialize_vocab<T, const N: usize>(values: [T; N]) -> Vec<String>
where
    T: Serialize,
{
    values
        .into_iter()
        .map(|value| serde_json::to_value(value).expect("serialize vocab value"))
        .map(json_string)
        .collect()
}

fn json_string(value: Value) -> String {
    value
        .as_str()
        .expect("controlled vocab should serialize as a string")
        .to_owned()
}

fn assert_unknown_variant<T>(value: Value, expected_fragment: &str)
where
    T: serde::de::DeserializeOwned,
{
    let error = match serde_json::from_value::<T>(value) {
        Ok(_) => panic!("deserialization should fail"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains(expected_fragment),
        "expected error to contain `{expected_fragment}`, got `{error}`"
    );
}

fn assert_missing_field<T>(value: Value, field_name: &str)
where
    T: serde::de::DeserializeOwned,
{
    let error = match serde_json::from_value::<T>(value) {
        Ok(_) => panic!("deserialization should fail"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains(field_name),
        "expected error to mention missing field `{field_name}`, got `{error}`"
    );
}
