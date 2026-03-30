use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ir::{ColumnName, MutationKind, ScalarValue, SessionOpKind};

pub type SqlState = String;
pub type ResultRow = Vec<ScalarValue>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result_kind", rename_all = "snake_case")]
pub enum KernelResult {
    Ack(AckResult),
    Read(ReadResult),
    Mutation(MutationResult),
    Refusal(RefusalResult),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckResult {
    pub tag: ResultTag,
    pub rows_affected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadResult {
    pub columns: Vec<ColumnName>,
    pub rows: Vec<ResultRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationResult {
    pub tag: ResultTag,
    pub rows_affected: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning_rows: Vec<ResultRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefusalResult {
    pub code: String,
    pub message: String,
    pub sqlstate: SqlState,
    pub detail: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultTag {
    SetParam,
    Begin,
    Commit,
    Rollback,
    Sync,
    Insert,
    Upsert,
    Update,
    Delete,
}

impl From<SessionOpKind> for ResultTag {
    fn from(value: SessionOpKind) -> Self {
        match value {
            SessionOpKind::SetParam => Self::SetParam,
            SessionOpKind::Begin => Self::Begin,
            SessionOpKind::Commit => Self::Commit,
            SessionOpKind::Rollback => Self::Rollback,
            SessionOpKind::Sync => Self::Sync,
        }
    }
}

impl From<MutationKind> for ResultTag {
    fn from(value: MutationKind) -> Self {
        match value {
            MutationKind::Insert => Self::Insert,
            MutationKind::Upsert => Self::Upsert,
            MutationKind::Update => Self::Update,
            MutationKind::Delete => Self::Delete,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::{Value, json};

    use crate::ir::{MutationKind, ScalarValue, SessionOpKind};

    use super::{AckResult, KernelResult, MutationResult, ReadResult, RefusalResult, ResultTag};

    fn object_keys(value: &Value) -> BTreeSet<String> {
        value
            .as_object()
            .expect("json object")
            .keys()
            .cloned()
            .collect()
    }

    fn enum_token<T: serde::Serialize>(value: T) -> String {
        serde_json::to_value(value)
            .expect("serialize enum")
            .as_str()
            .expect("enum string")
            .to_owned()
    }

    #[test]
    fn session_and_mutation_kinds_map_into_normalized_result_tags() {
        assert_eq!(ResultTag::from(SessionOpKind::Begin), ResultTag::Begin);
        assert_eq!(ResultTag::from(SessionOpKind::Sync), ResultTag::Sync);
        assert_eq!(ResultTag::from(MutationKind::Insert), ResultTag::Insert);
        assert_eq!(ResultTag::from(MutationKind::Delete), ResultTag::Delete);
    }

    #[test]
    fn controlled_result_tag_vocab_matches_plan() {
        assert_eq!(
            vec![
                enum_token(ResultTag::SetParam),
                enum_token(ResultTag::Begin),
                enum_token(ResultTag::Commit),
                enum_token(ResultTag::Rollback),
                enum_token(ResultTag::Sync),
                enum_token(ResultTag::Insert),
                enum_token(ResultTag::Upsert),
                enum_token(ResultTag::Update),
                enum_token(ResultTag::Delete),
            ],
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
    }

    #[test]
    fn result_types_serialize_with_protocol_agnostic_shape() {
        let ack = AckResult {
            tag: ResultTag::Commit,
            rows_affected: 0,
        };
        let read = ReadResult {
            columns: vec![String::from("deal_id"), String::from("deal_name")],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-1")),
                ScalarValue::Text(String::from("Alpha")),
            ]],
        };
        let mutation = MutationResult {
            tag: ResultTag::Insert,
            rows_affected: 1,
            returning_rows: vec![vec![ScalarValue::Text(String::from("deal-1"))]],
        };
        let refusal = RefusalResult {
            code: String::from("unsupported_shape"),
            message: String::from("unsupported live query shape"),
            sqlstate: String::from("0A000"),
            detail: BTreeMap::from([(String::from("table"), String::from("public.deals"))]),
        };

        assert_eq!(
            serde_json::to_value(&ack).expect("serialize ack"),
            json!({
                "tag": "commit",
                "rows_affected": 0
            })
        );
        assert_eq!(
            serde_json::to_value(&read).expect("serialize read"),
            json!({
                "columns": ["deal_id", "deal_name"],
                "rows": [[
                    {"text": "deal-1"},
                    {"text": "Alpha"}
                ]]
            })
        );
        assert_eq!(
            serde_json::to_value(&mutation).expect("serialize mutation"),
            json!({
                "tag": "insert",
                "rows_affected": 1,
                "returning_rows": [[{"text": "deal-1"}]]
            })
        );
        assert_eq!(
            serde_json::to_value(&refusal).expect("serialize refusal"),
            json!({
                "code": "unsupported_shape",
                "message": "unsupported live query shape",
                "sqlstate": "0A000",
                "detail": {
                    "table": "public.deals"
                }
            })
        );
    }

    #[test]
    fn mutation_result_omits_empty_returning_rows() {
        let mutation = MutationResult {
            tag: ResultTag::Update,
            rows_affected: 2,
            returning_rows: Vec::new(),
        };

        assert_eq!(
            serde_json::to_value(&mutation).expect("serialize mutation"),
            json!({
                "tag": "update",
                "rows_affected": 2
            })
        );
    }

    #[test]
    fn normalized_results_keep_required_fields_and_omit_absent_optionals() {
        let ack = serde_json::to_value(AckResult {
            tag: ResultTag::Begin,
            rows_affected: 0,
        })
        .expect("serialize ack");
        assert_eq!(
            object_keys(&ack),
            BTreeSet::from([String::from("rows_affected"), String::from("tag")])
        );

        let read = serde_json::to_value(ReadResult {
            columns: vec![String::from("deal_id")],
            rows: vec![vec![ScalarValue::Text(String::from("deal-1"))]],
        })
        .expect("serialize read");
        assert_eq!(
            object_keys(&read),
            BTreeSet::from([String::from("columns"), String::from("rows")])
        );

        let mutation = serde_json::to_value(MutationResult {
            tag: ResultTag::Delete,
            rows_affected: 2,
            returning_rows: Vec::new(),
        })
        .expect("serialize mutation");
        assert_eq!(
            object_keys(&mutation),
            BTreeSet::from([String::from("rows_affected"), String::from("tag")])
        );

        let refusal = serde_json::to_value(RefusalResult {
            code: String::from("unsupported_shape"),
            message: String::from("unsupported live query shape"),
            sqlstate: String::from("0A000"),
            detail: BTreeMap::new(),
        })
        .expect("serialize refusal");
        assert_eq!(
            object_keys(&refusal),
            BTreeSet::from([
                String::from("code"),
                String::from("detail"),
                String::from("message"),
                String::from("sqlstate"),
            ])
        );
        assert_eq!(refusal["detail"], json!({}));
    }

    #[test]
    fn kernel_results_serialize_with_snake_case_vocab() {
        let mutation = KernelResult::Mutation(MutationResult {
            tag: ResultTag::Upsert,
            rows_affected: 1,
            returning_rows: vec![vec![ScalarValue::Text(String::from("deal-1"))]],
        });
        let refusal = KernelResult::Refusal(RefusalResult {
            code: String::from("unsupported_shape"),
            message: String::from("window functions are outside the declared subset"),
            sqlstate: String::from("0A000"),
            detail: BTreeMap::from([(String::from("shape"), String::from("window_function"))]),
        });

        let mutation_json = serde_json::to_value(mutation).expect("serialize mutation result");
        assert_eq!(mutation_json["result_kind"], "mutation");
        assert_eq!(mutation_json["tag"], "upsert");
        assert_eq!(mutation_json["rows_affected"], 1);

        let refusal_json = serde_json::to_value(refusal).expect("serialize refusal result");
        assert_eq!(refusal_json["result_kind"], "refusal");
        assert_eq!(refusal_json["sqlstate"], "0A000");
        assert_eq!(
            refusal_json["detail"],
            json!({
                "shape": "window_function"
            })
        );
    }
}
