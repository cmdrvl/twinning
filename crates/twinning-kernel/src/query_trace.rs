use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ir::Operation,
    result::{KernelResult, ResultTag},
};

pub const QUERY_TRACE_VERSION: &str = "twinning.query-trace.v0";
pub const QUERY_TRACE_REFUSED_TEXT_VERSION: &str = "twinning.query-trace.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceArtifact {
    pub version: String,
    pub policy: QueryTracePolicy,
    pub events: Vec<QueryTraceEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTracePolicy {
    pub sql_text: String,
    pub bind_values: String,
}

impl Default for QueryTracePolicy {
    fn default() -> Self {
        Self {
            sql_text: String::from("redacted_hash_only"),
            bind_values: String::from("redacted_hash_or_null"),
        }
    }
}

impl QueryTracePolicy {
    pub fn refused_text() -> Self {
        Self {
            sql_text: String::from("refused_text"),
            bind_values: String::from("redacted_hash_or_null"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceEvent {
    pub event_id: u64,
    pub session_id: String,
    pub protocol: String,
    pub statement_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binds: Option<BindTrace>,
    pub transaction_before: String,
    pub transaction_after: String,
    pub result: QueryTraceResult,
}

#[derive(Debug, Clone)]
pub struct QueryTraceEventSeed {
    pub session_id: String,
    pub protocol: String,
    pub statement_kind: String,
    pub sql_text: Option<String>,
    pub sql_hash: Option<String>,
    pub operation_hash: Option<String>,
    pub binds: Option<BindTrace>,
    pub transaction_before: String,
    pub transaction_after: String,
    pub result: QueryTraceResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindTrace {
    pub count: usize,
    pub values: Vec<BindValueTrace>,
}

impl BindTrace {
    pub fn from_values(values: impl IntoIterator<Item = Option<String>>) -> Self {
        let values = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| BindValueTrace::new(index + 1, value.as_deref()))
            .collect::<Vec<_>>();
        Self {
            count: values.len(),
            values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindValueTrace {
    pub position: usize,
    pub policy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

impl BindValueTrace {
    fn new(position: usize, value: Option<&str>) -> Self {
        match value {
            Some(value) => Self {
                position,
                policy: String::from("sha256"),
                hash: Some(sha256_prefixed(value.as_bytes())),
            },
            None => Self {
                position,
                policy: String::from("null"),
                hash: None,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceResult {
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    pub returned_rows: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlstate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal_code: Option<String>,
}

impl QueryTraceResult {
    pub fn from_kernel_result(result: &KernelResult) -> Self {
        match result {
            KernelResult::Ack(ack) => Self::success(ack.tag, ack.rows_affected, 0),
            KernelResult::Mutation(mutation) => Self::success(
                mutation.tag,
                mutation.rows_affected,
                mutation.returning_rows.len() as u64,
            ),
            KernelResult::Read(read) => Self {
                outcome: String::from("success"),
                tag: Some(String::from("select")),
                rows_affected: None,
                returned_rows: read.rows.len() as u64,
                sqlstate: None,
                refusal_code: None,
            },
            KernelResult::Refusal(refusal) => Self {
                outcome: String::from("refusal"),
                tag: None,
                rows_affected: None,
                returned_rows: 0,
                sqlstate: Some(refusal.sqlstate.clone()),
                refusal_code: Some(refusal.code.clone()),
            },
        }
    }

    fn success(tag: ResultTag, rows_affected: u64, returned_rows: u64) -> Self {
        Self {
            outcome: String::from("success"),
            tag: Some(result_tag_token(tag)),
            rows_affected: Some(rows_affected),
            returned_rows,
            sqlstate: None,
            refusal_code: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SharedQueryTrace {
    inner: Arc<Mutex<QueryTraceRecorder>>,
    capture_refused_sql: bool,
}

impl SharedQueryTrace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_policy(policy: QueryTracePolicy) -> Self {
        let capture_refused_sql = policy.sql_text == "refused_text";
        Self {
            inner: Arc::new(Mutex::new(QueryTraceRecorder {
                policy,
                events: Vec::new(),
            })),
            capture_refused_sql,
        }
    }

    pub fn captures_refused_sql_text(&self) -> bool {
        self.capture_refused_sql
    }

    pub fn record(&self, seed: QueryTraceEventSeed) -> io::Result<()> {
        self.inner
            .lock()
            .map_err(|error| io::Error::other(format!("query trace lock poisoned: {error}")))?
            .record(seed);
        Ok(())
    }

    pub fn artifact(&self) -> io::Result<QueryTraceArtifact> {
        Ok(self
            .inner
            .lock()
            .map_err(|error| io::Error::other(format!("query trace lock poisoned: {error}")))?
            .artifact())
    }
}

#[derive(Debug, Clone, Default)]
struct QueryTraceRecorder {
    policy: QueryTracePolicy,
    events: Vec<QueryTraceEvent>,
}

impl QueryTraceRecorder {
    fn record(&mut self, seed: QueryTraceEventSeed) {
        let event_id = self.events.len() as u64 + 1;
        let sql_text = if self.policy.sql_text == "refused_text" && seed.result.outcome == "refusal"
        {
            seed.sql_text
        } else {
            None
        };
        self.events.push(QueryTraceEvent {
            event_id,
            session_id: seed.session_id,
            protocol: seed.protocol,
            statement_kind: seed.statement_kind,
            sql_text,
            sql_hash: seed.sql_hash,
            operation_hash: seed.operation_hash,
            binds: seed.binds,
            transaction_before: seed.transaction_before,
            transaction_after: seed.transaction_after,
            result: seed.result,
        });
    }

    fn artifact(&self) -> QueryTraceArtifact {
        QueryTraceArtifact {
            version: String::from(if self.policy.sql_text == "refused_text" {
                QUERY_TRACE_REFUSED_TEXT_VERSION
            } else {
                QUERY_TRACE_VERSION
            }),
            policy: self.policy.clone(),
            events: self.events.clone(),
        }
    }
}

pub fn write_query_trace(path: &Path, artifact: &QueryTraceArtifact) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(artifact).map_err(io::Error::other)?;
    std::fs::write(path, bytes)
}

pub fn sql_hash(sql: &str) -> String {
    sha256_prefixed(sql.as_bytes())
}

pub fn operation_hash(operation: &Operation) -> Option<String> {
    if matches!(operation, Operation::Refusal(_)) {
        return None;
    }

    serde_json::to_vec(operation)
        .ok()
        .map(|bytes| sha256_prefixed(&bytes))
}

pub fn statement_kind(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("unknown")
        .to_ascii_lowercase()
}

fn result_tag_token(tag: ResultTag) -> String {
    serde_json::to_value(tag)
        .expect("serialize result tag")
        .as_str()
        .expect("result tag token")
        .to_owned()
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::{
        BindTrace, QueryTraceEventSeed, QueryTracePolicy, QueryTraceResult, SharedQueryTrace,
    };

    fn seed(sql_text: Option<&str>, outcome: &str) -> QueryTraceEventSeed {
        QueryTraceEventSeed {
            session_id: "session-1".into(),
            protocol: "simple_query".into(),
            statement_kind: "select".into(),
            sql_text: sql_text.map(str::to_owned),
            sql_hash: Some("sha256:sql".into()),
            operation_hash: None,
            binds: Some(BindTrace::from_values([Some(
                "r3_bind_value_must_not_be_recorded_7f91".into(),
            )])),
            transaction_before: "idle".into(),
            transaction_after: "idle".into(),
            result: QueryTraceResult {
                outcome: outcome.into(),
                tag: None,
                rows_affected: None,
                returned_rows: 0,
                sqlstate: (outcome == "refusal").then(|| "0A000".into()),
                refusal_code: (outcome == "refusal").then(|| "unsupported_shape".into()),
            },
        }
    }

    #[test]
    fn default_trace_serialization_remains_byte_identical() {
        let trace = SharedQueryTrace::new();
        trace
            .record(seed(None, "success"))
            .expect("record default trace event");
        let bytes = serde_json::to_vec(&trace.artifact().expect("default trace artifact"))
            .expect("serialize default trace");
        let expected = r#"{"version":"twinning.query-trace.v0","policy":{"sql_text":"redacted_hash_only","bind_values":"redacted_hash_or_null"},"events":[{"event_id":1,"session_id":"session-1","protocol":"simple_query","statement_kind":"select","sql_hash":"sha256:sql","binds":{"count":1,"values":[{"position":1,"policy":"sha256","hash":"sha256:da99f17e7aaf6f711b85661bd693549b46d40069a3f27a1caf30ae25226d840d"}]},"transaction_before":"idle","transaction_after":"idle","result":{"outcome":"success","returned_rows":0}}]}"#;
        assert_eq!(expected.as_bytes(), bytes);
    }

    #[test]
    fn refused_text_policy_records_only_refused_sql_and_never_bind_values() {
        let trace = SharedQueryTrace::with_policy(QueryTracePolicy::refused_text());
        trace
            .record(seed(Some("SELECT accepted"), "success"))
            .expect("record accepted event");
        trace
            .record(seed(Some("SELECT refused"), "refusal"))
            .expect("record refused event");
        let artifact = trace.artifact().expect("refused text trace artifact");
        assert_eq!("twinning.query-trace.v1", artifact.version);
        assert_eq!("refused_text", artifact.policy.sql_text);
        assert!(artifact.events[0].sql_text.is_none());
        assert_eq!(
            Some("SELECT refused"),
            artifact.events[1].sql_text.as_deref()
        );
        assert_eq!("select", artifact.events[1].statement_kind);
        assert_eq!(
            "unsupported_shape",
            artifact.events[1].result.refusal_code.as_deref().unwrap()
        );
        assert!(
            !serde_json::to_string(&artifact)
                .unwrap()
                .contains("r3_bind_value_must_not_be_recorded_7f91")
        );
    }

    #[test]
    fn refused_text_trace_uses_the_v1_schema_contract() {
        let schema: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../schemas/twinning.query-trace.v1.schema.json"
        )))
        .expect("v1 query trace schema is valid JSON");
        assert_eq!(
            "twinning.query-trace.v1",
            schema["properties"]["version"]["const"]
        );
        assert_eq!(
            "string",
            schema["$defs"]["event"]["properties"]["sql_text"]["type"]
        );
        assert_eq!(
            "refused_text",
            schema["properties"]["policy"]["properties"]["sql_text"]["const"]
        );
    }
}
