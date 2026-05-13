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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceEvent {
    pub event_id: u64,
    pub session_id: String,
    pub protocol: String,
    pub statement_kind: String,
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
}

impl SharedQueryTrace {
    pub fn new() -> Self {
        Self::default()
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
    events: Vec<QueryTraceEvent>,
}

impl QueryTraceRecorder {
    fn record(&mut self, seed: QueryTraceEventSeed) {
        let event_id = self.events.len() as u64 + 1;
        self.events.push(QueryTraceEvent {
            event_id,
            session_id: seed.session_id,
            protocol: seed.protocol,
            statement_kind: seed.statement_kind,
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
            version: String::from(QUERY_TRACE_VERSION),
            policy: QueryTracePolicy::default(),
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
