//! REST canary manifest evaluation over recorded session logs.

use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::session_log::RestSessionLog;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RestCanaryManifest {
    #[serde(default)]
    pub assertions: Vec<RestCanaryAssertion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RestCanaryAssertion {
    EndpointCalled {
        method: String,
        path: String,
    },
    StatusReturned {
        method: String,
        path: String,
        status: u16,
    },
    ConstraintTriggered {
        constraint: String,
    },
    RefusalIssued {
        code: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestCanaryReport {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<RestCanaryFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestCanaryFailure {
    pub assertion: String,
    pub expected: String,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum RestCanaryError {
    #[error("failed to read REST canary manifest `{path}`: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse REST canary manifest `{path}`: {source}")]
    Parse {
        path: String,
        source: serde_json::Error,
    },
}

impl RestCanaryManifest {
    pub fn load(path: &Path) -> Result<Self, RestCanaryError> {
        let bytes = fs::read(path).map_err(|source| RestCanaryError::Read {
            path: path.display().to_string(),
            source,
        })?;
        serde_json::from_slice(&bytes).map_err(|source| RestCanaryError::Parse {
            path: path.display().to_string(),
            source,
        })
    }

    pub fn evaluate(&self, log: &RestSessionLog) -> RestCanaryReport {
        let mut failures = Vec::new();
        for assertion in &self.assertions {
            if let Some(failure) = evaluate_assertion(assertion, log) {
                failures.push(failure);
            }
        }

        RestCanaryReport {
            total: self.assertions.len(),
            passed: self.assertions.len() - failures.len(),
            failed: failures.len(),
            failures,
        }
    }
}

fn evaluate_assertion(
    assertion: &RestCanaryAssertion,
    log: &RestSessionLog,
) -> Option<RestCanaryFailure> {
    match assertion {
        RestCanaryAssertion::EndpointCalled { method, path } => log
            .requests
            .iter()
            .any(|request| request.method == *method && request.route == *path)
            .then_some(())
            .is_none()
            .then(|| RestCanaryFailure {
                assertion: String::from("endpoint_called"),
                expected: format!("{} {}", method, path),
                detail: String::from("endpoint was never called"),
            }),
        RestCanaryAssertion::StatusReturned {
            method,
            path,
            status,
        } => log
            .requests
            .iter()
            .any(|request| {
                request.method == *method && request.route == *path && request.status == *status
            })
            .then_some(())
            .is_none()
            .then(|| RestCanaryFailure {
                assertion: String::from("status_returned"),
                expected: format!("{} {} -> {}", method, path, status),
                detail: String::from("status was never observed for route"),
            }),
        RestCanaryAssertion::ConstraintTriggered { constraint } => log
            .requests
            .iter()
            .any(|request| request.constraint_violation.as_ref() == Some(constraint))
            .then_some(())
            .is_none()
            .then(|| RestCanaryFailure {
                assertion: String::from("constraint_triggered"),
                expected: constraint.clone(),
                detail: String::from("constraint violation was never observed"),
            }),
        RestCanaryAssertion::RefusalIssued { code } => log
            .requests
            .iter()
            .any(|request| request.refusal.as_ref() == Some(code))
            .then_some(())
            .is_none()
            .then(|| RestCanaryFailure {
                assertion: String::from("refusal_issued"),
                expected: code.clone(),
                detail: String::from("refusal was never observed"),
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::{RestCanaryAssertion, RestCanaryManifest};
    use crate::protocol::rest::session_log::{RestRequestLog, RestSessionLog};

    #[test]
    fn canary_report_records_passing_and_failing_assertions() {
        let manifest = RestCanaryManifest {
            assertions: vec![
                RestCanaryAssertion::EndpointCalled {
                    method: String::from("POST"),
                    path: String::from("/files"),
                },
                RestCanaryAssertion::RefusalIssued {
                    code: String::from("not_found"),
                },
                RestCanaryAssertion::StatusReturned {
                    method: String::from("DELETE"),
                    path: String::from("/files/{id}"),
                    status: 204,
                },
            ],
        };
        let mut log = RestSessionLog::default();
        log.record(RestRequestLog {
            method: String::from("POST"),
            path: String::from("/files"),
            route: String::from("/files"),
            status: 201,
            duration_ms: 1,
            constraint_violation: None,
            refusal: None,
        });
        log.record(RestRequestLog {
            method: String::from("GET"),
            path: String::from("/files/1"),
            route: String::from("/files/{id}"),
            status: 404,
            duration_ms: 1,
            constraint_violation: None,
            refusal: Some(String::from("not_found")),
        });

        let report = manifest.evaluate(&log);

        assert_eq!(report.total, 3);
        assert_eq!(report.passed, 2);
        assert_eq!(report.failed, 1);
        assert_eq!(report.failures[0].assertion, "status_returned");
    }
}
