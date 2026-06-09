//! Request/session recording for REST twins.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RestSessionLog {
    #[serde(default)]
    pub requests: Vec<RestRequestLog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestRequestLog {
    pub method: String,
    pub path: String,
    pub route: String,
    pub status: u16,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_stub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint_violation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RestSessionSummary {
    pub request_count: usize,
    pub endpoints_exercised: Vec<String>,
    pub endpoints_not_exercised: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub response_stubs: BTreeMap<String, u64>,
    pub constraint_violations: BTreeMap<String, u64>,
    pub refusals: BTreeMap<String, u64>,
}

impl RestSessionLog {
    pub fn record(&mut self, request: RestRequestLog) {
        self.requests.push(request);
    }

    pub fn summary(&self, declared_endpoints: &[String]) -> RestSessionSummary {
        let exercised = self
            .requests
            .iter()
            .filter(|request| request.route != "unmatched")
            .map(|request| format!("{} {}", request.method, request.route))
            .collect::<BTreeSet<_>>();
        let declared = declared_endpoints.iter().cloned().collect::<BTreeSet<_>>();

        RestSessionSummary {
            request_count: self.requests.len(),
            endpoints_exercised: exercised.iter().cloned().collect(),
            endpoints_not_exercised: declared.difference(&exercised).cloned().collect(),
            response_stubs: count_by(
                self.requests
                    .iter()
                    .filter_map(|request| request.response_stub.as_deref()),
            ),
            constraint_violations: count_by(
                self.requests
                    .iter()
                    .filter_map(|request| request.constraint_violation.as_deref()),
            ),
            refusals: count_by(
                self.requests
                    .iter()
                    .filter_map(|request| request.refusal.as_deref()),
            ),
        }
    }
}

pub fn constraint_violation_kind(code: &str) -> Option<&'static str> {
    match code {
        "unique_violation" => Some("unique"),
        "not_null_violation" => Some("not_null"),
        "foreign_key_violation" => Some("foreign_key"),
        "check_violation" => Some("check"),
        "invalid_text_representation" | "type_mismatch" => Some("type_coercion"),
        _ => None,
    }
}

fn count_by<'a>(values: impl Iterator<Item = &'a str>) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value.to_owned()).or_insert(0) += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::{RestRequestLog, RestSessionLog};

    #[test]
    fn summary_tracks_exercised_missing_refusals_and_constraints() {
        let mut log = RestSessionLog::default();
        log.record(RestRequestLog {
            method: String::from("POST"),
            path: String::from("/files"),
            route: String::from("/files"),
            status: 201,
            duration_ms: 1,
            response_stub: Some(String::from("create_file")),
            constraint_violation: None,
            refusal: None,
        });
        log.record(RestRequestLog {
            method: String::from("GET"),
            path: String::from("/files/1"),
            route: String::from("/files/{id}"),
            status: 404,
            duration_ms: 1,
            response_stub: None,
            constraint_violation: Some(String::from("type_coercion")),
            refusal: Some(String::from("not_found")),
        });

        let summary = log.summary(&[
            String::from("POST /files"),
            String::from("GET /files/{id}"),
            String::from("DELETE /files/{id}"),
        ]);

        assert_eq!(summary.request_count, 2);
        assert_eq!(
            summary.endpoints_exercised,
            vec!["GET /files/{id}", "POST /files"]
        );
        assert_eq!(summary.endpoints_not_exercised, vec!["DELETE /files/{id}"]);
        assert_eq!(summary.response_stubs["create_file"], 1);
        assert_eq!(summary.refusals["not_found"], 1);
        assert_eq!(summary.constraint_violations["type_coercion"], 1);
    }
}
