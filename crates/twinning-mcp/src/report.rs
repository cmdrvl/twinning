//! Validates committed twin reports and renders bounded MCP response bodies.

use std::fs;
use std::path::Path;

use serde_json::{Value, json};

use crate::{
    errors::ToolError,
    registry::{TwinKind, TwinStatus},
    supervisor::StopResult,
};

const MAX_REPORT_RESPONSE_BYTES: usize = 256 * 1024;

pub fn validate_report(
    report: &Value,
    kind: TwinKind,
    path: &Path,
    stderr_path: &Path,
) -> Result<(), ToolError> {
    let expected_version = match kind {
        TwinKind::Postgres => "twinning.v0",
        TwinKind::Rest => "twinning.rest-report.v0",
    };
    if report["version"] != expected_version {
        return Err(unreadable(
            format!("expected a {expected_version} report at {}", path.display()),
            stderr_path,
        ));
    }
    let valid_outcome = match kind {
        TwinKind::Postgres => matches!(
            report["outcome"].as_str(),
            Some("READY" | "VIOLATIONS" | "FAIL")
        ),
        TwinKind::Rest => matches!(
            report["outcome"].as_str(),
            Some("PASS" | "FAIL" | "VIOLATIONS" | "REFUSAL")
        ),
    };
    if !valid_outcome {
        return Err(unreadable(
            format!("report outcome is not readable at {}", path.display()),
            stderr_path,
        ));
    }
    if kind == TwinKind::Rest && !valid_rest_session(&report["session"]) {
        return Err(unreadable(
            format!(
                "REST report session has an unsupported shape at {}",
                path.display()
            ),
            stderr_path,
        ));
    }
    Ok(())
}

pub fn read_report(path: &Path, stderr_path: &Path, kind: TwinKind) -> Result<Value, ToolError> {
    let bytes = fs::read(path).map_err(|error| {
        unreadable(
            format!("report unreadable at {}: {error}", path.display()),
            stderr_path,
        )
    })?;
    let report: Value = serde_json::from_slice(&bytes).map_err(|error| {
        unreadable(
            format!("report JSON invalid at {}: {error}", path.display()),
            stderr_path,
        )
    })?;
    validate_report(&report, kind, path, stderr_path)?;
    Ok(report)
}

pub fn read_trace(path: &Path, stderr_path: &Path) -> Result<Value, ToolError> {
    let bytes = fs::read(path).map_err(|error| {
        unreadable(
            format!("query trace unreadable at {}: {error}", path.display()),
            stderr_path,
        )
    })?;
    let trace: Value = serde_json::from_slice(&bytes).map_err(|error| {
        unreadable(
            format!("query trace JSON invalid at {}: {error}", path.display()),
            stderr_path,
        )
    })?;
    if trace["version"] != "twinning.query-trace.v0" || !trace["events"].is_array() {
        return Err(unreadable(
            format!("query trace has an unsupported shape at {}", path.display()),
            stderr_path,
        ));
    }
    Ok(trace)
}

pub fn stop_summary(result: &StopResult) -> Result<Value, ToolError> {
    if result.kind == TwinKind::Rest {
        return Ok(rest_summary(
            &result.report,
            &result.report_path,
            Some(result),
        ));
    }
    let trace = read_trace(
        &result.trace_path,
        &result.report_path.with_file_name("stderr.log"),
    )?;
    let report = &result.report;
    let constraints = report["constraints"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    let tables = report["tables"].as_object().cloned().unwrap_or_default();
    Ok(json!({
        "twin_id": result.twin_id,
        "status": status_name(result.status),
        "outcome": report["outcome"],
        "exit_code": result.exit_code,
        "constraints": constraints,
        "tables": tables,
        "request_count": trace["events"].as_array().map_or(0, Vec::len),
        "report_path": result.report_path,
        "trace_path": result.trace_path
    }))
}

pub fn full_report(report: Value, trace: &Value, report_path: &Path) -> Value {
    let mut response = report.clone();
    let summary = trace_summary(trace);
    response["trace_summary"] = summary.clone();
    let Ok(rendered) = serde_json::to_vec(&response) else {
        return response;
    };
    if rendered.len() <= MAX_REPORT_RESPONSE_BYTES {
        return response;
    }

    json!({
        "version": report["version"],
        "outcome": report["outcome"],
        "trace_summary": summary,
        "truncated": true,
        "report_path": report_path
    })
}

pub fn full_rest_report(report: &Value, report_path: &Path) -> Value {
    rest_summary(report, report_path, None)
}

fn rest_summary(report: &Value, report_path: &Path, stopped: Option<&StopResult>) -> Value {
    let session = &report["session"];
    let (endpoints_exercised, exercised_truncated) = capped_array(&session["endpoints_exercised"]);
    let (endpoints_not_exercised, not_exercised_truncated) =
        capped_array(&session["endpoints_not_exercised"]);
    let (outcomes, outcomes_truncated) = capped_object(&session["outcomes"]);
    let (refusals, refusals_truncated) = capped_object(&session["refusals"]);
    let mut truncated_fields = Vec::new();
    for (field, truncated) in [
        ("endpoints_exercised", exercised_truncated),
        ("endpoints_not_exercised", not_exercised_truncated),
        ("outcomes", outcomes_truncated),
        ("refusals", refusals_truncated),
    ] {
        if truncated {
            truncated_fields.push(field);
        }
    }
    let mut response = json!({
        "kind": "rest",
        "version": report["version"],
        "outcome": report["outcome"],
        "request_count": session["request_count"],
        "outcomes": outcomes,
        "refusals": refusals,
        "endpoints_exercised": endpoints_exercised,
        "endpoints_not_exercised": endpoints_not_exercised,
        "truncated": !truncated_fields.is_empty(),
        "truncated_fields": truncated_fields,
        "report_path": report_path
    });
    if let Some(result) = stopped {
        response["twin_id"] = Value::String(result.twin_id.clone());
        response["status"] = Value::String(status_name(result.status).to_owned());
        response["exit_code"] = Value::from(result.exit_code);
    }
    response
}

fn valid_rest_session(session: &Value) -> bool {
    session["request_count"].as_u64().is_some()
        && session["endpoints_exercised"]
            .as_array()
            .is_some_and(|values| all_strings(values))
        && session["endpoints_not_exercised"]
            .as_array()
            .is_some_and(|values| all_strings(values))
        && valid_count_map(session.get("outcomes"), true)
        && valid_count_map(session.get("response_stubs"), true)
        && valid_count_map(session.get("constraint_violations"), false)
        && valid_count_map(session.get("refusals"), false)
}

fn all_strings(values: &[Value]) -> bool {
    values.iter().all(Value::is_string)
}

fn capped_array(value: &Value) -> (Vec<Value>, bool) {
    let values = value.as_array().map(Vec::as_slice).unwrap_or_default();
    (values.iter().take(50).cloned().collect(), values.len() > 50)
}

fn capped_object(value: &Value) -> (Value, bool) {
    let Some(object) = value.as_object() else {
        return (Value::Object(Default::default()), false);
    };
    let mut bounded = serde_json::Map::new();
    let mut truncated = false;
    for (index, (key, value)) in object.iter().enumerate() {
        if index == 50 {
            truncated = true;
            break;
        }
        bounded.insert(key.clone(), value.clone());
    }
    (Value::Object(bounded), truncated)
}

fn valid_count_map(value: Option<&Value>, optional: bool) -> bool {
    match value {
        Some(Value::Object(object)) => object.values().all(|count| count.as_u64().is_some()),
        None => optional,
        _ => false,
    }
}

pub fn trace_summary(trace: &Value) -> Value {
    json!({
        "version": trace["version"],
        "policy": trace["policy"],
        "request_count": trace["events"].as_array().map_or(0, Vec::len)
    })
}

fn status_name(status: TwinStatus) -> &'static str {
    match status {
        TwinStatus::Starting | TwinStatus::Running => "running",
        TwinStatus::Stopping => "running",
        TwinStatus::Stopped => "stopped",
        TwinStatus::Expired => "expired",
    }
}

fn unreadable(detail: String, stderr_path: &Path) -> ToolError {
    ToolError {
        code: "E_REPORT_UNREADABLE",
        message: format!("{detail}; stderr_tail={}", stderr_tail(stderr_path)),
        hint: "Read stderr.log and start a new twin",
    }
}

fn stderr_tail(path: &Path) -> String {
    const TAIL_BYTES: u64 = 2048;
    let Ok(bytes) = fs::read(path) else {
        return "<unavailable>".into();
    };
    let start = bytes.len().saturating_sub(TAIL_BYTES as usize);
    let rendered = String::from_utf8_lossy(&bytes[start..]);
    let cleaned = rendered
        .lines()
        .collect::<Vec<_>>()
        .join(" | ")
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    if cleaned.is_empty() {
        "<empty>".into()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{full_report, full_rest_report, trace_summary, validate_report};
    use crate::registry::TwinKind;

    const READY_REPORT: &str = include_str!("../tests/fixtures/report_ready.json");
    const VIOLATIONS_REPORT: &str = include_str!("../tests/fixtures/report_violations.json");
    const REFUSAL_REPORT: &str = include_str!("../tests/fixtures/refusal.json");

    #[test]
    fn u12_accepts_ready_and_violations_reports_and_rejects_refusal() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("report.json");
        let stderr = temp.path().join("stderr.log");
        std::fs::write(&stderr, "diagnostic tail").unwrap();
        for fixture in [READY_REPORT, VIOLATIONS_REPORT] {
            let report: serde_json::Value = serde_json::from_str(fixture).unwrap();
            assert!(validate_report(&report, TwinKind::Postgres, &path, &stderr).is_ok());
        }
        let refusal: serde_json::Value = serde_json::from_str(REFUSAL_REPORT).unwrap();
        let error = validate_report(&refusal, TwinKind::Postgres, &path, &stderr).unwrap_err();
        assert_eq!("E_REPORT_UNREADABLE", error.code);
        assert!(error.message.contains("diagnostic tail"));
    }

    #[test]
    fn u13_large_report_is_bounded_with_path_and_truncation_marker() {
        let oversized = json!({
            "version":"twinning.v0",
            "outcome":"READY",
            "large":"x".repeat(300 * 1024)
        });
        let trace = json!({"version":"twinning.query-trace.v0","policy":"redacted","events":[]});
        let response = full_report(
            oversized,
            &trace,
            std::path::Path::new("/state/report.json"),
        );
        let rendered = serde_json::to_vec(&response).unwrap();
        assert!(rendered.len() <= 256 * 1024);
        assert_eq!(true, response["truncated"]);
        assert_eq!("/state/report.json", response["report_path"]);
    }

    #[test]
    fn trace_summary_counts_events_without_exposing_query_text() {
        let trace = json!({
            "version":"twinning.query-trace.v0",
            "policy":"redacted",
            "events":[{"sql_hash":"sha256:abc","sql":"must not be copied"}]
        });
        let summary = trace_summary(&trace);
        assert_eq!(1, summary["request_count"]);
        assert!(summary.get("events").is_none());
        assert!(summary.get("sql").is_none());
    }

    #[test]
    fn rest_report_validation_and_summary_drop_spec_and_cap_lists() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("report.json");
        let stderr = temp.path().join("stderr.log");
        let report = json!({
            "version":"twinning.rest-report.v0",
            "outcome":"PASS",
            "spec":{"source":"SECRET_SPEC_TEXT"},
            "session":{
                "request_count":3,
                "endpoints_exercised":(0..60).map(|index| format!("GET /items/{index}")).collect::<Vec<_>>(),
                "endpoints_not_exercised":(0..60).map(|index| format!("POST /items/{index}")).collect::<Vec<_>>(),
                "outcomes":{"response_stub":2},
                "constraint_violations":{},
                "refusals":{"unauthorized":1}
            }
        });
        assert!(validate_report(&report, TwinKind::Rest, &path, &stderr).is_ok());
        let summary = full_rest_report(&report, &path);
        assert_eq!(50, summary["endpoints_exercised"].as_array().unwrap().len());
        assert_eq!(
            50,
            summary["endpoints_not_exercised"].as_array().unwrap().len()
        );
        assert_eq!(true, summary["truncated"]);
        assert!(summary.get("spec").is_none());
        assert!(!summary.to_string().contains("SECRET_SPEC_TEXT"));

        for outcome in ["FAIL", "VIOLATIONS", "REFUSAL"] {
            let mut accepted = report.clone();
            accepted["outcome"] = serde_json::Value::String(outcome.into());
            assert!(validate_report(&accepted, TwinKind::Rest, &path, &stderr).is_ok());
        }
        let empty_session = json!({
            "version":"twinning.rest-report.v0",
            "outcome":"PASS",
            "spec":{"source":"api.yaml","hash":"sha256:abc","resource_count":0,"route_count":0,"security_schemes_bypassed":[]},
            "session":{"request_count":0,"endpoints_exercised":[],"endpoints_not_exercised":[],"constraint_violations":{},"refusals":{}}
        });
        assert!(validate_report(&empty_session, TwinKind::Rest, &path, &stderr).is_ok());
        assert_eq!(
            json!({}),
            full_rest_report(&empty_session, &path)["outcomes"]
        );
        let mut wrong_version = report.clone();
        wrong_version["version"] = serde_json::Value::String("twinning.v0".into());
        assert_eq!(
            "E_REPORT_UNREADABLE",
            validate_report(&wrong_version, TwinKind::Rest, &path, &stderr)
                .unwrap_err()
                .code
        );
    }
}
