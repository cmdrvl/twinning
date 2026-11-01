//! Validates committed twin reports and renders bounded MCP response bodies.

use std::fs;
use std::path::Path;

use serde_json::{Value, json};

use crate::{errors::ToolError, registry::TwinStatus, supervisor::StopResult};

const MAX_REPORT_RESPONSE_BYTES: usize = 256 * 1024;

pub fn validate_report(report: &Value, path: &Path, stderr_path: &Path) -> Result<(), ToolError> {
    if report["version"] != "twinning.v0" {
        return Err(unreadable(
            format!("expected a twinning.v0 report at {}", path.display()),
            stderr_path,
        ));
    }
    // Current interactive twinning emits FAIL for an embedded verify violation;
    // VIOLATIONS remains accepted for the spec's declared report vocabulary.
    if !matches!(
        report["outcome"].as_str(),
        Some("READY" | "VIOLATIONS" | "FAIL")
    ) {
        return Err(unreadable(
            format!("report outcome is not readable at {}", path.display()),
            stderr_path,
        ));
    }
    Ok(())
}

pub fn read_report(path: &Path, stderr_path: &Path) -> Result<Value, ToolError> {
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
    validate_report(&report, path, stderr_path)?;
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

    use super::{full_report, trace_summary, validate_report};

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
            assert!(validate_report(&report, &path, &stderr).is_ok());
        }
        let refusal: serde_json::Value = serde_json::from_str(REFUSAL_REPORT).unwrap();
        let error = validate_report(&refusal, &path, &stderr).unwrap_err();
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
}
