use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use tempfile::tempdir;
use twinning::{
    config::RestConfig, protocol::rest::listener::start_test_server,
    protocol::rest::policy::RoutingConfig,
};

// Fixture sanity:
// - Primary resource: File, exposed as `/files`
// - Primary key: non-composite `id`
// - Required fields: `id`, `name`
// - Step 7 omits required field: `name`
// - Step 8 sends string value for integer field: `size`

struct HttpResponse {
    status: u16,
    body: String,
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("rest")
        .join("minimal-api.yaml")
}

fn test_config(report_path: PathBuf) -> RestConfig {
    RestConfig {
        spec_path: fixture_path(),
        host: String::from("127.0.0.1"),
        port: 0,
        run_command: None,
        serve: true,
        serve_defaulted: false,
        report_path: Some(report_path),
        canary_path: None,
        strict: false,
        routing: RoutingConfig::default(),
        auth_mode: None,
        chaos: None,
        json: false,
    }
}

fn request(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).expect("connect to REST twin");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    let body = body.unwrap_or_default();
    let content_headers = if body.is_empty() {
        String::new()
    } else {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
    };
    let raw = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer test-token\r\nConnection: close\r\n{content_headers}\r\n{body}"
    );
    stream
        .write_all(raw.as_bytes())
        .expect("write HTTP request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read HTTP response");

    parse_response(&response)
}

fn parse_response(raw: &str) -> HttpResponse {
    let (head, body) = raw.split_once("\r\n\r\n").expect("HTTP response head");
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("HTTP status")
        .parse::<u16>()
        .expect("numeric status");

    HttpResponse {
        status,
        body: body.to_owned(),
    }
}

fn json_body(response: &HttpResponse) -> Value {
    serde_json::from_str(&response.body).expect("response body should be JSON")
}

fn wait_for_report(path: &Path) -> Value {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(raw) = fs::read_to_string(path)
            && let Ok(report) = serde_json::from_str(&raw)
        {
            return report;
        }
        assert!(
            Instant::now() < deadline,
            "REST report was not written to {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn metadata_api_smoke_exercises_crud_refusals_and_rest_report() {
    let dir = tempdir().expect("tempdir");
    let report_path = dir.path().join("rest-report.json");
    let (addr, shutdown_tx) = start_test_server(test_config(report_path.clone()));

    let post = request(
        addr,
        "POST",
        "/files",
        Some(r#"{"id":1,"name":"foo.txt","size":1024}"#),
    );
    assert_eq!(post.status, 201, "POST /files body: {}", post.body);
    assert_eq!(json_body(&post)["id"], json!(1));

    let first_get = request(addr, "GET", "/files/1", None);
    assert_eq!(
        first_get.status, 200,
        "GET /files/1 body: {}",
        first_get.body
    );
    assert_eq!(
        json_body(&first_get),
        json!({ "id": 1, "name": "foo.txt", "size": 1024 })
    );

    let patch = request(addr, "PATCH", "/files/1", Some(r#"{"name":"bar.txt"}"#));
    assert_eq!(patch.status, 200, "PATCH /files/1 body: {}", patch.body);
    assert_eq!(json_body(&patch)["name"], json!("bar.txt"));

    let second_get = request(addr, "GET", "/files/1", None);
    assert_eq!(
        second_get.status, 200,
        "second GET /files/1 body: {}",
        second_get.body
    );
    assert_eq!(json_body(&second_get)["name"], json!("bar.txt"));

    let delete = request(addr, "DELETE", "/files/1", None);
    assert_eq!(delete.status, 204, "DELETE /files/1 body: {}", delete.body);
    assert!(
        delete.body.is_empty(),
        "204 response should not emit a body"
    );

    let missing_get = request(addr, "GET", "/files/1", None);
    assert_eq!(
        missing_get.status, 404,
        "missing GET /files/1 body: {}",
        missing_get.body
    );

    let missing_required = request(addr, "POST", "/files", Some(r#"{"id":2,"size":2048}"#));
    assert_eq!(
        missing_required.status, 422,
        "missing required response body: {}",
        missing_required.body
    );

    let type_mismatch = request(
        addr,
        "POST",
        "/files",
        Some(r#"{"id":2,"name":"baz.txt","size":"not-an-int"}"#),
    );
    assert_eq!(
        type_mismatch.status, 400,
        "type mismatch response body: {}",
        type_mismatch.body
    );

    shutdown_tx.send(()).expect("shutdown REST test server");
    let report = wait_for_report(&report_path);

    assert_eq!(report["version"], "twinning.rest-report.v0");
    assert!(report.get("spec").is_some(), "report should include spec");
    assert!(
        report.get("session").is_some(),
        "report should include session"
    );
    assert_eq!(report["session"]["request_count"], 8);
    assert_eq!(report["session"]["refusals"]["not_found"], 1);
    assert_eq!(report["session"]["refusals"]["missing_required_field"], 1);
    assert_eq!(report["session"]["refusals"]["type_mismatch"], 1);

    let exercised = report["session"]["endpoints_exercised"]
        .as_array()
        .expect("exercised endpoints array")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    for expected in [
        "POST /files",
        "GET /files/{id}",
        "PATCH /files/{id}",
        "DELETE /files/{id}",
    ] {
        assert!(
            exercised.contains(&expected),
            "missing exercised endpoint {expected}: {exercised:?}"
        );
    }
}

#[test]
fn doctor_health_json_still_works_with_rest_module_loaded() {
    let output = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_twinning")))
        .args(["doctor", "health", "--json"])
        .output()
        .expect("run twinning doctor health");

    assert!(
        output.status.success(),
        "doctor health should succeed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let health: Value = serde_json::from_slice(&output.stdout).expect("doctor JSON");
    assert_eq!(health["version"], "twinning.v0");
    assert_eq!(health["mode"], "doctor_health");
    assert_eq!(health["outcome"], "READY");
}
