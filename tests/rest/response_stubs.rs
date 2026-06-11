use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    time::Duration,
};

use serde_json::Value;
use tempfile::tempdir;
use twinning::{
    config::RestConfig,
    protocol::rest::{
        config::ChaosConfig, listener::start_embedded_server, policy::RoutingConfig,
        spec::parse_rest_catalog_bytes,
    },
};

struct HttpResponse {
    status: u16,
    headers: String,
    body: String,
}

fn response_stub_spec() -> (tempfile::TempDir, PathBuf) {
    let dir = tempdir().expect("tempdir");
    let spec_path = dir.path().join("response-stubs.yaml");
    fs::write(
        &spec_path,
        r##"
openapi: 3.0.3
info: { title: Response stubs, version: "1.0" }
components:
  securitySchemes:
    ApiKeyAuth:
      type: apiKey
      name: X-TEST-KEY
      in: header
  schemas:
    File:
      type: object
      required: [id, name]
      additionalProperties: false
      properties:
        id: { type: integer }
        name: { type: string }
paths:
  /files:
    post:
      security:
        - ApiKeyAuth: []
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/File"
      responses:
        "201":
          description: Created.
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/File"
x-twinning:
  response-stubs:
    - id: file_create_stub
      method: POST
      path: /files
      when:
        body-json-equals:
          name: Ada
          id: 1
      status: 202
      headers:
        X-Twinning-Stub: file_create_stub
      body:
        stubbed: true
        source: response-stub
"##,
    )
    .expect("write spec");
    (dir, spec_path)
}

fn openfigi_stub_fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("rest")
        .join("openfigi_v2_v3")
        .join("response-stub-schema.yaml")
}

fn test_config(spec_path: PathBuf) -> RestConfig {
    RestConfig {
        spec_path,
        host: String::from("127.0.0.1"),
        port: 0,
        run_command: None,
        serve: true,
        serve_defaulted: false,
        report_path: None,
        canary_path: None,
        strict: false,
        routing: RoutingConfig::default(),
        server_variables: BTreeMap::new(),
        auth_mode: None,
        chaos: None,
        json: false,
    }
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).expect("connect to REST twin");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    let mut raw = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        raw.push_str(name);
        raw.push_str(": ");
        raw.push_str(value);
        raw.push_str("\r\n");
    }
    raw.push_str("\r\n");
    raw.push_str(body);
    stream.write_all(raw.as_bytes()).expect("write request");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
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
        headers: head.to_owned(),
        body: body.to_owned(),
    }
}

#[test]
fn response_stub_exact_hit_uses_canonical_json_body_match() {
    let (_dir, spec_path) = response_stub_spec();
    let server = start_embedded_server(test_config(spec_path)).expect("server starts");

    let response = request(
        server.addr(),
        "POST",
        "/files",
        &[
            ("Content-Type", "application/json"),
            ("X-TEST-KEY", "test-token"),
        ],
        r#"{"id":1,"name":"Ada"}"#,
    );
    assert_eq!(response.status, 202, "body: {}", response.body);
    assert!(
        response
            .headers
            .to_ascii_lowercase()
            .contains("x-twinning-stub: file_create_stub"),
        "headers: {}",
        response.headers
    );
    assert_eq!(
        serde_json::from_str::<Value>(&response.body).expect("json response"),
        serde_json::json!({ "stubbed": true, "source": "response-stub" })
    );

    let session = server.shutdown().expect("shutdown");
    assert_eq!(session.summary.response_stubs["file_create_stub"], 1);
}

#[test]
fn response_stub_body_mismatch_falls_through_to_normal_dispatch() {
    let (_dir, spec_path) = response_stub_spec();
    let server = start_embedded_server(test_config(spec_path)).expect("server starts");

    let response = request(
        server.addr(),
        "POST",
        "/files",
        &[
            ("Content-Type", "application/json"),
            ("X-TEST-KEY", "test-token"),
        ],
        r#"{"id":2,"name":"Grace"}"#,
    );
    assert_eq!(response.status, 201, "body: {}", response.body);
    assert!(
        !response.body.contains("response-stub"),
        "body should come from normal dispatch: {}",
        response.body
    );

    let session = server.shutdown().expect("shutdown");
    assert!(session.summary.response_stubs.is_empty());
}

#[test]
fn auth_shape_runs_before_response_stubs() {
    let (_dir, spec_path) = response_stub_spec();
    let server = start_embedded_server(test_config(spec_path)).expect("server starts");

    let response = request(
        server.addr(),
        "POST",
        "/files",
        &[("Content-Type", "application/json")],
        r#"{"id":1,"name":"Ada"}"#,
    );
    assert_eq!(response.status, 401, "body: {}", response.body);
    assert!(response.body.contains(r#""code":"unauthorized""#));

    let session = server.shutdown().expect("shutdown");
    assert!(session.summary.response_stubs.is_empty());
}

#[test]
fn chaos_rate_limit_runs_before_response_stubs() {
    let (_dir, spec_path) = response_stub_spec();
    let mut config = test_config(spec_path);
    config.chaos = Some(ChaosConfig {
        rate_limit_per_million: ChaosConfig::SCALE,
        server_error_per_million: 0,
        timeout_per_million: 0,
    });
    let server = start_embedded_server(config).expect("server starts");

    let response = request(
        server.addr(),
        "POST",
        "/files",
        &[
            ("Content-Type", "application/json"),
            ("X-TEST-KEY", "test-token"),
        ],
        r#"{"id":1,"name":"Ada"}"#,
    );
    assert_eq!(response.status, 429, "body: {}", response.body);
    assert!(
        response
            .headers
            .to_ascii_lowercase()
            .contains("retry-after: 1"),
        "headers: {}",
        response.headers
    );
    assert!(response.body.contains(r#""code":"rate_limited""#));

    let session = server.shutdown().expect("shutdown");
    assert!(session.summary.response_stubs.is_empty());
}

#[test]
fn response_stub_unknown_mounted_route_refuses_startup() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, spec_path) = response_stub_spec();
    let raw = fs::read_to_string(&spec_path).expect("read spec");
    fs::write(
        &spec_path,
        raw.replace("path: /files", "path: /missing-route"),
    )
    .expect("rewrite spec");

    let error = match start_embedded_server(test_config(spec_path)) {
        Ok(server) => {
            let _ = server.shutdown();
            return Err(std::io::Error::other("startup should fail").into());
        }
        Err(error) => error,
    };
    let rendered = serde_json::to_value(error.as_ref()).expect("serialize refusal");
    assert_eq!(rendered["refusal"]["code"], "E_REST_INVALID_X_TWINNING");
    assert_eq!(
        rendered["refusal"]["detail"]["reason"],
        "unknown_stub_route"
    );
    Ok(())
}

#[test]
fn openfigi_mapping_response_stub_returns_protocol_shaped_array() {
    let mut config = test_config(openfigi_stub_fixture_path());
    config
        .server_variables
        .insert("basePath".to_owned(), "v3".to_owned());
    config.routing.server_variables = config.server_variables.clone();
    let server = start_embedded_server(config).expect("server starts");

    let response = request(
        server.addr(),
        "POST",
        "/v3/mapping",
        &[
            ("Content-Type", "application/json"),
            ("X-OPENFIGI-APIKEY", "test-token"),
        ],
        r#"[{"idType":"ID_CUSIP","idValue":"037833100"}]"#,
    );
    assert_eq!(response.status, 200, "body: {}", response.body);
    assert_ne!(response.body, r#"{"data":null,"warning":null}"#);

    let body = serde_json::from_str::<Value>(&response.body).expect("OpenFIGI JSON");
    assert!(body.is_array(), "body should be top-level array: {body}");
    assert_eq!(body[0]["data"][0]["figi"], "BBG000B9XRY4");
    assert_eq!(body[0]["data"][0]["ticker"], "AAPL");
    assert_eq!(body[0]["data"][0]["name"], "APPLE INC");
    assert_eq!(body[0]["data"][0]["securityType"], "Common Stock");
    assert_eq!(body[0]["data"][0]["exchCode"], "US");

    let session = server.shutdown().expect("shutdown");
    assert_eq!(session.summary.response_stubs["openfigi_cusip_success"], 1);
}

#[test]
fn response_stub_fixture_parses_as_openapi() {
    let path = openfigi_stub_fixture_path();
    let raw = fs::read(&path).expect("read fixture");
    let catalog =
        parse_rest_catalog_bytes(&raw, display_path(&path)).expect("response-stub fixture parses");

    assert_eq!(
        catalog
            .x_twinning
            .as_ref()
            .expect("x-twinning")
            .response_stubs
            .len(),
        2
    );
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}
