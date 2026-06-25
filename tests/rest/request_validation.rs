use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    time::Duration,
};

use serde_json::{Value, json};
use twinning::{
    config::RestConfig,
    protocol::rest::{listener::start_embedded_server, policy::RoutingConfig},
};

struct HttpResponse {
    status: u16,
    body: String,
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("rest")
        .join("lineage-bulk.yaml")
}

fn test_config() -> RestConfig {
    RestConfig {
        spec_path: fixture_path(),
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
    content_type: Option<&str>,
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
    if let Some(content_type) = content_type {
        raw.push_str("Content-Type: ");
        raw.push_str(content_type);
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
        body: body.to_owned(),
    }
}

fn json_body(response: &HttpResponse) -> Value {
    serde_json::from_str(&response.body).expect("response body should be JSON")
}

#[test]
fn request_body_validation_precedes_stubs_and_unsupported_shape() {
    let server = start_embedded_server(test_config()).expect("server starts");
    let addr = server.addr();
    let bulk_path = "/metadata-api/v2/lineage/bulk";

    let stubbed = request(
        addr,
        "POST",
        bulk_path,
        Some("application/json"),
        r#"{"edges":[{"upstream_key":"table://warehouse.raw/orders","downstream_key":"table://warehouse.mart/orders","source":"sql_lineage","confidence":null}]}"#,
    );
    assert_eq!(stubbed.status, 202, "body: {}", stubbed.body);
    assert_eq!(
        json_body(&stubbed),
        json!({ "request_valid": true, "stubbed": true })
    );

    let valid_unsupported = request(
        addr,
        "POST",
        bulk_path,
        Some("application/vnd.metadata+json; charset=utf-8"),
        r#"{"edges":[{"upstream_key":"u","downstream_key":"d","source":"manual"}]}"#,
    );
    assert_eq!(
        valid_unsupported.status, 501,
        "body: {}",
        valid_unsupported.body
    );
    let valid_unsupported_body = json_body(&valid_unsupported);
    assert_eq!(valid_unsupported_body["code"], "unsupported_shape");
    assert_eq!(valid_unsupported_body["request_valid"], true);
    assert_eq!(
        valid_unsupported_body["request_schema"],
        "#/components/schemas/BulkLineageRequest"
    );

    let missing_edges = request(
        addr,
        "POST",
        bulk_path,
        Some("application/json"),
        r#"{"extra":1}"#,
    );
    assert_eq!(missing_edges.status, 422, "body: {}", missing_edges.body);
    let missing_edges_body = json_body(&missing_edges);
    assert_eq!(missing_edges_body["code"], "schema_validation_failed");
    assert_eq!(missing_edges_body["path"], "$.edges");

    let non_array_edges = request(
        addr,
        "POST",
        bulk_path,
        Some("application/json"),
        r#"{"edges":"not-array"}"#,
    );
    assert_eq!(
        non_array_edges.status, 422,
        "invalid body should not receive response stub: {}",
        non_array_edges.body
    );
    let non_array_body = json_body(&non_array_edges);
    assert_eq!(non_array_body["code"], "schema_validation_failed");
    assert_eq!(non_array_body["path"], "$.edges");
    assert_eq!(non_array_body["expected"], "array");
    assert_eq!(non_array_body["received"], "string");

    let missing_downstream = request(
        addr,
        "POST",
        bulk_path,
        Some("application/json"),
        r#"{"edges":[{"upstream_key":"u"}]}"#,
    );
    assert_eq!(
        missing_downstream.status, 422,
        "body: {}",
        missing_downstream.body
    );
    assert_eq!(
        json_body(&missing_downstream)["path"],
        "$.edges[0].downstream_key"
    );

    let non_string_upstream = request(
        addr,
        "POST",
        bulk_path,
        Some("application/json"),
        r#"{"edges":[{"upstream_key":1,"downstream_key":"d"}]}"#,
    );
    assert_eq!(
        non_string_upstream.status, 422,
        "body: {}",
        non_string_upstream.body
    );
    let non_string_body = json_body(&non_string_upstream);
    assert_eq!(non_string_body["path"], "$.edges[0].upstream_key");
    assert_eq!(non_string_body["expected"], "string");
    assert_eq!(non_string_body["received"], "integer");

    let malformed = request(addr, "POST", bulk_path, Some("application/json"), "{");
    assert_eq!(malformed.status, 400, "body: {}", malformed.body);
    assert_eq!(json_body(&malformed)["code"], "invalid_json");

    let wrong_content_type = request(
        addr,
        "POST",
        bulk_path,
        Some("text/plain"),
        r#"{"edges":[{"upstream_key":"u","downstream_key":"d"}]}"#,
    );
    assert_eq!(
        wrong_content_type.status, 415,
        "body: {}",
        wrong_content_type.body
    );
    assert_eq!(
        json_body(&wrong_content_type)["code"],
        "unsupported_media_type"
    );

    let empty_required = request(addr, "POST", bulk_path, Some("application/json"), "");
    assert_eq!(empty_required.status, 422, "body: {}", empty_required.body);
    assert_eq!(json_body(&empty_required)["path"], "$");

    let empty_optional = request(
        addr,
        "POST",
        "/metadata-api/v2/lineage/optional",
        Some("application/json"),
        "",
    );
    assert_eq!(
        empty_optional.status, 501,
        "optional absent body is valid but route remains unsupported: {}",
        empty_optional.body
    );
    assert_eq!(json_body(&empty_optional)["request_valid"], true);

    let session = server.shutdown().expect("shutdown");
    assert_eq!(session.summary.response_stubs["lineage_bulk_ok"], 1);
    assert_eq!(session.summary.outcomes["response_stub"], 1);
    assert_eq!(session.summary.outcomes["valid_unsupported"], 2);
    assert_eq!(session.summary.outcomes["schema_validation_refusal"], 5);
    assert_eq!(session.summary.outcomes["rest_refusal"], 2);
    assert_eq!(session.summary.refusals["schema_validation_failed"], 5);
    assert_eq!(session.summary.refusals["unsupported_shape"], 2);
    assert_eq!(session.summary.refusals["invalid_json"], 1);
    assert_eq!(session.summary.refusals["unsupported_media_type"], 1);
}
