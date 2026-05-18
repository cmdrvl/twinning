use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    time::Duration,
};

use twinning::{
    config::RestConfig,
    protocol::rest::{config::ChaosConfig, listener::start_test_server, policy::RoutingConfig},
};

struct HttpResponse {
    status: u16,
    headers: String,
    body: String,
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("rest")
        .join("minimal-api.yaml")
}

fn test_config(chaos: Option<ChaosConfig>) -> RestConfig {
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
        auth_mode: None,
        chaos,
        json: false,
    }
}

fn request(addr: SocketAddr, method: &str, path: &str) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).expect("connect to REST twin");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    let raw = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer test-token\r\nConnection: close\r\n\r\n"
    );
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
fn chaos_rate_limit_one_forces_429_before_normal_dispatch() {
    let (addr, shutdown_tx) = start_test_server(test_config(Some(ChaosConfig {
        rate_limit_per_million: ChaosConfig::SCALE,
        server_error_per_million: 0,
        timeout_per_million: 0,
    })));

    let response = request(addr, "GET", "/files");

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

    shutdown_tx.send(()).expect("shutdown");
}

#[test]
fn chaos_server_error_one_forces_500_before_normal_dispatch() {
    let (addr, shutdown_tx) = start_test_server(test_config(Some(ChaosConfig {
        rate_limit_per_million: 0,
        server_error_per_million: ChaosConfig::SCALE,
        timeout_per_million: 0,
    })));

    let response = request(addr, "GET", "/files");

    assert_eq!(response.status, 500, "body: {}", response.body);
    assert!(response.body.contains(r#""code":"internal_error""#));

    shutdown_tx.send(()).expect("shutdown");
}
