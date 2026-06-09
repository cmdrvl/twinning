#![forbid(unsafe_code)]
#![cfg(feature = "rest")]

use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    time::Duration,
};

use tempfile::{TempDir, tempdir};
use twinning::{
    config::RestConfig, protocol::rest::listener::start_test_server,
    protocol::rest::policy::RoutingConfig,
};

struct SpecFixture {
    _dir: TempDir,
    path: PathBuf,
}

struct HttpResponse {
    status: u16,
    headers: String,
    body: String,
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
        server_variables: std::collections::BTreeMap::new(),
        auth_mode: None,
        chaos: None,
        json: false,
    }
}

fn write_spec(name: &str, body: &str) -> SpecFixture {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join(name);
    fs::write(&path, body).expect("write spec");
    SpecFixture { _dir: dir, path }
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: Option<&str>,
) -> HttpResponse {
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
    let auth_header = authorization
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    let raw = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{auth_header}Connection: close\r\n{content_headers}\r\n{body}"
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

fn bearer_challenge(headers: &str) -> bool {
    headers
        .to_ascii_lowercase()
        .contains("www-authenticate: bearer realm=\"twinning\"")
}

#[test]
fn onepassword_and_slack_bearer_routes_require_authorization_shape() {
    let cases = [
        (
            "1Password",
            write_onepassword_spec(),
            vec![
                ("/vaults", "bearerAuth"),
                ("/vaults/vault-1/items", "bearerAuth"),
            ],
        ),
        (
            "Slack",
            write_slack_spec(),
            vec![("/conversations", "slackAuth"), ("/messages", "slackAuth")],
        ),
    ];
    let mut required_routes = 0;
    let mut missing_credentials_401 = 0;

    for (api_name, fixture, routes) in cases {
        let (addr, shutdown_tx) = start_test_server(test_config(fixture.path.clone()));

        for (path, scheme_name) in routes {
            required_routes += 1;

            let missing = request(addr, "GET", path, None, None);
            assert_eq!(
                missing.status, 401,
                "{api_name} {path} without credentials should be unauthorized: {}",
                missing.body
            );
            assert!(
                bearer_challenge(&missing.headers),
                "{api_name} {path} should include a bearer challenge: {}",
                missing.headers
            );
            assert!(
                missing.body.contains(r#""code":"unauthorized""#),
                "{api_name} {path} should return unauthorized JSON: {}",
                missing.body
            );
            assert!(
                missing
                    .body
                    .contains(&format!("Authorization required: {scheme_name} (header)")),
                "{api_name} {path} should name the required scheme: {}",
                missing.body
            );
            missing_credentials_401 += 1;

            let present = request(addr, "GET", path, Some("Bearer any-test-token"), None);
            assert_eq!(
                present.status, 200,
                "{api_name} {path} with any bearer value should pass auth shape: {}",
                present.body
            );
        }

        shutdown_tx.send(()).expect("shutdown REST test server");
    }

    assert_eq!(
        missing_credentials_401, required_routes,
        "100% of auth-required routes should return 401 when credentials are missing"
    );
}

#[test]
fn petstore_public_routes_do_not_false_positive_unauthorized() {
    let fixture = write_petstore_spec();
    let (addr, shutdown_tx) = start_test_server(test_config(fixture.path));
    let mut public_routes = 0;
    let mut unauthorized_false_positives = 0;

    let list = request(addr, "GET", "/pets", None, None);
    public_routes += 1;
    unauthorized_false_positives += usize::from(list.status == 401);
    assert_eq!(list.status, 200, "public GET /pets body: {}", list.body);

    let body = r#"{"id":1,"name":"Fido","status":"available"}"#;
    let create = request(addr, "POST", "/pets", None, Some(body));
    public_routes += 1;
    unauthorized_false_positives += usize::from(create.status == 401);
    assert_eq!(
        create.status, 201,
        "public POST /pets body: {}",
        create.body
    );

    let get_one = request(addr, "GET", "/pets/1", None, None);
    public_routes += 1;
    unauthorized_false_positives += usize::from(get_one.status == 401);
    assert_eq!(
        get_one.status, 200,
        "public GET /pets/1 body: {}",
        get_one.body
    );

    assert_eq!(public_routes, 3);
    assert_eq!(
        unauthorized_false_positives, 0,
        "public Petstore routes should have 0 false-positive 401s"
    );

    shutdown_tx.send(()).expect("shutdown REST test server");
}

fn write_onepassword_spec() -> SpecFixture {
    write_spec(
        "onepassword-auth.yaml",
        r##"
openapi: 3.0.3
info:
  title: 1Password auth compliance fixture
  version: "1.0"
security:
  - bearerAuth: []
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
  schemas:
    Vault:
      type: object
      required: [id, name]
      properties:
        id: { type: string }
        name: { type: string }
    Item:
      type: object
      required: [id, vaultUuid, title]
      properties:
        id: { type: string }
        vaultUuid: { type: string }
        title: { type: string }
paths:
  /vaults:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Vault"
  /vaults/{vaultUuid}/items:
    parameters:
      - name: vaultUuid
        in: path
        required: true
        schema: { type: string }
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Item"
"##,
    )
}

fn write_slack_spec() -> SpecFixture {
    write_spec(
        "slack-auth.yaml",
        r##"
openapi: 3.0.3
info:
  title: Slack auth compliance fixture
  version: "1.0"
security:
  - slackAuth: []
components:
  securitySchemes:
    slackAuth:
      type: http
      scheme: bearer
  schemas:
    Conversation:
      type: object
      required: [id, name]
      properties:
        id: { type: string }
        name: { type: string }
    Message:
      type: object
      required: [id, text]
      properties:
        id: { type: string }
        text: { type: string }
paths:
  /conversations:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Conversation"
  /messages:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Message"
"##,
    )
}

fn write_petstore_spec() -> SpecFixture {
    write_spec(
        "petstore-public.yaml",
        r##"
openapi: 3.0.3
info:
  title: Petstore auth compliance fixture
  version: "1.0"
components:
  schemas:
    Pet:
      type: object
      required: [id, name]
      properties:
        id: { type: integer }
        name: { type: string }
        status: { type: string }
paths:
  /pets:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Pet"
    post:
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/Pet"
      responses:
        "201":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pet"
  /pets/{petId}:
    parameters:
      - name: petId
        in: path
        required: true
        schema: { type: integer }
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pet"
"##,
    )
}
