use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use arrow_array::{Array, StringArray};
use arrow_ipc::reader::StreamReader;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use tempfile::tempdir;
use twinning_snowflake::{
    config::{SnowflakeConfig, SnowflakeConfigParts},
    listener::start_embedded_server,
};

#[test]
fn snowflake_smoke_go_style_http_sequence_and_report() {
    let report_dir = tempdir().expect("report tempdir");
    let report_path = report_dir.path().join("snowflake-report.json");
    let server = start_embedded_server(SnowflakeConfig::from_parts(
        SnowflakeConfigParts {
            schema_path: Some(fixture_schema_path()),
            host: "127.0.0.1".to_owned(),
            port: 0,
            run_command: None,
            serve: true,
            report_path: Some(report_path.clone()),
            materialize_source_url: None,
            max_rows_per_table: 100_000,
        },
        true,
    ))
    .expect("Snowflake server starts");
    let base_url = format!("http://{}", server.addr());

    let login = post_json(
        &format!("{base_url}/session/v1/login-request"),
        None,
        json!({
            "data": {
                "CLIENT_APP_ID": "Go",
                "ACCOUNT_NAME": "fakesnow",
                "LOGIN_NAME": "go_user",
                "SESSION_PARAMETERS": {}
            }
        }),
    );
    assert_eq!(true, login["success"]);
    let token = login["data"]["token"].as_str().expect("session token");

    let select = query(&base_url, token, "SELECT 1 AS n");
    assert_eq!(true, select["success"]);
    assert_eq!("N", select["Data"]["rowtype"][0]["name"]);
    assert_eq!("fixed", select["Data"]["rowtype"][0]["type"]);
    assert_eq!(38, select["Data"]["rowtype"][0]["precision"]);
    assert_eq!(0, select["Data"]["rowtype"][0]["scale"]);
    let batch = arrow_batch(&select);
    assert_eq!(1, batch.num_rows());
    assert_eq!(1, batch.num_columns());
    assert_eq!("N", batch.schema().field(0).name());
    assert_eq!(
        Some(&"FIXED".to_owned()),
        batch.schema().field(0).metadata().get("logicalType")
    );

    let warehouses = query(&base_url, token, "SHOW WAREHOUSES");
    assert_eq!("name", warehouses["Data"]["rowtype"][0]["name"]);
    let warehouse_batch = arrow_batch(&warehouses);
    assert_eq!("TWIN_WH", string_value(&warehouse_batch, 0, 0));

    let tables = query(&base_url, token, "SHOW TABLES");
    assert_eq!(2, tables["Data"]["total"]);
    let tables_batch = arrow_batch(&tables);
    assert_eq!(2, tables_batch.num_rows());
    let table_names = [
        string_value(&tables_batch, 1, 0),
        string_value(&tables_batch, 1, 1),
    ];
    assert!(table_names.contains(&"OBSERVATIONS".to_owned()));
    assert!(table_names.contains(&"ACCOUNT_EVENTS".to_owned()));

    let logout = post_json(
        &format!("{base_url}/session?delete=true"),
        Some(token),
        json!({}),
    );
    assert_eq!(true, logout["success"]);

    let deleted_status = post_json_error_status(
        &format!("{base_url}/queries/v1/query-request"),
        Some(token),
        json!({"sqlText": "SELECT 1 AS n"}),
    );
    assert_eq!(401, deleted_status);

    server.shutdown().expect("server shutdown");
    let report: Value =
        serde_json::from_str(&fs::read_to_string(&report_path).expect("read report"))
            .expect("report json");
    assert_eq!("twinning.snowflake-report.v0", report["version"]);
    assert_eq!("PASS", report["outcome"]);
    assert_eq!("schema_file", report["catalog"]["source"]);
    assert_eq!(2, report["catalog"]["table_count"]);
    assert_eq!(10, report["catalog"]["total_rows_materialized"]);
    assert_eq!(6, report["session"]["request_count"]);
    assert_eq!(3, report["session"]["query_count"]);
    assert_eq!(2, report["session"]["show_count"]);
    assert_eq!(1, report["session"]["error_count"]);
}

#[test]
fn snowflake_cli_materializes_source_rows_with_fake_python_connector() {
    let workspace = tempdir().expect("workspace");
    let schema_path = workspace.path().join("schema.sql");
    let report_path = workspace.path().join("snowflake-report.json");
    let fake_python = workspace.path().join("fake-python.py");
    fs::write(
        &schema_path,
        "CREATE TABLE observations (id NUMBER(38,0), label VARCHAR(32));\n",
    )
    .expect("write schema");
    write_fake_python(&fake_python);

    let output = Command::new(twinning_bin())
        .arg("snowflake")
        .arg("--schema")
        .arg(&schema_path)
        .arg("--materialize-source-url")
        .arg("snowflake://fixture-account/TWINDB")
        .arg("--max-rows-per-table")
        .arg("2")
        .arg("--report")
        .arg(&report_path)
        .arg("--run")
        .arg("true")
        .arg("--json")
        .env("TWINNING_SNOWFLAKE_PYTHON_BIN", &fake_python)
        .env("SNOWFLAKE_USER", "fixture-user")
        .env("SNOWFLAKE_PASSWORD", "fixture-password")
        .output()
        .expect("run twinning snowflake materialization");

    assert!(
        output.status.success(),
        "snowflake materialization should succeed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: Value = serde_json::from_slice(&output.stdout).expect("run JSON stdout");
    assert_eq!("twinning.snowflake-run.v0", stdout["version"]);

    let report: Value =
        serde_json::from_str(&fs::read_to_string(&report_path).expect("read report"))
            .expect("report JSON");
    assert_eq!(
        "snowflake_connector_fetchall",
        report["catalog"]["source_materialization"]["method"]
    );
    assert_eq!(
        1,
        report["catalog"]["source_materialization"]["table_count"]
    );
    assert_eq!(2, report["catalog"]["source_materialization"]["row_count"]);
    assert_eq!(2, report["catalog"]["total_rows_materialized"]);
    assert!(
        report["catalog"]["source_materialization"]["source_identity"]
            .as_str()
            .expect("source identity")
            .starts_with("sha256:")
    );
}

fn query(base_url: &str, token: &str, sql: &str) -> Value {
    post_json(
        &format!("{base_url}/queries/v1/query-request"),
        Some(token),
        json!({"sqlText": sql, "sequenceId": 1}),
    )
}

fn post_json(url: &str, token: Option<&str>, body: Value) -> Value {
    let mut request = ureq::post(url).set("Content-Type", "application/json");
    if let Some(token) = token {
        request = request.set("authorization", &format!("Snowflake Token=\"{token}\""));
    }
    request
        .send_json(body)
        .expect("HTTP request succeeds")
        .into_json()
        .expect("HTTP JSON response parses")
}

fn post_json_error_status(url: &str, token: Option<&str>, body: Value) -> u16 {
    let mut request = ureq::post(url).set("Content-Type", "application/json");
    if let Some(token) = token {
        request = request.set("authorization", &format!("Snowflake Token=\"{token}\""));
    }
    match request.send_json(body) {
        Err(ureq::Error::Status(status, _)) => status,
        Err(error) => panic!("expected HTTP status error, got {error}"),
        Ok(response) => panic!("expected HTTP status error, got {}", response.status()),
    }
}

fn arrow_batch(response: &Value) -> arrow_array::RecordBatch {
    let encoded = response["Data"]["rowsetBase64"]
        .as_str()
        .expect("rowsetBase64 string");
    assert!(!encoded.is_empty(), "rowsetBase64 should be non-empty");
    let bytes = BASE64.decode(encoded).expect("rowsetBase64 decodes");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).expect("IPC stream");
    reader.next().expect("one IPC batch").expect("batch reads")
}

fn string_value(batch: &arrow_array::RecordBatch, column: usize, row: usize) -> String {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column")
        .value(row)
        .to_owned()
}

fn fixture_schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("canaries")
        .join("snowflake_smoke")
        .join("schema.sql")
}

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn write_fake_python(path: &Path) {
    fs::write(
        path,
        r#"#!/usr/bin/env python3
import json
import sys

if sys.argv[1:3] == ["-c", "import snowflake.connector"]:
    sys.exit(0)

if len(sys.argv) >= 11 and sys.argv[1] == "-c":
    table = sys.argv[8]
    columns = json.loads(sys.argv[10])
    if table == "OBSERVATIONS":
        print(json.dumps({
            "columns": columns,
            "rows": [
                [1, "alpha"],
                [2, "beta"]
            ]
        }))
        sys.exit(0)

print("unexpected fake python invocation: " + " ".join(sys.argv[1:]), file=sys.stderr)
sys.exit(1)
"#,
    )
    .expect("write fake python");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)
            .expect("fake python metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod fake python");
    }
}
