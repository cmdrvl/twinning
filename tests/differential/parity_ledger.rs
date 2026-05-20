#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

use super::runner::DIFFERENTIAL_POSTGRES_URL_ENV;

const FIXTURE_ROOT: &str = "tests/fixtures/differential/parity_ledger";
const FIXTURE_VERSION: &str = "twinning.differential.parity-ledger.v0";
const LEDGER_VERSION: &str = "twinning.parity-ledger.v0";

#[test]
fn parity_ledger_fixture_is_checked_in_and_documents_requirement_matrix() {
    let fixture = load_fixture();

    assert_eq!(fixture.version, FIXTURE_VERSION);
    assert_eq!(fixture.cases.len(), 4);
    assert!(
        schema_path().exists(),
        "parity ledger schema fixture should be checked in"
    );

    let matrix_ids = fixture
        .requirement_matrix
        .iter()
        .map(|requirement| requirement.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        matrix_ids,
        BTreeSet::from([
            "process_run_once_twin",
            "declared_success_parity",
            "catalog_metadata_parity",
            "subset_refusal_parity",
        ])
    );

    for requirement in &fixture.requirement_matrix {
        assert!(
            !requirement.completion_criterion.trim().is_empty(),
            "requirement `{}` should name the completion criterion it advances",
            requirement.id
        );
        assert!(
            !requirement.coverage.trim().is_empty(),
            "requirement `{}` should describe coverage",
            requirement.id
        );
        assert!(
            !requirement.cases.is_empty(),
            "requirement `{}` should map to at least one case",
            requirement.id
        );
    }

    let cases_by_id = fixture
        .cases
        .iter()
        .map(|case| (case.id.as_str(), case))
        .collect::<BTreeMap<_, _>>();
    assert!(cases_by_id.contains_key("write_insert_basic"));
    assert!(cases_by_id.contains_key("read_select_by_pk"));
    assert!(cases_by_id.contains_key("metadata_public_base_tables"));
    assert!(cases_by_id.contains_key("outside_subset_relation"));

    assert_eq!(
        cases_by_id["write_insert_basic"]
            .twin_expected
            .outcome_class,
        "success"
    );
    assert_eq!(
        cases_by_id["write_insert_basic"]
            .twin_expected
            .command_tag
            .as_deref(),
        Some("INSERT 0 1")
    );
    assert_eq!(
        cases_by_id["read_select_by_pk"]
            .twin_expected
            .columns
            .as_deref(),
        Some([String::from("deal_id"), String::from("deal_name")].as_slice())
    );
    assert_eq!(
        cases_by_id["metadata_public_base_tables"]
            .twin_expected
            .rows
            .as_ref()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("table_name")),
        Some(&Value::String(String::from("deals")))
    );
    assert_eq!(
        cases_by_id["outside_subset_relation"]
            .twin_expected
            .sqlstate
            .as_deref(),
        Some("42P01")
    );
}

#[test]
fn run_once_parity_ledger_records_success_and_protocol_refusal_entries() {
    let fixture = load_fixture();
    let workspace = tempdir().expect("create parity workspace");
    let client_path = write_parity_client(workspace.path());
    let reference_path = write_reference_observations(workspace.path(), &fixture);
    let ledger_path = workspace.path().join("parity-ledger.json");
    let port = reserve_local_port();
    let run_command = format!(
        "python3 {} 127.0.0.1 {} {} {} {}",
        shell_quote(&client_path),
        port,
        shell_quote(&fixture_path()),
        shell_quote(&reference_path),
        shell_quote(&ledger_path),
    );

    let output = Command::new(twinning_bin())
        .arg("postgres")
        .arg("--schema")
        .arg(schema_path())
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--run")
        .arg(run_command)
        .arg("--json")
        .output()
        .expect("run twinning parity ledger harness");

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    assert!(
        output.status.success(),
        "parity ledger run_once should exit cleanly: stdout={stdout}; stderr={stderr}"
    );
    assert!(
        stderr.is_empty(),
        "parity ledger run_once should not write stderr: {stderr}"
    );

    let report: Value = serde_json::from_str(&stdout).expect("parse run report");
    assert_eq!(report["version"], "twinning.v0");
    assert_eq!(report["mode"], "run_once");
    assert_eq!(report["run"]["exit_code"], 0, "child failed: {stdout}");
    assert_eq!(report["outcome"], "READY");

    let ledger: ParityLedger =
        serde_json::from_str(&fs::read_to_string(&ledger_path).expect("read parity ledger"))
            .expect("parse parity ledger");
    assert_eq!(ledger.version, LEDGER_VERSION);
    let expected_mode = if env::var_os(DIFFERENTIAL_POSTGRES_URL_ENV).is_some() {
        "live_postgres"
    } else {
        "pinned_fixture"
    };
    assert_eq!(ledger.reference.mode, expected_mode);
    if expected_mode == "live_postgres" {
        assert_eq!(
            ledger.reference.reset_scope.as_deref(),
            Some("public_schema")
        );
        assert!(
            ledger.reference.source_identity.starts_with("sha256:"),
            "live reference source identity should be hashed"
        );
    } else {
        assert_eq!(
            ledger.reference.source_identity,
            "tests/fixtures/differential/parity_ledger/cases.json"
        );
    }
    assert_eq!(ledger.entries.len(), 4);
    assert!(
        ledger.entries.iter().all(|entry| entry.verdict == "pass"),
        "all parity ledger entries should pass: {ledger:#?}"
    );

    let entries_by_id = ledger
        .entries
        .iter()
        .map(|entry| (entry.query_id.as_str(), entry))
        .collect::<BTreeMap<_, _>>();

    let write = entries_by_id["write_insert_basic"];
    assert_eq!(
        write.client_surface,
        "extended_query_parse_bind_execute_sync"
    );
    assert_eq!(write.expected.command_tag.as_deref(), Some("INSERT 0 1"));
    assert_eq!(write.twin_observed.rows_affected, Some(1));
    assert_eq!(
        write.postgres_observed.command_tag.as_deref(),
        Some("INSERT 0 1")
    );

    let read = entries_by_id["read_select_by_pk"];
    assert_eq!(
        read.twin_observed.columns.as_deref(),
        Some([String::from("deal_id"), String::from("deal_name")].as_slice())
    );
    assert_eq!(
        read.twin_observed
            .rows
            .as_ref()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("deal_name")),
        Some(&Value::String(String::from("Alpha")))
    );

    let metadata = entries_by_id["metadata_public_base_tables"];
    assert_eq!(
        metadata.twin_observed.columns.as_deref(),
        Some([String::from("table_name")].as_slice())
    );
    assert_eq!(
        metadata
            .twin_observed
            .rows
            .as_ref()
            .and_then(|rows| rows.get(1))
            .and_then(|row| row.get("table_name")),
        Some(&Value::String(String::from("tenants")))
    );
    assert_eq!(metadata.postgres_observed.rows, metadata.twin_observed.rows);

    let refusal = entries_by_id["outside_subset_relation"];
    assert_eq!(refusal.twin_observed.sqlstate.as_deref(), Some("42P01"));
    assert_eq!(
        refusal.twin_observed.code.as_deref(),
        Some("undefined_table")
    );
    let detail = refusal
        .twin_observed
        .detail
        .as_deref()
        .expect("refusal detail");
    assert!(detail.contains("shape=relation_outside_declared_subset"));
    assert!(detail.contains("declared_tables=public.deals, public.tenants"));
    assert_eq!(refusal.postgres_observed.sqlstate.as_deref(), Some("42P01"));
}

#[test]
fn live_reference_sql_rendering_quotes_params_without_reordering_placeholders() {
    let sql = render_param_sql(
        "INSERT INTO public.example (a, b, c) VALUES ($1, $2, $10)",
        &[
            String::from("alpha"),
            String::from("O'Reilly"),
            String::from("three"),
            String::from("four"),
            String::from("five"),
            String::from("six"),
            String::from("seven"),
            String::from("eight"),
            String::from("nine"),
            String::from("ten"),
        ],
    );

    assert_eq!(
        sql,
        "INSERT INTO public.example (a, b, c) VALUES ('alpha', 'O''Reilly', 'ten')"
    );
}

#[test]
fn live_reference_csv_parser_preserves_headers_and_quoted_values() {
    let records =
        parse_csv_records("deal_id,deal_name\r\ndeal-001,\"Alpha, Inc.\"\r\n").expect("parse csv");

    assert_eq!(
        records,
        vec![
            vec![String::from("deal_id"), String::from("deal_name")],
            vec![String::from("deal-001"), String::from("Alpha, Inc.")],
        ]
    );
}

#[test]
fn live_reference_sqlstate_parser_prefers_actual_sqlstate_over_error_prefix() {
    let stderr = "\
ERROR:  42P01: relation \"public.audit_log\" does not exist
LINE 1: SELECT id FROM public.audit_log WHERE id = 'audit-001'
                       ^
LOCATION:  parserOpenTable, parse_relation.c:1428
";

    assert_eq!(extract_sqlstate(stderr).as_deref(), Ok("42P01"));
}

#[derive(Debug, Deserialize)]
struct ParityFixture {
    version: String,
    requirement_matrix: Vec<Requirement>,
    cases: Vec<ParityCase>,
}

#[derive(Debug, Deserialize)]
struct Requirement {
    id: String,
    completion_criterion: String,
    coverage: String,
    cases: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ParityCase {
    id: String,
    operation: String,
    sql: String,
    #[serde(default)]
    params: Vec<String>,
    #[serde(default)]
    setup: Vec<CaseStatement>,
    postgres_observed: Observation,
    twin_expected: Observation,
}

#[derive(Debug, Deserialize)]
struct CaseStatement {
    sql: String,
    #[serde(default)]
    params: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ParityLedger {
    version: String,
    reference: LedgerReference,
    entries: Vec<ParityLedgerEntry>,
}

#[derive(Debug, Deserialize)]
struct LedgerReference {
    mode: String,
    source_identity: String,
    reset_scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ParityLedgerEntry {
    query_id: String,
    client_surface: String,
    expected: Observation,
    postgres_observed: Observation,
    twin_observed: Observation,
    verdict: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Observation {
    outcome_class: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows_affected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<Vec<BTreeMap<String, Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sqlstate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    detail_contains: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ReferenceObservations {
    mode: String,
    source_identity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reset_scope: Option<String>,
    observations: BTreeMap<String, Observation>,
}

fn load_fixture() -> ParityFixture {
    serde_json::from_str(&fs::read_to_string(fixture_path()).expect("read parity fixture"))
        .expect("parse parity fixture")
}

fn fixture_path() -> PathBuf {
    repo_root().join(FIXTURE_ROOT).join("cases.json")
}

fn schema_path() -> PathBuf {
    repo_root().join(FIXTURE_ROOT).join("schema.sql")
}

fn write_parity_client(dir: &Path) -> PathBuf {
    let client_path = dir.join("parity_client.py");
    fs::write(&client_path, PARITY_CLIENT).expect("write parity client");
    client_path
}

fn write_reference_observations(dir: &Path, fixture: &ParityFixture) -> PathBuf {
    let reference = if let Some(url) = env::var_os(DIFFERENTIAL_POSTGRES_URL_ENV) {
        let url = url
            .into_string()
            .expect("TWINNING_DIFF_POSTGRES_URL should be valid UTF-8");
        live_postgres_reference_observations(&url, fixture)
            .expect("live Postgres reference observations")
    } else {
        pinned_reference_observations(fixture)
    };
    let reference_path = dir.join("reference-observations.json");
    fs::write(
        &reference_path,
        serde_json::to_string_pretty(&reference).expect("serialize reference observations") + "\n",
    )
    .expect("write reference observations");
    reference_path
}

fn pinned_reference_observations(fixture: &ParityFixture) -> ReferenceObservations {
    ReferenceObservations {
        mode: String::from("pinned_fixture"),
        source_identity: String::from(FIXTURE_ROOT) + "/cases.json",
        reset_scope: None,
        observations: fixture
            .cases
            .iter()
            .map(|case| (case.id.clone(), case.postgres_observed.clone()))
            .collect(),
    }
}

fn live_postgres_reference_observations(
    source_url: &str,
    fixture: &ParityFixture,
) -> Result<ReferenceObservations, String> {
    reset_live_postgres_reference(source_url)?;
    let mut observations = BTreeMap::new();
    for case in &fixture.cases {
        for setup in &case.setup {
            run_live_postgres_statement(source_url, setup, "setup")?;
        }
        let observation = run_live_postgres_case(source_url, case)
            .map_err(|error| format!("live Postgres case `{}` failed: {error}", case.id))?;
        observations.insert(case.id.clone(), observation);
    }
    Ok(ReferenceObservations {
        mode: String::from("live_postgres"),
        source_identity: format!("sha256:{}", sha256_hex(source_url.as_bytes())),
        reset_scope: Some(String::from("public_schema")),
        observations,
    })
}

fn reset_live_postgres_reference(source_url: &str) -> Result<(), String> {
    let reset = run_psql(
        source_url,
        &[
            "--set=ON_ERROR_STOP=1",
            "--command=DROP SCHEMA IF EXISTS public CASCADE",
            "--command=CREATE SCHEMA public",
            &format!("--file={}", schema_path().display()),
        ],
    )?;
    assert_psql_success(reset, "reset live Postgres reference").map(|_| ())
}

fn run_live_postgres_statement(
    source_url: &str,
    statement: &CaseStatement,
    context: &str,
) -> Result<(), String> {
    let sql = render_param_sql(&statement.sql, &statement.params);
    let output = run_psql(
        source_url,
        &[
            "--set=ON_ERROR_STOP=1",
            "--command=BEGIN",
            &format!("--command={sql}"),
            "--command=COMMIT",
        ],
    )?;
    assert_psql_success(output, context).map(|_| ())
}

fn run_live_postgres_case(source_url: &str, case: &ParityCase) -> Result<Observation, String> {
    let sql = render_param_sql(&case.sql, &case.params);
    if case.twin_expected.outcome_class == "refusal" {
        return run_live_postgres_refusal(source_url, &sql);
    }
    match case.operation.as_str() {
        "write" => run_live_postgres_write(source_url, &sql),
        "read" => run_live_postgres_read(source_url, &sql),
        other => Err(format!("unsupported parity operation `{other}`")),
    }
}

fn run_live_postgres_write(source_url: &str, sql: &str) -> Result<Observation, String> {
    let output = run_psql(
        source_url,
        &[
            "--set=ON_ERROR_STOP=1",
            "--command=BEGIN",
            &format!("--command={sql}"),
            "--command=COMMIT",
        ],
    )?;
    let stdout = assert_psql_success(output, "live Postgres write")?;
    let command_tag = stdout
        .lines()
        .map(str::trim)
        .find(|line| {
            line.starts_with("INSERT ")
                || line.starts_with("UPDATE ")
                || line.starts_with("DELETE ")
        })
        .ok_or_else(|| format!("live Postgres write did not emit a DML command tag: {stdout}"))?
        .to_owned();
    let rows_affected = decode_rows_affected(&command_tag)?;
    Ok(Observation {
        outcome_class: String::from("success"),
        rows_affected: Some(rows_affected),
        command_tag: Some(command_tag),
        columns: None,
        rows: None,
        sqlstate: None,
        code: None,
        message: None,
        detail: None,
        detail_contains: Vec::new(),
    })
}

fn run_live_postgres_read(source_url: &str, sql: &str) -> Result<Observation, String> {
    let copy_sql = format!("COPY ({sql}) TO STDOUT WITH (FORMAT CSV, HEADER)");
    let output = run_psql(
        source_url,
        &["--set=ON_ERROR_STOP=1", &format!("--command={copy_sql}")],
    )?;
    let stdout = assert_psql_success(output, "live Postgres read")?;
    let records = parse_csv_records(&stdout)?;
    let (columns, data_rows) = records
        .split_first()
        .ok_or_else(|| String::from("live Postgres read returned no CSV header"))?;
    let rows = data_rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .cloned()
                .zip(row.iter().cloned().map(Value::String))
                .collect::<BTreeMap<_, _>>()
        })
        .collect::<Vec<_>>();
    Ok(Observation {
        outcome_class: String::from("success"),
        command_tag: None,
        rows_affected: None,
        columns: Some(columns.clone()),
        rows: Some(rows),
        sqlstate: None,
        code: None,
        message: None,
        detail: None,
        detail_contains: Vec::new(),
    })
}

fn run_live_postgres_refusal(source_url: &str, sql: &str) -> Result<Observation, String> {
    let output = run_psql(
        source_url,
        &[
            "--set=ON_ERROR_STOP=1",
            "--set=VERBOSITY=verbose",
            &format!("--command={sql}"),
        ],
    )?;
    if output.status.success() {
        return Err(String::from(
            "live Postgres refusal case unexpectedly succeeded",
        ));
    }
    let stderr = String::from_utf8(output.stderr)
        .map_err(|error| format!("psql stderr was not UTF-8: {error}"))?;
    Ok(Observation {
        outcome_class: String::from("refusal"),
        command_tag: None,
        rows_affected: None,
        columns: None,
        rows: None,
        sqlstate: Some(extract_sqlstate(&stderr)?),
        code: None,
        message: Some(first_non_empty_line(&stderr)),
        detail: Some(stderr),
        detail_contains: Vec::new(),
    })
}

fn run_psql(source_url: &str, args: &[&str]) -> Result<Output, String> {
    Command::new("psql")
        .arg("-X")
        .arg("--dbname")
        .arg(source_url)
        .args(args)
        .output()
        .map_err(|error| format!("failed to run psql for live Postgres reference: {error}"))
}

fn assert_psql_success(output: Output, context: &str) -> Result<String, String> {
    let stdout = String::from_utf8(output.stdout)
        .map_err(|error| format!("psql stdout was not UTF-8: {error}"))?;
    if output.status.success() {
        return Ok(stdout);
    }
    let stderr = String::from_utf8(output.stderr)
        .map_err(|error| format!("psql stderr was not UTF-8: {error}"))?;
    Err(format!(
        "{context} failed: stdout={stdout}; stderr={stderr}"
    ))
}

fn render_param_sql(sql: &str, params: &[String]) -> String {
    let mut rendered = sql.to_owned();
    for index in (1..=params.len()).rev() {
        rendered = rendered.replace(
            &format!("${index}"),
            &postgres_text_literal(&params[index - 1]),
        );
    }
    rendered
}

fn postgres_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn decode_rows_affected(command_tag: &str) -> Result<u64, String> {
    let parts = command_tag.split_whitespace().collect::<Vec<_>>();
    if parts.len() == 3 && parts[0] == "INSERT" {
        return parts[2]
            .parse()
            .map_err(|error| format!("parse INSERT rows affected: {error}"));
    }
    if parts.len() == 2 && matches!(parts[0], "UPDATE" | "DELETE") {
        return parts[1]
            .parse()
            .map_err(|error| format!("parse DML rows affected: {error}"));
    }
    Ok(0)
}

fn parse_csv_records(input: &str) -> Result<Vec<Vec<String>>, String> {
    let mut records = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut chars = input.chars().peekable();
    let mut in_quotes = false;
    let mut saw_anything = false;

    while let Some(ch) = chars.next() {
        saw_anything = true;
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                row.push(std::mem::take(&mut field));
            }
            '\n' if !in_quotes => {
                row.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut row));
            }
            '\r' if !in_quotes => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                row.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut row));
            }
            _ => field.push(ch),
        }
    }

    if in_quotes {
        return Err(String::from("unterminated quoted CSV field"));
    }
    if saw_anything && (!field.is_empty() || !row.is_empty()) {
        row.push(field);
        records.push(row);
    }
    Ok(records)
}

fn extract_sqlstate(stderr: &str) -> Result<String, String> {
    let mut fallback = None;
    for token in stderr.split(|ch: char| !ch.is_ascii_alphanumeric()) {
        if token.len() == 5
            && token
                .chars()
                .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
        {
            if token.chars().any(|ch| ch.is_ascii_digit()) {
                return Ok(token.to_owned());
            }
            fallback = Some(token.to_owned());
        }
    }
    if let Some(sqlstate) = fallback {
        return Ok(sqlstate);
    }
    Err(format!(
        "could not extract SQLSTATE from psql stderr: {stderr}"
    ))
}

fn first_non_empty_line(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("")
        .to_owned()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn reserve_local_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind local port");
    let port = listener.local_addr().expect("listener addr").port();
    drop(listener);
    port
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

const PARITY_CLIENT: &str = r#"
import json
import socket
import struct
import sys

TEXT_OID = 25

host = sys.argv[1]
port = int(sys.argv[2])
fixture_path = sys.argv[3]
reference_path = sys.argv[4]
ledger_path = sys.argv[5]
fixture = json.loads(open(fixture_path, encoding="utf-8").read())
reference = json.loads(open(reference_path, encoding="utf-8").read())
reference_observations = reference["observations"]

sock = socket.create_connection((host, port), timeout=2)
sock.settimeout(2)


def fail(message):
    raise SystemExit(message)


def framed(tag, body=b""):
    sock.sendall(tag + struct.pack("!I", len(body) + 4) + body)


def read_exact(size):
    body = b""
    while len(body) < size:
        chunk = sock.recv(size - len(body))
        if not chunk:
            fail("connection closed while reading backend frame")
        body += chunk
    return body


def read_frame():
    header = read_exact(5)
    tag = header[:1]
    length = struct.unpack("!I", header[1:])[0]
    return tag, read_exact(length - 4)


def read_until_ready():
    frames = []
    while True:
        frame = read_frame()
        frames.append(frame)
        if frame[0] == b"Z":
            return frames


def expect_ready(frames, status):
    tag, body = frames[-1]
    if tag != b"Z" or body != status:
        fail(f"expected ReadyForQuery {status!r}, got tag={tag!r} body={body!r}")


def expect_command(sql, ready_status):
    framed(b"Q", sql.encode("utf-8") + b"\x00")
    frames = read_until_ready()
    if frames[0][0] != b"C":
        fail(f"{sql} expected CommandComplete, got {frames[0][0]!r}")
    expect_ready(frames, ready_status)


def startup():
    body = struct.pack("!I", 196608)
    for name, value in ((b"user", b"postgres"), (b"database", b"postgres"), (b"application_name", b"parity-ledger")):
        body += name + b"\x00" + value + b"\x00"
    body += b"\x00"
    sock.sendall(struct.pack("!I", len(body) + 4) + body)
    frames = read_until_ready()
    expect_ready(frames, b"I")


def decode_command_complete(frame):
    tag, body = frame
    if tag != b"C":
        fail(f"expected CommandComplete, got {tag!r}")
    return str(body[:-1], encoding="utf-8")


def decode_rows_affected(command_tag):
    parts = command_tag.split()
    if len(parts) == 3 and parts[0] == "INSERT":
        return int(parts[2])
    if len(parts) == 2 and parts[0] in ("UPDATE", "DELETE"):
        return int(parts[1])
    return 0


def decode_row_description(frame):
    tag, body = frame
    if tag != b"T":
        fail(f"expected RowDescription, got {tag!r}")
    field_count = struct.unpack("!h", body[:2])[0]
    offset = 2
    columns = []
    for _ in range(field_count):
        end = body.index(b"\x00", offset)
        columns.append(str(body[offset:end], encoding="utf-8"))
        offset = end + 1 + 18
    return columns


def decode_data_row(frame):
    tag, body = frame
    if tag != b"D":
        fail(f"expected DataRow, got {tag!r}")
    column_count = struct.unpack("!h", body[:2])[0]
    offset = 2
    values = []
    for _ in range(column_count):
        length = struct.unpack("!i", body[offset:offset + 4])[0]
        offset += 4
        if length == -1:
            values.append(None)
            continue
        raw = body[offset:offset + length]
        offset += length
        values.append(str(raw, encoding="utf-8"))
    if offset != len(body):
        fail(f"DataRow had {len(body) - offset} trailing bytes")
    return values


def decode_error(frame):
    tag, body = frame
    if tag != b"E":
        fail(f"expected ErrorResponse, got {tag!r}")
    fields = {}
    offset = 0
    while offset < len(body):
        field_type = body[offset:offset + 1]
        offset += 1
        if field_type == b"\x00":
            break
        end = body.index(b"\x00", offset)
        fields[str(field_type, encoding="ascii")] = str(body[offset:end], encoding="utf-8")
        offset = end + 1
    return {
        "outcome_class": "refusal",
        "sqlstate": fields.get("C"),
        "code": fields.get("V"),
        "message": fields.get("M"),
        "detail": fields.get("D", ""),
    }


def execute_extended(name, sql, params):
    body = name.encode("utf-8") + b"\x00" + sql.encode("utf-8") + b"\x00"
    body += struct.pack("!h", len(params))
    for _ in params:
        body += struct.pack("!I", TEXT_OID)
    framed(b"P", body)

    portal = f"{name}_portal"
    body = portal.encode("utf-8") + b"\x00" + name.encode("utf-8") + b"\x00"
    body += struct.pack("!h", 0)
    body += struct.pack("!h", len(params))
    for value in params:
        if value is None:
            body += struct.pack("!i", -1)
            continue
        encoded = str(value).encode("utf-8")
        body += struct.pack("!i", len(encoded)) + encoded
    body += struct.pack("!h", 0)
    framed(b"B", body)
    framed(b"E", portal.encode("utf-8") + b"\x00" + struct.pack("!i", 0))
    framed(b"S")

    frames = read_until_ready()
    index = 0
    if frames[index][0] == b"1":
        index += 1
    if frames[index][0] == b"2":
        index += 1
    if frames[index][0] == b"E":
        expect_ready(frames, b"E")
        return decode_error(frames[index])
    if frames[index][0] == b"T":
        columns = decode_row_description(frames[index])
        index += 1
        rows = []
        while frames[index][0] == b"D":
            rows.append(dict(zip(columns, decode_data_row(frames[index]))))
            index += 1
        command_tag = decode_command_complete(frames[index])
        expect_ready(frames, b"I")
        return {
            "outcome_class": "success",
            "columns": columns,
            "rows": rows,
            "command_tag": command_tag,
        }
    command_tag = decode_command_complete(frames[index])
    return {
        "outcome_class": "success",
        "command_tag": command_tag,
        "rows_affected": decode_rows_affected(command_tag),
    }


def expected_matches(expected, observed):
    if expected.get("outcome_class") != observed.get("outcome_class"):
        return False
    if expected.get("outcome_class") == "success":
        for key in ("command_tag", "rows_affected", "columns", "rows"):
            if key in expected and expected.get(key) != observed.get(key):
                return False
        return True
    if expected.get("outcome_class") == "refusal":
        for key in ("sqlstate", "code"):
            if key in expected and expected.get(key) != observed.get(key):
                return False
        detail = observed.get("detail", "")
        for fragment in expected.get("detail_contains", []):
            if fragment not in detail:
                return False
        return True
    return False


def reference_matches(reference_observed, twin_observed):
    if reference_observed.get("outcome_class") != twin_observed.get("outcome_class"):
        return False
    if reference_observed.get("outcome_class") == "success":
        for key in ("command_tag", "rows_affected", "columns", "rows"):
            if key in reference_observed and reference_observed.get(key) != twin_observed.get(key):
                return False
        return True
    if reference_observed.get("outcome_class") == "refusal":
        return reference_observed.get("sqlstate") == twin_observed.get("sqlstate")
    return False


def run_setup(case):
    for index, setup in enumerate(case.get("setup", []), start=1):
        expect_command("BEGIN", b"T")
        observed = execute_extended(f"{case['id']}_setup_{index}", setup["sql"], setup.get("params", []))
        if observed.get("outcome_class") != "success":
            fail(f"setup for {case['id']} refused: {observed!r}")
        expect_command("COMMIT", b"I")


def run_case(case):
    run_setup(case)
    expected = case["twin_expected"]
    if expected["outcome_class"] == "refusal":
        expect_command("BEGIN", b"T")
        observed = execute_extended(case["id"], case["sql"], case.get("params", []))
        expect_command("ROLLBACK", b"I")
    elif case["operation"] == "write":
        expect_command("BEGIN", b"T")
        observed = execute_extended(case["id"], case["sql"], case.get("params", []))
        expect_command("COMMIT", b"I")
    else:
        observed = execute_extended(case["id"], case["sql"], case.get("params", []))
    return observed


startup()
entries = []
for case in fixture["cases"]:
    observed = run_case(case)
    reference_observed = reference_observations[case["id"]]
    entries.append({
        "query_id": case["id"],
        "client_surface": case["client_surface"],
        "sql": case["sql"],
        "expected": case["twin_expected"],
        "postgres_observed": reference_observed,
        "twin_observed": observed,
        "verdict": "pass" if expected_matches(case["twin_expected"], observed) and reference_matches(reference_observed, observed) else "fail",
    })

framed(b"X")
sock.close()

with open(ledger_path, "w", encoding="utf-8") as ledger_file:
    json.dump({
        "version": "twinning.parity-ledger.v0",
        "reference": {
            "mode": reference["mode"],
            "source_identity": reference["source_identity"],
            "reset_scope": reference.get("reset_scope"),
        },
        "entries": entries,
    }, ledger_file, indent=2, sort_keys=True)
    ledger_file.write("\n")
"#;
