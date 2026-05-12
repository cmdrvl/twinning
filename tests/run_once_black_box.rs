#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::{Value, json};
use tempfile::tempdir;

#[path = "support.rs"]
mod support;

const EXTENDED_QUERY_SHAPE: &str = "extended_query_parse_bind_execute_sync";

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

#[test]
fn manifest_backed_extended_canaries_run_black_box_through_run_once() {
    let manifest = support::manifest();
    let cases = manifest
        .canaries
        .iter()
        .filter(|canary| {
            canary
                .session_shapes
                .iter()
                .any(|shape| shape == EXTENDED_QUERY_SHAPE)
        })
        .collect::<Vec<_>>();

    assert!(
        !cases.is_empty(),
        "manifest should declare at least one extended-query run_once canary"
    );

    for canary in cases {
        assert_supported_black_box_shapes(canary);

        let dir = tempdir().expect("tempdir");
        let schema_path = write_black_box_schema(dir.path());
        let child_path = write_black_box_child(dir.path());
        let case_path = write_case_file(dir.path(), canary);
        let diagnostics_path = diagnostics_path(dir.path(), canary);
        let report_path = dir.path().join(format!("{}.run.json", canary.id));
        let snapshot_path = dir.path().join(format!("{}.run.twin", canary.id));
        let port = reserve_local_port();

        let run_command = format!(
            "python3 {} 127.0.0.1 {} {}",
            shell_quote(&child_path),
            port,
            shell_quote(&case_path),
        );
        let output = Command::new(twinning_bin())
            .arg("postgres")
            .arg("--schema")
            .arg(&schema_path)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--run")
            .arg(run_command)
            .arg("--report")
            .arg(&report_path)
            .arg("--snapshot")
            .arg(&snapshot_path)
            .arg("--json")
            .output()
            .unwrap_or_else(|error| panic!("run twinning for `{}`: {error}", canary.id));

        let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
        let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
        let diagnostics = fs::read_to_string(&diagnostics_path).unwrap_or_default();
        assert!(
            output.status.success(),
            "black-box run_once canary `{}` should exit cleanly: stdout={stdout}; stderr={stderr}; diagnostics={diagnostics}",
            canary.id
        );
        assert!(
            stderr.is_empty(),
            "black-box run_once canary `{}` should not write stderr: {stderr}",
            canary.id
        );

        let report: Value = serde_json::from_str(&stdout).expect("parse run report");
        let expected_rows = expected_row_count(canary);
        assert_eq!(report["version"], "twinning.v0");
        assert_eq!(report["mode"], "run_once");
        assert_eq!(
            report["run"]["exit_code"], 0,
            "black-box client `{}` failed: report={}; diagnostics={diagnostics}",
            canary.id, stdout
        );
        assert_eq!(
            report["outcome"], "READY",
            "black-box run_once canary `{}` should be ready: report={}; diagnostics={diagnostics}",
            canary.id, stdout
        );
        assert_eq!(report["tables"]["public.deals"]["rows"], expected_rows);

        let written_report: Value =
            serde_json::from_str(&fs::read_to_string(&report_path).expect("read report"))
                .expect("parse written report");
        assert_eq!(
            written_report["tables"]["public.deals"]["rows"],
            expected_rows
        );

        let snapshot: Value =
            serde_json::from_str(&fs::read_to_string(&snapshot_path).expect("read snapshot"))
                .expect("parse snapshot");
        assert_eq!(snapshot["table_rows"]["public.deals"], expected_rows);
        let rows = snapshot["relations"]["public.deals"]
            .as_array()
            .unwrap_or_else(|| panic!("snapshot should include public.deals for `{}`", canary.id));
        assert_eq!(rows.len() as u64, expected_rows);
        assert_snapshot_rows_match_manifest_case(canary, rows);
    }
}

fn assert_supported_black_box_shapes(canary: &support::CanaryDefinition) {
    let supported_session_shapes = BTreeSet::from([
        "startup_auth_v3",
        "parameter_status_baseline",
        "set_application_name",
        "tx_begin",
        "tx_commit",
        "tx_rollback",
        "extended_query_parse_bind_execute_sync",
    ]);
    let supported_write_shapes = BTreeSet::from([
        "insert_values",
        "insert_returning",
        "upsert_pk",
        "upsert_unique",
    ]);
    let supported_read_shapes =
        BTreeSet::from(["select_by_pk", "select_filtered_scan", "select_is_null"]);
    let supported_required_sqlstates =
        BTreeSet::from(["23502", "23503", "23505", "23514", "22P02"]);
    let supported_unsupported_policies = BTreeSet::from(["refusal"]);

    for shape in &canary.session_shapes {
        assert!(
            supported_session_shapes.contains(shape.as_str()),
            "black-box run_once canary `{}` has no client mapping for manifest session shape `{shape}`",
            canary.id
        );
    }
    for shape in &canary.write_shapes {
        assert!(
            supported_write_shapes.contains(shape.as_str()),
            "black-box run_once canary `{}` has no client mapping for manifest write shape `{shape}`",
            canary.id
        );
    }
    for shape in &canary.read_shapes {
        assert!(
            supported_read_shapes.contains(shape.as_str()),
            "black-box run_once canary `{}` has no client mapping for manifest read shape `{shape}`",
            canary.id
        );
    }
    for sqlstate in &canary.required_sqlstates {
        assert!(
            supported_required_sqlstates.contains(sqlstate.as_str()),
            "black-box run_once canary `{}` has no client probe for manifest SQLSTATE `{sqlstate}`",
            canary.id
        );
    }
    assert!(
        supported_unsupported_policies.contains(canary.unsupported_policy.as_str()),
        "black-box run_once canary `{}` has no client mapping for unsupported policy `{}`",
        canary.id,
        canary.unsupported_policy
    );
}

fn expected_row_count(canary: &support::CanaryDefinition) -> u64 {
    expected_final_rows(canary).len() as u64
}

fn expected_final_rows(canary: &support::CanaryDefinition) -> BTreeMap<String, String> {
    let mut rows = BTreeMap::new();
    if canary
        .write_shapes
        .iter()
        .any(|shape| shape == "insert_values")
    {
        rows.insert(
            format!("{}-insert-values", canary.id),
            String::from("Inserted"),
        );
    }
    if canary
        .write_shapes
        .iter()
        .any(|shape| shape == "insert_returning")
    {
        rows.insert(
            format!("{}-insert-returning", canary.id),
            String::from("Returning"),
        );
    }
    if canary.write_shapes.iter().any(|shape| shape == "upsert_pk") {
        assert!(
            canary
                .write_shapes
                .iter()
                .any(|shape| shape == "insert_values"),
            "black-box run_once canary `{}` needs insert_values before upsert_pk so the client exercises the conflict-update path",
            canary.id
        );
        rows.insert(
            format!("{}-insert-values", canary.id),
            String::from("Upsert PK"),
        );
    }
    if canary
        .write_shapes
        .iter()
        .any(|shape| shape == "upsert_unique")
    {
        assert!(
            canary
                .write_shapes
                .iter()
                .any(|shape| shape == "insert_values"),
            "black-box run_once canary `{}` needs insert_values before upsert_unique so the client exercises the conflict-update path",
            canary.id
        );
        rows.insert(
            format!("{}-insert-values", canary.id),
            String::from("Upsert Unique"),
        );
    }

    rows
}

fn assert_snapshot_rows_match_manifest_case(
    canary: &support::CanaryDefinition,
    rows: &[serde_json::Value],
) {
    let actual_rows = rows
        .iter()
        .map(|row| {
            let deal_id = row["deal_id"]["value"]
                .as_str()
                .unwrap_or_else(|| {
                    panic!("snapshot row for `{}` should include deal_id", canary.id)
                })
                .to_owned();
            let deal_name = row["deal_name"]["value"]
                .as_str()
                .unwrap_or_else(|| {
                    panic!(
                        "snapshot row `{deal_id}` for `{}` should include deal_name",
                        canary.id
                    )
                })
                .to_owned();
            (deal_id, deal_name)
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        actual_rows,
        expected_final_rows(canary),
        "snapshot rows for `{}` should match the manifest-driven mutation shapes",
        canary.id
    );
}

fn write_case_file(dir: &Path, canary: &support::CanaryDefinition) -> PathBuf {
    let case_path = dir.join(format!("{}.case.json", canary.id));
    let case = json!({
        "id": canary.id,
        "client": canary.client,
        "diagnostics_path": diagnostics_path(dir, canary),
        "session_shapes": canary.session_shapes,
        "write_shapes": canary.write_shapes,
        "read_shapes": canary.read_shapes,
        "required_sqlstates": canary.required_sqlstates,
        "unsupported_policy": canary.unsupported_policy,
    });
    fs::write(
        &case_path,
        serde_json::to_string_pretty(&case).expect("render case json"),
    )
    .expect("write case json");
    case_path
}

fn diagnostics_path(dir: &Path, canary: &support::CanaryDefinition) -> PathBuf {
    dir.join(format!("{}.client.err", canary.id))
}

fn write_black_box_schema(dir: &Path) -> PathBuf {
    let schema_path = dir.join("schema.sql");
    fs::write(
        &schema_path,
        r#"
        CREATE TABLE public.tenants (
            tenant_id TEXT PRIMARY KEY
        );

        CREATE TABLE public.deals (
            tenant_id TEXT REFERENCES public.tenants (tenant_id),
            deal_id TEXT PRIMARY KEY,
            external_key TEXT UNIQUE,
            deal_name TEXT NOT NULL,
            status TEXT CHECK (status IN ('open', 'closed')),
            amount INTEGER CHECK (amount >= 0)
        );
        "#,
    )
    .expect("write schema");
    schema_path
}

fn write_black_box_child(dir: &Path) -> PathBuf {
    let child_path = dir.join("black_box_client.py");
    fs::write(&child_path, BLACK_BOX_CHILD).expect("write child");
    child_path
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

const BLACK_BOX_CHILD: &str = r#"
import json
import socket
import struct
import sys

TEXT_OID = 25


host = sys.argv[1]
port = int(sys.argv[2])
case_path = sys.argv[3]
case = json.loads(open(case_path, encoding="utf-8").read())


def fail(message):
    diagnostics_path = case.get("diagnostics_path")
    if diagnostics_path:
        with open(diagnostics_path, "w", encoding="utf-8") as diagnostics:
            diagnostics.write(message + "\n")
    raise SystemExit(message)


sock = socket.create_connection((host, port), timeout=2)
sock.settimeout(2)


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


def expect_tag(frame, expected):
    tag, body = frame
    if tag != expected:
        fail(f"expected backend frame {expected!r}, got {tag!r} body={body!r}")
    return body


def decode_error_sqlstate(frame):
    tag, body = frame
    if tag != b"E":
        fail(f"expected ErrorResponse, got {tag!r}")
    offset = 0
    while offset < len(body):
        field_type = body[offset:offset + 1]
        offset += 1
        if field_type == b"\x00":
            break
        end = body.index(b"\x00", offset)
        value = body[offset:end].decode("utf-8")
        offset = end + 1
        if field_type == b"C":
            return value
    fail("ErrorResponse did not include SQLSTATE")


def decode_data_row(frame):
    body = expect_tag(frame, b"D")
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
        values.append(raw.decode("utf-8"))
    if offset != len(body):
        fail(f"DataRow had {len(body) - offset} trailing bytes")
    return values


def startup():
    body = struct.pack("!I", 196608)
    for name, value in ((b"user", b"postgres"), (b"database", b"postgres"), (b"application_name", case["client"].encode("utf-8"))):
        body += name + b"\x00" + value + b"\x00"
    body += b"\x00"
    sock.sendall(struct.pack("!I", len(body) + 4) + body)
    frames = read_until_ready()
    expect_ready(frames, b"I")


def query(sql, ready_status):
    framed(b"Q", sql.encode("utf-8") + b"\x00")
    frames = read_until_ready()
    expect_tag(frames[0], b"C")
    expect_ready(frames, ready_status)


def parse_bind_execute(name, sql, params, expect_returning=False, expect_select=False, ready_status=b"T"):
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
        encoded = value.encode("utf-8")
        body += struct.pack("!i", len(encoded)) + encoded
    body += struct.pack("!h", 0)
    framed(b"B", body)
    framed(b"E", portal.encode("utf-8") + b"\x00" + struct.pack("!i", 0))
    framed(b"S")

    frames = read_until_ready()
    expect_tag(frames[0], b"1")
    expect_tag(frames[1], b"2")
    if expect_returning or expect_select:
        expect_tag(frames[2], b"T")
        index = 3
        rows = []
        while index < len(frames) and frames[index][0] == b"D":
            rows.append(decode_data_row(frames[index]))
            index += 1
        expect_tag(frames[index], b"C")
        if expect_returning and len(rows) != 1:
            fail(f"expected one RETURNING row for {name}, got {rows!r}")
        if expect_select and not rows:
            fail(f"expected at least one SELECT row for {name}")
    else:
        expect_tag(frames[2], b"C")
        rows = []
    expect_ready(frames, ready_status)
    return rows


def parse_bind_execute_error(name, sql, params, expected_sqlstate):
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
        encoded = value.encode("utf-8")
        body += struct.pack("!i", len(encoded)) + encoded
    body += struct.pack("!h", 0)
    framed(b"B", body)
    framed(b"E", portal.encode("utf-8") + b"\x00" + struct.pack("!i", 0))
    framed(b"S")

    frames = read_until_ready()
    expect_tag(frames[0], b"1")
    expect_tag(frames[1], b"2")
    actual_sqlstate = decode_error_sqlstate(frames[2])
    if actual_sqlstate != expected_sqlstate:
        fail(f"{name} expected SQLSTATE {expected_sqlstate}, got {actual_sqlstate}")
    expect_ready(frames, b"E")


def execute_write_shape(shape):
    canary_id = case["id"]
    if shape == "insert_values":
        parse_bind_execute(
            "insert_values",
            "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6)",
            [
                None,
                f"{canary_id}-insert-values",
                f"{canary_id}-external-seed",
                "Inserted",
                None if "select_is_null" in case["read_shapes"] else "open",
                "100",
            ],
        )
    elif shape == "insert_returning":
        parse_bind_execute(
            "insert_returning",
            "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6) RETURNING deal_name",
            [None, f"{canary_id}-insert-returning", f"{canary_id}-external-insert-returning", "Returning", "open", "110"],
            expect_returning=True,
        )
    elif shape == "upsert_pk":
        parse_bind_execute(
            "upsert_pk",
            "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (deal_id) DO UPDATE SET deal_name = EXCLUDED.deal_name RETURNING deal_name",
            [None, f"{canary_id}-insert-values", f"{canary_id}-external-seed", "Upsert PK", "open", "120"],
            expect_returning=True,
        )
    elif shape == "upsert_unique":
        parse_bind_execute(
            "upsert_unique",
            "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (external_key) DO UPDATE SET deal_name = EXCLUDED.deal_name RETURNING deal_name",
            [None, f"{canary_id}-upsert-unique", f"{canary_id}-external-seed", "Upsert Unique", "open", "130"],
            expect_returning=True,
        )
    else:
        fail(f"unmapped manifest write shape {shape!r}")


def execute_read_shape(shape):
    canary_id = case["id"]
    if shape == "select_by_pk":
        rows = parse_bind_execute(
            "select_by_pk",
            "SELECT deal_id, deal_name FROM public.deals WHERE deal_id = $1",
            [f"{canary_id}-insert-values"],
            expect_select=True,
            ready_status=b"I",
        )
        if not any(row[0] == f"{canary_id}-insert-values" for row in rows):
            fail(f"select_by_pk did not return the committed insert row: {rows!r}")
    elif shape == "select_filtered_scan":
        expected_name = "Upsert Unique" if "upsert_unique" in case["write_shapes"] else "Upsert PK" if "upsert_pk" in case["write_shapes"] else "Inserted"
        rows = parse_bind_execute(
            "select_filtered_scan",
            "SELECT deal_id, deal_name FROM public.deals WHERE deal_name = $1",
            [expected_name],
            expect_select=True,
            ready_status=b"I",
        )
        if not any(row == [f"{canary_id}-insert-values", expected_name] for row in rows):
            fail(f"select_filtered_scan did not return the committed final row: {rows!r}")
    elif shape == "select_is_null":
        rows = parse_bind_execute(
            "select_is_null",
            "SELECT deal_id FROM public.deals WHERE status IS NULL",
            [],
            expect_select=True,
            ready_status=b"I",
        )
        if not any(row == [f"{canary_id}-insert-values"] for row in rows):
            fail(f"select_is_null did not return the committed NULL-status row: {rows!r}")
    else:
        fail(f"unmapped manifest read shape {shape!r}")


def execute_required_sqlstate(sqlstate):
    canary_id = case["id"]
    if sqlstate == "23502":
        sql = "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6)"
        params = [None, f"{canary_id}-probe-not-null", f"{canary_id}-external-not-null", None, "open", "100"]
    elif sqlstate == "23503":
        sql = "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6)"
        params = ["missing-tenant", f"{canary_id}-probe-fk", f"{canary_id}-external-fk", "FK Probe", "open", "100"]
    elif sqlstate == "23505":
        if "insert_values" not in case["write_shapes"]:
            fail("23505 probe requires insert_values to create the committed unique surface")
        sql = "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6)"
        params = [None, f"{canary_id}-probe-unique", f"{canary_id}-external-seed", "Unique Probe", "open", "100"]
    elif sqlstate == "23514":
        sql = "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6)"
        params = [None, f"{canary_id}-probe-check", f"{canary_id}-external-check", "Check Probe", "archived", "100"]
    elif sqlstate == "22P02":
        sql = "INSERT INTO public.deals (tenant_id, deal_id, external_key, deal_name, status, amount) VALUES ($1, $2, $3, $4, $5, $6)"
        params = [None, f"{canary_id}-probe-bad-int", f"{canary_id}-external-bad-int", "Bad Int Probe", "open", "not-an-int"]
    else:
        fail(f"unmapped manifest SQLSTATE {sqlstate!r}")

    query("BEGIN", b"T")
    parse_bind_execute_error(f"probe_{sqlstate}", sql, params, sqlstate)
    query("ROLLBACK", b"I")


startup()
query("BEGIN", b"T")
for write_shape in case["write_shapes"]:
    execute_write_shape(write_shape)
query("COMMIT", b"I")

for read_shape in case["read_shapes"]:
    execute_read_shape(read_shape)

for sqlstate in case["required_sqlstates"]:
    execute_required_sqlstate(sqlstate)

if case["unsupported_policy"] == "refusal":
    framed(b"P", b"bad_statement\x00BEGIN\x00" + struct.pack("!h", 0))
    framed(b"S")
    frames = read_until_ready()
    if decode_error_sqlstate(frames[0]) != "0A000":
        fail("unsupported prepared BEGIN should be a 0A000 protocol-visible refusal")

framed(b"X")
sock.close()
"#;
