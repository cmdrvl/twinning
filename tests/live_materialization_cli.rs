#![forbid(unsafe_code)]
#![cfg(feature = "postgres")]

use std::{fs, path::Path, process::Command};

use tempfile::tempdir;

#[test]
fn postgres_cli_materializes_declared_source_rows_into_snapshot() {
    let workspace = tempdir().expect("workspace");
    let schema_path = workspace.path().join("schema.sql");
    let report_path = workspace.path().join("out").join("report.json");
    let snapshot_path = workspace.path().join("out").join("materialized.twin");
    let fake_psql = workspace.path().join("fake-psql.py");

    fs::write(
        &schema_path,
        r#"
        CREATE TABLE public.tenants (
            tenant_id TEXT PRIMARY KEY,
            tenant_name TEXT NOT NULL
        );

        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            tenant_id TEXT REFERENCES public.tenants (tenant_id),
            amount INTEGER,
            active BOOLEAN
        );
        "#,
    )
    .expect("write schema");
    write_fake_psql(&fake_psql);

    let output = Command::new(twinning_bin())
        .arg("postgres")
        .arg("--schema")
        .arg(&schema_path)
        .arg("--materialize-source-url")
        .arg("postgres://fixture-source")
        .arg("--report")
        .arg(&report_path)
        .arg("--snapshot")
        .arg(&snapshot_path)
        .arg("--json")
        .env("TWINNING_PSQL_BIN", &fake_psql)
        .output()
        .expect("run twinning materialization");

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
    assert!(
        output.status.success(),
        "materialization should succeed: stdout={stdout}; stderr={stderr}"
    );
    assert!(stderr.is_empty(), "stderr should stay empty: {stderr}");

    let report: serde_json::Value = serde_json::from_str(&stdout).expect("parse report");
    assert_eq!(report["outcome"], "READY");
    assert_eq!(
        report["source_materialization"]["method"],
        "psql_copy_stdout"
    );
    assert_eq!(report["source_materialization"]["table_count"], 2);
    assert_eq!(report["source_materialization"]["row_count"], 4);
    assert_eq!(report["tables"]["public.deals"]["rows"], 2);
    assert_eq!(report["tables"]["public.tenants"]["rows"], 2);
    assert!(
        report["source_materialization"]["source_identity"]
            .as_str()
            .expect("source identity")
            .starts_with("sha256:")
    );
    assert!(
        report["snapshot"]["snapshot_hash"]
            .as_str()
            .expect("snapshot hash")
            .starts_with("sha256:")
    );

    let snapshot: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&snapshot_path).expect("read snapshot"))
            .expect("parse snapshot");
    assert_eq!(snapshot["mode"], "committed_state");
    assert_eq!(
        snapshot["source_materialization"],
        report["source_materialization"]
    );
    assert_eq!(snapshot["table_rows"]["public.deals"], 2);

    let deals = snapshot["relations"]["public.deals"]
        .as_array()
        .expect("deals relation");
    assert_eq!(deals.len(), 2);
    assert_eq!(deals[0]["deal_id"]["value"], "deal-1");
    assert_eq!(deals[0]["amount"]["kind"], "integer");
    assert_eq!(deals[0]["amount"]["value"], 100);
    assert_eq!(deals[0]["active"]["kind"], "boolean");
    assert_eq!(deals[0]["active"]["value"], true);
}

#[test]
fn materialization_refuses_restore_bootstrap_source_mix() {
    let output = Command::new(twinning_bin())
        .arg("postgres")
        .arg("--restore")
        .arg("missing.twin")
        .arg("--materialize-source-url")
        .arg("postgres://fixture-source")
        .arg("--json")
        .output()
        .expect("run twinning refusal");

    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("parse refusal");
    assert_eq!(
        json["refusal"]["code"],
        "E_MATERIALIZATION_BOOTSTRAP_SOURCE"
    );
}

fn write_fake_psql(path: &Path) {
    fs::write(
        path,
        r#"#!/usr/bin/env python3
import sys

args = "\n".join(sys.argv[1:])
if 'FROM "public"."deals"' in args:
    print("deal_id,tenant_id,amount,active")
    print("deal-2,tenant-b,200,f")
    print("deal-1,tenant-a,100,t")
elif 'FROM "public"."tenants"' in args:
    print("tenant_id,tenant_name")
    print("tenant-b,Tenant B")
    print("tenant-a,Tenant A")
else:
    print(f"unexpected psql command: {args}", file=sys.stderr)
    sys.exit(1)
"#,
    )
    .expect("write fake psql");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)
            .expect("fake psql metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod fake psql");
    }
}

fn twinning_bin() -> &'static str {
    env!("CARGO_BIN_EXE_twinning")
}
