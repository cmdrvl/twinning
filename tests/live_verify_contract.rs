#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use serde_json::{Value, json};
use twinning::{
    catalog::parse_postgres_schema,
    report::{
        LiveVerifyArtifact, RatioMap, SchemaReport, SnapshotReport, TwinReport, TwinReportSeed,
        VerifyArtifactReport,
    },
};

fn seeded_catalog() -> twinning::catalog::Catalog {
    parse_postgres_schema(
        r#"
        CREATE TABLE public.tenants (
            tenant_id TEXT PRIMARY KEY,
            tenant_name TEXT NOT NULL
        );

        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            deal_name TEXT NOT NULL,
            CONSTRAINT deals_tenant_fk
                FOREIGN KEY (tenant_id) REFERENCES public.tenants (tenant_id)
        );
        "#,
    )
    .expect("schema should parse")
}

fn seeded_report() -> TwinReport {
    let catalog = seeded_catalog();
    TwinReport::from_seed(TwinReportSeed {
        engine: "postgres",
        host: "127.0.0.1",
        port: 5432,
        schema: SchemaReport {
            source: String::from("schema.sql"),
            hash: String::from("sha256:schema"),
            table_count: catalog.table_count,
            column_count: catalog.column_count,
            index_count: catalog.index_count,
            constraint_count: catalog.constraint_count,
        },
        verify_artifact: Some(VerifyArtifactReport {
            source: String::from("constraints.verify.json"),
            hash: String::from("sha256:artifact"),
            loaded: 2,
        }),
        verify: None,
        catalog: &catalog,
        snapshot: SnapshotReport {
            restored_from: None,
            written_to: None,
            snapshot_hash: None,
        },
        run: None,
        null_rates: None,
        fk_coverage: None,
        warnings: Vec::new(),
    })
}

fn schema_correct_verify_report() -> Value {
    json!({
        "tool": "verify",
        "version": "verify.report.v1",
        "execution_mode": "embedded",
        "outcome": "FAIL",
        "constraint_set_id": "deals-portable",
        "constraint_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bindings": {
            "public.deals": {
                "kind": "relation",
                "source": "public.deals",
                "content_hash": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "input_verification": null
            }
        },
        "summary": {
            "total_rules": 2,
            "passed_rules": 1,
            "failed_rules": 1,
            "by_severity": {
                "error": 1,
                "warn": 0
            }
        },
        "policy_signals": {
            "severity_band": "ERROR_PRESENT"
        },
        "results": [
            {
                "rule_id": "DEAL_EXISTS",
                "severity": "error",
                "status": "fail",
                "violation_count": 1,
                "affected": [
                    {
                        "binding": "public.deals",
                        "key": {
                            "deal_id": "deal-1"
                        },
                        "field": "deal_id",
                        "value": null
                    }
                ]
            }
        ],
        "refusal": null
    })
}

#[test]
fn public_live_verify_artifact_parse_preserves_identity_and_loaded_count() {
    let artifact = LiveVerifyArtifact::parse(
        "constraints.verify.json",
        br#"
        {
          "version": "verify.constraint.v1",
          "constraint_set_id": "deals-portable",
          "bindings": [
            { "name": "public.deals", "kind": "relation" }
          ],
          "rules": [
            {
              "id": "deal_exists",
              "severity": "error",
              "portability": "portable",
              "check": {
                "op": "not_null",
                "binding": "public.deals",
                "columns": ["deal_id"]
              }
            },
            {
              "id": "tenant_exists",
              "severity": "warn",
              "portability": "portable",
              "check": {
                "op": "row_count",
                "binding": "public.deals",
                "compare": { "gte": 1 }
              }
            }
          ]
        }
        "#,
    )
    .expect("artifact should parse");

    assert_eq!(artifact.constraint_set_id, "deals-portable");
    assert_eq!(artifact.report.source, "constraints.verify.json");
    assert_eq!(artifact.report.loaded, 2);
    assert!(artifact.report.hash.starts_with("sha256:"));
    assert!(artifact.batch_only_rule.is_none());
}

#[test]
fn public_live_verify_artifact_parse_detects_batch_only_rule() {
    let artifact = LiveVerifyArtifact::parse(
        "orphan_rows.verify.json",
        br#"
        {
          "version": "verify.constraint.v1",
          "constraint_set_id": "deals-batch-only",
          "bindings": [
            { "name": "public.deals", "kind": "relation" }
          ],
          "rules": [
            {
              "id": "orphan_rows",
              "severity": "error",
              "portability": "batch_only",
              "check": {
                "op": "query_zero_rows",
                "bindings": ["public.deals"],
                "query": "SELECT 1"
              }
            }
          ]
        }
        "#,
    )
    .expect("artifact should parse");

    let batch_only_rule = artifact
        .batch_only_rule
        .expect("batch-only rule should be preserved");
    assert_eq!(batch_only_rule.rule_id, "orphan_rows");
    assert_eq!(batch_only_rule.op, "query_zero_rows");
}

#[test]
fn public_report_surface_keeps_verify_payload_separate_from_twin_native_metrics() {
    let mut report = seeded_report();
    report
        .attach_verify_report(schema_correct_verify_report())
        .expect("verify report should attach");

    report.attach_null_rates(RatioMap::from([(
        String::from("public.deals.deal_name"),
        0.25,
    )]));
    report.attach_fk_coverage(BTreeMap::from([(
        String::from("public.deals.tenant_id"),
        1.0,
    )]));

    let rendered: Value =
        serde_json::from_str(&report.render_json().expect("report json")).expect("parse report");

    assert_eq!(
        rendered["verify_artifact"]["source"],
        "constraints.verify.json"
    );
    assert_eq!(rendered["verify"]["version"], "verify.report.v1");
    assert_eq!(rendered["verify"]["tool"], "verify");
    assert_eq!(rendered["verify"]["execution_mode"], "embedded");
    assert_eq!(rendered["verify"]["constraint_set_id"], "deals-portable");
    assert_eq!(rendered["verify"]["summary"]["failed_rules"], 1);
    assert_eq!(
        rendered["verify"]["summary"]["by_severity"]["error"],
        json!(1)
    );
    assert_eq!(rendered["verify"]["results"][0]["rule_id"], "DEAL_EXISTS");
    assert_eq!(
        rendered["null_rates"]["public.deals.deal_name"],
        json!(0.25)
    );
    assert_eq!(
        rendered["fk_coverage"]["public.deals.tenant_id"],
        json!(1.0)
    );
    assert_eq!(rendered["constraints"]["fk_violations"], 0);
    assert_eq!(rendered["mode"], "bootstrap");
}

#[test]
fn public_report_surface_rejects_partial_verify_payloads() {
    let mut report = seeded_report();
    let error = report
        .attach_verify_report(json!({
            "tool": "verify",
            "version": "verify.report.v1",
            "outcome": "FAIL"
        }))
        .expect_err("partial verify payload should be rejected");

    assert!(
        error.contains("schema-correct `verify.report.v1`"),
        "unexpected error: {error}"
    );
    assert!(report.verify.is_none());
}

#[test]
fn twinning_schema_verify_section_tracks_verify_report_required_fields() {
    let schema: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/twinning.v0.schema.json"
    )))
    .expect("parse local twinning schema");

    let verify = &schema["$defs"]["verify_report"];
    let required = verify["required"]
        .as_array()
        .expect("verify required array")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();

    assert_eq!(
        required,
        vec![
            "tool",
            "version",
            "execution_mode",
            "outcome",
            "constraint_set_id",
            "constraint_hash",
            "bindings",
            "summary",
            "policy_signals",
            "results",
            "refusal"
        ]
    );
    assert_eq!(verify["properties"]["tool"]["const"], "verify");
    assert_eq!(verify["properties"]["version"]["const"], "verify.report.v1");
    assert_eq!(
        verify["properties"]["execution_mode"]["enum"],
        json!(["batch", "embedded"])
    );
    assert_eq!(
        verify["properties"]["outcome"]["enum"],
        json!(["PASS", "FAIL", "REFUSAL"])
    );
    assert_eq!(
        verify["properties"]["constraint_hash"]["$ref"],
        "#/$defs/sha256"
    );
}
