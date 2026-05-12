#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::{Value, json};
use tempfile::tempdir;
use twinning::{
    cli::Engine,
    migration_proof::TwinPairEvidenceKind,
    orchestration_manifest::{
        TWIN_PAIR_ORCHESTRATION_MANIFEST_VERSION, TwinPairEndpointBootstrap,
        load_twin_pair_orchestration_manifest,
    },
};

const FIXTURE_ROOT: &str = "tests/fixtures/differential/twin_pair_orchestration";

#[test]
fn twin_pair_orchestration_manifest_fixture_parses_contract_surface() {
    let manifest = load_twin_pair_orchestration_manifest(&manifest_path()).expect("load manifest");

    assert_eq!(manifest.version, TWIN_PAIR_ORCHESTRATION_MANIFEST_VERSION);
    assert_eq!(manifest.proof_id, "loan-performance-migration-proof:v0");
    assert_eq!(manifest.left_endpoint.endpoint_id, "legacy");
    assert_eq!(manifest.right_endpoint.endpoint_id, "candidate");
    assert_eq!(manifest.left_endpoint.engine, Engine::Postgres);
    assert_eq!(manifest.right_endpoint.engine, Engine::Postgres);
    assert_ne!(
        manifest.left_endpoint.endpoint_id,
        manifest.right_endpoint.endpoint_id
    );

    assert!(matches!(
        &manifest.left_endpoint.bootstrap,
        TwinPairEndpointBootstrap::Restore { .. }
    ));
    if let TwinPairEndpointBootstrap::Restore { snapshot } = &manifest.left_endpoint.bootstrap {
        assert_eq!(snapshot, "out/legacy.twin");
    }

    assert!(matches!(
        &manifest.right_endpoint.bootstrap,
        TwinPairEndpointBootstrap::Schema { .. }
    ));
    if let TwinPairEndpointBootstrap::Schema {
        schema,
        declaration,
        load,
    } = &manifest.right_endpoint.bootstrap
    {
        assert!(schema.ends_with("schema.sql"));
        assert!(
            declaration
                .as_ref()
                .expect("declaration")
                .ends_with("declaration.json")
        );
        assert_eq!(load, &vec![String::from("load/candidate.sql")]);
    }

    assert_eq!(manifest.target_evidence.len(), 3);
    assert_eq!(
        manifest.target_evidence[0].artifact_kind,
        TwinPairEvidenceKind::Verify
    );
    assert_eq!(
        manifest.target_evidence[1].artifact_kind,
        TwinPairEvidenceKind::Benchmark
    );
    assert_eq!(
        manifest.target_evidence[2].artifact_kind,
        TwinPairEvidenceKind::Assess
    );
    assert_eq!(manifest.artifact_outputs.report, "out/twin-pair-proof.json");

    let schema: Value =
        serde_json::from_str(&fs::read_to_string(schema_path()).expect("read manifest schema"))
            .expect("parse manifest schema");
    assert_eq!(
        schema["properties"]["version"]["const"],
        TWIN_PAIR_ORCHESTRATION_MANIFEST_VERSION
    );
    assert_eq!(
        schema["$defs"]["endpoint"]["properties"]["engine"]["const"],
        "postgres"
    );
}

#[test]
fn twin_pair_orchestration_manifest_refuses_non_postgres_or_scored_evidence() {
    let workspace = tempdir().expect("manifest workspace");

    let mut non_postgres = fixture_json();
    non_postgres["left_endpoint"]["engine"] = json!("oracle");
    let non_postgres_path = workspace.path().join("non-postgres.json");
    write_json(&non_postgres_path, &non_postgres);
    let refusal = load_twin_pair_orchestration_manifest(&non_postgres_path)
        .expect_err("oracle endpoint should refuse");
    let rendered: Value =
        serde_json::from_str(&refusal.render(true).expect("render refusal")).expect("refusal json");
    assert_eq!(
        rendered["refusal"]["code"],
        "E_TWIN_PAIR_ORCHESTRATION_MANIFEST"
    );
    assert_eq!(
        rendered["refusal"]["detail"]["validation"]["engine"],
        "oracle"
    );

    let mut scored_evidence = fixture_json();
    scored_evidence["target_evidence"][0]["score"] = json!(0.99);
    let scored_evidence_path = workspace.path().join("scored-evidence.json");
    write_json(&scored_evidence_path, &scored_evidence);
    let refusal = load_twin_pair_orchestration_manifest(&scored_evidence_path)
        .expect_err("scored evidence should refuse");
    let rendered: Value =
        serde_json::from_str(&refusal.render(true).expect("render refusal")).expect("refusal json");
    assert_eq!(
        rendered["refusal"]["code"],
        "E_TWIN_PAIR_ORCHESTRATION_MANIFEST"
    );
    assert!(
        rendered["refusal"]["detail"]["error"]
            .as_str()
            .expect("parse error")
            .contains("unknown field `score`")
    );
}

fn fixture_json() -> Value {
    serde_json::from_str(&fs::read_to_string(manifest_path()).expect("read manifest"))
        .expect("parse fixture json")
}

fn write_json(path: &Path, value: &Value) {
    fs::write(
        path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(value).expect("render fixture")
        ),
    )
    .expect("write fixture");
}

fn manifest_path() -> PathBuf {
    fixture_dir().join("manifest.json")
}

fn schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("twinning.twin-pair-orchestration-manifest.v0.schema.json")
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT)
}
