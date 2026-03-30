#![forbid(unsafe_code)]

use std::{fs, path::PathBuf};

use serde_json::Value;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn manifest_lists_expected_canaries_and_matching_harness_files() {
    let root = repo_root();
    let manifest_path = root.join("canaries").join("manifest.v0.json");
    let manifest = fs::read_to_string(&manifest_path).expect("read canary manifest");
    let json: Value = serde_json::from_str(&manifest).expect("parse canary manifest");

    assert_eq!(json["version"], "twinning.canary-manifest.v0");
    assert_eq!(json["engine"], "postgres");

    let canaries = json["canaries"].as_array().expect("canaries array");
    let ids = canaries
        .iter()
        .map(|canary| canary["id"].as_str().expect("canary id string").to_owned())
        .collect::<Vec<_>>();

    assert_eq!(
        ids,
        vec![
            String::from("psql_smoke"),
            String::from("psycopg2_params"),
            String::from("sqlalchemy_core"),
            String::from("extractor_canary"),
        ]
    );

    for id in ids {
        let harness = root.join("tests").join("canaries").join(format!("{id}.rs"));
        let fixture = root
            .join("tests")
            .join("fixtures")
            .join("canaries")
            .join(&id);
        assert!(harness.exists(), "missing canary harness file for `{id}`");
        assert!(fixture.exists(), "missing canary fixture dir for `{id}`");
    }
}
