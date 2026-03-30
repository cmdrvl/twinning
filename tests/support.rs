#![allow(dead_code)]

use std::{collections::BTreeSet, fs, path::PathBuf, sync::OnceLock};

use serde::Deserialize;

const MANIFEST_VERSION: &str = "twinning.canary-manifest.v0";
const MANIFEST_ENGINE: &str = "postgres";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct CanaryManifest {
    pub(crate) version: String,
    pub(crate) engine: String,
    pub(crate) canaries: Vec<CanaryDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct CanaryDefinition {
    pub(crate) id: String,
    pub(crate) client: String,
    pub(crate) session_shapes: Vec<String>,
    pub(crate) write_shapes: Vec<String>,
    pub(crate) read_shapes: Vec<String>,
    pub(crate) required_sqlstates: Vec<String>,
    pub(crate) unsupported_policy: String,
}

static MANIFEST: OnceLock<CanaryManifest> = OnceLock::new();

pub(crate) fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub(crate) fn manifest() -> &'static CanaryManifest {
    MANIFEST.get_or_init(load_manifest)
}

pub(crate) fn manifest_canary_ids() -> Vec<&'static str> {
    manifest()
        .canaries
        .iter()
        .map(|canary| canary.id.as_str())
        .collect()
}

pub(crate) fn canary_by_id(id: &str) -> &'static CanaryDefinition {
    manifest()
        .canaries
        .iter()
        .find(|canary| canary.id == id)
        .expect("requested canary should exist in the checked-in manifest")
}

pub(crate) fn harness_path(id: &str) -> PathBuf {
    let canary = canary_by_id(id);
    repo_root()
        .join("tests")
        .join("canaries")
        .join(format!("{}.rs", canary.id))
}

pub(crate) fn fixture_dir(id: &str) -> PathBuf {
    let canary = canary_by_id(id);
    repo_root()
        .join("tests")
        .join("fixtures")
        .join("canaries")
        .join(&canary.id)
}

pub(crate) fn canary_fixture_dir_for_test(test_name: &str) -> PathBuf {
    let canary = canary_by_id(test_name);
    let fixture_dir = fixture_dir(&canary.id);
    assert!(
        fixture_dir.exists(),
        "missing canary fixture dir for `{}` at `{}`",
        canary.id,
        fixture_dir.display()
    );
    fixture_dir
}

pub(crate) fn assert_canary_layout(id: &str) {
    let harness = harness_path(id);
    let fixture = fixture_dir(id);

    assert!(
        harness.exists(),
        "missing canary harness file for `{id}` at `{}`",
        harness.display()
    );
    assert!(
        fixture.exists(),
        "missing canary fixture dir for `{id}` at `{}`",
        fixture.display()
    );
}

fn load_manifest() -> CanaryManifest {
    let path = manifest_path();
    let raw = fs::read_to_string(&path).expect("checked-in canary manifest should be readable");
    let manifest: CanaryManifest =
        serde_json::from_str(&raw).expect("checked-in canary manifest should parse");
    validate_manifest(&manifest);
    manifest
}

fn validate_manifest(manifest: &CanaryManifest) {
    assert_eq!(manifest.version, MANIFEST_VERSION);
    assert_eq!(manifest.engine, MANIFEST_ENGINE);
    assert!(
        !manifest.canaries.is_empty(),
        "checked-in canary manifest must declare at least one canary"
    );

    let mut ids = BTreeSet::new();
    for canary in &manifest.canaries {
        assert!(
            !canary.id.trim().is_empty(),
            "canary ids must not be empty in `{}`",
            manifest_path().display()
        );
        assert!(
            ids.insert(canary.id.clone()),
            "duplicate canary id `{}` in `{}`",
            canary.id,
            manifest_path().display()
        );
    }
}

fn manifest_path() -> PathBuf {
    repo_root().join("canaries").join("manifest.v0.json")
}
