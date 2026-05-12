#![forbid(unsafe_code)]

use std::{fs, path::PathBuf};

use serde_json::Value;

#[test]
fn replay_proof_backend_policy_pins_deferred_backend_boundary() {
    let policy = fs::read_to_string(policy_path()).expect("read replay/proof backend policy");

    assert!(policy.contains("twinning.replay-backend-policy.v0"));
    for backend_class in [
        "in_memory_snapshot",
        "snapshot_backed",
        "disk_backed",
        "delegated_postgres",
    ] {
        assert!(
            policy.contains(backend_class),
            "policy should name backend class `{backend_class}`"
        );
    }
    for invariant in [
        "Protocol-visible behavior must be equivalent",
        "Unsupported shapes remain SKIP rows or protocol-visible refusals",
        "Reports and snapshots see committed state only",
        "Twin A may delegate storage only when all of these are true",
        "Twin B must remain a `twinning` materialization",
    ] {
        assert!(
            policy.contains(invariant),
            "policy should pin invariant `{invariant}`"
        );
    }

    let operator: Value =
        serde_json::from_str(&fs::read_to_string(operator_path()).expect("read operator manifest"))
            .expect("parse operator manifest");
    let implemented_surface = operator["current_runtime_behavior"]["implemented_surface"]
        .as_array()
        .expect("implemented surface")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(implemented_surface.contains("replay/proof backend policy"));

    let deferred_surface = operator["current_runtime_behavior"]["deferred_surface"]
        .as_array()
        .expect("deferred surface")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(deferred_surface.contains("snapshot-backed, disk-backed, and delegated"));
}

fn policy_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("docs")
        .join("REPLAY_PROOF_BACKEND_POLICY.md")
}

fn operator_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("operator.json")
}
