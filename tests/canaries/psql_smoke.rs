use std::path::PathBuf;

#[test]
#[ignore = "pgwire compatibility canary not implemented yet"]
fn psql_smoke() {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("canaries")
        .join("psql_smoke");
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for psql_smoke"
    );
}
