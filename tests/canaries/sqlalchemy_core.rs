use std::path::PathBuf;

#[test]
#[ignore = "pgwire compatibility canary not implemented yet"]
fn sqlalchemy_core() {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("canaries")
        .join("sqlalchemy_core");
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for sqlalchemy_core"
    );
}
