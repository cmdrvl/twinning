use std::path::PathBuf;

#[test]
#[ignore = "pgwire compatibility canary not implemented yet"]
fn psycopg2_params() {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("canaries")
        .join("psycopg2_params");
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for psycopg2_params"
    );
}
