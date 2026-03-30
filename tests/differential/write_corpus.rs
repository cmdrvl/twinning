use std::path::PathBuf;

#[test]
#[ignore = "differential write corpus not implemented yet"]
fn write_corpus() {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("differential")
        .join("write_corpus");
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for write_corpus"
    );
}
