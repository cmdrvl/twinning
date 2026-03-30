use std::path::PathBuf;

#[test]
#[ignore = "differential read corpus not implemented yet"]
fn read_corpus() {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("differential")
        .join("read_corpus");
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for read_corpus"
    );
}
