use std::path::PathBuf;

#[test]
#[ignore = "extractor compatibility canary not implemented yet"]
fn extractor_canary() {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("canaries")
        .join("extractor_canary");
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for extractor_canary"
    );
}
