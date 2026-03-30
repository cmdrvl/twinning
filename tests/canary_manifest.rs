#![forbid(unsafe_code)]

#[path = "support.rs"]
mod support;

#[test]
fn manifest_lists_expected_canaries_and_matching_harness_files() {
    let manifest = support::manifest();
    let ids = support::manifest_canary_ids();

    assert_eq!(manifest.version, "twinning.canary-manifest.v0");
    assert_eq!(manifest.engine, "postgres");
    assert_eq!(
        ids,
        vec![
            "psql_smoke",
            "psycopg2_params",
            "sqlalchemy_core",
            "extractor_canary",
        ]
    );

    for id in ids {
        support::assert_canary_layout(id);
    }
}
