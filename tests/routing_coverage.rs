#![cfg(feature = "rest")]

use std::{fs, path::Path};

use twinning::protocol::rest::{
    policy::RoutingConfig,
    spec::parse_rest_catalog_bytes,
    topology::{Confidence, build_spec_topology},
};

#[track_caller]
fn check_topology_coverage(
    spec_path: &str,
    config: RoutingConfig,
    min_route_coverage: f64,
    min_confidence_coverage: f64,
) -> (usize, usize, usize) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/routing")
        .join(spec_path);
    let raw = fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "failed to read routing fixture `{}`: {error}",
            path.display()
        )
    });
    let catalog = parse_rest_catalog_bytes(&raw, path.display().to_string())
        .unwrap_or_else(|error| panic!("failed to parse routing fixture `{spec_path}`: {error:?}"));
    let topology = build_spec_topology(&catalog, &config);
    let total = catalog.paths.len();
    let classified = topology.path_bindings.len();
    let high_confidence_count = topology
        .path_bindings
        .values()
        .filter(|entry| matches!(entry.confidence, Confidence::Pinned | Confidence::High))
        .count();
    let route_coverage = coverage(classified, total);
    let confidence_coverage = coverage(high_confidence_count, classified);

    eprintln!(
        "{spec_path}: classified={classified}, total={total}, high_or_pinned={high_confidence_count}, warnings={:?}",
        topology.warnings
    );

    assert!(
        route_coverage >= min_route_coverage,
        "{spec_path} route coverage {route_coverage:.3} below minimum {min_route_coverage:.3}"
    );
    assert!(
        confidence_coverage >= min_confidence_coverage,
        "{spec_path} high/pinned confidence coverage {confidence_coverage:.3} below minimum {min_confidence_coverage:.3}"
    );

    (classified, total, high_confidence_count)
}

fn coverage(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[test]
fn petstore_100pct_all_high() {
    assert_eq!(
        check_topology_coverage("petstore.yaml", RoutingConfig::default(), 1.0, 1.0),
        (2, 2, 2)
    );
}

#[test]
fn reqres_80pct_90pct_high() {
    let (classified, total, high_confidence_count) =
        check_topology_coverage("reqres.yaml", RoutingConfig::default(), 0.80, 0.90);
    assert_eq!((classified, total), (3, 3));
    assert_eq!(high_confidence_count, classified);
}

#[test]
fn wiremock_admin_80pct_90pct_pinned_or_high() {
    let (classified, total, high_confidence_count) =
        check_topology_coverage("wiremock-admin.yaml", RoutingConfig::default(), 0.80, 0.90);
    assert_eq!((classified, total), (3, 3));
    assert_eq!(high_confidence_count, classified);
}

#[test]
fn uber_estimates_80pct_90pct_high() {
    let (classified, total, high_confidence_count) =
        check_topology_coverage("uber-estimates.yaml", RoutingConfig::default(), 0.80, 0.90);
    assert_eq!((classified, total), (3, 3));
    assert_eq!(high_confidence_count, classified);
}

#[test]
fn onepassword_80pct_90pct_high() {
    let (classified, total, high_confidence_count) =
        check_topology_coverage("1password.yaml", RoutingConfig::default(), 0.80, 0.90);
    assert_eq!((classified, total), (3, 3));
    assert_eq!(high_confidence_count, classified);
}

#[test]
fn google_books_80pct_80pct_high() {
    check_topology_coverage("google-books.yaml", RoutingConfig::default(), 0.80, 0.80);
}
