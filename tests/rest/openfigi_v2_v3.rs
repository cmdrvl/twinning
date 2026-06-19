use std::{collections::BTreeMap, path::PathBuf};

use axum::http::{HeaderMap, Uri, header};
use twinning::protocol::rest::{
    normalize::{IrOp, NormalizeRequest, normalize_request},
    policy::RoutingConfig,
    routes::{Method, PathPattern, RouteKind, RouteRegistry, build_route_registry, match_route},
    seed::seed_from_spec,
    spec::{RestCatalog, parse_rest_catalog_bytes},
    topology::build_spec_topology,
};
use twinning::{
    backend::BaseSnapshotBackend,
    kernel::{read::execute_read, storage::TableStorage},
    result::KernelResult,
};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("rest")
        .join("openfigi_v2_v3")
        .join("schema.json")
}

fn openfigi_catalog() -> RestCatalog {
    let path = fixture_path();
    let raw = std::fs::read(&path).expect("read OpenFIGI fixture");
    parse_rest_catalog_bytes(&raw, path.display().to_string()).expect("parse OpenFIGI schema")
}

fn route_pattern(pattern: &PathPattern) -> String {
    let parts = pattern
        .segments
        .iter()
        .map(|segment| match segment {
            twinning::protocol::rest::routes::PathSegment::Literal(value) => value.clone(),
            twinning::protocol::rest::routes::PathSegment::Param(name) => format!("{{{name}}}"),
            twinning::protocol::rest::routes::PathSegment::Template {
                prefix,
                name,
                suffix,
            } => {
                format!("{prefix}{{{name}}}{suffix}")
            }
        })
        .collect::<Vec<_>>();
    format!("/{}", parts.join("/"))
}

fn registry_with_server_variable(name: &str, value: &str) -> RouteRegistry {
    let catalog = openfigi_catalog();
    let mut config = RoutingConfig::default();
    config
        .server_variables
        .insert(name.to_owned(), value.to_owned());
    let topology = build_spec_topology(&catalog, &config);
    build_route_registry(&catalog, &topology, &config)
}

fn route_matrix(registry: &RouteRegistry) -> BTreeMap<String, String> {
    registry
        .routes
        .iter()
        .map(|(method, pattern, entry)| {
            (
                format!("{} {}", method.as_str(), route_pattern(pattern)),
                serde_json::to_string(&entry.kind).expect("serialize route kind"),
            )
        })
        .collect()
}

fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    headers
}

fn empty_backend(catalog: &RestCatalog) -> BaseSnapshotBackend {
    let tables = catalog
        .catalog
        .tables
        .iter()
        .map(|table| TableStorage::new(table).expect("table storage should build"));
    BaseSnapshotBackend::new(tables).expect("backend should build")
}

#[test]
fn openfigi_fixture_preserves_server_variable_contract() {
    let catalog = openfigi_catalog();

    assert_eq!(
        catalog.spec_hash,
        "sha256:5afdc7dd1da201fbf83be014df847996250abd138a760844625c5bc0384aea4f"
    );
    assert_eq!(catalog.paths.len(), 4);
    for path in ["/mapping", "/filter", "/search", "/mapping/values/{key}"] {
        assert!(
            catalog.paths.contains_key(path),
            "missing OpenFIGI path {path}"
        );
    }

    let server = catalog.servers.first().expect("OpenFIGI server");
    assert_eq!(server.url, "https://api.openfigi.com/{basePath}");
    let base_path = server.variables.get("basePath").expect("basePath variable");
    assert_eq!(base_path.default, "v3");
    assert_eq!(base_path.enum_values, vec!["v3", "v2"]);

    let filter = catalog
        .resources
        .get("filterrequests")
        .expect("FilterRequest resource");
    assert!(
        filter
            .columns
            .iter()
            .any(|column| column.name == "contractSize" && column.normalized_type == "array")
    );

    let values = catalog
        .resources
        .get("inline_response_200s")
        .expect("mapping values response resource");
    assert_eq!(values.primary_key, Some(vec![String::from("key")]));
    assert_eq!(values.meta.path_lookup_columns, vec![String::from("key")]);
    assert!(
        values
            .columns
            .iter()
            .any(|column| column.name == "values" && column.normalized_type == "array")
    );
}

#[test]
fn selected_openfigi_base_path_mounts_versioned_routes() {
    let registry = registry_with_server_variable("basePath", "v3");
    let matrix = route_matrix(&registry);

    for route in [
        "POST /v3/mapping",
        "POST /v3/filter",
        "POST /v3/search",
        "GET /v3/mapping/values/{key}",
    ] {
        assert!(matrix.contains_key(route), "missing mounted route {route}");
    }

    assert!(
        match_route(&registry.routes, Method::Post, "/v3/mapping").is_some(),
        "mounted mapping route should match"
    );
    assert!(
        match_route(&registry.routes, Method::Post, "/mapping").is_none(),
        "source path should not match when server variable mounting is selected"
    );

    let filter = match_route(&registry.routes, Method::Post, "/v3/filter")
        .expect("filter route should match");
    assert_eq!(filter.entry.kind, RouteKind::Create);
    let search = match_route(&registry.routes, Method::Post, "/v3/search")
        .expect("search route should match");
    assert_eq!(search.entry.kind, RouteKind::Create);
    let values = match_route(&registry.routes, Method::Get, "/v3/mapping/values/idType")
        .expect("mapping values route should match");
    assert_eq!(values.entry.kind, RouteKind::ReadOne);
    assert_eq!(values.entry.response_fields, vec![String::from("values")]);
    assert_eq!(
        values.path_params,
        BTreeMap::from([(String::from("key"), String::from("idType"))])
    );
}

#[test]
fn unselected_openfigi_schema_keeps_legacy_exact_paths() {
    let catalog = openfigi_catalog();
    let config = RoutingConfig::default();
    let topology = build_spec_topology(&catalog, &config);
    let registry = build_route_registry(&catalog, &topology, &config);

    assert!(match_route(&registry.routes, Method::Post, "/mapping").is_some());
    assert!(match_route(&registry.routes, Method::Post, "/v3/mapping").is_none());
}

#[test]
fn openfigi_array_bodies_normalize_without_shape_refusals() {
    let registry = registry_with_server_variable("basePath", "v3");
    let catalog = openfigi_catalog();
    let headers = json_headers();

    let mapping = match_route(&registry.routes, Method::Post, "/v3/mapping")
        .expect("mapping route should match");
    let mapping_op = normalize_request(
        &catalog,
        mapping.entry,
        NormalizeRequest {
            method: Method::Post,
            path_params: &mapping.path_params,
            session_id: "openfigi-mapping",
            uri: &Uri::from_static("/v3/mapping"),
            headers: &headers,
            body: br#"[{"idType":"ID_BB","idValue":"IBM US"}]"#,
        },
    )
    .expect("top-level array body should normalize");
    let IrOp::Mutation(mapping_mutation) = mapping_op else {
        panic!("expected mapping mutation");
    };
    assert_eq!(mapping_mutation.rows.len(), 1);

    let filter = match_route(&registry.routes, Method::Post, "/v3/filter")
        .expect("filter route should match");
    let filter_op = normalize_request(
        &catalog,
        filter.entry,
        NormalizeRequest {
            method: Method::Post,
            path_params: &filter.path_params,
            session_id: "openfigi-filter",
            uri: &Uri::from_static("/v3/filter"),
            headers: &headers,
            body: br#"{"query":"Apple","contractSize":[100,200]}"#,
        },
    )
    .expect("object body with interval array should normalize");
    assert!(matches!(filter_op, IrOp::Mutation(_)));
}

#[test]
fn openfigi_mapping_values_seed_and_read_by_path_key() {
    let catalog = openfigi_catalog();
    let mut backend = empty_backend(&catalog);
    let seed = seed_from_spec(&catalog, &mut backend).expect("seed OpenFIGI examples");
    assert!(
        seed.rows_seeded >= 3,
        "expected mapping value response examples to seed rows, got {seed:?}"
    );
    backend.promote_overlay_to_base();

    let registry = registry_with_server_variable("basePath", "v3");
    let values = match_route(&registry.routes, Method::Get, "/v3/mapping/values/idType")
        .expect("mapping values route should match");
    let op = normalize_request(
        &catalog,
        values.entry,
        NormalizeRequest {
            method: Method::Get,
            path_params: &values.path_params,
            session_id: "openfigi-values",
            uri: &Uri::from_static("/v3/mapping/values/idType"),
            headers: &HeaderMap::new(),
            body: b"",
        },
    )
    .expect("values lookup should normalize");
    let IrOp::Read(read) = op else {
        panic!("expected values read");
    };
    assert_eq!(read.table, "inline_response_200s");
    assert_eq!(read.projection, vec![String::from("values")]);

    let result = execute_read(&catalog.catalog, &backend, &read);
    let KernelResult::Read(read_result) = result else {
        panic!("expected read result, got {result:?}");
    };
    assert_eq!(read_result.rows.len(), 1);
    assert_eq!(
        serde_json::to_value(&read_result.rows[0][0]).expect("serialize scalar"),
        serde_json::json!({
            "array": [
                { "text": "ID_BB" },
                { "text": "ID_CINS" },
                { "text": "ID_CUSIP" }
            ]
        })
    );
}
