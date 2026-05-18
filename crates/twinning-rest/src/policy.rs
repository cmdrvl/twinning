//! REST routing policy configuration.

use std::{collections::BTreeMap, error::Error, fmt, str::FromStr};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use super::{routes::RouteEntry, spec::RestCatalog, xext::XTwinningExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
pub enum RoutingPolicy {
    #[serde(rename = "flat-crud", alias = "flat_crud", alias = "flatcrud")]
    FlatCrud,
    #[serde(rename = "schema-first", alias = "schema_first", alias = "schemafirst")]
    SchemaFirst,
    #[serde(
        rename = "prefix-scoped",
        alias = "prefix_scoped",
        alias = "prefixscoped"
    )]
    PrefixScoped,
    #[serde(rename = "auto")]
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseRoutingPolicyError {
    value: String,
}

impl ParseRoutingPolicyError {
    fn new(value: &str) -> Self {
        Self {
            value: value.to_owned(),
        }
    }
}

impl fmt::Display for ParseRoutingPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unsupported REST routing policy `{}`; expected flat-crud, schema-first, prefix-scoped, or auto",
            self.value
        )
    }
}

impl Error for ParseRoutingPolicyError {}

impl fmt::Display for RoutingPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::FlatCrud => "flat-crud",
            Self::SchemaFirst => "schema-first",
            Self::PrefixScoped => "prefix-scoped",
            Self::Auto => "auto",
        })
    }
}

impl FromStr for RoutingPolicy {
    type Err = ParseRoutingPolicyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "flat-crud" | "flat_crud" | "flatcrud" => Ok(Self::FlatCrud),
            "schema-first" | "schema_first" | "schemafirst" => Ok(Self::SchemaFirst),
            "prefix-scoped" | "prefix_scoped" | "prefixscoped" => Ok(Self::PrefixScoped),
            "auto" => Ok(Self::Auto),
            _ => Err(ParseRoutingPolicyError::new(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingConfig {
    pub policy: RoutingPolicy,
    pub base_prefix: Option<String>,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            policy: RoutingPolicy::Auto,
            base_prefix: None,
        }
    }
}

pub fn resolve_routing_config(
    cli_policy: Option<RoutingPolicy>,
    cli_base_prefix: Option<String>,
    x_twinning: Option<&XTwinningExt>,
) -> RoutingConfig {
    let extension_policy = x_twinning.and_then(|extension| extension.routing);
    let extension_base_prefix =
        x_twinning.and_then(|extension| extension.base_prefix.as_ref().cloned());

    RoutingConfig {
        policy: cli_policy
            .or(extension_policy)
            .unwrap_or(RoutingPolicy::Auto),
        base_prefix: cli_base_prefix.or(extension_base_prefix),
    }
}

pub fn schema_first_classify(route: &RouteEntry, catalog: &RestCatalog) -> Option<String> {
    let response_resource = route.response_resource_name.as_ref()?;
    let resource = catalog.resources.get(response_resource)?;

    if let Some(item_resource) = &resource.meta.item_resource {
        return catalog
            .resources
            .contains_key(item_resource)
            .then(|| item_resource.clone());
    }

    Some(response_resource.clone())
}

pub fn normalize_resource_name(segment: &str) -> String {
    if segment.is_empty() {
        return String::new();
    }

    let mut normalized = segment.trim().to_ascii_lowercase();
    if normalized.len() > 3 && normalized.ends_with("ies") {
        normalized.truncate(normalized.len() - 3);
        normalized.push('y');
    } else if normalized.len() > 2 && normalized.ends_with('s') {
        normalized.pop();
    }

    let mut chars = normalized.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    first.to_uppercase().collect::<String>() + chars.as_str()
}

pub fn flat_crud_classify(
    path: &str,
    catalog: &RestCatalog,
    base_prefix: Option<&str>,
) -> Option<String> {
    let request_segments = path_segments(path);
    let remaining_segments = if let Some(prefix) = base_prefix {
        let prefix_segments = path_segments(prefix);
        if !starts_with_segments(&request_segments, &prefix_segments) {
            return None;
        }
        &request_segments[prefix_segments.len()..]
    } else {
        request_segments.as_slice()
    };

    let segment = remaining_segments.first()?;
    let normalized = normalize_resource_name(segment);
    let catalog_resource = catalog_resource_name(&normalized);

    let mut candidates = Vec::new();
    push_unique(&mut candidates, (*segment).to_owned());
    push_unique(&mut candidates, segment.to_ascii_lowercase());
    push_unique(&mut candidates, normalized);
    push_unique(&mut candidates, catalog_resource);

    candidates
        .into_iter()
        .find(|candidate| catalog.resources.contains_key(candidate))
}

pub fn auto_detect_prefix(paths: &[&str]) -> Option<String> {
    if paths.len() < 2 {
        return None;
    }

    let mut counts = BTreeMap::<Vec<String>, usize>::new();
    for path in paths {
        let segments = path_segments(path);
        for depth in 1..segments.len() {
            if !segments[..depth]
                .iter()
                .all(|segment| is_literal_prefix_segment(segment))
            {
                continue;
            }
            let prefix = segments[..depth]
                .iter()
                .map(|segment| (*segment).to_owned())
                .collect::<Vec<_>>();
            *counts.entry(prefix).or_default() += 1;
        }
    }

    let total = paths.len();
    let mut best = None::<(Vec<String>, usize)>;
    for (segments, count) in counts {
        if count * 100 < total * 60 {
            continue;
        }

        let replace = best.as_ref().is_none_or(|(best_segments, best_count)| {
            count > *best_count
                || (count == *best_count && segments.len() > best_segments.len())
                || (count == *best_count
                    && segments.len() == best_segments.len()
                    && segments < *best_segments)
        });

        if replace {
            best = Some((segments, count));
        }
    }

    best.map(|(segments, _)| format!("/{}", segments.join("/")))
}

pub fn prefix_scoped_classify(
    path: &str,
    catalog: &RestCatalog,
    base_prefix: &str,
) -> Option<String> {
    let prefix = normalize_prefix(base_prefix);
    if prefix.is_empty() {
        return flat_crud_classify(path, catalog, None);
    }

    let request_segments = path_segments(path);
    let prefix_segments = path_segments(&prefix);
    if !starts_with_segments(&request_segments, &prefix_segments) {
        return None;
    }

    let remaining_segments = &request_segments[prefix_segments.len()..];
    if remaining_segments.is_empty() {
        return None;
    }

    let scoped_path = format!("/{}", remaining_segments.join("/"));
    flat_crud_classify(&scoped_path, catalog, None)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixScopedClassifier {
    pub detected_prefix: Option<String>,
    pub effective_prefix: String,
}

impl PrefixScopedClassifier {
    pub fn new(all_paths: &[&str], config_prefix: Option<&str>) -> Self {
        let detected_prefix = auto_detect_prefix(all_paths);
        let effective_prefix = config_prefix
            .map(normalize_prefix)
            .filter(|prefix| !prefix.is_empty())
            .or_else(|| detected_prefix.clone())
            .unwrap_or_default();

        Self {
            detected_prefix,
            effective_prefix,
        }
    }

    pub fn classify(&self, path: &str, catalog: &RestCatalog) -> Option<String> {
        if self.effective_prefix.is_empty() {
            None
        } else {
            prefix_scoped_classify(path, catalog, &self.effective_prefix)
        }
    }
}

fn path_segments(path: &str) -> Vec<&str> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn is_literal_prefix_segment(segment: &str) -> bool {
    !segment.contains('{') && !segment.contains('}')
}

fn normalize_prefix(prefix: &str) -> String {
    let segments = path_segments(prefix);
    if segments.is_empty() {
        String::new()
    } else {
        format!("/{}", segments.join("/"))
    }
}

fn starts_with_segments(path_segments: &[&str], prefix_segments: &[&str]) -> bool {
    prefix_segments.len() <= path_segments.len()
        && path_segments
            .iter()
            .zip(prefix_segments)
            .all(|(path, prefix)| path == prefix)
}

fn catalog_resource_name(normalized_schema_name: &str) -> String {
    if normalized_schema_name.is_empty() {
        String::new()
    } else {
        format!("{}s", normalized_schema_name.to_ascii_lowercase())
    }
}

fn push_unique(candidates: &mut Vec<String>, candidate: String) {
    if !candidate.is_empty() && !candidates.iter().any(|seen| seen == &candidate) {
        candidates.push(candidate);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PrefixScopedClassifier, RoutingConfig, RoutingPolicy, auto_detect_prefix,
        flat_crud_classify, normalize_resource_name, prefix_scoped_classify,
        resolve_routing_config, schema_first_classify,
    };
    use crate::protocol::rest::{
        auth::RestAuthMode,
        routes::{Method, PathPattern, RouteEntry, build_route_registry_with_config},
        spec::{RestCatalog, parse_rest_catalog_bytes},
        xext::XTwinningExt,
    };

    fn catalog_and_route(spec: &str, method: Method, path: &str) -> (RestCatalog, RouteEntry) {
        let catalog =
            parse_rest_catalog_bytes(spec.as_bytes(), "policy.yaml").expect("spec should parse");
        let registry = build_route_registry_with_config(&catalog, &RoutingConfig::default());
        let pattern = PathPattern::parse(path);
        let route = registry
            .routes
            .into_iter()
            .find(|(candidate_method, candidate_pattern, _)| {
                *candidate_method == method && candidate_pattern == &pattern
            })
            .map(|(_, _, entry)| entry)
            .unwrap_or_else(|| panic!("missing route for {method} {path}"));
        (catalog, route)
    }

    fn catalog(spec: &str) -> RestCatalog {
        parse_rest_catalog_bytes(spec.as_bytes(), "policy.yaml").expect("spec should parse")
    }

    #[test]
    fn routing_policy_from_str_roundtrip() {
        for policy in [
            RoutingPolicy::FlatCrud,
            RoutingPolicy::SchemaFirst,
            RoutingPolicy::PrefixScoped,
            RoutingPolicy::Auto,
        ] {
            assert_eq!(
                policy.to_string().parse::<RoutingPolicy>(),
                Ok(policy),
                "{policy}"
            );
        }

        assert_eq!("flat_crud".parse(), Ok(RoutingPolicy::FlatCrud));
        assert_eq!("schemafirst".parse(), Ok(RoutingPolicy::SchemaFirst));
        assert_eq!("prefix_scoped".parse(), Ok(RoutingPolicy::PrefixScoped));
        assert!("nested".parse::<RoutingPolicy>().is_err());
    }

    #[test]
    fn resolve_routing_config_cli_wins() {
        let extension = XTwinningExt {
            routing: Some(RoutingPolicy::SchemaFirst),
            base_prefix: Some("/v1".to_owned()),
            auth_mode: Some(RestAuthMode::Bypass),
        };

        assert_eq!(
            resolve_routing_config(
                Some(RoutingPolicy::PrefixScoped),
                Some("/override".to_owned()),
                Some(&extension),
            ),
            RoutingConfig {
                policy: RoutingPolicy::PrefixScoped,
                base_prefix: Some("/override".to_owned()),
            }
        );

        assert_eq!(
            resolve_routing_config(None, None, Some(&extension)),
            RoutingConfig {
                policy: RoutingPolicy::SchemaFirst,
                base_prefix: Some("/v1".to_owned()),
            }
        );

        assert_eq!(
            resolve_routing_config(None, None, None),
            RoutingConfig::default()
        );
    }

    #[test]
    fn schema_first_direct_hit() {
        let (catalog, route) = catalog_and_route(
            r##"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths:
  /api/animals:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pet"
"##,
            Method::Get,
            "/api/animals",
        );

        assert_eq!(route.response_resource_name.as_deref(), Some("pets"));
        assert_eq!(
            schema_first_classify(&route, &catalog).as_deref(),
            Some("pets")
        );
    }

    #[test]
    fn schema_first_array_unwrap() {
        let (catalog, route) = catalog_and_route(
            r##"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
    Pets:
      type: array
      items:
        $ref: "#/components/schemas/Pet"
paths:
  /search:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pets"
"##,
            Method::Get,
            "/search",
        );

        assert_eq!(route.response_resource_name.as_deref(), Some("petss"));
        assert_eq!(
            catalog
                .resources
                .get("petss")
                .and_then(|resource| resource.meta.item_resource.as_deref()),
            Some("pets")
        );
        assert_eq!(
            schema_first_classify(&route, &catalog).as_deref(),
            Some("pets")
        );
    }

    #[test]
    fn schema_first_miss() {
        let (catalog, route) = catalog_and_route(
            r#"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths:
  /pets:
    get:
      responses: {}
"#,
            Method::Get,
            "/pets",
        );

        assert_eq!(route.response_resource_name, None);
        assert_eq!(schema_first_classify(&route, &catalog), None);
    }

    #[test]
    fn schema_first_unknown_schema() {
        let (catalog, route) = catalog_and_route(
            r##"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths:
  /mystery:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Mystery"
"##,
            Method::Get,
            "/mystery",
        );

        assert_eq!(route.response_resource_name.as_deref(), Some("mysterys"));
        assert_eq!(schema_first_classify(&route, &catalog), None);
    }

    #[test]
    fn normalize_entities() {
        assert_eq!(normalize_resource_name("entities"), "Entity");
        assert_eq!(normalize_resource_name("pets"), "Pet");
        assert_eq!(normalize_resource_name("Pets"), "Pet");
        assert_eq!(normalize_resource_name("pet"), "Pet");
        assert_eq!(normalize_resource_name(""), "");
    }

    #[test]
    fn flat_crud_pets_plural() {
        let catalog = catalog(
            r#"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths: {}
"#,
        );

        assert_eq!(
            flat_crud_classify("/pets", &catalog, None).as_deref(),
            Some("pets")
        );
    }

    #[test]
    fn flat_crud_users_plural() {
        let catalog = catalog(
            r#"
openapi: 3.0.3
components:
  schemas:
    User:
      type: object
      properties:
        id: { type: integer }
paths: {}
"#,
        );

        assert_eq!(
            flat_crud_classify("/users", &catalog, None).as_deref(),
            Some("users")
        );
    }

    #[test]
    fn flat_crud_already_singular() {
        let catalog = catalog(
            r#"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths: {}
"#,
        );

        assert_eq!(
            flat_crud_classify("/pet", &catalog, None).as_deref(),
            Some("pets")
        );
    }

    #[test]
    fn flat_crud_with_prefix() {
        let catalog = catalog(
            r#"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths: {}
"#,
        );

        assert_eq!(
            flat_crud_classify("/api/pets", &catalog, Some("/api")).as_deref(),
            Some("pets")
        );
    }

    #[test]
    fn flat_crud_no_match() {
        let catalog = catalog(
            r#"
openapi: 3.0.3
components:
  schemas:
    PriceEstimate:
      type: object
      properties:
        id: { type: integer }
paths: {}
"#,
        );

        assert_eq!(
            flat_crud_classify("/v1/estimates/price", &catalog, None),
            None
        );
    }

    #[test]
    fn prefix_scoped_auto_detect_prefix_api() {
        assert_eq!(
            auto_detect_prefix(&["/api/users", "/api/products", "/api/orders"]).as_deref(),
            Some("/api")
        );
    }

    #[test]
    fn prefix_scoped_auto_detect_prefix_v1() {
        assert_eq!(
            auto_detect_prefix(&["/v1/me", "/v1/estimates/price", "/v1/estimates/time"]).as_deref(),
            Some("/v1")
        );
    }

    #[test]
    fn prefix_scoped_auto_detect_prefix_mixed() {
        assert_eq!(
            auto_detect_prefix(&["/api/users", "/health", "/metrics"]),
            None
        );
    }

    #[test]
    fn prefix_scoped_auto_detect_prefix_admin() {
        assert_eq!(
            auto_detect_prefix(&[
                "/__admin/mappings",
                "/__admin/requests",
                "/__admin/scenarios",
            ])
            .as_deref(),
            Some("/__admin")
        );
    }

    #[test]
    fn prefix_scoped_single_path_has_no_auto_prefix() {
        assert_eq!(auto_detect_prefix(&["/api/users"]), None);
    }

    #[test]
    fn prefix_scoped_classify_reqres() {
        let catalog = catalog(
            r#"
openapi: 3.0.3
components:
  schemas:
    User:
      type: object
      properties:
        id: { type: integer }
paths: {}
"#,
        );

        assert_eq!(
            prefix_scoped_classify("/api/users", &catalog, "/api").as_deref(),
            Some("users")
        );
    }

    #[test]
    fn prefix_scoped_user_override_beats_auto_detect() {
        let classifier = PrefixScopedClassifier::new(
            &["/api/users", "/api/products", "/api/orders"],
            Some("/v1"),
        );

        assert_eq!(classifier.detected_prefix.as_deref(), Some("/api"));
        assert_eq!(classifier.effective_prefix, "/v1");
    }
}
