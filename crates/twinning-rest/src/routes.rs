//! REST route classification and path matching for the HTTP protocol adapter.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
};

use serde::{Deserialize, Serialize};

use super::{
    policy::{
        PrefixScopedClassifier, RoutingConfig, RoutingPolicy, flat_crud_classify,
        resolve_routing_config, schema_first_classify,
    },
    spec::{
        MediaTypeObject, OperationObject, PathItem, RequestBodyObject, ResourceColumn,
        ResourceSchema, RestCatalog, SchemaObject, SecurityRequirement, resolve_response_object,
    },
    topology::{Confidence, EvidenceSource, ResourceRelationship, ResourceTopology, TopologyEntry},
};

pub type RouteTable = Vec<(Method, PathPattern, RouteEntry)>;

const APPLICATION_FORM_URLENCODED: &str = "application/x-www-form-urlencoded";
const SUPPORTED_REQUEST_BODY_CONTENT_TYPES: &str =
    "application/json or application/x-www-form-urlencoded";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteRegistry {
    pub routes: RouteTable,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathPattern {
    pub segments: Vec<PathSegment>,
}

impl PathPattern {
    pub fn parse(path: &str) -> Self {
        let segments = path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(PathSegment::parse)
            .collect();

        Self { segments }
    }

    pub fn captures(&self, request_path: &str) -> Option<BTreeMap<String, String>> {
        let request_segments = request_segments(request_path);
        if request_segments.len() != self.segments.len() {
            return None;
        }

        let mut captures = BTreeMap::new();
        for (pattern, actual) in self.segments.iter().zip(request_segments) {
            match pattern {
                PathSegment::Literal(expected)
                    if expected == &percent_decode_path_segment(actual) => {}
                PathSegment::Literal(_) => return None,
                PathSegment::Param(name) if !actual.is_empty() => {
                    captures.insert(name.clone(), actual.to_owned());
                }
                PathSegment::Param(_) => return None,
                PathSegment::Template {
                    prefix,
                    name,
                    suffix,
                } => {
                    let captured = template_capture(actual, prefix, suffix)?;
                    captures.insert(name.clone(), captured.to_owned());
                }
            }
        }

        Some(captures)
    }

    fn path_params(&self) -> Vec<String> {
        self.segments
            .iter()
            .filter_map(|segment| match segment {
                PathSegment::Literal(_) => None,
                PathSegment::Param(name) => Some(name.clone()),
                PathSegment::Template { name, .. } => Some(name.clone()),
            })
            .collect()
    }

    fn param_count(&self) -> usize {
        self.segments
            .iter()
            .filter(|segment| {
                matches!(
                    segment,
                    PathSegment::Param(_) | PathSegment::Template { .. }
                )
            })
            .count()
    }

    fn literal_count(&self) -> usize {
        self.segments
            .iter()
            .filter(|segment| {
                matches!(
                    segment,
                    PathSegment::Literal(_) | PathSegment::Template { .. }
                )
            })
            .count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathSegment {
    Literal(String),
    Param(String),
    Template {
        prefix: String,
        name: String,
        suffix: String,
    },
}

impl PathSegment {
    fn parse(segment: &str) -> Self {
        if let Some(name) = segment
            .strip_prefix('{')
            .and_then(|segment| segment.strip_suffix('}'))
            .filter(|name| !name.is_empty())
        {
            Self::Param(name.to_owned())
        } else if let Some((prefix, name, suffix)) = parse_template_segment(segment) {
            Self::Template {
                prefix,
                name,
                suffix,
            }
        } else {
            Self::Literal(segment.to_owned())
        }
    }
}

fn parse_template_segment(segment: &str) -> Option<(String, String, String)> {
    let open = segment.find('{')?;
    let close = segment[open + 1..].find('}')? + open + 1;
    if segment[close + 1..].contains('{') {
        return None;
    }

    let name = &segment[open + 1..close];
    if name.is_empty() {
        return None;
    }

    let prefix = &segment[..open];
    let suffix = &segment[close + 1..];
    if prefix.is_empty() && suffix.is_empty() {
        return None;
    }

    Some((prefix.to_owned(), name.to_owned(), suffix.to_owned()))
}

fn template_capture<'a>(actual: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let without_prefix = actual.strip_prefix(prefix)?;
    let captured = without_prefix.strip_suffix(suffix)?;
    (!captured.is_empty()).then_some(captured)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteEntry {
    pub kind: RouteKind,
    pub resource_name: String,
    pub path_params: Vec<String>,
    pub path_param_specs: Vec<PathParamSpec>,
    pub declared_query_params: Vec<String>,
    pub query_params: Vec<QueryParamSpec>,
    pub required_auth_schemes: Vec<String>,
    pub request_body_declared: bool,
    pub request_body_schema_present: bool,
    pub request_body_schema: Option<SchemaObject>,
    pub request_schema_ref: Option<String>,
    pub request_resource_name: Option<String>,
    pub response_schema_ref: Option<String>,
    pub response_resource_name: Option<String>,
    pub matched_policy: Option<RoutingPolicy>,
    pub effective_resource_name: Option<String>,
    pub routing_evidence: Option<EvidenceSource>,
    pub confidence: Option<Confidence>,
    pub conflict: Option<String>,
    pub success_response: Option<SuccessResponse>,
    pub response_wrapper: Option<ResponseWrapper>,
    pub response_fields: Vec<String>,
    pub pagination: Option<PaginationStyle>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathParamSpec {
    pub name: String,
    pub required: bool,
    pub schema: Option<SchemaObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryParamSpec {
    pub name: String,
    pub required: bool,
    pub schema: Option<SchemaObject>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuccessResponse {
    pub status: u16,
    pub has_body: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseWrapper {
    pub array_field: String,
    pub payload_shape: ResponseWrapperPayloadShape,
    pub count_field: Option<String>,
    pub status_field: Option<String>,
    pub status_code_field: Option<String>,
    pub message_field: Option<String>,
    pub static_fields: Vec<ResponseWrapperStaticField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseWrapperPayloadShape {
    Array,
    Object,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseWrapperStaticField {
    pub name: String,
    pub value: ResponseWrapperStaticValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ResponseWrapperStaticValue {
    Null,
    Object,
    Array,
    Boolean(bool),
    Integer(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteKind {
    Create,
    ReadMany,
    ReadOne,
    Update,
    Delete,
    Refusal { detail: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaginationStyle {
    OffsetLimit,
    PageNumber,
    Cursor,
}

impl PaginationStyle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OffsetLimit => "offset_limit",
            Self::PageNumber => "page_number",
            Self::Cursor => "cursor",
        }
    }
}

impl fmt::Display for PaginationStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchedRoute<'a> {
    pub method: Method,
    pub pattern: &'a PathPattern,
    pub entry: &'a RouteEntry,
    pub path_params: BTreeMap<String, String>,
}

struct RouteBuildContext<'a> {
    catalog: &'a RestCatalog,
    config: &'a RoutingConfig,
    topology: &'a ResourceTopology,
    prefix_scoped: &'a PrefixScopedClassifier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassificationSource {
    Topology(Confidence),
    Waterfall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Classification {
    kind: RouteKind,
    matched_policy: Option<RoutingPolicy>,
    effective_resource_name: Option<String>,
    routing_evidence: Option<EvidenceSource>,
    confidence: Option<Confidence>,
    conflict: Option<String>,
    response_fields_resource_name: String,
    source: ClassificationSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InlineResponseTarget {
    resource_name: String,
    body_shape: ResponseBodyShape,
}

#[derive(Debug, Clone, Copy)]
struct RouteClassifyInput<'a> {
    catalog: &'a RestCatalog,
    path: &'a str,
    method: Method,
    pattern: &'a PathPattern,
    resource_name: &'a str,
    topology: &'a ResourceTopology,
    config: &'a RoutingConfig,
    prefix_scoped: &'a PrefixScopedClassifier,
    response_body_shape: Option<ResponseBodyShape>,
    inline_response_target: Option<&'a InlineResponseTarget>,
    draft: &'a RouteEntry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseBodyShape {
    Object,
    Array,
    ArrayWrapper,
}

#[derive(Debug, Default)]
struct RouteStats {
    topology_pinned: usize,
    topology_high: usize,
    topology_medium: usize,
    waterfall: usize,
    refused: usize,
}

impl RouteStats {
    fn record(&mut self, source: ClassificationSource, kind: &RouteKind) {
        match source {
            ClassificationSource::Topology(Confidence::Pinned) => self.topology_pinned += 1,
            ClassificationSource::Topology(Confidence::High) => self.topology_high += 1,
            ClassificationSource::Topology(Confidence::Medium) => self.topology_medium += 1,
            ClassificationSource::Topology(Confidence::Low) => self.waterfall += 1,
            ClassificationSource::Waterfall => self.waterfall += 1,
        }

        if matches!(kind, RouteKind::Refusal { .. }) {
            self.refused += 1;
        }
    }

    fn summary(&self) -> String {
        format!(
            "[rest] topology: {} pinned, {} high, {} medium | waterfall fallback: {} | refused: {}",
            self.topology_pinned,
            self.topology_high,
            self.topology_medium,
            self.waterfall,
            self.refused
        )
    }
}

pub fn build_route_registry(
    catalog: &RestCatalog,
    topology: &ResourceTopology,
    config: &RoutingConfig,
) -> RouteRegistry {
    build_route_registry_inner(catalog, topology, config)
}

pub fn build_route_registry_with_config(
    catalog: &RestCatalog,
    config: &RoutingConfig,
) -> RouteRegistry {
    let topology = super::topology::build_spec_topology(catalog, config);
    build_route_registry(catalog, &topology, config)
}

fn build_route_registry_inner(
    catalog: &RestCatalog,
    topology: &ResourceTopology,
    config: &RoutingConfig,
) -> RouteRegistry {
    let mut routes = Vec::new();
    let mut warnings = topology
        .warnings
        .iter()
        .map(|warning| {
            format!(
                "Routing topology warning for {}: {}",
                warning.path, warning.description
            )
        })
        .collect::<Vec<_>>();
    let mut stats = RouteStats::default();
    let mut paths = catalog.paths.iter().collect::<Vec<_>>();
    paths.sort_by_key(|(path, _)| path.as_str());
    let all_paths = paths
        .iter()
        .map(|(path, _)| path.as_str())
        .collect::<Vec<_>>();
    let effective_config = effective_routing_config(catalog, config);
    let prefix_scoped =
        PrefixScopedClassifier::new(&all_paths, effective_config.base_prefix.as_deref());
    let context = RouteBuildContext {
        catalog,
        config: &effective_config,
        topology,
        prefix_scoped: &prefix_scoped,
    };

    for (path, path_item) in paths {
        let source_pattern = PathPattern::parse(path);
        let mounted_path = mounted_route_path(catalog, &effective_config, path);
        let mounted_pattern = PathPattern::parse(&mounted_path);
        for (method, operation) in operations(path_item) {
            let (entry, source) = route_entry(
                &context,
                path,
                path_item,
                operation,
                method,
                &source_pattern,
            );
            stats.record(source, &entry.kind);
            if let RouteKind::Refusal { detail } = &entry.kind {
                warnings.push(detail.clone());
            }
            if let Some(style) = entry.pagination {
                warnings.push(format!(
                    "Pagination detected on {path}: {style}. REST twin returns all rows. Client may need to handle response truncation."
                ));
            }
            routes.push((method, mounted_pattern.clone(), entry.clone()));
        }
    }

    sort_routes_by_specificity(&mut routes);
    warnings.push(stats.summary());
    RouteRegistry { routes, warnings }
}

fn mounted_route_path(catalog: &RestCatalog, config: &RoutingConfig, source_path: &str) -> String {
    let Some(prefix) = selected_server_mount_prefix(catalog, config) else {
        return source_path.to_owned();
    };
    join_paths(&prefix, source_path)
}

fn selected_server_mount_prefix(catalog: &RestCatalog, config: &RoutingConfig) -> Option<String> {
    if config.server_variables.is_empty() {
        return None;
    }

    catalog
        .servers
        .iter()
        .filter_map(|server| rendered_server_path(server, &config.server_variables))
        .find(|path| !path.is_empty() && path != "/")
}

fn rendered_server_path(
    server: &super::spec::ServerObject,
    selected: &BTreeMap<String, String>,
) -> Option<String> {
    let mut rendered = server.url.clone();
    for (name, variable) in &server.variables {
        let value = selected
            .get(name)
            .map(String::as_str)
            .unwrap_or(&variable.default);
        rendered = rendered.replace(&format!("{{{name}}}"), value);
    }
    if rendered.contains('{') || rendered.contains('}') {
        return None;
    }
    let path = server_url_path(&rendered)?;
    Some(normalize_mount_prefix(path))
}

fn server_url_path(url: &str) -> Option<&str> {
    if let Some(after_scheme) = url.split_once("://").map(|(_, rest)| rest) {
        let path_start = after_scheme.find('/')?;
        return Some(&after_scheme[path_start..]);
    }

    Some(url)
}

fn normalize_mount_prefix(path: &str) -> String {
    let without_query = path
        .split_once('?')
        .map(|(head, _)| head)
        .unwrap_or(path)
        .split_once('#')
        .map(|(head, _)| head)
        .unwrap_or(path);
    let trimmed = without_query.trim_matches('/');
    if trimmed.is_empty() {
        String::from("/")
    } else {
        format!("/{trimmed}")
    }
}

fn join_paths(prefix: &str, source_path: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    let source = source_path.trim_start_matches('/');
    if prefix.is_empty() || prefix == "/" {
        format!("/{source}")
    } else if source.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix}/{source}")
    }
}

pub fn build_route_table(catalog: &RestCatalog) -> RouteTable {
    build_route_registry_with_config(catalog, &RoutingConfig::default()).routes
}

fn effective_routing_config(catalog: &RestCatalog, config: &RoutingConfig) -> RoutingConfig {
    let cli_policy = (config.policy != RoutingPolicy::Auto).then_some(config.policy);
    let mut routing = resolve_routing_config(
        cli_policy,
        config.base_prefix.clone(),
        catalog.x_twinning.as_ref(),
    );
    routing.server_variables = config.server_variables.clone();
    routing
}

pub fn match_route<'a>(
    routes: &'a RouteTable,
    method: Method,
    request_path: &str,
) -> Option<MatchedRoute<'a>> {
    routes
        .iter()
        .find_map(|(candidate_method, pattern, entry)| {
            if *candidate_method != method {
                return None;
            }

            pattern
                .captures(request_path)
                .map(|path_params| MatchedRoute {
                    method: *candidate_method,
                    pattern,
                    entry,
                    path_params,
                })
        })
}

fn route_entry(
    context: &RouteBuildContext<'_>,
    path: &str,
    path_item: &PathItem,
    operation: &OperationObject,
    method: Method,
    pattern: &PathPattern,
) -> (RouteEntry, ClassificationSource) {
    let resource_name = resource_name(pattern);
    let path_params = pattern.path_params();
    let path_param_specs = path_param_specs(context.catalog, path_item, operation, &path_params);
    let query_params = query_param_specs(context.catalog, path_item, operation);
    let declared_query_params = query_params
        .iter()
        .map(|param| param.name.clone())
        .collect::<Vec<_>>();
    let auth_schemes = required_auth_schemes(context.catalog, operation);
    let request_body_declared = operation.request_body.is_some();
    let request_body_schema = request_body_schema(context.catalog, operation).cloned();
    let request_body_schema_present = request_body_schema.is_some();
    let request_schema_ref = request_body_schema.as_ref().and_then(schema_ref);
    let request_resource_name = request_schema_ref
        .as_deref()
        .and_then(component_resource_name);
    let response_schema_ref = response_schema_ref(context.catalog, operation);
    let success_response = success_response(context.catalog, operation);
    let response_body_shape = response_body_shape(context.catalog, operation);
    let inline_response_target =
        inline_response_target(context.catalog, operation, &resource_name, pattern);
    let response_resource_name = response_schema_ref
        .as_deref()
        .and_then(component_resource_name);

    let draft = RouteEntry {
        kind: route_refusal(path),
        resource_name: resource_name.clone(),
        path_params: path_params.clone(),
        path_param_specs: path_param_specs.clone(),
        declared_query_params: declared_query_params.clone(),
        query_params: query_params.clone(),
        required_auth_schemes: auth_schemes.clone(),
        request_body_declared,
        request_body_schema_present,
        request_body_schema: request_body_schema.clone(),
        request_schema_ref: request_schema_ref.clone(),
        request_resource_name: request_resource_name.clone(),
        response_schema_ref: response_schema_ref.clone(),
        response_resource_name: response_resource_name.clone(),
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        success_response,
        response_wrapper: None,
        response_fields: Vec::new(),
        pagination: None,
    };
    let mut classification = classify_route(RouteClassifyInput {
        catalog: context.catalog,
        path,
        method,
        pattern,
        resource_name: &resource_name,
        topology: context.topology,
        config: context.config,
        prefix_scoped: context.prefix_scoped,
        response_body_shape,
        inline_response_target: inline_response_target.as_ref(),
        draft: &draft,
    });
    if let Some(request_body_refusal) =
        unsupported_request_body_refusal(context.catalog, operation, path, method)
    {
        classification = request_body_refusal;
    } else if let Some(target_refusal) = request_target_required_fields_refusal(
        context.catalog,
        path,
        &classification,
        request_schema_ref.as_deref(),
        request_resource_name.as_deref(),
    ) {
        classification = target_refusal;
    } else if matches!(classification.kind, RouteKind::Refusal { .. })
        && response_schema_ref.is_none()
        && matches!(
            response_body_shape,
            Some(ResponseBodyShape::Object | ResponseBodyShape::ArrayWrapper)
        )
    {
        classification = inline_response_schema_refusal(path, response_body_shape);
    } else if response_has_scalar_resource(
        context.catalog,
        response_resource_name.as_deref(),
        success_response,
    ) {
        classification = scalar_response_refusal(path, response_resource_name.as_deref());
    } else if !matches!(classification.kind, RouteKind::Refusal { .. })
        && response_resource_is_object_map(context.catalog, response_resource_name.as_deref())
    {
        classification =
            object_map_response_refusal(context.catalog, path, response_resource_name.as_deref());
    } else if let Some(non_crud_refusal) =
        non_crud_read_response_refusal(path, method, pattern, &classification, response_body_shape)
    {
        classification = non_crud_refusal;
    } else if let Some(path_lookup_refusal) =
        no_primary_key_read_one_path_lookup_refusal(context.catalog, path, pattern, &classification)
    {
        classification = path_lookup_refusal;
    } else if !matches!(classification.kind, RouteKind::Refusal { .. })
        && is_mutation_method(method)
        && let Some(fields) = unsupported_response_returning_fields(
            context.catalog,
            response_resource_name.as_deref(),
            success_response,
        )
    {
        classification =
            unsupported_response_fields_refusal(path, response_resource_name.as_deref(), &fields);
    } else if let Some(action_refusal) = no_body_action_response_refusal(
        context.catalog,
        path,
        method,
        pattern,
        &classification,
        request_body_declared,
        response_resource_name.as_deref(),
    ) {
        classification = action_refusal;
    }
    let pagination = if method == Method::Get && classification.kind == RouteKind::ReadMany {
        pagination_style(&declared_query_params)
    } else {
        None
    };
    let response_fields = response_fields(
        context.catalog,
        &classification.response_fields_resource_name,
        response_schema_ref.as_deref(),
    );
    let response_wrapper = response_wrapper(
        context.catalog,
        operation,
        response_schema_ref.as_deref(),
        classification.effective_resource_name.as_deref(),
        &resource_name,
        pattern,
    );
    let source = classification.source;

    (
        RouteEntry {
            kind: classification.kind,
            resource_name,
            path_params,
            path_param_specs,
            declared_query_params,
            query_params,
            required_auth_schemes: auth_schemes,
            request_body_declared,
            request_body_schema_present,
            request_body_schema,
            request_schema_ref,
            request_resource_name,
            response_schema_ref,
            response_resource_name,
            matched_policy: classification.matched_policy,
            effective_resource_name: classification.effective_resource_name,
            routing_evidence: classification.routing_evidence,
            confidence: classification.confidence,
            conflict: classification.conflict,
            success_response,
            response_wrapper,
            response_fields,
            pagination,
        },
        source,
    )
}

fn required_auth_schemes(catalog: &RestCatalog, operation: &OperationObject) -> Vec<String> {
    let requirements = operation
        .security
        .as_deref()
        .unwrap_or(catalog.security_requirements.as_slice());
    security_requirement_scheme_names(requirements)
}

fn security_requirement_scheme_names(requirements: &[SecurityRequirement]) -> Vec<String> {
    requirements
        .iter()
        .flat_map(|requirement| requirement.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn strip_base_prefix_pattern(
    pattern: &PathPattern,
    base_prefix: Option<&str>,
) -> Option<PathPattern> {
    let base_prefix = base_prefix?;
    let prefix_pattern = PathPattern::parse(base_prefix);
    if prefix_pattern.segments.is_empty() {
        return Some(pattern.clone());
    }

    if prefix_pattern.segments.len() > pattern.segments.len() {
        return None;
    }

    pattern
        .segments
        .starts_with(&prefix_pattern.segments)
        .then(|| PathPattern {
            segments: pattern.segments[prefix_pattern.segments.len()..].to_vec(),
        })
}

fn classify_route(input: RouteClassifyInput<'_>) -> Classification {
    if let Some(topology_entry) = input.topology.path_bindings.get(input.path)
        && confidence_reaches_classifier(topology_entry.confidence)
        && topology_entry_matches_pattern(input.topology, input.pattern, topology_entry)
    {
        return classify_from_topology(input, topology_entry);
    }

    classify_with_waterfall(input)
}

fn classify_from_topology(input: RouteClassifyInput<'_>, entry: &TopologyEntry) -> Classification {
    let relationship_pattern = topology_relationship_pattern(input.topology, input.pattern);
    let kind = route_kind_from_relationship(
        input.path,
        input.method,
        route_relationship_for_response_shape(
            input.method,
            entry.relationship,
            input.response_body_shape,
            &relationship_pattern,
            &entry.resource_name,
        ),
    );
    let matched_policy = (!matches!(kind, RouteKind::Refusal { .. }))
        .then(|| routing_policy_from_evidence(entry.winning_evidence, input.config));
    let routing_evidence = matched_policy.map(|_| entry.winning_evidence);
    let effective_resource_name = matched_policy.map(|_| entry.resource_name.clone());
    let response_fields_resource_name = effective_resource_name
        .clone()
        .unwrap_or_else(|| entry.resource_name.clone());

    Classification {
        kind,
        matched_policy,
        effective_resource_name,
        routing_evidence,
        confidence: Some(entry.confidence),
        conflict: entry.conflict.clone(),
        response_fields_resource_name,
        source: ClassificationSource::Topology(entry.confidence),
    }
}

fn topology_relationship_pattern(
    topology: &ResourceTopology,
    pattern: &PathPattern,
) -> PathPattern {
    topology
        .prefix_scopes
        .iter()
        .filter_map(|scope| {
            let prefix_pattern = PathPattern::parse(&scope.prefix);
            pattern
                .segments
                .starts_with(&prefix_pattern.segments)
                .then_some((prefix_pattern.segments.len(), scope.prefix.as_str()))
        })
        .max_by_key(|(prefix_len, _)| *prefix_len)
        .and_then(|(_, prefix)| strip_base_prefix_pattern(pattern, Some(prefix)))
        .unwrap_or_else(|| pattern.clone())
}

fn route_relationship_for_response_shape(
    method: Method,
    relationship: ResourceRelationship,
    response_body_shape: Option<ResponseBodyShape>,
    pattern: &PathPattern,
    resource_name: &str,
) -> ResourceRelationship {
    match (method, relationship, response_body_shape) {
        (
            Method::Get,
            ResourceRelationship::RootSingleton,
            Some(ResponseBodyShape::Array | ResponseBodyShape::ArrayWrapper),
        ) => ResourceRelationship::RootCollection,
        (
            Method::Get,
            ResourceRelationship::ChildSingleton,
            Some(ResponseBodyShape::Array | ResponseBodyShape::ArrayWrapper),
        ) => ResourceRelationship::ChildCollection,
        (Method::Get, ResourceRelationship::ChildCollection, Some(ResponseBodyShape::Object))
            if !terminal_literal_matches_resource(pattern, resource_name) =>
        {
            ResourceRelationship::ChildSingleton
        }
        (Method::Get, ResourceRelationship::RootCollection, Some(ResponseBodyShape::Object))
            if root_object_response_should_be_singleton(pattern, resource_name) =>
        {
            ResourceRelationship::RootSingleton
        }
        _ => relationship,
    }
}

fn root_object_response_should_be_singleton(pattern: &PathPattern, resource_name: &str) -> bool {
    if terminal_param_without_parent_scope(pattern) {
        return true;
    }

    if pattern.path_params().is_empty() && pattern.segments.len() > 1 {
        return true;
    }

    let [PathSegment::Literal(path_resource)] = pattern.segments.as_slice() else {
        return false;
    };

    let path_hint = canonical_path_resource_hint(path_resource);
    let resource_hint = canonical_response_wrapper_name(resource_name);
    if path_hint.is_empty() || resource_hint.is_empty() {
        return false;
    }

    path_hint != resource_hint
        && path_hint != trim_plural_suffix(&resource_hint)
        && trim_plural_suffix(&path_hint) != trim_plural_suffix(&resource_hint)
}

fn terminal_param_without_parent_scope(pattern: &PathPattern) -> bool {
    let [prefix @ .., PathSegment::Literal(_), terminal] = pattern.segments.as_slice() else {
        return false;
    };
    if !matches!(
        terminal,
        PathSegment::Param(_) | PathSegment::Template { .. }
    ) {
        return false;
    }

    prefix.iter().all(|segment| {
        !matches!(
            segment,
            PathSegment::Param(_) | PathSegment::Template { .. }
        )
    })
}

fn canonical_path_resource_hint(value: &str) -> String {
    let without_extension = value.split_once('.').map(|(head, _)| head).unwrap_or(value);
    canonical_response_wrapper_name(without_extension)
}

fn classify_with_waterfall(input: RouteClassifyInput<'_>) -> Classification {
    let policies = waterfall_policies(input.config.policy);
    for policy in policies {
        if let Some(classification) = classify_with_policy(input, policy) {
            return classification;
        }
    }

    if !input.catalog.resources.contains_key(input.resource_name) {
        return refusal_classification(input.path);
    }

    let kind = route_kind_from_crud_pattern(input.path, input.method, input.pattern);
    if matches!(kind, RouteKind::Refusal { .. }) {
        refusal_classification(input.path)
    } else {
        Classification {
            kind,
            matched_policy: Some(RoutingPolicy::FlatCrud),
            effective_resource_name: Some(input.resource_name.to_owned()),
            routing_evidence: Some(EvidenceSource::FlatCrud),
            confidence: None,
            conflict: None,
            response_fields_resource_name: input.resource_name.to_owned(),
            source: ClassificationSource::Waterfall,
        }
    }
}

fn classify_with_policy(
    input: RouteClassifyInput<'_>,
    policy: RoutingPolicy,
) -> Option<Classification> {
    let (resource_name, policy_pattern) = match policy {
        RoutingPolicy::PrefixScoped => {
            let resource_name = input.prefix_scoped.classify(input.path, input.catalog)?;
            let policy_pattern = strip_base_prefix_pattern(
                input.pattern,
                Some(input.prefix_scoped.effective_prefix.as_str()),
            )
            .unwrap_or_else(|| input.pattern.clone());
            (resource_name, policy_pattern)
        }
        RoutingPolicy::FlatCrud => {
            let resource_name = flat_crud_classify(
                input.path,
                input.catalog,
                input.config.base_prefix.as_deref(),
            )?;
            let policy_pattern =
                strip_base_prefix_pattern(input.pattern, input.config.base_prefix.as_deref())
                    .unwrap_or_else(|| input.pattern.clone());
            (resource_name, policy_pattern)
        }
        RoutingPolicy::SchemaFirst => {
            let resource_name =
                schema_first_classify(input.draft, input.catalog).or_else(|| {
                    input
                        .inline_response_target
                        .map(|target| target.resource_name.clone())
                })?;
            (resource_name, input.pattern.clone())
        }
        RoutingPolicy::Auto => return None,
    };

    let kind = if policy == RoutingPolicy::SchemaFirst {
        let schema_first_shape = input
            .inline_response_target
            .map(|target| target.body_shape)
            .or(input.response_body_shape);
        route_kind_from_schema_first_response(
            input.path,
            input.method,
            &policy_pattern,
            schema_first_shape,
            &resource_name,
        )
    } else {
        route_kind_from_crud_pattern(input.path, input.method, &policy_pattern)
    };
    if matches!(kind, RouteKind::Refusal { .. }) {
        return None;
    }

    Some(Classification {
        kind,
        matched_policy: Some(policy),
        effective_resource_name: Some(resource_name.clone()),
        routing_evidence: Some(evidence_from_routing_policy(policy)),
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name,
        source: ClassificationSource::Waterfall,
    })
}

fn route_kind_from_relationship(
    path: &str,
    method: Method,
    relationship: ResourceRelationship,
) -> RouteKind {
    match relationship {
        ResourceRelationship::RootCollection | ResourceRelationship::ChildCollection => {
            match method {
                Method::Get => RouteKind::ReadMany,
                Method::Post => RouteKind::Create,
                Method::Head | Method::Put | Method::Patch | Method::Delete => route_refusal(path),
            }
        }
        ResourceRelationship::RootSingleton | ResourceRelationship::ChildSingleton => {
            match method {
                Method::Get => RouteKind::ReadOne,
                Method::Put | Method::Patch => RouteKind::Update,
                Method::Delete => RouteKind::Delete,
                Method::Head | Method::Post => route_refusal(path),
            }
        }
    }
}

fn route_kind_from_crud_pattern(path: &str, method: Method, pattern: &PathPattern) -> RouteKind {
    match (method, pattern.segments.as_slice()) {
        (Method::Post, [PathSegment::Literal(_resource)]) => RouteKind::Create,
        (Method::Get, [PathSegment::Literal(_resource)]) => RouteKind::ReadMany,
        (Method::Get, [PathSegment::Literal(_resource), PathSegment::Param(_id)]) => {
            RouteKind::ReadOne
        }
        (
            Method::Put | Method::Patch,
            [PathSegment::Literal(_resource), PathSegment::Param(_id)],
        ) => RouteKind::Update,
        (Method::Delete, [PathSegment::Literal(_resource), PathSegment::Param(_id)]) => {
            RouteKind::Delete
        }
        _ => route_refusal(path),
    }
}

fn route_kind_from_schema_first_response(
    path: &str,
    method: Method,
    pattern: &PathPattern,
    response_body_shape: Option<ResponseBodyShape>,
    resource_name: &str,
) -> RouteKind {
    if method == Method::Get
        && pattern.path_params().is_empty()
        && matches!(response_body_shape, Some(ResponseBodyShape::Object))
        && root_object_response_should_be_singleton(pattern, resource_name)
    {
        return RouteKind::ReadOne;
    }

    let crud_kind = route_kind_from_crud_pattern(path, method, pattern);
    if !matches!(crud_kind, RouteKind::Refusal { .. }) {
        return crud_kind;
    }

    if method != Method::Get || !pattern.path_params().is_empty() {
        return crud_kind;
    }

    match response_body_shape {
        Some(ResponseBodyShape::Array | ResponseBodyShape::ArrayWrapper) => RouteKind::ReadMany,
        Some(ResponseBodyShape::Object) => RouteKind::ReadOne,
        None => crud_kind,
    }
}

fn confidence_reaches_classifier(confidence: Confidence) -> bool {
    matches!(
        confidence,
        Confidence::Pinned | Confidence::High | Confidence::Medium
    )
}

fn topology_entry_matches_pattern(
    topology: &ResourceTopology,
    pattern: &PathPattern,
    entry: &TopologyEntry,
) -> bool {
    match entry.winning_evidence {
        EvidenceSource::SchemaFirst | EvidenceSource::XTwinning => true,
        EvidenceSource::FlatCrud | EvidenceSource::PrefixScoped | EvidenceSource::Waterfall => {
            relationship_matches_pattern(topology, pattern, entry.relationship)
        }
    }
}

fn relationship_matches_pattern(
    topology: &ResourceTopology,
    pattern: &PathPattern,
    relationship: ResourceRelationship,
) -> bool {
    relationship_matches_suffix(&pattern.segments, relationship)
        || topology.prefix_scopes.iter().any(|scope| {
            let prefix = PathPattern::parse(&scope.prefix);
            pattern.segments.starts_with(&prefix.segments)
                && relationship_matches_suffix(
                    &pattern.segments[prefix.segments.len()..],
                    relationship,
                )
        })
}

fn relationship_matches_suffix(
    segments: &[PathSegment],
    relationship: ResourceRelationship,
) -> bool {
    matches!(
        (relationship, segments),
        (
            ResourceRelationship::RootCollection,
            [PathSegment::Literal(_)]
        ) | (
            ResourceRelationship::RootSingleton,
            [PathSegment::Literal(_), PathSegment::Param(_)]
        ) | (
            ResourceRelationship::ChildCollection,
            [
                PathSegment::Literal(_),
                PathSegment::Param(_),
                PathSegment::Literal(_),
            ],
        ) | (
            ResourceRelationship::ChildSingleton,
            [
                PathSegment::Literal(_),
                PathSegment::Param(_),
                PathSegment::Literal(_),
                PathSegment::Param(_),
            ],
        )
    )
}

fn routing_policy_from_evidence(evidence: EvidenceSource, config: &RoutingConfig) -> RoutingPolicy {
    match evidence {
        EvidenceSource::FlatCrud | EvidenceSource::Waterfall => RoutingPolicy::FlatCrud,
        EvidenceSource::SchemaFirst => RoutingPolicy::SchemaFirst,
        EvidenceSource::PrefixScoped => RoutingPolicy::PrefixScoped,
        EvidenceSource::XTwinning if config.policy != RoutingPolicy::Auto => config.policy,
        EvidenceSource::XTwinning if config.base_prefix.is_some() => RoutingPolicy::PrefixScoped,
        EvidenceSource::XTwinning => RoutingPolicy::Auto,
    }
}

fn evidence_from_routing_policy(policy: RoutingPolicy) -> EvidenceSource {
    match policy {
        RoutingPolicy::FlatCrud => EvidenceSource::FlatCrud,
        RoutingPolicy::SchemaFirst => EvidenceSource::SchemaFirst,
        RoutingPolicy::PrefixScoped => EvidenceSource::PrefixScoped,
        RoutingPolicy::Auto => EvidenceSource::Waterfall,
    }
}

fn waterfall_policies(policy: RoutingPolicy) -> Vec<RoutingPolicy> {
    match policy {
        RoutingPolicy::Auto => vec![
            RoutingPolicy::PrefixScoped,
            RoutingPolicy::FlatCrud,
            RoutingPolicy::SchemaFirst,
        ],
        other => vec![other],
    }
}

fn refusal_classification(path: &str) -> Classification {
    Classification {
        kind: route_refusal(path),
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    }
}

fn inline_response_schema_refusal(
    path: &str,
    response_body_shape: Option<ResponseBodyShape>,
) -> Classification {
    let shape = match response_body_shape {
        Some(ResponseBodyShape::ArrayWrapper) => "inline response object wrapper schema",
        Some(ResponseBodyShape::Object) => "inline response object schema",
        Some(ResponseBodyShape::Array) | None => "inline response schema",
    };

    Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route uses an {shape}, which is not materialized as an executable REST resource in v0. Path: {path}"
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    }
}

fn non_crud_read_response_refusal(
    path: &str,
    method: Method,
    pattern: &PathPattern,
    classification: &Classification,
    response_body_shape: Option<ResponseBodyShape>,
) -> Option<Classification> {
    if method != Method::Get || classification.kind != RouteKind::ReadOne {
        return None;
    }

    if path_contains_literal(pattern, "watch") {
        return Some(non_crud_refusal_classification(
            path,
            "watch/stream response",
        ));
    }

    if response_body_shape != Some(ResponseBodyShape::Object)
        || !pattern.path_params().is_empty()
        || pattern.segments.len() <= 1
    {
        return None;
    }

    let effective_resource = classification.effective_resource_name.as_deref()?;
    (!terminal_literal_matches_resource(pattern, effective_resource))
        .then(|| non_crud_refusal_classification(path, "static discovery-style object response"))
}

fn non_crud_refusal_classification(path: &str, shape: &str) -> Classification {
    Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route declares a non-CRUD {shape}, which is not materialized as an executable REST resource in v0. Path: {path}"
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    }
}

fn no_primary_key_read_one_path_lookup_refusal(
    catalog: &RestCatalog,
    path: &str,
    pattern: &PathPattern,
    classification: &Classification,
) -> Option<Classification> {
    if classification.kind != RouteKind::ReadOne || pattern.path_params().is_empty() {
        return None;
    }

    let effective_resource = classification.effective_resource_name.as_deref()?;
    let resource = catalog.resources.get(effective_resource)?;
    if resource.primary_key.is_some() {
        return None;
    }

    let path_params = pattern.path_params();
    if path_params.len() != 1 {
        return None;
    }

    if path_params
        .iter()
        .any(|path_param| path_param_matches_declared_column(resource, path_param))
        || identity_alias_path_param_can_materialize_required_scalar(resource, &path_params)
    {
        return None;
    }

    Some(no_primary_key_path_lookup_refusal(
        path,
        effective_resource,
        String::from("no path parameter can materialize a declared column"),
    ))
}

fn no_primary_key_path_lookup_refusal(
    path: &str,
    effective_resource_name: &str,
    reason: String,
) -> Classification {
    Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route requires path lookup on resource `{effective_resource_name}`, but the resource has no primary key and {reason}. Path: {path}"
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    }
}

fn path_param_matches_declared_column(resource: &ResourceSchema, path_param: &str) -> bool {
    resource
        .columns
        .iter()
        .any(|column| path_param_can_materialize_column(resource, path_param, &column.name))
}

fn identity_alias_path_param_can_materialize_required_scalar(
    resource: &ResourceSchema,
    path_params: &[String],
) -> bool {
    let [path_param] = path_params else {
        return false;
    };

    matches!(
        canonical_response_wrapper_name(path_param).as_str(),
        "pk" | "key"
    ) && sole_required_scalar_column(resource).is_some()
}

fn sole_required_scalar_column(resource: &ResourceSchema) -> Option<&ResourceColumn> {
    let columns = resource
        .required
        .iter()
        .filter_map(|field| resource.columns.iter().find(|column| column.name == *field))
        .filter(|column| is_rest_returning_type(&column.normalized_type))
        .collect::<Vec<_>>();

    match columns.as_slice() {
        [column] => Some(*column),
        _ => None,
    }
}

fn path_contains_literal(pattern: &PathPattern, literal: &str) -> bool {
    pattern.segments.iter().any(|segment| match segment {
        PathSegment::Literal(value) => value == literal,
        PathSegment::Param(_) | PathSegment::Template { .. } => false,
    })
}

fn terminal_literal_matches_resource(pattern: &PathPattern, resource_name: &str) -> bool {
    let Some(terminal) = last_literal_segment(pattern) else {
        return false;
    };

    let terminal_hint = canonical_path_resource_hint(terminal);
    let resource_hint = canonical_response_wrapper_name(resource_name);
    !terminal_hint.is_empty()
        && !resource_hint.is_empty()
        && (terminal_hint == resource_hint
            || terminal_hint == trim_plural_suffix(&resource_hint)
            || trim_plural_suffix(&terminal_hint) == trim_plural_suffix(&resource_hint))
}

fn response_resource_is_object_map(catalog: &RestCatalog, resource_name: Option<&str>) -> bool {
    resource_name
        .and_then(|resource_name| catalog.resources.get(resource_name))
        .and_then(|resource| resource.meta.object_map_value_resource.as_ref())
        .is_some()
}

fn response_has_scalar_resource(
    catalog: &RestCatalog,
    resource_name: Option<&str>,
    success_response: Option<SuccessResponse>,
) -> bool {
    if !success_response.is_some_and(|response| response.has_body) {
        return false;
    }

    resource_name
        .and_then(|resource_name| catalog.resources.get(resource_name))
        .is_some_and(|resource| {
            resource.columns.is_empty()
                && resource.meta.item_resource.is_none()
                && resource.meta.object_map_value_resource.is_none()
        })
}

fn object_map_response_refusal(
    catalog: &RestCatalog,
    path: &str,
    response_resource_name: Option<&str>,
) -> Classification {
    let schema = response_resource_name
        .and_then(|resource_name| catalog.resources.get(resource_name))
        .map(|resource| resource.schema_name.as_str())
        .unwrap_or("<unknown>");

    Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route uses object-map response schema `{schema}`, which is not materialized as executable REST resource rows in v0. Path: {path}"
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    }
}

fn scalar_response_refusal(path: &str, response_resource_name: Option<&str>) -> Classification {
    let resource = response_resource_name.unwrap_or("<unknown>");
    Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route declares scalar response resource `{resource}`, which is not materialized as an executable REST response in v0. Path: {path}"
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    }
}

fn unsupported_response_fields_refusal(
    path: &str,
    response_resource_name: Option<&str>,
    fields: &[String],
) -> Classification {
    let resource = response_resource_name.unwrap_or("<unknown>");
    Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route response resource `{resource}` declares unsupported response field(s) [{}]; REST twin v0 cannot materialize nested, array, or binary response fields without returning a partial body. Path: {path}",
                fields.join(", ")
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    }
}

fn unsupported_response_returning_fields(
    catalog: &RestCatalog,
    response_resource_name: Option<&str>,
    success_response: Option<SuccessResponse>,
) -> Option<Vec<String>> {
    if !success_response.is_some_and(|response| response.has_body) {
        return None;
    }

    let resource = response_resource_name.and_then(|name| catalog.resources.get(name))?;
    let fields = resource
        .columns
        .iter()
        .filter(|column| !is_rest_returning_type(&column.normalized_type))
        .map(|column| format!("{} ({})", column.name, column.normalized_type))
        .collect::<Vec<_>>();

    (!fields.is_empty()).then_some(fields)
}

fn no_body_action_response_refusal(
    catalog: &RestCatalog,
    path: &str,
    method: Method,
    pattern: &PathPattern,
    classification: &Classification,
    request_body_declared: bool,
    response_resource_name: Option<&str>,
) -> Option<Classification> {
    if method != Method::Post
        || request_body_declared
        || classification.kind != RouteKind::Create
        || !action_style_post_pattern(pattern)
    {
        return None;
    }

    let response_resource = response_resource_name?;
    let resource = catalog.resources.get(response_resource)?;
    if terminal_literal_looks_like_response_resource(pattern, resource) {
        return None;
    }

    let generated_fields = resource
        .columns
        .iter()
        .filter(|column| {
            !column.nullable
                && is_rest_returning_type(&column.normalized_type)
                && is_generated_secret_response_field(&column.name)
        })
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if generated_fields.is_empty() {
        let required_fields = unsynthesizable_no_body_action_fields(resource, pattern);
        if required_fields.is_empty() {
            return None;
        }

        return Some(Classification {
            kind: RouteKind::Refusal {
                detail: format!(
                    "REST route is an action-style POST with no request body and required response field(s) [{}] on resource `{response_resource}`; REST twin v0 cannot synthesize those values. Path: {path}",
                    required_fields.join(", ")
                ),
            },
            matched_policy: None,
            effective_resource_name: None,
            routing_evidence: None,
            confidence: None,
            conflict: None,
            response_fields_resource_name: resource_name(&PathPattern::parse(path)),
            source: ClassificationSource::Waterfall,
        });
    }

    Some(Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route is an action-style POST with no request body and generated response field(s) [{}] on resource `{response_resource}`; REST twin v0 cannot synthesize those values. Path: {path}",
                generated_fields.join(", ")
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    })
}

fn terminal_literal_looks_like_response_resource(
    pattern: &PathPattern,
    resource: &ResourceSchema,
) -> bool {
    let Some(terminal) = terminal_literal_segment(pattern) else {
        return false;
    };
    let terminal = canonical_path_resource_hint(terminal);
    if terminal.is_empty() {
        return false;
    }

    response_resource_keys(resource)
        .iter()
        .any(|resource_key| resource_key_matches_terminal_hint(resource_key, &terminal))
}

fn terminal_literal_segment(pattern: &PathPattern) -> Option<&str> {
    match pattern.segments.last()? {
        PathSegment::Literal(value) => Some(value.as_str()),
        PathSegment::Param(_) | PathSegment::Template { .. } => None,
    }
}

fn response_resource_keys(resource: &ResourceSchema) -> Vec<String> {
    let mut keys = Vec::new();
    push_unique_key(
        &mut keys,
        canonical_response_wrapper_name(&resource.schema_name),
    );
    push_unique_key(
        &mut keys,
        canonical_response_wrapper_name(&resource.resource_name),
    );
    push_unique_key(
        &mut keys,
        trim_plural_suffix(&canonical_response_wrapper_name(&resource.resource_name)).to_owned(),
    );
    keys
}

fn resource_key_matches_terminal_hint(resource_key: &str, terminal: &str) -> bool {
    terminal_hint_variants(terminal).iter().any(|variant| {
        resource_key == variant
            || trim_plural_suffix(resource_key) == trim_plural_suffix(variant)
            || resource_key.ends_with(variant)
            || trim_plural_suffix(resource_key).ends_with(trim_plural_suffix(variant))
    })
}

fn terminal_hint_variants(terminal: &str) -> Vec<String> {
    let mut variants = Vec::new();
    push_unique_key(&mut variants, terminal.to_owned());
    if terminal == "meta" {
        push_unique_key(&mut variants, String::from("metadata"));
    }
    variants
}

fn unsynthesizable_no_body_action_fields(
    resource: &ResourceSchema,
    pattern: &PathPattern,
) -> Vec<String> {
    resource
        .columns
        .iter()
        .filter(|column| {
            !column.nullable
                && is_rest_returning_type(&column.normalized_type)
                && !path_params_can_materialize_column(resource, pattern, &column.name)
        })
        .map(|column| column.name.clone())
        .collect()
}

fn path_params_can_materialize_column(
    resource: &ResourceSchema,
    pattern: &PathPattern,
    column_name: &str,
) -> bool {
    pattern
        .path_params()
        .iter()
        .any(|param| path_param_can_materialize_column(resource, param, column_name))
}

fn path_param_can_materialize_column(
    resource: &ResourceSchema,
    path_param: &str,
    column_name: &str,
) -> bool {
    let column_key = canonical_response_wrapper_name(column_name);
    let param_key = canonical_response_wrapper_name(path_param);
    if param_key == column_key {
        return true;
    }

    path_param_resource_prefixes(resource).iter().any(|prefix| {
        param_key
            .strip_prefix(prefix)
            .is_some_and(|stripped| stripped == column_key)
    })
}

fn path_param_resource_prefixes(resource: &ResourceSchema) -> Vec<String> {
    let mut prefixes = Vec::new();
    push_unique_key(
        &mut prefixes,
        canonical_response_wrapper_name(&resource.schema_name),
    );
    push_unique_key(
        &mut prefixes,
        trim_plural_suffix(&canonical_response_wrapper_name(&resource.resource_name)).to_owned(),
    );
    prefixes
}

fn push_unique_key(keys: &mut Vec<String>, key: String) {
    if !key.is_empty() && !keys.iter().any(|seen| seen == &key) {
        keys.push(key);
    }
}

fn is_generated_secret_response_field(field: &str) -> bool {
    let field = canonical_response_wrapper_name(field);
    matches!(field.as_str(), "apikey" | "clientkey" | "hmackey")
}

fn action_style_post_pattern(pattern: &PathPattern) -> bool {
    if pattern.segments.len() <= 1 {
        return false;
    }

    matches!(
        pattern.segments.last(),
        Some(PathSegment::Literal(_) | PathSegment::Template { .. })
    )
}

fn is_mutation_method(method: Method) -> bool {
    matches!(
        method,
        Method::Post | Method::Put | Method::Patch | Method::Delete
    )
}

fn unsupported_request_body_refusal(
    catalog: &RestCatalog,
    operation: &OperationObject,
    path: &str,
    method: Method,
) -> Option<Classification> {
    if !matches!(method, Method::Post | Method::Put | Method::Patch) {
        return None;
    }

    let request_body = resolved_request_body(catalog, operation)?;
    if request_body_declares_supported_content(request_body) {
        if let Some(kind) = unsupported_json_request_body_top_level_kind(request_body) {
            return Some(Classification {
                kind: RouteKind::Refusal {
                    detail: format!(
                        "REST route declares top-level JSON request body schema type `{kind}`, which REST twin v0 cannot materialize from request bodies. Path: {path}"
                    ),
                },
                matched_policy: None,
                effective_resource_name: None,
                routing_evidence: None,
                confidence: None,
                conflict: None,
                response_fields_resource_name: resource_name(&PathPattern::parse(path)),
                source: ClassificationSource::Waterfall,
            });
        }
        return None;
    }

    let content_types = request_body_content_types(request_body);
    let content_detail = if content_types.is_empty() {
        String::from("<none>")
    } else {
        content_types.join(", ")
    };

    Some(Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route declares unsupported request body content type(s) [{content_detail}]; REST twin v0 only supports {SUPPORTED_REQUEST_BODY_CONTENT_TYPES} request bodies. Path: {path}"
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: resource_name(&PathPattern::parse(path)),
        source: ClassificationSource::Waterfall,
    })
}

fn unsupported_json_request_body_top_level_kind(
    request_body: &RequestBodyObject,
) -> Option<String> {
    let schema = request_body_json_schema(request_body)?;
    if schema.reference.is_some() || schema_declares_object(schema) {
        return None;
    }

    if schema_declares_array(schema) {
        return Some(String::from("array"));
    }

    let kind = schema_type_string(schema)?;
    (!kind.is_empty()).then_some(kind)
}

fn request_target_required_fields_refusal(
    catalog: &RestCatalog,
    path: &str,
    classification: &Classification,
    request_schema_ref: Option<&str>,
    request_resource_name: Option<&str>,
) -> Option<Classification> {
    if classification.kind != RouteKind::Create {
        return None;
    }

    let target_resource_name = classification.effective_resource_name.as_deref()?;
    let request_resource_name = request_resource_name?;
    if request_resource_name == target_resource_name {
        return None;
    }

    let target_resource = catalog.resources.get(target_resource_name)?;
    let request_resource = catalog.resources.get(request_resource_name)?;
    let missing = target_resource
        .columns
        .iter()
        .filter(|column| {
            !column.nullable && !resource_declares_body_field(request_resource, &column.name)
        })
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();

    if missing.is_empty() {
        return None;
    }

    let request_schema = request_schema_ref.unwrap_or("<request body schema>");
    Some(Classification {
        kind: RouteKind::Refusal {
            detail: format!(
                "REST route request body schema `{request_schema}` omits required target column(s) [{}] for resource `{}`; REST twin v0 cannot synthesize server-generated or default values. Path: {path}",
                missing.join(", "),
                target_resource.resource_name
            ),
        },
        matched_policy: None,
        effective_resource_name: None,
        routing_evidence: None,
        confidence: None,
        conflict: None,
        response_fields_resource_name: classification.response_fields_resource_name.clone(),
        source: ClassificationSource::Waterfall,
    })
}

fn resource_declares_body_field(resource: &ResourceSchema, field: &str) -> bool {
    if resource.columns.iter().any(|column| column.name == field) {
        return true;
    }

    let mut matches = resource
        .columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(field));
    matches.next().is_some() && matches.next().is_none()
}

fn route_refusal(path: &str) -> RouteKind {
    RouteKind::Refusal {
        detail: format!("REST route is outside the CRUD route subset in v0. Path: {path}"),
    }
}

fn resource_name(pattern: &PathPattern) -> String {
    pattern
        .segments
        .first()
        .map(|segment| match segment {
            PathSegment::Literal(value) | PathSegment::Param(value) => value.clone(),
            PathSegment::Template { name, .. } => name.clone(),
        })
        .unwrap_or_default()
}

fn path_param_specs(
    catalog: &RestCatalog,
    path_item: &PathItem,
    operation: &OperationObject,
    template_path_params: &[String],
) -> Vec<PathParamSpec> {
    let mut seen = BTreeSet::new();
    let template_path_params = template_path_params.iter().collect::<BTreeSet<_>>();
    path_item
        .parameters
        .iter()
        .chain(operation.parameters.iter())
        .filter_map(|parameter| resolve_parameter_object(catalog, parameter))
        .filter(|parameter| parameter.location.as_deref() == Some("path"))
        .filter_map(|parameter| {
            let name = parameter.name.as_ref()?;
            if !template_path_params.contains(name) {
                return None;
            }
            seen.insert(name.clone()).then(|| PathParamSpec {
                name: name.clone(),
                required: parameter.required.unwrap_or(true),
                schema: parameter
                    .schema
                    .as_ref()
                    .map(|schema| resolve_param_schema(catalog, schema)),
            })
        })
        .collect()
}

fn query_param_specs(
    catalog: &RestCatalog,
    path_item: &PathItem,
    operation: &OperationObject,
) -> Vec<QueryParamSpec> {
    let mut seen = BTreeSet::new();
    let mut params = Vec::new();

    for scheme in &catalog.security_schemes {
        if scheme.location.as_deref() == Some("query")
            && let Some(name) = &scheme.parameter_name
            && seen.insert(name.clone())
        {
            params.push(QueryParamSpec {
                name: name.clone(),
                required: false,
                schema: None,
            });
        }
    }

    params.extend(
        path_item
            .parameters
            .iter()
            .chain(operation.parameters.iter())
            .filter_map(|parameter| resolve_parameter_object(catalog, parameter))
            .filter(|parameter| parameter.location.as_deref() == Some("query"))
            .filter_map(|parameter| {
                let name = parameter.name.as_ref()?;
                seen.insert(name.clone()).then(|| QueryParamSpec {
                    name: name.clone(),
                    required: parameter.required.unwrap_or(false),
                    schema: parameter
                        .schema
                        .as_ref()
                        .map(|schema| resolve_param_schema(catalog, schema)),
                })
            }),
    );
    params
}

fn resolve_param_schema(catalog: &RestCatalog, schema: &SchemaObject) -> SchemaObject {
    let mut resolved = schema.clone();
    let mut seen = BTreeSet::new();

    while let Some(reference) = resolved.reference.as_deref() {
        let Some(schema_name) = component_schema_name(reference) else {
            break;
        };
        if !seen.insert(schema_name.to_owned()) {
            break;
        }
        let Some(next) = catalog.component_schemas.get(schema_name) else {
            break;
        };
        resolved = next.clone();
    }

    resolved
}

fn resolve_parameter_object<'a>(
    catalog: &'a RestCatalog,
    parameter: &'a super::spec::ParameterObject,
) -> Option<&'a super::spec::ParameterObject> {
    let Some(reference) = parameter.reference.as_deref() else {
        return Some(parameter);
    };
    let name = reference.strip_prefix("#/components/parameters/")?;
    catalog
        .component_parameters
        .get(name)
        .or_else(|| (!parameter_reference_only(parameter)).then_some(parameter))
}

fn parameter_reference_only(parameter: &super::spec::ParameterObject) -> bool {
    parameter.name.is_none()
        && parameter.location.is_none()
        && parameter.required.is_none()
        && parameter.schema.is_none()
}

fn pagination_style(params: &[String]) -> Option<PaginationStyle> {
    let params = params
        .iter()
        .map(|param| param.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();

    if params.contains("limit") && params.contains("offset") {
        Some(PaginationStyle::OffsetLimit)
    } else if params.contains("page") && params.contains("per_page") {
        Some(PaginationStyle::PageNumber)
    } else if params.contains("cursor") && params.contains("limit") {
        Some(PaginationStyle::Cursor)
    } else {
        None
    }
}

fn response_schema_ref(catalog: &RestCatalog, operation: &OperationObject) -> Option<String> {
    for status in ["200", "201", "202", "204"] {
        if let Some(reference) = operation
            .responses
            .get(status)
            .and_then(|response| response_ref(catalog, response))
        {
            return Some(reference);
        }
    }

    let mut responses = operation.responses.iter().collect::<Vec<_>>();
    responses.sort_by_key(|(status, _)| status.as_str());
    responses
        .into_iter()
        .filter(|(status, _)| status.starts_with('2'))
        .find_map(|(_, response)| response_ref(catalog, response))
}

fn success_response(catalog: &RestCatalog, operation: &OperationObject) -> Option<SuccessResponse> {
    for status in ["200", "201", "202", "204"] {
        if let Some(response) = operation.responses.get(status)
            && let Some(success) = success_response_for(catalog, status, response)
        {
            return Some(success);
        }
    }

    let mut responses = operation.responses.iter().collect::<Vec<_>>();
    responses.sort_by_key(|(status, _)| status.as_str());
    responses
        .into_iter()
        .filter(|(status, _)| status.starts_with('2'))
        .find_map(|(status, response)| success_response_for(catalog, status, response))
}

fn success_response_for(
    catalog: &RestCatalog,
    status: &str,
    response: &super::spec::ResponseObject,
) -> Option<SuccessResponse> {
    let status = status.parse::<u16>().ok()?;
    Some(SuccessResponse {
        status,
        has_body: response_has_body(catalog, response),
    })
}

fn response_has_body(catalog: &RestCatalog, response: &super::spec::ResponseObject) -> bool {
    let resolved = resolve_response_object(catalog, response);
    response_ref(catalog, response).is_some() || !resolved.content.is_empty()
}

fn response_body_shape(
    catalog: &RestCatalog,
    operation: &OperationObject,
) -> Option<ResponseBodyShape> {
    for status in ["200", "201", "202", "204"] {
        if let Some(response) = operation.responses.get(status)
            && let Some(shape) = response_body_shape_for_response(catalog, response)
        {
            return Some(shape);
        }
    }

    let mut responses = operation.responses.iter().collect::<Vec<_>>();
    responses.sort_by_key(|(status, _)| status.as_str());
    responses
        .into_iter()
        .filter(|(status, _)| status.starts_with('2'))
        .find_map(|(_, response)| response_body_shape_for_response(catalog, response))
}

fn response_body_shape_for_response(
    catalog: &RestCatalog,
    response: &super::spec::ResponseObject,
) -> Option<ResponseBodyShape> {
    response_schema(catalog, response).and_then(|schema| schema_body_shape(catalog, schema))
}

fn response_schema<'a>(
    catalog: &'a RestCatalog,
    response: &'a super::spec::ResponseObject,
) -> Option<&'a SchemaObject> {
    let response = resolve_response_object(catalog, response);

    content_json_schema(&response.content).or_else(|| {
        let mut content = response.content.iter().collect::<Vec<_>>();
        content.sort_by_key(|(media_type, _)| media_type.as_str());
        content
            .into_iter()
            .find_map(|(_, media)| media.schema.as_ref())
    })
}

fn schema_body_shape(catalog: &RestCatalog, schema: &SchemaObject) -> Option<ResponseBodyShape> {
    if schema_declares_array(schema) {
        return Some(ResponseBodyShape::Array);
    }

    if let Some(resource_name) = schema
        .reference
        .as_deref()
        .and_then(component_resource_name)
    {
        let resource = catalog.resources.get(&resource_name)?;
        return if resource.meta.item_resource.is_some() {
            Some(ResponseBodyShape::Array)
        } else if resource_is_array_wrapper(resource) {
            Some(ResponseBodyShape::ArrayWrapper)
        } else if resource.columns.is_empty() {
            None
        } else {
            Some(ResponseBodyShape::Object)
        };
    }

    if schema_declares_array_wrapper(catalog, schema) {
        return Some(ResponseBodyShape::ArrayWrapper);
    }

    if schema_declares_object(schema) || !schema.properties.is_empty() {
        return Some(ResponseBodyShape::Object);
    }

    None
}

fn schema_declares_array_wrapper(catalog: &RestCatalog, schema: &SchemaObject) -> bool {
    let mut has_array_property = false;
    for (field, property) in &schema.properties {
        if matches!(
            schema_body_shape(catalog, property),
            Some(ResponseBodyShape::Array)
        ) {
            has_array_property = true;
            continue;
        }
        if !is_response_wrapper_metadata_field(field) {
            return false;
        }
    }

    has_array_property
}

fn schema_declares_array(schema: &SchemaObject) -> bool {
    schema.items.is_some() || schema_type_string(schema).as_deref() == Some("array")
}

fn schema_declares_object(schema: &SchemaObject) -> bool {
    schema_type_string(schema).as_deref() == Some("object")
}

fn schema_type_string(schema: &SchemaObject) -> Option<String> {
    match &schema.schema_type {
        Some(serde_json::Value::String(kind)) => Some(kind.clone()),
        Some(serde_json::Value::Array(kinds)) => kinds
            .iter()
            .find_map(serde_json::Value::as_str)
            .map(str::to_owned),
        Some(serde_json::Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

fn response_fields(
    catalog: &RestCatalog,
    resource_name: &str,
    response_schema_ref: Option<&str>,
) -> Vec<String> {
    let response_resource = response_schema_ref
        .and_then(component_resource_name)
        .unwrap_or_else(|| resource_name.to_owned());

    catalog
        .resources
        .get(&response_resource)
        .filter(|resource| !resource.columns.is_empty())
        .or_else(|| catalog.resources.get(resource_name))
        .map(|resource| {
            resource
                .columns
                .iter()
                .filter(|column| !is_path_lookup_column(resource, &column.name))
                .filter(|column| is_rest_returning_type(&column.normalized_type))
                .map(|column| column.name.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn is_path_lookup_column(resource: &ResourceSchema, column_name: &str) -> bool {
    resource
        .meta
        .path_lookup_columns
        .iter()
        .any(|name| name == column_name)
}

fn response_wrapper(
    catalog: &RestCatalog,
    operation: &OperationObject,
    response_schema_ref: Option<&str>,
    effective_resource_name: Option<&str>,
    path_resource_name: &str,
    pattern: &PathPattern,
) -> Option<ResponseWrapper> {
    component_response_wrapper(
        catalog,
        response_schema_ref,
        effective_resource_name,
        path_resource_name,
        pattern,
    )
    .or_else(|| {
        inline_response_wrapper(
            catalog,
            operation,
            effective_resource_name,
            path_resource_name,
            pattern,
        )
    })
}

fn component_response_wrapper(
    catalog: &RestCatalog,
    response_schema_ref: Option<&str>,
    effective_resource_name: Option<&str>,
    path_resource_name: &str,
    pattern: &PathPattern,
) -> Option<ResponseWrapper> {
    let response_resource = response_schema_ref.and_then(component_resource_name)?;
    let resource = catalog.resources.get(&response_resource)?;
    if resource.meta.item_resource.is_some() {
        return None;
    }
    let hints = [
        effective_resource_name,
        Some(path_resource_name),
        last_literal_segment(pattern),
    ];
    if !resource_is_array_wrapper_for_hints(resource, &hints) {
        return None;
    }

    let array_fields = resource
        .columns
        .iter()
        .filter(|column| response_wrapper_payload_column_matches(column, &hints))
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    if array_fields.is_empty() {
        return None;
    }

    let array_field = array_fields
        .iter()
        .copied()
        .find(|field| response_wrapper_payload_field_matches(field, &hints))?
        .to_owned();

    let count_field = recognized_response_wrapper_field(
        resource,
        &array_field,
        &["count", "total", "totalcount"],
    );
    let status_field = recognized_response_wrapper_field(resource, &array_field, &["status"]);
    let status_code_field =
        recognized_response_wrapper_field(resource, &array_field, &["statuscode"]);
    let message_field = recognized_response_wrapper_field(resource, &array_field, &["message"]);
    let recognized_fields = [
        count_field.as_deref(),
        status_field.as_deref(),
        status_code_field.as_deref(),
        message_field.as_deref(),
    ];
    let mut static_fields = resource
        .columns
        .iter()
        .filter(|column| column.name != array_field)
        .filter(|column| {
            !recognized_fields
                .iter()
                .flatten()
                .any(|field| *field == column.name)
        })
        .map(|column| ResponseWrapperStaticField {
            name: column.name.clone(),
            value: response_wrapper_static_value_for_column(column),
        })
        .collect::<Vec<_>>();
    static_fields.sort_by(|left, right| left.name.cmp(&right.name));

    Some(ResponseWrapper {
        array_field,
        payload_shape: ResponseWrapperPayloadShape::Array,
        count_field,
        status_field,
        status_code_field,
        message_field,
        static_fields,
    })
}

fn inline_response_wrapper(
    catalog: &RestCatalog,
    operation: &OperationObject,
    effective_resource_name: Option<&str>,
    path_resource_name: &str,
    pattern: &PathPattern,
) -> Option<ResponseWrapper> {
    let schema = response_schema_for_operation(catalog, operation)?;
    if schema.reference.is_some() || schema_declares_array(schema) {
        return None;
    }

    let array_fields = schema
        .properties
        .iter()
        .filter(|(_, property)| {
            matches!(
                schema_body_shape(catalog, property),
                Some(ResponseBodyShape::Array)
            )
        })
        .map(|(field, _)| field.as_str())
        .collect::<Vec<_>>();
    let hints = [
        effective_resource_name,
        Some(path_resource_name),
        last_literal_segment(pattern),
    ];
    if array_fields.is_empty() {
        return inline_singleton_response_wrapper(schema, catalog, &hints);
    }

    let array_resource_fields = schema
        .properties
        .iter()
        .filter_map(|(field, property)| {
            schema_array_item_target_resource(catalog, property).map(|_| field.as_str())
        })
        .collect::<Vec<_>>();
    let array_field = array_resource_fields
        .iter()
        .copied()
        .find(|field| response_wrapper_payload_field_matches(field, &hints))
        .or_else(|| {
            array_fields
                .iter()
                .copied()
                .find(|field| response_wrapper_payload_field_matches(field, &hints))
        })
        .or_else(|| (array_resource_fields.len() == 1).then(|| array_resource_fields[0]))
        .or_else(|| (array_fields.len() == 1).then(|| array_fields[0]))?
        .to_owned();

    let count_field =
        recognized_response_wrapper_property(schema, &array_field, &["count", "total"]);
    let status_field = recognized_response_wrapper_property(schema, &array_field, &["status"]);
    let status_code_field =
        recognized_response_wrapper_property(schema, &array_field, &["statuscode"]);
    let message_field = recognized_response_wrapper_property(schema, &array_field, &["message"]);
    let static_fields = schema
        .properties
        .iter()
        .filter(|(field, _)| field.as_str() != array_field)
        .filter(|(field, _)| !is_response_wrapper_metadata_field(field))
        .map(|(field, property)| ResponseWrapperStaticField {
            name: field.clone(),
            value: response_wrapper_static_value(property),
        })
        .collect();

    Some(ResponseWrapper {
        array_field,
        payload_shape: ResponseWrapperPayloadShape::Array,
        count_field,
        status_field,
        status_code_field,
        message_field,
        static_fields,
    })
}

fn inline_singleton_response_wrapper(
    schema: &SchemaObject,
    catalog: &RestCatalog,
    hints: &[Option<&str>; 3],
) -> Option<ResponseWrapper> {
    let object_resource_fields = schema
        .properties
        .iter()
        .filter_map(|(field, property)| {
            schema_object_target_resource(catalog, property).map(|_| field.as_str())
        })
        .collect::<Vec<_>>();
    if object_resource_fields.is_empty() {
        return None;
    }

    let payload_field = object_resource_fields
        .iter()
        .copied()
        .find(|field| response_wrapper_payload_field_matches(field, hints))
        .or_else(|| (object_resource_fields.len() == 1).then(|| object_resource_fields[0]))?
        .to_owned();

    let count_field =
        recognized_response_wrapper_property(schema, &payload_field, &["count", "total"]);
    let status_field = recognized_response_wrapper_property(schema, &payload_field, &["status"]);
    let status_code_field =
        recognized_response_wrapper_property(schema, &payload_field, &["statuscode"]);
    let message_field = recognized_response_wrapper_property(schema, &payload_field, &["message"]);
    let static_fields = schema
        .properties
        .iter()
        .filter(|(field, _)| field.as_str() != payload_field)
        .filter(|(field, _)| !is_response_wrapper_metadata_field(field))
        .map(|(field, property)| ResponseWrapperStaticField {
            name: field.clone(),
            value: response_wrapper_static_value(property),
        })
        .collect();

    Some(ResponseWrapper {
        array_field: payload_field,
        payload_shape: ResponseWrapperPayloadShape::Object,
        count_field,
        status_field,
        status_code_field,
        message_field,
        static_fields,
    })
}

fn response_schema_for_operation<'a>(
    catalog: &'a RestCatalog,
    operation: &'a OperationObject,
) -> Option<&'a SchemaObject> {
    for status in ["200", "201", "202", "204"] {
        if let Some(response) = operation.responses.get(status)
            && let Some(schema) = response_schema(catalog, response)
        {
            return Some(schema);
        }
    }

    let mut responses = operation.responses.iter().collect::<Vec<_>>();
    responses.sort_by_key(|(status, _)| status.as_str());
    responses
        .into_iter()
        .filter(|(status, _)| status.starts_with('2'))
        .find_map(|(_, response)| response_schema(catalog, response))
}

fn inline_response_target(
    catalog: &RestCatalog,
    operation: &OperationObject,
    path_resource_name: &str,
    pattern: &PathPattern,
) -> Option<InlineResponseTarget> {
    let schema = response_schema_for_operation(catalog, operation)?;
    if schema.reference.is_some() {
        return None;
    }

    if schema_declares_array(schema) {
        let resource_name = schema_response_target_resource(catalog, schema)?.0;
        return Some(InlineResponseTarget {
            resource_name,
            body_shape: ResponseBodyShape::Array,
        });
    }

    if !schema_declares_object(schema) && schema.properties.is_empty() {
        return None;
    }

    let hints = [
        None,
        Some(path_resource_name),
        last_literal_segment(pattern),
    ];
    let array_payload_shape = inline_array_payload_field(catalog, schema, &hints).is_some();
    let candidates = schema
        .properties
        .iter()
        .filter_map(|(field, property)| {
            schema_response_target_resource(catalog, property)
                .map(|(resource_name, shape)| (field.as_str(), resource_name, shape))
        })
        .collect::<Vec<_>>();

    let (_, resource_name, body_shape) = candidates
        .iter()
        .find(|(field, _, _)| response_wrapper_payload_field_matches(field, &hints))
        .or_else(|| (candidates.len() == 1).then(|| &candidates[0]))?;

    Some(InlineResponseTarget {
        resource_name: resource_name.clone(),
        body_shape: if array_payload_shape {
            ResponseBodyShape::Array
        } else {
            *body_shape
        },
    })
}

fn inline_array_payload_field(
    catalog: &RestCatalog,
    schema: &SchemaObject,
    hints: &[Option<&str>; 3],
) -> Option<String> {
    let array_fields = schema
        .properties
        .iter()
        .filter(|(_, property)| {
            matches!(
                schema_body_shape(catalog, property),
                Some(ResponseBodyShape::Array)
            )
        })
        .map(|(field, _)| field.as_str())
        .collect::<Vec<_>>();
    if array_fields.is_empty() {
        return None;
    }

    let array_resource_fields = schema
        .properties
        .iter()
        .filter_map(|(field, property)| {
            schema_array_item_target_resource(catalog, property).map(|_| field.as_str())
        })
        .collect::<Vec<_>>();

    array_resource_fields
        .iter()
        .copied()
        .find(|field| response_wrapper_payload_field_matches(field, hints))
        .or_else(|| {
            array_fields
                .iter()
                .copied()
                .find(|field| response_wrapper_payload_field_matches(field, hints))
        })
        .or_else(|| (array_resource_fields.len() == 1).then(|| array_resource_fields[0]))
        .or_else(|| (array_fields.len() == 1).then(|| array_fields[0]))
        .map(str::to_owned)
}

fn schema_response_target_resource(
    catalog: &RestCatalog,
    schema: &SchemaObject,
) -> Option<(String, ResponseBodyShape)> {
    if schema_declares_array(schema) {
        return schema_array_item_target_resource(catalog, schema)
            .map(|resource_name| (resource_name, ResponseBodyShape::Array));
    }

    let resource_name = schema
        .reference
        .as_deref()
        .and_then(component_resource_name)?;
    let resource = catalog.resources.get(&resource_name)?;
    if let Some(item_resource) = &resource.meta.item_resource {
        return catalog
            .resources
            .contains_key(item_resource)
            .then(|| (item_resource.clone(), ResponseBodyShape::Array));
    }

    (!resource.columns.is_empty()).then_some((resource_name, ResponseBodyShape::Object))
}

fn schema_array_item_target_resource(
    catalog: &RestCatalog,
    schema: &SchemaObject,
) -> Option<String> {
    if let Some(resource_name) = schema
        .reference
        .as_deref()
        .and_then(component_resource_name)
    {
        let resource = catalog.resources.get(&resource_name)?;
        if let Some(item_resource) = &resource.meta.item_resource {
            return catalog
                .resources
                .contains_key(item_resource)
                .then(|| item_resource.clone());
        }
    }

    schema
        .items
        .as_deref()
        .and_then(|item| schema_response_target_resource(catalog, item))
        .map(|(resource_name, _)| resource_name)
}

fn schema_object_target_resource(catalog: &RestCatalog, schema: &SchemaObject) -> Option<String> {
    let (resource_name, shape) = schema_response_target_resource(catalog, schema)?;
    matches!(shape, ResponseBodyShape::Object).then_some(resource_name)
}

fn recognized_response_wrapper_property(
    schema: &SchemaObject,
    array_field: &str,
    names: &[&str],
) -> Option<String> {
    schema
        .properties
        .keys()
        .find(|field| {
            field.as_str() != array_field
                && names
                    .iter()
                    .any(|name| canonical_response_wrapper_name(field) == *name)
        })
        .cloned()
}

fn response_wrapper_static_value(schema: &SchemaObject) -> ResponseWrapperStaticValue {
    match schema_type_string(schema).as_deref() {
        Some("object") => ResponseWrapperStaticValue::Object,
        Some("array") => ResponseWrapperStaticValue::Array,
        Some("boolean") => ResponseWrapperStaticValue::Boolean(false),
        Some("integer" | "number") => ResponseWrapperStaticValue::Integer(0),
        Some("string") => ResponseWrapperStaticValue::String(String::new()),
        Some(_) | None => ResponseWrapperStaticValue::Null,
    }
}

fn response_wrapper_static_value_for_column(column: &ResourceColumn) -> ResponseWrapperStaticValue {
    match column.normalized_type.as_str() {
        "json" if column.declared_type == "object" => ResponseWrapperStaticValue::Object,
        "json" if column.declared_type == "array" => ResponseWrapperStaticValue::Array,
        "array" => ResponseWrapperStaticValue::Array,
        "boolean" => ResponseWrapperStaticValue::Boolean(false),
        "integer" | "bigint" | "smallint" | "numeric" | "float" => {
            ResponseWrapperStaticValue::Integer(0)
        }
        "text" | "timestamp" | "date" | "bytes" => {
            ResponseWrapperStaticValue::String(String::new())
        }
        _ => ResponseWrapperStaticValue::Null,
    }
}

fn recognized_response_wrapper_field(
    resource: &ResourceSchema,
    array_field: &str,
    names: &[&str],
) -> Option<String> {
    resource
        .columns
        .iter()
        .find(|column| {
            column.name != array_field
                && names
                    .iter()
                    .any(|name| canonical_response_wrapper_name(&column.name) == *name)
        })
        .map(|column| column.name.clone())
}

fn resource_is_array_wrapper(resource: &ResourceSchema) -> bool {
    resource_is_array_wrapper_for_hints(resource, &[None, None, None])
}

fn resource_is_array_wrapper_for_hints(
    resource: &ResourceSchema,
    hints: &[Option<&str>; 3],
) -> bool {
    let generic_detection = hints.iter().all(Option::is_none);
    let mut payload_field_count = 0usize;
    let mut has_payload_field = false;
    let mut has_named_payload_field = false;
    let mut has_wrapper_metadata = false;
    let mut has_strong_wrapper_metadata = false;
    let mut ordinary_static_fields = 0usize;
    for column in &resource.columns {
        if response_wrapper_payload_column_matches(column, hints) {
            payload_field_count += 1;
            has_payload_field = true;
            has_named_payload_field = true;
            continue;
        }
        if generic_detection && column.normalized_type == "array" {
            payload_field_count += 1;
            has_payload_field = true;
            continue;
        }
        if !is_response_wrapper_metadata_field(&column.name) {
            if is_response_wrapper_static_column(column) {
                ordinary_static_fields += 1;
                continue;
            }
            return false;
        }
        has_wrapper_metadata = true;
        has_strong_wrapper_metadata |= is_strong_response_wrapper_metadata_field(&column.name);
    }

    has_payload_field
        && payload_field_count == 1
        && (has_wrapper_metadata || (has_named_payload_field && ordinary_static_fields == 0))
        && (has_named_payload_field || has_strong_wrapper_metadata)
        && (has_named_payload_field || ordinary_static_fields == 0)
}

fn response_wrapper_payload_column_matches(
    column: &ResourceColumn,
    hints: &[Option<&str>; 3],
) -> bool {
    match column.normalized_type.as_str() {
        "array" => response_wrapper_payload_field_matches(&column.name, hints),
        "json" if canonical_response_wrapper_name(&column.name) == "results" => {
            response_wrapper_payload_field_matches(&column.name, hints)
        }
        _ => false,
    }
}

fn response_wrapper_payload_field_matches(field: &str, hints: &[Option<&str>; 3]) -> bool {
    is_standard_response_wrapper_payload_field(field)
        || hints
            .iter()
            .flatten()
            .any(|hint| response_wrapper_name_matches(field, hint))
}

fn is_standard_response_wrapper_payload_field(field: &str) -> bool {
    matches!(
        canonical_response_wrapper_name(field).as_str(),
        "data" | "entries" | "items" | "results"
    )
}

fn is_response_wrapper_static_column(column: &ResourceColumn) -> bool {
    matches!(
        column.normalized_type.as_str(),
        "boolean"
            | "integer"
            | "bigint"
            | "smallint"
            | "numeric"
            | "float"
            | "text"
            | "timestamp"
            | "date"
            | "bytes"
            | "json"
    )
}

fn is_response_wrapper_metadata_field(field: &str) -> bool {
    matches!(
        canonical_response_wrapper_name(field).as_str(),
        "count"
            | "total"
            | "totalpages"
            | "totalcount"
            | "status"
            | "statuscode"
            | "message"
            | "meta"
            | "metadata"
            | "pagination"
            | "links"
            | "link"
            | "offset"
            | "order"
            | "limit"
            | "page"
            | "perpage"
            | "next"
            | "previous"
    )
}

fn is_strong_response_wrapper_metadata_field(field: &str) -> bool {
    matches!(
        canonical_response_wrapper_name(field).as_str(),
        "count"
            | "total"
            | "totalpages"
            | "totalcount"
            | "status"
            | "statuscode"
            | "message"
            | "pagination"
            | "offset"
            | "order"
            | "limit"
            | "page"
            | "perpage"
            | "next"
            | "previous"
    )
}

fn last_literal_segment(pattern: &PathPattern) -> Option<&str> {
    pattern
        .segments
        .iter()
        .rev()
        .find_map(|segment| match segment {
            PathSegment::Literal(value) => Some(value.as_str()),
            PathSegment::Param(_) | PathSegment::Template { .. } => None,
        })
}

fn response_wrapper_name_matches(field: &str, hint: &str) -> bool {
    let field = canonical_response_wrapper_name(field);
    let hint = canonical_response_wrapper_name(hint);
    if field.is_empty() || hint.is_empty() {
        return false;
    }

    field == hint
        || trim_plural_suffix(&field) == trim_plural_suffix(&hint)
        || field.ends_with(&hint)
        || trim_plural_suffix(&field).ends_with(trim_plural_suffix(&hint))
}

fn canonical_response_wrapper_name(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_lowercase())
        .collect()
}

fn trim_plural_suffix(value: &str) -> &str {
    value.strip_suffix('s').unwrap_or(value)
}

fn is_rest_returning_type(normalized_type: &str) -> bool {
    !matches!(normalized_type, "bytes")
}

fn component_resource_name(reference: &str) -> Option<String> {
    component_schema_name(reference)
        .map(|schema_name| format!("{}s", schema_name.to_ascii_lowercase()))
}

fn component_schema_name(reference: &str) -> Option<&str> {
    reference.strip_prefix("#/components/schemas/")
}

fn component_request_body_name(reference: &str) -> Option<String> {
    reference
        .strip_prefix("#/components/requestBodies/")
        .map(str::to_owned)
}

fn request_body_schema<'a>(
    catalog: &'a RestCatalog,
    operation: &'a OperationObject,
) -> Option<&'a SchemaObject> {
    let request_body = resolved_request_body(catalog, operation)?;
    request_body_schema_from_body(request_body)
}

fn request_body_schema_from_body(request_body: &RequestBodyObject) -> Option<&SchemaObject> {
    request_body_json_schema(request_body)
        .or_else(|| request_body_form_urlencoded_schema(request_body))
}

fn resolved_request_body<'a>(
    catalog: &'a RestCatalog,
    operation: &'a OperationObject,
) -> Option<&'a RequestBodyObject> {
    let request_body = operation.request_body.as_ref()?;
    if let Some(reference) = &request_body.reference {
        let request_body_name = component_request_body_name(reference)?;
        catalog.component_request_bodies.get(&request_body_name)
    } else {
        Some(request_body)
    }
}

fn request_body_json_schema(request_body: &RequestBodyObject) -> Option<&SchemaObject> {
    content_json_schema(&request_body.content)
}

fn request_body_form_urlencoded_schema(request_body: &RequestBodyObject) -> Option<&SchemaObject> {
    content_form_urlencoded_schema(&request_body.content)
}

fn request_body_declares_supported_content(request_body: &RequestBodyObject) -> bool {
    request_body.content.keys().any(|media_type| {
        is_json_media_type(media_type) || is_form_urlencoded_media_type(media_type)
    })
}

fn request_body_content_types(request_body: &RequestBodyObject) -> Vec<String> {
    let mut content_types = request_body.content.keys().cloned().collect::<Vec<_>>();
    content_types.sort();
    content_types
}

fn response_ref(catalog: &RestCatalog, response: &super::spec::ResponseObject) -> Option<String> {
    let response = resolve_response_object(catalog, response);

    if let Some(reference) = &response.reference {
        return Some(reference.clone());
    }

    content_json_schema(&response.content)
        .and_then(schema_ref)
        .or_else(|| {
            let mut content = response.content.iter().collect::<Vec<_>>();
            content.sort_by_key(|(media_type, _)| media_type.as_str());
            content
                .into_iter()
                .find_map(|(_, media)| media.schema.as_ref().and_then(schema_ref))
        })
}

fn content_json_schema(content: &HashMap<String, MediaTypeObject>) -> Option<&SchemaObject> {
    let mut json_content = content
        .iter()
        .filter(|(media_type, _)| is_json_media_type(media_type))
        .collect::<Vec<_>>();
    json_content.sort_by_key(|(media_type, _)| json_media_type_rank(media_type));
    json_content
        .into_iter()
        .find_map(|(_, media)| media.schema.as_ref())
}

fn content_form_urlencoded_schema(
    content: &HashMap<String, MediaTypeObject>,
) -> Option<&SchemaObject> {
    content
        .iter()
        .filter(|(media_type, _)| is_form_urlencoded_media_type(media_type))
        .find_map(|(_, media)| media.schema.as_ref())
}

fn is_json_media_type(media_type: &str) -> bool {
    let base = media_type_base(media_type);
    base == "application/json" || (base.starts_with("application/") && base.ends_with("+json"))
}

fn is_form_urlencoded_media_type(media_type: &str) -> bool {
    media_type_base(media_type) == APPLICATION_FORM_URLENCODED
}

fn json_media_type_rank(media_type: &str) -> (u8, String) {
    let base = media_type_base(media_type);
    let rank = if base == "application/json" { 0 } else { 1 };
    (rank, base)
}

fn media_type_base(media_type: &str) -> String {
    media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn schema_ref(schema: &SchemaObject) -> Option<String> {
    schema
        .reference
        .clone()
        .or_else(|| schema.items.as_deref().and_then(schema_ref))
}

fn operations(path_item: &PathItem) -> Vec<(Method, &OperationObject)> {
    let mut operations = Vec::new();
    if let Some(operation) = &path_item.get {
        operations.push((Method::Get, operation));
    }
    if let Some(operation) = &path_item.head {
        operations.push((Method::Head, operation));
    }
    if let Some(operation) = &path_item.post {
        operations.push((Method::Post, operation));
    }
    if let Some(operation) = &path_item.put {
        operations.push((Method::Put, operation));
    }
    if let Some(operation) = &path_item.patch {
        operations.push((Method::Patch, operation));
    }
    if let Some(operation) = &path_item.delete {
        operations.push((Method::Delete, operation));
    }
    operations
}

fn sort_routes_by_specificity(routes: &mut RouteTable) {
    routes.sort_by(|left, right| {
        let (_, left_pattern, _) = left;
        let (_, right_pattern, _) = right;

        right_pattern
            .segments
            .len()
            .cmp(&left_pattern.segments.len())
            .then_with(|| {
                right_pattern
                    .literal_count()
                    .cmp(&left_pattern.literal_count())
            })
            .then_with(|| left_pattern.param_count().cmp(&right_pattern.param_count()))
            .then_with(|| pattern_key(left_pattern).cmp(&pattern_key(right_pattern)))
            .then_with(|| method_order(left.0).cmp(&method_order(right.0)))
    });
}

fn pattern_key(pattern: &PathPattern) -> String {
    pattern
        .segments
        .iter()
        .map(|segment| match segment {
            PathSegment::Literal(value) | PathSegment::Param(value) => value.clone(),
            PathSegment::Template {
                prefix,
                name,
                suffix,
            } => format!("{prefix}{{{name}}}{suffix}"),
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn method_order(method: Method) -> usize {
    match method {
        Method::Get => 0,
        Method::Head => 1,
        Method::Post => 2,
        Method::Put => 3,
        Method::Patch => 4,
        Method::Delete => 5,
    }
}

fn request_segments(path: &str) -> Vec<&str> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn percent_decode_path_segment(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
        {
            decoded.push((high << 4) | low);
            index += 3;
            continue;
        }

        decoded.push(bytes[index]);
        index += 1;
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::rest::spec::parse_rest_catalog_bytes;

    fn registry(spec: &str) -> RouteRegistry {
        let catalog =
            parse_rest_catalog_bytes(spec.as_bytes(), "test.yaml").expect("catalog should parse");
        build_route_registry_with_config(&catalog, &RoutingConfig::default())
    }

    fn catalog(spec: &str) -> RestCatalog {
        parse_rest_catalog_bytes(spec.as_bytes(), "test.yaml").expect("catalog should parse")
    }

    fn registry_with_config(spec: &str, config: &RoutingConfig) -> RouteRegistry {
        let catalog =
            parse_rest_catalog_bytes(spec.as_bytes(), "test.yaml").expect("catalog should parse");
        build_route_registry_with_config(&catalog, config)
    }

    fn registry_with_topology(
        catalog: &RestCatalog,
        topology: &ResourceTopology,
        config: &RoutingConfig,
    ) -> RouteRegistry {
        build_route_registry(catalog, topology, config)
    }

    fn route<'a>(registry: &'a RouteRegistry, method: Method, path: &str) -> &'a RouteEntry {
        registry
            .routes
            .iter()
            .find(|(candidate_method, pattern, _)| {
                *candidate_method == method && pattern == &PathPattern::parse(path)
            })
            .map(|(_, _, entry)| entry)
            .expect("route should exist")
    }

    #[test]
    fn minimal_resource_crud_routes_classify() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      properties:
        id: { type: string }
        name: { type: string }
paths:
  /files:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/File"
    post:
      responses:
        "201":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/File"
  /files/{id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/File"
    put:
      responses: {}
    patch:
      responses: {}
    delete:
      responses: {}
"##,
        );

        assert_eq!(
            route(&registry, Method::Post, "/files").kind,
            RouteKind::Create
        );
        assert_eq!(
            route(&registry, Method::Get, "/files").kind,
            RouteKind::ReadMany
        );
        assert_eq!(
            route(&registry, Method::Get, "/files/{id}").kind,
            RouteKind::ReadOne
        );
        assert_eq!(
            route(&registry, Method::Put, "/files/{id}").kind,
            RouteKind::Update
        );
        assert_eq!(
            route(&registry, Method::Patch, "/files/{id}").kind,
            RouteKind::Update
        );
        assert_eq!(
            route(&registry, Method::Delete, "/files/{id}").kind,
            RouteKind::Delete
        );
        assert_eq!(
            route(&registry, Method::Get, "/files/{id}").path_params,
            vec!["id"]
        );
        assert_eq!(
            route(&registry, Method::Get, "/files")
                .response_schema_ref
                .as_deref(),
            Some("#/components/schemas/File")
        );
        assert_eq!(
            route(&registry, Method::Get, "/files")
                .response_resource_name
                .as_deref(),
            Some("files")
        );
        assert_eq!(
            route(&registry, Method::Get, "/files").matched_policy,
            Some(RoutingPolicy::SchemaFirst)
        );
        assert_eq!(
            route(&registry, Method::Get, "/files")
                .effective_resource_name
                .as_deref(),
            Some("files")
        );
        assert_eq!(
            route(&registry, Method::Post, "/files").response_fields,
            vec!["id", "name"]
        );
        assert_eq!(
            route(&registry, Method::Post, "/files").success_response,
            Some(SuccessResponse {
                status: 201,
                has_body: true
            })
        );
        assert!(!route(&registry, Method::Post, "/files").request_body_declared);
    }

    #[test]
    fn route_auth_schemes_use_global_security() {
        let registry = registry(
            r##"
openapi: 3.0.3
security:
  - bearerAuth: []
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
  schemas:
    Vault:
      type: object
      properties:
        id: { type: string }
paths:
  /vaults:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Vault"
"##,
        );

        assert_eq!(
            route(&registry, Method::Get, "/vaults").required_auth_schemes,
            vec!["bearerAuth"]
        );
    }

    #[test]
    fn route_auth_schemes_operation_empty_security_makes_public() {
        let registry = registry(
            r##"
openapi: 3.0.3
security:
  - bearerAuth: []
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
  schemas:
    Pet:
      type: object
      properties:
        id: { type: string }
paths:
  /pets:
    get:
      security: []
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Pet"
"##,
        );

        assert!(
            route(&registry, Method::Get, "/pets")
                .required_auth_schemes
                .is_empty()
        );
    }

    #[test]
    fn route_auth_schemes_operation_override_beats_global() {
        let registry = registry(
            r##"
openapi: 3.0.3
security:
  - bearerAuth: []
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
    slackAuth:
      type: oauth2
  schemas:
    Message:
      type: object
      properties:
        id: { type: string }
paths:
  /messages:
    get:
      security:
        - slackAuth: []
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Message"
"##,
        );

        assert_eq!(
            route(&registry, Method::Get, "/messages").required_auth_schemes,
            vec!["slackAuth"]
        );
    }

    #[test]
    fn route_auth_schemes_default_public_when_no_security() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: string }
paths:
  /pets:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Pet"
"##,
        );

        assert!(
            route(&registry, Method::Get, "/pets")
                .required_auth_schemes
                .is_empty()
        );
    }

    #[test]
    fn route_entry_records_declared_no_content_success_response() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Rule:
      type: object
      properties:
        type: { type: string }
        config: { type: string }
paths:
  /rules:
    post:
      requestBody:
        content:
          '*/*': {}
      responses:
        "204":
          description: No content
"##,
        );

        assert!(route(&registry, Method::Post, "/rules").request_body_declared);
        assert!(!route(&registry, Method::Post, "/rules").request_body_schema_present);
        assert_eq!(
            route(&registry, Method::Post, "/rules").success_response,
            Some(SuccessResponse {
                status: 204,
                has_body: false
            })
        );
    }

    #[test]
    fn non_json_mutation_request_body_is_route_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      properties:
        Id: { type: string }
        Name: { type: string }
paths:
  /Files:
    post:
      requestBody:
        content:
          multipart/form-data:
            schema:
              type: object
              properties:
                file: { type: string, format: binary }
      responses:
        "201":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/File"
"##,
        );

        let create = route(&registry, Method::Post, "/Files");
        assert!(create.request_body_declared);
        assert!(!create.request_body_schema_present);
        assert!(matches!(
            create.kind,
            RouteKind::Refusal { ref detail }
                if detail.contains("multipart/form-data")
                    && detail.contains("only supports application/json or application/x-www-form-urlencoded request bodies")
                    && detail.contains("/Files")
        ));
    }

    #[test]
    fn parameterized_json_request_body_media_type_is_supported() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Activity:
      type: object
      properties:
        id: { type: integer }
        title: { type: string }
paths:
  /Activities:
    post:
      requestBody:
        content:
          application/json; v=1.0:
            schema:
              $ref: "#/components/schemas/Activity"
      responses:
        "200":
          content:
            application/json; v=1.0:
              schema:
                $ref: "#/components/schemas/Activity"
  /Activities/{id}:
    put:
      requestBody:
        content:
          application/*+json; v=1.0:
            schema:
              $ref: "#/components/schemas/Activity"
      responses:
        "200":
          content:
            application/json; v=1.0:
              schema:
                $ref: "#/components/schemas/Activity"
"##,
        );

        let create = route(&registry, Method::Post, "/Activities");
        assert_eq!(create.kind, RouteKind::Create);
        assert!(create.request_body_declared);
        assert!(create.request_body_schema_present);
        assert_eq!(
            create.request_schema_ref.as_deref(),
            Some("#/components/schemas/Activity")
        );

        let update = route(&registry, Method::Put, "/Activities/{id}");
        assert_eq!(update.kind, RouteKind::Update);
        assert!(update.request_body_declared);
        assert!(update.request_body_schema_present);
        assert_eq!(
            update.request_schema_ref.as_deref(),
            Some("#/components/schemas/Activity")
        );
    }

    #[test]
    fn required_array_request_body_field_classifies_as_create() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    ContractCallsRequest:
      type: object
      required: [fromChain, contractCalls]
      properties:
        fromChain: { type: number }
        contractCalls:
          type: array
          items:
            type: object
            properties:
              toContractAddress: { type: string }
paths:
  /v1/quote/contractCalls:
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/ContractCallsRequest"
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ContractCallsRequest"
"##,
        );

        let create = route(&registry, Method::Post, "/v1/quote/contractCalls");
        assert_eq!(create.kind, RouteKind::Create);
        assert!(create.request_body_declared);
        assert_eq!(
            create.request_schema_ref.as_deref(),
            Some("#/components/schemas/ContractCallsRequest")
        );
        assert_eq!(
            create.request_resource_name.as_deref(),
            Some("contractcallsrequests")
        );
    }

    #[test]
    fn optional_array_request_body_field_classifies_as_create() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    BatchRequest:
      type: object
      required: [name]
      properties:
        name: { type: string }
        items:
          type: array
          items: { type: string }
paths:
  /batches:
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/BatchRequest"
      responses:
        "201":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/BatchRequest"
"##,
        );

        let create = route(&registry, Method::Post, "/batches");
        assert_eq!(create.kind, RouteKind::Create);
        assert!(create.request_body_declared);
        assert_eq!(
            create.request_schema_ref.as_deref(),
            Some("#/components/schemas/BatchRequest")
        );
        assert_eq!(
            create.request_resource_name.as_deref(),
            Some("batchrequests")
        );
    }

    #[test]
    fn top_level_scalar_json_request_body_is_route_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    IdentityProviderRepresentation:
      type: object
      properties:
        alias: { type: string }
paths:
  /organizations/{org-id}/identity-providers:
    post:
      parameters:
        - name: org-id
          in: path
          required: true
          schema: { type: string }
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: string
      responses:
        "204":
          description: No Content
  /organizations/{org-id}/identity-providers/{alias}:
    get:
      parameters:
        - name: org-id
          in: path
          required: true
          schema: { type: string }
        - name: alias
          in: path
          required: true
          schema: { type: string }
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/IdentityProviderRepresentation"
"##,
        );

        let route = route(
            &registry,
            Method::Post,
            "/organizations/{org-id}/identity-providers",
        );
        assert!(matches!(
            route.kind,
            RouteKind::Refusal { ref detail }
                if detail.contains("top-level JSON request body schema type `string`")
                    && detail.contains("/organizations/{org-id}/identity-providers")
        ));
    }

    #[test]
    fn component_request_schema_missing_target_required_columns_is_route_refusal() {
        let config = RoutingConfig {
            policy: RoutingPolicy::PrefixScoped,
            base_prefix: Some("/v1/advanced".to_owned()),
            ..RoutingConfig::default()
        };
        let registry = registry_with_config(
            r##"
openapi: 3.0.3
components:
  schemas:
    RoutesRequest:
      type: object
      required: [fromAmount, fromChainId, fromTokenAddress, toChainId, toTokenAddress]
      properties:
        fromAmount: { type: string }
        fromChainId: { type: number }
        fromTokenAddress: { type: string }
        toChainId: { type: number }
        toTokenAddress: { type: string }
    Route:
      type: object
      required: [id, fromAmount, fromAmountUSD, steps, toAmount, toAmountMin, toAmountUSD, toToken]
      properties:
        id: { type: string }
        fromAmount: { type: string }
        fromAmountUSD: { type: string }
        steps:
          type: array
          items: { type: object }
        toAmount: { type: string }
        toAmountMin: { type: string }
        toAmountUSD: { type: string }
        toToken: { type: object }
    RoutesResponse:
      type: object
      properties:
        routes:
          type: array
          items:
            $ref: "#/components/schemas/Route"
paths:
  /v1/advanced/routes:
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/RoutesRequest"
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/RoutesResponse"
"##,
            &config,
        );

        let create = route(&registry, Method::Post, "/v1/advanced/routes");
        assert!(matches!(
            create.kind,
            RouteKind::Refusal { ref detail }
                if detail.contains("request body schema `#/components/schemas/RoutesRequest`")
                    && detail.contains("fromAmountUSD")
                    && detail.contains("steps")
                    && detail.contains("resource `routes`")
                    && detail.contains("/v1/advanced/routes")
        ));
    }

    #[test]
    fn component_request_schema_covering_target_required_columns_stays_create() {
        let config = RoutingConfig {
            policy: RoutingPolicy::PrefixScoped,
            base_prefix: Some("/v1/advanced".to_owned()),
            ..RoutingConfig::default()
        };
        let registry = registry_with_config(
            r##"
openapi: 3.0.3
components:
  schemas:
    RouteRequest:
      type: object
      required: [id, name]
      properties:
        id: { type: string }
        name: { type: string }
        note: { type: string }
    Route:
      type: object
      required: [id, name]
      properties:
        id: { type: string }
        name: { type: string }
paths:
  /v1/advanced/routes:
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/RouteRequest"
      responses:
        "201":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Route"
"##,
            &config,
        );

        assert_eq!(
            route(&registry, Method::Post, "/v1/advanced/routes").kind,
            RouteKind::Create
        );
    }

    #[test]
    fn form_urlencoded_request_body_media_type_is_supported() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    AccountLink:
      type: object
      properties:
        id: { type: string }
        object: { type: string }
paths:
  /v1/account_links:
    post:
      requestBody:
        content:
          application/x-www-form-urlencoded:
            schema:
              type: object
              required: [account, type]
              properties:
                account: { type: string }
                type: { type: string }
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/AccountLink"
"##,
        );

        let create = route(&registry, Method::Post, "/v1/account_links");
        assert_eq!(create.kind, RouteKind::Create);
        assert!(create.request_body_declared);
        assert!(create.request_body_schema_present);
        assert!(create.request_schema_ref.is_none());
    }

    #[test]
    fn singular_path_uses_flat_crud_effective_resource() {
        let registry = registry(
            r#"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths:
  /pet:
    get:
      responses: {}
"#,
        );

        let entry = route(&registry, Method::Get, "/pet");
        assert_eq!(entry.kind, RouteKind::ReadMany);
        assert_eq!(entry.resource_name, "pet");
        assert_eq!(entry.matched_policy, Some(RoutingPolicy::FlatCrud));
        assert_eq!(entry.effective_resource_name.as_deref(), Some("pets"));
        assert_eq!(entry.response_fields, vec!["id"]);
    }

    #[test]
    fn child_object_response_get_routes_as_singleton() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    ArtifactMetaData:
      type: object
      properties:
        id: { type: string }
        globalId: { type: integer }
paths:
  /artifacts/{artifactId}/meta:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ArtifactMetaData"
    post:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ArtifactMetaData"
"##,
        );

        assert_eq!(
            route(&registry, Method::Get, "/artifacts/{artifactId}/meta").kind,
            RouteKind::ReadOne
        );
        assert_eq!(
            route(&registry, Method::Post, "/artifacts/{artifactId}/meta").kind,
            RouteKind::Create
        );
    }

    #[test]
    fn root_object_response_get_routes_as_singleton() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Folder:
      type: object
      properties:
        Name: { type: string }
        Id: { type: string }
paths:
  /Inbox:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Folder"
"##,
        );

        let inbox = route(&registry, Method::Get, "/Inbox");
        assert_eq!(inbox.kind, RouteKind::ReadOne);
        assert_eq!(inbox.effective_resource_name.as_deref(), Some("folders"));
        assert_eq!(inbox.response_fields, vec!["Id", "Name"]);
    }

    #[test]
    fn multi_literal_object_response_get_routes_as_singleton() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    InstallStatus:
      type: object
      properties:
        status: { type: object }
paths:
  /crx/packmgr/installstatus.jsp:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/InstallStatus"
"##,
        );

        let install_status = route(&registry, Method::Get, "/crx/packmgr/installstatus.jsp");
        assert_eq!(install_status.kind, RouteKind::ReadOne);
        assert_eq!(
            install_status.effective_resource_name.as_deref(),
            Some("installstatuss")
        );
        assert_eq!(install_status.response_fields, vec!["status"]);
    }

    #[test]
    fn kubernetes_discovery_endpoint_routes_as_non_crud_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    APIResource:
      type: object
      properties:
        name: { type: string }
    APIResourceList:
      type: object
      required: [groupVersion, resources]
      properties:
        apiVersion: { type: string }
        groupVersion: { type: string }
        kind: { type: string }
        resources:
          type: array
          items:
            $ref: "#/components/schemas/APIResource"
paths:
  /api/v1/:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/APIResourceList"
"##,
        );

        let route = route(&registry, Method::Get, "/api/v1/");
        assert!(matches!(
            route.kind,
            RouteKind::Refusal { ref detail }
                if detail.contains("static discovery-style object response")
        ));
    }

    #[test]
    fn kubernetes_watch_endpoint_routes_as_non_crud_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    RawExtension:
      type: object
      additionalProperties: true
    WatchEvent:
      type: object
      required: [type, object]
      properties:
        type: { type: string }
        object:
          $ref: "#/components/schemas/RawExtension"
paths:
  /api/v1/watch/configmaps:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/WatchEvent"
"##,
        );

        let route = route(&registry, Method::Get, "/api/v1/watch/configmaps");
        assert!(matches!(
            route.kind,
            RouteKind::Refusal { ref detail } if detail.contains("watch/stream response")
        ));
    }

    #[test]
    fn multi_literal_terminal_param_object_response_get_routes_as_singleton() {
        let registry = registry(
            r##"
openapi: 3.1.0
components:
  schemas:
    PostureIntegration:
      type: object
      required: [id]
      properties:
        id: { type: string }
        provider: { type: string }
paths:
  /posture/integrations/{id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/PostureIntegration"
"##,
        );

        let integration = route(&registry, Method::Get, "/posture/integrations/{id}");
        assert_eq!(integration.kind, RouteKind::ReadOne);
        assert_eq!(
            integration.effective_resource_name.as_deref(),
            Some("postureintegrations")
        );
        assert_eq!(integration.response_fields, vec!["id", "provider"]);
    }

    #[test]
    fn child_wrapper_object_response_stays_collection_and_records_wrapper() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Version:
      type: object
      properties:
        id: { type: string }
    VersionSearchResults:
      type: object
      required: [count, versions]
      properties:
        count: { type: integer }
        versions:
          type: array
          items:
            $ref: "#/components/schemas/Version"
paths:
  /artifacts/{artifactId}/versions:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/VersionSearchResults"
"##,
        );

        let route = route(&registry, Method::Get, "/artifacts/{artifactId}/versions");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("versions"),
                payload_shape: ResponseWrapperPayloadShape::Array,
                count_field: Some(String::from("count")),
                status_field: None,
                status_code_field: None,
                message_field: None,
                static_fields: Vec::new(),
            })
        );
    }

    #[test]
    fn component_paginated_wrapper_response_records_static_metadata_fields() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    LegacyUser:
      type: object
      properties:
        id: { type: integer }
    LegacyUserListResponse:
      type: object
      required: [page, per_page, total, total_pages, data]
      properties:
        page: { type: integer }
        per_page: { type: integer }
        total: { type: integer }
        total_pages: { type: integer }
        data:
          type: array
          items:
            $ref: "#/components/schemas/LegacyUser"
        support:
          type: object
          additionalProperties: true
paths:
  /api/users:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/LegacyUserListResponse"
"##,
        );

        let route = route(&registry, Method::Get, "/api/users");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("data"),
                payload_shape: ResponseWrapperPayloadShape::Array,
                count_field: Some(String::from("total")),
                status_field: None,
                status_code_field: None,
                message_field: None,
                static_fields: vec![
                    ResponseWrapperStaticField {
                        name: String::from("page"),
                        value: ResponseWrapperStaticValue::Integer(0),
                    },
                    ResponseWrapperStaticField {
                        name: String::from("per_page"),
                        value: ResponseWrapperStaticValue::Integer(0),
                    },
                    ResponseWrapperStaticField {
                        name: String::from("support"),
                        value: ResponseWrapperStaticValue::Object,
                    },
                    ResponseWrapperStaticField {
                        name: String::from("total_pages"),
                        value: ResponseWrapperStaticValue::Integer(0),
                    },
                ],
            })
        );
    }

    #[test]
    fn component_data_only_wrapper_response_records_wrapper() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Terminal:
      type: object
      properties:
        id: { type: string }
        status: { type: string }
    ListTerminalsResponse:
      type: object
      properties:
        data:
          type: array
          items:
            $ref: "#/components/schemas/Terminal"
paths:
  /terminals:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ListTerminalsResponse"
"##,
        );

        let route = route(&registry, Method::Get, "/terminals");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("data"),
                payload_shape: ResponseWrapperPayloadShape::Array,
                count_field: None,
                status_field: None,
                status_code_field: None,
                message_field: None,
                static_fields: Vec::new(),
            })
        );
    }

    #[test]
    fn component_entries_wrapper_response_records_wrapper() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    User:
      type: object
      properties:
        id: { type: string }
        name: { type: string }
    Users:
      type: object
      allOf:
        - type: object
          properties:
            limit: { type: integer }
            next_marker:
              type: string
              nullable: true
        - type: object
          properties:
            total_count: { type: integer }
            limit: { type: integer }
            offset: { type: integer }
            order:
              type: array
              items:
                type: object
                properties:
                  by: { type: string }
                  direction: { type: string }
        - type: object
          properties:
            entries:
              type: array
              items:
                $ref: "#/components/schemas/User"
paths:
  /users:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Users"
"##,
        );

        let route = route(&registry, Method::Get, "/users");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("entries"),
                payload_shape: ResponseWrapperPayloadShape::Array,
                count_field: Some(String::from("total_count")),
                status_field: None,
                status_code_field: None,
                message_field: None,
                static_fields: vec![
                    ResponseWrapperStaticField {
                        name: String::from("limit"),
                        value: ResponseWrapperStaticValue::Integer(0),
                    },
                    ResponseWrapperStaticField {
                        name: String::from("next_marker"),
                        value: ResponseWrapperStaticValue::String(String::new()),
                    },
                    ResponseWrapperStaticField {
                        name: String::from("offset"),
                        value: ResponseWrapperStaticValue::Integer(0),
                    },
                    ResponseWrapperStaticField {
                        name: String::from("order"),
                        value: ResponseWrapperStaticValue::Array,
                    },
                ],
            })
        );
    }

    #[test]
    fn nested_component_response_wrapper_routes_as_collection() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Availability:
      type: object
      properties:
        pid: { type: string }
    Deprecations:
      type: object
      properties: {}
    Episode:
      type: object
      properties:
        pid: { type: string }
    Filters:
      type: object
      properties: {}
    Nitro:
      type: object
      properties:
        deprecations:
          $ref: "#/components/schemas/Deprecations"
        filters:
          $ref: "#/components/schemas/Filters"
        pagination:
          $ref: "#/components/schemas/Pagination"
        results:
          $ref: "#/components/schemas/Results"
        sorts:
          $ref: "#/components/schemas/Sorts"
    Pagination:
      type: object
      properties:
        page: { type: integer }
        page_size: { type: integer }
    Results:
      type: object
      properties:
        availability:
          $ref: "#/components/schemas/Availability"
        episode:
          $ref: "#/components/schemas/Episode"
        page: { type: integer }
        page_size: { type: integer }
        total: { type: integer }
    Sorts:
      type: object
      properties: {}
paths:
  /availabilities:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Nitro"
  /v1/episodes/{pid}/ancestors:
    get:
      parameters:
        - name: pid
          in: path
          required: true
          schema: { type: string }
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Nitro"
"##,
        );

        for path in ["/availabilities", "/v1/episodes/{pid}/ancestors"] {
            let route = route(&registry, Method::Get, path);
            assert_eq!(route.kind, RouteKind::ReadMany, "{path}");
            assert_eq!(route.response_resource_name.as_deref(), Some("nitros"));
            let wrapper = route
                .response_wrapper
                .as_ref()
                .expect("nested results envelope should be treated as response wrapper");
            assert_eq!(wrapper.array_field, "results");
            assert_eq!(wrapper.count_field, None);
            assert_eq!(
                wrapper
                    .static_fields
                    .iter()
                    .map(|field| field.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["deprecations", "filters", "pagination", "sorts"]
            );
        }
    }

    #[test]
    fn inline_wrapper_object_response_records_wrapper_static_fields() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Product:
      type: object
      properties:
        id: { type: integer }
    Products:
      type: array
      items:
        $ref: "#/components/schemas/Product"
paths:
  /products:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                required: [credit, products]
                properties:
                  credit: { type: number }
                  products:
                    $ref: "#/components/schemas/Products"
"##,
        );

        let route = route(&registry, Method::Get, "/products");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("products"),
                payload_shape: ResponseWrapperPayloadShape::Array,
                count_field: None,
                status_field: None,
                status_code_field: None,
                message_field: None,
                static_fields: vec![ResponseWrapperStaticField {
                    name: String::from("credit"),
                    value: ResponseWrapperStaticValue::Integer(0),
                }],
            })
        );
    }

    #[test]
    fn inline_wrapper_uses_single_array_payload_when_path_hint_differs() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Message:
      type: object
      properties:
        ts: { type: string }
        text: { type: string }
paths:
  /conversations.history:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                required: [ok, messages]
                properties:
                  ok: { type: boolean }
                  channel_actions_ts:
                    items:
                      type: integer
                  has_more: { type: boolean }
                  messages:
                    type: array
                    items:
                      $ref: "#/components/schemas/Message"
"##,
        );

        let route = route(&registry, Method::Get, "/conversations.history");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(route.effective_resource_name.as_deref(), Some("messages"));
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("messages"),
                payload_shape: ResponseWrapperPayloadShape::Array,
                count_field: None,
                status_field: None,
                status_code_field: None,
                message_field: None,
                static_fields: vec![
                    ResponseWrapperStaticField {
                        name: String::from("channel_actions_ts"),
                        value: ResponseWrapperStaticValue::Null,
                    },
                    ResponseWrapperStaticField {
                        name: String::from("has_more"),
                        value: ResponseWrapperStaticValue::Boolean(false),
                    },
                    ResponseWrapperStaticField {
                        name: String::from("ok"),
                        value: ResponseWrapperStaticValue::Boolean(false),
                    },
                ],
            })
        );
    }

    #[test]
    fn inline_singleton_wrapper_routes_as_read_one_and_records_wrapper() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    defs_ok_true:
      type: boolean
    objs_team:
      type: object
      properties:
        id: { type: string }
        name: { type: string }
paths:
  /team.info:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                required: [ok, team]
                properties:
                  ok:
                    $ref: "#/components/schemas/defs_ok_true"
                  team:
                    $ref: "#/components/schemas/objs_team"
"##,
        );

        let route = route(&registry, Method::Get, "/team.info");
        assert_eq!(route.kind, RouteKind::ReadOne);
        assert_eq!(route.effective_resource_name.as_deref(), Some("objs_teams"));
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("team"),
                payload_shape: ResponseWrapperPayloadShape::Object,
                count_field: None,
                status_field: None,
                status_code_field: None,
                message_field: None,
                static_fields: vec![ResponseWrapperStaticField {
                    name: String::from("ok"),
                    value: ResponseWrapperStaticValue::Null,
                }],
            })
        );
    }

    #[test]
    fn inline_data_array_response_routes_schema_first_collection() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    ProjectCompact:
      type: object
      properties:
        gid: { type: string }
        name: { type: string }
paths:
  /projects:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                properties:
                  data:
                    type: array
                    items:
                      $ref: "#/components/schemas/ProjectCompact"
"##,
        );

        let route = route(&registry, Method::Get, "/projects");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(route.matched_policy, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(
            route.effective_resource_name.as_deref(),
            Some("projectcompacts")
        );
        assert_eq!(route.response_fields, vec!["gid", "name"]);
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("data"),
                payload_shape: ResponseWrapperPayloadShape::Array,
                count_field: None,
                status_field: None,
                status_code_field: None,
                message_field: None,
                static_fields: Vec::new(),
            })
        );
    }

    #[test]
    fn component_response_ref_routes_schema_first_collection() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  responses:
    GetWebhookEventLogsResponse:
      description: EventLogs
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/GetWebhookEventLogsResponse"
  schemas:
    WebhookEventLog:
      type: object
      properties:
        id: { type: string }
    GetWebhookEventLogsResponse:
      type: object
      required: [status_code, status, data]
      properties:
        data:
          type: array
          items:
            $ref: "#/components/schemas/WebhookEventLog"
        status: { type: string }
        status_code: { type: integer }
paths:
  /webhook/logs:
    get:
      responses:
        "200":
          $ref: "#/components/responses/GetWebhookEventLogsResponse"
"##,
        );

        let route = route(&registry, Method::Get, "/webhook/logs");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(route.matched_policy, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(
            route.response_schema_ref.as_deref(),
            Some("#/components/schemas/GetWebhookEventLogsResponse")
        );
        assert_eq!(
            route.response_resource_name.as_deref(),
            Some("getwebhookeventlogsresponses")
        );
        assert_eq!(
            route.response_wrapper,
            Some(ResponseWrapper {
                array_field: String::from("data"),
                payload_shape: ResponseWrapperPayloadShape::Array,
                count_field: None,
                status_field: Some(String::from("status")),
                status_code_field: Some(String::from("status_code")),
                message_field: None,
                static_fields: Vec::new(),
            })
        );
    }

    #[test]
    fn schema_first_read_one_without_pk_or_path_lookup_is_route_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    AudioAnalysisObject:
      type: object
      properties:
        bars:
          type: array
          items: { type: object }
        beats:
          type: array
          items: { type: object }
        track:
          type: object
paths:
  /audio-analysis/{id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/AudioAnalysisObject"
"##,
        );

        let route = route(&registry, Method::Get, "/audio-analysis/{id}");
        assert!(matches!(
            route.kind,
            RouteKind::Refusal { ref detail }
                if detail.contains("audioanalysisobjects")
                    && detail.contains("no primary key")
                    && detail.contains("no path parameter can materialize a declared column")
                    && detail.contains("/audio-analysis/{id}")
        ));
        assert_eq!(route.matched_policy, None);
    }

    #[test]
    fn root_object_response_with_incidental_array_field_is_not_a_wrapper() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Metrics:
      type: object
      required: [numSpecs]
      properties:
        datasets:
          type: array
          items: {}
        numSpecs: { type: integer }
        numAPIs: { type: integer }
        numEndpoints: { type: integer }
paths:
  /metrics.json:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Metrics"
"##,
        );

        let route = route(&registry, Method::Get, "/metrics.json");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(route.matched_policy, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(route.effective_resource_name.as_deref(), Some("metricss"));
        assert_eq!(route.response_wrapper, None);
    }

    #[test]
    fn child_singleton_component_with_incidental_array_field_is_not_a_wrapper() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Person:
      type: object
      properties:
        account: { type: string }
        id: { type: string }
        full_name_aliases:
          type: array
          items: { type: string }
paths:
  /v1/accounts/{account}/people/{person}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Person"
"##,
        );

        let route = route(
            &registry,
            Method::Get,
            "/v1/accounts/{account}/people/{person}",
        );
        assert_eq!(route.kind, RouteKind::ReadOne);
        assert_eq!(route.effective_resource_name.as_deref(), Some("persons"));
        assert_eq!(route.response_wrapper, None);
    }

    #[test]
    fn child_singleton_component_with_standard_data_map_is_not_a_wrapper() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    ConfigMap:
      type: object
      properties:
        apiVersion: { type: string }
        kind: { type: string }
        metadata:
          type: object
          additionalProperties: true
        data:
          type: object
          additionalProperties:
            type: string
        immutable: { type: boolean }
paths:
  /api/v1/namespaces/{namespace}/configmaps/{name}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ConfigMap"
"##,
        );

        let route = route(
            &registry,
            Method::Get,
            "/api/v1/namespaces/{namespace}/configmaps/{name}",
        );
        assert_eq!(route.kind, RouteKind::ReadOne);
        assert_eq!(route.effective_resource_name.as_deref(), Some("configmaps"));
        assert_eq!(route.response_wrapper, None);
    }

    #[test]
    fn inline_object_response_schema_gets_precise_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
paths:
  /providers.json:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                required: [data]
                properties:
                  data:
                    type: array
                    items:
                      type: string
"##,
        );

        let route = route(&registry, Method::Get, "/providers.json");
        assert_eq!(
            route.kind,
            RouteKind::Refusal {
                detail: String::from(
                    "REST route uses an inline response object wrapper schema, which is not materialized as an executable REST resource in v0. Path: /providers.json"
                )
            }
        );
    }

    #[test]
    fn object_map_response_schema_gets_precise_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    API:
      type: object
      properties:
        title: { type: string }
    APIs:
      type: object
      additionalProperties:
        $ref: "#/components/schemas/API"
paths:
  /list.json:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/APIs"
"##,
        );

        let route = route(&registry, Method::Get, "/list.json");
        assert_eq!(
            route.kind,
            RouteKind::Refusal {
                detail: String::from(
                    "REST route uses object-map response schema `APIs`, which is not materialized as executable REST resource rows in v0. Path: /list.json"
                )
            }
        );
    }

    #[test]
    fn scalar_component_response_schema_gets_precise_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    AddCouponRequest:
      type: object
      properties:
        coupon_code: { type: string }
        amount: { type: string }
    AddCouponResponse:
      type: string
paths:
  /addcouponrequests:
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/AddCouponRequest"
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/AddCouponResponse"
"##,
        );

        let route = route(&registry, Method::Post, "/addcouponrequests");
        assert_eq!(
            route.response_resource_name.as_deref(),
            Some("addcouponresponses")
        );
        assert_eq!(
            route.kind,
            RouteKind::Refusal {
                detail: String::from(
                    "REST route declares scalar response resource `addcouponresponses`, which is not materialized as an executable REST response in v0. Path: /addcouponrequests"
                )
            }
        );
    }

    #[test]
    fn no_body_action_post_with_generated_response_fields_gets_precise_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    GenerateApiKeyResponse:
      type: object
      required: [apiKey]
      properties:
        apiKey: { type: string }
paths:
  /companies/{companyId}/apiCredentials/{apiCredentialId}/generateApiKey:
    post:
      parameters:
        - name: companyId
          in: path
          required: true
          schema: { type: string }
        - name: apiCredentialId
          in: path
          required: true
          schema: { type: string }
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/GenerateApiKeyResponse"
"##,
        );

        let route = route(
            &registry,
            Method::Post,
            "/companies/{companyId}/apiCredentials/{apiCredentialId}/generateApiKey",
        );
        assert_eq!(
            route.response_resource_name.as_deref(),
            Some("generateapikeyresponses")
        );
        assert_eq!(
            route.kind,
            RouteKind::Refusal {
                detail: String::from(
                    "REST route is an action-style POST with no request body and generated response field(s) [apiKey] on resource `generateapikeyresponses`; REST twin v0 cannot synthesize those values. Path: /companies/{companyId}/apiCredentials/{apiCredentialId}/generateApiKey"
                )
            }
        );
    }

    #[test]
    fn no_body_action_post_with_unsynthesizable_required_fields_gets_precise_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    BulkImport:
      type: object
      required: [id, status]
      properties:
        id: { type: integer }
        status: { type: string }
paths:
  /bulk_imports/{import_id}/cancel:
    post:
      parameters:
        - name: import_id
          in: path
          required: true
          schema: { type: integer }
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/BulkImport"
"##,
        );

        let route = route(&registry, Method::Post, "/bulk_imports/{import_id}/cancel");
        assert_eq!(route.response_resource_name.as_deref(), Some("bulkimports"));
        assert_eq!(
            route.kind,
            RouteKind::Refusal {
                detail: String::from(
                    "REST route is an action-style POST with no request body and required response field(s) [id, status] on resource `bulkimports`; REST twin v0 cannot synthesize those values. Path: /bulk_imports/{import_id}/cancel"
                )
            }
        );
    }

    #[test]
    fn polymorphic_component_response_schema_routes_as_object_resource() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    bank_account:
      type: object
      required: [id, object, routing_number]
      properties:
        available_payout_methods:
          type: array
          items: { type: string }
        id: { type: string }
        object: { type: string }
        routing_number: { type: string }
        status: { type: string }
    card:
      type: object
      required: [id, object, brand]
      properties:
        available_payout_methods:
          type: array
          items: { type: string }
        id: { type: string }
        object: { type: string }
        brand: { type: string }
        status: { type: string }
    external_account:
      anyOf:
        - $ref: "#/components/schemas/bank_account"
        - $ref: "#/components/schemas/card"
paths:
  /v1/accounts/{account}/bank_accounts/{id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/external_account"
"##,
        );

        let route = route(
            &registry,
            Method::Get,
            "/v1/accounts/{account}/bank_accounts/{id}",
        );
        assert_eq!(route.kind, RouteKind::ReadOne);
        assert_eq!(
            route.response_resource_name.as_deref(),
            Some("external_accounts")
        );
        assert_eq!(
            route.response_fields,
            vec![
                "available_payout_methods",
                "brand",
                "id",
                "object",
                "routing_number",
                "status"
            ]
        );
        assert_eq!(route.response_wrapper, None);
    }

    #[test]
    fn flat_crud_base_prefix_classifies_prefixed_paths() {
        let config = RoutingConfig {
            policy: RoutingPolicy::FlatCrud,
            base_prefix: Some("/api".to_owned()),
            ..RoutingConfig::default()
        };
        let registry = registry_with_config(
            r#"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths:
  /api/pets:
    get:
      responses: {}
  /api/pets/{id}:
    get:
      responses: {}
"#,
            &config,
        );

        let read_many = route(&registry, Method::Get, "/api/pets");
        assert_eq!(read_many.kind, RouteKind::ReadMany);
        assert_eq!(read_many.resource_name, "api");
        assert_eq!(read_many.effective_resource_name.as_deref(), Some("pets"));

        let read_one = route(&registry, Method::Get, "/api/pets/{id}");
        assert_eq!(read_one.kind, RouteKind::ReadOne);
        assert_eq!(read_one.path_params, vec!["id"]);
        assert_eq!(read_one.matched_policy, Some(RoutingPolicy::FlatCrud));
        assert_eq!(read_one.effective_resource_name.as_deref(), Some("pets"));
    }

    #[test]
    fn auto_policy_uses_prefix_scoped_classifier_when_prefix_detected() {
        let registry = registry(
            r#"
openapi: 3.0.3
components:
  schemas:
    User:
      type: object
      properties:
        id: { type: integer }
    Unknown:
      type: object
      properties:
        id: { type: integer }
paths:
  /api/users:
    get:
      responses: {}
  /api/users/{id}:
    get:
      responses: {}
  /api/unknown:
    get:
      responses: {}
"#,
        );

        let read_many = route(&registry, Method::Get, "/api/users");
        assert_eq!(read_many.kind, RouteKind::ReadMany);
        assert_eq!(read_many.matched_policy, Some(RoutingPolicy::PrefixScoped));
        assert_eq!(read_many.effective_resource_name.as_deref(), Some("users"));

        let read_one = route(&registry, Method::Get, "/api/users/{id}");
        assert_eq!(read_one.kind, RouteKind::ReadOne);
        assert_eq!(read_one.matched_policy, Some(RoutingPolicy::PrefixScoped));
        assert_eq!(read_one.effective_resource_name.as_deref(), Some("users"));

        let unknown = route(&registry, Method::Get, "/api/unknown");
        assert_eq!(unknown.kind, RouteKind::ReadMany);
        assert_eq!(unknown.effective_resource_name.as_deref(), Some("unknowns"));
    }

    #[test]
    fn prefixed_plural_root_object_response_stays_collection() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Customer:
      type: object
      properties:
        id: { type: string }
        email: { type: string }
    Price:
      type: object
      properties:
        id: { type: string }
paths:
  /v1/customers:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Customer"
  /v1/customers/{customer}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Customer"
  /v1/prices:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Price"
"##,
        );

        let customers = route(&registry, Method::Get, "/v1/customers");
        assert_eq!(customers.kind, RouteKind::ReadMany);
        assert_eq!(
            customers.effective_resource_name.as_deref(),
            Some("customers")
        );

        let customer = route(&registry, Method::Get, "/v1/customers/{customer}");
        assert_eq!(customer.kind, RouteKind::ReadOne);
    }

    #[test]
    fn prefixed_child_collection_object_response_stays_collection() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Customer:
      type: object
      properties:
        id: { type: string }
    PaymentMethod:
      type: object
      properties:
        id: { type: string }
        type: { type: string }
paths:
  /v1/customers:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Customer"
  /v1/customers/{customer}/payment_methods:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/PaymentMethod"
  /v1/customers/{customer}/payment_methods/{payment_method}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/PaymentMethod"
"##,
        );

        let payment_methods = route(
            &registry,
            Method::Get,
            "/v1/customers/{customer}/payment_methods",
        );
        assert_eq!(payment_methods.kind, RouteKind::ReadMany);
        assert_eq!(
            payment_methods.effective_resource_name.as_deref(),
            Some("paymentmethods")
        );

        let payment_method = route(
            &registry,
            Method::Get,
            "/v1/customers/{customer}/payment_methods/{payment_method}",
        );
        assert_eq!(payment_method.kind, RouteKind::ReadOne);
    }

    #[test]
    fn classify_uses_topology_first() {
        let catalog = catalog(
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
        );
        let topology =
            super::super::topology::build_spec_topology(&catalog, &RoutingConfig::default());
        let registry = registry_with_topology(&catalog, &topology, &RoutingConfig::default());

        let route = route(&registry, Method::Get, "/pets");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(route.matched_policy, Some(RoutingPolicy::FlatCrud));
        assert_eq!(route.effective_resource_name.as_deref(), Some("pets"));
        assert!(
            registry
                .warnings
                .iter()
                .any(|warning| { warning.contains("[rest] topology: 0 pinned, 1 high, 0 medium") })
        );
    }

    #[test]
    fn classify_falls_back_to_waterfall() {
        let catalog = catalog(
            r#"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      properties:
        id: { type: integer }
paths:
  /pet:
    get:
      responses: {}
"#,
        );
        let empty_topology = ResourceTopology {
            path_bindings: std::collections::HashMap::new(),
            prefix_scopes: Vec::new(),
            warnings: Vec::new(),
        };
        let registry = registry_with_topology(&catalog, &empty_topology, &RoutingConfig::default());

        let route = route(&registry, Method::Get, "/pet");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(route.matched_policy, Some(RoutingPolicy::FlatCrud));
        assert_eq!(route.effective_resource_name.as_deref(), Some("pets"));
        assert!(
            registry
                .warnings
                .iter()
                .any(|warning| { warning.contains("waterfall fallback: 1") })
        );
    }

    #[test]
    fn response_fields_include_json_and_array_returning_columns() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      properties:
        id: { type: string }
        payload: { type: object }
        raw: { type: string, format: binary }
        tags:
          type: array
          items: { type: string }
        name: { type: string }
        ratio: { type: number }
paths:
  /files:
    post:
      responses:
        "201":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/File"
"##,
        );

        assert_eq!(
            route(&registry, Method::Post, "/files").response_fields,
            vec!["id", "name", "payload", "ratio", "tags"]
        );
    }

    #[test]
    fn mutation_response_with_unsupported_fields_gets_precise_refusal() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      properties:
        id: { type: string }
        payload: { type: object }
        raw: { type: string, format: binary }
        tags:
          type: array
          items: { type: string }
        name: { type: string }
        ratio: { type: number }
paths:
  /files/{id}/cancel:
    post:
      parameters:
        - name: id
          in: path
          required: true
          schema: { type: string }
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/File"
"##,
        );

        let route = route(&registry, Method::Post, "/files/{id}/cancel");
        assert_eq!(
            route.kind,
            RouteKind::Refusal {
                detail: String::from(
                    "REST route response resource `files` declares unsupported response field(s) [raw (bytes)]; REST twin v0 cannot materialize nested, array, or binary response fields without returning a partial body. Path: /files/{id}/cancel"
                )
            }
        );
        assert_eq!(
            route.response_fields,
            vec!["id", "name", "payload", "ratio", "tags"]
        );
    }

    #[test]
    fn route_entry_defaults() {
        let registry = registry(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      properties:
        id: { type: string }
paths:
  /unknown:
    get:
      responses: {}
"#,
        );

        let entry = route(&registry, Method::Get, "/unknown");
        assert_eq!(entry.response_resource_name, None);
        assert_eq!(entry.matched_policy, None);
        assert_eq!(entry.effective_resource_name, None);
    }

    #[test]
    fn route_entry_has_response_resource_name() {
        let registry = registry(
            r##"
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
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pet"
"##,
        );

        let entry = route(&registry, Method::Get, "/pets");
        assert_eq!(
            entry.response_schema_ref.as_deref(),
            Some("#/components/schemas/Pet")
        );
        assert_eq!(entry.response_resource_name.as_deref(), Some("pets"));
        assert_eq!(entry.matched_policy, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(entry.effective_resource_name.as_deref(), Some("pets"));
    }

    #[test]
    fn response_fields_fall_back_from_array_wrappers_and_ignore_default_error_schema() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Pet:
      required: [id, name]
      properties:
        id: { type: integer, format: int64 }
        name: { type: string }
        tag: { type: string }
    Pets:
      type: array
      items:
        $ref: "#/components/schemas/Pet"
    Error:
      required: [code, message]
      properties:
        code: { type: integer }
        message: { type: string }
paths:
  /pets:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pets"
    post:
      responses:
        "201":
          description: created
        default:
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Error"
"##,
        );

        assert_eq!(
            route(&registry, Method::Get, "/pets").response_fields,
            vec!["id", "name", "tag"]
        );
        assert_eq!(
            route(&registry, Method::Post, "/pets").response_schema_ref,
            None
        );
        assert_eq!(
            route(&registry, Method::Post, "/pets").response_fields,
            vec!["id", "name", "tag"]
        );
    }

    #[test]
    fn request_body_schema_refs_are_attached_to_mutation_routes() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  requestBodies:
    TodoPatchBody:
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/TodoPatch"
  schemas:
    Todo:
      type: object
      required: [id, title]
      properties:
        id: { type: string }
        title: { type: string }
    NewTodo:
      type: object
      required: [title]
      properties:
        title: { type: string }
    TodoPatch:
      type: object
      properties:
        title: { type: string }
paths:
  /todos:
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/NewTodo"
      responses:
        "201":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Todo"
  /todos/{id}:
    put:
      requestBody:
        $ref: "#/components/requestBodies/TodoPatchBody"
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Todo"
"##,
        );

        let create = route(&registry, Method::Post, "/todos");
        assert_eq!(
            create.request_schema_ref.as_deref(),
            Some("#/components/schemas/NewTodo")
        );
        assert_eq!(create.request_resource_name.as_deref(), Some("newtodos"));

        let update = route(&registry, Method::Put, "/todos/{id}");
        assert_eq!(
            update.request_schema_ref.as_deref(),
            Some("#/components/schemas/TodoPatch")
        );
        assert_eq!(update.request_resource_name.as_deref(), Some("todopatchs"));
    }

    #[test]
    fn classify_nested_path_reaches_topology() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Deal:
      type: object
      required: [id]
      properties:
        id: { type: string }
    Loan:
      type: object
      required: [id]
      properties:
        id: { type: string }
paths:
  /deals/{deal_id}/loans/{loan_id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Loan"
"##,
        );

        let route = route(&registry, Method::Get, "/deals/{deal_id}/loans/{loan_id}");
        assert_eq!(route.kind, RouteKind::ReadOne);
        assert_eq!(route.matched_policy, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(route.effective_resource_name.as_deref(), Some("loans"));
        assert!(!registry.warnings.iter().any(|warning| {
            warning.contains("Nested resource paths not supported in REST twin v0")
        }));
    }

    #[test]
    fn path_filter_with_array_response_classifies_as_read_many() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Association:
      type: object
      properties:
        ObjectId: { type: string }
        FileId: { type: string }
paths:
  /Associations/{ObjectId}:
    get:
      parameters:
        - name: ObjectId
          in: path
          required: true
          schema: { type: string }
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Association"
"##,
        );

        let route = route(&registry, Method::Get, "/Associations/{ObjectId}");
        assert_eq!(route.kind, RouteKind::ReadMany);
        assert_eq!(route.matched_policy, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(
            route.effective_resource_name.as_deref(),
            Some("associations")
        );
    }

    #[test]
    fn classify_parent_scoped_collection_reaches_topology() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Vault:
      type: object
      required: [id]
      properties:
        id: { type: string }
    Item:
      type: object
      required: [id]
      properties:
        id: { type: string }
paths:
  /vaults/{vaultUuid}/items:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Item"
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/Item"
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Item"
"##,
        );

        let read_many = route(&registry, Method::Get, "/vaults/{vaultUuid}/items");
        assert_eq!(read_many.kind, RouteKind::ReadMany);
        assert_eq!(read_many.matched_policy, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(read_many.effective_resource_name.as_deref(), Some("items"));

        let create = route(&registry, Method::Post, "/vaults/{vaultUuid}/items");
        assert_eq!(create.kind, RouteKind::Create);
        assert_eq!(create.matched_policy, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(create.effective_resource_name.as_deref(), Some("items"));
        assert!(!registry.warnings.iter().any(|warning| {
            warning.contains(
                "REST route is outside the CRUD route subset in v0. Path: /vaults/{vaultUuid}/items",
            )
        }));
    }

    #[test]
    fn child_path_shape_beats_parent_flat_candidate_when_response_is_wrapper() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Collection:
      type: object
      required: [id]
      properties:
        id: { type: string }
    Item:
      type: object
      required: [id, type, stac_version]
      properties:
        id: { type: string }
        type: { type: string }
        stac_version: { type: string }
    ItemCollection:
      type: object
      properties:
        items:
          type: array
          items:
            $ref: "#/components/schemas/Item"
  responses:
    Features:
      description: feature collection
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/ItemCollection"
paths:
  /collections/{collectionId}/items:
    get:
      responses:
        "200":
          $ref: "#/components/responses/Features"
    post:
      requestBody:
        content:
          application/json:
            schema:
              oneOf:
                - $ref: "#/components/schemas/Item"
                - $ref: "#/components/schemas/ItemCollection"
      responses:
        "201":
          description: created
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Item"
"##,
        );

        let read_many = route(&registry, Method::Get, "/collections/{collectionId}/items");
        assert_eq!(read_many.effective_resource_name.as_deref(), Some("items"));

        let create = route(&registry, Method::Post, "/collections/{collectionId}/items");
        assert_eq!(create.kind, RouteKind::Create);
        assert_eq!(create.effective_resource_name.as_deref(), Some("items"));
        assert!(
            !registry
                .warnings
                .iter()
                .any(|warning| warning.contains("resource `collections`"))
        );
    }

    #[test]
    fn classify_prefix_path_reaches_topology() {
        let registry = registry(
            r#"
openapi: 3.0.3
components:
  schemas:
    Mapping:
      type: object
      required: [id]
      properties:
        id: { type: string }
paths:
  /__admin/mappings:
    get:
      responses: {}
    post:
      responses: {}
  /__admin/mappings/{stubMappingId}:
    get:
      responses: {}
    delete:
      responses: {}
  /__admin/reset:
    post:
      responses: {}
"#,
        );

        let read_many = route(&registry, Method::Get, "/__admin/mappings");
        assert_eq!(read_many.kind, RouteKind::ReadMany);
        assert_eq!(read_many.matched_policy, Some(RoutingPolicy::PrefixScoped));
        assert_eq!(
            read_many.effective_resource_name.as_deref(),
            Some("mappings")
        );

        assert_eq!(
            route(&registry, Method::Post, "/__admin/mappings").kind,
            RouteKind::Create
        );
        assert_eq!(
            route(&registry, Method::Get, "/__admin/mappings/{stubMappingId}").kind,
            RouteKind::ReadOne
        );
        assert_eq!(
            route(
                &registry,
                Method::Delete,
                "/__admin/mappings/{stubMappingId}"
            )
            .kind,
            RouteKind::Delete
        );

        assert_eq!(
            route(&registry, Method::Post, "/__admin/reset").kind,
            RouteKind::Refusal {
                detail: "REST route is outside the CRUD route subset in v0. Path: /__admin/reset"
                    .to_owned()
            }
        );
        assert!(!registry.warnings.iter().any(|warning| {
            warning.contains("CRUD-shaped routes behind fixed path prefixes are unsupported")
        }));
    }

    #[test]
    fn head_operations_are_preserved_as_declared_route_refusals() {
        let registry = registry(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id]
      properties:
        id: { type: string }
paths:
  /files/{id}:
    head:
      responses:
        "200":
          description: exists
"#,
        );

        let route = route(&registry, Method::Head, "/files/{id}");
        assert_eq!(
            route.kind,
            RouteKind::Refusal {
                detail: "REST route is outside the CRUD route subset in v0. Path: /files/{id}"
                    .to_owned()
            }
        );
        assert!(registry.warnings.iter().any(|warning| {
            warning.contains("REST route is outside the CRUD route subset in v0. Path: /files/{id}")
        }));
    }

    #[test]
    fn read_many_pagination_params_are_attached() {
        let registry = registry(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id]
      properties:
        id: { type: string }
paths:
  /files:
    parameters:
      - name: cursor
        in: query
    get:
      parameters:
        - name: limit
          in: query
        - name: include_archived
          in: query
      responses: {}
"#,
        );

        let route = route(&registry, Method::Get, "/files");
        assert_eq!(
            route.declared_query_params,
            vec!["cursor", "limit", "include_archived"]
        );
        assert_eq!(route.pagination, Some(PaginationStyle::Cursor));
        assert!(
            registry
                .warnings
                .iter()
                .any(|warning| { warning.contains("Pagination detected on /files: cursor") })
        );
    }

    #[test]
    fn query_api_key_security_scheme_is_allowed_for_auth_bypass() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  securitySchemes:
    api_key:
      type: apiKey
      in: query
      name: api_key
  schemas:
    File:
      type: object
      required: [id]
      properties:
        id: { type: string }
paths:
  /files:
    get:
      parameters:
        - name: limit
          in: query
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/File"
"##,
        );

        let route = route(&registry, Method::Get, "/files");
        assert_eq!(route.declared_query_params, vec!["api_key", "limit"]);
        assert_eq!(route.pagination, None);
    }

    #[test]
    fn component_parameter_refs_are_resolved_for_query_params() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  parameters:
    per-page:
      name: per_page
      in: query
      schema:
        type: integer
    page:
      name: page
      in: query
      schema:
        type: integer
    owner:
      name: owner
      in: path
      required: true
      schema:
        type: string
  schemas:
    Issue:
      type: object
      required: [id]
      properties:
        id: { type: integer }
paths:
  /repos/{owner}/issues:
    get:
      parameters:
        - $ref: "#/components/parameters/owner"
        - $ref: "#/components/parameters/per-page"
        - $ref: "#/components/parameters/page"
        - name: state
          in: query
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Issue"
"##,
        );

        let route = route(&registry, Method::Get, "/repos/{owner}/issues");
        assert_eq!(
            route.declared_query_params,
            vec!["per_page", "page", "state"]
        );
        assert_eq!(route.path_param_specs.len(), 1);
        assert_eq!(route.path_param_specs[0].name, "owner");
        assert_eq!(
            schema_type_string(
                route.path_param_specs[0]
                    .schema
                    .as_ref()
                    .expect("path parameter schema should resolve")
            )
            .as_deref(),
            Some("string")
        );
        assert_eq!(route.pagination, Some(PaginationStyle::PageNumber));
    }

    #[test]
    fn non_template_path_parameters_do_not_become_runtime_requirements() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    SbomScan:
      type: object
      required: [id]
      properties:
        id: { type: integer }
paths:
  /jobs/{id}/sbom_scans/{sbom_digest}:
    get:
      parameters:
        - name: id
          in: path
          required: true
          schema: { type: integer }
        - name: sbom_scan_id
          in: path
          required: true
          schema: { type: string }
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/SbomScan"
"##,
        );

        let route = route(
            &registry,
            Method::Get,
            "/jobs/{id}/sbom_scans/{sbom_digest}",
        );
        assert_eq!(route.path_params, vec!["id", "sbom_digest"]);
        assert_eq!(route.path_param_specs.len(), 1);
        assert_eq!(route.path_param_specs[0].name, "id");
        assert_eq!(
            schema_type_string(
                route.path_param_specs[0]
                    .schema
                    .as_ref()
                    .expect("template path parameter schema should remain attached")
            )
            .as_deref(),
            Some("integer")
        );
    }

    #[test]
    fn query_param_specs_preserve_required_schema_and_enum_refs() {
        let registry = registry(
            r##"
openapi: 3.0.3
components:
  schemas:
    Currency:
      type: string
      enum: [HUF, EUR]
    ConversationRate:
      type: object
      properties:
        conversation_rate: { type: number }
paths:
  /currencies:
    get:
      parameters:
        - name: from
          in: query
          required: true
          schema:
            $ref: "#/components/schemas/Currency"
        - name: to
          in: query
          required: true
          schema:
            $ref: "#/components/schemas/Currency"
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ConversationRate"
"##,
        );

        let route = route(&registry, Method::Get, "/currencies");
        assert_eq!(route.declared_query_params, vec!["from", "to"]);
        assert_eq!(route.query_params.len(), 2);
        assert!(route.query_params.iter().all(|param| param.required));
        let from_schema = route.query_params[0]
            .schema
            .as_ref()
            .expect("query schema should be preserved");
        assert_eq!(schema_type_string(from_schema).as_deref(), Some("string"));
        assert_eq!(
            from_schema.enum_values,
            vec![
                serde_json::Value::String(String::from("HUF")),
                serde_json::Value::String(String::from("EUR")),
            ]
        );
    }

    #[test]
    fn literal_patterns_win_over_path_params() {
        let registry = registry(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id]
      properties:
        id: { type: string }
paths:
  /files/{id}:
    get:
      responses: {}
  /files/health:
    get:
      responses: {}
"#,
        );

        let matched = match_route(&registry.routes, Method::Get, "/files/health")
            .expect("literal route should match");

        assert_eq!(
            matched.pattern,
            &PathPattern {
                segments: vec![
                    PathSegment::Literal("files".to_owned()),
                    PathSegment::Literal("health".to_owned()),
                ],
            }
        );
        assert!(matches!(matched.entry.kind, RouteKind::Refusal { .. }));

        let matched = match_route(&registry.routes, Method::Get, "/files/abc123")
            .expect("parameter route should match");
        assert_eq!(
            matched.path_params,
            BTreeMap::from([("id".to_owned(), "abc123".to_owned())])
        );
        assert_eq!(matched.entry.kind, RouteKind::ReadOne);
    }

    #[test]
    fn percent_encoded_literal_segments_match_openapi_literals() {
        let pattern = PathPattern::parse("/metadata_templates/schema#classifications");
        assert!(
            pattern
                .captures("/metadata_templates/schema%23classifications")
                .is_some()
        );

        let pattern = PathPattern::parse("/files/{file_id}/metadata/{scope}");
        let captures = pattern
            .captures("/files/file%201/metadata/enterprise%2Fstrict")
            .expect("encoded path params should match without changing capture encoding");
        assert_eq!(
            captures.get("file_id").map(String::as_str),
            Some("file%201")
        );
        assert_eq!(
            captures.get("scope").map(String::as_str),
            Some("enterprise%2Fstrict")
        );
    }

    #[test]
    fn param_suffix_patterns_match_declared_operation() {
        let registry = registry(
            r#"
openapi: 3.0.3
paths:
  /v1/{name}:
    get:
      responses: {}
  /v1/{name}:borrow:
    post:
      responses: {}
  /v1/{name}:return:
    post:
      responses: {}
"#,
        );

        let matched = match_route(
            &registry.routes,
            Method::Post,
            "/v1/shelves%2F1%2Fbooks%2F2:borrow",
        )
        .expect("declared borrow route should match");

        assert_eq!(
            matched.path_params,
            BTreeMap::from([("name".to_owned(), "shelves%2F1%2Fbooks%2F2".to_owned())])
        );
        assert_eq!(
            matched.entry.kind,
            RouteKind::Refusal {
                detail:
                    "REST route is outside the CRUD route subset in v0. Path: /v1/{name}:borrow"
                        .to_owned(),
            }
        );
        assert!(match_route(&registry.routes, Method::Post, "/v1/shelves").is_none());
    }
}
