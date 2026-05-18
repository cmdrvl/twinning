//! Resource topology inference for adaptive REST routing.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use super::{
    policy::{RoutingConfig, RoutingPolicy, resolve_routing_config},
    routes::{PathPattern, PathSegment},
    spec::{
        OperationObject, PathItem, RequestBodyObject, ResponseObject, RestCatalog, SchemaObject,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceTopology {
    pub path_bindings: HashMap<String, TopologyEntry>,
    pub prefix_scopes: Vec<PrefixScope>,
    pub warnings: Vec<TopologyWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyEntry {
    pub resource_name: String,
    pub confidence: Confidence,
    pub relationship: ResourceRelationship,
    pub winning_evidence: EvidenceSource,
    pub conflict: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Confidence {
    Pinned,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceRelationship {
    RootCollection,
    RootSingleton,
    ChildCollection,
    ChildSingleton,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvidenceSource {
    XTwinning,
    FlatCrud,
    SchemaFirst,
    PrefixScoped,
    Waterfall,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixScope {
    pub prefix: String,
    pub path_count: usize,
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyWarning {
    pub path: String,
    pub flat_crud_candidate: Option<String>,
    pub schema_first_candidate: Option<String>,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PrefixMatch {
    candidate: String,
    relationship: ResourceRelationship,
    prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopologyCandidates {
    flat: Option<String>,
    schema: Option<String>,
}

pub fn build_spec_topology(catalog: &RestCatalog, config: &RoutingConfig) -> ResourceTopology {
    let effective_config = effective_routing_config(catalog, config);
    let x_twinning_pinned = x_twinning_contributed_routing(catalog, config);
    let prefix_scopes = build_prefix_scopes(catalog, &effective_config);
    let mut path_bindings = HashMap::new();
    let mut warnings = Vec::new();

    let mut paths = catalog.paths.iter().collect::<Vec<_>>();
    paths.sort_by_key(|(path, _)| path.as_str());

    for (path, path_item) in paths {
        if let Some(entry) = topology_entry(
            catalog,
            &effective_config,
            &prefix_scopes,
            x_twinning_pinned,
            path,
            path_item,
            &mut warnings,
        ) {
            path_bindings.insert(path.clone(), entry);
        }
    }

    ResourceTopology {
        path_bindings,
        prefix_scopes,
        warnings,
    }
}

fn effective_routing_config(catalog: &RestCatalog, config: &RoutingConfig) -> RoutingConfig {
    let cli_policy = (config.policy != RoutingPolicy::Auto).then_some(config.policy);
    resolve_routing_config(
        cli_policy,
        config.base_prefix.clone(),
        catalog.x_twinning.as_ref(),
    )
}

fn x_twinning_contributed_routing(catalog: &RestCatalog, config: &RoutingConfig) -> bool {
    if config.policy != RoutingPolicy::Auto || config.base_prefix.is_some() {
        return false;
    }

    catalog
        .x_twinning
        .as_ref()
        .is_some_and(|extension| extension.routing.is_some() || extension.base_prefix.is_some())
}

fn topology_entry(
    catalog: &RestCatalog,
    config: &RoutingConfig,
    prefix_scopes: &[PrefixScope],
    x_twinning_pinned: bool,
    path: &str,
    path_item: &PathItem,
    warnings: &mut Vec<TopologyWarning>,
) -> Option<TopologyEntry> {
    let pattern = PathPattern::parse(path);
    let flat_candidate = flat_crud_candidate(&pattern);
    let schema_candidate = schema_first_candidate(catalog, path_item);
    let path_shape = path_shape_binding(&pattern, 0);
    let prefix_match = prefix_scoped_candidate(catalog, prefix_scopes, &pattern);
    let prefix_relationship = prefix_scoped_relationship(prefix_scopes, &pattern);
    let pinned_source = x_twinning_pinned.then_some(EvidenceSource::XTwinning);

    match config.policy {
        RoutingPolicy::FlatCrud => flat_candidate
            .filter(|candidate| catalog.resources.contains_key(candidate))
            .map(|candidate| {
                let relationship = path_shape
                    .as_ref()
                    .map(|binding| binding.relationship)
                    .unwrap_or(ResourceRelationship::RootCollection);
                bound_entry(
                    candidate,
                    Confidence::Pinned,
                    relationship,
                    pinned_source.unwrap_or(EvidenceSource::FlatCrud),
                    None,
                )
            }),
        RoutingPolicy::SchemaFirst => {
            let relationship = prefix_match
                .as_ref()
                .map(|candidate| candidate.relationship)
                .or_else(|| path_shape.as_ref().map(|binding| binding.relationship))
                .or(prefix_relationship)
                .unwrap_or(ResourceRelationship::RootCollection);
            schema_candidate.map(|candidate| {
                bound_entry(
                    candidate,
                    Confidence::Pinned,
                    relationship,
                    pinned_source.unwrap_or(EvidenceSource::SchemaFirst),
                    None,
                )
            })
        }
        RoutingPolicy::PrefixScoped => prefix_match.map(|candidate| {
            bound_entry(
                candidate.candidate,
                Confidence::Pinned,
                candidate.relationship,
                pinned_source.unwrap_or(EvidenceSource::PrefixScoped),
                None,
            )
        }),
        RoutingPolicy::Auto => {
            if let Some(candidate) = prefix_match {
                return Some(bound_entry(
                    candidate.candidate,
                    prefix_confidence(prefix_scopes, &candidate.prefix),
                    candidate.relationship,
                    pinned_source.unwrap_or(EvidenceSource::PrefixScoped),
                    None,
                ));
            }

            auto_topology_entry(
                catalog,
                path,
                &pattern,
                TopologyCandidates {
                    flat: flat_candidate,
                    schema: schema_candidate,
                },
                path_shape,
                prefix_relationship,
                warnings,
            )
        }
    }
}

fn auto_topology_entry(
    catalog: &RestCatalog,
    path: &str,
    pattern: &PathPattern,
    candidates: TopologyCandidates,
    path_shape: Option<PathShapeBinding>,
    prefix_relationship: Option<ResourceRelationship>,
    warnings: &mut Vec<TopologyWarning>,
) -> Option<TopologyEntry> {
    let flat_in_catalog = candidates
        .flat
        .as_ref()
        .filter(|candidate| catalog.resources.contains_key(*candidate))
        .cloned();
    let schema_in_catalog = candidates
        .schema
        .as_ref()
        .filter(|candidate| catalog.resources.contains_key(*candidate))
        .cloned();
    let path_relationship = path_shape
        .as_ref()
        .map(|binding| binding.relationship)
        .or(prefix_relationship);
    let path_shape_in_catalog = path_shape
        .as_ref()
        .filter(|binding| catalog.resources.contains_key(&binding.candidate))
        .cloned();

    if has_more_than_one_parameterized_segment(pattern)
        && let Some(candidate) = schema_in_catalog
    {
        let relationship = path_relationship.unwrap_or(ResourceRelationship::ChildSingleton);
        let conflict = conflict_description(
            path,
            &flat_in_catalog,
            &Some(candidate.clone()),
            EvidenceSource::SchemaFirst,
            warnings,
        );
        return Some(bound_entry(
            candidate,
            Confidence::Medium,
            relationship,
            EvidenceSource::SchemaFirst,
            conflict,
        ));
    }

    if let Some(path_shape) = &path_shape_in_catalog
        && child_path_shape_disambiguates_parent(
            catalog,
            &flat_in_catalog,
            &schema_in_catalog,
            path_shape,
        )
    {
        let conflict =
            child_path_shape_conflict_description(path, &flat_in_catalog, path_shape, warnings);
        return Some(bound_entry(
            path_shape.candidate.clone(),
            Confidence::High,
            path_shape.relationship,
            EvidenceSource::Waterfall,
            conflict,
        ));
    }

    if let (Some(schema), Some(path_shape)) = (&schema_in_catalog, &path_shape_in_catalog)
        && schema == &path_shape.candidate
    {
        let conflict = conflict_description(
            path,
            &flat_in_catalog,
            &schema_in_catalog,
            EvidenceSource::SchemaFirst,
            warnings,
        );
        return Some(bound_entry(
            schema.clone(),
            Confidence::High,
            path_shape.relationship,
            EvidenceSource::SchemaFirst,
            conflict,
        ));
    }

    match (flat_in_catalog, schema_in_catalog) {
        (Some(flat), Some(schema)) if flat != schema => {
            let relationship = path_relationship.unwrap_or(ResourceRelationship::RootCollection);
            if response_wrapper_like_resource(catalog, &schema)
                && matches!(
                    relationship,
                    ResourceRelationship::RootCollection | ResourceRelationship::RootSingleton
                )
            {
                let conflict = conflict_description(
                    path,
                    &Some(flat.clone()),
                    &Some(schema),
                    EvidenceSource::FlatCrud,
                    warnings,
                );
                Some(bound_entry(
                    flat,
                    Confidence::Medium,
                    relationship,
                    EvidenceSource::FlatCrud,
                    conflict,
                ))
            } else {
                let conflict = conflict_description(
                    path,
                    &Some(flat),
                    &Some(schema.clone()),
                    EvidenceSource::SchemaFirst,
                    warnings,
                );
                Some(bound_entry(
                    schema,
                    Confidence::Medium,
                    relationship,
                    EvidenceSource::SchemaFirst,
                    conflict,
                ))
            }
        }
        (Some(resource), Some(_)) | (Some(resource), None) => {
            let relationship = path_relationship.unwrap_or(ResourceRelationship::RootCollection);
            Some(bound_entry(
                resource,
                Confidence::High,
                relationship,
                EvidenceSource::FlatCrud,
                None,
            ))
        }
        (None, Some(resource)) => {
            let relationship = path_relationship.unwrap_or(ResourceRelationship::RootCollection);
            Some(bound_entry(
                resource,
                Confidence::High,
                relationship,
                EvidenceSource::SchemaFirst,
                None,
            ))
        }
        (None, None) => path_shape_in_catalog.map(|binding| {
            bound_entry(
                binding.candidate,
                Confidence::Low,
                binding.relationship,
                EvidenceSource::Waterfall,
                None,
            )
        }),
    }
}

fn child_path_shape_disambiguates_parent(
    catalog: &RestCatalog,
    flat_candidate: &Option<String>,
    schema_candidate: &Option<String>,
    path_shape: &PathShapeBinding,
) -> bool {
    if !matches!(
        path_shape.relationship,
        ResourceRelationship::ChildCollection | ResourceRelationship::ChildSingleton
    ) {
        return false;
    }

    let Some(flat_candidate) = flat_candidate else {
        return false;
    };
    if flat_candidate == &path_shape.candidate {
        return false;
    }

    match schema_candidate {
        None => true,
        Some(schema_candidate) if schema_candidate == &path_shape.candidate => false,
        Some(schema_candidate) => response_wrapper_like_resource(catalog, schema_candidate),
    }
}

fn child_path_shape_conflict_description(
    path: &str,
    flat_candidate: &Option<String>,
    path_shape: &PathShapeBinding,
    warnings: &mut Vec<TopologyWarning>,
) -> Option<String> {
    let Some(flat) = flat_candidate else {
        return None;
    };
    if flat == &path_shape.candidate {
        return None;
    }

    let description = format!(
        "Flat CRUD candidate `{flat}` disagrees with child path candidate `{}`; child path shape wins.",
        path_shape.candidate
    );
    warnings.push(TopologyWarning {
        path: path.to_owned(),
        flat_crud_candidate: Some(flat.clone()),
        schema_first_candidate: Some(path_shape.candidate.clone()),
        description: description.clone(),
    });
    Some(description)
}

fn response_wrapper_like_resource(catalog: &RestCatalog, resource_name: &str) -> bool {
    let Some(resource) = catalog.resources.get(resource_name) else {
        return false;
    };

    let schema_name = resource.schema_name.to_ascii_lowercase();
    if schema_name.ends_with("response") || schema_name.ends_with("result") {
        return true;
    }

    let has_array_field = resource
        .columns
        .iter()
        .any(|column| column.normalized_type == "array");
    let has_metadata_field = resource.columns.iter().any(|column| {
        matches!(
            column.name.as_str(),
            "count" | "message" | "status" | "status_code" | "code"
        )
    });
    has_array_field && has_metadata_field
}

fn bound_entry(
    resource_name: String,
    confidence: Confidence,
    relationship: ResourceRelationship,
    winning_evidence: EvidenceSource,
    conflict: Option<String>,
) -> TopologyEntry {
    TopologyEntry {
        resource_name,
        confidence,
        relationship,
        winning_evidence,
        conflict,
    }
}

fn build_prefix_scopes(catalog: &RestCatalog, config: &RoutingConfig) -> Vec<PrefixScope> {
    if let Some(prefix) = &config.base_prefix {
        let prefix = normalize_prefix(prefix);
        return vec![PrefixScope {
            path_count: count_paths_with_prefix(catalog, &prefix),
            prefix,
            pinned: true,
        }];
    }

    let mut counts = BTreeMap::<Vec<String>, usize>::new();
    for path in catalog.paths.keys() {
        let pattern = PathPattern::parse(path);
        for prefix in literal_prefixes(&pattern) {
            *counts.entry(prefix).or_default() += 1;
        }
    }

    counts
        .into_iter()
        .filter(|(segments, count)| {
            *count >= 3 && !catalog.resources.contains_key(&segments.join("/"))
        })
        .map(|(first, path_count)| PrefixScope {
            prefix: format!("/{}", first.join("/")),
            path_count,
            pinned: false,
        })
        .collect()
}

fn literal_prefixes(pattern: &PathPattern) -> Vec<Vec<String>> {
    let mut prefixes = Vec::new();
    let mut current = Vec::new();
    for segment in &pattern.segments {
        let PathSegment::Literal(value) = segment else {
            break;
        };
        current.push(value.clone());
        prefixes.push(current.clone());
    }
    prefixes
}

fn count_paths_with_prefix(catalog: &RestCatalog, prefix: &str) -> usize {
    catalog
        .paths
        .keys()
        .filter(|path| path_starts_with_prefix(path, prefix))
        .count()
}

fn path_starts_with_prefix(path: &str, prefix: &str) -> bool {
    let path_pattern = PathPattern::parse(path);
    let prefix_segments = prefix_segments(prefix);
    has_literal_prefix(&path_pattern.segments, &prefix_segments)
}

fn prefix_scoped_candidate(
    catalog: &RestCatalog,
    prefix_scopes: &[PrefixScope],
    pattern: &PathPattern,
) -> Option<PrefixMatch> {
    prefix_scopes
        .iter()
        .filter_map(|scope| {
            let prefix_segments = prefix_segments(&scope.prefix);
            if !has_literal_prefix(&pattern.segments, &prefix_segments) {
                return None;
            }
            let binding = prefix_scoped_path_shape_binding(pattern, prefix_segments.len())?;
            catalog
                .resources
                .contains_key(&binding.candidate)
                .then_some(PrefixMatch {
                    candidate: binding.candidate,
                    relationship: binding.relationship,
                    prefix: scope.prefix.clone(),
                })
        })
        .max_by_key(|candidate| prefix_segments(&candidate.prefix).len())
}

fn prefix_scoped_relationship(
    prefix_scopes: &[PrefixScope],
    pattern: &PathPattern,
) -> Option<ResourceRelationship> {
    prefix_scopes
        .iter()
        .filter_map(|scope| {
            let prefix_segments = prefix_segments(&scope.prefix);
            if !has_literal_prefix(&pattern.segments, &prefix_segments) {
                return None;
            }

            let relationship =
                prefix_scoped_path_shape_binding(pattern, prefix_segments.len())?.relationship;
            Some((prefix_segments.len(), relationship))
        })
        .max_by_key(|(prefix_len, _)| *prefix_len)
        .map(|(_, relationship)| relationship)
}

fn prefix_confidence(prefix_scopes: &[PrefixScope], prefix: &str) -> Confidence {
    if prefix_scopes
        .iter()
        .any(|scope| scope.prefix == prefix && scope.pinned)
    {
        Confidence::Pinned
    } else {
        Confidence::High
    }
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim().trim_matches('/');
    if trimmed.is_empty() {
        "/".to_owned()
    } else {
        format!("/{trimmed}")
    }
}

fn prefix_segments(prefix: &str) -> Vec<String> {
    prefix
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_owned)
        .collect()
}

fn has_literal_prefix(segments: &[PathSegment], prefix_segments: &[String]) -> bool {
    if prefix_segments.len() > segments.len() {
        return false;
    }

    segments
        .iter()
        .zip(prefix_segments)
        .all(|(segment, prefix)| matches!(segment, PathSegment::Literal(value) if value == prefix))
}

fn flat_crud_candidate(pattern: &PathPattern) -> Option<String> {
    first_literal_segment(pattern).map(str::to_owned)
}

fn first_literal_segment(pattern: &PathPattern) -> Option<&str> {
    pattern.segments.first().and_then(|segment| match segment {
        PathSegment::Literal(value) => Some(value.as_str()),
        PathSegment::Param(_) | PathSegment::Template { .. } => None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathShapeBinding {
    candidate: String,
    relationship: ResourceRelationship,
}

fn path_shape_binding(pattern: &PathPattern, prefix_len: usize) -> Option<PathShapeBinding> {
    let suffix = pattern.segments.get(prefix_len..)?;
    let (candidate, relationship) = match suffix {
        [PathSegment::Literal(resource)] => {
            (resource.clone(), ResourceRelationship::RootCollection)
        }
        [PathSegment::Literal(resource), segment] if is_path_parameter_segment(segment) => {
            (resource.clone(), ResourceRelationship::RootSingleton)
        }
        [.., PathSegment::Literal(resource), segment] if is_path_parameter_segment(segment) => {
            if !has_parent_path_parameter(&suffix[..suffix.len() - 2]) {
                return None;
            }
            (resource.clone(), ResourceRelationship::ChildSingleton)
        }
        [.., PathSegment::Literal(resource)] => {
            if !has_parent_path_parameter(&suffix[..suffix.len() - 1]) {
                return None;
            }
            (resource.clone(), ResourceRelationship::ChildCollection)
        }
        _ => return None,
    };

    Some(PathShapeBinding {
        candidate,
        relationship,
    })
}

fn prefix_scoped_path_shape_binding(
    pattern: &PathPattern,
    prefix_len: usize,
) -> Option<PathShapeBinding> {
    let suffix = pattern.segments.get(prefix_len..)?;
    let (candidate, relationship) = match suffix {
        [PathSegment::Literal(resource)] => {
            (resource.clone(), ResourceRelationship::RootCollection)
        }
        [PathSegment::Literal(resource), segment] if is_path_parameter_segment(segment) => {
            (resource.clone(), ResourceRelationship::RootSingleton)
        }
        [
            PathSegment::Literal(_),
            parent_id,
            PathSegment::Literal(resource),
        ] if is_path_parameter_segment(parent_id) => {
            (resource.clone(), ResourceRelationship::ChildCollection)
        }
        [
            PathSegment::Literal(_),
            parent_id,
            PathSegment::Literal(resource),
            child_id,
        ] if is_path_parameter_segment(parent_id) && is_path_parameter_segment(child_id) => {
            (resource.clone(), ResourceRelationship::ChildSingleton)
        }
        _ => return None,
    };

    Some(PathShapeBinding {
        candidate,
        relationship,
    })
}

fn has_parent_path_parameter(segments: &[PathSegment]) -> bool {
    segments.iter().any(is_path_parameter_segment)
}

fn is_path_parameter_segment(segment: &PathSegment) -> bool {
    matches!(
        segment,
        PathSegment::Param(_) | PathSegment::Template { .. }
    )
}

fn has_more_than_one_parameterized_segment(pattern: &PathPattern) -> bool {
    pattern
        .segments
        .iter()
        .filter(|segment| matches!(segment, PathSegment::Param(_)))
        .count()
        > 1
}

fn conflict_description(
    path: &str,
    flat_candidate: &Option<String>,
    schema_candidate: &Option<String>,
    winner: EvidenceSource,
    warnings: &mut Vec<TopologyWarning>,
) -> Option<String> {
    let (Some(flat), Some(schema)) = (flat_candidate, schema_candidate) else {
        return None;
    };
    if flat == schema {
        return None;
    }

    let winner = match winner {
        EvidenceSource::FlatCrud => "flat CRUD wins",
        EvidenceSource::SchemaFirst => "schema-first wins",
        EvidenceSource::PrefixScoped => "prefix-scoped wins",
        EvidenceSource::Waterfall => "waterfall wins",
        EvidenceSource::XTwinning => "x-twinning wins",
    };
    let description = format!(
        "Flat CRUD candidate `{flat}` disagrees with schema-first candidate `{schema}`; {winner}."
    );
    warnings.push(TopologyWarning {
        path: path.to_owned(),
        flat_crud_candidate: Some(flat.clone()),
        schema_first_candidate: Some(schema.clone()),
        description: description.clone(),
    });
    Some(description)
}

fn schema_first_candidate(catalog: &RestCatalog, path_item: &PathItem) -> Option<String> {
    let mut candidates = Vec::new();

    let read_operations = read_operations(path_item);
    if !read_operations.is_empty() {
        for operation in read_operations {
            collect_response_schema_candidates(operation, &mut candidates);
        }
        return first_catalog_schema_candidate(catalog, candidates);
    }

    for operation in operations(path_item) {
        collect_response_schema_candidates(operation, &mut candidates);
    }
    if let Some(candidate) = first_catalog_schema_candidate(catalog, candidates) {
        return Some(candidate);
    }

    let mut request_candidates = Vec::new();
    for operation in operations(path_item) {
        collect_request_schema_candidates(catalog, operation, &mut request_candidates);
    }
    first_catalog_schema_candidate(catalog, request_candidates)
}

fn first_catalog_schema_candidate(
    catalog: &RestCatalog,
    candidates: Vec<String>,
) -> Option<String> {
    candidates
        .into_iter()
        .filter(|candidate| resource_has_fields(catalog, candidate))
        .find(|candidate| catalog.resources.contains_key(candidate))
}

fn resource_has_fields(catalog: &RestCatalog, resource_name: &str) -> bool {
    catalog
        .resources
        .get(resource_name)
        .is_some_and(|resource| !resource.columns.is_empty())
}

fn collect_response_schema_candidates(operation: &OperationObject, candidates: &mut Vec<String>) {
    let mut responses = operation.responses.iter().collect::<Vec<_>>();
    responses.sort_by_key(|(status, _)| status.as_str());

    for (status, response) in responses {
        if !status.starts_with('2') {
            continue;
        }
        if let Some(candidate) = response_schema_resource(response) {
            candidates.push(candidate);
        }
    }
}

fn collect_request_schema_candidates(
    catalog: &RestCatalog,
    operation: &OperationObject,
    candidates: &mut Vec<String>,
) {
    if let Some(candidate) = operation
        .request_body
        .as_ref()
        .and_then(|request_body| request_schema_resource(catalog, request_body))
    {
        candidates.push(candidate);
    }
}

fn response_schema_resource(response: &ResponseObject) -> Option<String> {
    if let Some(reference) = &response.reference {
        return component_resource_name(reference);
    }

    response
        .content
        .get("application/json")
        .and_then(|media| media.schema.as_ref())
        .and_then(schema_resource_name)
        .or_else(|| {
            let mut content = response.content.iter().collect::<Vec<_>>();
            content.sort_by_key(|(media_type, _)| media_type.as_str());
            content
                .into_iter()
                .find_map(|(_, media)| media.schema.as_ref().and_then(schema_resource_name))
        })
}

fn request_schema_resource(
    catalog: &RestCatalog,
    request_body: &RequestBodyObject,
) -> Option<String> {
    let request_body = if let Some(reference) = &request_body.reference {
        let request_body_name = reference.strip_prefix("#/components/requestBodies/")?;
        catalog.component_request_bodies.get(request_body_name)?
    } else {
        request_body
    };

    request_body
        .content
        .get("application/json")
        .and_then(|media| media.schema.as_ref())
        .and_then(schema_resource_name)
        .or_else(|| {
            let mut content = request_body.content.iter().collect::<Vec<_>>();
            content.sort_by_key(|(media_type, _)| media_type.as_str());
            content
                .into_iter()
                .find_map(|(_, media)| media.schema.as_ref().and_then(schema_resource_name))
        })
}

fn schema_resource_name(schema: &SchemaObject) -> Option<String> {
    schema
        .reference
        .as_deref()
        .and_then(component_resource_name)
        .or_else(|| schema.items.as_deref().and_then(schema_resource_name))
}

fn component_resource_name(reference: &str) -> Option<String> {
    reference
        .strip_prefix("#/components/schemas/")
        .map(|schema_name| format!("{}s", schema_name.to_ascii_lowercase()))
}

fn operations(path_item: &PathItem) -> Vec<&OperationObject> {
    let mut operations = Vec::new();
    if let Some(operation) = &path_item.get {
        operations.push(operation);
    }
    if let Some(operation) = &path_item.head {
        operations.push(operation);
    }
    if let Some(operation) = &path_item.post {
        operations.push(operation);
    }
    if let Some(operation) = &path_item.put {
        operations.push(operation);
    }
    if let Some(operation) = &path_item.patch {
        operations.push(operation);
    }
    if let Some(operation) = &path_item.delete {
        operations.push(operation);
    }
    operations
}

fn read_operations(path_item: &PathItem) -> Vec<&OperationObject> {
    let mut operations = Vec::new();
    if let Some(operation) = &path_item.get {
        operations.push(operation);
    }
    if let Some(operation) = &path_item.head {
        operations.push(operation);
    }
    operations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::rest::{
        policy::RoutingConfig,
        routes::{Method, RouteKind, build_route_registry},
        spec::{RestCatalog, parse_rest_catalog_bytes},
    };

    fn topology(spec: &str) -> ResourceTopology {
        topology_with_config(spec, &RoutingConfig::default())
    }

    fn topology_with_config(spec: &str, config: &RoutingConfig) -> ResourceTopology {
        let catalog =
            parse_rest_catalog_bytes(spec.as_bytes(), "topology.yaml").expect("spec parses");
        build_spec_topology(&catalog, config)
    }

    fn catalog_and_topology(spec: &str) -> (RestCatalog, ResourceTopology) {
        let catalog =
            parse_rest_catalog_bytes(spec.as_bytes(), "topology.yaml").expect("spec parses");
        let topology = build_spec_topology(&catalog, &RoutingConfig::default());
        (catalog, topology)
    }

    fn entry<'a>(topology: &'a ResourceTopology, path: &str) -> &'a TopologyEntry {
        topology
            .path_bindings
            .get(path)
            .unwrap_or_else(|| panic!("missing topology binding for {path}"))
    }

    #[test]
    fn topology_petstore_high_confidence() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    Pet:
      type: object
      required: [id, name]
      properties:
        id: { type: integer }
        name: { type: string }
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
  /pets/{id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Pet"
"##,
        );

        assert_eq!(entry(&topology, "/pets").resource_name, "pets");
        assert_eq!(entry(&topology, "/pets").confidence, Confidence::High);
        assert_eq!(
            entry(&topology, "/pets").relationship,
            ResourceRelationship::RootCollection
        );
        assert_eq!(
            entry(&topology, "/pets/{id}").relationship,
            ResourceRelationship::RootSingleton
        );
    }

    #[test]
    fn topology_schema_first_keeps_item_relationship_when_path_resource_differs() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    userRequest:
      type: object
      properties:
        name: { type: string }
        job: { type: string }
paths:
  /users:
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/userRequest"
      responses: {}
  /users/{id}:
    put:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/userRequest"
      responses: {}
    delete:
      responses:
        "204": { description: deleted }
"##,
        );

        let collection = entry(&topology, "/users");
        assert_eq!(collection.resource_name, "userrequests");
        assert_eq!(
            collection.relationship,
            ResourceRelationship::RootCollection
        );
        assert_eq!(collection.confidence, Confidence::High);
        assert_eq!(collection.winning_evidence, EvidenceSource::SchemaFirst);

        let item = entry(&topology, "/users/{id}");
        assert_eq!(item.resource_name, "userrequests");
        assert_eq!(item.relationship, ResourceRelationship::RootSingleton);
        assert_eq!(item.confidence, Confidence::High);
        assert_eq!(item.winning_evidence, EvidenceSource::SchemaFirst);
    }

    #[test]
    fn topology_read_paths_do_not_inherit_sibling_mutation_request_schema() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    Order:
      type: object
      properties:
        id: { type: string }
    ProductQuantity:
      type: object
      properties:
        product_id: { type: integer }
        quantity: { type: integer }
paths:
  /orders/{order_id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                properties:
                  orders:
                    $ref: "#/components/schemas/Order"
    put:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/ProductQuantity"
      responses:
        "200": { description: updated }
"##,
        );

        let entry = entry(&topology, "/orders/{order_id}");
        assert_eq!(entry.resource_name, "orders");
        assert_eq!(entry.relationship, ResourceRelationship::RootSingleton);
        assert_ne!(entry.resource_name, "productquantitys");
    }

    #[test]
    fn topology_schema_first_prefixed_item_uses_prefix_relationship() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    fax.v1.fax:
      type: object
      properties:
        sid: { type: string }
        status: { type: string }
    fax.v1.fax.fax_media:
      type: object
      properties:
        sid: { type: string }
paths:
  /v1/Faxes:
    get:
      responses:
        "200":
          description: ok
  /v1/Faxes/{Sid}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/fax.v1.fax"
    delete:
      responses:
        "204": { description: deleted }
  /v1/Faxes/{FaxSid}/Media:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/fax.v1.fax.fax_media"
"##,
        );

        let entry = entry(&topology, "/v1/Faxes/{Sid}");
        assert_eq!(entry.resource_name, "fax.v1.faxs");
        assert_eq!(entry.relationship, ResourceRelationship::RootSingleton);
        assert_eq!(entry.confidence, Confidence::High);
        assert_eq!(entry.winning_evidence, EvidenceSource::SchemaFirst);
    }

    #[test]
    fn topology_schema_first_multi_literal_prefix_item_uses_singleton_relationship() {
        let spec = r##"
openapi: 3.0.3
components:
  schemas:
    Activity:
      type: object
      properties:
        id: { type: integer }
        title: { type: string }
    Author:
      type: object
      properties:
        id: { type: integer }
        firstName: { type: string }
    Book:
      type: object
      properties:
        id: { type: integer }
        title: { type: string }
paths:
  /api/v1/Activities:
    get:
      responses:
        "200":
          content:
            application/json; v=1.0:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Activity"
  /api/v1/Activities/{id}:
    get:
      responses:
        "200":
          content:
            application/json; v=1.0:
              schema:
                $ref: "#/components/schemas/Activity"
    delete:
      responses:
        "200": { description: deleted }
  /api/v1/Authors:
    get:
      responses:
        "200":
          content:
            application/json; v=1.0:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Author"
  /api/v1/Authors/{id}:
    get:
      responses:
        "200":
          content:
            application/json; v=1.0:
              schema:
                $ref: "#/components/schemas/Author"
  /api/v1/Books:
    get:
      responses:
        "200":
          content:
            application/json; v=1.0:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Book"
"##;
        let (catalog, topology) = catalog_and_topology(spec);

        assert!(
            topology
                .prefix_scopes
                .iter()
                .any(|scope| scope.prefix == "/api/v1")
        );
        let item = entry(&topology, "/api/v1/Activities/{id}");
        assert_eq!(item.resource_name, "activitys");
        assert_eq!(item.relationship, ResourceRelationship::RootSingleton);
        assert_eq!(item.confidence, Confidence::High);
        assert_eq!(item.winning_evidence, EvidenceSource::SchemaFirst);

        let registry = build_route_registry(&catalog, &topology, &RoutingConfig::default());
        let get_route = registry
            .routes
            .iter()
            .find(|(method, pattern, _)| {
                *method == Method::Get
                    && pattern
                        == &crate::protocol::rest::routes::PathPattern::parse(
                            "/api/v1/Activities/{id}",
                        )
            })
            .map(|(_, _, route)| route)
            .expect("activity item GET route should exist");
        assert_eq!(get_route.kind, RouteKind::ReadOne);

        let delete_route = registry
            .routes
            .iter()
            .find(|(method, pattern, _)| {
                *method == Method::Delete
                    && pattern
                        == &crate::protocol::rest::routes::PathPattern::parse(
                            "/api/v1/Activities/{id}",
                        )
            })
            .map(|(_, _, route)| route)
            .expect("activity item DELETE route should exist");
        assert_eq!(delete_route.kind, RouteKind::Delete);
    }

    #[test]
    fn topology_reqres_prefix_detected() {
        let topology = topology(
            r##"
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
    get: { responses: {} }
  /api/users/{id}:
    get: { responses: {} }
  /api/unknown:
    get: { responses: {} }
"##,
        );

        assert_eq!(
            topology.prefix_scopes,
            vec![PrefixScope {
                prefix: "/api".to_owned(),
                path_count: 3,
                pinned: false,
            }]
        );
        assert_eq!(entry(&topology, "/api/users").resource_name, "users");
        assert_eq!(
            entry(&topology, "/api/users").winning_evidence,
            EvidenceSource::PrefixScoped
        );
        assert_eq!(entry(&topology, "/api/users").confidence, Confidence::High);
    }

    #[test]
    fn topology_1password_nested_child() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    Vault:
      type: object
      properties:
        id: { type: string }
    Item:
      type: object
      properties:
        id: { type: string }
paths:
  /vaults/{id}/items:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Item"
"##,
        );

        let entry = entry(&topology, "/vaults/{id}/items");
        assert_eq!(entry.resource_name, "items");
        assert_eq!(entry.relationship, ResourceRelationship::ChildCollection);
        assert_eq!(entry.confidence, Confidence::High);
    }

    #[test]
    fn topology_multi_parameter_parent_collection_uses_child_relationship() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    Issue:
      type: object
      properties:
        id: { type: integer }
paths:
  /repos/{owner}/{repo}/issues:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Issue"
    post:
      responses:
        "201":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Issue"
  /repos/{owner}/{repo}/issues/{issue_number}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Issue"
"##,
        );

        let collection = entry(&topology, "/repos/{owner}/{repo}/issues");
        assert_eq!(collection.resource_name, "issues");
        assert_eq!(
            collection.relationship,
            ResourceRelationship::ChildCollection
        );
        assert_eq!(collection.confidence, Confidence::Medium);

        let singleton = entry(&topology, "/repos/{owner}/{repo}/issues/{issue_number}");
        assert_eq!(singleton.resource_name, "issues");
        assert_eq!(singleton.relationship, ResourceRelationship::ChildSingleton);
    }

    #[test]
    fn topology_does_not_claim_multi_param_resource_paths_as_prefix_scoped() {
        let spec = r##"
openapi: 3.0.3
components:
  schemas:
    Issue:
      type: object
      properties:
        id: { type: integer }
    Label:
      type: object
      properties:
        id: { type: integer }
    Hook:
      type: object
      properties:
        id: { type: integer }
paths:
  /repos/{owner}/{repo}/issues:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Issue"
  /repos/{owner}/{repo}/labels:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Label"
  /repos/{owner}/{repo}/hooks:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Hook"
"##;
        let (catalog, topology) = catalog_and_topology(spec);

        let issues = entry(&topology, "/repos/{owner}/{repo}/issues");
        assert_eq!(issues.resource_name, "issues");
        assert_eq!(issues.relationship, ResourceRelationship::ChildCollection);
        assert_eq!(issues.winning_evidence, EvidenceSource::SchemaFirst);

        let registry = build_route_registry(&catalog, &topology, &RoutingConfig::default());
        let issue_route = registry
            .routes
            .iter()
            .find(|(method, pattern, _)| {
                *method == Method::Get
                    && pattern == &PathPattern::parse("/repos/{owner}/{repo}/issues")
            })
            .map(|(_, _, route)| route)
            .expect("issue collection route should exist");
        assert_eq!(issue_route.kind, RouteKind::ReadMany);
        assert_eq!(
            issue_route.effective_resource_name.as_deref(),
            Some("issues")
        );
        assert_eq!(
            issue_route.routing_evidence,
            Some(EvidenceSource::SchemaFirst)
        );
    }

    #[test]
    fn topology_multi_prefix() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    User:
      type: object
      properties:
        id: { type: integer }
    Order:
      type: object
      properties:
        id: { type: integer }
paths:
  /v1/users:
    get: { responses: {} }
  /v1/users/{id}:
    get: { responses: {} }
  /v1/orders:
    get: { responses: {} }
  /v2/users:
    get: { responses: {} }
  /v2/users/{id}:
    get: { responses: {} }
  /v2/orders:
    get: { responses: {} }
"##,
        );

        assert_eq!(
            topology.prefix_scopes,
            vec![
                PrefixScope {
                    prefix: "/v1".to_owned(),
                    path_count: 3,
                    pinned: false,
                },
                PrefixScope {
                    prefix: "/v2".to_owned(),
                    path_count: 3,
                    pinned: false,
                },
            ]
        );
        assert_eq!(entry(&topology, "/v2/orders").resource_name, "orders");
    }

    #[test]
    fn topology_conflict_logged() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    Vault:
      type: object
      properties:
        id: { type: string }
    Item:
      type: object
      properties:
        id: { type: string }
paths:
  /vaults/{id}/items:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Item"
"##,
        );

        assert_eq!(topology.warnings.len(), 1);
        assert_eq!(topology.warnings[0].path, "/vaults/{id}/items");
        assert_eq!(
            topology.warnings[0].flat_crud_candidate.as_deref(),
            Some("vaults")
        );
        assert_eq!(
            topology.warnings[0].schema_first_candidate.as_deref(),
            Some("items")
        );
        assert_eq!(
            entry(&topology, "/vaults/{id}/items").conflict.as_deref(),
            Some(
                "Flat CRUD candidate `vaults` disagrees with schema-first candidate `items`; schema-first wins."
            )
        );
    }

    #[test]
    fn topology_response_wrapper_does_not_override_root_mutation_target() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    Queue:
      type: object
      required: [name]
      properties:
        name: { type: string }
    ApiResponse:
      type: object
      properties:
        count: { type: integer }
        message: { type: string }
        queues:
          type: array
          items:
            $ref: "#/components/schemas/Queue"
paths:
  /queues/{queueName}:
    delete:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ApiResponse"
"##,
        );

        let entry = entry(&topology, "/queues/{queueName}");
        assert_eq!(entry.resource_name, "queues");
        assert_eq!(entry.relationship, ResourceRelationship::RootSingleton);
        assert_eq!(entry.confidence, Confidence::Medium);
        assert_eq!(entry.winning_evidence, EvidenceSource::FlatCrud);
        assert_eq!(
            entry.conflict.as_deref(),
            Some(
                "Flat CRUD candidate `queues` disagrees with schema-first candidate `apiresponses`; flat CRUD wins."
            )
        );
    }

    #[test]
    fn topology_ambiguity_rule_nested_prefers_schema_first() {
        let topology = topology(
            r##"
openapi: 3.0.3
components:
  schemas:
    Vault:
      type: object
      properties:
        id: { type: string }
    Item:
      type: object
      properties:
        id: { type: string }
paths:
  /vaults/{vault_id}/items/{item_id}:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Item"
"##,
        );

        let entry = entry(&topology, "/vaults/{vault_id}/items/{item_id}");
        assert_eq!(entry.resource_name, "items");
        assert_eq!(entry.relationship, ResourceRelationship::ChildSingleton);
        assert_eq!(entry.confidence, Confidence::Medium);
        assert_eq!(entry.winning_evidence, EvidenceSource::SchemaFirst);
    }

    #[test]
    fn topology_x_twinning_pinned() {
        let topology = topology(
            r##"
openapi: 3.0.3
x-twinning:
  routing: prefix-scoped
  base-prefix: /api
components:
  schemas:
    File:
      type: object
      properties:
        id: { type: string }
paths:
  /api/files:
    get: { responses: {} }
"##,
        );

        assert_eq!(
            topology.prefix_scopes,
            vec![PrefixScope {
                prefix: "/api".to_owned(),
                path_count: 1,
                pinned: true,
            }]
        );
        let entry = entry(&topology, "/api/files");
        assert_eq!(entry.resource_name, "files");
        assert_eq!(entry.confidence, Confidence::Pinned);
        assert_eq!(entry.winning_evidence, EvidenceSource::XTwinning);
    }
}
