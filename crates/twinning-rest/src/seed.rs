//! Seeding committed REST twin state from OpenAPI examples.

use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::TableCatalog,
    ir::{MutationKind, MutationOp, ScalarValue},
    kernel::mutation::execute_mutation,
    result::{KernelResult, RefusalResult},
};

use super::{
    policy::RoutingConfig,
    routes::{Method, PathPattern, RouteKind, build_route_registry_with_config},
    spec::{
        ExampleObject, MediaTypeObject, OperationObject, PathItem, ResourceColumn, ResourceSchema,
        RestCatalog,
    },
};

const SEED_SESSION_ID: &str = "rest-spec-seed";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SeedResult {
    pub examples_found: usize,
    pub rows_seeded: usize,
    pub resources_seeded: usize,
    pub skipped_examples: usize,
    pub warnings: Vec<SeedWarning>,
}

impl SeedResult {
    pub fn summary_message(&self) -> String {
        if self.examples_found == 0 {
            return "No spec examples found - backend starts empty".to_owned();
        }

        format!(
            "Seeded {} rows across {} resource types from spec examples",
            self.rows_seeded, self.resources_seeded
        )
    }

    fn push_warning(
        &mut self,
        resource_name: Option<String>,
        field: Option<String>,
        message: impl Into<String>,
    ) {
        self.warnings.push(SeedWarning {
            resource_name,
            field,
            message: message.into(),
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedWarning {
    pub resource_name: Option<String>,
    pub field: Option<String>,
    pub message: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SeedError {
    #[error("backend has no initialized table for REST resource `{resource}`")]
    MissingBackendTable { resource: String },
}

#[derive(Debug, Clone)]
struct SeedExample {
    resource_name: String,
    source: String,
    value: JsonValue,
}

pub fn seed_from_spec(
    catalog: &RestCatalog,
    backend: &mut BaseSnapshotBackend,
) -> Result<SeedResult, SeedError> {
    let examples = collect_examples(catalog);
    let mut result = SeedResult {
        examples_found: examples.len(),
        ..SeedResult::default()
    };
    let mut seeded_resources = BTreeSet::new();
    let mut seen_pk_values = BTreeMap::<String, BTreeSet<String>>::new();
    let mut no_pk_warned = BTreeSet::<String>::new();

    if examples.is_empty() {
        result.push_warning(None, None, result.summary_message());
        return Ok(result);
    }

    for example in examples {
        let Some(resource) = catalog.resources.get(&example.resource_name) else {
            result.skipped_examples += 1;
            result.push_warning(
                Some(example.resource_name),
                None,
                format!(
                    "Example `{}` targets a resource that is not present in the REST catalog.",
                    example.source
                ),
            );
            continue;
        };

        if backend.base_table(&resource.resource_name).is_none() {
            return Err(SeedError::MissingBackendTable {
                resource: resource.resource_name.clone(),
            });
        }

        let Some(table) = catalog.catalog.table(&resource.resource_name) else {
            result.skipped_examples += 1;
            result.push_warning(
                Some(resource.resource_name.clone()),
                None,
                format!(
                    "Example `{}` targets a resource without a kernel table.",
                    example.source
                ),
            );
            continue;
        };

        if table.primary_key.is_none() && no_pk_warned.insert(resource.resource_name.clone()) {
            result.push_warning(
                Some(resource.resource_name.clone()),
                None,
                format!(
                    "Resource {} has no declared PK - all examples will be inserted without deduplication.",
                    resource.resource_name
                ),
            );
        }

        let Some(mutation) = mutation_from_example(resource, &example, &mut result) else {
            result.skipped_examples += 1;
            continue;
        };

        if let Some(pk_key) = primary_key_dedup_key(table, &mutation) {
            let seen_for_resource = seen_pk_values
                .entry(resource.resource_name.clone())
                .or_default();
            if !seen_for_resource.insert(pk_key) {
                result.skipped_examples += 1;
                continue;
            }
        }

        match execute_mutation(&catalog.catalog, backend, &mutation) {
            KernelResult::Mutation(mutation_result) => {
                result.rows_seeded += mutation_result.rows_affected as usize;
                if mutation_result.rows_affected > 0 {
                    seeded_resources.insert(resource.resource_name.clone());
                }
            }
            KernelResult::Refusal(refusal) => {
                result.skipped_examples += 1;
                push_kernel_refusal_warning(&mut result, resource, &example, refusal);
            }
            KernelResult::Ack(_) | KernelResult::Read(_) => {
                result.skipped_examples += 1;
                result.push_warning(
                    Some(resource.resource_name.clone()),
                    None,
                    format!(
                        "Example `{}` produced a non-mutation kernel result and was skipped.",
                        example.source
                    ),
                );
            }
        }
    }

    result.resources_seeded = seeded_resources.len();
    result.push_warning(None, None, result.summary_message());
    Ok(result)
}

fn collect_examples(catalog: &RestCatalog) -> Vec<SeedExample> {
    let mut examples = Vec::new();

    let mut resources = catalog.resources.values().collect::<Vec<_>>();
    resources.sort_by_key(|resource| resource.resource_name.as_str());
    for resource in resources {
        if let Some(value) = &resource.example {
            examples.push(SeedExample {
                resource_name: resource.resource_name.clone(),
                source: format!("components.schemas.{}.example", resource.schema_name),
                value: value.clone(),
            });
        }
    }

    collect_component_examples(catalog, &mut examples);
    collect_path_examples(catalog, PathExamplePass::Inline, &mut examples);
    collect_path_examples(catalog, PathExamplePass::Named, &mut examples);
    collect_path_response_examples(catalog, &mut examples);

    examples
}

fn collect_component_examples(catalog: &RestCatalog, examples: &mut Vec<SeedExample>) {
    let mut component_examples = catalog.component_examples.iter().collect::<Vec<_>>();
    component_examples.sort_by_key(|(name, _)| name.as_str());

    for (name, example) in component_examples {
        let Some(value) = resolve_example_value(catalog, example) else {
            continue;
        };
        let Some(resource_name) = infer_component_example_resource(catalog, name, &value) else {
            continue;
        };
        examples.push(SeedExample {
            resource_name,
            source: format!("components.examples.{name}.value"),
            value,
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathExamplePass {
    Inline,
    Named,
}

fn collect_path_examples(
    catalog: &RestCatalog,
    pass: PathExamplePass,
    examples: &mut Vec<SeedExample>,
) {
    let registry = build_route_registry_with_config(catalog, &RoutingConfig::default());
    let mut paths = catalog.paths.iter().collect::<Vec<_>>();
    paths.sort_by_key(|(path, _)| path.as_str());

    for (path, path_item) in paths {
        let pattern = PathPattern::parse(path);
        for (method, operation) in operations(path_item) {
            let Some(entry) = registry
                .routes
                .iter()
                .find(|(candidate_method, candidate_pattern, _)| {
                    *candidate_method == method && *candidate_pattern == pattern
                })
                .map(|(_, _, entry)| entry)
            else {
                continue;
            };

            if matches!(entry.kind, RouteKind::Refusal { .. }) {
                continue;
            }

            let Some(media) = json_request_media(operation) else {
                continue;
            };

            match pass {
                PathExamplePass::Inline => {
                    if let Some(value) = &media.example {
                        examples.push(SeedExample {
                            resource_name: entry.resource_name.clone(),
                            source: format!("{path}.{}.requestBody.example", method.as_str()),
                            value: value.clone(),
                        });
                    }
                }
                PathExamplePass::Named => {
                    let mut named_examples = media.examples.iter().collect::<Vec<_>>();
                    named_examples.sort_by_key(|(name, _)| name.as_str());
                    for (name, example) in named_examples {
                        if let Some(value) = resolve_example_value(catalog, example) {
                            examples.push(SeedExample {
                                resource_name: entry.resource_name.clone(),
                                source: format!(
                                    "{path}.{}.requestBody.examples.{name}",
                                    method.as_str()
                                ),
                                value,
                            });
                        }
                    }
                }
            }
        }
    }
}

fn collect_path_response_examples(catalog: &RestCatalog, examples: &mut Vec<SeedExample>) {
    let registry = build_route_registry_with_config(catalog, &RoutingConfig::default());
    let mut paths = catalog.paths.iter().collect::<Vec<_>>();
    paths.sort_by_key(|(path, _)| path.as_str());

    for (path, path_item) in paths {
        let pattern = PathPattern::parse(path);
        for (method, operation) in operations(path_item) {
            if method != Method::Get {
                continue;
            }
            let Some(entry) = registry
                .routes
                .iter()
                .find(|(candidate_method, candidate_pattern, _)| {
                    *candidate_method == method && *candidate_pattern == pattern
                })
                .map(|(_, _, entry)| entry)
            else {
                continue;
            };
            if matches!(entry.kind, RouteKind::Refusal { .. }) {
                continue;
            }
            let resource_name = entry
                .effective_resource_name
                .as_deref()
                .unwrap_or(entry.resource_name.as_str());
            let Some(resource) = catalog.resources.get(resource_name) else {
                continue;
            };
            let [selector_column] = resource.meta.path_lookup_columns.as_slice() else {
                continue;
            };
            let public_columns = public_seed_columns(resource);
            let Some(media) = json_success_response_media(operation) else {
                continue;
            };

            let mut named_examples = media.examples.iter().collect::<Vec<_>>();
            named_examples.sort_by_key(|(name, _)| name.as_str());
            for (name, example) in named_examples {
                let Some(value) = resolve_example_value(catalog, example) else {
                    continue;
                };
                if let Some(mut object) = response_example_object(resource, &public_columns, value)
                {
                    object.insert(selector_column.clone(), JsonValue::String(name.clone()));
                    examples.push(SeedExample {
                        resource_name: resource.resource_name.clone(),
                        source: format!("{path}.{}.responses.2xx.examples.{name}", method.as_str()),
                        value: JsonValue::Object(object.into_iter().collect()),
                    });
                }
            }
        }
    }
}

fn public_seed_columns(resource: &ResourceSchema) -> Vec<String> {
    resource
        .columns
        .iter()
        .filter(|column| {
            !resource
                .meta
                .path_lookup_columns
                .iter()
                .any(|hidden| hidden == &column.name)
        })
        .map(|column| column.name.clone())
        .collect()
}

fn response_example_object(
    resource: &ResourceSchema,
    public_columns: &[String],
    value: JsonValue,
) -> Option<BTreeMap<String, JsonValue>> {
    if let JsonValue::Object(object) = &value {
        let object = object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        if public_columns
            .iter()
            .any(|column| object.contains_key(column))
        {
            return Some(object);
        }
    }

    let [column] = public_columns else {
        return None;
    };
    let resource_column = resource
        .columns
        .iter()
        .find(|candidate| candidate.name == *column)?;
    if resource_column.normalized_type != "array" || !value.is_array() {
        return None;
    }

    Some(BTreeMap::from([(column.clone(), value)]))
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

fn json_request_media(operation: &OperationObject) -> Option<&MediaTypeObject> {
    let request_body = operation.request_body.as_ref()?;
    request_body.content.get("application/json").or_else(|| {
        let mut content = request_body.content.iter().collect::<Vec<_>>();
        content.sort_by_key(|(media_type, _)| media_type.as_str());
        content
            .into_iter()
            .find(|(media_type, _)| media_type.eq_ignore_ascii_case("application/json"))
            .map(|(_, media)| media)
    })
}

fn json_success_response_media(operation: &OperationObject) -> Option<&MediaTypeObject> {
    let response = operation
        .responses
        .iter()
        .filter_map(|(status, response)| {
            status
                .parse::<u16>()
                .ok()
                .filter(|status| (200..300).contains(status))
                .map(|status| (status, response))
        })
        .min_by_key(|(status, _)| *status)
        .map(|(_, response)| response)?;

    response.content.get("application/json").or_else(|| {
        let mut content = response.content.iter().collect::<Vec<_>>();
        content.sort_by_key(|(media_type, _)| media_type.as_str());
        content
            .into_iter()
            .find(|(media_type, _)| media_type.eq_ignore_ascii_case("application/json"))
            .map(|(_, media)| media)
    })
}

fn resolve_example_value(catalog: &RestCatalog, example: &ExampleObject) -> Option<JsonValue> {
    if let Some(value) = &example.value {
        return Some(value.clone());
    }

    let reference = example.reference.as_ref()?;
    let name = reference.strip_prefix("#/components/examples/")?;
    catalog
        .component_examples
        .get(name)
        .and_then(|example| example.value.clone())
}

fn infer_component_example_resource(
    catalog: &RestCatalog,
    example_name: &str,
    value: &JsonValue,
) -> Option<String> {
    let lower_name = example_name.to_ascii_lowercase();
    let mut resources = catalog.resources.values().collect::<Vec<_>>();
    resources.sort_by_key(|resource| resource.resource_name.as_str());

    for resource in &resources {
        let schema_name = resource.schema_name.to_ascii_lowercase();
        if lower_name == schema_name
            || lower_name == resource.resource_name
            || lower_name.starts_with(&schema_name)
            || lower_name.starts_with(&resource.resource_name)
            || lower_name.ends_with(&schema_name)
            || lower_name.ends_with(&resource.resource_name)
        {
            return Some(resource.resource_name.clone());
        }
    }

    let object = value.as_object()?;
    let keys = object.keys().collect::<BTreeSet<_>>();
    let matching_resources = resources
        .into_iter()
        .filter(|resource| {
            let columns = resource
                .columns
                .iter()
                .map(|column| &column.name)
                .collect::<BTreeSet<_>>();
            !keys.is_empty()
                && keys.iter().all(|key| columns.contains(*key))
                && resource
                    .required
                    .iter()
                    .all(|required| keys.contains(required))
        })
        .collect::<Vec<_>>();

    match matching_resources.as_slice() {
        [resource] => Some(resource.resource_name.clone()),
        _ => None,
    }
}

fn mutation_from_example(
    resource: &ResourceSchema,
    example: &SeedExample,
    result: &mut SeedResult,
) -> Option<MutationOp> {
    let Some(object) = example.value.as_object() else {
        result.push_warning(
            Some(resource.resource_name.clone()),
            None,
            format!(
                "Example `{}` is {}, but REST seed examples must be JSON objects.",
                example.source,
                json_type_name(&example.value)
            ),
        );
        return None;
    };

    for required in &resource.required {
        if !object.contains_key(required) {
            result.push_warning(
                Some(resource.resource_name.clone()),
                Some(required.clone()),
                format!(
                    "Example `{}` is missing required field `{required}`.",
                    example.source
                ),
            );
            return None;
        }
    }

    let columns_by_name = resource
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<BTreeMap<_, _>>();

    if !resource.additional_properties_allowed {
        let mut fields = object.keys().collect::<Vec<_>>();
        fields.sort();
        for field in fields {
            if !columns_by_name.contains_key(field.as_str()) {
                result.push_warning(
                    Some(resource.resource_name.clone()),
                    Some(field.clone()),
                    format!(
                        "Example `{}` contains unknown field `{field}` and additionalProperties is false.",
                        example.source
                    ),
                );
                return None;
            }
        }
    }

    let mut columns = Vec::new();
    let mut row = Vec::new();
    for column in &resource.columns {
        let Some(value) = object.get(&column.name) else {
            continue;
        };
        match scalar_value_for_column(column, value) {
            Ok(value) => {
                columns.push(column.name.clone());
                row.push(value);
            }
            Err(error) => {
                result.push_warning(
                    Some(resource.resource_name.clone()),
                    Some(column.name.clone()),
                    format!(
                        "Example `{}` has invalid value for `{}`: expected {}, received {}.",
                        example.source, column.name, error.expected, error.received
                    ),
                );
                return None;
            }
        }
    }

    Some(MutationOp {
        session_id: SEED_SESSION_ID.to_owned(),
        table: resource.resource_name.clone(),
        kind: MutationKind::Insert,
        columns,
        rows: vec![row],
        conflict_target: None,
        update_columns: Vec::new(),
        predicate: None,
        returning: Vec::new(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SeedCoercionError {
    expected: String,
    received: String,
}

fn scalar_value_for_column(
    column: &ResourceColumn,
    value: &JsonValue,
) -> Result<ScalarValue, SeedCoercionError> {
    if value.is_null() {
        return Ok(ScalarValue::Null);
    }

    match column.normalized_type.as_str() {
        "bigint" | "integer" | "smallint" => value
            .as_i64()
            .map(ScalarValue::Integer)
            .ok_or_else(|| coercion_error(column, value)),
        "float" | "numeric" => {
            if value.is_number() {
                Ok(ScalarValue::Text(value.to_string()))
            } else {
                Err(coercion_error(column, value))
            }
        }
        "boolean" => value
            .as_bool()
            .map(ScalarValue::Boolean)
            .ok_or_else(|| coercion_error(column, value)),
        "bytes" if column.format.as_deref() == Some("byte") => value
            .as_str()
            .and_then(openapi_byte_string_to_bytea_hex)
            .map(ScalarValue::Text)
            .ok_or_else(|| SeedCoercionError {
                expected: "base64 byte string".to_owned(),
                received: json_type_name(value).to_owned(),
            }),
        "timestamp" | "date" | "bytes" | "text" => value
            .as_str()
            .map(|value| ScalarValue::Text(value.to_owned()))
            .ok_or_else(|| coercion_error(column, value)),
        "json" => Ok(ScalarValue::Json(value.clone())),
        "array" => value
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .map(json_to_rest_scalar)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .map(ScalarValue::Array)
            .ok_or_else(|| coercion_error(column, value)),
        _ => value
            .as_str()
            .map(|value| ScalarValue::Text(value.to_owned()))
            .ok_or_else(|| coercion_error(column, value)),
    }
}

fn json_to_rest_scalar(value: &JsonValue) -> Result<ScalarValue, SeedCoercionError> {
    match value {
        JsonValue::Null => Ok(ScalarValue::Null),
        JsonValue::Bool(value) => Ok(ScalarValue::Boolean(*value)),
        JsonValue::Number(value) => Ok(value
            .as_i64()
            .map(ScalarValue::Integer)
            .unwrap_or_else(|| ScalarValue::Json(JsonValue::Number(value.clone())))),
        JsonValue::String(value) => Ok(ScalarValue::Text(value.clone())),
        JsonValue::Array(values) => values
            .iter()
            .map(json_to_rest_scalar)
            .collect::<Result<Vec<_>, _>>()
            .map(ScalarValue::Array),
        JsonValue::Object(_) => Ok(ScalarValue::Json(value.clone())),
    }
}

fn openapi_byte_string_to_bytea_hex(value: &str) -> Option<String> {
    let bytes = BASE64_STANDARD.decode(value).ok()?;
    let mut encoded = String::with_capacity(2 + bytes.len() * 2);
    encoded.push_str("\\x");

    for byte in bytes {
        encoded.push(hex_char(byte >> 4));
        encoded.push(hex_char(byte & 0x0f));
    }

    Some(encoded)
}

fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        10..=15 => char::from(b'a' + (nibble - 10)),
        _ => unreachable!("nibble is masked to four bits"),
    }
}

fn coercion_error(column: &ResourceColumn, value: &JsonValue) -> SeedCoercionError {
    SeedCoercionError {
        expected: column.normalized_type.clone(),
        received: json_type_name(value).to_owned(),
    }
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(number) if number.is_i64() || number.is_u64() => "integer",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn primary_key_dedup_key(table: &TableCatalog, mutation: &MutationOp) -> Option<String> {
    let primary_key = table.primary_key.as_ref()?;
    let row = mutation.rows.first()?;
    let mut parts = Vec::with_capacity(primary_key.columns.len());

    for column in &primary_key.columns {
        let index = mutation
            .columns
            .iter()
            .position(|candidate| candidate == column)?;
        parts.push(
            serde_json::to_string(&row[index])
                .expect("ScalarValue serialization should be infallible"),
        );
    }

    Some(parts.join("\u{1f}"))
}

fn push_kernel_refusal_warning(
    result: &mut SeedResult,
    resource: &ResourceSchema,
    example: &SeedExample,
    refusal: RefusalResult,
) {
    result.push_warning(
        Some(resource.resource_name.clone()),
        refusal
            .detail
            .get("column")
            .or_else(|| refusal.detail.get("columns"))
            .cloned(),
        format!(
            "Example `{}` was skipped after kernel refusal `{}`: {}",
            example.source, refusal.code, refusal.message
        ),
    );
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        backend::{Backend, BaseSnapshotBackend},
        kernel::{storage::TableStorage, value::KernelValue},
        protocol::rest::spec::parse_rest_catalog_bytes,
    };

    use super::{SeedError, seed_from_spec};

    fn parse(raw: &str) -> super::RestCatalog {
        parse_rest_catalog_bytes(raw.as_bytes(), "seed.yaml").expect("spec should parse")
    }

    fn empty_backend(catalog: &super::RestCatalog) -> BaseSnapshotBackend {
        let tables = catalog
            .catalog
            .tables
            .iter()
            .map(|table| TableStorage::new(table).expect("table storage should build"));
        BaseSnapshotBackend::new(tables).expect("backend should build")
    }

    #[test]
    fn schema_level_examples_seed_rows_through_kernel_mutation_path() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      properties:
        id: { type: integer }
        name: { type: string }
        size: { type: number, format: decimal }
      example:
        id: 1
        name: first.txt
        size: 10.50
paths: {}
"#,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.examples_found, 1);
        assert_eq!(result.rows_seeded, 1);
        assert_eq!(result.resources_seeded, 1);
        assert_eq!(
            backend
                .visible_table("files")
                .expect("files table")
                .row_count(),
            1
        );
        assert_eq!(
            result.summary_message(),
            "Seeded 1 rows across 1 resource types from spec examples"
        );
    }

    #[test]
    fn no_examples_returns_empty_result_with_startup_message() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id]
      properties:
        id: { type: string }
paths: {}
"#,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.examples_found, 0);
        assert_eq!(result.rows_seeded, 0);
        assert_eq!(
            result.summary_message(),
            "No spec examples found - backend starts empty"
        );
        assert!(
            result
                .warnings
                .iter()
                .any(|warning| warning.message.contains("No spec examples found"))
        );
    }

    #[test]
    fn type_mismatch_in_example_skips_row_and_records_warning() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, size]
      properties:
        id: { type: string }
        size: { type: integer }
      example:
        id: first
        size: large
paths: {}
"#,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.rows_seeded, 0);
        assert_eq!(result.skipped_examples, 1);
        assert_eq!(
            backend
                .visible_table("files")
                .expect("files table")
                .row_count(),
            0
        );
        assert!(result.warnings.iter().any(|warning| {
            warning.field.as_deref() == Some("size")
                && warning.message.contains("expected integer")
                && warning.message.contains("received string")
        }));
    }

    #[test]
    fn byte_format_examples_seed_base64_as_kernel_bytes() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, content]
      properties:
        id: { type: string }
        content: { type: string, format: byte }
      example:
        id: file-1
        content: aGVsbG8=
paths: {}
"#,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.examples_found, 1);
        assert_eq!(result.rows_seeded, 1);
        assert_eq!(result.skipped_examples, 0);

        let table = catalog.catalog.table("files").expect("files table");
        let content_index = table
            .columns
            .iter()
            .position(|column| column.name == "content")
            .expect("content column");
        let row = backend
            .visible_table("files")
            .expect("files storage")
            .rows()
            .next()
            .expect("seeded row");

        assert_eq!(
            row.values[content_index],
            KernelValue::Bytes(b"hello".to_vec())
        );
    }

    #[test]
    fn array_example_fields_seed_as_kernel_arrays() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    Booking:
      type: object
      required: [id]
      properties:
        id: { type: string }
        rooms:
          type: array
          items: { type: string }
      example:
        id: booking-1
        rooms: [deluxe, suite]
paths: {}
"#,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.rows_seeded, 1);
        assert_eq!(result.skipped_examples, 0);

        let table = catalog.catalog.table("bookings").expect("bookings table");
        let rooms_index = table
            .columns
            .iter()
            .position(|column| column.name == "rooms")
            .expect("rooms column");
        let row = backend
            .visible_table("bookings")
            .expect("bookings storage")
            .rows()
            .next()
            .expect("seeded row");
        assert_eq!(
            row.values[rooms_index],
            KernelValue::Array(vec![
                KernelValue::Text(String::from("deluxe")),
                KernelValue::Text(String::from("suite")),
            ])
        );
    }

    #[test]
    fn resource_without_pk_seeds_all_examples_and_warns_once() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    LogEntry:
      type: object
      required: [message]
      properties:
        message: { type: string }
paths:
  /logentrys:
    post:
      requestBody:
        content:
          application/json:
            example:
              message: from-path
      responses: {}
"#,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.rows_seeded, 1);
        assert_eq!(
            backend
                .visible_table("logentrys")
                .expect("log table")
                .row_count(),
            1
        );
        assert!(result.warnings.iter().any(|warning| {
            warning.resource_name.as_deref() == Some("logentrys")
                && warning.message.contains("has no declared PK")
        }));
    }

    #[test]
    fn duplicate_pk_examples_keep_first_seen_value() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      properties:
        id: { type: integer }
        name: { type: string }
      example:
        id: 1
        name: schema-first
paths:
  /files:
    post:
      requestBody:
        content:
          application/json:
            example:
              id: 1
              name: path-second
      responses: {}
"#,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.rows_seeded, 1);
        assert_eq!(result.skipped_examples, 1);
        let rows = backend
            .visible_table("files")
            .expect("files table")
            .rows()
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            serde_json::to_value(&rows[0].values[1]).expect("serialize value"),
            json!({ "kind": "text", "value": "schema-first" })
        );
    }

    #[test]
    fn nested_path_request_examples_are_not_seeded() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      properties:
        id: { type: integer }
        name: { type: string }
paths:
  /files/{file_id}/versions/{version_id}:
    post:
      requestBody:
        content:
          application/json:
            example:
              id: 1
              name: nested
      responses: {}
"#,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.examples_found, 0);
        assert_eq!(result.rows_seeded, 0);
        assert_eq!(
            backend
                .visible_table("files")
                .expect("files table")
                .row_count(),
            0
        );
    }

    #[test]
    fn referenced_component_examples_seed_when_path_declares_request_body_example() {
        let catalog = parse(
            r##"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      properties:
        id: { type: integer }
        name: { type: string }
  examples:
    FileCreate:
      value:
        id: 7
        name: component-ref
paths:
  /files:
    post:
      requestBody:
        content:
          application/json:
            examples:
              default:
                $ref: "#/components/examples/FileCreate"
      responses: {}
"##,
        );
        let mut backend = empty_backend(&catalog);

        let result = seed_from_spec(&catalog, &mut backend).expect("seed should succeed");

        assert_eq!(result.rows_seeded, 1);
        assert_eq!(
            backend
                .visible_table("files")
                .expect("files table")
                .row_count(),
            1
        );
    }

    #[test]
    fn missing_backend_table_is_a_hard_seed_error() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id]
      properties:
        id: { type: string }
      example: { id: first }
paths: {}
"#,
        );
        let mut backend = BaseSnapshotBackend::new(Vec::new()).expect("empty backend");

        let error = seed_from_spec(&catalog, &mut backend).expect_err("missing table should fail");

        assert_eq!(
            error,
            SeedError::MissingBackendTable {
                resource: "files".to_owned()
            }
        );
    }
}
