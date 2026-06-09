//! OpenAPI spec parsing for the REST protocol adapter.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    path::Path,
    time::Duration,
};

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};

use crate::{
    catalog::{Catalog, ColumnCatalog, KeyConstraint, TableCatalog},
    refusal::{RefusalEnvelope, RefusalResult},
};

use super::xext::{XTwinningExt, parse_x_twinning};

const MAX_RAW_SPEC_BYTES: usize = 20 * 1024 * 1024;
const MAX_SEMANTIC_SPEC_BYTES: usize = 10 * 1024 * 1024;
const REMOTE_REF_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestCatalog {
    pub resources: HashMap<String, ResourceSchema>,
    pub catalog: Catalog,
    pub spec_hash: String,
    pub servers: Vec<ServerObject>,
    pub x_twinning: Option<XTwinningExt>,
    pub security_schemes: Vec<SecurityScheme>,
    pub security_requirements: Vec<SecurityRequirement>,
    pub component_schemas: HashMap<String, SchemaObject>,
    pub paths: HashMap<String, PathItem>,
    pub component_responses: HashMap<String, ResponseObject>,
    pub remote_response_refs: HashMap<String, ResponseObject>,
    pub component_request_bodies: HashMap<String, RequestBodyObject>,
    pub component_parameters: HashMap<String, ParameterObject>,
    pub component_examples: HashMap<String, ExampleObject>,
    pub warnings: Vec<SpecWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSchema {
    pub schema_name: String,
    pub resource_name: String,
    pub columns: Vec<ResourceColumn>,
    pub required: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub additional_properties_allowed: bool,
    pub example: Option<JsonValue>,
    pub meta: ResourceMeta,
    pub warnings: Vec<SpecWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResourceMeta {
    pub item_resource: Option<String>,
    pub object_map_value_resource: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path_lookup_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceColumn {
    pub name: String,
    pub declared_type: String,
    pub normalized_type: String,
    pub nullable: bool,
    pub format: Option<String>,
    pub warnings: Vec<SpecWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityScheme {
    pub name: String,
    pub kind: Option<String>,
    pub scheme: Option<String>,
    pub bearer_format: Option<String>,
    pub location: Option<String>,
    pub parameter_name: Option<String>,
    pub raw: JsonValue,
}

pub type SecurityRequirement = HashMap<String, Vec<String>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecWarning {
    pub resource_name: Option<String>,
    pub field: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenApiDoc {
    pub openapi: Option<String>,
    pub info: Option<InfoObject>,
    #[serde(default)]
    pub servers: Vec<ServerObject>,
    #[serde(default)]
    pub security: Vec<SecurityRequirement>,
    #[serde(default)]
    pub components: ComponentsObject,
    #[serde(default)]
    pub paths: HashMap<String, PathItem>,
    #[serde(default, flatten)]
    pub extensions: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfoObject {
    pub title: Option<String>,
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ServerObject {
    pub url: String,
    pub description: Option<String>,
    #[serde(default)]
    pub variables: HashMap<String, ServerVariableObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ServerVariableObject {
    #[serde(rename = "enum", default)]
    pub enum_values: Vec<String>,
    pub default: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ComponentsObject {
    #[serde(default)]
    pub schemas: HashMap<String, SchemaObject>,
    #[serde(default)]
    pub responses: HashMap<String, ResponseObject>,
    #[serde(default)]
    pub security_schemes: HashMap<String, SecuritySchemeObject>,
    #[serde(default)]
    pub request_bodies: HashMap<String, RequestBodyObject>,
    #[serde(default)]
    pub parameters: HashMap<String, ParameterObject>,
    #[serde(default)]
    pub examples: HashMap<String, ExampleObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PathItem {
    #[serde(default)]
    pub parameters: Vec<ParameterObject>,
    pub get: Option<OperationObject>,
    pub head: Option<OperationObject>,
    pub post: Option<OperationObject>,
    pub put: Option<OperationObject>,
    pub patch: Option<OperationObject>,
    pub delete: Option<OperationObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OperationObject {
    #[serde(default)]
    pub parameters: Vec<ParameterObject>,
    pub security: Option<Vec<SecurityRequirement>>,
    pub request_body: Option<RequestBodyObject>,
    #[serde(default, deserialize_with = "deserialize_response_map")]
    pub responses: HashMap<String, ResponseObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(untagged)]
enum ResponseStatusCodeKey {
    Text(String),
    Number(u16),
}

impl ResponseStatusCodeKey {
    fn into_string(self) -> String {
        match self {
            Self::Text(value) => value,
            Self::Number(value) => value.to_string(),
        }
    }
}

fn deserialize_response_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, ResponseObject>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = BTreeMap::<ResponseStatusCodeKey, ResponseObject>::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .map(|(status, response)| (status.into_string(), response))
        .collect())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ParameterObject {
    #[serde(rename = "$ref")]
    pub reference: Option<String>,
    pub name: Option<String>,
    #[serde(rename = "in")]
    pub location: Option<String>,
    pub required: Option<bool>,
    pub schema: Option<SchemaObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RequestBodyObject {
    #[serde(rename = "$ref")]
    pub reference: Option<String>,
    #[serde(default)]
    pub content: HashMap<String, MediaTypeObject>,
    pub required: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ResponseObject {
    #[serde(rename = "$ref")]
    pub reference: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub content: HashMap<String, MediaTypeObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MediaTypeObject {
    pub schema: Option<SchemaObject>,
    pub example: Option<JsonValue>,
    #[serde(default)]
    pub examples: HashMap<String, ExampleObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ExampleObject {
    #[serde(rename = "$ref")]
    pub reference: Option<String>,
    pub summary: Option<String>,
    pub value: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SecuritySchemeObject {
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub scheme: Option<String>,
    pub bearer_format: Option<String>,
    #[serde(rename = "in")]
    pub location: Option<String>,
    pub name: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SchemaObject {
    #[serde(rename = "$ref")]
    pub reference: Option<String>,
    #[serde(rename = "type")]
    pub schema_type: Option<JsonValue>,
    #[serde(rename = "enum", default)]
    pub enum_values: Vec<JsonValue>,
    pub format: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, SchemaObject>,
    #[serde(default)]
    pub required: Vec<String>,
    #[serde(default)]
    pub all_of: Vec<SchemaObject>,
    #[serde(default)]
    pub one_of: Vec<SchemaObject>,
    #[serde(default)]
    pub any_of: Vec<SchemaObject>,
    pub additional_properties: Option<JsonValue>,
    pub example: Option<JsonValue>,
    pub items: Option<Box<SchemaObject>>,
}

pub fn load_rest_catalog(path: &Path) -> RefusalResult<RestCatalog> {
    let metadata = fs::metadata(path).map_err(|error| Box::new(refusal_io_read(path, &error)))?;
    if metadata.len() > MAX_RAW_SPEC_BYTES as u64 {
        return Err(Box::new(openapi_refusal(
            "E_OPENAPI_SPEC_TOO_LARGE",
            format!(
                "OpenAPI spec `{}` exceeds the 20MB raw safety limit.",
                path.display()
            ),
            json!({
                "path": path.display().to_string(),
                "max_bytes": MAX_RAW_SPEC_BYTES,
                "actual_bytes": metadata.len(),
                "limit_kind": "raw",
            }),
            None,
        )));
    }

    let raw = fs::read(path).map_err(|error| Box::new(refusal_io_read(path, &error)))?;
    parse_rest_catalog_bytes(&raw, path.display().to_string())
}

pub fn parse_rest_catalog_bytes(
    raw_spec: &[u8],
    source: impl Into<String>,
) -> RefusalResult<RestCatalog> {
    let source = source.into();
    if raw_spec.len() > MAX_RAW_SPEC_BYTES {
        return Err(Box::new(openapi_refusal(
            "E_OPENAPI_SPEC_TOO_LARGE",
            format!("OpenAPI spec `{source}` exceeds the 20MB raw safety limit."),
            json!({
                "source": source,
                "max_bytes": MAX_RAW_SPEC_BYTES,
                "actual_bytes": raw_spec.len(),
                "limit_kind": "raw",
            }),
            None,
        )));
    }

    let spec_hash = spec_hash(raw_spec);
    let raw_value = parse_openapi_raw_value(raw_spec, &source)?;
    enforce_semantic_spec_size(&raw_value, &source)?;

    validate_openapi_version(&source, &raw_value)?;

    let document: OpenApiDoc = serde_yaml::from_value(raw_value).map_err(|error| {
        Box::new(openapi_refusal(
            "E_OPENAPI_PARSE",
            format!("OpenAPI spec `{source}` could not be decoded."),
            json!({ "source": source, "error": error.to_string() }),
            None,
        ))
    })?;

    build_rest_catalog(document, spec_hash)
}

fn enforce_semantic_spec_size(raw_value: &serde_yaml::Value, source: &str) -> RefusalResult<()> {
    let compact = serde_json::to_vec(raw_value).map_err(|error| {
        Box::new(openapi_refusal(
            "E_OPENAPI_PARSE",
            format!("OpenAPI spec `{source}` could not be normalized for size validation."),
            json!({ "source": source, "error": error.to_string() }),
            None,
        ))
    })?;

    if compact.len() > MAX_SEMANTIC_SPEC_BYTES {
        return Err(Box::new(openapi_refusal(
            "E_OPENAPI_SPEC_TOO_LARGE",
            format!("OpenAPI spec `{source}` exceeds the 10MB semantic safety limit."),
            json!({
                "source": source,
                "max_bytes": MAX_SEMANTIC_SPEC_BYTES,
                "actual_bytes": compact.len(),
                "limit_kind": "semantic",
            }),
            None,
        )));
    }

    Ok(())
}

fn parse_openapi_raw_value(raw_spec: &[u8], source: &str) -> RefusalResult<serde_yaml::Value> {
    match serde_yaml::from_slice(raw_spec) {
        Ok(value) => Ok(value),
        Err(error) if should_retry_oversized_yaml_integer(&error) => {
            let Some(sanitized) = quote_oversized_yaml_integer_scalars(raw_spec) else {
                return Err(Box::new(openapi_parse_refusal(source, &error)));
            };

            serde_yaml::from_str(&sanitized)
                .map_err(|_| Box::new(openapi_parse_refusal(source, &error)))
        }
        Err(error) => Err(Box::new(openapi_parse_refusal(source, &error))),
    }
}

fn should_retry_oversized_yaml_integer(error: &serde_yaml::Error) -> bool {
    let message = error.to_string();
    message.contains(" as u128") || message.contains(" as i128")
}

fn quote_oversized_yaml_integer_scalars(raw_spec: &[u8]) -> Option<String> {
    let raw = std::str::from_utf8(raw_spec).ok()?;
    let mut changed = false;
    let mut sanitized = String::with_capacity(raw.len());

    for line in raw.split_inclusive('\n') {
        let (line_body, line_ending) = line
            .strip_suffix("\r\n")
            .map(|body| (body, "\r\n"))
            .or_else(|| line.strip_suffix('\n').map(|body| (body, "\n")))
            .unwrap_or((line, ""));

        let quoted = quote_oversized_yaml_integer_line(line_body);
        changed |= quoted.as_deref().is_some();
        sanitized.push_str(quoted.as_deref().unwrap_or(line_body));
        sanitized.push_str(line_ending);
    }

    changed.then_some(sanitized)
}

fn quote_oversized_yaml_integer_line(line: &str) -> Option<String> {
    let colon_index = line.find(':')?;
    let after_colon = &line[colon_index + 1..];
    let leading_ws_len = after_colon.len() - after_colon.trim_start().len();
    let value_with_suffix = &after_colon[leading_ws_len..];
    let (integer, suffix) = split_plain_integer_scalar(value_with_suffix)?;

    if !is_oversized_signed_integer(integer) {
        return None;
    }

    let prefix = &line[..colon_index + 1 + leading_ws_len];
    Some(format!("{prefix}\"{integer}\"{suffix}"))
}

fn split_plain_integer_scalar(value: &str) -> Option<(&str, &str)> {
    let mut end = 0;
    let mut saw_digit = false;

    for (index, character) in value.char_indices() {
        if index == 0 && (character == '-' || character == '+') {
            end = character.len_utf8();
            continue;
        }
        if character.is_ascii_digit() {
            saw_digit = true;
            end = index + character.len_utf8();
            continue;
        }
        break;
    }

    if !saw_digit {
        return None;
    }

    let integer = &value[..end];
    let digits = integer.trim_start_matches(['-', '+']);
    debug_assert!(digits.chars().all(|character| character.is_ascii_digit()));
    let suffix = &value[end..];
    let trimmed_suffix = suffix.trim_start();
    if !trimmed_suffix.is_empty() && !trimmed_suffix.starts_with('#') {
        return None;
    }

    Some((integer, suffix))
}

fn is_oversized_signed_integer(value: &str) -> bool {
    let unsigned = value.trim_start_matches(['-', '+']).trim_start_matches('0');
    if unsigned.is_empty() {
        return false;
    }

    let limit = if value.starts_with('-') {
        "9223372036854775808"
    } else {
        "9223372036854775807"
    };

    unsigned.len() > limit.len() || (unsigned.len() == limit.len() && unsigned > limit)
}

fn openapi_parse_refusal(source: &str, error: &serde_yaml::Error) -> RefusalEnvelope {
    openapi_refusal(
        "E_OPENAPI_PARSE",
        format!("OpenAPI spec `{source}` is not valid YAML or JSON."),
        json!({ "source": source, "error": error.to_string() }),
        None,
    )
}

pub fn build_rest_catalog(
    mut document: OpenApiDoc,
    spec_hash: String,
) -> RefusalResult<RestCatalog> {
    let mut resources = HashMap::new();
    let mut tables_by_name = BTreeMap::new();
    let mut warnings = Vec::new();
    let x_twinning = parse_x_twinning(&document);
    let remote_response_refs = resolve_remote_response_refs(&mut document, &mut warnings);
    let component_schemas = document.components.schemas.clone();

    let mut schemas = document
        .components
        .schemas
        .iter()
        .collect::<Vec<(&String, &SchemaObject)>>();
    schemas.sort_by_key(|(name, _)| *name);

    for (schema_name, schema) in schemas {
        let resource_schema =
            build_resource_schema(schema_name, schema, &document.components.schemas)?;
        warnings.extend(resource_schema.warnings.clone());
        resources.insert(resource_schema.resource_name.clone(), resource_schema);
    }

    add_path_lookup_selector_columns(&mut resources, &document);

    for resource in resources.values() {
        let table = table_from_resource(resource);
        tables_by_name.insert(table.name.clone(), table);
    }

    let tables = tables_by_name.into_values().collect::<Vec<_>>();
    let column_count = tables.iter().map(|table| table.columns.len()).sum();
    let index_count = 0;
    let constraint_count = tables.iter().map(TableCatalog::constraint_count).sum();
    let table_count = tables.len();

    let security_schemes = security_schemes(document.components.security_schemes);

    Ok(RestCatalog {
        resources,
        catalog: Catalog {
            dialect: "openapi".to_owned(),
            tables,
            table_count,
            column_count,
            index_count,
            constraint_count,
        },
        spec_hash,
        servers: document.servers,
        x_twinning,
        security_schemes,
        security_requirements: document.security,
        component_schemas,
        paths: document.paths,
        component_responses: document.components.responses,
        remote_response_refs,
        component_request_bodies: document.components.request_bodies,
        component_parameters: document.components.parameters,
        component_examples: document.components.examples,
        warnings,
    })
}

pub fn component_response_name(reference: &str) -> Option<&str> {
    reference.strip_prefix("#/components/responses/")
}

fn remote_response_ref(reference: &str) -> bool {
    is_remote_schema_ref(reference)
}

pub fn resolve_response_object<'a>(
    catalog: &'a RestCatalog,
    response: &'a ResponseObject,
) -> &'a ResponseObject {
    let mut current = response;
    let mut seen = Vec::new();

    for _ in 0..16 {
        let Some(reference) = current.reference.as_deref() else {
            return current;
        };
        if remote_response_ref(reference) {
            let Some(next) = catalog.remote_response_refs.get(reference) else {
                return current;
            };
            current = next;
            continue;
        }
        let Some(name) = component_response_name(reference) else {
            return current;
        };
        if seen.iter().any(|seen_name| seen_name == name) {
            return current;
        }
        let Some(next) = catalog.component_responses.get(name) else {
            return current;
        };

        seen.push(name.to_owned());
        current = next;
    }

    current
}

fn resolve_remote_response_refs(
    document: &mut OpenApiDoc,
    warnings: &mut Vec<SpecWarning>,
) -> HashMap<String, ResponseObject> {
    let references = collect_remote_response_refs(document);
    let mut cache = HashMap::new();
    let mut resolved = HashMap::new();

    for reference in references {
        match resolve_remote_response_ref(&reference, &mut cache, &mut document.components.schemas)
        {
            Some(response) => {
                resolved.insert(reference, response);
            }
            None => warnings.push(SpecWarning {
                resource_name: None,
                field: None,
                message: format!("Remote response ref could not be resolved: {reference}"),
            }),
        }
    }

    resolved
}

fn collect_remote_response_refs(document: &OpenApiDoc) -> BTreeSet<String> {
    let mut references = BTreeSet::new();

    for path_item in document.paths.values() {
        for operation in [
            &path_item.get,
            &path_item.head,
            &path_item.post,
            &path_item.put,
            &path_item.patch,
            &path_item.delete,
        ]
        .into_iter()
        .flatten()
        {
            for response in operation.responses.values() {
                collect_remote_response_ref(response, &mut references);
            }
        }
    }

    for response in document.components.responses.values() {
        collect_remote_response_ref(response, &mut references);
    }

    references
}

fn collect_remote_response_ref(response: &ResponseObject, references: &mut BTreeSet<String>) {
    if let Some(reference) = response.reference.as_deref()
        && remote_response_ref(reference)
    {
        references.insert(reference.to_owned());
    }
}

fn resolve_remote_response_ref(
    reference: &str,
    cache: &mut HashMap<String, JsonValue>,
    schemas: &mut HashMap<String, SchemaObject>,
) -> Option<ResponseObject> {
    let value = fetch_remote_ref(reference, cache)?;
    let response = serde_json::from_value::<ResponseObject>(value).map_err(|error| {
        eprintln!(
            "[rest] warning: remote response ref `{reference}` did not decode as an OpenAPI response object: {error}"
        );
        error
    }).ok()?;

    let base_url = remote_ref_base_url(reference);
    let mut importing = BTreeSet::new();
    import_response_schema_dependencies(&response, base_url, cache, schemas, &mut importing);
    Some(response)
}

fn import_response_schema_dependencies(
    response: &ResponseObject,
    base_url: &str,
    cache: &mut HashMap<String, JsonValue>,
    schemas: &mut HashMap<String, SchemaObject>,
    importing: &mut BTreeSet<String>,
) {
    for media in response.content.values() {
        if let Some(schema) = media.schema.as_ref() {
            import_schema_dependencies(schema, base_url, cache, schemas, importing);
        }
    }
}

fn import_schema_dependencies(
    schema: &SchemaObject,
    base_url: &str,
    cache: &mut HashMap<String, JsonValue>,
    schemas: &mut HashMap<String, SchemaObject>,
    importing: &mut BTreeSet<String>,
) {
    if let Some(reference) = schema.reference.as_deref()
        && let Some(schema_name) = local_schema_ref_name(reference)
    {
        import_remote_schema(schema_name, base_url, cache, schemas, importing);
    }

    for property in schema.properties.values() {
        import_schema_dependencies(property, base_url, cache, schemas, importing);
    }
    for item in &schema.all_of {
        import_schema_dependencies(item, base_url, cache, schemas, importing);
    }
    for item in &schema.one_of {
        import_schema_dependencies(item, base_url, cache, schemas, importing);
    }
    for item in &schema.any_of {
        import_schema_dependencies(item, base_url, cache, schemas, importing);
    }
    if let Some(items) = schema.items.as_deref() {
        import_schema_dependencies(items, base_url, cache, schemas, importing);
    }
    if let Some(JsonValue::Object(_)) = &schema.additional_properties
        && let Some(additional_schema) = schema_additional_properties_schema(schema)
    {
        import_schema_dependencies(&additional_schema, base_url, cache, schemas, importing);
    }
}

fn import_remote_schema(
    schema_name: &str,
    base_url: &str,
    cache: &mut HashMap<String, JsonValue>,
    schemas: &mut HashMap<String, SchemaObject>,
    importing: &mut BTreeSet<String>,
) {
    if schemas.contains_key(schema_name) || !importing.insert(schema_name.to_owned()) {
        return;
    }

    let reference = format!("{base_url}#/components/schemas/{schema_name}");
    let Some(value) = fetch_remote_ref(&reference, cache) else {
        importing.remove(schema_name);
        return;
    };
    let Ok(mut schema) = serde_json::from_value::<SchemaObject>(value).map_err(|error| {
        eprintln!(
            "[rest] warning: remote schema ref `{reference}` did not decode as an OpenAPI schema object: {error}"
        );
        error
    }) else {
        importing.remove(schema_name);
        return;
    };

    qualify_local_schema_refs_as_remote(&mut schema, base_url);
    schemas.insert(schema_name.to_owned(), schema);
    importing.remove(schema_name);
}

fn qualify_local_schema_refs_as_remote(schema: &mut SchemaObject, base_url: &str) {
    if let Some(reference) = schema.reference.as_mut()
        && reference.starts_with("#/components/schemas/")
    {
        *reference = format!("{base_url}{reference}");
    }

    for property in schema.properties.values_mut() {
        qualify_local_schema_refs_as_remote(property, base_url);
    }
    for item in &mut schema.all_of {
        qualify_local_schema_refs_as_remote(item, base_url);
    }
    for item in &mut schema.one_of {
        qualify_local_schema_refs_as_remote(item, base_url);
    }
    for item in &mut schema.any_of {
        qualify_local_schema_refs_as_remote(item, base_url);
    }
    if let Some(items) = schema.items.as_deref_mut() {
        qualify_local_schema_refs_as_remote(items, base_url);
    }
    if let Some(JsonValue::Object(_)) = &schema.additional_properties
        && let Some(mut additional_schema) = schema_additional_properties_schema(schema)
    {
        qualify_local_schema_refs_as_remote(&mut additional_schema, base_url);
        if let Ok(value) = serde_json::to_value(additional_schema) {
            schema.additional_properties = Some(value);
        }
    }
}

fn local_schema_ref_name(reference: &str) -> Option<&str> {
    reference.strip_prefix("#/components/schemas/")
}

fn remote_ref_base_url(reference: &str) -> &str {
    reference
        .split_once('#')
        .map(|(base_url, _)| base_url)
        .unwrap_or(reference)
}

fn fetch_remote_ref(url: &str, cache: &mut HashMap<String, JsonValue>) -> Option<JsonValue> {
    let (base_url, fragment) = url.split_once('#').unwrap_or((url, ""));
    let document = if let Some(cached) = cache.get(base_url) {
        cached.clone()
    } else {
        let document = fetch_remote_document(base_url)?;
        cache.insert(base_url.to_owned(), document.clone());
        document
    };

    if fragment.is_empty() {
        return Some(document);
    }

    document.pointer(fragment).cloned().or_else(|| {
        eprintln!("[rest] warning: remote ref `{url}` has no matching JSON pointer fragment");
        None
    })
}

fn fetch_remote_document(url: &str) -> Option<JsonValue> {
    let response = ureq::get(url)
        .timeout(REMOTE_REF_TIMEOUT)
        .call()
        .map_err(|error| {
            eprintln!("[rest] warning: remote OpenAPI ref document `{url}` fetch failed: {error}");
            error
        })
        .ok()?;

    let body = response
        .into_string()
        .map_err(|error| {
            eprintln!(
                "[rest] warning: remote OpenAPI ref document `{url}` body read failed: {error}"
            );
            error
        })
        .ok()?;

    match serde_json::from_str::<JsonValue>(&body) {
        Ok(value) => Some(value),
        Err(json_error) => serde_yaml::from_str::<JsonValue>(&body)
            .map_err(|yaml_error| {
                eprintln!(
                    "[rest] warning: remote OpenAPI ref document `{url}` parse failed as JSON ({json_error}) and YAML ({yaml_error})"
                );
                yaml_error
            })
            .ok(),
    }
}

fn build_resource_schema(
    schema_name: &str,
    schema: &SchemaObject,
    schemas: &HashMap<String, SchemaObject>,
) -> RefusalResult<ResourceSchema> {
    let resolved = resolve_schema_for_resource(schema_name, schema, schemas)?;
    let resource_name = resource_name(schema_name);
    let meta = resource_meta(schema);
    let mut warnings = Vec::new();
    warnings.extend(remote_schema_ref_warnings(schema_name, schema));

    if resolved.properties.is_empty() {
        match schema_type_string(&resolved).as_deref() {
            Some("object") => warnings.push(SpecWarning {
                resource_name: Some(resource_name.clone()),
                field: None,
                message: "Schema has type object but no properties; resource has zero fields."
                    .to_owned(),
            }),
            Some("array") => {}
            Some(schema_type) => warnings.push(SpecWarning {
                resource_name: Some(resource_name.clone()),
                field: None,
                message: format!(
                    "Schema has type {schema_type} and is not materialized as REST resource fields in v0."
                ),
            }),
            None => warnings.push(SpecWarning {
                resource_name: Some(resource_name.clone()),
                field: None,
                message: "Schema has no properties; resource has zero fields.".to_owned(),
            }),
        }
    }

    let mut required = canonical_required_fields(&resolved.properties, &resolved.required);
    required.sort();
    required.dedup();

    let mut columns = Vec::with_capacity(resolved.properties.len());
    for (field_name, property) in &resolved.properties {
        let column = column_from_schema(&resource_name, field_name, property, schemas, &required)?;
        warnings.extend(column.warnings.clone());
        columns.push(column);
    }

    let primary_key = primary_key_for_resource(schema_name, &columns, &required);
    if let Some(primary_key_columns) = &primary_key {
        for column in &mut columns {
            if primary_key_columns.iter().any(|pk| pk == &column.name) {
                column.nullable = false;
            }
        }
    }

    Ok(ResourceSchema {
        schema_name: schema_name.to_owned(),
        resource_name,
        columns,
        required,
        primary_key,
        additional_properties_allowed: additional_properties_allowed(&resolved),
        example: resolved.example.clone(),
        meta,
        warnings,
    })
}

fn canonical_required_fields(
    properties: &BTreeMap<String, SchemaObject>,
    required: &[String],
) -> Vec<String> {
    required
        .iter()
        .map(|field| canonical_property_name(properties, field).unwrap_or_else(|| field.clone()))
        .collect()
}

fn canonical_property_name(
    properties: &BTreeMap<String, SchemaObject>,
    field: &str,
) -> Option<String> {
    if properties.contains_key(field) {
        return Some(field.to_owned());
    }

    let mut matches = properties
        .keys()
        .filter(|property| property.eq_ignore_ascii_case(field));
    let matched = matches.next()?;
    if matches.next().is_none() {
        Some(matched.clone())
    } else {
        None
    }
}

fn resource_meta(schema: &SchemaObject) -> ResourceMeta {
    ResourceMeta {
        item_resource: array_item_resource_name(schema),
        object_map_value_resource: object_map_value_resource_name(schema),
        path_lookup_columns: Vec::new(),
    }
}

fn add_path_lookup_selector_columns(
    resources: &mut HashMap<String, ResourceSchema>,
    document: &OpenApiDoc,
) {
    for (path, path_item) in &document.paths {
        let path_params = path_template_params(path);
        let [path_param] = path_params.as_slice() else {
            continue;
        };
        let Some(operation) = path_item.get.as_ref() else {
            continue;
        };
        let Some(response_resource_name) =
            success_response_schema_ref(operation).and_then(|reference| {
                schema_ref_name(reference)
                    .ok()
                    .map(|schema_name| resource_name(&schema_name))
            })
        else {
            continue;
        };
        let Some(resource) = resources.get_mut(&response_resource_name) else {
            continue;
        };
        if !resource_can_use_path_selector(resource, path_param) {
            continue;
        }

        resource.columns.push(ResourceColumn {
            name: path_param.clone(),
            declared_type: String::from("string"),
            normalized_type: String::from("text"),
            nullable: false,
            format: None,
            warnings: Vec::new(),
        });
        resource.required.push(path_param.clone());
        resource.required.sort();
        resource.required.dedup();
        resource.primary_key = Some(vec![path_param.clone()]);
        resource.meta.path_lookup_columns.push(path_param.clone());
        resource.meta.path_lookup_columns.sort();
        resource.meta.path_lookup_columns.dedup();
    }
}

fn resource_can_use_path_selector(resource: &ResourceSchema, path_param: &str) -> bool {
    if resource.primary_key.is_some()
        || resource
            .columns
            .iter()
            .any(|column| column.name == path_param)
    {
        return false;
    }
    if !path_param_is_value_selector(path_param) {
        return false;
    }

    let public_columns = resource
        .columns
        .iter()
        .filter(|column| {
            !resource
                .meta
                .path_lookup_columns
                .iter()
                .any(|hidden| hidden == &column.name)
        })
        .collect::<Vec<_>>();
    let [column] = public_columns.as_slice() else {
        return false;
    };

    column.normalized_type == "array" && matches!(column.name.as_str(), "data" | "items" | "values")
}

fn path_param_is_value_selector(path_param: &str) -> bool {
    matches!(
        canonical_identifier(path_param).as_str(),
        "key" | "field" | "name" | "type"
    )
}

fn canonical_identifier(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(|character| character.to_lowercase())
        .collect()
}

fn success_response_schema_ref(operation: &OperationObject) -> Option<&str> {
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
        .map(|(_, response)| response)
        .or_else(|| operation.responses.get("default"))?;

    response
        .content
        .iter()
        .filter(|(content_type, _)| content_type_is_json(content_type))
        .filter_map(|(_, media)| media.schema.as_ref())
        .filter_map(|schema| schema.reference.as_deref())
        .next()
}

fn content_type_is_json(content_type: &str) -> bool {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();

    media_type == "application/json"
        || (media_type.starts_with("application/") && media_type.ends_with("+json"))
}

fn path_template_params(path: &str) -> Vec<String> {
    let mut params = Vec::new();
    let mut remaining = path;
    while let Some((_, after_open)) = remaining.split_once('{') {
        let Some((name, after_close)) = after_open.split_once('}') else {
            break;
        };
        if !name.is_empty() {
            params.push(name.to_owned());
        }
        remaining = after_close;
    }
    params
}

fn array_item_resource_name(schema: &SchemaObject) -> Option<String> {
    if schema_type_string(schema).as_deref() != Some("array") {
        return None;
    }

    schema.items.as_deref().and_then(schema_item_resource_name)
}

fn schema_item_resource_name(schema: &SchemaObject) -> Option<String> {
    schema
        .reference
        .as_deref()
        .and_then(|reference| schema_ref_name(reference).ok())
        .map(|schema_name| resource_name(&schema_name))
        .or_else(|| array_item_resource_name(schema))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircularRefMode {
    Fatal,
    Placeholder,
}

fn object_map_value_resource_name(schema: &SchemaObject) -> Option<String> {
    if schema_type_string(schema).as_deref() != Some("object") {
        return None;
    }

    let additional_properties = schema.additional_properties.as_ref()?;
    let additional_schema = match additional_properties {
        JsonValue::Object(_) => {
            serde_json::from_value::<SchemaObject>(additional_properties.clone()).ok()?
        }
        JsonValue::Bool(_)
        | JsonValue::Null
        | JsonValue::Array(_)
        | JsonValue::Number(_)
        | JsonValue::String(_) => {
            return None;
        }
    };

    schema_item_resource_name(&additional_schema)
}

fn resolve_schema_for_resource(
    schema_name: &str,
    schema: &SchemaObject,
    schemas: &HashMap<String, SchemaObject>,
) -> RefusalResult<SchemaObject> {
    let mut ref_stack = vec![schema_name.to_owned()];
    resolve_schema(
        schema_name,
        schema,
        schemas,
        &mut ref_stack,
        CircularRefMode::Fatal,
    )
}

fn resolve_schema(
    schema_name: &str,
    schema: &SchemaObject,
    schemas: &HashMap<String, SchemaObject>,
    ref_stack: &mut Vec<String>,
    circular_ref_mode: CircularRefMode,
) -> RefusalResult<SchemaObject> {
    if let Some(reference) = &schema.reference {
        return resolve_schema_reference(
            schema_name,
            reference,
            schemas,
            ref_stack,
            circular_ref_mode,
        );
    }

    let mut resolved = if !schema.all_of.is_empty() {
        merge_all_of(schema_name, schema, schemas, ref_stack, circular_ref_mode)?
    } else {
        schema.clone()
    };

    resolved.properties =
        resolve_schema_properties(schema_name, resolved.properties, schemas, ref_stack)?;

    if let Some(items) = resolved.items.take() {
        resolved.items = Some(Box::new(resolve_schema(
            schema_name,
            &items,
            schemas,
            ref_stack,
            CircularRefMode::Placeholder,
        )?));
    }

    resolved.one_of = resolve_schema_variants(schema_name, resolved.one_of, schemas, ref_stack)?;
    resolved.any_of = resolve_schema_variants(schema_name, resolved.any_of, schemas, ref_stack)?;
    merge_polymorphic_variant_properties(&mut resolved);

    Ok(schema_with_implicit_object_type(&resolved))
}

fn resolve_schema_reference(
    schema_name: &str,
    reference: &str,
    schemas: &HashMap<String, SchemaObject>,
    ref_stack: &mut Vec<String>,
    circular_ref_mode: CircularRefMode,
) -> RefusalResult<SchemaObject> {
    if is_remote_schema_ref(reference) {
        return Ok(remote_schema_ref_placeholder());
    }

    let target_name = schema_ref_name(reference)?;
    if ref_stack.iter().any(|seen| seen == &target_name) {
        return match circular_ref_mode {
            CircularRefMode::Fatal => Err(Box::new(circular_ref_refusal(schema_name, reference))),
            CircularRefMode::Placeholder => Ok(circular_schema_ref_placeholder()),
        };
    }
    let target = schemas.get(&target_name).ok_or_else(|| {
        Box::new(openapi_refusal(
            "E_OPENAPI_REF",
            format!("OpenAPI schema `{schema_name}` references unknown schema `{target_name}`."),
            json!({ "schema": schema_name, "ref": reference }),
            None,
        ))
    })?;

    if circular_ref_mode == CircularRefMode::Placeholder
        && schema_is_nested_object_ref_boundary(target)
    {
        return Ok(circular_schema_ref_placeholder());
    }

    ref_stack.push(target_name.clone());
    let resolved = resolve_schema(&target_name, target, schemas, ref_stack, circular_ref_mode);
    ref_stack.pop();
    resolved
}

fn schema_is_nested_object_ref_boundary(schema: &SchemaObject) -> bool {
    schema_type_string(schema).as_deref() == Some("object")
        || !schema.properties.is_empty()
        || !schema.all_of.is_empty()
        || schema
            .additional_properties
            .as_ref()
            .is_some_and(|value| !matches!(value, JsonValue::Bool(false)))
}

fn remote_schema_ref_placeholder() -> SchemaObject {
    SchemaObject {
        schema_type: Some(JsonValue::String("object".to_owned())),
        additional_properties: Some(JsonValue::Bool(true)),
        ..SchemaObject::default()
    }
}

fn circular_schema_ref_placeholder() -> SchemaObject {
    SchemaObject {
        schema_type: Some(JsonValue::String("object".to_owned())),
        additional_properties: Some(JsonValue::Bool(true)),
        ..SchemaObject::default()
    }
}

fn resolve_schema_properties(
    schema_name: &str,
    properties: BTreeMap<String, SchemaObject>,
    schemas: &HashMap<String, SchemaObject>,
    ref_stack: &mut Vec<String>,
) -> RefusalResult<BTreeMap<String, SchemaObject>> {
    let mut resolved = BTreeMap::new();
    for (field_name, property) in properties {
        resolved.insert(
            field_name,
            resolve_schema(
                schema_name,
                &property,
                schemas,
                ref_stack,
                CircularRefMode::Placeholder,
            )?,
        );
    }
    Ok(resolved)
}

fn resolve_schema_variants(
    schema_name: &str,
    variants: Vec<SchemaObject>,
    schemas: &HashMap<String, SchemaObject>,
    ref_stack: &mut Vec<String>,
) -> RefusalResult<Vec<SchemaObject>> {
    variants
        .into_iter()
        .map(|variant| resolve_schema_variant(schema_name, variant, schemas, ref_stack.as_slice()))
        .collect()
}

fn resolve_schema_variant(
    schema_name: &str,
    variant: SchemaObject,
    schemas: &HashMap<String, SchemaObject>,
    ref_stack: &[String],
) -> RefusalResult<SchemaObject> {
    let Some(reference) = variant.reference.as_deref() else {
        return Ok(schema_with_implicit_object_type(&variant));
    };

    if is_remote_schema_ref(reference) {
        return Ok(remote_schema_ref_placeholder());
    }

    let target_name = schema_ref_name(reference)?;
    if ref_stack.iter().any(|seen| seen == &target_name) {
        return Ok(circular_schema_ref_placeholder());
    }
    let target = schemas.get(&target_name).ok_or_else(|| {
        Box::new(openapi_refusal(
            "E_OPENAPI_REF",
            format!("OpenAPI schema `{schema_name}` references unknown schema `{target_name}`."),
            json!({ "schema": schema_name, "ref": reference }),
            None,
        ))
    })?;

    Ok(schema_with_implicit_object_type(target))
}

fn merge_polymorphic_variant_properties(schema: &mut SchemaObject) {
    let variants = schema
        .one_of
        .iter()
        .chain(schema.any_of.iter())
        .filter(|variant| !variant.properties.is_empty())
        .collect::<Vec<_>>();
    if variants.is_empty() {
        return;
    }

    let mut required_intersection: Option<BTreeSet<String>> = None;
    for variant in variants {
        for (field_name, property) in &variant.properties {
            match schema.properties.get(field_name) {
                Some(existing) => {
                    let merged = merge_polymorphic_property(existing, property);
                    schema.properties.insert(field_name.clone(), merged);
                }
                None => {
                    schema
                        .properties
                        .insert(field_name.clone(), property.clone());
                }
            }
        }

        let variant_required = canonical_required_fields(&variant.properties, &variant.required)
            .into_iter()
            .collect::<BTreeSet<_>>();
        required_intersection = Some(match required_intersection {
            Some(intersection) => intersection
                .intersection(&variant_required)
                .cloned()
                .collect::<BTreeSet<_>>(),
            None => variant_required,
        });
    }

    if let Some(required) = required_intersection {
        schema.required.extend(required);
        schema.required.sort();
        schema.required.dedup();
    }
}

fn merge_polymorphic_property(existing: &SchemaObject, next: &SchemaObject) -> SchemaObject {
    if declared_type(existing) == declared_type(next) {
        return existing.clone();
    }

    SchemaObject {
        one_of: vec![existing.clone(), next.clone()],
        ..SchemaObject::default()
    }
}

fn merge_all_of(
    schema_name: &str,
    schema: &SchemaObject,
    schemas: &HashMap<String, SchemaObject>,
    ref_stack: &mut Vec<String>,
    circular_ref_mode: CircularRefMode,
) -> RefusalResult<SchemaObject> {
    let mut merged = SchemaObject {
        schema_type: Some(JsonValue::String("object".to_owned())),
        additional_properties: schema.additional_properties.clone(),
        example: schema.example.clone(),
        ..SchemaObject::default()
    };

    for item in &schema.all_of {
        let resolved = resolve_schema(schema_name, item, schemas, ref_stack, circular_ref_mode)?;

        for (field_name, property) in resolved.properties {
            if let Some(existing) = merged.properties.get(&field_name) {
                let left = declared_type(existing);
                let right = declared_type(&property);
                if left != right {
                    if let Some(merged_property) =
                        merge_all_of_conflicting_property(existing, &property)
                    {
                        merged.properties.insert(field_name, merged_property);
                        continue;
                    } else {
                        return Err(Box::new(openapi_refusal(
                            "E_OPENAPI_SCHEMA_CONFLICT",
                            format!(
                                "OpenAPI allOf merge for schema `{schema_name}` has conflicting types for field `{field_name}`."
                            ),
                            json!({
                                "schema": schema_name,
                                "field": field_name,
                                "left_type": left,
                                "right_type": right,
                            }),
                            None,
                        )));
                    }
                }
            }
            merged.properties.insert(field_name, property);
        }

        merged.required.extend(resolved.required);
    }

    merged.required.sort();
    merged.required.dedup();
    Ok(merged)
}

fn merge_all_of_conflicting_property(
    left: &SchemaObject,
    right: &SchemaObject,
) -> Option<SchemaObject> {
    if all_of_field_types_can_converge_as_text(left, right) {
        return Some(merge_polymorphic_property(left, right));
    }
    if all_of_field_types_can_converge_as_integer(left, right) {
        return if schema_type_string(left).as_deref() == Some("integer") {
            Some(left.clone())
        } else {
            Some(right.clone())
        };
    }
    None
}

fn all_of_field_types_can_converge_as_text(left: &SchemaObject, right: &SchemaObject) -> bool {
    (is_polymorphic_schema(left) && is_scalar_schema(right))
        || (is_polymorphic_schema(right) && is_scalar_schema(left))
        || (is_untyped_scalar_schema(left) && is_scalar_schema(right))
        || (is_untyped_scalar_schema(right) && is_scalar_schema(left))
}

fn all_of_field_types_can_converge_as_integer(left: &SchemaObject, right: &SchemaObject) -> bool {
    matches!(
        (
            schema_type_string(left).as_deref(),
            schema_type_string(right).as_deref()
        ),
        (Some("integer"), Some("number")) | (Some("number"), Some("integer"))
    )
}

fn is_polymorphic_schema(schema: &SchemaObject) -> bool {
    !schema.one_of.is_empty() || !schema.any_of.is_empty()
}

fn is_scalar_schema(schema: &SchemaObject) -> bool {
    if is_polymorphic_schema(schema) {
        return true;
    }

    schema.properties.is_empty()
        && schema.all_of.is_empty()
        && schema.items.is_none()
        && matches!(
            schema_type_string(schema).as_deref(),
            Some("string" | "integer" | "number" | "boolean") | None
        )
}

fn is_untyped_scalar_schema(schema: &SchemaObject) -> bool {
    !is_polymorphic_schema(schema)
        && schema.properties.is_empty()
        && schema.all_of.is_empty()
        && schema.items.is_none()
        && schema_type_string(schema).is_none()
}

fn column_from_schema(
    resource_name: &str,
    field_name: &str,
    property: &SchemaObject,
    schemas: &HashMap<String, SchemaObject>,
    required: &[String],
) -> RefusalResult<ResourceColumn> {
    let mut warnings = Vec::new();
    let resolved_property = if property.reference.is_some() || !property.all_of.is_empty() {
        let mut ref_stack = vec![resource_name.to_owned()];
        resolve_schema(
            resource_name,
            property,
            schemas,
            &mut ref_stack,
            CircularRefMode::Placeholder,
        )?
    } else {
        property.clone()
    };

    let declared_type = declared_type(&resolved_property);
    let normalized_type =
        normalized_type(&resolved_property, resource_name, field_name, &mut warnings);

    Ok(ResourceColumn {
        name: field_name.to_owned(),
        declared_type,
        normalized_type,
        nullable: !required.iter().any(|field| field == field_name),
        format: resolved_property.format,
        warnings,
    })
}

fn normalized_type(
    schema: &SchemaObject,
    resource_name: &str,
    field_name: &str,
    warnings: &mut Vec<SpecWarning>,
) -> String {
    if !schema.one_of.is_empty() {
        warnings.push(warning(
            resource_name,
            field_name,
            "oneOf is treated as text in REST twin v0.",
        ));
        return "text".to_owned();
    }
    if !schema.any_of.is_empty() {
        warnings.push(warning(
            resource_name,
            field_name,
            "anyOf is treated as text in REST twin v0.",
        ));
        return "text".to_owned();
    }

    match schema_type_string(schema).as_deref() {
        Some("string") => match schema.format.as_deref() {
            Some("date-time") => "timestamp".to_owned(),
            Some("date") => "date".to_owned(),
            Some("byte" | "binary") => "bytes".to_owned(),
            _ => "text".to_owned(),
        },
        Some("integer") => match schema.format.as_deref() {
            Some("int64") => "bigint".to_owned(),
            _ => "integer".to_owned(),
        },
        Some("number") => match schema.format.as_deref() {
            Some("decimal") => "numeric".to_owned(),
            _ => "float".to_owned(),
        },
        Some("boolean") => "boolean".to_owned(),
        Some("object") => "json".to_owned(),
        Some("array") => {
            warnings.push(warning(
                resource_name,
                field_name,
                "array is cataloged for JSON bodies and responses; array path/query filters remain unsupported in REST twin v0.",
            ));
            "array".to_owned()
        }
        Some(_) | None => {
            warnings.push(warning(
                resource_name,
                field_name,
                "missing or null JSON Schema type falls back to text in REST twin v0.",
            ));
            "text".to_owned()
        }
    }
}

fn table_from_resource(resource: &ResourceSchema) -> TableCatalog {
    TableCatalog {
        name: resource.resource_name.clone(),
        columns: resource
            .columns
            .iter()
            .map(|column| ColumnCatalog {
                name: column.name.clone(),
                declared_type: column.declared_type.clone(),
                normalized_type: column.normalized_type.clone(),
                nullable: column.nullable,
                default_sql: None,
            })
            .collect(),
        primary_key: resource.primary_key.as_ref().map(|columns| KeyConstraint {
            name: None,
            columns: columns.clone(),
        }),
        unique_constraints: vec![],
        foreign_keys: vec![],
        checks: vec![],
        indexes: vec![],
    }
}

fn primary_key_for_resource(
    schema_name: &str,
    columns: &[ResourceColumn],
    required: &[String],
) -> Option<Vec<String>> {
    let resource_id = format!("{}_id", schema_name.to_ascii_lowercase());
    for candidate in ["id", resource_id.as_str()] {
        if columns.iter().any(|column| column.name == candidate) {
            return Some(vec![candidate.to_owned()]);
        }
    }

    columns
        .iter()
        .find(|column| {
            column.format.as_deref() == Some("uuid")
                && required.iter().any(|field| field == &column.name)
        })
        .map(|column| vec![column.name.clone()])
}

fn security_schemes(schemes: HashMap<String, SecuritySchemeObject>) -> Vec<SecurityScheme> {
    let mut schemes = schemes.into_iter().collect::<Vec<_>>();
    schemes.sort_by(|(left, _), (right, _)| left.cmp(right));
    schemes
        .into_iter()
        .map(|(name, scheme)| SecurityScheme {
            name,
            kind: scheme.kind.clone(),
            scheme: scheme.scheme.clone(),
            bearer_format: scheme.bearer_format.clone(),
            location: scheme.location.clone(),
            parameter_name: scheme.name.clone(),
            raw: serde_json::to_value(&scheme).unwrap_or(JsonValue::Null),
        })
        .collect()
}

fn validate_openapi_version(source: &str, raw_value: &serde_yaml::Value) -> RefusalResult<()> {
    let version = raw_value
        .as_mapping()
        .and_then(|mapping| mapping.get(serde_yaml::Value::String("openapi".to_owned())))
        .and_then(serde_yaml::Value::as_str);

    let Some(version) = version else {
        return Err(Box::new(openapi_refusal(
            "E_OPENAPI_VERSION",
            "Unsupported OpenAPI version: missing. REST twin requires OpenAPI 3.x.",
            json!({ "source": source, "version": JsonValue::Null }),
            None,
        )));
    };

    if !version.starts_with("3.") {
        return Err(Box::new(openapi_refusal(
            "E_OPENAPI_VERSION",
            format!("Unsupported OpenAPI version: {version}. REST twin requires OpenAPI 3.x."),
            json!({ "source": source, "version": version }),
            None,
        )));
    }

    Ok(())
}

fn spec_hash(raw_spec: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_spec);
    format!("sha256:{:x}", hasher.finalize())
}

fn resource_name(schema_name: &str) -> String {
    format!("{}s", schema_name.to_ascii_lowercase())
}

fn additional_properties_allowed(schema: &SchemaObject) -> bool {
    !matches!(schema.additional_properties, Some(JsonValue::Bool(false)))
}

fn declared_type(schema: &SchemaObject) -> String {
    if !schema.one_of.is_empty() {
        return "oneOf".to_owned();
    }
    if !schema.any_of.is_empty() {
        return "anyOf".to_owned();
    }
    if schema.reference.is_some() {
        return "$ref".to_owned();
    }
    schema_type_string(schema).unwrap_or_else(|| "null".to_owned())
}

fn schema_type_string(schema: &SchemaObject) -> Option<String> {
    match &schema.schema_type {
        Some(JsonValue::String(kind)) => Some(kind.clone()),
        Some(JsonValue::Array(kinds)) => {
            kinds.iter().find_map(JsonValue::as_str).map(str::to_owned)
        }
        Some(JsonValue::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

fn schema_with_implicit_object_type(schema: &SchemaObject) -> SchemaObject {
    let mut resolved = schema.clone();
    if !resolved.properties.is_empty() && schema_type_string(&resolved).as_deref() != Some("object")
    {
        resolved.schema_type = Some(JsonValue::String("object".to_owned()));
    }
    resolved
}

fn remote_schema_ref_warnings(schema_name: &str, schema: &SchemaObject) -> Vec<SpecWarning> {
    let mut references = BTreeSet::new();
    collect_remote_schema_refs(schema, &mut references);
    let resource_name = resource_name(schema_name);
    references
        .into_iter()
        .map(|reference| SpecWarning {
            resource_name: Some(resource_name.clone()),
            field: None,
            message: format!(
                "Remote OpenAPI schema ref `{reference}` is cataloged as an object placeholder; REST twin v0 does not fetch remote schemas."
            ),
        })
        .collect()
}

fn collect_remote_schema_refs(schema: &SchemaObject, references: &mut BTreeSet<String>) {
    if let Some(reference) = &schema.reference
        && is_remote_schema_ref(reference)
    {
        references.insert(reference.clone());
    }

    for property in schema.properties.values() {
        collect_remote_schema_refs(property, references);
    }
    for item in &schema.all_of {
        collect_remote_schema_refs(item, references);
    }
    for item in &schema.one_of {
        collect_remote_schema_refs(item, references);
    }
    for item in &schema.any_of {
        collect_remote_schema_refs(item, references);
    }
    if let Some(items) = schema.items.as_deref() {
        collect_remote_schema_refs(items, references);
    }
    if let Some(JsonValue::Object(_)) = &schema.additional_properties
        && let Some(additional_schema) = schema_additional_properties_schema(schema)
    {
        collect_remote_schema_refs(&additional_schema, references);
    }
}

fn schema_additional_properties_schema(schema: &SchemaObject) -> Option<SchemaObject> {
    let JsonValue::Object(_) = schema.additional_properties.as_ref()? else {
        return None;
    };
    serde_json::from_value::<SchemaObject>(schema.additional_properties.clone()?).ok()
}

fn is_remote_schema_ref(reference: &str) -> bool {
    reference.starts_with("http://") || reference.starts_with("https://")
}

fn schema_ref_name(reference: &str) -> RefusalResult<String> {
    reference
        .strip_prefix("#/components/schemas/")
        .map(str::to_owned)
        .ok_or_else(|| {
            Box::new(openapi_refusal(
                "E_OPENAPI_REF",
                format!("Unsupported OpenAPI $ref `{reference}`. REST twin v0 only supports local component schema refs."),
                json!({ "ref": reference }),
                None,
            ))
        })
}

fn warning(resource_name: &str, field_name: &str, message: &str) -> SpecWarning {
    SpecWarning {
        resource_name: Some(resource_name.to_owned()),
        field: Some(field_name.to_owned()),
        message: message.to_owned(),
    }
}

fn circular_ref_refusal(schema_name: &str, reference: &str) -> RefusalEnvelope {
    openapi_refusal(
        "E_OPENAPI_REF_CIRCULAR",
        format!("Circular OpenAPI $ref chain detected while resolving `{schema_name}`."),
        json!({ "schema": schema_name, "ref": reference }),
        None,
    )
}

fn refusal_io_read(path: &Path, error: &std::io::Error) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_IO_READ",
        format!("Failed to read `{}`.", path.display()),
        json!({ "path": path.display().to_string(), "error": error.to_string() }),
        None,
    )
}

fn openapi_refusal(
    code: impl Into<String>,
    message: impl Into<String>,
    detail: JsonValue,
    next_command: Option<String>,
) -> RefusalEnvelope {
    RefusalEnvelope::new(code, message, detail, next_command)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        backend::{Backend, BaseSnapshotBackend},
        ir::{MutationKind, MutationOp, ScalarValue},
        kernel::{mutation::execute_mutation, storage::TableStorage},
        result::KernelResult,
    };

    use super::{MAX_SEMANTIC_SPEC_BYTES, parse_rest_catalog_bytes, spec_hash};

    fn parse(raw: &str) -> super::RestCatalog {
        parse_rest_catalog_bytes(raw.as_bytes(), "fixture.yaml").expect("spec should parse")
    }

    #[test]
    fn oversized_integer_schema_metadata_does_not_refuse_startup() {
        let catalog = parse(
            r#"
openapi: 3.0.3
components:
  schemas:
    DatabaseConfig:
      type: object
      properties:
        id:
          type: integer
          maximum: 18446744073709552000
          minimum: 4
paths: {}
"#,
        );

        assert!(catalog.resources.contains_key("databaseconfigs"));
        let resource = catalog
            .resources
            .get("databaseconfigs")
            .expect("database config resource");
        let id = resource
            .columns
            .iter()
            .find(|column| column.name == "id")
            .expect("id column");
        assert_eq!(id.normalized_type, "integer");
    }

    #[test]
    fn component_response_refs_resolve_to_response_objects() {
        let mut catalog = parse(
            r##"
openapi: 3.0.3
components:
  responses:
    PetList:
      description: ok
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/PetList"
  schemas:
    PetList:
      type: object
      properties:
        pets:
          type: array
          items:
            $ref: "#/components/schemas/Pet"
    Pet:
      type: object
      properties:
        id: { type: string }
paths: {}
"##,
        );
        let response = super::ResponseObject {
            reference: Some("#/components/responses/PetList".to_owned()),
            ..Default::default()
        };

        assert_eq!(
            super::component_response_name("#/components/responses/PetList"),
            Some("PetList")
        );
        let resolved = super::resolve_response_object(&catalog, &response);
        assert!(resolved.content.contains_key("application/json"));

        let remote_ref =
            "https://example.test/openapi.yaml#/components/responses/PetList".to_owned();
        catalog.remote_response_refs.insert(
            remote_ref.clone(),
            super::ResponseObject {
                description: Some("remote ok".to_owned()),
                content: HashMap::from([(
                    "application/json".to_owned(),
                    super::MediaTypeObject {
                        schema: Some(super::SchemaObject {
                            reference: Some("#/components/schemas/PetList".to_owned()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            },
        );
        let remote_response = super::ResponseObject {
            reference: Some(remote_ref),
            ..Default::default()
        };
        let resolved = super::resolve_response_object(&catalog, &remote_response);
        assert_eq!(resolved.description.as_deref(), Some("remote ok"));
        assert!(resolved.content.contains_key("application/json"));
    }

    #[test]
    fn flat_spec_builds_kernel_catalog_with_shared_resource_keys() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Files, version: "1.0" }
components:
  securitySchemes:
    BearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
  schemas:
    File:
      type: object
      required: [id, name]
      additionalProperties: false
      properties:
        id: { type: integer, format: int64 }
        name: { type: string }
        created_at: { type: string, format: date-time }
        payload: { type: string, format: binary }
paths:
  /files:
    get: { responses: { "200": { description: ok } } }
"#,
        );

        assert_eq!(catalog.catalog.dialect, "openapi");
        assert!(catalog.resources.contains_key("files"));
        assert!(catalog.catalog.table("files").is_some());
        assert_eq!(catalog.catalog.table_count, 1);
        assert_eq!(catalog.catalog.column_count, 4);
        assert_eq!(catalog.catalog.constraint_count, 1);
        assert_eq!(catalog.security_schemes[0].name, "BearerAuth");
        assert!(
            !catalog
                .warnings
                .iter()
                .any(|warning| warning.message.contains("bypasses all auth")),
            "auth mode is runtime-resolved and must not emit stale catalog warnings"
        );

        let files = catalog.resources.get("files").expect("files resource");
        assert_eq!(files.schema_name, "File");
        assert!(!files.additional_properties_allowed);
        assert_eq!(files.primary_key, Some(vec!["id".to_owned()]));
        assert_eq!(files.required, vec!["id".to_owned(), "name".to_owned()]);
        assert_eq!(files.columns[0].name, "created_at");
        assert_eq!(files.columns[0].normalized_type, "timestamp");
        assert_eq!(files.columns[1].name, "id");
        assert_eq!(files.columns[1].normalized_type, "bigint");
        assert!(!files.columns[1].nullable);
        assert_eq!(files.columns[3].normalized_type, "bytes");
    }

    #[test]
    fn parses_global_and_operation_security_requirements() {
        let catalog = parse(
            r#"
openapi: 3.0.3
security:
  - bearerAuth:
      - read:vaults
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
  /public:
    get:
      security: []
      responses: {}
  /vaults:
    get:
      responses: {}
"#,
        );

        assert_eq!(catalog.security_requirements.len(), 1);
        assert_eq!(
            catalog.security_requirements[0]
                .get("bearerAuth")
                .expect("bearer requirement"),
            &vec![String::from("read:vaults")]
        );
        assert!(
            catalog
                .paths
                .get("/public")
                .expect("public path")
                .get
                .as_ref()
                .expect("public get")
                .security
                .as_ref()
                .expect("explicit public security")
                .is_empty()
        );
        assert!(
            catalog
                .paths
                .get("/vaults")
                .expect("vaults path")
                .get
                .as_ref()
                .expect("vaults get")
                .security
                .is_none()
        );
    }

    #[test]
    fn required_fields_canonicalize_unique_case_mismatched_properties() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Xero Folder Shape, version: "1.0" }
components:
  schemas:
    Folder:
      required: [name]
      properties:
        Name: { type: string }
        Id: { type: string, format: uuid }
paths: {}
"#,
        );

        let folders = catalog.resources.get("folders").expect("folders resource");
        assert_eq!(folders.required, vec![String::from("Name")]);
        let name = folders
            .columns
            .iter()
            .find(|column| column.name == "Name")
            .expect("Name column");
        assert!(!name.nullable);
    }

    #[test]
    fn primary_key_inference_uses_case_canonicalized_id_required_field() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Xero FileObject Shape, version: "1.0" }
components:
  schemas:
    FileObject:
      required: [id, name]
      properties:
        Id: { type: string, format: uuid }
        Name: { type: string }
paths: {}
"#,
        );

        let fileobjects = catalog
            .resources
            .get("fileobjects")
            .expect("fileobjects resource");
        assert_eq!(
            fileobjects.required,
            vec![String::from("Id"), String::from("Name")]
        );
        assert_eq!(fileobjects.primary_key, Some(vec![String::from("Id")]));
    }

    #[test]
    fn yaml_numeric_response_status_keys_decode_as_strings() {
        let catalog = parse(
            r##"
openapi: 3.0.3
info: { title: Numeric Responses, version: "1.0" }
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
        200:
          description: ok
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Pet"
        default:
          description: fallback
"##,
        );

        let operation = catalog
            .paths
            .get("/pets")
            .and_then(|path| path.get.as_ref())
            .expect("GET /pets should parse");
        assert!(operation.responses.contains_key("200"));
        assert!(operation.responses.contains_key("default"));
        assert_eq!(
            operation
                .responses
                .get("200")
                .expect("200 response")
                .description
                .as_deref(),
            Some("ok")
        );
    }

    #[test]
    fn spec_with_ref_derives_resource_without_error() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Ref, version: "1.0" }
components:
  schemas:
    FileBase:
      type: object
      required: [id]
      properties:
        id: { type: string }
    File:
      $ref: '#/components/schemas/FileBase'
paths: {}
"#,
        );

        let files = catalog.resources.get("files").expect("files resource");
        assert_eq!(files.primary_key, Some(vec!["id".to_owned()]));
        assert_eq!(
            catalog.catalog.table("files").expect("files table").columns[0].normalized_type,
            "text"
        );
    }

    #[test]
    fn polymorphic_object_variants_catalog_union_fields() {
        let catalog = parse(
            r##"
openapi: 3.0.3
info: { title: Polymorphic, version: "1.0" }
components:
  schemas:
    bank_account:
      type: object
      required: [id, object, routing_number]
      properties:
        id: { type: string }
        object: { type: string }
        routing_number: { type: string }
    card:
      type: object
      required: [id, object, brand]
      properties:
        id: { type: string }
        object: { type: string }
        brand: { type: string }
    external_account:
      anyOf:
        - $ref: "#/components/schemas/bank_account"
        - $ref: "#/components/schemas/card"
paths: {}
"##,
        );

        let external_accounts = catalog
            .resources
            .get("external_accounts")
            .expect("external account resource");
        assert_eq!(
            external_accounts
                .columns
                .iter()
                .map(|column| (column.name.as_str(), column.nullable))
                .collect::<Vec<_>>(),
            vec![
                ("brand", true),
                ("id", false),
                ("object", false),
                ("routing_number", true)
            ]
        );
        assert!(
            external_accounts
                .warnings
                .iter()
                .all(|warning| !warning.message.contains("resource has zero fields")),
            "{:?}",
            external_accounts.warnings
        );
    }

    #[test]
    fn remote_schema_refs_are_placeholder_warnings_not_startup_refusals() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Remote Ref, version: "1.0" }
components:
  schemas:
    TileSet:
      type: object
      properties:
        link:
          $ref: 'https://schemas.opengis.net/ogcapi/tiles/part1/1.0/openapi/ogcapi-tiles-1.yaml#/components/schemas/link'
        links:
          type: array
          items:
            $ref: 'https://schemas.opengis.net/ogcapi/common/part1/1.0/openapi/schemas/link.yaml'
paths: {}
"#,
        );

        let tilesets = catalog
            .resources
            .get("tilesets")
            .expect("remote-ref resource");
        let columns = tilesets
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.normalized_type.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(columns, vec![("link", "json"), ("links", "array")]);
        assert!(tilesets.warnings.iter().any(|warning| warning.message.contains(
            "Remote OpenAPI schema ref `https://schemas.opengis.net/ogcapi/tiles/part1/1.0/openapi/ogcapi-tiles-1.yaml#/components/schemas/link` is cataloged as an object placeholder"
        )));
        assert!(tilesets.warnings.iter().any(|warning| warning.message.contains(
            "Remote OpenAPI schema ref `https://schemas.opengis.net/ogcapi/common/part1/1.0/openapi/schemas/link.yaml` is cataloged as an object placeholder"
        )));
    }

    #[test]
    fn schemas_with_properties_imply_object_and_keep_optional_columns() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Implicit Object, version: "1.0" }
components:
  schemas:
    Pet:
      required: [id, name]
      properties:
        id: { type: integer, format: int64 }
        name: { type: string }
        tag: { type: string }
paths: {}
"#,
        );

        let pets = catalog.resources.get("pets").expect("pets resource");
        assert_eq!(
            pets.columns
                .iter()
                .map(|column| (column.name.as_str(), column.nullable))
                .collect::<Vec<_>>(),
            vec![("id", false), ("name", false), ("tag", true)]
        );
        assert!(
            pets.warnings
                .iter()
                .all(|warning| { !warning.message.contains("resource has zero fields") })
        );
        assert_eq!(
            catalog
                .catalog
                .table("pets")
                .expect("pets table")
                .columns
                .len(),
            3
        );
    }

    #[test]
    fn scalar_component_schemas_do_not_emit_object_zero_field_warning() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Scalar Components, version: "1.0" }
components:
  schemas:
    EmptyThing:
      type: object
    TextResponse:
      type: string
    CountResponse:
      type: integer
paths: {}
"#,
        );

        let empty = catalog
            .resources
            .get("emptythings")
            .expect("empty object resource");
        assert!(empty.warnings.iter().any(|warning| warning.message
            == "Schema has type object but no properties; resource has zero fields."));

        for resource_name in ["textresponses", "countresponses"] {
            let resource = catalog
                .resources
                .get(resource_name)
                .expect("scalar resource");
            assert!(
                resource
                    .warnings
                    .iter()
                    .all(|warning| !warning.message.contains("type object")),
                "{resource_name} warnings: {:?}",
                resource.warnings
            );
            assert!(
                resource.warnings.iter().any(|warning| {
                    warning
                        .message
                        .contains("is not materialized as REST resource fields in v0")
                }),
                "{resource_name} warnings: {:?}",
                resource.warnings
            );
        }
    }

    #[test]
    fn all_of_merges_properties_and_required_fields() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: AllOf, version: "1.0" }
components:
  schemas:
    Identity:
      type: object
      required: [id]
      properties:
        id: { type: string }
    File:
      allOf:
        - $ref: '#/components/schemas/Identity'
        - type: object
          required: [name]
          properties:
            name: { type: string }
            size: { type: integer }
paths: {}
"#,
        );

        let files = catalog.resources.get("files").expect("files resource");
        assert_eq!(files.required, vec!["id".to_owned(), "name".to_owned()]);
        assert_eq!(files.columns.len(), 3);
        assert_eq!(files.primary_key, Some(vec!["id".to_owned()]));
    }

    #[test]
    fn all_of_polymorphic_scalar_conflicts_converge_to_text() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: GitHub AllOf, version: "1.0" }
components:
  schemas:
    RepositoryTimestamp:
      type: object
      properties:
        id: { type: integer }
        created_at:
          oneOf:
            - { type: integer }
            - { type: string, format: date-time }
    WebhookFork:
      allOf:
        - $ref: '#/components/schemas/RepositoryTimestamp'
        - type: object
          properties:
            created_at: { type: string, format: date-time }
paths: {}
"#,
        );

        let webhook_forks = catalog
            .resources
            .get("webhookforks")
            .expect("webhook fork resource should catalog");
        let created_at = webhook_forks
            .columns
            .iter()
            .find(|column| column.name == "created_at")
            .expect("created_at column");
        assert_eq!(created_at.declared_type, "oneOf");
        assert_eq!(created_at.normalized_type, "text");
        assert!(
            webhook_forks
                .warnings
                .iter()
                .any(|warning| warning.message == "oneOf is treated as text in REST twin v0.")
        );
    }

    #[test]
    fn all_of_incompatible_concrete_field_types_still_refuse() {
        let error = parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
info: { title: Concrete Conflict, version: "1.0" }
components:
  schemas:
    Thing:
      allOf:
        - type: object
          properties:
            value: { type: string }
        - type: object
          properties:
            value: { type: integer }
paths: {}
"#,
            "conflict.yaml",
        )
        .expect_err("concrete allOf type conflict should refuse");
        let rendered = error.render(true).expect("render refusal");
        assert!(rendered.contains("\"code\": \"E_OPENAPI_SCHEMA_CONFLICT\""));
        assert!(rendered.contains("\"field\": \"value\""));
    }

    #[test]
    fn all_of_untyped_scalar_placeholder_converges_with_concrete_scalar() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Untyped AllOf, version: "1.0" }
components:
  schemas:
    Repository:
      allOf:
        - type: object
          properties:
            language: { type: string }
        - type: object
          properties:
            language:
              description: GitHub sometimes leaves this scalar untyped
paths: {}
"#,
        );

        let repositories = catalog
            .resources
            .get("repositorys")
            .expect("repository resource should catalog");
        let language = repositories
            .columns
            .iter()
            .find(|column| column.name == "language")
            .expect("language column");
        assert_eq!(language.declared_type, "oneOf");
        assert_eq!(language.normalized_type, "text");
    }

    #[test]
    fn all_of_integer_number_conflicts_narrow_to_integer() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Numeric AllOf, version: "1.0" }
components:
  schemas:
    ProjectCardMove:
      allOf:
        - type: object
          properties:
            after_id: { type: integer }
        - type: object
          properties:
            after_id: { type: number }
paths: {}
"#,
        );

        let cards = catalog
            .resources
            .get("projectcardmoves")
            .expect("project card move resource should catalog");
        let after_id = cards
            .columns
            .iter()
            .find(|column| column.name == "after_id")
            .expect("after_id column");
        assert_eq!(after_id.declared_type, "integer");
        assert_eq!(after_id.normalized_type, "integer");
    }

    #[test]
    fn nested_non_circular_refs_resolve_through_all_of_and_properties() {
        let catalog = parse(
            r#"
openapi: 3.1.0
info: { title: Museum, version: "1.0" }
components:
  schemas:
    Email:
      type: string
      format: email
    Date:
      type: string
      format: date
    TicketId:
      type: string
    EventId:
      type: string
    TicketType:
      type: string
    Ticket:
      type: object
      required: [ticketType, ticketDate]
      properties:
        ticketId:
          $ref: '#/components/schemas/TicketId'
        ticketDate:
          $ref: '#/components/schemas/Date'
        ticketType:
          $ref: '#/components/schemas/TicketType'
        eventId:
          $ref: '#/components/schemas/EventId'
    BuyMuseumTickets:
      type: object
      allOf:
        - type: object
          required: [email]
          properties:
            email:
              $ref: '#/components/schemas/Email'
        - $ref: '#/components/schemas/Ticket'
paths: {}
"#,
        );

        let tickets = catalog
            .resources
            .get("buymuseumticketss")
            .expect("BuyMuseumTickets resource");
        assert_eq!(
            tickets.required,
            vec![
                "email".to_owned(),
                "ticketDate".to_owned(),
                "ticketType".to_owned()
            ]
        );

        let columns = tickets
            .columns
            .iter()
            .map(|column| {
                (
                    column.name.as_str(),
                    column.declared_type.as_str(),
                    column.normalized_type.as_str(),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            columns,
            vec![
                ("email", "string", "text", false),
                ("eventId", "string", "text", true),
                ("ticketDate", "string", "date", false),
                ("ticketId", "string", "text", true),
                ("ticketType", "string", "text", false),
            ]
        );
    }

    #[test]
    fn circular_ref_refuses() {
        let error = parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
info: { title: Circular, version: "1.0" }
components:
  schemas:
    A:
      $ref: '#/components/schemas/B'
    B:
      $ref: '#/components/schemas/A'
paths: {}
"#,
            "circular.yaml",
        )
        .expect_err("circular ref should refuse");
        let rendered = error.render(true).expect("render refusal");

        assert!(rendered.contains("\"code\": \"E_OPENAPI_REF_CIRCULAR\""));
    }

    #[test]
    fn recursive_property_refs_are_bounded_instead_of_refusing_catalog() {
        let catalog = parse(
            r##"
openapi: 3.0.3
info: { title: Recursive Property, version: "1.0" }
components:
  schemas:
    File:
      type: object
      properties:
        id: { type: string }
        links:
          type: object
          properties:
            data:
              type: array
              items:
                $ref: "#/components/schemas/FileLink"
    FileLink:
      type: object
      properties:
        id: { type: string }
        file:
          anyOf:
            - type: string
            - $ref: "#/components/schemas/File"
paths:
  /file_links:
    get:
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/FileLink"
"##,
        );

        let file_link = catalog
            .resources
            .get("filelinks")
            .expect("recursive schema should still catalog");
        let file_column = file_link
            .columns
            .iter()
            .find(|column| column.name == "file")
            .expect("recursive field should be bounded as a column");
        assert_eq!(file_column.normalized_type, "text");
        assert!(
            file_link
                .warnings
                .iter()
                .any(|warning| warning.message == "anyOf is treated as text in REST twin v0.")
        );
    }

    #[test]
    fn maps_each_json_schema_type_to_kernel_normalized_type() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Types, version: "1.0" }
components:
  schemas:
    Thing:
      type: object
      required: [uuid_id]
      properties:
        string_field: { type: string }
        timestamp_field: { type: string, format: date-time }
        date_field: { type: string, format: date }
        byte_field: { type: string, format: byte }
        int_field: { type: integer, format: int32 }
        bigint_field: { type: integer, format: int64 }
        float_field: { type: number, format: double }
        numeric_field: { type: number, format: decimal }
        bool_field: { type: boolean }
        object_field: { type: object }
        array_field: { type: array, items: { type: string } }
        missing_type: { description: fallback }
        union_field:
          oneOf:
            - { type: string }
            - { type: integer }
        uuid_id: { type: string, format: uuid }
paths: {}
"#,
        );

        let things = catalog.resources.get("things").expect("things resource");
        let normalized = things
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.normalized_type.as_str()))
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(normalized["string_field"], "text");
        assert_eq!(normalized["timestamp_field"], "timestamp");
        assert_eq!(normalized["date_field"], "date");
        assert_eq!(normalized["byte_field"], "bytes");
        assert_eq!(normalized["int_field"], "integer");
        assert_eq!(normalized["bigint_field"], "bigint");
        assert_eq!(normalized["float_field"], "float");
        assert_eq!(normalized["numeric_field"], "numeric");
        assert_eq!(normalized["bool_field"], "boolean");
        assert_eq!(normalized["object_field"], "json");
        assert_eq!(normalized["array_field"], "array");
        assert_eq!(normalized["missing_type"], "text");
        assert_eq!(normalized["union_field"], "text");
        assert_eq!(things.primary_key, Some(vec!["uuid_id".to_owned()]));
        assert!(
            things
                .warnings
                .iter()
                .any(|warning| warning.message.contains("array"))
        );
    }

    #[test]
    fn spec_hash_uses_raw_spec_bytes() {
        let raw = b"openapi: 3.0.3\ninfo: { title: Hash, version: '1' }\npaths: {}\n";
        let catalog = parse_rest_catalog_bytes(raw, "hash.yaml").expect("spec should parse");
        assert_eq!(
            catalog.spec_hash,
            "sha256:6c9ac73286a1953c8583bdb37b420396a8d74d1cabb10011f6a31657a18d6f60"
        );
    }

    #[test]
    fn raw_size_limit_ignores_semantically_empty_pretty_printing() {
        let mut raw =
            b"openapi: 3.0.3\ninfo: { title: Pretty, version: '1' }\npaths: {}\n".to_vec();
        raw.extend(std::iter::repeat_n(b'\n', MAX_SEMANTIC_SPEC_BYTES + 1));

        let catalog =
            parse_rest_catalog_bytes(&raw, "pretty.yaml").expect("whitespace-heavy spec parses");
        assert_eq!(catalog.spec_hash, spec_hash(&raw));
    }

    #[test]
    fn semantic_size_limit_still_refuses_large_material_specs() {
        let raw = format!(
            "openapi: 3.0.3\ninfo:\n  title: \"{}\"\n  version: '1'\npaths: {{}}\n",
            "x".repeat(MAX_SEMANTIC_SPEC_BYTES + 1)
        );

        let error = parse_rest_catalog_bytes(raw.as_bytes(), "semantic-large.yaml")
            .expect_err("semantic payload over limit should refuse");
        let rendered = error.render(true).expect("render refusal");
        assert!(rendered.contains("\"code\": \"E_OPENAPI_SPEC_TOO_LARGE\""));
        assert!(rendered.contains("\"limit_kind\": \"semantic\""));
    }

    #[test]
    fn unsupported_openapi_version_refuses_before_catalog_build() {
        let error = parse_rest_catalog_bytes(
            b"openapi: 2.0\ninfo: { title: Old, version: '1' }\npaths: {}\n",
            "swagger.yaml",
        )
        .expect_err("swagger should refuse");
        let rendered = error.render(true).expect("render refusal");

        assert!(rendered.contains("\"code\": \"E_OPENAPI_VERSION\""));
        assert!(rendered.contains("OpenAPI 3.x"));
    }

    #[test]
    fn generated_catalog_can_execute_kernel_mutation() {
        let catalog = parse(
            r#"
openapi: 3.0.3
info: { title: Files, version: "1.0" }
components:
  schemas:
    File:
      type: object
      required: [id, name]
      properties:
        id: { type: integer }
        name: { type: string }
paths: {}
"#,
        );
        let table = catalog.catalog.table("files").expect("files table");
        let storage = TableStorage::new(table).expect("table storage should build");
        let mut backend = BaseSnapshotBackend::new([storage]).expect("backend should build");

        let result = execute_mutation(
            &catalog.catalog,
            &mut backend,
            &MutationOp {
                session_id: "rest-req-1".to_owned(),
                table: "files".to_owned(),
                kind: MutationKind::Insert,
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![vec![
                    ScalarValue::Integer(1),
                    ScalarValue::Text("foo.txt".to_owned()),
                ]],
                conflict_target: None,
                update_columns: Vec::new(),
                predicate: None,
                returning: Vec::new(),
            },
        );

        let KernelResult::Mutation(result) = result else {
            panic!("expected mutation result");
        };
        assert_eq!(result.rows_affected, 1);
        assert_eq!(
            backend
                .visible_table("files")
                .expect("visible files table")
                .row_count(),
            1
        );
    }
}
