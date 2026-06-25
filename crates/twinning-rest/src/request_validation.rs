//! OpenAPI request-body validation for REST dispatch.

use axum::http::{HeaderMap, header};
use serde_json::Value as JsonValue;

use super::{
    normalize::RestRefusal,
    routes::RouteEntry,
    spec::{RestCatalog, SchemaObject},
};

const APPLICATION_JSON: &str = "application/json";
const APPLICATION_FORM_URLENCODED: &str = "application/x-www-form-urlencoded";
const SUPPORTED_REQUEST_BODY_CONTENT_TYPES: &str =
    "application/json, application/*+json, or application/x-www-form-urlencoded";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestValidationOutcome {
    NotDeclared,
    NonJsonContentType,
    NoJsonSchema { schema: String },
    AbsentOptional { schema: String },
    Valid { schema: String },
}

impl RequestValidationOutcome {
    pub fn request_valid(&self) -> bool {
        matches!(self, Self::AbsentOptional { .. } | Self::Valid { .. })
    }

    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::NoJsonSchema { schema }
            | Self::AbsentOptional { schema }
            | Self::Valid { schema } => Some(schema),
            Self::NotDeclared | Self::NonJsonContentType => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonPathSegment {
    Field(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JsonPath {
    segments: Vec<JsonPathSegment>,
}

impl JsonPath {
    pub fn root() -> Self {
        Self::default()
    }

    fn field(&self, name: &str) -> Self {
        let mut path = self.clone();
        path.segments.push(JsonPathSegment::Field(name.to_owned()));
        path
    }

    fn index(&self, index: usize) -> Self {
        let mut path = self.clone();
        path.segments.push(JsonPathSegment::Index(index));
        path
    }

    pub fn render(&self) -> String {
        let mut rendered = String::from("$");
        for segment in &self.segments {
            match segment {
                JsonPathSegment::Field(name) => {
                    rendered.push('.');
                    rendered.push_str(name);
                }
                JsonPathSegment::Index(index) => {
                    rendered.push('[');
                    rendered.push_str(&index.to_string());
                    rendered.push(']');
                }
            }
        }
        rendered
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestValidationError {
    path: JsonPath,
    detail: String,
    expected: Option<String>,
    received: Option<String>,
    schema: String,
}

impl RequestValidationError {
    fn new(
        path: JsonPath,
        detail: impl Into<String>,
        expected: Option<String>,
        received: Option<String>,
        schema: &str,
    ) -> Self {
        Self {
            path,
            detail: detail.into(),
            expected,
            received,
            schema: schema.to_owned(),
        }
    }

    fn into_refusal(self) -> RestRefusal {
        RestRefusal::SchemaValidation {
            path: self.path.render(),
            detail: self.detail,
            expected: self.expected,
            received: self.received,
            schema: Some(self.schema),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestBodyContentType {
    Json,
    FormUrlencoded,
}

pub fn validate_json_request_body(
    catalog: &RestCatalog,
    route: &RouteEntry,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<RequestValidationOutcome, RestRefusal> {
    if !route.request_body_declared {
        return Ok(RequestValidationOutcome::NotDeclared);
    }

    let schema_identity = request_schema_identity(route);
    if body.is_empty() {
        if route.request_body_required {
            return Err(RequestValidationError::new(
                JsonPath::root(),
                "required request body is missing",
                Some(String::from("JSON request body")),
                Some(String::from("empty body")),
                &schema_identity,
            )
            .into_refusal());
        }
        return Ok(RequestValidationOutcome::AbsentOptional {
            schema: schema_identity,
        });
    }

    match request_body_content_type(headers, route)? {
        RequestBodyContentType::Json => {}
        RequestBodyContentType::FormUrlencoded => {
            return Ok(RequestValidationOutcome::NonJsonContentType);
        }
    }

    let Some(schema) = route.request_body_json_schema.as_ref() else {
        return Ok(RequestValidationOutcome::NoJsonSchema {
            schema: schema_identity,
        });
    };

    let value =
        serde_json::from_slice::<JsonValue>(body).map_err(|error| RestRefusal::InvalidJson {
            detail: error.to_string(),
        })?;

    validate_value(catalog, schema, &schema_identity, &JsonPath::root(), &value)
        .map_err(RequestValidationError::into_refusal)?;

    Ok(RequestValidationOutcome::Valid {
        schema: schema_identity,
    })
}

fn request_schema_identity(route: &RouteEntry) -> String {
    if let Some(reference) = route
        .request_body_json_schema
        .as_ref()
        .and_then(|schema| schema.reference.as_deref())
    {
        return reference.to_owned();
    }
    if route.request_body_json_schema.is_some() {
        return String::from("<inline requestBody application/json schema>");
    }
    if route.request_body_declared {
        return String::from("<no requestBody application/json schema>");
    }
    String::from("<no requestBody>")
}

fn request_body_content_type(
    headers: &HeaderMap,
    route: &RouteEntry,
) -> Result<RequestBodyContentType, RestRefusal> {
    let Some(content_type) = headers.get(header::CONTENT_TYPE) else {
        return Ok(RequestBodyContentType::Json);
    };
    let content_type = content_type
        .to_str()
        .map_err(|_| RestRefusal::UnsupportedMediaType {
            expected: expected_content_types(route),
            received: Some(String::from("<non-utf8>")),
        })?;

    match media_type_base(content_type).as_str() {
        APPLICATION_JSON => Ok(RequestBodyContentType::Json),
        APPLICATION_FORM_URLENCODED => Ok(RequestBodyContentType::FormUrlencoded),
        media_type if media_type.starts_with("application/") && media_type.ends_with("+json") => {
            Ok(RequestBodyContentType::Json)
        }
        _ => Err(RestRefusal::UnsupportedMediaType {
            expected: expected_content_types(route),
            received: Some(content_type.to_owned()),
        }),
    }
}

fn expected_content_types(route: &RouteEntry) -> String {
    if route.request_body_content_types.is_empty() {
        return SUPPORTED_REQUEST_BODY_CONTENT_TYPES.to_owned();
    }
    route.request_body_content_types.join(", ")
}

fn media_type_base(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase()
}

fn validate_value(
    catalog: &RestCatalog,
    schema: &SchemaObject,
    schema_identity: &str,
    path: &JsonPath,
    value: &JsonValue,
) -> Result<(), RequestValidationError> {
    if let Some(reference) = &schema.reference {
        let resolved = resolve_schema_ref(catalog, reference).ok_or_else(|| {
            RequestValidationError::new(
                path.clone(),
                format!("schema reference `{reference}` cannot be resolved"),
                Some(reference.clone()),
                Some(json_kind(value)),
                schema_identity,
            )
        })?;
        return validate_value(catalog, resolved, schema_identity, path, value);
    }

    if value.is_null() {
        if schema_accepts_null(catalog, schema) {
            return Ok(());
        }
        return Err(RequestValidationError::new(
            path.clone(),
            format!("value at `{}` cannot be null", path.render()),
            Some(expected_type(schema)),
            Some(String::from("null")),
            schema_identity,
        ));
    }

    for item in &schema.all_of {
        validate_value(catalog, item, schema_identity, path, value)?;
    }

    if !schema.any_of.is_empty() {
        return validate_any_variant(catalog, &schema.any_of, schema_identity, path, value);
    }

    if !schema.one_of.is_empty() {
        return validate_any_variant(catalog, &schema.one_of, schema_identity, path, value);
    }

    if !schema.enum_values.is_empty() && !schema.enum_values.iter().any(|allowed| allowed == value)
    {
        return Err(RequestValidationError::new(
            path.clone(),
            format!("value at `{}` is not in the declared enum", path.render()),
            Some(format!(
                "one of {}",
                json_array_literal(&schema.enum_values)
            )),
            Some(json_value_literal(value)),
            schema_identity,
        ));
    }

    let types = schema_types(schema);
    if types.is_empty() {
        return validate_untyped_schema(catalog, schema, schema_identity, path, value);
    }

    if types.iter().any(|kind| kind == "null") && value.is_null() {
        return Ok(());
    }

    let mut first_error = None;
    for kind in &types {
        match validate_typed_value(catalog, schema, schema_identity, path, value, kind) {
            Ok(()) => return Ok(()),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }

    Err(first_error.unwrap_or_else(|| {
        RequestValidationError::new(
            path.clone(),
            format!("value at `{}` does not match schema", path.render()),
            Some(expected_type(schema)),
            Some(json_kind(value)),
            schema_identity,
        )
    }))
}

fn validate_any_variant(
    catalog: &RestCatalog,
    variants: &[SchemaObject],
    schema_identity: &str,
    path: &JsonPath,
    value: &JsonValue,
) -> Result<(), RequestValidationError> {
    if variants
        .iter()
        .any(|variant| validate_value(catalog, variant, schema_identity, path, value).is_ok())
    {
        return Ok(());
    }

    Err(RequestValidationError::new(
        path.clone(),
        format!(
            "value at `{}` does not match any declared schema variant",
            path.render()
        ),
        Some(String::from("one declared schema variant")),
        Some(json_kind(value)),
        schema_identity,
    ))
}

fn validate_untyped_schema(
    catalog: &RestCatalog,
    schema: &SchemaObject,
    schema_identity: &str,
    path: &JsonPath,
    value: &JsonValue,
) -> Result<(), RequestValidationError> {
    if !schema.properties.is_empty() || !schema.required.is_empty() {
        return validate_object(catalog, schema, schema_identity, path, value);
    }
    if schema.items.is_some() {
        return validate_array(catalog, schema, schema_identity, path, value);
    }
    Ok(())
}

fn validate_typed_value(
    catalog: &RestCatalog,
    schema: &SchemaObject,
    schema_identity: &str,
    path: &JsonPath,
    value: &JsonValue,
    kind: &str,
) -> Result<(), RequestValidationError> {
    match kind {
        "object" => validate_object(catalog, schema, schema_identity, path, value),
        "array" => validate_array(catalog, schema, schema_identity, path, value),
        "string" if value.is_string() => Ok(()),
        "integer" if json_is_integer(value) => Ok(()),
        "number" if value.is_number() => Ok(()),
        "boolean" if value.is_boolean() => Ok(()),
        "null" if value.is_null() => Ok(()),
        _ => Err(RequestValidationError::new(
            path.clone(),
            format!(
                "value at `{}` expected {} but received {}",
                path.render(),
                kind,
                json_kind(value)
            ),
            Some(kind.to_owned()),
            Some(json_kind(value)),
            schema_identity,
        )),
    }
}

fn validate_object(
    catalog: &RestCatalog,
    schema: &SchemaObject,
    schema_identity: &str,
    path: &JsonPath,
    value: &JsonValue,
) -> Result<(), RequestValidationError> {
    let Some(object) = value.as_object() else {
        return Err(RequestValidationError::new(
            path.clone(),
            format!(
                "value at `{}` expected object but received {}",
                path.render(),
                json_kind(value)
            ),
            Some(String::from("object")),
            Some(json_kind(value)),
            schema_identity,
        ));
    };

    for required in &schema.required {
        if !object.contains_key(required) {
            return Err(RequestValidationError::new(
                path.field(required),
                format!("required field `{required}` is missing"),
                Some(String::from("present field")),
                Some(String::from("missing")),
                schema_identity,
            ));
        }
    }

    let additional_schema = additional_properties_schema(schema);
    for field in sorted_object_keys(object) {
        let field_path = path.field(field);
        if let Some(property_schema) = schema.properties.get(field) {
            validate_value(
                catalog,
                property_schema,
                schema_identity,
                &field_path,
                &object[field],
            )?;
        } else if matches!(schema.additional_properties, Some(JsonValue::Bool(false))) {
            return Err(RequestValidationError::new(
                field_path,
                format!("field `{field}` is not declared by the request schema"),
                Some(String::from("declared field")),
                Some(String::from("unknown field")),
                schema_identity,
            ));
        } else if let Some(additional_schema) = additional_schema.as_ref() {
            validate_value(
                catalog,
                additional_schema,
                schema_identity,
                &field_path,
                &object[field],
            )?;
        }
    }

    Ok(())
}

fn validate_array(
    catalog: &RestCatalog,
    schema: &SchemaObject,
    schema_identity: &str,
    path: &JsonPath,
    value: &JsonValue,
) -> Result<(), RequestValidationError> {
    let Some(items) = value.as_array() else {
        return Err(RequestValidationError::new(
            path.clone(),
            format!(
                "value at `{}` expected array but received {}",
                path.render(),
                json_kind(value)
            ),
            Some(String::from("array")),
            Some(json_kind(value)),
            schema_identity,
        ));
    };

    let Some(item_schema) = schema.items.as_deref() else {
        return Ok(());
    };

    for (index, item) in items.iter().enumerate() {
        validate_value(
            catalog,
            item_schema,
            schema_identity,
            &path.index(index),
            item,
        )?;
    }

    Ok(())
}

fn resolve_schema_ref<'a>(catalog: &'a RestCatalog, reference: &str) -> Option<&'a SchemaObject> {
    reference
        .strip_prefix("#/components/schemas/")
        .and_then(|schema_name| catalog.component_schemas.get(schema_name))
}

fn schema_accepts_null(catalog: &RestCatalog, schema: &SchemaObject) -> bool {
    if schema.nullable || schema_types(schema).iter().any(|kind| kind == "null") {
        return true;
    }
    if let Some(reference) = &schema.reference
        && let Some(resolved) = resolve_schema_ref(catalog, reference)
    {
        return schema_accepts_null(catalog, resolved);
    }
    schema
        .any_of
        .iter()
        .chain(schema.one_of.iter())
        .any(|variant| schema_accepts_null(catalog, variant))
}

fn schema_types(schema: &SchemaObject) -> Vec<String> {
    match &schema.schema_type {
        Some(JsonValue::String(kind)) => vec![kind.clone()],
        Some(JsonValue::Array(kinds)) => kinds
            .iter()
            .filter_map(JsonValue::as_str)
            .map(str::to_owned)
            .collect(),
        Some(JsonValue::Null) | None => {
            if !schema.properties.is_empty() || !schema.required.is_empty() {
                vec![String::from("object")]
            } else if schema.items.is_some() {
                vec![String::from("array")]
            } else {
                Vec::new()
            }
        }
        Some(other) => vec![other.to_string()],
    }
}

fn expected_type(schema: &SchemaObject) -> String {
    let types = schema_types(schema);
    if types.is_empty() {
        return String::from("schema-compatible value");
    }
    types.join(" or ")
}

fn additional_properties_schema(schema: &SchemaObject) -> Option<SchemaObject> {
    let JsonValue::Object(_) = schema.additional_properties.as_ref()? else {
        return None;
    };
    serde_json::from_value::<SchemaObject>(schema.additional_properties.clone()?).ok()
}

fn sorted_object_keys(object: &serde_json::Map<String, JsonValue>) -> Vec<&str> {
    let mut keys = object.keys().map(String::as_str).collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}

fn json_is_integer(value: &JsonValue) -> bool {
    value.as_i64().is_some() || value.as_u64().is_some()
}

fn json_kind(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => String::from("null"),
        JsonValue::Bool(_) => String::from("boolean"),
        JsonValue::Number(number) if number.as_i64().is_some() || number.as_u64().is_some() => {
            String::from("integer")
        }
        JsonValue::Number(_) => String::from("number"),
        JsonValue::String(_) => String::from("string"),
        JsonValue::Array(_) => String::from("array"),
        JsonValue::Object(_) => String::from("object"),
    }
}

fn json_value_literal(value: &JsonValue) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| json_kind(value))
}

fn json_array_literal(values: &[JsonValue]) -> String {
    serde_json::to_string(values).unwrap_or_else(|_| String::from("<enum>"))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};
    use serde_json::json;

    use super::{JsonPath, RequestValidationOutcome, RestRefusal, validate_json_request_body};
    use crate::{
        policy::RoutingConfig,
        routes::{Method, RouteKind, build_route_registry_with_config, match_route},
        spec::parse_rest_catalog_bytes,
    };

    const CONFORMANCE_SPEC: &str = r##"
openapi: 3.0.3
info: { title: Request validation conformance, version: "1.0" }
components:
  requestBodies:
    ThingBody:
      required: true
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/ThingRequest"
  schemas:
    ThingRequest:
      type: object
      required: [name, child, items, mode]
      additionalProperties: false
      properties:
        name: { type: string }
        mode: { type: string, enum: [fast, slow] }
        child:
          $ref: "#/components/schemas/Child"
        items:
          type: array
          items:
            $ref: "#/components/schemas/Item"
        nullable_text:
          type: string
          nullable: true
        type_array_null:
          type: [string, "null"]
        any_null:
          anyOf:
            - { type: string }
            - { type: "null" }
    Child:
      type: object
      required: [key]
      additionalProperties: false
      properties:
        key: { type: string }
    Item:
      type: object
      required: [value]
      additionalProperties: false
      properties:
        value: { type: integer }
    BulkResponse:
      type: object
      required: [created, errors, results, total]
      additionalProperties: false
      properties:
        created: { type: integer }
        errors: { type: array, items: { type: object } }
        results: { type: array, items: { type: object } }
        total: { type: integer }
paths:
  /bulkresponses:
    post:
      requestBody:
        $ref: "#/components/requestBodies/ThingBody"
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/BulkResponse"
  /inline:
    post:
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [title, nested]
              additionalProperties: false
              properties:
                title: { type: string }
                nested:
                  type: object
                  required: [flag]
                  properties:
                    flag: { type: boolean }
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/BulkResponse"
"##;

    #[test]
    fn json_path_renders_deterministically() {
        assert_eq!(JsonPath::root().render(), "$");
        assert_eq!(JsonPath::root().field("field").render(), "$.field");
        assert_eq!(
            JsonPath::root().field("edges").index(0).render(),
            "$.edges[0]"
        );
        assert_eq!(
            JsonPath::root()
                .field("edges")
                .index(0)
                .field("downstream_key")
                .render(),
            "$.edges[0].downstream_key"
        );
    }

    #[test]
    fn conformance_corpus_accepts_supported_openapi_request_subset() {
        let valid = r#"{"name":"n","mode":"fast","child":{"key":"k"},"items":[{"value":1}],"nullable_text":null,"type_array_null":null,"any_null":null}"#;
        assert_valid("/bulkresponses", valid, Some("application/json"));
        assert_valid(
            "/bulkresponses",
            valid,
            Some("application/vnd.twinning+json; charset=utf-8"),
        );

        assert_valid(
            "/inline",
            r#"{"title":"inline","nested":{"flag":true}}"#,
            Some("application/json; charset=utf-8"),
        );
    }

    #[test]
    fn conformance_corpus_rejects_supported_openapi_request_subset() {
        let cases = [
            (
                "missing required object field",
                r#"{"mode":"fast","child":{"key":"k"},"items":[{"value":1}]}"#,
                "$.name",
                "present field",
                "missing",
            ),
            (
                "nested object path",
                r#"{"name":"n","mode":"fast","child":{},"items":[{"value":1}]}"#,
                "$.child.key",
                "present field",
                "missing",
            ),
            (
                "array item object path",
                r#"{"name":"n","mode":"fast","child":{"key":"k"},"items":[{}]}"#,
                "$.items[0].value",
                "present field",
                "missing",
            ),
            (
                "enum mismatch",
                r#"{"name":"n","mode":"turbo","child":{"key":"k"},"items":[{"value":1}]}"#,
                "$.mode",
                r#"one of ["fast","slow"]"#,
                r#""turbo""#,
            ),
            (
                "unknown field",
                r#"{"name":"n","mode":"fast","child":{"key":"k"},"items":[{"value":1}],"extra":1}"#,
                "$.extra",
                "declared field",
                "unknown field",
            ),
        ];

        for (name, body, path, expected, received) in cases {
            let refusal =
                validate("/bulkresponses", body, Some("application/json")).expect_err(name);
            assert_schema_refusal(&refusal, path, expected, received);
        }

        let refusal = validate(
            "/inline",
            r#"{"title":"inline","nested":{"flag":"yes"}}"#,
            Some("application/json"),
        )
        .expect_err("inline schema should validate nested properties");
        assert_schema_refusal_with_schema(
            &refusal,
            "$.nested.flag",
            "boolean",
            "string",
            "<inline requestBody application/json schema>",
        );
    }

    #[test]
    fn conformance_corpus_maps_body_and_media_failures() {
        let empty = validate("/bulkresponses", "", Some("application/json"))
            .expect_err("required requestBody cannot be empty");
        assert_schema_refusal(&empty, "$", "JSON request body", "empty body");

        let malformed = validate("/bulkresponses", "{", Some("application/json"))
            .expect_err("malformed JSON should fail");
        assert!(matches!(malformed, RestRefusal::InvalidJson { .. }));

        let media = validate("/bulkresponses", "{}", Some("text/plain"))
            .expect_err("unsupported content type should fail before validation");
        assert!(matches!(media, RestRefusal::UnsupportedMediaType { .. }));
    }

    #[test]
    fn validation_is_independent_of_materialization_shape() {
        let catalog =
            parse_rest_catalog_bytes(CONFORMANCE_SPEC.as_bytes(), "conformance.yaml".to_owned())
                .expect("parse conformance spec");
        let registry = build_route_registry_with_config(&catalog, &RoutingConfig::default());
        let matched =
            match_route(&registry.routes, Method::Post, "/bulkresponses").expect("matched route");
        assert!(matches!(matched.entry.kind, RouteKind::Refusal { .. }));

        let body = br#"{"name":"n","mode":"fast","child":{"key":"k"},"items":[{"value":1}]}"#;
        let outcome =
            validate_json_request_body(&catalog, matched.entry, &json_headers(None), body)
                .expect("valid request should not invoke materialization");
        assert!(matches!(outcome, RequestValidationOutcome::Valid { .. }));
    }

    fn validate(
        path: &str,
        body: &str,
        content_type: Option<&str>,
    ) -> Result<RequestValidationOutcome, RestRefusal> {
        let catalog =
            parse_rest_catalog_bytes(CONFORMANCE_SPEC.as_bytes(), "conformance.yaml".to_owned())
                .expect("parse conformance spec");
        let registry = build_route_registry_with_config(&catalog, &RoutingConfig::default());
        let matched = match_route(&registry.routes, Method::Post, path).expect("matched route");
        validate_json_request_body(
            &catalog,
            matched.entry,
            &json_headers(content_type),
            body.as_bytes(),
        )
    }

    fn assert_valid(path: &str, body: &str, content_type: Option<&str>) {
        let outcome = validate(path, body, content_type).expect("valid request");
        assert!(
            matches!(outcome, RequestValidationOutcome::Valid { .. }),
            "unexpected validation outcome: {outcome:?}"
        );
    }

    fn json_headers(content_type: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(content_type) = content_type {
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_str(content_type).expect("valid content type"),
            );
        }
        headers
    }

    fn assert_schema_refusal(
        refusal: &RestRefusal,
        expected_path: &str,
        expected: &str,
        received: &str,
    ) {
        assert_schema_refusal_with_schema(
            refusal,
            expected_path,
            expected,
            received,
            "#/components/schemas/ThingRequest",
        );
    }

    fn assert_schema_refusal_with_schema(
        refusal: &RestRefusal,
        expected_path: &str,
        expected: &str,
        received: &str,
        expected_schema: &str,
    ) {
        match refusal {
            RestRefusal::SchemaValidation {
                path,
                expected: Some(actual_expected),
                received: Some(actual_received),
                schema,
                ..
            } => {
                assert_eq!(path, expected_path, "refusal={}", json!(refusal));
                assert_eq!(actual_expected, expected, "refusal={}", json!(refusal));
                assert_eq!(actual_received, received, "refusal={}", json!(refusal));
                assert_eq!(schema.as_deref(), Some(expected_schema));
            }
            other => panic!("expected schema validation refusal, got {other:?}"),
        }
    }
}
