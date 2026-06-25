//! HTTP request normalization for REST operations.

use std::collections::{BTreeMap, BTreeSet};

use axum::http::{HeaderMap, Uri, header};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value as JsonValue};

use crate::ir::{
    AggregateSpec, MutationKind, MutationOp, PredicateComparison, PredicateExpr, PredicateOperator,
    ReadOp, ReadShape, ScalarValue,
};

use super::{
    routes::{Method, PathParamSpec, QueryParamSpec, RouteEntry, RouteKind},
    spec::{ResourceColumn, ResourceSchema, RestCatalog, SchemaObject},
};

const APPLICATION_JSON: &str = "application/json";
const APPLICATION_FORM_URLENCODED: &str = "application/x-www-form-urlencoded";
const SUPPORTED_REQUEST_BODY_CONTENT_TYPES: &str =
    "application/json or application/x-www-form-urlencoded";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestBodyContentType {
    Json,
    FormUrlencoded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IrOp {
    Read(ReadOp),
    Mutation(MutationOp),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum RestRefusal {
    MissingRequiredField {
        field: String,
        resource: String,
    },
    UnknownField {
        field: String,
        resource: String,
    },
    TypeMismatch {
        field: String,
        expected: String,
        received: String,
    },
    UnsupportedShape {
        detail: String,
    },
    UndeclaredQueryParam {
        param: String,
    },
    UnsupportedMediaType {
        expected: String,
        received: Option<String>,
    },
    InvalidJson {
        detail: String,
    },
    SchemaValidation {
        path: String,
        detail: String,
        expected: Option<String>,
        received: Option<String>,
        schema: Option<String>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct NormalizeRequest<'a> {
    pub method: Method,
    pub path_params: &'a BTreeMap<String, String>,
    pub session_id: &'a str,
    pub uri: &'a Uri,
    pub headers: &'a HeaderMap,
    pub body: &'a [u8],
}

pub fn normalize_request(
    catalog: &RestCatalog,
    route: &RouteEntry,
    request: NormalizeRequest<'_>,
) -> Result<IrOp, RestRefusal> {
    if let RouteKind::Refusal { detail } = &route.kind {
        return Err(RestRefusal::UnsupportedShape {
            detail: detail.clone(),
        });
    }

    let effective_resource_name = route
        .effective_resource_name
        .as_deref()
        .unwrap_or(route.resource_name.as_str());
    let resource = resource_schema(catalog, effective_resource_name)?;
    let request_resource = request_resource_schema(catalog, route, resource)?;
    ensure_declared_path_params(route, request.path_params)?;
    ensure_declared_query_params(route, request.uri)?;

    match &route.kind {
        RouteKind::Create => normalize_insert(resource, request_resource, route, request),
        RouteKind::ReadMany => {
            normalize_read_many(resource, route, request.session_id, request.uri)
        }
        RouteKind::ReadOne => {
            normalize_read_one(resource, route, request.path_params, request.session_id)
        }
        RouteKind::Update => normalize_update(resource, request_resource, route, request),
        RouteKind::Delete => {
            normalize_delete(resource, route, request.path_params, request.session_id)
        }
        RouteKind::Refusal { .. } => unreachable!("refusal routes return before resource lookup"),
    }
}

pub fn json_to_scalar_value(_field: &str, value: &JsonValue) -> Result<ScalarValue, RestRefusal> {
    match value {
        JsonValue::Null => Ok(ScalarValue::Null),
        JsonValue::Bool(value) => Ok(ScalarValue::Boolean(*value)),
        JsonValue::Number(value) => Ok(value
            .as_i64()
            .map(ScalarValue::Integer)
            .unwrap_or_else(|| ScalarValue::Json(JsonValue::Number(value.clone())))),
        JsonValue::String(value) => Ok(ScalarValue::Text(value.clone())),
        JsonValue::Object(_) => Ok(ScalarValue::Json(value.clone())),
        JsonValue::Array(values) => values
            .iter()
            .map(|value| json_to_scalar_value(_field, value))
            .collect::<Result<Vec<_>, _>>()
            .map(ScalarValue::Array),
    }
}

fn json_to_column_scalar_value(
    column: &ResourceColumn,
    value: &JsonValue,
) -> Result<ScalarValue, RestRefusal> {
    if matches!(value, JsonValue::Null) {
        return Ok(ScalarValue::Null);
    }

    match column.normalized_type.as_str() {
        "bigint" | "integer" | "smallint" => match value {
            JsonValue::Number(number) => number
                .as_i64()
                .map(ScalarValue::Integer)
                .ok_or_else(|| type_mismatch(column, "integer", value)),
            _ => Err(type_mismatch(column, "integer", value)),
        },
        "float" | "numeric" => match value {
            JsonValue::Number(number) => Ok(ScalarValue::Text(number.to_string())),
            _ => Err(type_mismatch(column, "number", value)),
        },
        "boolean" => match value {
            JsonValue::Bool(value) => Ok(ScalarValue::Boolean(*value)),
            _ => Err(type_mismatch(column, "boolean", value)),
        },
        "timestamp" | "date" | "bytes" | "text" => match value {
            JsonValue::String(value) => Ok(ScalarValue::Text(value.clone())),
            _ => Err(type_mismatch(column, "string", value)),
        },
        "json" => Ok(ScalarValue::Json(value.clone())),
        "array" => match value {
            JsonValue::Array(values) => values
                .iter()
                .map(|value| json_to_scalar_value(&column.name, value))
                .collect::<Result<Vec<_>, _>>()
                .map(ScalarValue::Array),
            _ => Err(type_mismatch(column, "array", value)),
        },
        _ => json_to_scalar_value(&column.name, value),
    }
}

fn type_mismatch(
    column: &ResourceColumn,
    expected: impl Into<String>,
    value: &JsonValue,
) -> RestRefusal {
    RestRefusal::TypeMismatch {
        field: column.name.clone(),
        expected: expected.into(),
        received: json_kind(value),
    }
}

pub fn coerce_path_or_query_value(
    field: &str,
    normalized_type: &str,
    raw: &str,
) -> Result<ScalarValue, RestRefusal> {
    match normalized_type {
        "integer" | "bigint" | "smallint" => {
            raw.parse::<i64>()
                .map(ScalarValue::Integer)
                .map_err(|_| RestRefusal::TypeMismatch {
                    field: field.to_owned(),
                    expected: normalized_type.to_owned(),
                    received: raw.to_owned(),
                })
        }
        "float" | "numeric" => Ok(ScalarValue::Text(raw.to_owned())),
        "boolean" => match raw.to_ascii_lowercase().as_str() {
            "true" => Ok(ScalarValue::Boolean(true)),
            "false" => Ok(ScalarValue::Boolean(false)),
            _ => Err(RestRefusal::TypeMismatch {
                field: field.to_owned(),
                expected: String::from("boolean"),
                received: raw.to_owned(),
            }),
        },
        "bytes" | "json" | "array" => Err(RestRefusal::UnsupportedShape {
            detail: format!(
                "PK or filter column type {normalized_type} cannot be used as a path or query parameter"
            ),
        }),
        _ => Ok(ScalarValue::Text(raw.to_owned())),
    }
}

fn normalize_insert(
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
    route: &RouteEntry,
    request: NormalizeRequest<'_>,
) -> Result<IrOp, RestRefusal> {
    let objects =
        parse_request_body_objects(route, request_resource, request.headers, request.body)?;
    let mut columns = None;
    let mut rows = Vec::new();
    for object in &objects {
        validate_inline_request_body_schema(route, object, true)?;
        if should_validate_request_resource_required_fields(route) {
            validate_required_for(request_resource, object, &resource.resource_name)?;
        }
        validate_present_fields_for_request_resource(route, resource, request_resource, object)?;
        ensure_insert_can_materialize_required_target_fields(
            resource,
            request_resource,
            route,
            object,
        )?;
        let (mut object_columns, mut row) = mutation_fields_from_object(
            resource,
            request_resource,
            route,
            object,
            BodyMode::PresentFields,
        )?;
        append_path_param_insert_fields(
            resource,
            route,
            request.path_params,
            &mut object_columns,
            &mut row,
        )?;
        if let Some(columns) = &columns {
            if columns != &object_columns {
                return Err(RestRefusal::UnsupportedShape {
                    detail: String::from(
                        "bulk JSON request body objects must materialize the same target columns",
                    ),
                });
            }
        } else {
            columns = Some(object_columns);
        }
        rows.push(row);
    }
    let returning = mutation_returning_columns(resource, route)?;

    Ok(IrOp::Mutation(MutationOp {
        session_id: request.session_id.to_owned(),
        table: resource.resource_name.clone(),
        kind: MutationKind::Insert,
        columns: columns.unwrap_or_default(),
        rows,
        conflict_target: None,
        update_columns: Vec::new(),
        predicate: None,
        returning,
    }))
}

fn normalize_read_many(
    resource: &ResourceSchema,
    route: &RouteEntry,
    session_id: &str,
    uri: &Uri,
) -> Result<IrOp, RestRefusal> {
    let predicate = query_predicate(resource, route, uri)?;
    let limit = query_limit(uri)?;

    Ok(IrOp::Read(ReadOp {
        session_id: session_id.to_owned(),
        table: resource.resource_name.clone(),
        shape: ReadShape::FilteredScan,
        projection: projection_columns(resource, route),
        predicate,
        aggregate: AggregateSpec::default(),
        group_by: Vec::new(),
        limit,
    }))
}

fn normalize_read_one(
    resource: &ResourceSchema,
    route: &RouteEntry,
    path_params: &BTreeMap<String, String>,
    session_id: &str,
) -> Result<IrOp, RestRefusal> {
    let (shape, predicate, limit) = if path_params.is_empty() {
        (ReadShape::FilteredScan, None, Some(1))
    } else if resource.primary_key.is_some() && path_params.len() == 1 {
        let predicate = pk_predicate(resource, route, path_params)?;
        let shape = if predicate_supports_point_lookup(&predicate) {
            ReadShape::PointLookup
        } else {
            ReadShape::FilteredScan
        };
        (shape, Some(predicate), None)
    } else {
        (
            ReadShape::FilteredScan,
            Some(pk_predicate(resource, route, path_params)?),
            None,
        )
    };

    Ok(IrOp::Read(ReadOp {
        session_id: session_id.to_owned(),
        table: resource.resource_name.clone(),
        shape,
        projection: projection_columns(resource, route),
        predicate,
        aggregate: AggregateSpec::default(),
        group_by: Vec::new(),
        limit,
    }))
}

fn predicate_supports_point_lookup(predicate: &PredicateExpr) -> bool {
    match predicate {
        PredicateExpr::Comparison(comparison) => comparison_supports_point_lookup(comparison),
        PredicateExpr::Conjunction(comparisons) => {
            !comparisons.is_empty() && comparisons.iter().all(comparison_supports_point_lookup)
        }
        PredicateExpr::Disjunction(_) => false,
    }
}

fn comparison_supports_point_lookup(comparison: &PredicateComparison) -> bool {
    comparison.operator == PredicateOperator::Eq && comparison.values.len() == 1
}

fn normalize_update(
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
    route: &RouteEntry,
    request: NormalizeRequest<'_>,
) -> Result<IrOp, RestRefusal> {
    let object = parse_request_body_object(route, request_resource, request.headers, request.body)?;
    validate_inline_request_body_schema(route, &object, request.method == Method::Put)?;
    validate_present_fields_for_request_resource(route, resource, request_resource, &object)?;
    let mode = match request.method {
        Method::Patch => BodyMode::PresentFields,
        Method::Put => {
            let inline_request_schema = uses_inline_request_body_schema(route);
            if !inline_request_schema {
                validate_required_for(request_resource, &object, &resource.resource_name)?;
            }
            if inline_request_schema || uses_distinct_request_resource(resource, request_resource) {
                BodyMode::PresentFields
            } else {
                BodyMode::FullReplace
            }
        }
        other => {
            return Err(RestRefusal::UnsupportedShape {
                detail: format!("method {other} cannot normalize an update route"),
            });
        }
    };
    let (columns, row) =
        mutation_fields_from_object(resource, request_resource, route, &object, mode)?;
    let returning = mutation_returning_columns(resource, route)?;

    Ok(IrOp::Mutation(MutationOp {
        session_id: request.session_id.to_owned(),
        table: resource.resource_name.clone(),
        kind: MutationKind::Update,
        columns,
        rows: vec![row],
        conflict_target: None,
        update_columns: Vec::new(),
        predicate: Some(pk_predicate(resource, route, request.path_params)?),
        returning,
    }))
}

fn normalize_delete(
    resource: &ResourceSchema,
    route: &RouteEntry,
    path_params: &BTreeMap<String, String>,
    session_id: &str,
) -> Result<IrOp, RestRefusal> {
    let returning = if route
        .success_response
        .is_some_and(|response| response.has_body)
    {
        mutation_returning_columns(resource, route)?
    } else {
        Vec::new()
    };

    Ok(IrOp::Mutation(MutationOp {
        session_id: session_id.to_owned(),
        table: resource.resource_name.clone(),
        kind: MutationKind::Delete,
        columns: Vec::new(),
        rows: Vec::new(),
        conflict_target: None,
        update_columns: Vec::new(),
        predicate: Some(pk_predicate(resource, route, path_params)?),
        returning,
    }))
}

fn resource_schema<'a>(
    catalog: &'a RestCatalog,
    resource_name: &str,
) -> Result<&'a ResourceSchema, RestRefusal> {
    catalog
        .resources
        .get(resource_name)
        .ok_or_else(|| RestRefusal::UnsupportedShape {
            detail: format!("resource `{resource_name}` is not declared in the REST catalog"),
        })
}

fn request_resource_schema<'a>(
    catalog: &'a RestCatalog,
    route: &RouteEntry,
    fallback: &'a ResourceSchema,
) -> Result<&'a ResourceSchema, RestRefusal> {
    let Some(resource_name) = &route.request_resource_name else {
        if route.request_body_declared
            && !route.request_body_schema_present
            && matches!(route.kind, RouteKind::Create | RouteKind::Update)
        {
            return Err(RestRefusal::UnsupportedShape {
                detail: format!(
                    "request body for resource `{}` does not declare an application/json or application/x-www-form-urlencoded schema; REST twin v0 cannot validate schema-less or wildcard content bodies",
                    fallback.resource_name
                ),
            });
        }

        return Ok(fallback);
    };

    catalog
        .resources
        .get(resource_name)
        .ok_or_else(|| RestRefusal::UnsupportedShape {
            detail: format!(
                "request body schema resource `{resource_name}` is not declared in the REST catalog"
            ),
        })
}

fn parse_request_body_object(
    route: &RouteEntry,
    request_resource: &ResourceSchema,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<BTreeMap<String, JsonValue>, RestRefusal> {
    match request_body_content_type(headers)? {
        RequestBodyContentType::Json => parse_json_body(body),
        RequestBodyContentType::FormUrlencoded => {
            parse_form_urlencoded_body(route, request_resource, body)
        }
    }
}

fn parse_request_body_objects(
    route: &RouteEntry,
    request_resource: &ResourceSchema,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Vec<BTreeMap<String, JsonValue>>, RestRefusal> {
    match request_body_content_type(headers)? {
        RequestBodyContentType::Json => parse_json_body_objects(body),
        RequestBodyContentType::FormUrlencoded => {
            parse_form_urlencoded_body(route, request_resource, body).map(|object| vec![object])
        }
    }
}

fn request_body_content_type(headers: &HeaderMap) -> Result<RequestBodyContentType, RestRefusal> {
    let Some(content_type) = headers.get(header::CONTENT_TYPE) else {
        return Ok(RequestBodyContentType::Json);
    };
    let content_type = content_type
        .to_str()
        .map_err(|_| RestRefusal::UnsupportedMediaType {
            expected: SUPPORTED_REQUEST_BODY_CONTENT_TYPES.to_owned(),
            received: Some(String::from("<non-utf8>")),
        })?;

    match media_type_base(content_type).as_str() {
        APPLICATION_JSON => Ok(RequestBodyContentType::Json),
        APPLICATION_FORM_URLENCODED => Ok(RequestBodyContentType::FormUrlencoded),
        media_type if media_type.starts_with("application/") && media_type.ends_with("+json") => {
            Ok(RequestBodyContentType::Json)
        }
        _ => Err(RestRefusal::UnsupportedMediaType {
            expected: SUPPORTED_REQUEST_BODY_CONTENT_TYPES.to_owned(),
            received: Some(content_type.to_owned()),
        }),
    }
}

fn ensure_declared_query_params(route: &RouteEntry, uri: &Uri) -> Result<(), RestRefusal> {
    let declared = route
        .declared_query_params
        .iter()
        .map(|param| param.as_str())
        .collect::<BTreeSet<_>>();
    let query_specs = route
        .query_params
        .iter()
        .map(|spec| (spec.name.as_str(), spec))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();

    for (param, value) in parse_query(uri.query().unwrap_or_default()) {
        if !declared.contains(param.as_str()) {
            return Err(RestRefusal::UndeclaredQueryParam { param });
        }
        seen.insert(param.clone());
        if let Some(spec) = query_specs.get(param.as_str()) {
            validate_query_param_value(spec, &value)?;
        }
    }

    for spec in &route.query_params {
        if spec.required && !seen.contains(&spec.name) {
            return Err(RestRefusal::MissingRequiredField {
                field: spec.name.clone(),
                resource: String::from("query"),
            });
        }
    }

    Ok(())
}

fn ensure_declared_path_params(
    route: &RouteEntry,
    path_params: &BTreeMap<String, String>,
) -> Result<(), RestRefusal> {
    for spec in &route.path_param_specs {
        let Some(value) = path_params.get(&spec.name) else {
            if spec.required {
                return Err(RestRefusal::MissingRequiredField {
                    field: spec.name.clone(),
                    resource: String::from("path"),
                });
            }
            continue;
        };
        validate_path_param_value(spec, value)?;
    }

    Ok(())
}

fn validate_query_param_value(spec: &QueryParamSpec, raw: &str) -> Result<(), RestRefusal> {
    validate_declared_param_value(&spec.name, spec.schema.as_ref(), raw)
}

fn validate_path_param_value(spec: &PathParamSpec, raw: &str) -> Result<(), RestRefusal> {
    validate_declared_param_value(&spec.name, spec.schema.as_ref(), raw)
}

fn validate_declared_param_value(
    name: &str,
    schema: Option<&SchemaObject>,
    raw: &str,
) -> Result<(), RestRefusal> {
    let Some(schema) = schema else {
        return Ok(());
    };
    if !schema.enum_values.is_empty() && !query_enum_contains(&schema.enum_values, raw) {
        return Err(RestRefusal::TypeMismatch {
            field: name.to_owned(),
            expected: format!("one of {}", query_enum_values(&schema.enum_values)),
            received: raw.to_owned(),
        });
    }

    let Some(kind) = schema_type_string(schema) else {
        return Ok(());
    };

    let valid = match kind.as_str() {
        "integer" => raw.parse::<i64>().is_ok(),
        "number" => raw.parse::<f64>().is_ok(),
        "boolean" => matches!(raw.to_ascii_lowercase().as_str(), "true" | "false"),
        "string" => true,
        // Complex query serialization styles are outside the current v0 validator.
        _ => true,
    };

    if valid {
        Ok(())
    } else {
        Err(RestRefusal::TypeMismatch {
            field: name.to_owned(),
            expected: kind,
            received: raw.to_owned(),
        })
    }
}

fn query_enum_contains(values: &[JsonValue], raw: &str) -> bool {
    values.iter().any(|value| match value {
        JsonValue::String(value) => value == raw,
        JsonValue::Number(value) => value.to_string() == raw,
        JsonValue::Bool(value) => raw.eq_ignore_ascii_case(&value.to_string()),
        JsonValue::Null => raw.is_empty(),
        JsonValue::Array(_) | JsonValue::Object(_) => false,
    })
}

fn query_enum_values(values: &[JsonValue]) -> String {
    values
        .iter()
        .map(|value| match value {
            JsonValue::String(value) => value.clone(),
            JsonValue::Number(value) => value.to_string(),
            JsonValue::Bool(value) => value.to_string(),
            JsonValue::Null => String::from("null"),
            JsonValue::Array(_) | JsonValue::Object(_) => value.to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_json_body(body: &[u8]) -> Result<BTreeMap<String, JsonValue>, RestRefusal> {
    if body.is_empty() {
        return Ok(BTreeMap::new());
    }

    match serde_json::from_slice::<JsonValue>(body).map_err(|error| RestRefusal::InvalidJson {
        detail: error.to_string(),
    })? {
        JsonValue::Object(object) => Ok(object.into_iter().collect()),
        other => Err(RestRefusal::TypeMismatch {
            field: String::from("<body>"),
            expected: String::from("object"),
            received: json_kind(&other),
        }),
    }
}

fn parse_json_body_objects(body: &[u8]) -> Result<Vec<BTreeMap<String, JsonValue>>, RestRefusal> {
    if body.is_empty() {
        return Ok(vec![BTreeMap::new()]);
    }

    match serde_json::from_slice::<JsonValue>(body).map_err(|error| RestRefusal::InvalidJson {
        detail: error.to_string(),
    })? {
        JsonValue::Object(object) => Ok(vec![object.into_iter().collect()]),
        JsonValue::Array(values) => values
            .into_iter()
            .enumerate()
            .map(|(index, value)| match value {
                JsonValue::Object(object) => Ok(object.into_iter().collect()),
                other => Err(RestRefusal::TypeMismatch {
                    field: format!("<body>[{index}]"),
                    expected: String::from("object"),
                    received: json_kind(&other),
                }),
            })
            .collect(),
        other => Err(RestRefusal::TypeMismatch {
            field: String::from("<body>"),
            expected: String::from("object or array<object>"),
            received: json_kind(&other),
        }),
    }
}

fn parse_form_urlencoded_body(
    route: &RouteEntry,
    request_resource: &ResourceSchema,
    body: &[u8],
) -> Result<BTreeMap<String, JsonValue>, RestRefusal> {
    if body.is_empty() {
        return Ok(BTreeMap::new());
    }

    let raw = std::str::from_utf8(body).map_err(|error| RestRefusal::InvalidJson {
        detail: format!("form-urlencoded body is not valid UTF-8: {error}"),
    })?;
    let mut object = BTreeMap::new();

    for (field, value) in parse_query(raw) {
        let key = FormFieldKey::parse(&field);
        insert_form_field(&mut object, route, request_resource, key, value)?;
    }

    Ok(object)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FormFieldKey {
    head: String,
    tail: Vec<String>,
}

impl FormFieldKey {
    fn parse(raw: &str) -> Self {
        let Some(open) = raw.find('[') else {
            return Self {
                head: raw.to_owned(),
                tail: Vec::new(),
            };
        };

        let head = raw[..open].to_owned();
        let mut tail = Vec::new();
        let mut rest = &raw[open..];
        while let Some(stripped) = rest.strip_prefix('[') {
            let Some(close) = stripped.find(']') else {
                return Self {
                    head: raw.to_owned(),
                    tail: Vec::new(),
                };
            };
            tail.push(stripped[..close].to_owned());
            rest = &stripped[close + 1..];
        }

        if !rest.is_empty() || head.is_empty() {
            Self {
                head: raw.to_owned(),
                tail: Vec::new(),
            }
        } else {
            Self { head, tail }
        }
    }
}

fn insert_form_field(
    object: &mut BTreeMap<String, JsonValue>,
    route: &RouteEntry,
    request_resource: &ResourceSchema,
    key: FormFieldKey,
    raw_value: String,
) -> Result<(), RestRefusal> {
    if key.tail.is_empty() {
        if form_field_expects_array(route, request_resource, &key.head) {
            let value = coerce_form_array_item(route, request_resource, &key.head, &raw_value);
            insert_form_array_value(object, &key.head, value)?;
        } else {
            let value = coerce_form_scalar(route, request_resource, &key.head, &raw_value);
            insert_form_scalar(object, &key.head, value);
        }
        return Ok(());
    }

    if key.tail.len() == 1 && key.tail[0].is_empty() {
        let value = coerce_form_array_item(route, request_resource, &key.head, &raw_value);
        insert_form_array_value(object, &key.head, value)?;
        return Ok(());
    }

    let entry = object
        .entry(key.head.clone())
        .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
    let JsonValue::Object(map) = entry else {
        return Err(RestRefusal::UnsupportedShape {
            detail: format!(
                "form-urlencoded request body mixes scalar and object values for field `{}`",
                key.head
            ),
        });
    };
    insert_nested_form_value(map, &key.tail, raw_value)
}

fn insert_nested_form_value(
    map: &mut serde_json::Map<String, JsonValue>,
    path: &[String],
    raw_value: String,
) -> Result<(), RestRefusal> {
    let Some((head, tail)) = path.split_first() else {
        return Ok(());
    };

    if tail.is_empty() {
        if head.is_empty() {
            return Err(RestRefusal::UnsupportedShape {
                detail: String::from(
                    "form-urlencoded request body cannot use anonymous array items at object root",
                ),
            });
        }
        insert_json_map_scalar(map, head, JsonValue::String(raw_value));
        return Ok(());
    }

    if tail.len() == 1 && tail[0].is_empty() {
        if head.is_empty() {
            return Err(RestRefusal::UnsupportedShape {
                detail: String::from(
                    "form-urlencoded request body cannot use anonymous nested array keys",
                ),
            });
        }
        insert_json_map_array_value(map, head, JsonValue::String(raw_value))?;
        return Ok(());
    }

    if head.is_empty() {
        return Err(RestRefusal::UnsupportedShape {
            detail: String::from(
                "form-urlencoded request body nested array objects are unsupported",
            ),
        });
    }

    let entry = map
        .entry(head.clone())
        .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
    let JsonValue::Object(child) = entry else {
        return Err(RestRefusal::UnsupportedShape {
            detail: format!(
                "form-urlencoded request body mixes scalar and object values for nested field `{head}`"
            ),
        });
    };
    insert_nested_form_value(child, tail, raw_value)
}

fn insert_form_scalar(object: &mut BTreeMap<String, JsonValue>, field: &str, value: JsonValue) {
    let expects_array = object
        .get(field)
        .is_some_and(|existing| existing.is_array());
    if expects_array {
        if let Some(JsonValue::Array(values)) = object.get_mut(field) {
            values.push(value);
        }
    } else if let Some(existing) = object.insert(field.to_owned(), value) {
        let current = object.get_mut(field).expect("field was just inserted");
        *current = JsonValue::Array(vec![existing, current.take()]);
    }
}

fn insert_form_array_value(
    object: &mut BTreeMap<String, JsonValue>,
    field: &str,
    value: JsonValue,
) -> Result<(), RestRefusal> {
    match object.entry(field.to_owned()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(JsonValue::Array(vec![value]));
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => match entry.get_mut() {
            JsonValue::Array(values) => {
                values.push(value);
                Ok(())
            }
            _ => Err(RestRefusal::UnsupportedShape {
                detail: format!(
                    "form-urlencoded request body mixes scalar and array values for field `{field}`"
                ),
            }),
        },
    }
}

fn insert_json_map_scalar(
    map: &mut serde_json::Map<String, JsonValue>,
    field: &str,
    value: JsonValue,
) {
    if let Some(existing) = map.insert(field.to_owned(), value) {
        let current = map.get_mut(field).expect("field was just inserted");
        *current = JsonValue::Array(vec![existing, current.take()]);
    }
}

fn insert_json_map_array_value(
    map: &mut serde_json::Map<String, JsonValue>,
    field: &str,
    value: JsonValue,
) -> Result<(), RestRefusal> {
    match map.entry(field.to_owned()) {
        serde_json::map::Entry::Vacant(entry) => {
            entry.insert(JsonValue::Array(vec![value]));
            Ok(())
        }
        serde_json::map::Entry::Occupied(mut entry) => match entry.get_mut() {
            JsonValue::Array(values) => {
                values.push(value);
                Ok(())
            }
            _ => Err(RestRefusal::UnsupportedShape {
                detail: format!(
                    "form-urlencoded request body mixes scalar and array values for nested field `{field}`"
                ),
            }),
        },
    }
}

fn coerce_form_scalar(
    route: &RouteEntry,
    request_resource: &ResourceSchema,
    field: &str,
    raw: &str,
) -> JsonValue {
    coerce_form_scalar_with_schema(
        raw,
        inline_request_property_schema(route, field),
        resource_column_for_body_field(request_resource, field),
    )
}

fn coerce_form_array_item(
    route: &RouteEntry,
    request_resource: &ResourceSchema,
    field: &str,
    raw: &str,
) -> JsonValue {
    let item_schema =
        inline_request_property_schema(route, field).and_then(|schema| schema.items.as_deref());
    let column = resource_column_for_body_field(request_resource, field)
        .filter(|column| column.normalized_type != "array");
    coerce_form_scalar_with_schema(raw, item_schema, column)
}

fn coerce_form_scalar_with_schema(
    raw: &str,
    schema: Option<&SchemaObject>,
    column: Option<&ResourceColumn>,
) -> JsonValue {
    let kind = schema
        .and_then(schema_type_string)
        .or_else(|| column.map(resource_column_json_kind));

    match kind.as_deref() {
        Some("integer") => raw
            .parse::<i64>()
            .map(Number::from)
            .map(JsonValue::Number)
            .unwrap_or_else(|_| JsonValue::String(raw.to_owned())),
        Some("number") => raw
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(raw.to_owned())),
        Some("boolean") => match raw.to_ascii_lowercase().as_str() {
            "true" => JsonValue::Bool(true),
            "false" => JsonValue::Bool(false),
            _ => JsonValue::String(raw.to_owned()),
        },
        Some("null") if raw.is_empty() => JsonValue::Null,
        _ => JsonValue::String(raw.to_owned()),
    }
}

fn form_field_expects_array(
    route: &RouteEntry,
    request_resource: &ResourceSchema,
    field: &str,
) -> bool {
    if let Some(schema) = inline_request_property_schema(route, field) {
        return schema_type_string(schema).is_some_and(|kind| kind == "array");
    }

    resource_column_for_body_field(request_resource, field)
        .is_some_and(|column| column.normalized_type == "array")
}

fn inline_request_property_schema<'a>(
    route: &'a RouteEntry,
    field: &str,
) -> Option<&'a SchemaObject> {
    if route.request_schema_ref.is_some() {
        return None;
    }
    let schema = route.request_body_schema.as_ref()?;
    if schema.reference.is_some() {
        return None;
    }
    schema.properties.get(field).or_else(|| {
        schema
            .properties
            .iter()
            .find(|(property, _)| property.eq_ignore_ascii_case(field))
            .map(|(_, schema)| schema)
    })
}

fn resource_column_json_kind(column: &ResourceColumn) -> String {
    match column.declared_type.as_str() {
        "integer" | "number" | "boolean" | "string" | "object" | "array" | "null" => {
            column.declared_type.clone()
        }
        _ => match column.normalized_type.as_str() {
            "bigint" | "integer" | "smallint" => String::from("integer"),
            "float" | "numeric" => String::from("number"),
            "boolean" => String::from("boolean"),
            "timestamp" | "date" | "bytes" | "text" => String::from("string"),
            "json" => String::from("object"),
            "array" => String::from("array"),
            _ => String::from("string"),
        },
    }
}

fn media_type_base(media_type: &str) -> String {
    media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn validate_required_for(
    resource: &ResourceSchema,
    object: &BTreeMap<String, JsonValue>,
    refusal_resource_name: &str,
) -> Result<(), RestRefusal> {
    for field in &resource.required {
        if !body_has_resource_field(resource, object, field) {
            return Err(RestRefusal::MissingRequiredField {
                field: field.clone(),
                resource: refusal_resource_name.to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_present_fields_for_request_resource(
    route: &RouteEntry,
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
    object: &BTreeMap<String, JsonValue>,
) -> Result<(), RestRefusal> {
    for (field, value) in object {
        let Some(column) = resource_column_for_body_field(request_resource, field) else {
            continue;
        };
        match validate_json_matches_resource_column(column, value) {
            Ok(()) => {}
            Err(RestRefusal::TypeMismatch { .. })
                if request_body_field_accepts_value(
                    route,
                    resource,
                    request_resource,
                    field,
                    value,
                ) => {}
            Err(refusal) => return Err(refusal),
        }
    }

    Ok(())
}

fn validate_json_matches_resource_column(
    column: &ResourceColumn,
    value: &JsonValue,
) -> Result<(), RestRefusal> {
    if matches!(value, JsonValue::Null) {
        return Ok(());
    }

    let expected = match column.declared_type.as_str() {
        "integer" => Some(("integer", value.as_i64().is_some())),
        "number" => Some(("number", value.is_number())),
        "boolean" => Some(("boolean", value.is_boolean())),
        "string" => Some(("string", value.is_string())),
        "object" => Some(("object", value.is_object())),
        "array" => Some(("array", value.is_array())),
        "oneOf" | "anyOf" | "$ref" | "null" => None,
        _ => match column.normalized_type.as_str() {
            "bigint" | "integer" | "smallint" => Some(("integer", value.as_i64().is_some())),
            "float" | "numeric" => Some(("number", value.is_number())),
            "boolean" => Some(("boolean", value.is_boolean())),
            "timestamp" | "date" | "bytes" | "text" => Some(("string", value.is_string())),
            "json" => Some(("object", value.is_object())),
            "array" => Some(("array", value.is_array())),
            _ => None,
        },
    };

    match expected {
        Some((_, true)) | None => Ok(()),
        Some((kind, false)) => Err(type_mismatch(column, kind, value)),
    }
}

fn validate_inline_request_body_schema(
    route: &RouteEntry,
    object: &BTreeMap<String, JsonValue>,
    enforce_required: bool,
) -> Result<(), RestRefusal> {
    if route.request_schema_ref.is_some() {
        return Ok(());
    }
    let Some(schema) = route.request_body_schema.as_ref() else {
        return Ok(());
    };
    if schema.reference.is_some() {
        return Ok(());
    }

    let refusal_resource = route
        .effective_resource_name
        .as_deref()
        .unwrap_or(route.resource_name.as_str());

    if enforce_required {
        for field in &schema.required {
            if !object.contains_key(field) {
                return Err(RestRefusal::MissingRequiredField {
                    field: field.clone(),
                    resource: refusal_resource.to_owned(),
                });
            }
        }
    }

    for (field, value) in object {
        if let Some(property_schema) = schema.properties.get(field) {
            validate_json_matches_inline_schema(field, property_schema, value)?;
        } else if matches!(schema.additional_properties, Some(JsonValue::Bool(false))) {
            return Err(RestRefusal::UnknownField {
                field: field.clone(),
                resource: refusal_resource.to_owned(),
            });
        }
    }

    Ok(())
}

fn validate_json_matches_inline_schema(
    field: &str,
    schema: &SchemaObject,
    value: &JsonValue,
) -> Result<(), RestRefusal> {
    if matches!(value, JsonValue::Null) || schema.reference.is_some() {
        return Ok(());
    }

    let Some(kind) = schema_type_string(schema) else {
        return Ok(());
    };

    let valid = match kind.as_str() {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "integer" => value.as_i64().is_some(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "string" => value.is_string(),
        _ => true,
    };

    if valid {
        Ok(())
    } else {
        Err(RestRefusal::TypeMismatch {
            field: field.to_owned(),
            expected: kind,
            received: json_kind(value),
        })
    }
}

fn schema_type_string(schema: &SchemaObject) -> Option<String> {
    match &schema.schema_type {
        Some(JsonValue::String(kind)) => Some(kind.clone()),
        Some(JsonValue::Array(kinds)) => kinds
            .iter()
            .find_map(JsonValue::as_str)
            .filter(|kind| *kind != "null")
            .map(str::to_owned),
        Some(JsonValue::Null) | None if schema.items.is_some() => Some(String::from("array")),
        Some(JsonValue::Null) | None if !schema.properties.is_empty() => {
            Some(String::from("object"))
        }
        Some(JsonValue::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

fn ensure_insert_can_materialize_required_target_fields(
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
    route: &RouteEntry,
    object: &BTreeMap<String, JsonValue>,
) -> Result<(), RestRefusal> {
    if !uses_distinct_request_resource(resource, request_resource)
        && !uses_inline_request_body_schema(route)
    {
        return Ok(());
    }

    let missing = resource
        .columns
        .iter()
        .filter(|column| {
            !column.nullable && !body_has_resource_field(resource, object, &column.name)
        })
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();

    if missing.is_empty() {
        return Ok(());
    }

    let request_schema = route
        .request_schema_ref
        .as_deref()
        .unwrap_or("<inline request body schema>");
    Err(RestRefusal::UnsupportedShape {
        detail: format!(
            "request body schema `{request_schema}` omits required target column(s) [{}] for resource `{}`; REST twin v0 cannot synthesize server-generated or default values",
            missing.join(", "),
            resource.resource_name
        ),
    })
}

fn uses_distinct_request_resource(
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
) -> bool {
    resource.resource_name != request_resource.resource_name
}

fn should_validate_request_resource_required_fields(route: &RouteEntry) -> bool {
    !uses_inline_request_body_schema(route)
}

fn uses_inline_request_body_schema(route: &RouteEntry) -> bool {
    route.request_schema_ref.is_none()
        && route
            .request_body_schema
            .as_ref()
            .is_some_and(|schema| schema.reference.is_none())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyMode {
    PresentFields,
    FullReplace,
}

fn mutation_fields_from_object(
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
    route: &RouteEntry,
    object: &BTreeMap<String, JsonValue>,
    mode: BodyMode,
) -> Result<(Vec<String>, Vec<ScalarValue>), RestRefusal> {
    let known = request_resource
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();

    for field in object.keys() {
        if !known.contains(field.as_str())
            && resource_column_for_body_field(request_resource, field).is_none()
            && !request_resource.additional_properties_allowed
        {
            return Err(RestRefusal::UnknownField {
                field: field.clone(),
                resource: resource.resource_name.clone(),
            });
        }
    }

    let mut columns = Vec::new();
    let mut row = Vec::new();
    match mode {
        BodyMode::PresentFields => {
            for (field, value) in object {
                let Some(column) = resource_column_for_body_field(resource, field) else {
                    continue;
                };
                if columns.iter().any(|seen| seen == &column.name) {
                    return Err(RestRefusal::UnsupportedShape {
                        detail: format!(
                            "request body contains multiple fields matching resource column `{}`",
                            column.name
                        ),
                    });
                }
                columns.push(column.name.clone());
                row.push(json_to_materialized_column_scalar_value(
                    route,
                    resource,
                    request_resource,
                    column,
                    field,
                    value,
                )?);
            }
        }
        BodyMode::FullReplace => {
            for column in &resource.columns {
                columns.push(column.name.clone());
                let value = body_value_for_column(object, column)
                    .map(|value| {
                        json_to_materialized_column_scalar_value(
                            route,
                            resource,
                            request_resource,
                            column,
                            &column.name,
                            value,
                        )
                    })
                    .unwrap_or(Ok(ScalarValue::Null))?;
                row.push(value);
            }
        }
    }

    Ok((columns, row))
}

fn json_to_materialized_column_scalar_value(
    route: &RouteEntry,
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
    column: &ResourceColumn,
    body_field: &str,
    value: &JsonValue,
) -> Result<ScalarValue, RestRefusal> {
    match json_to_column_scalar_value(column, value) {
        Ok(value) => Ok(value),
        Err(RestRefusal::TypeMismatch { .. })
            if request_body_field_accepts_value(
                route,
                resource,
                request_resource,
                body_field,
                value,
            ) =>
        {
            Err(divergent_request_target_field_refusal(
                resource,
                request_resource,
                column,
                body_field,
            ))
        }
        Err(refusal) => Err(refusal),
    }
}

fn request_body_field_accepts_value(
    route: &RouteEntry,
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
    field: &str,
    value: &JsonValue,
) -> bool {
    if let Some(schema) = inline_request_property_schema(route, field) {
        return validate_json_matches_inline_schema(field, schema, value).is_ok();
    }

    uses_distinct_request_resource(resource, request_resource)
        && resource_column_for_body_field(request_resource, field)
            .is_some_and(|column| validate_json_matches_resource_column(column, value).is_ok())
}

fn divergent_request_target_field_refusal(
    resource: &ResourceSchema,
    request_resource: &ResourceSchema,
    column: &ResourceColumn,
    body_field: &str,
) -> RestRefusal {
    let request_schema = if uses_distinct_request_resource(resource, request_resource) {
        request_resource.resource_name.as_str()
    } else {
        "<inline request body schema>"
    };
    RestRefusal::UnsupportedShape {
        detail: format!(
            "request body field `{body_field}` is valid for request schema `{request_schema}` but cannot be materialized into target resource `{}.{}` of type `{}`; REST twin v0 cannot coerce divergent request/response field types",
            resource.resource_name, column.name, column.declared_type
        ),
    }
}

fn append_path_param_insert_fields(
    resource: &ResourceSchema,
    route: &RouteEntry,
    path_params: &BTreeMap<String, String>,
    columns: &mut Vec<String>,
    row: &mut Vec<ScalarValue>,
) -> Result<(), RestRefusal> {
    let mut materialized_columns = columns.iter().cloned().collect::<BTreeSet<_>>();
    for param_name in ordered_path_params(route, path_params) {
        let Some(column) = resource_column_for_path_param(resource, &param_name) else {
            continue;
        };
        if !materialized_columns.insert(column.name.clone()) {
            continue;
        }
        let Some(raw_value) = path_params.get(&param_name) else {
            continue;
        };
        columns.push(column.name.clone());
        row.push(coerce_path_or_query_value(
            &column.name,
            &column.normalized_type,
            raw_value,
        )?);
    }
    Ok(())
}

fn query_predicate(
    resource: &ResourceSchema,
    route: &RouteEntry,
    uri: &Uri,
) -> Result<Option<PredicateExpr>, RestRefusal> {
    let declared = route
        .declared_query_params
        .iter()
        .map(|param| param.as_str())
        .collect::<BTreeSet<_>>();
    let mut comparisons = Vec::new();

    for (param, value) in parse_query(uri.query().unwrap_or_default()) {
        if !declared.contains(param.as_str()) {
            return Err(RestRefusal::UndeclaredQueryParam { param });
        }
        if is_pagination_param(&param) {
            continue;
        }
        let Some(column) = resource_column(resource, &param) else {
            // Declared operational query params are allowed even when they are not
            // filterable resource columns in the v0 kernel.
            continue;
        };
        comparisons.push(PredicateComparison {
            column: column.name.clone(),
            operator: PredicateOperator::Eq,
            values: vec![coerce_path_or_query_value(
                &column.name,
                &column.normalized_type,
                &value,
            )?],
        });
    }

    match comparisons.as_slice() {
        [] => Ok(None),
        [comparison] => Ok(Some(PredicateExpr::Comparison(comparison.clone()))),
        _ => Ok(Some(PredicateExpr::Conjunction(comparisons))),
    }
}

fn query_limit(uri: &Uri) -> Result<Option<u64>, RestRefusal> {
    for (param, value) in parse_query(uri.query().unwrap_or_default()) {
        if param != "limit" {
            continue;
        }

        return value
            .parse::<u64>()
            .ok()
            .filter(|limit| *limit > 0)
            .map(Some)
            .ok_or_else(|| RestRefusal::TypeMismatch {
                field: String::from("limit"),
                expected: String::from("positive integer"),
                received: value,
            });
    }

    Ok(None)
}

fn pk_predicate(
    resource: &ResourceSchema,
    route: &RouteEntry,
    path_params: &BTreeMap<String, String>,
) -> Result<PredicateExpr, RestRefusal> {
    if path_params.is_empty() {
        return Err(RestRefusal::UnsupportedShape {
            detail: format!(
                "REST twin v0 requires at least one path parameter for resource `{}`",
                resource.resource_name
            ),
        });
    }

    let ordered_params = ordered_path_params(route, path_params);
    if let Some(pk_columns) = resource.primary_key.as_ref() {
        if pk_columns.len() != 1 {
            return Err(RestRefusal::UnsupportedShape {
                detail: format!(
                    "resource `{}` has a composite primary key, which REST twin v0 does not support",
                    resource.resource_name
                ),
            });
        }

        let pk_column_name = pk_columns.first().expect("checked len above");
        let pk_column = resource_column(resource, pk_column_name).ok_or_else(|| {
            RestRefusal::UnsupportedShape {
                detail: format!(
                    "primary key column `{pk_column_name}` is not declared on resource `{}`",
                    resource.resource_name
                ),
            }
        })?;
        let (lookup_param_name, lookup_column, raw_value) = path_lookup_for_pk(
            resource,
            &ordered_params,
            path_params,
            pk_column_name,
            pk_column,
        )
        .ok_or_else(|| RestRefusal::UnsupportedShape {
                detail: format!(
                    "resource `{}` has no path parameter matching primary key column `{pk_column_name}`",
                    resource.resource_name
                ),
            })?;

        let mut comparisons = vec![comparison_for_path_param_column(
            route,
            &lookup_param_name,
            lookup_column,
            &raw_value,
        )?];
        let mut used_params = BTreeSet::from([lookup_param_name]);
        let mut used_columns = BTreeSet::from([lookup_column.name.clone()]);
        comparisons.extend(scope_path_comparisons(
            resource,
            route,
            &ordered_params,
            path_params,
            &mut used_params,
            &mut used_columns,
        )?);
        return Ok(predicate_from_comparisons(comparisons));
    }

    let mut used_params = BTreeSet::new();
    let mut used_columns = BTreeSet::new();
    if ordered_params.len() > 1 {
        let terminal_param = ordered_params
            .last()
            .expect("path params is not empty because path_params is not empty");
        if resource_column_for_path_param(resource, terminal_param).is_none() {
            return Err(RestRefusal::UnsupportedShape {
                detail: format!(
                    "resource `{}` has no primary key and terminal path parameter `{terminal_param}` does not match a declared column",
                    resource.resource_name
                ),
            });
        }
    }

    let comparisons = scope_path_comparisons(
        resource,
        route,
        &ordered_params,
        path_params,
        &mut used_params,
        &mut used_columns,
    )?;
    let comparisons = if comparisons.is_empty() {
        identity_alias_path_comparison(resource, route, &ordered_params, path_params)?
            .into_iter()
            .collect()
    } else {
        comparisons
    };
    if comparisons.is_empty() {
        Err(RestRefusal::UnsupportedShape {
            detail: format!(
                "resource `{}` has no primary key for path lookup",
                resource.resource_name
            ),
        })
    } else {
        Ok(predicate_from_comparisons(comparisons))
    }
}

fn ordered_path_params(route: &RouteEntry, path_params: &BTreeMap<String, String>) -> Vec<String> {
    let mut ordered = Vec::with_capacity(path_params.len());
    for name in &route.path_params {
        if path_params.contains_key(name) && !ordered.iter().any(|seen| seen == name) {
            ordered.push(name.clone());
        }
    }
    for name in path_params.keys() {
        if !ordered.iter().any(|seen| seen == name) {
            ordered.push(name.clone());
        }
    }
    ordered
}

fn identity_alias_path_comparison(
    resource: &ResourceSchema,
    route: &RouteEntry,
    ordered_params: &[String],
    path_params: &BTreeMap<String, String>,
) -> Result<Option<PredicateComparison>, RestRefusal> {
    if ordered_params.len() != 1 {
        return Ok(None);
    }

    let param_name = &ordered_params[0];
    if !is_identity_alias_path_param(param_name) {
        return Ok(None);
    }

    let Some(column) = sole_required_scalar_column(resource) else {
        return Ok(None);
    };
    let Some(raw_value) = path_params.get(param_name) else {
        return Ok(None);
    };

    comparison_for_path_param_column(route, param_name, column, raw_value).map(Some)
}

fn is_identity_alias_path_param(path_param: &str) -> bool {
    base_path_param_column_candidates(path_param)
        .iter()
        .any(|candidate| matches!(candidate.as_str(), "pk" | "key"))
}

fn sole_required_scalar_column(resource: &ResourceSchema) -> Option<&ResourceColumn> {
    let columns = resource
        .required
        .iter()
        .filter_map(|name| resource_column(resource, name))
        .filter(|column| !matches!(column.normalized_type.as_str(), "json" | "bytes" | "array"))
        .collect::<Vec<_>>();

    match columns.as_slice() {
        [column] => Some(*column),
        _ => None,
    }
}

fn single_path_value(path_params: &BTreeMap<String, String>) -> Option<(String, String)> {
    if path_params.len() == 1 {
        path_params
            .iter()
            .next()
            .map(|(name, value)| (name.clone(), value.clone()))
    } else {
        None
    }
}

fn path_lookup_for_pk<'a>(
    resource: &'a ResourceSchema,
    ordered_params: &[String],
    path_params: &BTreeMap<String, String>,
    pk_column_name: &str,
    pk_column: &'a ResourceColumn,
) -> Option<(String, &'a ResourceColumn, String)> {
    path_value_for_column(resource, ordered_params, path_params, pk_column_name)
        .map(|(param_name, raw_value)| (param_name, pk_column, raw_value))
        .or_else(|| terminal_path_lookup(resource, ordered_params, path_params, pk_column))
        .or_else(|| {
            single_path_value(path_params)
                .map(|(param_name, raw_value)| (param_name, pk_column, raw_value))
        })
}

fn terminal_path_lookup<'a>(
    resource: &'a ResourceSchema,
    ordered_params: &[String],
    path_params: &BTreeMap<String, String>,
    pk_column: &'a ResourceColumn,
) -> Option<(String, &'a ResourceColumn, String)> {
    if ordered_params.len() <= 1 {
        return None;
    }

    let terminal_param = ordered_params.last()?;
    let raw_value = path_params.get(terminal_param)?;
    let column = resource_column_for_path_param(resource, terminal_param)
        .or_else(|| terminal_path_param_targets_identity(terminal_param).then_some(pk_column))?;
    Some((terminal_param.clone(), column, raw_value.clone()))
}

fn terminal_path_param_targets_identity(path_param: &str) -> bool {
    base_path_param_column_candidates(path_param)
        .iter()
        .any(|candidate| candidate == "id" || candidate.ends_with("_id"))
}

fn path_value_for_column(
    resource: &ResourceSchema,
    ordered_params: &[String],
    path_params: &BTreeMap<String, String>,
    column_name: &str,
) -> Option<(String, String)> {
    ordered_params.iter().find_map(|param_name| {
        let column = resource_column_for_path_param(resource, param_name)?;
        if column.name == column_name {
            path_params
                .get(param_name)
                .map(|value| (param_name.clone(), value.clone()))
        } else {
            None
        }
    })
}

fn scope_path_comparisons(
    resource: &ResourceSchema,
    route: &RouteEntry,
    ordered_params: &[String],
    path_params: &BTreeMap<String, String>,
    used_params: &mut BTreeSet<String>,
    used_columns: &mut BTreeSet<String>,
) -> Result<Vec<PredicateComparison>, RestRefusal> {
    let mut comparisons = Vec::new();
    for param_name in ordered_params {
        if used_params.contains(param_name) {
            continue;
        }
        let Some(column) = resource_column_for_path_param(resource, param_name) else {
            continue;
        };
        if used_columns.contains(&column.name) {
            continue;
        }
        let Some(raw_value) = path_params.get(param_name) else {
            continue;
        };
        comparisons.push(comparison_for_path_param_column(
            route, param_name, column, raw_value,
        )?);
        used_params.insert(param_name.clone());
        used_columns.insert(column.name.clone());
    }
    Ok(comparisons)
}

fn comparison_for_path_param_column(
    route: &RouteEntry,
    param_name: &str,
    column: &ResourceColumn,
    raw_value: &str,
) -> Result<PredicateComparison, RestRefusal> {
    match comparison_for_column(column, raw_value) {
        Ok(comparison) => Ok(comparison),
        Err(RestRefusal::TypeMismatch { .. })
            if path_param_schema_allows_text(route, param_name) && !column.nullable =>
        {
            Ok(non_null_no_match_comparison(column))
        }
        Err(refusal) => Err(refusal),
    }
}

fn non_null_no_match_comparison(column: &ResourceColumn) -> PredicateComparison {
    PredicateComparison {
        column: column.name.clone(),
        operator: PredicateOperator::IsNull,
        values: Vec::new(),
    }
}

fn comparison_for_column(
    column: &ResourceColumn,
    raw_value: &str,
) -> Result<PredicateComparison, RestRefusal> {
    Ok(PredicateComparison {
        column: column.name.clone(),
        operator: PredicateOperator::Eq,
        values: vec![coerce_path_or_query_value(
            &column.name,
            &column.normalized_type,
            raw_value,
        )?],
    })
}

fn path_param_schema_allows_text(route: &RouteEntry, param_name: &str) -> bool {
    route
        .path_param_specs
        .iter()
        .find(|spec| spec.name == param_name)
        .and_then(|spec| spec.schema.as_ref())
        .is_some_and(schema_allows_text)
}

fn schema_allows_text(schema: &SchemaObject) -> bool {
    match &schema.schema_type {
        Some(JsonValue::String(kind)) if kind == "string" => return true,
        Some(JsonValue::Array(kinds))
            if kinds.iter().any(|kind| kind.as_str() == Some("string")) =>
        {
            return true;
        }
        _ => {}
    }

    schema.one_of.iter().any(schema_allows_text) || schema.any_of.iter().any(schema_allows_text)
}

fn predicate_from_comparisons(mut comparisons: Vec<PredicateComparison>) -> PredicateExpr {
    if comparisons.len() == 1 {
        PredicateExpr::Comparison(comparisons.pop().expect("checked len above"))
    } else {
        PredicateExpr::Conjunction(comparisons)
    }
}

fn projection_columns(resource: &ResourceSchema, route: &RouteEntry) -> Vec<String> {
    if route.response_resource_name.is_some() && !route.response_fields.is_empty() {
        return route.response_fields.clone();
    }

    legacy_projection_columns(resource)
}

fn legacy_projection_columns(resource: &ResourceSchema) -> Vec<String> {
    resource
        .columns
        .iter()
        .filter(|column| legacy_projection_type(&column.normalized_type))
        .map(|column| column.name.clone())
        .collect()
}

fn mutation_returning_columns(
    resource: &ResourceSchema,
    route: &RouteEntry,
) -> Result<Vec<String>, RestRefusal> {
    if route
        .success_response
        .is_some_and(|response| !response.has_body)
    {
        return Ok(Vec::new());
    }

    if route.response_fields.is_empty() {
        if route.response_resource_name.is_some()
            && route
                .success_response
                .is_some_and(|response| response.has_body)
        {
            let response_resource = route
                .response_resource_name
                .as_deref()
                .unwrap_or("<unknown>");
            return Err(RestRefusal::UnsupportedShape {
                detail: format!(
                    "mutation response resource `{response_resource}` has no scalar REST response fields that can be returned from target resource `{}`",
                    resource.resource_name
                ),
            });
        }
        return Ok(Vec::new());
    }

    let target_columns = result_scalar_columns(resource)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let missing = route
        .response_fields
        .iter()
        .filter(|field| !target_columns.contains(*field))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let response_resource = route
            .response_resource_name
            .as_deref()
            .unwrap_or("<unknown>");
        return Err(RestRefusal::UnsupportedShape {
            detail: format!(
                "mutation response resource `{response_resource}` requires response field(s) [{}] that are not produced by target resource `{}`; REST twin v0 cannot synthesize response bodies across distinct mutation resources",
                missing.join(", "),
                resource.resource_name
            ),
        });
    }

    Ok(route.response_fields.clone())
}

fn result_scalar_columns(resource: &ResourceSchema) -> Vec<String> {
    resource
        .columns
        .iter()
        .filter(|column| result_scalar_type(&column.normalized_type))
        .map(|column| column.name.clone())
        .collect()
}

fn result_scalar_type(normalized_type: &str) -> bool {
    !matches!(normalized_type, "bytes")
}

fn legacy_projection_type(normalized_type: &str) -> bool {
    !matches!(normalized_type, "json" | "bytes" | "array")
}

fn resource_column<'a>(resource: &'a ResourceSchema, name: &str) -> Option<&'a ResourceColumn> {
    resource.columns.iter().find(|column| column.name == name)
}

fn body_has_resource_field(
    resource: &ResourceSchema,
    object: &BTreeMap<String, JsonValue>,
    field: &str,
) -> bool {
    if object.contains_key(field) {
        return true;
    }

    let Some(column) = resource_column_for_body_field(resource, field) else {
        return false;
    };

    body_value_for_column(object, column).is_some()
}

fn body_value_for_column<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    column: &ResourceColumn,
) -> Option<&'a JsonValue> {
    object.get(&column.name).or_else(|| {
        object
            .iter()
            .find(|(field, _)| column.name.eq_ignore_ascii_case(field))
            .map(|(_, value)| value)
    })
}

fn resource_column_for_body_field<'a>(
    resource: &'a ResourceSchema,
    field: &str,
) -> Option<&'a ResourceColumn> {
    resource_column(resource, field).or_else(|| {
        let mut matches = resource
            .columns
            .iter()
            .filter(|column| column.name.eq_ignore_ascii_case(field));
        let column = matches.next()?;
        if matches.next().is_none() {
            Some(column)
        } else {
            None
        }
    })
}

fn resource_column_for_path_param<'a>(
    resource: &'a ResourceSchema,
    path_param: &str,
) -> Option<&'a ResourceColumn> {
    let candidates = path_param_column_candidates(resource, path_param);
    resource.columns.iter().find(|column| {
        candidates.iter().any(|candidate| {
            column.name == *candidate || column.name.eq_ignore_ascii_case(candidate)
        })
    })
}

fn path_param_column_candidates(resource: &ResourceSchema, path_param: &str) -> Vec<String> {
    let mut candidates = base_path_param_column_candidates(path_param);
    let snake_case = camel_or_pascal_to_snake_case(path_param);
    push_resource_prefix_stripped_candidates(&mut candidates, resource, &snake_case);
    candidates
}

fn base_path_param_column_candidates(path_param: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    push_candidate(&mut candidates, path_param.to_owned());
    push_candidate(&mut candidates, path_param.to_ascii_lowercase());
    push_candidate(&mut candidates, camel_or_pascal_to_snake_case(path_param));
    candidates
}

fn push_resource_prefix_stripped_candidates(
    candidates: &mut Vec<String>,
    resource: &ResourceSchema,
    snake_case_path_param: &str,
) {
    for prefix in resource_path_param_prefixes(resource) {
        if let Some(stripped) = snake_case_path_param
            .strip_prefix(&format!("{prefix}_"))
            .filter(|stripped| !stripped.is_empty())
        {
            push_candidate(candidates, stripped.to_owned());
        }
    }
}

fn resource_path_param_prefixes(resource: &ResourceSchema) -> Vec<String> {
    let mut prefixes = Vec::new();
    push_candidate(
        &mut prefixes,
        camel_or_pascal_to_snake_case(&resource.schema_name),
    );
    push_candidate(
        &mut prefixes,
        singular_resource_name(&resource.resource_name),
    );
    prefixes
}

fn singular_resource_name(resource_name: &str) -> String {
    resource_name
        .strip_suffix("ies")
        .map(|prefix| format!("{prefix}y"))
        .or_else(|| resource_name.strip_suffix('s').map(str::to_owned))
        .unwrap_or_else(|| resource_name.to_owned())
}

fn push_candidate(candidates: &mut Vec<String>, candidate: String) {
    if !candidate.is_empty() && !candidates.iter().any(|seen| seen == &candidate) {
        candidates.push(candidate);
    }
}

fn camel_or_pascal_to_snake_case(value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(value.len());

    for (index, ch) in chars.iter().copied().enumerate() {
        if !ch.is_ascii_alphanumeric() {
            if !output.ends_with('_') && !output.is_empty() {
                output.push('_');
            }
            continue;
        }

        if ch.is_ascii_uppercase() {
            let previous = index.checked_sub(1).and_then(|idx| chars.get(idx)).copied();
            let next = chars.get(index + 1).copied();
            let needs_separator = previous.is_some_and(|prev| {
                prev.is_ascii_lowercase()
                    || prev.is_ascii_digit()
                    || (prev.is_ascii_uppercase()
                        && next.is_some_and(|next| next.is_ascii_lowercase()))
            });
            if needs_separator && !output.ends_with('_') && !output.is_empty() {
                output.push('_');
            }
            output.push(ch.to_ascii_lowercase());
        } else {
            output.push(ch.to_ascii_lowercase());
        }
    }

    output.trim_matches('_').to_owned()
}

fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (decode_form_component(key), decode_form_component(value))
        })
        .collect()
}

fn decode_form_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = hex_digit(bytes[index + 1]);
                let low = hex_digit(bytes[index + 2]);
                if let (Some(high), Some(low)) = (high, low) {
                    decoded.push((high << 4) | low);
                    index += 3;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn is_pagination_param(name: &str) -> bool {
    matches!(name, "limit" | "offset" | "page" | "per_page" | "cursor")
}

fn json_kind(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => String::from("null"),
        JsonValue::Bool(_) => String::from("boolean"),
        JsonValue::Number(_) => String::from("number"),
        JsonValue::String(_) => String::from("string"),
        JsonValue::Array(_) => String::from("array"),
        JsonValue::Object(_) => String::from("object"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::http::{HeaderMap, Uri};
    use serde_json::json;

    use crate::ir::{MutationKind, PredicateExpr, PredicateOperator, ReadShape, ScalarValue};

    use super::super::{
        routes::{Method, PathPattern, RouteEntry, RouteKind},
        spec::{SchemaObject, parse_rest_catalog_bytes},
    };
    use super::*;

    fn rest_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    File:
      type: object
      required: [id, name]
      additionalProperties: false
      properties:
        id: { type: integer }
        name: { type: string }
        ratio: { type: number }
        metadata: { type: object }
        blob: { type: string, format: binary }
paths:
  /files:
    get:
      parameters:
        - name: name
          in: query
          schema: { type: string }
        - name: limit
          in: query
          schema: { type: integer }
        - name: offset
          in: query
          schema: { type: integer }
      responses: {}
    post:
      responses: {}
  /files/{id}:
    get:
      responses: {}
    put:
      responses: {}
    patch:
      responses: {}
    delete:
      responses: {}
"#,
            "test.yaml",
        )
        .expect("catalog")
    }

    fn todo_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br##"
openapi: 3.0.3
components:
  schemas:
    Todo:
      type: object
      required: [id, title, completed]
      properties:
        id: { type: string }
        title: { type: string }
        description: { type: string }
        completed: { type: boolean }
    NewTodo:
      type: object
      required: [title]
      properties:
        title: { type: string }
        description: { type: string }
        external_id: { type: string }
    UpdateTodo:
      type: object
      properties:
        title: { type: string }
        description: { type: string }
        completed: { type: boolean }
paths:
  /todos:
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/NewTodo"
      responses: {}
  /todos/{id}:
    put:
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/UpdateTodo"
      responses: {}
"##,
            "todo.yaml",
        )
        .expect("catalog")
    }

    fn fax_media_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    FaxMedia:
      type: object
      properties:
        fax_sid: { type: string }
        sid: { type: string }
        content_type: { type: string }
paths: {}
"#,
            "fax-media.yaml",
        )
        .expect("catalog")
    }

    fn authentiq_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    AuthentiqID:
      type: object
      required: [sub]
      properties:
        devtoken: { type: string }
        sub: { type: string }
paths: {}
"#,
            "authentiq.yaml",
        )
        .expect("catalog")
    }

    fn queue_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    Queue:
      type: object
      required: [name]
      properties:
        name: { type: string }
paths: {}
"#,
            "queue.yaml",
        )
        .expect("catalog")
    }

    fn folder_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    Folder:
      required: [name]
      properties:
        Name: { type: string }
        FileCount: { type: integer }
        Id: { type: string, format: uuid }
        IsInbox: { type: boolean }
paths: {}
"#,
            "xero-folder.yaml",
        )
        .expect("catalog")
    }

    fn file_object_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    FileObject:
      type: object
      required: [id, name]
      properties:
        Id: { type: string, format: uuid }
        Name: { type: string }
        FolderId: { type: string, format: uuid }
paths: {}
"#,
            "xero-file-object.yaml",
        )
        .expect("catalog")
    }

    fn github_issue_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    Issue:
      type: object
      properties:
        id: { type: integer }
        owner: { type: string }
        repo: { type: string }
        number: { type: integer }
        title: { type: string }
paths: {}
"#,
            "github-issue.yaml",
        )
        .expect("catalog")
    }

    fn deploy_key_catalog() -> RestCatalog {
        parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    deploy-key:
      type: object
      properties:
        id: { type: integer }
        title: { type: string }
paths: {}
"#,
            "deploy-key.yaml",
        )
        .expect("catalog")
    }

    fn route(kind: RouteKind) -> RouteEntry {
        RouteEntry {
            kind,
            resource_name: String::from("files"),
            path_params: vec![String::from("id")],
            path_param_specs: Vec::new(),
            declared_query_params: vec![
                String::from("name"),
                String::from("limit"),
                String::from("offset"),
            ],
            query_params: Vec::new(),
            required_auth_schemes: Vec::new(),
            request_body_declared: false,
            request_body_required: false,
            request_body_content_types: Vec::new(),
            request_body_schema_present: false,
            request_body_schema: None,
            request_body_json_schema: None,
            request_schema_ref: None,
            request_resource_name: None,
            response_schema_ref: None,
            response_resource_name: None,
            matched_policy: None,
            effective_resource_name: None,
            routing_evidence: None,
            confidence: None,
            conflict: None,
            success_response: None,
            response_wrapper: None,
            response_fields: vec![String::from("id"), String::from("name")],
            pagination: None,
        }
    }

    fn fax_media_route(kind: RouteKind) -> RouteEntry {
        let mut route = route(kind);
        route.resource_name = String::from("faxmedias");
        route.effective_resource_name = Some(String::from("faxmedias"));
        route.path_params = vec![String::from("FaxSid"), String::from("Sid")];
        route.response_fields = vec![
            String::from("content_type"),
            String::from("fax_sid"),
            String::from("sid"),
        ];
        route
    }

    fn github_issue_route(kind: RouteKind) -> RouteEntry {
        let mut route = route(kind);
        route.resource_name = String::from("issues");
        route.effective_resource_name = Some(String::from("issues"));
        route.path_params = vec![
            String::from("owner"),
            String::from("repo"),
            String::from("issue_number"),
        ];
        route.response_fields = vec![
            String::from("id"),
            String::from("number"),
            String::from("owner"),
            String::from("repo"),
            String::from("title"),
        ];
        route
    }

    fn deploy_key_route(kind: RouteKind) -> RouteEntry {
        let mut route = route(kind);
        route.resource_name = String::from("deploy-keys");
        route.effective_resource_name = Some(String::from("deploy-keys"));
        route.path_params = vec![
            String::from("owner"),
            String::from("repo"),
            String::from("key_id"),
        ];
        route.response_fields = vec![String::from("id"), String::from("title")];
        route
    }

    fn slug_child_route(kind: RouteKind) -> RouteEntry {
        let mut route = deploy_key_route(kind);
        route.path_params = vec![
            String::from("owner"),
            String::from("repo"),
            String::from("slug"),
        ];
        route
    }

    fn authentiq_route(kind: RouteKind) -> RouteEntry {
        let mut route = route(kind);
        route.resource_name = String::from("authentiqids");
        route.effective_resource_name = Some(String::from("authentiqids"));
        route.path_params = vec![String::from("PK")];
        route.response_fields = vec![String::from("devtoken"), String::from("sub")];
        route
    }

    fn queue_route(kind: RouteKind) -> RouteEntry {
        let mut route = route(kind);
        route.resource_name = String::from("queues");
        route.effective_resource_name = Some(String::from("queues"));
        route.path_params = vec![String::from("queueName")];
        route.response_fields = vec![String::from("name")];
        route
    }

    fn folder_route(kind: RouteKind) -> RouteEntry {
        let mut route = route(kind);
        route.resource_name = String::from("folders");
        route.effective_resource_name = Some(String::from("folders"));
        route.path_params = vec![String::from("FolderId")];
        route.response_fields = vec![
            String::from("FileCount"),
            String::from("Id"),
            String::from("IsInbox"),
            String::from("Name"),
        ];
        route
    }

    fn file_object_route(kind: RouteKind) -> RouteEntry {
        let mut route = route(kind);
        route.resource_name = String::from("fileobjects");
        route.effective_resource_name = Some(String::from("fileobjects"));
        route.path_params = vec![String::from("FileId")];
        route.response_fields = vec![
            String::from("FolderId"),
            String::from("Id"),
            String::from("Name"),
        ];
        route
    }

    fn todo_route(kind: RouteKind, schema_ref: &str, request_resource_name: &str) -> RouteEntry {
        RouteEntry {
            kind,
            resource_name: String::from("todos"),
            path_params: vec![String::from("id")],
            path_param_specs: Vec::new(),
            declared_query_params: Vec::new(),
            query_params: Vec::new(),
            required_auth_schemes: Vec::new(),
            request_body_declared: true,
            request_body_required: true,
            request_body_content_types: vec![String::from("application/json")],
            request_body_schema_present: true,
            request_body_schema: None,
            request_body_json_schema: None,
            request_schema_ref: Some(schema_ref.to_owned()),
            request_resource_name: Some(request_resource_name.to_owned()),
            response_schema_ref: Some(String::from("#/components/schemas/Todo")),
            response_resource_name: Some(String::from("todos")),
            matched_policy: None,
            effective_resource_name: None,
            routing_evidence: None,
            confidence: None,
            conflict: None,
            success_response: None,
            response_wrapper: None,
            response_fields: vec![
                String::from("completed"),
                String::from("description"),
                String::from("id"),
                String::from("title"),
            ],
            pagination: None,
        }
    }

    fn inline_schema(raw: &str) -> SchemaObject {
        serde_yaml::from_str(raw).expect("inline schema")
    }

    fn captures(id: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(String::from("id"), id.to_owned())])
    }

    fn fax_media_captures(fax_sid: &str, sid: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (String::from("FaxSid"), fax_sid.to_owned()),
            (String::from("Sid"), sid.to_owned()),
        ])
    }

    fn github_issue_captures(issue_number: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (String::from("owner"), String::from("octocat")),
            (String::from("repo"), String::from("Hello-World")),
            (String::from("issue_number"), issue_number.to_owned()),
        ])
    }

    fn deploy_key_captures(key_id: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (String::from("owner"), String::from("octocat")),
            (String::from("repo"), String::from("Hello-World")),
            (String::from("key_id"), key_id.to_owned()),
        ])
    }

    fn slug_child_captures(slug: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (String::from("owner"), String::from("octocat")),
            (String::from("repo"), String::from("Hello-World")),
            (String::from("slug"), slug.to_owned()),
        ])
    }

    fn pk_capture(value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(String::from("PK"), value.to_owned())])
    }

    fn uri(value: &str) -> Uri {
        value.parse().expect("uri")
    }

    fn form_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded"
                .parse()
                .expect("header value"),
        );
        headers
    }

    #[test]
    fn post_valid_body_produces_insert_mutation() {
        let catalog = rest_catalog();
        let op = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-1",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": "alpha", "ratio": 1.25 }"#,
            },
        )
        .expect("normalizes");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.session_id, "rest-1");
        assert_eq!(mutation.table, "files");
        assert_eq!(mutation.kind, MutationKind::Insert);
        assert_eq!(mutation.columns, vec!["id", "name", "ratio"]);
        assert_eq!(
            mutation.rows,
            vec![vec![
                ScalarValue::Integer(1),
                ScalarValue::Text(String::from("alpha")),
                ScalarValue::Text(String::from("1.25")),
            ]]
        );
        assert!(mutation.update_columns.is_empty());
        assert_eq!(mutation.returning, vec!["id", "name"]);
    }

    #[test]
    fn mutation_returning_uses_declared_response_fields() {
        let catalog = rest_catalog();
        let mut create = route(RouteKind::Create);
        create.response_fields = vec![String::from("id")];

        let op = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-response-fields",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": "alpha", "ratio": 1.25 }"#,
            },
        )
        .expect("normalizes");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.returning, vec!["id"]);
    }

    #[test]
    fn mutation_response_fields_must_come_from_target_resource() {
        let catalog = rest_catalog();
        let mut create = route(RouteKind::Create);
        create.response_resource_name = Some(String::from("relayresponses"));
        create.response_fields = vec![String::from("status")];

        let refusal = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-response-mismatch",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": "alpha" }"#,
            },
        )
        .expect_err("response fields outside target resource should refuse before encoding");

        assert!(matches!(
            refusal,
            RestRefusal::UnsupportedShape { ref detail }
                if detail.contains("relayresponses")
                    && detail.contains("status")
                    && detail.contains("files")
                    && detail.contains("cannot synthesize response bodies")
        ));
    }

    #[test]
    fn mutation_response_with_no_scalar_fields_refuses_before_encoding() {
        let catalog = rest_catalog();
        let mut create = route(RouteKind::Create);
        create.response_resource_name = Some(String::from("tools"));
        create.response_fields = Vec::new();
        create.success_response = Some(crate::protocol::rest::routes::SuccessResponse {
            status: 200,
            has_body: true,
        });

        let refusal = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-empty-response-fields",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": "alpha" }"#,
            },
        )
        .expect_err("empty declared response fields should refuse before encoding");

        assert!(matches!(
            refusal,
            RestRefusal::UnsupportedShape { ref detail }
                if detail.contains("tools")
                    && detail.contains("no scalar REST response fields")
                    && detail.contains("files")
        ));
    }

    #[test]
    fn post_form_urlencoded_body_produces_insert_mutation() {
        let catalog = rest_catalog();
        let headers = form_headers();
        let op = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-form",
                uri: &uri("/files"),
                headers: &headers,
                body: b"id=7&name=hello+world&ratio=1.25",
            },
        )
        .expect("normalizes form body");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.session_id, "rest-form");
        assert_eq!(mutation.table, "files");
        assert_eq!(mutation.kind, MutationKind::Insert);
        assert_eq!(mutation.columns, vec!["id", "name", "ratio"]);
        assert_eq!(
            mutation.rows,
            vec![vec![
                ScalarValue::Integer(7),
                ScalarValue::Text(String::from("hello world")),
                ScalarValue::Text(String::from("1.25")),
            ]]
        );
    }

    #[test]
    fn post_form_urlencoded_body_validates_typed_fields() {
        let catalog = rest_catalog();
        let headers = form_headers();
        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-form-type",
                uri: &uri("/files"),
                headers: &headers,
                body: b"id=not-an-integer&name=alpha",
            },
        )
        .expect_err("invalid form integer should be rejected");

        assert_eq!(
            refusal,
            RestRefusal::TypeMismatch {
                field: String::from("id"),
                expected: String::from("integer"),
                received: String::from("string"),
            }
        );
    }

    #[test]
    fn spec_valid_inline_request_field_type_conflict_is_unsupported_shape() {
        let catalog = rest_catalog();
        let headers = form_headers();
        let mut create = route(RouteKind::Create);
        create.request_body_declared = true;
        create.request_body_schema_present = true;
        create.request_body_schema = Some(inline_schema(
            r#"
type: object
required: [id, name, ratio]
properties:
  id: { type: integer }
  name: { type: string }
  ratio: { type: string }
"#,
        ));

        let refusal = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-form-request-target-conflict",
                uri: &uri("/files"),
                headers: &headers,
                body: b"id=7&name=alpha&ratio=tomorrow",
            },
        )
        .expect_err("request-valid but target-incompatible field should be unsupported");

        match refusal {
            RestRefusal::UnsupportedShape { detail } => {
                assert!(detail.contains("request body field `ratio`"));
                assert!(detail.contains("target resource `files.ratio`"));
                assert!(detail.contains("divergent request/response field types"));
            }
            other => panic!("expected unsupported shape, got {other:?}"),
        }
    }

    #[test]
    fn inline_form_schema_string_beats_target_array_shape() {
        let catalog = parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    UserGroup:
      type: object
      required: [id, users]
      properties:
        id: { type: string }
        users:
          type: array
          items: { type: string }
paths: {}
"#,
            "usergroups.yaml",
        )
        .expect("catalog");
        let headers = form_headers();
        let mut create = route(RouteKind::Create);
        create.resource_name = String::from("usergroups");
        create.effective_resource_name = Some(String::from("usergroups"));
        create.request_body_declared = true;
        create.request_body_schema_present = true;
        create.request_body_schema = Some(inline_schema(
            r#"
type: object
required: [id, users]
properties:
  id: { type: string }
  users: { type: string }
"#,
        ));

        let refusal = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-form-inline-string-target-array",
                uri: &uri("/usergroups"),
                headers: &headers,
                body: b"id=S123&users=U123,U456",
            },
        )
        .expect_err("target array materialization should be an unsupported shape");

        match refusal {
            RestRefusal::UnsupportedShape { detail } => {
                assert!(detail.contains("target resource `usergroups.users`"));
                assert!(detail.contains("divergent request/response field types"));
            }
            other => panic!("expected unsupported shape, got {other:?}"),
        }
    }

    #[test]
    fn inline_request_schema_missing_target_fields_gets_precise_unsupported_refusal() {
        let catalog = parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    account_link:
      type: object
      required: [created, object, url]
      properties:
        created: { type: integer }
        object: { type: string }
        url: { type: string }
paths: {}
"#,
            "stripe-account-link.yaml",
        )
        .expect("catalog");
        let inline_schema: SchemaObject = serde_yaml::from_str(
            r#"
type: object
required: [account, type]
properties:
  account: { type: string }
  type: { type: string }
"#,
        )
        .expect("inline schema");
        let mut create = route(RouteKind::Create);
        create.resource_name = String::from("account_links");
        create.effective_resource_name = Some(String::from("account_links"));
        create.request_body_declared = true;
        create.request_body_schema_present = true;
        create.request_body_schema = Some(inline_schema);
        create.response_fields = vec![
            String::from("created"),
            String::from("object"),
            String::from("url"),
        ];

        let headers = form_headers();
        let refusal = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-account-link",
                uri: &uri("/v1/account_links"),
                headers: &headers,
                body: b"type=account_onboarding&account=acct_test",
            },
        )
        .expect_err("response-only target fields need an explicit unsupported refusal");

        assert!(matches!(
            refusal,
            RestRefusal::UnsupportedShape { ref detail }
                if detail.contains("<inline request body schema>")
                    && detail.contains("created")
                    && detail.contains("object")
                    && detail.contains("url")
                    && detail.contains("cannot synthesize server-generated or default values")
        ));
    }

    #[test]
    fn case_mismatched_required_property_materializes_canonical_field() {
        let catalog = folder_catalog();

        for (session_id, body) in [
            ("rest-folder-pascal", br#"{ "Name": "My Docs" }"# as &[u8]),
            ("rest-folder-lower", br#"{ "name": "My Docs" }"# as &[u8]),
        ] {
            let op = normalize_request(
                &catalog,
                &folder_route(RouteKind::Create),
                NormalizeRequest {
                    method: Method::Post,
                    path_params: &BTreeMap::new(),
                    session_id,
                    uri: &uri("/Folders"),
                    headers: &HeaderMap::new(),
                    body,
                },
            )
            .expect("case-only required mismatch should normalize to declared property");

            let IrOp::Mutation(mutation) = op else {
                panic!("expected mutation");
            };
            assert_eq!(mutation.kind, MutationKind::Insert);
            assert_eq!(mutation.table, "folders");
            assert_eq!(mutation.columns, vec![String::from("Name")]);
            assert_eq!(
                mutation.rows,
                vec![vec![ScalarValue::Text(String::from("My Docs"))]]
            );
        }
    }

    #[test]
    fn child_create_materializes_matching_path_params_into_insert_row() {
        let catalog = fax_media_catalog();
        let captures = fax_media_captures("FX111", "ME222");
        let op = normalize_request(
            &catalog,
            &fax_media_route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &captures,
                session_id: "rest-child-create",
                uri: &uri("/v1/Faxes/FX111/Media"),
                headers: &HeaderMap::new(),
                body: br#"{ "content_type": "image/png", "sid": "ME222" }"#,
            },
        )
        .expect("child create should materialize parent path fields");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Insert);
        assert_eq!(mutation.table, "faxmedias");
        assert_eq!(mutation.columns, vec!["content_type", "sid", "fax_sid"]);
        assert_eq!(
            mutation.rows,
            vec![vec![
                ScalarValue::Text(String::from("image/png")),
                ScalarValue::Text(String::from("ME222")),
                ScalarValue::Text(String::from("FX111")),
            ]]
        );
    }

    #[test]
    fn post_validates_request_schema_before_target_resource_insert_shape() {
        let catalog = todo_catalog();
        let create = todo_route(
            RouteKind::Create,
            "#/components/schemas/NewTodo",
            "newtodos",
        );

        let missing_title = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-newtodo-missing",
                uri: &uri("/todos"),
                headers: &HeaderMap::new(),
                body: br#"{ "description": "2% milk" }"#,
            },
        )
        .expect_err("request schema should require title");
        assert_eq!(
            missing_title,
            RestRefusal::MissingRequiredField {
                field: String::from("title"),
                resource: String::from("todos")
            }
        );

        let request_only_type_mismatch = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-newtodo-request-only-type",
                uri: &uri("/todos"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": "todo-1", "title": "Buy milk", "completed": false, "external_id": { "bad": ["type"] } }"#,
            },
        )
        .expect_err("request-only fields should still be type checked");
        assert_eq!(
            request_only_type_mismatch,
            RestRefusal::TypeMismatch {
                field: String::from("external_id"),
                expected: String::from("string"),
                received: String::from("object"),
            }
        );

        let unsupported_defaults = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-newtodo-defaults",
                uri: &uri("/todos"),
                headers: &HeaderMap::new(),
                body: br#"{ "title": "Buy milk", "description": "2% milk" }"#,
            },
        )
        .expect_err("missing required target fields should be explicit");
        assert!(matches!(
            unsupported_defaults,
            RestRefusal::UnsupportedShape { ref detail }
                if detail.contains("cannot synthesize")
                    && detail.contains("id")
                    && detail.contains("completed")
        ));

        let op = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-newtodo-full",
                uri: &uri("/todos"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": "todo-1", "title": "Buy milk", "completed": false }"#,
            },
        )
        .expect(
            "extra target fields are allowed when request schema permits additional properties",
        );
        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.table, "todos");
        assert_eq!(
            mutation.columns,
            vec![
                String::from("completed"),
                String::from("id"),
                String::from("title")
            ]
        );
    }

    #[test]
    fn schemaless_declared_request_body_refuses_before_response_resource_validation() {
        let catalog = rest_catalog();
        let mut create = route(RouteKind::Create);
        create.request_body_declared = true;
        create.request_body_schema_present = false;

        let refusal = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-schemaless",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "content": "syntax = \"proto3\";" }"#,
            },
        )
        .expect_err("schema-less bodies should not be validated as response resources");

        assert!(matches!(
            refusal,
            RestRefusal::UnsupportedShape { ref detail }
                if detail.contains("does not declare an application/json or application/x-www-form-urlencoded schema")
                    && detail.contains("schema-less or wildcard content bodies")
        ));
    }

    #[test]
    fn inline_request_body_schema_validates_required_fields_and_types() {
        let catalog = parse_rest_catalog_bytes(
            br#"
openapi: 3.0.3
components:
  schemas:
    JwtResponse:
      type: object
      properties:
        message: { type: string }
        role: { type: string }
        token: { type: string }
        type: { type: string }
paths: {}
"#,
            "jwt.yaml",
        )
        .expect("catalog");
        let inline_schema: SchemaObject = serde_yaml::from_str(
            r#"
type: object
required: [email, password]
properties:
  email: { type: string }
  password: { type: string }
"#,
        )
        .expect("inline schema");
        let mut create = route(RouteKind::Create);
        create.resource_name = String::from("jwtresponses");
        create.effective_resource_name = Some(String::from("jwtresponses"));
        create.request_body_declared = true;
        create.request_body_schema_present = true;
        create.request_body_schema = Some(inline_schema);
        create.response_fields = vec![
            String::from("message"),
            String::from("role"),
            String::from("token"),
            String::from("type"),
        ];

        let missing = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "inline-missing",
                uri: &uri("/identity/api/auth/login"),
                headers: &HeaderMap::new(),
                body: br#"{}"#,
            },
        )
        .expect_err("inline request body schema required fields should be enforced");
        assert_eq!(
            missing,
            RestRefusal::MissingRequiredField {
                field: String::from("email"),
                resource: String::from("jwtresponses"),
            }
        );

        let invalid_type = normalize_request(
            &catalog,
            &create,
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "inline-type",
                uri: &uri("/identity/api/auth/login"),
                headers: &HeaderMap::new(),
                body: br#"{ "email": { "bad": ["type"] }, "password": "Password123!" }"#,
            },
        )
        .expect_err("inline request body schema field types should be enforced");
        assert_eq!(
            invalid_type,
            RestRefusal::TypeMismatch {
                field: String::from("email"),
                expected: String::from("string"),
                received: String::from("object"),
            }
        );
    }

    #[test]
    fn get_one_uses_pk_point_lookup_with_path_value_coercion() {
        let catalog = rest_catalog();
        let op = normalize_request(
            &catalog,
            &route(RouteKind::ReadOne),
            NormalizeRequest {
                method: Method::Get,
                path_params: &captures("42"),
                session_id: "rest-2",
                uri: &uri("/files/42"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("normalizes");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::PointLookup);
        assert_eq!(read.table, "files");
        assert_eq!(read.projection, vec!["id", "name", "ratio"]);
        let Some(PredicateExpr::Comparison(comparison)) = read.predicate else {
            panic!("expected comparison");
        };
        assert_eq!(comparison.column, "id");
        assert_eq!(comparison.operator, PredicateOperator::Eq);
        assert_eq!(comparison.values, vec![ScalarValue::Integer(42)]);
    }

    #[test]
    fn rest_reads_project_only_result_scalar_columns() {
        let catalog = rest_catalog();
        let op = normalize_request(
            &catalog,
            &route(RouteKind::ReadMany),
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-safe-projection",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("normalizes");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.projection, vec!["id", "name", "ratio"]);
    }

    #[test]
    fn file_id_single_path_param_uses_case_canonicalized_id_primary_key() {
        let catalog = file_object_catalog();
        let captures = BTreeMap::from([(
            String::from("FileId"),
            String::from("4ff1e5cc-9835-40d5-bb18-09fdb118db9c"),
        )]);
        let op = normalize_request(
            &catalog,
            &file_object_route(RouteKind::ReadOne),
            NormalizeRequest {
                method: Method::Get,
                path_params: &captures,
                session_id: "rest-file-id",
                uri: &uri("/Files/4ff1e5cc-9835-40d5-bb18-09fdb118db9c"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("single FileId path parameter should map to the Id primary key");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::PointLookup);
        assert_eq!(read.table, "fileobjects");
        let Some(PredicateExpr::Comparison(comparison)) = read.predicate else {
            panic!("expected pk predicate");
        };
        assert_eq!(comparison.column, "Id");
        assert_eq!(
            comparison.values,
            vec![ScalarValue::Text(String::from(
                "4ff1e5cc-9835-40d5-bb18-09fdb118db9c"
            ))]
        );
    }

    #[test]
    fn root_singleton_read_without_path_params_uses_bounded_scan() {
        let catalog = rest_catalog();
        let mut route = route(RouteKind::ReadOne);
        route.path_params.clear();
        let op = normalize_request(
            &catalog,
            &route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-root-singleton",
                uri: &uri("/metrics.json"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("root singleton should normalize without a path lookup");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::FilteredScan);
        assert!(read.predicate.is_none());
        assert_eq!(read.limit, Some(1));
    }

    #[test]
    fn get_one_uses_matching_path_param_when_resource_has_no_pk() {
        let mut catalog = rest_catalog();
        catalog
            .resources
            .get_mut("files")
            .expect("resource")
            .primary_key = None;
        let mut route = route(RouteKind::ReadOne);
        route.path_params = vec![String::from("name")];
        let path_params = BTreeMap::from([(String::from("name"), String::from("alpha"))]);

        let op = normalize_request(
            &catalog,
            &route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &path_params,
                session_id: "rest-path-key",
                uri: &uri("/files/alpha"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("matching path parameter should be usable as lookup key");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::FilteredScan);
        assert_eq!(read.table, "files");
        let Some(PredicateExpr::Comparison(comparison)) = read.predicate else {
            panic!("expected comparison");
        };
        assert_eq!(comparison.column, "name");
        assert_eq!(
            comparison.values,
            vec![ScalarValue::Text(String::from("alpha"))]
        );
    }

    #[test]
    fn get_one_uses_identity_alias_path_param_for_sole_required_field_when_resource_has_no_pk() {
        let catalog = authentiq_catalog();
        let captures = pk_capture("public-key-1");
        let op = normalize_request(
            &catalog,
            &authentiq_route(RouteKind::ReadOne),
            NormalizeRequest {
                method: Method::Get,
                path_params: &captures,
                session_id: "rest-pk-alias",
                uri: &uri("/key/public-key-1"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("identity-like path parameter should map to the sole required field");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::FilteredScan);
        assert_eq!(read.table, "authentiqids");
        let Some(PredicateExpr::Comparison(comparison)) = read.predicate else {
            panic!("expected comparison");
        };
        assert_eq!(comparison.column, "sub");
        assert_eq!(
            comparison.values,
            vec![ScalarValue::Text(String::from("public-key-1"))]
        );
    }

    #[test]
    fn delete_uses_identity_alias_path_param_for_sole_required_field_when_resource_has_no_pk() {
        let catalog = authentiq_catalog();
        let captures = pk_capture("public-key-1");
        let op = normalize_request(
            &catalog,
            &authentiq_route(RouteKind::Delete),
            NormalizeRequest {
                method: Method::Delete,
                path_params: &captures,
                session_id: "rest-pk-alias-delete",
                uri: &uri("/key/public-key-1"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("identity-like path parameter should map to the sole required field");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Delete);
        assert_eq!(mutation.table, "authentiqids");
        let Some(PredicateExpr::Comparison(comparison)) = mutation.predicate else {
            panic!("expected comparison");
        };
        assert_eq!(comparison.column, "sub");
    }

    #[test]
    fn delete_uses_resource_prefixed_path_param_when_resource_has_no_pk() {
        let catalog = queue_catalog();
        let captures = BTreeMap::from([(String::from("queueName"), String::from("demo"))]);
        let op = normalize_request(
            &catalog,
            &queue_route(RouteKind::Delete),
            NormalizeRequest {
                method: Method::Delete,
                path_params: &captures,
                session_id: "rest-resource-prefix-delete",
                uri: &uri("/queues/demo"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("resource-prefixed path parameter should map to the stripped field");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Delete);
        assert_eq!(mutation.table, "queues");
        let Some(PredicateExpr::Comparison(comparison)) = mutation.predicate else {
            panic!("expected comparison");
        };
        assert_eq!(comparison.column, "name");
        assert_eq!(
            comparison.values,
            vec![ScalarValue::Text(String::from("demo"))]
        );
    }

    #[test]
    fn child_get_one_uses_terminal_path_param_and_parent_scope_when_resource_has_no_pk() {
        let catalog = fax_media_catalog();
        let captures = fax_media_captures("FX111", "ME222");
        let op = normalize_request(
            &catalog,
            &fax_media_route(RouteKind::ReadOne),
            NormalizeRequest {
                method: Method::Get,
                path_params: &captures,
                session_id: "rest-child-read",
                uri: &uri("/v1/Faxes/FX111/Media/ME222"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("child singleton route should normalize");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::FilteredScan);
        assert_eq!(read.table, "faxmedias");
        let Some(PredicateExpr::Conjunction(comparisons)) = read.predicate else {
            panic!("expected scoped child predicate");
        };
        assert_eq!(comparisons.len(), 2);
        assert_eq!(comparisons[0].column, "fax_sid");
        assert_eq!(
            comparisons[0].values,
            vec![ScalarValue::Text(String::from("FX111"))]
        );
        assert_eq!(comparisons[1].column, "sid");
        assert_eq!(
            comparisons[1].values,
            vec![ScalarValue::Text(String::from("ME222"))]
        );
    }

    #[test]
    fn child_delete_uses_terminal_path_param_and_parent_scope_when_resource_has_no_pk() {
        let catalog = fax_media_catalog();
        let captures = fax_media_captures("FX111", "ME222");
        let op = normalize_request(
            &catalog,
            &fax_media_route(RouteKind::Delete),
            NormalizeRequest {
                method: Method::Delete,
                path_params: &captures,
                session_id: "rest-child-delete",
                uri: &uri("/v1/Faxes/FX111/Media/ME222"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("child delete route should normalize");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Delete);
        assert_eq!(mutation.table, "faxmedias");
        let Some(PredicateExpr::Conjunction(comparisons)) = mutation.predicate else {
            panic!("expected scoped child predicate");
        };
        assert_eq!(
            comparisons
                .iter()
                .map(|comparison| comparison.column.as_str())
                .collect::<Vec<_>>(),
            vec!["fax_sid", "sid"]
        );
    }

    #[test]
    fn child_get_one_uses_terminal_path_column_and_parent_scope_when_resource_has_pk() {
        let catalog = github_issue_catalog();
        let captures = github_issue_captures("7");
        let op = normalize_request(
            &catalog,
            &github_issue_route(RouteKind::ReadOne),
            NormalizeRequest {
                method: Method::Get,
                path_params: &captures,
                session_id: "rest-child-pk-alias",
                uri: &uri("/repos/octocat/Hello-World/issues/7"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("child singleton route should use terminal identifier column");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::FilteredScan);
        assert_eq!(read.table, "issues");
        let Some(PredicateExpr::Conjunction(comparisons)) = read.predicate else {
            panic!("expected scoped child predicate");
        };
        assert_eq!(comparisons.len(), 3);
        assert_eq!(comparisons[0].column, "number");
        assert_eq!(comparisons[0].values, vec![ScalarValue::Integer(7)]);
        assert_eq!(comparisons[1].column, "owner");
        assert_eq!(
            comparisons[1].values,
            vec![ScalarValue::Text(String::from("octocat"))]
        );
        assert_eq!(comparisons[2].column, "repo");
        assert_eq!(
            comparisons[2].values,
            vec![ScalarValue::Text(String::from("Hello-World"))]
        );
    }

    #[test]
    fn child_delete_uses_terminal_identity_alias_as_pk_when_no_column_alias_matches() {
        let catalog = deploy_key_catalog();
        let captures = deploy_key_captures("10");
        let op = normalize_request(
            &catalog,
            &deploy_key_route(RouteKind::Delete),
            NormalizeRequest {
                method: Method::Delete,
                path_params: &captures,
                session_id: "rest-child-key-id",
                uri: &uri("/repos/octocat/Hello-World/keys/10"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("ID-like terminal parameter should map to the primary key");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Delete);
        assert_eq!(mutation.table, "deploy-keys");
        let Some(PredicateExpr::Comparison(comparison)) = mutation.predicate else {
            panic!("expected pk predicate");
        };
        assert_eq!(comparison.column, "id");
        assert_eq!(comparison.values, vec![ScalarValue::Integer(10)]);
    }

    #[test]
    fn child_lookup_with_pk_still_refuses_non_identity_terminal_param_without_column_match() {
        let catalog = deploy_key_catalog();
        let captures = slug_child_captures("demo");
        let refusal = normalize_request(
            &catalog,
            &slug_child_route(RouteKind::ReadOne),
            NormalizeRequest {
                method: Method::Get,
                path_params: &captures,
                session_id: "rest-child-non-id",
                uri: &uri("/repos/octocat/Hello-World/keys/demo"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect_err("non-identity terminal path parameter should not be guessed as a pk");

        assert!(matches!(refusal, RestRefusal::UnsupportedShape { .. }));
    }

    #[test]
    fn read_many_accepts_declared_query_params_and_applies_limit() {
        let catalog = rest_catalog();
        let op = normalize_request(
            &catalog,
            &route(RouteKind::ReadMany),
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-3",
                uri: &uri("/files?name=alpha&limit=10&offset=20"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("normalizes");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::FilteredScan);
        let Some(PredicateExpr::Comparison(comparison)) = read.predicate else {
            panic!("expected single comparison");
        };
        assert_eq!(comparison.column, "name");
        assert_eq!(
            comparison.values,
            vec![ScalarValue::Text(String::from("alpha"))]
        );
        assert_eq!(read.limit, Some(10));
    }

    #[test]
    fn read_many_validates_declared_path_param_schema() {
        let catalog = rest_catalog();
        let mut read_route = route(RouteKind::ReadMany);
        read_route.path_param_specs.push(PathParamSpec {
            name: String::from("id"),
            required: true,
            schema: Some(SchemaObject {
                schema_type: Some(JsonValue::String(String::from("integer"))),
                ..SchemaObject::default()
            }),
        });
        let path_params = captures("not-an-id");

        let refusal = normalize_request(
            &catalog,
            &read_route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &path_params,
                session_id: "rest-read-many-path-param",
                uri: &uri("/files/not-an-id"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect_err("read-many path params should validate before returning data");

        assert_eq!(
            refusal,
            RestRefusal::TypeMismatch {
                field: String::from("id"),
                expected: String::from("integer"),
                received: String::from("not-an-id"),
            }
        );
    }

    #[test]
    fn string_capable_path_param_can_target_integer_pk_as_no_match_predicate() {
        let catalog = rest_catalog();
        let mut read_route = route(RouteKind::ReadOne);
        read_route.path_param_specs.push(PathParamSpec {
            name: String::from("id"),
            required: true,
            schema: Some(SchemaObject {
                one_of: vec![
                    SchemaObject {
                        schema_type: Some(JsonValue::String(String::from("string"))),
                        ..SchemaObject::default()
                    },
                    SchemaObject {
                        schema_type: Some(JsonValue::String(String::from("integer"))),
                        ..SchemaObject::default()
                    },
                ],
                ..SchemaObject::default()
            }),
        });

        let op = normalize_request(
            &catalog,
            &read_route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &captures("my-group"),
                session_id: "rest-string-id-path-param",
                uri: &uri("/files/my-group"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("string-capable path params should not fail integer pk coercion");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::FilteredScan);
        let Some(PredicateExpr::Comparison(comparison)) = read.predicate else {
            panic!("expected single comparison");
        };
        assert_eq!(comparison.column, "id");
        assert_eq!(comparison.operator, PredicateOperator::IsNull);
        assert!(comparison.values.is_empty());
    }

    #[test]
    fn normalization_targets_effective_route_resource_name() {
        let catalog = rest_catalog();
        let mut route = route(RouteKind::ReadMany);
        route.resource_name = String::from("file");
        route.effective_resource_name = Some(String::from("files"));

        let op = normalize_request(
            &catalog,
            &route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-effective-resource",
                uri: &uri("/file"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("normalizes");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.table, "files");
        assert_eq!(read.shape, ReadShape::FilteredScan);
    }

    #[test]
    fn read_many_refuses_invalid_limit_values() {
        let catalog = rest_catalog();

        for (raw_limit, expected_received) in [("abc", "abc"), ("0", "0"), ("-1", "-1")] {
            let request_uri = uri(&format!("/files?limit={raw_limit}"));
            let refusal = normalize_request(
                &catalog,
                &route(RouteKind::ReadMany),
                NormalizeRequest {
                    method: Method::Get,
                    path_params: &BTreeMap::new(),
                    session_id: "rest-invalid-limit",
                    uri: &request_uri,
                    headers: &HeaderMap::new(),
                    body: b"",
                },
            )
            .expect_err("invalid limit should refuse");

            assert_eq!(
                refusal,
                RestRefusal::TypeMismatch {
                    field: String::from("limit"),
                    expected: String::from("positive integer"),
                    received: String::from(expected_received),
                }
            );
        }
    }

    #[test]
    fn declared_integer_query_param_refuses_invalid_value() {
        let catalog = rest_catalog();
        let mut read_route = route(RouteKind::ReadMany);
        read_route.declared_query_params.push(String::from("page"));
        read_route.query_params.push(QueryParamSpec {
            name: String::from("page"),
            required: false,
            schema: Some(SchemaObject {
                schema_type: Some(JsonValue::String(String::from("integer"))),
                ..SchemaObject::default()
            }),
        });

        let refusal = normalize_request(
            &catalog,
            &read_route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-invalid-query",
                uri: &uri("/files?page=not-an-integer"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect_err("invalid declared query value should refuse");

        assert_eq!(
            refusal,
            RestRefusal::TypeMismatch {
                field: String::from("page"),
                expected: String::from("integer"),
                received: String::from("not-an-integer"),
            }
        );
    }

    #[test]
    fn required_query_param_refuses_when_absent() {
        let catalog = rest_catalog();
        let mut read_route = route(RouteKind::ReadMany);
        read_route.declared_query_params.push(String::from("from"));
        read_route.query_params.push(QueryParamSpec {
            name: String::from("from"),
            required: true,
            schema: Some(SchemaObject {
                schema_type: Some(JsonValue::String(String::from("string"))),
                ..SchemaObject::default()
            }),
        });

        let refusal = normalize_request(
            &catalog,
            &read_route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-missing-query",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect_err("missing required query value should refuse");

        assert_eq!(
            refusal,
            RestRefusal::MissingRequiredField {
                field: String::from("from"),
                resource: String::from("query"),
            }
        );
    }

    #[test]
    fn enum_query_param_refuses_outside_declared_values() {
        let catalog = rest_catalog();
        let mut read_route = route(RouteKind::ReadMany);
        read_route
            .declared_query_params
            .push(String::from("currency"));
        read_route.query_params.push(QueryParamSpec {
            name: String::from("currency"),
            required: false,
            schema: Some(SchemaObject {
                schema_type: Some(JsonValue::String(String::from("string"))),
                enum_values: vec![
                    JsonValue::String(String::from("HUF")),
                    JsonValue::String(String::from("EUR")),
                ],
                ..SchemaObject::default()
            }),
        });

        let refusal = normalize_request(
            &catalog,
            &read_route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-invalid-enum-query",
                uri: &uri("/files?currency=USD"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect_err("query enum values should be enforced");

        assert_eq!(
            refusal,
            RestRefusal::TypeMismatch {
                field: String::from("currency"),
                expected: String::from("one of HUF, EUR"),
                received: String::from("USD"),
            }
        );
    }

    #[test]
    fn read_many_ignores_declared_non_column_query_params() {
        let catalog = rest_catalog();
        let mut read_route = route(RouteKind::ReadMany);
        read_route
            .declared_query_params
            .extend([String::from("latitude"), String::from("longitude")]);

        let op = normalize_request(
            &catalog,
            &read_route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-operational-query",
                uri: &uri("/files?latitude=37.775&longitude=-122.418"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("normalizes");

        let IrOp::Read(read) = op else {
            panic!("expected read");
        };
        assert_eq!(read.shape, ReadShape::FilteredScan);
        assert!(read.predicate.is_none());
        assert_eq!(read.limit, None);
    }

    #[test]
    fn undeclared_query_param_refuses() {
        let catalog = rest_catalog();
        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::ReadMany),
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-4",
                uri: &uri("/files?unknown=1"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect_err("refuses");

        assert_eq!(
            refusal,
            RestRefusal::UndeclaredQueryParam {
                param: String::from("unknown")
            }
        );
    }

    #[test]
    fn mutation_routes_refuse_undeclared_query_params() {
        let catalog = rest_catalog();
        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-mutation-query",
                uri: &uri("/files?unknown=1"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": "alpha" }"#,
            },
        )
        .expect_err("mutation should reject undeclared query params");

        assert_eq!(
            refusal,
            RestRefusal::UndeclaredQueryParam {
                param: String::from("unknown")
            }
        );
    }

    #[test]
    fn mutation_routes_allow_declared_query_params() {
        let catalog = rest_catalog();
        let op = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-mutation-declared-query",
                uri: &uri("/files?name=alpha"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": "alpha" }"#,
            },
        )
        .expect("declared mutation query params should not block the mutation");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Insert);
    }

    #[test]
    fn route_refusal_preserves_detail_without_resource_lookup() {
        let catalog = rest_catalog();
        let detail = "Nested resource paths not supported in REST twin v0. Path: /{dataset}/{version}/fields";
        let refusal_route = RouteEntry {
            kind: RouteKind::Refusal {
                detail: detail.to_owned(),
            },
            resource_name: String::from("dataset"),
            path_params: vec![String::from("dataset"), String::from("version")],
            path_param_specs: Vec::new(),
            declared_query_params: Vec::new(),
            query_params: Vec::new(),
            required_auth_schemes: Vec::new(),
            request_body_declared: false,
            request_body_required: false,
            request_body_content_types: Vec::new(),
            request_body_schema_present: false,
            request_body_schema: None,
            request_body_json_schema: None,
            request_schema_ref: None,
            request_resource_name: None,
            response_schema_ref: None,
            response_resource_name: None,
            matched_policy: None,
            effective_resource_name: None,
            routing_evidence: None,
            confidence: None,
            conflict: None,
            success_response: None,
            response_wrapper: None,
            response_fields: Vec::new(),
            pagination: None,
        };

        let refusal = normalize_request(
            &catalog,
            &refusal_route,
            NormalizeRequest {
                method: Method::Get,
                path_params: &BTreeMap::new(),
                session_id: "rest-route-refusal",
                uri: &uri("/oa_citations/v1/fields"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect_err("refuses with route detail");

        assert_eq!(
            refusal,
            RestRefusal::UnsupportedShape {
                detail: detail.to_owned()
            }
        );
    }

    #[test]
    fn missing_required_field_refuses() {
        let catalog = rest_catalog();
        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-5",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1 }"#,
            },
        )
        .expect_err("refuses");

        assert_eq!(
            refusal,
            RestRefusal::MissingRequiredField {
                field: String::from("name"),
                resource: String::from("files"),
            }
        );
    }

    #[test]
    fn post_rejects_number_for_string_body_field() {
        let catalog = rest_catalog();
        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-string-kind",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": 99 }"#,
            },
        )
        .expect_err("JSON numbers should not coerce into string fields");

        assert_eq!(
            refusal,
            RestRefusal::TypeMismatch {
                field: String::from("name"),
                expected: String::from("string"),
                received: String::from("number"),
            }
        );
    }

    #[test]
    fn post_rejects_string_for_integer_body_field() {
        let catalog = rest_catalog();
        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-integer-kind",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": "1", "name": "alpha" }"#,
            },
        )
        .expect_err("JSON strings should not coerce into integer fields");

        assert_eq!(
            refusal,
            RestRefusal::TypeMismatch {
                field: String::from("id"),
                expected: String::from("integer"),
                received: String::from("string"),
            }
        );
    }

    #[test]
    fn post_rejects_string_for_number_body_field() {
        let catalog = rest_catalog();
        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-number-kind",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": "alpha", "ratio": "1.25" }"#,
            },
        )
        .expect_err("JSON strings should not coerce into number fields");

        assert_eq!(
            refusal,
            RestRefusal::TypeMismatch {
                field: String::from("ratio"),
                expected: String::from("number"),
                received: String::from("string"),
            }
        );
    }

    #[test]
    fn unknown_field_refuses_when_schema_forbids_additional_properties() {
        let catalog = rest_catalog();
        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-6",
                uri: &uri("/files"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 1, "name": "alpha", "extra": true }"#,
            },
        )
        .expect_err("refuses");

        assert_eq!(
            refusal,
            RestRefusal::UnknownField {
                field: String::from("extra"),
                resource: String::from("files"),
            }
        );
    }

    #[test]
    fn composite_pk_refuses_for_path_lookup() {
        let mut catalog = rest_catalog();
        catalog
            .resources
            .get_mut("files")
            .expect("resource")
            .primary_key = Some(vec![String::from("id"), String::from("name")]);

        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::ReadOne),
            NormalizeRequest {
                method: Method::Get,
                path_params: &captures("1"),
                session_id: "rest-7",
                uri: &uri("/files/1"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect_err("refuses");

        assert!(matches!(refusal, RestRefusal::UnsupportedShape { .. }));
    }

    #[test]
    fn object_and_array_body_values_round_trip_as_json_capable_scalars() {
        assert_eq!(
            json_to_scalar_value("metadata", &json!({ "nested": true })),
            Ok(ScalarValue::Json(json!({ "nested": true })))
        );
        assert_eq!(
            json_to_scalar_value("tags", &json!(["a"])),
            Ok(ScalarValue::Array(vec![ScalarValue::Text(String::from(
                "a"
            ))]))
        );
    }

    #[test]
    fn update_uses_empty_update_columns_and_patch_present_fields() {
        let catalog = rest_catalog();
        let op = normalize_request(
            &catalog,
            &route(RouteKind::Update),
            NormalizeRequest {
                method: Method::Patch,
                path_params: &captures("3"),
                session_id: "rest-8",
                uri: &uri("/files/3"),
                headers: &HeaderMap::new(),
                body: br#"{ "name": "beta" }"#,
            },
        )
        .expect("normalizes");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Update);
        assert_eq!(mutation.columns, vec!["name"]);
        assert_eq!(
            mutation.rows,
            vec![vec![ScalarValue::Text(String::from("beta"))]]
        );
        assert!(mutation.update_columns.is_empty());
        let Some(PredicateExpr::Comparison(comparison)) = mutation.predicate else {
            panic!("expected pk predicate");
        };
        assert_eq!(comparison.values, vec![ScalarValue::Integer(3)]);
    }

    #[test]
    fn put_full_replace_sets_absent_optional_fields_to_null() {
        let catalog = rest_catalog();
        let op = normalize_request(
            &catalog,
            &route(RouteKind::Update),
            NormalizeRequest {
                method: Method::Put,
                path_params: &captures("4"),
                session_id: "rest-9",
                uri: &uri("/files/4"),
                headers: &HeaderMap::new(),
                body: br#"{ "id": 4, "name": "gamma" }"#,
            },
        )
        .expect("normalizes");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(
            mutation.columns,
            vec!["blob", "id", "metadata", "name", "ratio"]
        );
        assert_eq!(
            mutation.rows,
            vec![vec![
                ScalarValue::Null,
                ScalarValue::Integer(4),
                ScalarValue::Null,
                ScalarValue::Text(String::from("gamma")),
                ScalarValue::Null,
            ]]
        );
        assert!(mutation.update_columns.is_empty());
    }

    #[test]
    fn put_with_distinct_request_schema_updates_present_fields_only() {
        let catalog = todo_catalog();
        let update = todo_route(
            RouteKind::Update,
            "#/components/schemas/UpdateTodo",
            "updatetodos",
        );
        let op = normalize_request(
            &catalog,
            &update,
            NormalizeRequest {
                method: Method::Put,
                path_params: &captures("todo-1"),
                session_id: "rest-update-todo",
                uri: &uri("/todos/todo-1"),
                headers: &HeaderMap::new(),
                body: br#"{ "completed": true }"#,
            },
        )
        .expect("update request schema has no required fields");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Update);
        assert_eq!(mutation.table, "todos");
        assert_eq!(mutation.columns, vec![String::from("completed")]);
        assert_eq!(mutation.rows[0], vec![ScalarValue::Boolean(true)]);
    }

    #[test]
    fn put_with_inline_request_schema_updates_present_fields_only() {
        let catalog = rest_catalog();
        let inline_schema: SchemaObject = serde_yaml::from_str(
            r#"
type: object
properties:
  name: { type: string }
"#,
        )
        .expect("inline schema");
        let mut update = route(RouteKind::Update);
        update.request_body_declared = true;
        update.request_body_schema_present = true;
        update.request_body_schema = Some(inline_schema);

        let op = normalize_request(
            &catalog,
            &update,
            NormalizeRequest {
                method: Method::Put,
                path_params: &captures("4"),
                session_id: "rest-inline-update",
                uri: &uri("/files/4"),
                headers: &HeaderMap::new(),
                body: br#"{ "name": "gamma" }"#,
            },
        )
        .expect("inline request schema should not require response-only fields");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Update);
        assert_eq!(mutation.columns, vec![String::from("name")]);
        assert_eq!(
            mutation.rows,
            vec![vec![ScalarValue::Text(String::from("gamma"))]]
        );
        let Some(PredicateExpr::Comparison(comparison)) = mutation.predicate else {
            panic!("expected pk predicate");
        };
        assert_eq!(comparison.values, vec![ScalarValue::Integer(4)]);
    }

    #[test]
    fn non_json_content_type_refuses_for_mutation_body() {
        let catalog = rest_catalog();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "text/plain".parse().expect("header value"),
        );

        let refusal = normalize_request(
            &catalog,
            &route(RouteKind::Create),
            NormalizeRequest {
                method: Method::Post,
                path_params: &BTreeMap::new(),
                session_id: "rest-10",
                uri: &uri("/files"),
                headers: &headers,
                body: br#"{ "id": 1, "name": "alpha" }"#,
            },
        )
        .expect_err("refuses");

        assert_eq!(
            refusal,
            RestRefusal::UnsupportedMediaType {
                expected: String::from(SUPPORTED_REQUEST_BODY_CONTENT_TYPES),
                received: Some(String::from("text/plain")),
            }
        );
    }

    #[test]
    fn path_pattern_captures_feed_normalizer() {
        let catalog = rest_catalog();
        let pattern = PathPattern::parse("/files/{id}");
        let captures = pattern.captures("/files/9").expect("captures");
        let op = normalize_request(
            &catalog,
            &route(RouteKind::Delete),
            NormalizeRequest {
                method: Method::Delete,
                path_params: &captures,
                session_id: "rest-11",
                uri: &uri("/files/9"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("normalizes");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Delete);
        assert!(mutation.columns.is_empty());
        assert!(mutation.rows.is_empty());
    }

    #[test]
    fn delete_with_declared_json_response_requests_returning_columns() {
        let catalog = rest_catalog();
        let mut route = route(RouteKind::Delete);
        route.success_response = Some(crate::protocol::rest::routes::SuccessResponse {
            status: 202,
            has_body: true,
        });
        route.response_fields = vec![String::from("id"), String::from("name")];

        let op = normalize_request(
            &catalog,
            &route,
            NormalizeRequest {
                method: Method::Delete,
                path_params: &captures("9"),
                session_id: "rest-delete-returning",
                uri: &uri("/files/9"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("normalizes");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Delete);
        assert_eq!(mutation.returning, vec!["id", "name"]);
    }

    #[test]
    fn delete_without_declared_body_omits_returning_columns() {
        let catalog = rest_catalog();
        let mut route = route(RouteKind::Delete);
        route.success_response = Some(crate::protocol::rest::routes::SuccessResponse {
            status: 204,
            has_body: false,
        });
        route.response_fields = vec![String::from("id"), String::from("name")];

        let op = normalize_request(
            &catalog,
            &route,
            NormalizeRequest {
                method: Method::Delete,
                path_params: &captures("9"),
                session_id: "rest-delete-no-returning",
                uri: &uri("/files/9"),
                headers: &HeaderMap::new(),
                body: b"",
            },
        )
        .expect("normalizes");

        let IrOp::Mutation(mutation) = op else {
            panic!("expected mutation");
        };
        assert_eq!(mutation.kind, MutationKind::Delete);
        assert!(mutation.returning.is_empty());
    }
}
