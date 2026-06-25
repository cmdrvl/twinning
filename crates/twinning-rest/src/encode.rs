//! REST response encoding for the HTTP protocol adapter.

use axum::{
    body::Body,
    http::{StatusCode, header},
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::{Number as JsonNumber, Value as JsonValue, json};

use crate::{
    ir::{ColumnName, ScalarValue},
    kernel::value::KernelValue,
    result::{KernelResult, MutationResult, ReadResult, RefusalResult, ResultRow, ResultTag},
};

use super::{
    normalize::RestRefusal,
    routes::{
        ResponseWrapper, ResponseWrapperPayloadShape, ResponseWrapperStaticValue, RouteEntry,
        RouteKind,
    },
};

const APPLICATION_JSON: &str = "application/json";

#[derive(Debug, Clone, PartialEq, Eq)]
struct EncodedRestResponse {
    status: StatusCode,
    body: Option<String>,
    content_type_json: bool,
}

impl EncodedRestResponse {
    fn json(status: StatusCode, body: impl Into<String>) -> Self {
        Self {
            status,
            body: Some(body.into()),
            content_type_json: true,
        }
    }

    fn empty(status: StatusCode) -> Self {
        Self {
            status,
            body: None,
            content_type_json: false,
        }
    }

    fn into_response(self) -> Response {
        let mut builder = Response::builder().status(self.status);
        if self.content_type_json {
            builder = builder.header(header::CONTENT_TYPE, APPLICATION_JSON);
        }

        builder
            .body(Body::from(self.body.unwrap_or_default()))
            .expect("REST response status and headers are static")
    }
}

pub fn encode(result: KernelResult, route: &RouteEntry, session_id: &str) -> Response {
    let _ = session_id;
    encode_payload(result, route).into_response()
}

pub fn encode_rest_refusal(refusal: RestRefusal, session_id: &str) -> Response {
    let _ = session_id;
    encode_rest_refusal_payload(refusal).into_response()
}

pub fn kernel_value_to_json(value: &KernelValue) -> JsonValue {
    match value {
        KernelValue::Null => JsonValue::Null,
        KernelValue::Bigint(value) => JsonValue::Number((*value).into()),
        KernelValue::Integer(value) => JsonValue::Number((i64::from(*value)).into()),
        KernelValue::Smallint(value) => JsonValue::Number((i64::from(*value)).into()),
        KernelValue::Numeric(value) => JsonValue::String(value.clone()),
        KernelValue::Float(value) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        KernelValue::Boolean(value) => JsonValue::Bool(*value),
        KernelValue::Timestamp(value) | KernelValue::Date(value) | KernelValue::Text(value) => {
            JsonValue::String(value.clone())
        }
        KernelValue::Bytes(value) => JsonValue::String(BASE64_STANDARD.encode(value)),
        KernelValue::Json(value) => value.clone(),
        KernelValue::Array(values) => JsonValue::Array(
            values
                .iter()
                .map(kernel_value_to_json)
                .collect::<Vec<JsonValue>>(),
        ),
    }
}

fn encode_payload(result: KernelResult, route: &RouteEntry) -> EncodedRestResponse {
    match result {
        KernelResult::Mutation(mutation) => encode_mutation_payload(mutation, route),
        KernelResult::Read(read) => encode_read_payload(read, route),
        KernelResult::Refusal(refusal) => encode_kernel_refusal_payload(refusal),
        _ => internal_error_payload("REST encoder received a session result from the kernel"),
    }
}

fn encode_mutation_payload(mutation: MutationResult, route: &RouteEntry) -> EncodedRestResponse {
    match mutation.tag {
        ResultTag::Insert => {
            let status = mutation_success_status(route, StatusCode::CREATED);
            if mutation_success_omits_body(route) {
                return EncodedRestResponse::empty(status);
            }

            if mutation.returning_rows.is_empty() {
                return EncodedRestResponse::json(status, "{}");
            }

            encode_mutation_returning_payload(
                status,
                &route.response_fields,
                &mutation.returning_rows,
            )
        }
        ResultTag::Update => {
            if mutation.rows_affected == 0 {
                return not_found_payload();
            }

            let status = mutation_success_status(route, StatusCode::OK);
            if mutation_success_omits_body(route) {
                return EncodedRestResponse::empty(status);
            }

            if mutation.returning_rows.is_empty() {
                return EncodedRestResponse::json(status, "{}");
            }

            encode_mutation_returning_payload(
                status,
                &route.response_fields,
                &mutation.returning_rows,
            )
        }
        ResultTag::Delete => {
            if mutation.rows_affected == 0 {
                return not_found_payload();
            }

            let status = mutation_success_status(route, StatusCode::NO_CONTENT);
            if mutation_success_omits_body(route) || status == StatusCode::NO_CONTENT {
                return EncodedRestResponse::empty(status);
            }

            if mutation.returning_rows.is_empty() {
                return EncodedRestResponse::json(status, "{}");
            }

            encode_mutation_returning_payload(
                status,
                &route.response_fields,
                &mutation.returning_rows,
            )
        }
        _ => internal_error_payload("REST encoder received an unsupported mutation result tag"),
    }
}

fn mutation_success_status(route: &RouteEntry, fallback: StatusCode) -> StatusCode {
    route
        .success_response
        .and_then(|response| StatusCode::from_u16(response.status).ok())
        .unwrap_or(fallback)
}

fn mutation_success_omits_body(route: &RouteEntry) -> bool {
    route.success_response.is_some_and(|response| {
        response.status == StatusCode::NO_CONTENT.as_u16() || !response.has_body
    })
}

fn encode_mutation_returning_payload(
    status: StatusCode,
    columns: &[ColumnName],
    rows: &[ResultRow],
) -> EncodedRestResponse {
    if columns.is_empty() {
        return internal_error_payload(
            "mutation result contains RETURNING rows without REST response column metadata",
        );
    }

    match rows {
        [row] => match row_object_body(columns, row) {
            Ok(body) => EncodedRestResponse::json(status, body),
            Err(detail) => internal_error_payload(detail),
        },
        _ => internal_error_payload("REST mutation returned more than one resource row"),
    }
}

fn encode_read_payload(read: ReadResult, route: &RouteEntry) -> EncodedRestResponse {
    match &route.kind {
        RouteKind::ReadOne => encode_read_one_payload(read, route.response_wrapper.as_ref()),
        RouteKind::ReadMany => encode_read_many_payload(read, route.response_wrapper.as_ref()),
        _ => internal_error_payload("REST encoder received a read result for a non-read route"),
    }
}

fn encode_read_one_payload(
    read: ReadResult,
    wrapper: Option<&ResponseWrapper>,
) -> EncodedRestResponse {
    if let Some(wrapper) = wrapper
        && wrapper.payload_shape == ResponseWrapperPayloadShape::Object
    {
        return encode_wrapped_read_one_payload(wrapper, read);
    }

    match read.rows.as_slice() {
        [] => not_found_payload(),
        [row] => match row_object_body(&read.columns, row) {
            Ok(body) => EncodedRestResponse::json(StatusCode::OK, body),
            Err(detail) => internal_error_payload(detail),
        },
        _ => internal_error_payload("point lookup returned multiple rows"),
    }
}

fn not_found_payload() -> EncodedRestResponse {
    EncodedRestResponse::json(
        StatusCode::NOT_FOUND,
        json_string(&json!({
            "code": "not_found",
            "detail": "resource not found"
        })),
    )
}

fn encode_read_many_payload(
    read: ReadResult,
    wrapper: Option<&ResponseWrapper>,
) -> EncodedRestResponse {
    match rows_array_body(&read.columns, &read.rows) {
        Ok(body) => match wrapper {
            Some(wrapper) => encode_wrapped_read_many_payload(wrapper, read.rows.len(), body),
            None => EncodedRestResponse::json(StatusCode::OK, body),
        },
        Err(detail) => internal_error_payload(detail),
    }
}

fn encode_wrapped_read_many_payload(
    wrapper: &ResponseWrapper,
    row_count: usize,
    rows_body: String,
) -> EncodedRestResponse {
    let rows = match serde_json::from_str::<JsonValue>(&rows_body) {
        Ok(value) => value,
        Err(error) => {
            return internal_error_payload(format!("REST row array JSON is invalid: {error}"));
        }
    };
    let mut fields = response_wrapper_fields(wrapper, row_count);
    fields.push((wrapper.array_field.as_str(), rows));

    match object_body_from_ordered_fields(&fields) {
        Ok(body) => EncodedRestResponse::json(StatusCode::OK, body),
        Err(detail) => internal_error_payload(detail),
    }
}

fn encode_wrapped_read_one_payload(
    wrapper: &ResponseWrapper,
    read: ReadResult,
) -> EncodedRestResponse {
    let payload = match read.rows.as_slice() {
        [] => JsonValue::Null,
        [row] => match row_object_value(&read.columns, row) {
            Ok(value) => value,
            Err(detail) => return internal_error_payload(detail),
        },
        _ => return internal_error_payload("singleton read returned multiple rows"),
    };

    let mut fields = response_wrapper_fields(wrapper, read.rows.len());
    fields.push((wrapper.array_field.as_str(), payload));

    match object_body_from_ordered_fields(&fields) {
        Ok(body) => EncodedRestResponse::json(StatusCode::OK, body),
        Err(detail) => internal_error_payload(detail),
    }
}

fn response_wrapper_fields(wrapper: &ResponseWrapper, row_count: usize) -> Vec<(&str, JsonValue)> {
    let mut fields = Vec::new();

    if let Some(field) = wrapper.status_code_field.as_deref() {
        fields.push((
            field,
            JsonValue::Number(JsonNumber::from(StatusCode::OK.as_u16())),
        ));
    }
    if let Some(field) = wrapper.status_field.as_deref() {
        fields.push((field, JsonValue::String(String::from("OK"))));
    }
    if let Some(field) = wrapper.message_field.as_deref() {
        fields.push((field, JsonValue::String(String::new())));
    }
    if let Some(field) = wrapper.count_field.as_deref() {
        fields.push((field, JsonValue::Number(JsonNumber::from(row_count))));
    }
    for field in &wrapper.static_fields {
        fields.push((
            field.name.as_str(),
            response_wrapper_static_value(&field.value),
        ));
    }

    fields
}

fn response_wrapper_static_value(value: &ResponseWrapperStaticValue) -> JsonValue {
    match value {
        ResponseWrapperStaticValue::Null => JsonValue::Null,
        ResponseWrapperStaticValue::Object => JsonValue::Object(serde_json::Map::new()),
        ResponseWrapperStaticValue::Array => JsonValue::Array(Vec::new()),
        ResponseWrapperStaticValue::Boolean(value) => JsonValue::Bool(*value),
        ResponseWrapperStaticValue::Integer(value) => JsonValue::Number(JsonNumber::from(*value)),
        ResponseWrapperStaticValue::String(value) => JsonValue::String(value.clone()),
    }
}

fn encode_kernel_refusal_payload(refusal: RefusalResult) -> EncodedRestResponse {
    match refusal.code.as_str() {
        "unique_violation" => EncodedRestResponse::json(
            StatusCode::CONFLICT,
            json_string(&json!({
                "code": "conflict",
                "field": refusal_field(&refusal)
            })),
        ),
        "not_null_violation" | "foreign_key_violation" | "check_violation" => {
            EncodedRestResponse::json(
                StatusCode::UNPROCESSABLE_ENTITY,
                json_string(&json!({
                    "code": "validation_failed",
                    "errors": [validation_error_body(&refusal)]
                })),
            )
        }
        "invalid_text_representation" => EncodedRestResponse::json(
            StatusCode::BAD_REQUEST,
            json_string(&json!({
                "code": "type_mismatch",
                "field": refusal_field(&refusal),
                "detail": refusal.message
            })),
        ),
        "unsupported_mutation_shape" | "unsupported_read_shape" | "missing_predicate" => {
            EncodedRestResponse::json(
                StatusCode::NOT_IMPLEMENTED,
                json_string(&json!({ "code": "unsupported_shape" })),
            )
        }
        _ => EncodedRestResponse::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_string(&json!({
                "code": "kernel_error",
                "detail": refusal.message
            })),
        ),
    }
}

fn encode_rest_refusal_payload(refusal: RestRefusal) -> EncodedRestResponse {
    match refusal {
        RestRefusal::MissingRequiredField { field, resource } => EncodedRestResponse::json(
            StatusCode::UNPROCESSABLE_ENTITY,
            json_string(&json!({
                "code": "missing_required_field",
                "detail": format!("required field `{field}` is missing on resource `{resource}`"),
                "field": field,
                "resource": resource
            })),
        ),
        RestRefusal::UnknownField { field, resource } => EncodedRestResponse::json(
            StatusCode::UNPROCESSABLE_ENTITY,
            json_string(&json!({
                "code": "unknown_field",
                "detail": format!("field `{field}` is not declared on resource `{resource}`"),
                "field": field,
                "resource": resource
            })),
        ),
        RestRefusal::TypeMismatch {
            field,
            expected,
            received,
        } => EncodedRestResponse::json(
            StatusCode::BAD_REQUEST,
            json_string(&json!({
                "code": "type_mismatch",
                "detail": format!("field `{field}` expected {expected} but received {received}"),
                "field": field,
                "expected": expected,
                "received": received
            })),
        ),
        RestRefusal::UnsupportedShape { detail } => EncodedRestResponse::json(
            StatusCode::NOT_IMPLEMENTED,
            json_string(&json!({
                "code": "unsupported_shape",
                "detail": detail
            })),
        ),
        RestRefusal::UndeclaredQueryParam { param } => EncodedRestResponse::json(
            StatusCode::BAD_REQUEST,
            json_string(&json!({
                "code": "undeclared_query_param",
                "detail": format!("query parameter `{param}` is not declared for this route"),
                "param": param
            })),
        ),
        RestRefusal::UnsupportedMediaType { expected, received } => {
            let mut body = json!({
                "code": "unsupported_media_type",
                "detail": format!("request body must use {expected}"),
                "expected": expected
            });
            if let JsonValue::Object(object) = &mut body
                && let Some(received) = received
            {
                object.insert(String::from("received"), JsonValue::String(received));
            }

            EncodedRestResponse::json(StatusCode::UNSUPPORTED_MEDIA_TYPE, json_string(&body))
        }
        RestRefusal::InvalidJson { detail } => EncodedRestResponse::json(
            StatusCode::BAD_REQUEST,
            json_string(&json!({
                "code": "invalid_json",
                "detail": detail
            })),
        ),
        RestRefusal::SchemaValidation {
            path,
            detail,
            expected,
            received,
            schema,
        } => {
            let mut body = json!({
                "code": "schema_validation_failed",
                "detail": detail,
                "path": path,
            });
            if let JsonValue::Object(object) = &mut body {
                if let Some(expected) = expected {
                    object.insert(String::from("expected"), JsonValue::String(expected));
                }
                if let Some(received) = received {
                    object.insert(String::from("received"), JsonValue::String(received));
                }
                if let Some(schema) = schema {
                    object.insert(String::from("schema"), JsonValue::String(schema));
                }
            }

            EncodedRestResponse::json(StatusCode::UNPROCESSABLE_ENTITY, json_string(&body))
        }
    }
}

fn validation_error_body(refusal: &RefusalResult) -> JsonValue {
    json!({
        "field": refusal_field(refusal),
        "detail": refusal.message
    })
}

fn refusal_field(refusal: &RefusalResult) -> String {
    refusal
        .detail
        .get("column")
        .or_else(|| refusal.detail.get("columns"))
        .or_else(|| refusal.detail.get("field"))
        .or_else(|| refusal.detail.get("constraint"))
        .cloned()
        .unwrap_or_else(|| String::from("<unknown>"))
}

fn internal_error_payload(detail: impl Into<String>) -> EncodedRestResponse {
    EncodedRestResponse::json(
        StatusCode::INTERNAL_SERVER_ERROR,
        json_string(&json!({
            "code": "kernel_error",
            "detail": detail.into()
        })),
    )
}

fn row_object_body(columns: &[ColumnName], row: &ResultRow) -> Result<String, String> {
    let fields = row_object_fields(columns, row)?;
    object_body_from_ordered_fields(&fields)
}

fn row_object_value(columns: &[ColumnName], row: &ResultRow) -> Result<JsonValue, String> {
    let fields = row_object_fields(columns, row)?;
    Ok(JsonValue::Object(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    ))
}

fn row_object_fields<'a>(
    columns: &'a [ColumnName],
    row: &'a ResultRow,
) -> Result<Vec<(&'a str, JsonValue)>, String> {
    if columns.len() != row.len() {
        return Err(format!(
            "result row arity {} does not match response column count {}",
            row.len(),
            columns.len()
        ));
    }

    let fields = columns
        .iter()
        .zip(row.iter())
        .map(|(column, value)| (column.as_str(), scalar_value_to_json(value)))
        .collect::<Vec<_>>();
    Ok(fields)
}

fn rows_array_body(columns: &[ColumnName], rows: &[ResultRow]) -> Result<String, String> {
    let mut body = String::from("[");
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push_str(&row_object_body(columns, row)?);
    }
    body.push(']');
    Ok(body)
}

fn object_body_from_ordered_fields(fields: &[(&str, JsonValue)]) -> Result<String, String> {
    let mut body = String::from("{");
    for (index, (name, value)) in fields.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push_str(&json_string(&JsonValue::String((*name).to_owned())));
        body.push(':');
        body.push_str(&json_string(value));
    }
    body.push('}');
    Ok(body)
}

fn scalar_value_to_json(value: &ScalarValue) -> JsonValue {
    match value {
        ScalarValue::Null => JsonValue::Null,
        ScalarValue::Boolean(value) => JsonValue::Bool(*value),
        ScalarValue::Integer(value) => JsonValue::Number((*value).into()),
        ScalarValue::Json(value) => value.clone(),
        ScalarValue::Text(value) => JsonValue::String(value.clone()),
        ScalarValue::Array(values) => JsonValue::Array(
            values
                .iter()
                .map(scalar_value_to_json)
                .collect::<Vec<JsonValue>>(),
        ),
    }
}

fn json_string(value: &JsonValue) -> String {
    serde_json::to_string(value).expect("REST response JSON values are serializable")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::http::{StatusCode, header};
    use serde_json::json;

    use crate::{
        ir::ScalarValue,
        kernel::value::KernelValue,
        result::{KernelResult, MutationResult, ReadResult, RefusalResult, ResultTag},
    };

    use super::super::normalize::RestRefusal;
    use super::super::routes::{
        ResponseWrapper, ResponseWrapperPayloadShape, ResponseWrapperStaticField,
        ResponseWrapperStaticValue, RouteEntry, RouteKind, SuccessResponse,
    };
    use super::{
        APPLICATION_JSON, EncodedRestResponse, encode, encode_payload, encode_rest_refusal_payload,
        json_string, kernel_value_to_json,
    };

    trait MethodlessRouteExt {
        fn route(kind: RouteKind) -> RouteEntry;
    }

    impl MethodlessRouteExt for RouteEntry {
        fn route(kind: RouteKind) -> RouteEntry {
            RouteEntry {
                kind,
                resource_name: String::from("files"),
                path_params: vec![String::from("id")],
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
                response_schema_ref: Some(String::from("#/components/schemas/File")),
                response_resource_name: Some(String::from("files")),
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
    }

    #[test]
    fn read_one_serializes_single_resource_with_declared_column_order() {
        let route = RouteEntry::route(RouteKind::ReadOne);
        let payload = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("name"), String::from("id")],
                rows: vec![vec![
                    ScalarValue::Text(String::from("alpha")),
                    ScalarValue::Integer(7),
                ]],
            }),
            &route,
        );

        assert_eq!(payload.status, StatusCode::OK);
        assert_eq!(payload.body.as_deref(), Some(r#"{"name":"alpha","id":7}"#));
        assert!(payload.content_type_json);
    }

    #[test]
    fn read_one_empty_result_is_not_found_json() {
        let route = RouteEntry::route(RouteKind::ReadOne);
        let payload = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id")],
                rows: Vec::new(),
            }),
            &route,
        );

        assert_eq!(payload.status, StatusCode::NOT_FOUND);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(payload.body.as_deref().expect("body"))
                .expect("valid json"),
            json!({
                "code": "not_found",
                "detail": "resource not found"
            })
        );
    }

    #[test]
    fn read_many_serializes_array_and_keeps_empty_arrays_successful() {
        let route = RouteEntry::route(RouteKind::ReadMany);
        let populated = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id"), String::from("name")],
                rows: vec![
                    vec![
                        ScalarValue::Integer(1),
                        ScalarValue::Text(String::from("alpha")),
                    ],
                    vec![ScalarValue::Integer(2), ScalarValue::Null],
                ],
            }),
            &route,
        );
        let empty = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id")],
                rows: Vec::new(),
            }),
            &route,
        );

        assert_eq!(populated.status, StatusCode::OK);
        assert_eq!(
            populated.body.as_deref(),
            Some(r#"[{"id":1,"name":"alpha"},{"id":2,"name":null}]"#)
        );
        assert_eq!(empty.status, StatusCode::OK);
        assert_eq!(empty.body.as_deref(), Some("[]"));
    }

    #[test]
    fn read_many_preserves_declared_wrapper_object_shape() {
        let mut route = RouteEntry::route(RouteKind::ReadMany);
        route.response_wrapper = Some(ResponseWrapper {
            array_field: String::from("artifacts"),
            payload_shape: ResponseWrapperPayloadShape::Array,
            count_field: Some(String::from("count")),
            status_field: None,
            status_code_field: None,
            message_field: None,
            static_fields: vec![
                ResponseWrapperStaticField {
                    name: String::from("credit"),
                    value: ResponseWrapperStaticValue::Integer(0),
                },
                ResponseWrapperStaticField {
                    name: String::from("metadata"),
                    value: ResponseWrapperStaticValue::Object,
                },
                ResponseWrapperStaticField {
                    name: String::from("order"),
                    value: ResponseWrapperStaticValue::Array,
                },
            ],
        });

        let populated = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id"), String::from("name")],
                rows: vec![vec![
                    ScalarValue::Integer(1),
                    ScalarValue::Text(String::from("alpha")),
                ]],
            }),
            &route,
        );
        let empty = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id")],
                rows: Vec::new(),
            }),
            &route,
        );

        assert_eq!(populated.status, StatusCode::OK);
        assert_eq!(
            json_body(&populated),
            json!({
                "count": 1,
                "credit": 0,
                "metadata": {},
                "order": [],
                "artifacts": [
                    {
                        "id": 1,
                        "name": "alpha"
                    }
                ]
            })
        );
        assert_eq!(empty.status, StatusCode::OK);
        assert_eq!(
            json_body(&empty),
            json!({
                "count": 0,
                "credit": 0,
                "metadata": {},
                "order": [],
                "artifacts": []
            })
        );
    }

    #[test]
    fn read_one_preserves_declared_wrapper_object_shape() {
        let mut route = RouteEntry::route(RouteKind::ReadOne);
        route.response_wrapper = Some(ResponseWrapper {
            array_field: String::from("team"),
            payload_shape: ResponseWrapperPayloadShape::Object,
            count_field: None,
            status_field: None,
            status_code_field: None,
            message_field: None,
            static_fields: vec![ResponseWrapperStaticField {
                name: String::from("ok"),
                value: ResponseWrapperStaticValue::Boolean(false),
            }],
        });

        let populated = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id"), String::from("name")],
                rows: vec![vec![
                    ScalarValue::Text(String::from("T0123")),
                    ScalarValue::Text(String::from("Engineering")),
                ]],
            }),
            &route,
        );
        let empty = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id")],
                rows: Vec::new(),
            }),
            &route,
        );

        assert_eq!(populated.status, StatusCode::OK);
        assert_eq!(
            json_body(&populated),
            json!({
                "ok": false,
                "team": {
                    "id": "T0123",
                    "name": "Engineering"
                }
            })
        );
        assert_eq!(empty.status, StatusCode::OK);
        assert_eq!(
            json_body(&empty),
            json!({
                "ok": false,
                "team": null
            })
        );
    }

    #[test]
    fn read_many_with_top_level_array_response_stays_array_shape() {
        let mut route = RouteEntry::route(RouteKind::ReadMany);
        route.response_resource_name = Some(String::from("files"));
        route.effective_resource_name = Some(String::from("files"));

        let populated = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id"), String::from("name")],
                rows: vec![vec![
                    ScalarValue::Integer(1),
                    ScalarValue::Text(String::from("alpha")),
                ]],
            }),
            &route,
        );
        let empty = encode_payload(
            KernelResult::Read(ReadResult {
                columns: vec![String::from("id")],
                rows: Vec::new(),
            }),
            &route,
        );

        assert_eq!(populated.status, StatusCode::OK);
        assert_eq!(
            json_body(&populated),
            json!([
                {
                    "id": 1,
                    "name": "alpha"
                }
            ])
        );
        assert_eq!(empty.status, StatusCode::OK);
        assert_eq!(json_body(&empty), json!([]));
    }

    #[test]
    fn insert_update_and_delete_map_to_rest_statuses() {
        let create_route = RouteEntry::route(RouteKind::Create);
        let update_route = RouteEntry::route(RouteKind::Update);
        let delete_route = RouteEntry::route(RouteKind::Delete);
        let insert = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Insert,
                rows_affected: 1,
                returning_rows: vec![vec![
                    ScalarValue::Integer(7),
                    ScalarValue::Text(String::from("alpha")),
                ]],
            }),
            &create_route,
        );
        let update = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Update,
                rows_affected: 1,
                returning_rows: vec![vec![
                    ScalarValue::Integer(7),
                    ScalarValue::Text(String::from("beta")),
                ]],
            }),
            &update_route,
        );
        let delete = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Delete,
                rows_affected: 1,
                returning_rows: Vec::new(),
            }),
            &delete_route,
        );
        let insert_without_returning = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Insert,
                rows_affected: 1,
                returning_rows: Vec::new(),
            }),
            &create_route,
        );
        let update_without_returning = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Update,
                rows_affected: 1,
                returning_rows: Vec::new(),
            }),
            &update_route,
        );
        let missing_update = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Update,
                rows_affected: 0,
                returning_rows: Vec::new(),
            }),
            &update_route,
        );

        assert_eq!(insert.status, StatusCode::CREATED);
        assert_eq!(insert.body.as_deref(), Some(r#"{"id":7,"name":"alpha"}"#));
        assert_eq!(update.status, StatusCode::OK);
        assert_eq!(update.body.as_deref(), Some(r#"{"id":7,"name":"beta"}"#));
        assert_eq!(
            insert_without_returning,
            EncodedRestResponse::json(StatusCode::CREATED, "{}")
        );
        assert_eq!(
            update_without_returning,
            EncodedRestResponse::json(StatusCode::OK, "{}")
        );
        assert_eq!(missing_update.status, StatusCode::NOT_FOUND);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                missing_update.body.as_deref().expect("body")
            )
            .expect("valid json"),
            json!({
                "code": "not_found",
                "detail": "resource not found"
            })
        );
        assert_eq!(delete, EncodedRestResponse::empty(StatusCode::NO_CONTENT));
    }

    #[test]
    fn delete_zero_rows_returns_not_found_before_success_status() {
        let mut route = RouteEntry::route(RouteKind::Delete);
        route.success_response = Some(SuccessResponse {
            status: StatusCode::ACCEPTED.as_u16(),
            has_body: true,
        });

        let missing_delete = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Delete,
                rows_affected: 0,
                returning_rows: Vec::new(),
            }),
            &route,
        );

        assert_eq!(missing_delete.status, StatusCode::NOT_FOUND);
        assert_eq!(
            json_body(&missing_delete),
            json!({
                "code": "not_found",
                "detail": "resource not found"
            })
        );
    }

    #[test]
    fn delete_with_declared_json_body_encodes_returning_row() {
        let mut route = RouteEntry::route(RouteKind::Delete);
        route.success_response = Some(SuccessResponse {
            status: StatusCode::ACCEPTED.as_u16(),
            has_body: true,
        });

        let payload = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Delete,
                rows_affected: 1,
                returning_rows: vec![vec![
                    ScalarValue::Integer(7),
                    ScalarValue::Text(String::from("gone")),
                ]],
            }),
            &route,
        );

        assert_eq!(payload.status, StatusCode::ACCEPTED);
        assert_eq!(payload.body.as_deref(), Some(r#"{"id":7,"name":"gone"}"#));
    }

    #[test]
    fn declared_no_content_mutation_response_omits_returning_body() {
        let mut route = RouteEntry::route(RouteKind::Create);
        route.success_response = Some(SuccessResponse {
            status: StatusCode::NO_CONTENT.as_u16(),
            has_body: false,
        });

        let payload = encode_payload(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Insert,
                rows_affected: 1,
                returning_rows: vec![vec![
                    ScalarValue::Integer(7),
                    ScalarValue::Text(String::from("alpha")),
                ]],
            }),
            &route,
        );

        assert_eq!(payload, EncodedRestResponse::empty(StatusCode::NO_CONTENT));
    }

    #[test]
    fn kernel_constraint_refusals_map_to_http_error_shapes() {
        let route = RouteEntry::route(RouteKind::Create);

        let unique = encode_payload(
            KernelResult::Refusal(refusal(
                "unique_violation",
                "already exists",
                [("columns", "id")],
            )),
            &route,
        );
        let not_null = encode_payload(
            KernelResult::Refusal(refusal(
                "not_null_violation",
                "id is required",
                [("column", "id")],
            )),
            &route,
        );
        let unsupported = encode_payload(
            KernelResult::Refusal(refusal("unsupported_mutation_shape", "outside subset", [])),
            &route,
        );
        let other = encode_payload(
            KernelResult::Refusal(refusal("storage_error", "backend exploded", [])),
            &route,
        );
        let type_mismatch = encode_payload(
            KernelResult::Refusal(refusal(
                "invalid_text_representation",
                "invalid input syntax for integer",
                [("column", "size")],
            )),
            &route,
        );

        assert_eq!(unique.status, StatusCode::CONFLICT);
        assert_eq!(
            json_body(&unique),
            json!({ "code": "conflict", "field": "id" })
        );
        assert_eq!(not_null.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            json_body(&not_null),
            json!({
                "code": "validation_failed",
                "errors": [{ "field": "id", "detail": "id is required" }]
            })
        );
        assert_eq!(unsupported.status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            json_body(&unsupported),
            json!({ "code": "unsupported_shape" })
        );
        assert_eq!(other.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            json_body(&other),
            json!({ "code": "kernel_error", "detail": "backend exploded" })
        );
        assert_eq!(type_mismatch.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            json_body(&type_mismatch),
            json!({
                "code": "type_mismatch",
                "field": "size",
                "detail": "invalid input syntax for integer"
            })
        );
    }

    #[test]
    fn rest_refusals_map_to_http_statuses_and_json_shapes() {
        let cases = [
            (
                RestRefusal::MissingRequiredField {
                    field: String::from("id"),
                    resource: String::from("files"),
                },
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({
                    "code": "missing_required_field",
                    "detail": "required field `id` is missing on resource `files`",
                    "field": "id",
                    "resource": "files"
                }),
            ),
            (
                RestRefusal::UnknownField {
                    field: String::from("extra"),
                    resource: String::from("files"),
                },
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({
                    "code": "unknown_field",
                    "detail": "field `extra` is not declared on resource `files`",
                    "field": "extra",
                    "resource": "files"
                }),
            ),
            (
                RestRefusal::TypeMismatch {
                    field: String::from("id"),
                    expected: String::from("integer"),
                    received: String::from("abc"),
                },
                StatusCode::BAD_REQUEST,
                json!({
                    "code": "type_mismatch",
                    "detail": "field `id` expected integer but received abc",
                    "field": "id",
                    "expected": "integer",
                    "received": "abc"
                }),
            ),
            (
                RestRefusal::UnsupportedShape {
                    detail: String::from("nested paths are unsupported"),
                },
                StatusCode::NOT_IMPLEMENTED,
                json!({
                    "code": "unsupported_shape",
                    "detail": "nested paths are unsupported"
                }),
            ),
            (
                RestRefusal::UndeclaredQueryParam {
                    param: String::from("sort"),
                },
                StatusCode::BAD_REQUEST,
                json!({
                    "code": "undeclared_query_param",
                    "detail": "query parameter `sort` is not declared for this route",
                    "param": "sort"
                }),
            ),
            (
                RestRefusal::UnsupportedMediaType {
                    expected: String::from("application/json"),
                    received: Some(String::from("text/plain")),
                },
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                json!({
                    "code": "unsupported_media_type",
                    "detail": "request body must use application/json",
                    "expected": "application/json",
                    "received": "text/plain"
                }),
            ),
            (
                RestRefusal::InvalidJson {
                    detail: String::from("expected value at line 1 column 1"),
                },
                StatusCode::BAD_REQUEST,
                json!({
                    "code": "invalid_json",
                    "detail": "expected value at line 1 column 1"
                }),
            ),
            (
                RestRefusal::SchemaValidation {
                    path: String::from("$.edges[0].downstream_key"),
                    detail: String::from("required field `downstream_key` is missing"),
                    expected: Some(String::from("present field")),
                    received: Some(String::from("missing")),
                    schema: Some(String::from("#/components/schemas/BulkLineageRequest")),
                },
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({
                    "code": "schema_validation_failed",
                    "detail": "required field `downstream_key` is missing",
                    "path": "$.edges[0].downstream_key",
                    "expected": "present field",
                    "received": "missing",
                    "schema": "#/components/schemas/BulkLineageRequest"
                }),
            ),
        ];

        for (refusal, expected_status, expected_body) in cases {
            let payload = encode_rest_refusal_payload(refusal);

            assert_eq!(payload.status, expected_status);
            assert_eq!(json_body(&payload), expected_body);
            assert!(payload.content_type_json);
        }
    }

    #[test]
    fn json_responses_set_content_type_and_delete_204_does_not() {
        let route = RouteEntry::route(RouteKind::Create);
        let insert = encode(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Insert,
                rows_affected: 1,
                returning_rows: Vec::new(),
            }),
            &route,
            "rest-test",
        );
        let delete = encode(
            KernelResult::Mutation(MutationResult {
                tag: ResultTag::Delete,
                rows_affected: 1,
                returning_rows: Vec::new(),
            }),
            &route,
            "rest-test",
        );

        assert_eq!(insert.status(), StatusCode::CREATED);
        assert_eq!(
            insert.headers().get(header::CONTENT_TYPE).expect("json"),
            APPLICATION_JSON
        );
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);
        assert!(delete.headers().get(header::CONTENT_TYPE).is_none());
    }

    #[test]
    fn kernel_value_json_mapping_handles_float_and_bytes_defensively() {
        assert_eq!(kernel_value_to_json(&KernelValue::Bigint(7)), json!(7));
        assert_eq!(kernel_value_to_json(&KernelValue::Integer(7)), json!(7));
        assert_eq!(kernel_value_to_json(&KernelValue::Smallint(7)), json!(7));
        assert_eq!(
            kernel_value_to_json(&KernelValue::Numeric(String::from("7.25"))),
            json!("7.25")
        );
        assert_eq!(kernel_value_to_json(&KernelValue::Float(7.25)), json!(7.25));
        assert_eq!(
            kernel_value_to_json(&KernelValue::Float(f64::NAN)),
            serde_json::Value::Null
        );
        assert_eq!(
            kernel_value_to_json(&KernelValue::Float(f64::INFINITY)),
            serde_json::Value::Null
        );
        assert_eq!(
            kernel_value_to_json(&KernelValue::Bytes(vec![0xde, 0xad])),
            json!("3q0=")
        );
    }

    fn refusal<const N: usize>(
        code: &str,
        message: &str,
        detail: [(&str, &str); N],
    ) -> RefusalResult {
        RefusalResult {
            code: code.to_owned(),
            message: message.to_owned(),
            sqlstate: String::from("XX000"),
            detail: detail
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    fn json_body(payload: &EncodedRestResponse) -> serde_json::Value {
        serde_json::from_str(payload.body.as_deref().expect("json body")).expect("valid json")
    }

    #[test]
    fn json_string_helper_is_compact() {
        assert_eq!(json_string(&json!({ "code": "ok" })), r#"{"code":"ok"}"#);
    }
}
