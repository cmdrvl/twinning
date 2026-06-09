//! OpenAPI `x-twinning` extension parsing.

use std::collections::{BTreeMap, BTreeSet};

use axum::http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::refusal::{RefusalEnvelope, RefusalResult};

use super::{auth::RestAuthMode, policy::RoutingPolicy, routes::Method, spec::OpenApiDoc};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XTwinningExt {
    pub routing: Option<RoutingPolicy>,
    pub base_prefix: Option<String>,
    pub auth_mode: Option<RestAuthMode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub response_stubs: Vec<ResponseStub>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseStub {
    pub id: String,
    pub method: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_json_equals: Option<JsonValue>,
    pub status: u16,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    pub body: ResponseStubBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ResponseStubBody {
    Json(JsonValue),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RawXTwinningExt {
    #[serde(default, alias = "routing-policy", alias = "routing_policy")]
    routing: Option<String>,
    #[serde(
        default,
        rename = "base-prefix",
        alias = "base_prefix",
        alias = "basePrefix"
    )]
    base_prefix: Option<String>,
    #[serde(default, rename = "auth-mode", alias = "auth_mode", alias = "authMode")]
    auth_mode: Option<String>,
    #[serde(default)]
    auth: Option<RawXTwinningAuth>,
    #[serde(
        default,
        rename = "response-stubs",
        alias = "response_stubs",
        alias = "responseStubs"
    )]
    response_stubs: Vec<RawResponseStub>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct RawXTwinningAuth {
    mode: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawResponseStub {
    id: String,
    method: String,
    path: String,
    #[serde(default)]
    when: Option<RawResponseStubWhen>,
    status: u16,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: Option<JsonValue>,
    #[serde(default, rename = "body-text", alias = "body_text", alias = "bodyText")]
    body_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawResponseStubWhen {
    #[serde(
        default,
        rename = "body-json-equals",
        alias = "body_json_equals",
        alias = "bodyJsonEquals"
    )]
    body_json_equals: Option<JsonValue>,
}

pub fn parse_x_twinning(document: &OpenApiDoc) -> RefusalResult<Option<XTwinningExt>> {
    let Some(value) = document.extensions.get("x-twinning") else {
        return Ok(None);
    };
    let raw: RawXTwinningExt = serde_yaml::from_value(value.clone()).map_err(|error| {
        Box::new(x_twinning_refusal(
            "decode",
            format!("x-twinning extension could not be decoded: {error}"),
        ))
    })?;
    let response_stubs = parse_response_stubs(raw.response_stubs)?;

    Ok(Some(XTwinningExt {
        routing: raw.routing.as_deref().and_then(|value| value.parse().ok()),
        base_prefix: raw.base_prefix,
        auth_mode: raw
            .auth_mode
            .as_deref()
            .or_else(|| raw.auth.as_ref().and_then(|auth| auth.mode.as_deref()))
            .and_then(|value| value.parse().ok()),
        response_stubs,
    }))
}

pub fn resolve_auth_mode(
    cli_override: Option<RestAuthMode>,
    extension: Option<&XTwinningExt>,
) -> RestAuthMode {
    cli_override
        .or_else(|| extension.and_then(|extension| extension.auth_mode))
        .unwrap_or_default()
}

fn parse_response_stubs(raw_stubs: Vec<RawResponseStub>) -> RefusalResult<Vec<ResponseStub>> {
    let mut ids = BTreeSet::new();
    let mut parsed = Vec::with_capacity(raw_stubs.len());

    for raw in raw_stubs {
        let stub = parse_response_stub(raw)?;
        if !ids.insert(stub.id.clone()) {
            return Err(Box::new(x_twinning_refusal(
                "duplicate_stub_id",
                format!("response stub id `{}` is declared more than once", stub.id),
            )));
        }
        parsed.push(stub);
    }

    Ok(parsed)
}

fn parse_response_stub(raw: RawResponseStub) -> RefusalResult<ResponseStub> {
    let id = raw.id.trim();
    if id.is_empty() {
        return Err(Box::new(x_twinning_refusal(
            "missing_stub_id",
            "response stub id must be non-empty",
        )));
    }

    let method = parse_stub_method(&raw.method, id)?;
    let path = raw.path.trim();
    if !path.starts_with('/') {
        return Err(Box::new(x_twinning_refusal(
            "invalid_stub_path",
            format!("response stub `{id}` path must start with `/`"),
        )));
    }

    if !(100..=599).contains(&raw.status) {
        return Err(Box::new(x_twinning_refusal(
            "invalid_stub_status",
            format!("response stub `{id}` status must be between 100 and 599"),
        )));
    }

    validate_headers(id, &raw.headers)?;

    let body_json_equals = raw
        .when
        .map(|when| {
            when.body_json_equals.ok_or_else(|| {
                Box::new(x_twinning_refusal(
                    "unsupported_stub_matcher",
                    format!(
                        "response stub `{id}` matcher must use body-json-equals in this release"
                    ),
                ))
            })
        })
        .transpose()?;

    let body = match (raw.body, raw.body_text) {
        (Some(body), None) => ResponseStubBody::Json(body),
        (None, Some(body)) => ResponseStubBody::Text(body),
        (None, None) => {
            return Err(Box::new(x_twinning_refusal(
                "missing_stub_body",
                format!("response stub `{id}` must declare body or body-text"),
            )));
        }
        (Some(_), Some(_)) => {
            return Err(Box::new(x_twinning_refusal(
                "ambiguous_stub_body",
                format!("response stub `{id}` must not declare both body and body-text"),
            )));
        }
    };

    Ok(ResponseStub {
        id: id.to_owned(),
        method,
        path: path.to_owned(),
        body_json_equals,
        status: raw.status,
        headers: raw.headers,
        body,
    })
}

fn parse_stub_method(method: &str, id: &str) -> RefusalResult<String> {
    let normalized = method.trim().to_ascii_uppercase();
    let valid = [
        Method::Get,
        Method::Head,
        Method::Post,
        Method::Put,
        Method::Patch,
        Method::Delete,
    ]
    .into_iter()
    .any(|method| method.as_str() == normalized);

    if valid {
        Ok(normalized)
    } else {
        Err(Box::new(x_twinning_refusal(
            "invalid_stub_method",
            format!("response stub `{id}` has unsupported method `{method}`"),
        )))
    }
}

fn validate_headers(id: &str, headers: &BTreeMap<String, String>) -> RefusalResult<()> {
    for (name, value) in headers {
        HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            Box::new(x_twinning_refusal(
                "invalid_stub_header",
                format!("response stub `{id}` header `{name}` is invalid: {error}"),
            ))
        })?;
        HeaderValue::from_str(value).map_err(|error| {
            Box::new(x_twinning_refusal(
                "invalid_stub_header",
                format!("response stub `{id}` header `{name}` value is invalid: {error}"),
            ))
        })?;
    }
    Ok(())
}

pub fn x_twinning_refusal(reason: &str, detail: impl Into<String>) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_REST_INVALID_X_TWINNING",
        "Invalid REST x-twinning extension.",
        json!({ "protocol": "rest", "reason": reason, "detail": detail.into() }),
        Some(
            "Fix the OpenAPI x-twinning extension and rerun `twinning rest --spec <FILE> --json`."
                .to_owned(),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::{ResponseStubBody, XTwinningExt, parse_x_twinning, resolve_auth_mode};
    use crate::protocol::rest::{
        auth::RestAuthMode,
        policy::RoutingPolicy,
        spec::{OpenApiDoc, parse_rest_catalog_bytes},
    };

    #[test]
    fn parse_x_twinning_full() {
        let raw = r#"
openapi: 3.0.3
info: { title: Routing, version: "1.0" }
x-twinning:
  routing: prefix-scoped
  base-prefix: /api/v1
  auth-mode: bypass
components:
  schemas:
    File:
      type: object
      required: [id]
      properties:
        id: { type: integer }
paths: {}
"#;

        let document: OpenApiDoc = serde_yaml::from_str(raw).expect("doc parses");
        let extension = parse_x_twinning(&document)
            .expect("x-twinning parses")
            .expect("x-twinning extension");

        assert_eq!(
            extension,
            XTwinningExt {
                routing: Some(RoutingPolicy::PrefixScoped),
                base_prefix: Some("/api/v1".to_owned()),
                auth_mode: Some(RestAuthMode::Bypass),
                response_stubs: Vec::new(),
            }
        );

        let catalog = parse_rest_catalog_bytes(raw.as_bytes(), "x.yaml").expect("catalog parses");
        assert_eq!(catalog.x_twinning, Some(extension));
    }

    #[test]
    fn parse_x_twinning_nested_auth_mode() {
        let raw = r#"
openapi: 3.0.3
info: { title: Routing, version: "1.0" }
x-twinning:
  auth:
    mode: shape
components:
  schemas:
    File:
      type: object
      required: [id]
      properties:
        id: { type: integer }
paths: {}
"#;

        let document: OpenApiDoc = serde_yaml::from_str(raw).expect("doc parses");
        let extension = parse_x_twinning(&document)
            .expect("x-twinning parses")
            .expect("x-twinning extension");

        assert_eq!(extension.auth_mode, Some(RestAuthMode::Shape));
    }

    #[test]
    fn parse_response_stub_with_array_body_matcher() {
        let raw = r#"
openapi: 3.0.3
info: { title: Stubs, version: "1.0" }
x-twinning:
  response-stubs:
    - id: openfigi_cusip_success
      method: post
      path: /v3/mapping
      when:
        body-json-equals:
          - idType: ID_CUSIP
            idValue: "037833100"
      status: 200
      headers:
        Content-Type: application/json
      body:
        - data:
            - figi: BBG000B9XRY4
paths: {}
"#;

        let document: OpenApiDoc = serde_yaml::from_str(raw).expect("doc parses");
        let extension = parse_x_twinning(&document)
            .expect("x-twinning parses")
            .expect("x-twinning extension");

        assert_eq!(extension.response_stubs.len(), 1);
        let stub = &extension.response_stubs[0];
        assert_eq!(stub.id, "openfigi_cusip_success");
        assert_eq!(stub.method, "POST");
        assert_eq!(stub.path, "/v3/mapping");
        assert_eq!(stub.status, 200);
        assert_eq!(
            stub.body_json_equals,
            Some(serde_json::json!([{ "idType": "ID_CUSIP", "idValue": "037833100" }]))
        );
        assert!(matches!(stub.body, ResponseStubBody::Json(_)));
    }

    #[test]
    fn duplicate_response_stub_ids_are_rejected() {
        let raw = r#"
openapi: 3.0.3
info: { title: Stubs, version: "1.0" }
x-twinning:
  response-stubs:
    - id: duplicate
      method: GET
      path: /one
      status: 200
      body: { ok: true }
    - id: duplicate
      method: GET
      path: /two
      status: 200
      body: { ok: true }
paths: {}
"#;

        let document: OpenApiDoc = serde_yaml::from_str(raw).expect("doc parses");
        let error = parse_x_twinning(&document).expect_err("duplicate ids fail");
        let rendered = serde_json::to_value(error.as_ref()).expect("serialize refusal");
        assert_eq!(rendered["refusal"]["code"], "E_REST_INVALID_X_TWINNING");
        assert_eq!(rendered["refusal"]["detail"]["reason"], "duplicate_stub_id");
    }

    #[test]
    fn auth_mode_resolution_uses_cli_then_extension_then_shape_default() {
        let extension = XTwinningExt {
            routing: None,
            base_prefix: None,
            auth_mode: Some(RestAuthMode::Bypass),
            response_stubs: Vec::new(),
        };

        assert_eq!(
            resolve_auth_mode(Some(RestAuthMode::Shape), Some(&extension)),
            RestAuthMode::Shape
        );
        assert_eq!(
            resolve_auth_mode(None, Some(&extension)),
            RestAuthMode::Bypass
        );
        assert_eq!(resolve_auth_mode(None, None), RestAuthMode::Shape);
    }
}
