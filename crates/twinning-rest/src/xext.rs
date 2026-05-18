//! OpenAPI `x-twinning` extension parsing.

use serde::{Deserialize, Serialize};

use super::{auth::RestAuthMode, policy::RoutingPolicy, spec::OpenApiDoc};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XTwinningExt {
    pub routing: Option<RoutingPolicy>,
    pub base_prefix: Option<String>,
    pub auth_mode: Option<RestAuthMode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct RawXTwinningAuth {
    mode: Option<String>,
}

pub fn parse_x_twinning(document: &OpenApiDoc) -> Option<XTwinningExt> {
    let value = document.extensions.get("x-twinning")?;
    let raw: RawXTwinningExt = serde_yaml::from_value(value.clone()).ok()?;

    Some(XTwinningExt {
        routing: raw.routing.as_deref().and_then(|value| value.parse().ok()),
        base_prefix: raw.base_prefix,
        auth_mode: raw
            .auth_mode
            .as_deref()
            .or_else(|| raw.auth.as_ref().and_then(|auth| auth.mode.as_deref()))
            .and_then(|value| value.parse().ok()),
    })
}

pub fn resolve_auth_mode(
    cli_override: Option<RestAuthMode>,
    extension: Option<&XTwinningExt>,
) -> RestAuthMode {
    cli_override
        .or_else(|| extension.and_then(|extension| extension.auth_mode))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{XTwinningExt, parse_x_twinning, resolve_auth_mode};
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
        let extension = parse_x_twinning(&document).expect("x-twinning extension");

        assert_eq!(
            extension,
            XTwinningExt {
                routing: Some(RoutingPolicy::PrefixScoped),
                base_prefix: Some("/api/v1".to_owned()),
                auth_mode: Some(RestAuthMode::Bypass),
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
        let extension = parse_x_twinning(&document).expect("x-twinning extension");

        assert_eq!(extension.auth_mode, Some(RestAuthMode::Shape));
    }

    #[test]
    fn auth_mode_resolution_uses_cli_then_extension_then_shape_default() {
        let extension = XTwinningExt {
            routing: None,
            base_prefix: None,
            auth_mode: Some(RestAuthMode::Bypass),
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
