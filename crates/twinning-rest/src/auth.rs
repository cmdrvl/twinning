//! Auth shape extraction for REST request handling.

use std::fmt;

use axum::http::{HeaderMap, header};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use super::spec::SecurityScheme;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "snake_case")]
pub enum RestAuthMode {
    Bypass,
    #[default]
    Shape,
}

impl RestAuthMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bypass => "bypass",
            Self::Shape => "shape",
        }
    }
}

impl fmt::Display for RestAuthMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for RestAuthMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            value if value.eq_ignore_ascii_case("bypass") => Ok(Self::Bypass),
            value if value.eq_ignore_ascii_case("shape") => Ok(Self::Shape),
            _ => Err(format!("unsupported REST auth mode `{value}`")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthExtract {
    Present,
    Missing {
        scheme: String,
        location: AuthLocation,
        header_name: Option<String>,
    },
    Malformed {
        scheme: String,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthLocation {
    Header,
    Query,
    Cookie,
}

impl AuthLocation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Query => "query",
            Self::Cookie => "cookie",
        }
    }
}

impl fmt::Display for AuthLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn check_auth(
    required: &[String],
    schemes: &[SecurityScheme],
    headers: &HeaderMap,
    query: &str,
) -> AuthExtract {
    if required.is_empty() {
        return AuthExtract::Present;
    }

    let mut first_missing = None;
    let mut first_malformed = None;

    for required_scheme in required {
        let result = schemes
            .iter()
            .find(|scheme| scheme.name == *required_scheme)
            .map_or_else(
                || AuthExtract::Missing {
                    scheme: required_scheme.clone(),
                    location: AuthLocation::Header,
                    header_name: None,
                },
                |scheme| check_scheme(scheme, headers, query),
            );

        match result {
            AuthExtract::Present => return AuthExtract::Present,
            AuthExtract::Missing { .. } if first_missing.is_none() => {
                first_missing = Some(result);
            }
            AuthExtract::Malformed { .. } if first_malformed.is_none() => {
                first_malformed = Some(result);
            }
            AuthExtract::Missing { .. } | AuthExtract::Malformed { .. } => {}
        }
    }

    first_malformed
        .or(first_missing)
        .unwrap_or(AuthExtract::Present)
}

fn check_scheme(scheme: &SecurityScheme, headers: &HeaderMap, query: &str) -> AuthExtract {
    let kind = scheme.kind.as_deref().unwrap_or_default();
    if kind.eq_ignore_ascii_case("http") {
        return check_http_scheme(scheme, headers);
    }

    if kind.eq_ignore_ascii_case("apiKey") {
        return check_api_key_scheme(scheme, headers, query);
    }

    if kind.eq_ignore_ascii_case("oauth2") || kind.eq_ignore_ascii_case("openIdConnect") {
        return check_authorization_scheme(scheme, headers, "Bearer");
    }

    AuthExtract::Missing {
        scheme: scheme.name.clone(),
        location: AuthLocation::Header,
        header_name: None,
    }
}

fn check_http_scheme(scheme: &SecurityScheme, headers: &HeaderMap) -> AuthExtract {
    let Some(auth_scheme) = scheme.scheme.as_deref() else {
        return AuthExtract::Missing {
            scheme: scheme.name.clone(),
            location: AuthLocation::Header,
            header_name: Some("Authorization".to_owned()),
        };
    };

    if auth_scheme.eq_ignore_ascii_case("bearer") {
        return check_authorization_scheme(scheme, headers, "Bearer");
    }

    if auth_scheme.eq_ignore_ascii_case("basic") {
        return check_authorization_scheme(scheme, headers, "Basic");
    }

    AuthExtract::Missing {
        scheme: scheme.name.clone(),
        location: AuthLocation::Header,
        header_name: Some("Authorization".to_owned()),
    }
}

fn check_authorization_scheme(
    scheme: &SecurityScheme,
    headers: &HeaderMap,
    expected_prefix: &str,
) -> AuthExtract {
    let missing = || AuthExtract::Missing {
        scheme: scheme.name.clone(),
        location: AuthLocation::Header,
        header_name: Some("Authorization".to_owned()),
    };

    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return missing();
    };

    let Ok(value) = value.to_str() else {
        return AuthExtract::Malformed {
            scheme: scheme.name.clone(),
            detail: format!(
                "Authorization header for {} is not valid UTF-8",
                scheme.name
            ),
        };
    };

    if value.trim().is_empty() {
        return missing();
    }

    if has_prefixed_credential(value, expected_prefix) {
        AuthExtract::Present
    } else {
        AuthExtract::Malformed {
            scheme: scheme.name.clone(),
            detail: format!(
                "Authorization header for {} must use {} credentials",
                scheme.name, expected_prefix
            ),
        }
    }
}

fn has_prefixed_credential(value: &str, expected_prefix: &str) -> bool {
    let Some((prefix, credential)) = value.split_once(' ') else {
        return false;
    };

    prefix.eq_ignore_ascii_case(expected_prefix) && !credential.trim().is_empty()
}

fn check_api_key_scheme(scheme: &SecurityScheme, headers: &HeaderMap, query: &str) -> AuthExtract {
    match scheme.location.as_deref() {
        Some(location) if location.eq_ignore_ascii_case("header") => {
            check_api_key_header(scheme, headers)
        }
        Some(location) if location.eq_ignore_ascii_case("query") => {
            check_api_key_query(scheme, query)
        }
        Some(location) if location.eq_ignore_ascii_case("cookie") => {
            check_api_key_cookie(scheme, headers)
        }
        _ => AuthExtract::Missing {
            scheme: scheme.name.clone(),
            location: AuthLocation::Header,
            header_name: scheme.parameter_name.clone(),
        },
    }
}

fn check_api_key_header(scheme: &SecurityScheme, headers: &HeaderMap) -> AuthExtract {
    let Some(parameter_name) = scheme.parameter_name.as_deref() else {
        return AuthExtract::Missing {
            scheme: scheme.name.clone(),
            location: AuthLocation::Header,
            header_name: None,
        };
    };

    let missing = || AuthExtract::Missing {
        scheme: scheme.name.clone(),
        location: AuthLocation::Header,
        header_name: Some(parameter_name.to_owned()),
    };

    let Some(value) = headers.get(parameter_name) else {
        return missing();
    };

    let Ok(value) = value.to_str() else {
        return AuthExtract::Malformed {
            scheme: scheme.name.clone(),
            detail: format!(
                "{} header for {} is not valid UTF-8",
                parameter_name, scheme.name
            ),
        };
    };

    if value.trim().is_empty() {
        missing()
    } else {
        AuthExtract::Present
    }
}

fn check_api_key_query(scheme: &SecurityScheme, query: &str) -> AuthExtract {
    let Some(parameter_name) = scheme.parameter_name.as_deref() else {
        return AuthExtract::Missing {
            scheme: scheme.name.clone(),
            location: AuthLocation::Query,
            header_name: None,
        };
    };

    if query_has_non_empty_parameter(query, parameter_name) {
        AuthExtract::Present
    } else {
        AuthExtract::Missing {
            scheme: scheme.name.clone(),
            location: AuthLocation::Query,
            header_name: None,
        }
    }
}

fn check_api_key_cookie(scheme: &SecurityScheme, headers: &HeaderMap) -> AuthExtract {
    let Some(parameter_name) = scheme.parameter_name.as_deref() else {
        return AuthExtract::Missing {
            scheme: scheme.name.clone(),
            location: AuthLocation::Cookie,
            header_name: Some("Cookie".to_owned()),
        };
    };

    for value in headers.get_all(header::COOKIE) {
        let Ok(value) = value.to_str() else {
            return AuthExtract::Malformed {
                scheme: scheme.name.clone(),
                detail: format!("Cookie header for {} is not valid UTF-8", scheme.name),
            };
        };

        if cookie_has_non_empty_value(value, parameter_name) {
            return AuthExtract::Present;
        }
    }

    AuthExtract::Missing {
        scheme: scheme.name.clone(),
        location: AuthLocation::Cookie,
        header_name: Some("Cookie".to_owned()),
    }
}

fn query_has_non_empty_parameter(query: &str, parameter_name: &str) -> bool {
    query
        .trim_start_matches('?')
        .split('&')
        .filter(|part| !part.is_empty())
        .any(|part| {
            let (name, value) = part.split_once('=').unwrap_or((part, ""));
            name == parameter_name && !value.trim().is_empty()
        })
}

fn cookie_has_non_empty_value(cookie_header: &str, parameter_name: &str) -> bool {
    cookie_header.split(';').any(|cookie| {
        let cookie = cookie.trim();
        let Some((name, value)) = cookie.split_once('=') else {
            return false;
        };
        name.trim() == parameter_name && !value.trim().is_empty()
    })
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use serde_json::Value as JsonValue;

    use super::*;

    fn scheme(name: &str, kind: &str) -> SecurityScheme {
        SecurityScheme {
            name: name.to_owned(),
            kind: Some(kind.to_owned()),
            scheme: None,
            bearer_format: None,
            location: None,
            parameter_name: None,
            raw: JsonValue::Null,
        }
    }

    fn bearer_scheme() -> SecurityScheme {
        let mut scheme = scheme("bearerAuth", "http");
        scheme.scheme = Some("bearer".to_owned());
        scheme
    }

    fn basic_scheme() -> SecurityScheme {
        let mut scheme = scheme("basicAuth", "http");
        scheme.scheme = Some("basic".to_owned());
        scheme
    }

    fn api_key_scheme(name: &str, location: &str, parameter_name: &str) -> SecurityScheme {
        let mut scheme = scheme(name, "apiKey");
        scheme.location = Some(location.to_owned());
        scheme.parameter_name = Some(parameter_name.to_owned());
        scheme
    }

    fn required(name: &str) -> Vec<String> {
        vec![name.to_owned()]
    }

    #[test]
    fn rest_auth_mode_parse_display_and_default_are_stable() {
        assert_eq!(RestAuthMode::default(), RestAuthMode::Shape);
        assert_eq!(RestAuthMode::Bypass.to_string(), "bypass");
        assert_eq!(RestAuthMode::Shape.to_string(), "shape");
        assert_eq!("bypass".parse::<RestAuthMode>(), Ok(RestAuthMode::Bypass));
        assert_eq!("SHAPE".parse::<RestAuthMode>(), Ok(RestAuthMode::Shape));
        assert!("validate".parse::<RestAuthMode>().is_err());
    }

    #[test]
    fn public_route_is_present() {
        assert_eq!(
            check_auth(&[], &[bearer_scheme()], &HeaderMap::new(), ""),
            AuthExtract::Present
        );
    }

    #[test]
    fn bearer_present() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer token"),
        );

        assert_eq!(
            check_auth(&required("bearerAuth"), &[bearer_scheme()], &headers, ""),
            AuthExtract::Present
        );
    }

    #[test]
    fn bearer_missing() {
        assert_eq!(
            check_auth(
                &required("bearerAuth"),
                &[bearer_scheme()],
                &HeaderMap::new(),
                ""
            ),
            AuthExtract::Missing {
                scheme: "bearerAuth".to_owned(),
                location: AuthLocation::Header,
                header_name: Some("Authorization".to_owned()),
            }
        );
    }

    #[test]
    fn bearer_malformed() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Token token"),
        );

        assert_eq!(
            check_auth(&required("bearerAuth"), &[bearer_scheme()], &headers, ""),
            AuthExtract::Malformed {
                scheme: "bearerAuth".to_owned(),
                detail: "Authorization header for bearerAuth must use Bearer credentials"
                    .to_owned(),
            }
        );
    }

    #[test]
    fn basic_present() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic abc123"),
        );

        assert_eq!(
            check_auth(&required("basicAuth"), &[basic_scheme()], &headers, ""),
            AuthExtract::Present
        );
    }

    #[test]
    fn api_key_header_present_and_missing() {
        let scheme = api_key_scheme("apiKey", "header", "X-API-Key");
        let mut headers = HeaderMap::new();
        headers.insert("X-API-Key", HeaderValue::from_static("secret"));

        assert_eq!(
            check_auth(
                &required("apiKey"),
                std::slice::from_ref(&scheme),
                &headers,
                ""
            ),
            AuthExtract::Present
        );
        assert_eq!(
            check_auth(&required("apiKey"), &[scheme], &HeaderMap::new(), ""),
            AuthExtract::Missing {
                scheme: "apiKey".to_owned(),
                location: AuthLocation::Header,
                header_name: Some("X-API-Key".to_owned()),
            }
        );
    }

    #[test]
    fn api_key_query_present_and_missing() {
        let scheme = api_key_scheme("apiKey", "query", "api_key");

        assert_eq!(
            check_auth(
                &required("apiKey"),
                std::slice::from_ref(&scheme),
                &HeaderMap::new(),
                "limit=10&api_key=secret"
            ),
            AuthExtract::Present
        );
        assert_eq!(
            check_auth(
                &required("apiKey"),
                &[scheme],
                &HeaderMap::new(),
                "limit=10&api_key="
            ),
            AuthExtract::Missing {
                scheme: "apiKey".to_owned(),
                location: AuthLocation::Query,
                header_name: None,
            }
        );
    }

    #[test]
    fn api_key_cookie_present_and_missing() {
        let scheme = api_key_scheme("apiKey", "cookie", "session");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("theme=light; session=secret"),
        );

        assert_eq!(
            check_auth(
                &required("apiKey"),
                std::slice::from_ref(&scheme),
                &headers,
                ""
            ),
            AuthExtract::Present
        );
        assert_eq!(
            check_auth(&required("apiKey"), &[scheme], &HeaderMap::new(), ""),
            AuthExtract::Missing {
                scheme: "apiKey".to_owned(),
                location: AuthLocation::Cookie,
                header_name: Some("Cookie".to_owned()),
            }
        );
    }

    #[test]
    fn any_required_scheme_may_satisfy_auth() {
        let mut headers = HeaderMap::new();
        headers.insert("X-API-Key", HeaderValue::from_static("secret"));

        assert_eq!(
            check_auth(
                &["bearerAuth".to_owned(), "apiKey".to_owned()],
                &[
                    bearer_scheme(),
                    api_key_scheme("apiKey", "header", "X-API-Key")
                ],
                &headers,
                ""
            ),
            AuthExtract::Present
        );
    }
}
