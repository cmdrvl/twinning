//! Startup routing attribution report for REST route classification.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::{
    canary::RestCanaryReport,
    policy::RoutingPolicy,
    routes::{PathPattern, PathSegment, RouteKind, RouteRegistry},
    session_log::RestSessionSummary,
    spec::{RestCatalog, SecurityScheme},
    topology::{Confidence, EvidenceSource},
};

pub const REST_REPORT_VERSION: &str = "twinning.rest-report.v0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestReport {
    pub version: String,
    pub outcome: String,
    pub spec: RestSpecReport,
    pub session: RestSessionSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary: Option<RestCanaryReport>,
    pub next_step: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestSpecReport {
    pub source: String,
    pub hash: String,
    pub resource_count: usize,
    pub route_count: usize,
    pub security_schemes_bypassed: Vec<String>,
}

impl RestReport {
    pub fn new(
        spec: RestSpecReport,
        session: RestSessionSummary,
        canary: Option<RestCanaryReport>,
    ) -> Self {
        let outcome = if canary.as_ref().is_some_and(|canary| canary.failed > 0) {
            "FAIL"
        } else {
            "PASS"
        };

        Self {
            version: REST_REPORT_VERSION.to_owned(),
            outcome: outcome.to_owned(),
            spec,
            session,
            canary,
            next_step: String::from(
                "Inspect the REST session coverage and canary failures before relying on this twin for client migration proof.",
            ),
        }
    }

    pub fn render_json(&self) -> Result<String, serde_json::Error> {
        let mut rendered = serde_json::to_string_pretty(self)?;
        rendered.push('\n');
        Ok(rendered)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteAttribution {
    pub method: String,
    pub path: String,
    pub matched_policy: Option<EvidenceSource>,
    pub effective_resource: Option<String>,
    pub auth: String,
    pub confidence: Option<Confidence>,
    pub conflict: Option<String>,
    pub status: AttributionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttributionStatus {
    Ok,
    Refused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingReport {
    pub routes: Vec<RouteAttribution>,
}

#[derive(Debug)]
struct RenderedRow {
    method: String,
    path: String,
    policy: String,
    resource: String,
    auth: String,
    confidence: String,
    status: String,
    annotation: String,
}

#[derive(Debug, Default)]
struct Summary {
    ok: usize,
    pinned: usize,
    high: usize,
    medium: usize,
    low: usize,
    waterfall: usize,
    refused: usize,
    conflicts: usize,
    auth_required: usize,
    auth_public: usize,
}

impl RoutingReport {
    pub fn from_registry(registry: &RouteRegistry, catalog: &RestCatalog) -> Self {
        let routes = registry
            .routes
            .iter()
            .map(|(method, pattern, entry)| {
                let status = if matches!(entry.kind, RouteKind::Refusal { .. }) {
                    AttributionStatus::Refused
                } else {
                    AttributionStatus::Ok
                };
                let matched_policy = entry
                    .routing_evidence
                    .or_else(|| entry.matched_policy.map(evidence_from_policy));

                RouteAttribution {
                    method: method.as_str().to_owned(),
                    path: path_pattern_string(pattern),
                    matched_policy,
                    effective_resource: entry.effective_resource_name.clone(),
                    auth: auth_label(&entry.required_auth_schemes, &catalog.security_schemes),
                    confidence: entry.confidence,
                    conflict: entry.conflict.clone(),
                    status,
                }
            })
            .collect();

        Self { routes }
    }

    pub fn log_at_startup(&self) {
        eprintln!("{self}");
    }

    fn summary(&self) -> Summary {
        let mut summary = Summary::default();
        for route in &self.routes {
            match route.status {
                AttributionStatus::Ok => {
                    summary.ok += 1;
                    match route.confidence {
                        Some(Confidence::Pinned) => summary.pinned += 1,
                        Some(Confidence::High) => summary.high += 1,
                        Some(Confidence::Medium) => summary.medium += 1,
                        Some(Confidence::Low) => summary.low += 1,
                        None => summary.waterfall += 1,
                    }
                }
                AttributionStatus::Refused => summary.refused += 1,
            }

            if route.conflict.is_some() {
                summary.conflicts += 1;
            }
            if route.auth == "public" {
                summary.auth_public += 1;
            } else {
                summary.auth_required += 1;
            }
        }
        summary
    }
}

impl fmt::Display for RoutingReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rows = self.routes.iter().map(render_row).collect::<Vec<_>>();
        let method_width = column_width("METHOD", rows.iter().map(|row| row.method.as_str()));
        let path_width = column_width("PATH", rows.iter().map(|row| row.path.as_str()));
        let policy_width = column_width("POLICY", rows.iter().map(|row| row.policy.as_str()));
        let resource_width = column_width("RESOURCE", rows.iter().map(|row| row.resource.as_str()));
        let auth_width = column_width("AUTH", rows.iter().map(|row| row.auth.as_str()));
        let confidence_width =
            column_width("CONFIDENCE", rows.iter().map(|row| row.confidence.as_str()));
        let status_width = column_width("STATUS", rows.iter().map(|row| row.status.as_str()));

        writeln!(
            f,
            "{:<method_width$}  {:<path_width$}  {:<policy_width$}  {:<resource_width$}  {:<auth_width$}  {:<confidence_width$}  {:<status_width$}",
            "METHOD", "PATH", "POLICY", "RESOURCE", "AUTH", "CONFIDENCE", "STATUS"
        )?;
        writeln!(
            f,
            "{:-<method_width$}  {:-<path_width$}  {:-<policy_width$}  {:-<resource_width$}  {:-<auth_width$}  {:-<confidence_width$}  {:-<status_width$}",
            "", "", "", "", "", "", ""
        )?;

        for row in rows {
            writeln!(
                f,
                "{:<method_width$}  {:<path_width$}  {:<policy_width$}  {:<resource_width$}  {:<auth_width$}  {:<confidence_width$}  {:<status_width$}{}",
                row.method,
                row.path,
                row.policy,
                row.resource,
                row.auth,
                row.confidence,
                row.status,
                row.annotation
            )?;
        }

        let summary = self.summary();
        writeln!(
            f,
            "[rest] {} {} ok ({}) | {} waterfall {} | {} refused | {} {}, {} {} public",
            summary.ok,
            plural(summary.ok, "route", "routes"),
            confidence_summary(&summary),
            summary.waterfall,
            plural(summary.waterfall, "fallback", "fallbacks"),
            summary.refused,
            summary.auth_required,
            plural(
                summary.auth_required,
                "route requires auth",
                "routes require auth"
            ),
            summary.auth_public,
            plural(summary.auth_public, "route is", "routes are")
        )?;
        if summary.conflicts > 0 {
            writeln!(
                f,
                "       warning: {} {} detected - see rows marked [conflict] above",
                summary.conflicts,
                plural(summary.conflicts, "conflict", "conflicts")
            )?;
        }

        Ok(())
    }
}

fn render_row(route: &RouteAttribution) -> RenderedRow {
    let mut annotation = String::new();
    if route.confidence == Some(Confidence::Low) {
        annotation.push_str(" [low confidence]");
    }
    if let Some(conflict) = &route.conflict {
        annotation.push_str(" [conflict] [");
        annotation.push_str(conflict);
        annotation.push(']');
    }

    RenderedRow {
        method: route.method.clone(),
        path: route.path.clone(),
        policy: route
            .matched_policy
            .map(evidence_label)
            .unwrap_or("-")
            .to_owned(),
        resource: route
            .effective_resource
            .as_deref()
            .unwrap_or("-")
            .to_owned(),
        auth: route.auth.clone(),
        confidence: route
            .confidence
            .map(confidence_label)
            .unwrap_or("-")
            .to_owned(),
        status: status_label(route.status).to_owned(),
        annotation,
    }
}

fn auth_label(required: &[String], schemes: &[SecurityScheme]) -> String {
    if required.is_empty() {
        return String::from("public");
    }

    let mut labels = required
        .iter()
        .map(|name| {
            let scheme = schemes.iter().find(|scheme| scheme.name == *name);
            auth_scheme_label(name, scheme)
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels.dedup();

    if labels.len() == 1 {
        labels.remove(0)
    } else {
        format!("multi({})", labels.join("|"))
    }
}

fn auth_scheme_label(name: &str, scheme: Option<&SecurityScheme>) -> String {
    let Some(scheme) = scheme else {
        return name.to_owned();
    };

    match scheme
        .kind
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("http") => http_auth_label(scheme),
        Some("apikey") => api_key_auth_label(scheme),
        Some("oauth2" | "openidconnect") => String::from("bearer"),
        Some(kind) => kind.to_owned(),
        None => name.to_owned(),
    }
}

fn http_auth_label(scheme: &SecurityScheme) -> String {
    match scheme
        .scheme
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("bearer") => String::from("bearer"),
        Some("basic") => String::from("basic"),
        Some(value) => format!("http({value})"),
        None => String::from("http"),
    }
}

fn api_key_auth_label(scheme: &SecurityScheme) -> String {
    let name = scheme
        .parameter_name
        .as_deref()
        .unwrap_or(scheme.name.as_str());
    match scheme
        .location
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("query") => format!("apiKey(?{name})"),
        Some("cookie") => format!("apiKey(cookie:{name})"),
        Some("header") | None => format!("apiKey({name})"),
        Some(location) => format!("apiKey({location}:{name})"),
    }
}

fn column_width<'a>(header: &str, values: impl Iterator<Item = &'a str>) -> usize {
    values
        .map(str::len)
        .max()
        .unwrap_or_default()
        .max(header.len())
}

fn confidence_summary(summary: &Summary) -> String {
    let mut parts = Vec::new();
    if summary.high > 0 {
        parts.push(format!("{} high", summary.high));
    }
    if summary.pinned > 0 {
        parts.push(format!("{} pinned", summary.pinned));
    }
    if summary.medium > 0 {
        parts.push(format!("{} medium", summary.medium));
    }
    if summary.low > 0 {
        parts.push(format!("{} low", summary.low));
    }

    if parts.is_empty() {
        String::from("0 confidence")
    } else {
        parts.join(", ")
    }
}

fn plural(count: usize, singular: &'static str, plural: &'static str) -> &'static str {
    if count == 1 { singular } else { plural }
}

fn status_label(status: AttributionStatus) -> &'static str {
    match status {
        AttributionStatus::Ok => "ok",
        AttributionStatus::Refused => "REFUSED",
    }
}

fn confidence_label(confidence: Confidence) -> &'static str {
    match confidence {
        Confidence::Pinned => "pinned",
        Confidence::High => "high",
        Confidence::Medium => "medium",
        Confidence::Low => "low",
    }
}

fn evidence_label(evidence: EvidenceSource) -> &'static str {
    match evidence {
        EvidenceSource::XTwinning => "x-twinning",
        EvidenceSource::FlatCrud => "flat-crud",
        EvidenceSource::SchemaFirst => "schema-first",
        EvidenceSource::PrefixScoped => "prefix-scoped",
        EvidenceSource::Waterfall => "waterfall",
    }
}

fn evidence_from_policy(policy: RoutingPolicy) -> EvidenceSource {
    match policy {
        RoutingPolicy::FlatCrud => EvidenceSource::FlatCrud,
        RoutingPolicy::SchemaFirst => EvidenceSource::SchemaFirst,
        RoutingPolicy::PrefixScoped => EvidenceSource::PrefixScoped,
        RoutingPolicy::Auto => EvidenceSource::Waterfall,
    }
}

fn path_pattern_string(pattern: &PathPattern) -> String {
    if pattern.segments.is_empty() {
        return String::from("/");
    }

    let mut path = String::new();
    for segment in &pattern.segments {
        path.push('/');
        match segment {
            PathSegment::Literal(value) => path.push_str(value),
            PathSegment::Param(value) => {
                path.push('{');
                path.push_str(value);
                path.push('}');
            }
            PathSegment::Template {
                prefix,
                name,
                suffix,
            } => {
                path.push_str(prefix);
                path.push('{');
                path.push_str(name);
                path.push('}');
                path.push_str(suffix);
            }
        }
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attribution(
        policy: Option<EvidenceSource>,
        confidence: Option<Confidence>,
        conflict: Option<&str>,
        status: AttributionStatus,
    ) -> RouteAttribution {
        RouteAttribution {
            method: String::from("GET"),
            path: String::from("/vaults/{id}/items"),
            matched_policy: policy,
            effective_resource: policy.map(|_| String::from("Item")),
            auth: String::from("public"),
            confidence,
            conflict: conflict.map(str::to_owned),
            status,
        }
    }

    #[test]
    fn routing_report_shows_conflict_annotation() {
        let report = RoutingReport {
            routes: vec![attribution(
                Some(EvidenceSource::SchemaFirst),
                Some(Confidence::High),
                Some("flat-crud said: Vault"),
                AttributionStatus::Ok,
            )],
        };

        let rendered = report.to_string();

        assert!(rendered.contains("[conflict]"), "{rendered}");
        assert!(rendered.contains("[flat-crud said: Vault]"), "{rendered}");
    }

    #[test]
    fn routing_report_confidence_column() {
        let report = RoutingReport {
            routes: vec![attribution(
                Some(EvidenceSource::XTwinning),
                Some(Confidence::Pinned),
                None,
                AttributionStatus::Ok,
            )],
        };

        let rendered = report.to_string();

        assert!(rendered.contains("CONFIDENCE"), "{rendered}");
        assert!(rendered.contains("x-twinning"), "{rendered}");
        assert!(rendered.contains("pinned"), "{rendered}");
    }

    #[test]
    fn routing_report_summary_with_warning() {
        let report = RoutingReport {
            routes: vec![
                attribution(
                    Some(EvidenceSource::SchemaFirst),
                    Some(Confidence::High),
                    Some("flat-crud said: Vault"),
                    AttributionStatus::Ok,
                ),
                attribution(None, None, None, AttributionStatus::Refused),
            ],
        };

        let rendered = report.to_string();

        assert!(
            rendered.contains("[rest] 1 route ok (1 high)"),
            "{rendered}"
        );
        assert!(rendered.contains("1 refused"), "{rendered}");
        assert!(
            rendered.contains("warning: 1 conflict detected"),
            "{rendered}"
        );
    }

    #[test]
    fn routing_report_auth_column_and_summary() {
        let mut public = attribution(
            Some(EvidenceSource::SchemaFirst),
            Some(Confidence::High),
            None,
            AttributionStatus::Ok,
        );
        public.path = String::from("/status");
        public.auth = String::from("public");
        let mut secured = attribution(
            Some(EvidenceSource::SchemaFirst),
            Some(Confidence::High),
            None,
            AttributionStatus::Ok,
        );
        secured.auth = String::from("bearer");
        let report = RoutingReport {
            routes: vec![public, secured],
        };

        let rendered = report.to_string();

        assert!(rendered.contains("AUTH"), "{rendered}");
        assert!(rendered.contains("public"), "{rendered}");
        assert!(rendered.contains("bearer"), "{rendered}");
        assert!(
            rendered.contains("1 route requires auth, 1 route is public"),
            "{rendered}"
        );
    }

    #[test]
    fn auth_label_uses_security_scheme_shape() {
        let schemes = vec![
            SecurityScheme {
                name: String::from("bearerAuth"),
                kind: Some(String::from("http")),
                scheme: Some(String::from("bearer")),
                bearer_format: None,
                location: None,
                parameter_name: None,
                raw: serde_json::Value::Null,
            },
            SecurityScheme {
                name: String::from("apiKeyHeader"),
                kind: Some(String::from("apiKey")),
                scheme: None,
                bearer_format: None,
                location: Some(String::from("header")),
                parameter_name: Some(String::from("X-API-KEY")),
                raw: serde_json::Value::Null,
            },
            SecurityScheme {
                name: String::from("apiKeyQuery"),
                kind: Some(String::from("apiKey")),
                scheme: None,
                bearer_format: None,
                location: Some(String::from("query")),
                parameter_name: Some(String::from("api_key")),
                raw: serde_json::Value::Null,
            },
        ];

        assert_eq!(
            auth_label(&[String::from("bearerAuth")], &schemes),
            "bearer"
        );
        assert_eq!(
            auth_label(&[String::from("apiKeyHeader")], &schemes),
            "apiKey(X-API-KEY)"
        );
        assert_eq!(
            auth_label(&[String::from("apiKeyQuery")], &schemes),
            "apiKey(?api_key)"
        );
        assert_eq!(
            auth_label(
                &[String::from("apiKeyHeader"), String::from("bearerAuth")],
                &schemes
            ),
            "multi(apiKey(X-API-KEY)|bearer)"
        );
        assert_eq!(
            auth_label(&[String::from("unknownAuth")], &schemes),
            "unknownAuth"
        );
    }
}
