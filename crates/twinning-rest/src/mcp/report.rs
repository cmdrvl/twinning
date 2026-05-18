//! Report and session accounting for the MCP HTTP twin.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    catalog::{McpCatalog, McpCatalogSource},
    dispatcher::{ToolExecutability, classify_tool_executability},
    version::SUPPORTED_MCP_PROTOCOL_VERSIONS,
};

pub const MCP_REPORT_VERSION: &str = "twinning.mcp-report.v0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpReport {
    pub version: String,
    pub outcome: String,
    pub catalog: McpCatalogReport,
    pub session: McpSessionSummary,
    pub warnings: Vec<String>,
    pub next_step: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpCatalogReport {
    pub source: String,
    pub server_name: String,
    pub server_version: String,
    pub catalog_hash: String,
    pub tool_count: usize,
    pub resource_count: usize,
    pub prompt_count: usize,
    pub tools_stubbable: usize,
    pub tools_unsupported: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpSessionSummary {
    pub request_count: usize,
    pub method_counts: BTreeMap<String, u64>,
    pub refusal_count: u64,
    pub tool_calls: McpToolCallSummary,
    pub protocol_versions: McpProtocolVersionSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpToolCallSummary {
    pub stubbable: u64,
    pub unsupported: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpProtocolVersionSummary {
    pub supported: Vec<String>,
    pub requested: BTreeMap<String, u64>,
    pub negotiated: BTreeMap<String, u64>,
    pub unsupported_requested: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpSessionLog {
    requests: Vec<McpRequestLog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRequestLog {
    pub method: Option<String>,
    pub refusal: bool,
    pub tool_call: Option<McpToolCallOutcome>,
    pub protocol_version: Option<McpProtocolVersionLog>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpToolCallOutcome {
    Stubbable,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpProtocolVersionLog {
    pub requested: Option<String>,
    pub negotiated: String,
    pub supported: bool,
}

impl McpReport {
    pub fn new(catalog: &McpCatalog, session: McpSessionSummary) -> Self {
        let warnings = protocol_warnings(&session.protocol_versions);
        Self {
            version: MCP_REPORT_VERSION.to_owned(),
            outcome: "PASS".to_owned(),
            catalog: McpCatalogReport::from_catalog(catalog),
            session,
            warnings,
            next_step: String::from(
                "Inspect tool executability. Tools with no output schema return unsupported_shape; add outputSchema to the MCP server definition to enable stub execution.",
            ),
        }
    }

    pub fn render_json(&self) -> Result<String, serde_json::Error> {
        let mut rendered = serde_json::to_string_pretty(self)?;
        rendered.push('\n');
        Ok(rendered)
    }
}

impl McpCatalogReport {
    pub fn from_catalog(catalog: &McpCatalog) -> Self {
        let tools_stubbable = catalog
            .tools
            .iter()
            .filter(|tool| classify_tool_executability(tool) == ToolExecutability::Stubbable)
            .count();
        let tools_unsupported = catalog.tools.len().saturating_sub(tools_stubbable);

        Self {
            source: source_label(&catalog.source).to_owned(),
            server_name: catalog.server_info.name.clone(),
            server_version: catalog.server_info.version.clone(),
            catalog_hash: catalog.catalog_hash.clone(),
            tool_count: catalog.tools.len(),
            resource_count: catalog.resources.len(),
            prompt_count: catalog.prompts.len(),
            tools_stubbable,
            tools_unsupported,
        }
    }
}

impl McpSessionLog {
    pub fn record(&mut self, request: McpRequestLog) {
        self.requests.push(request);
    }

    pub fn summary(&self) -> McpSessionSummary {
        let mut summary = McpSessionSummary {
            request_count: self.requests.len(),
            ..McpSessionSummary::default()
        };

        for request in &self.requests {
            if let Some(method) = &request.method {
                *summary.method_counts.entry(method.clone()).or_insert(0) += 1;
            }
            if request.refusal {
                summary.refusal_count += 1;
            }
            match request.tool_call {
                Some(McpToolCallOutcome::Stubbable) => summary.tool_calls.stubbable += 1,
                Some(McpToolCallOutcome::Unsupported) => summary.tool_calls.unsupported += 1,
                None => {}
            }
            if let Some(protocol_version) = &request.protocol_version {
                if let Some(requested) = &protocol_version.requested {
                    *summary
                        .protocol_versions
                        .requested
                        .entry(requested.clone())
                        .or_insert(0) += 1;
                    if !protocol_version.supported {
                        *summary
                            .protocol_versions
                            .unsupported_requested
                            .entry(requested.clone())
                            .or_insert(0) += 1;
                    }
                }
                *summary
                    .protocol_versions
                    .negotiated
                    .entry(protocol_version.negotiated.clone())
                    .or_insert(0) += 1;
            }
        }

        summary
    }
}

impl Default for McpProtocolVersionSummary {
    fn default() -> Self {
        Self {
            supported: SUPPORTED_MCP_PROTOCOL_VERSIONS
                .iter()
                .map(|version| (*version).to_owned())
                .collect(),
            requested: BTreeMap::new(),
            negotiated: BTreeMap::new(),
            unsupported_requested: BTreeMap::new(),
        }
    }
}

pub fn source_label(source: &McpCatalogSource) -> &'static str {
    match source {
        McpCatalogSource::LiveServer { .. } => "live_server",
        McpCatalogSource::Manifest { .. } => "manifest",
    }
}

fn protocol_warnings(summary: &McpProtocolVersionSummary) -> Vec<String> {
    summary
        .unsupported_requested
        .keys()
        .map(|requested| {
            format!(
                "Unsupported MCP protocol version requested: {requested}; negotiated {}",
                SUPPORTED_MCP_PROTOCOL_VERSIONS
                    .last()
                    .expect("MCP supported protocol versions are checked in")
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::mcp::{
        catalog::{McpCatalog, McpCatalogSource, McpServerInfo, McpTool},
        report::{McpReport, McpRequestLog, McpSessionLog, McpToolCallOutcome, source_label},
    };

    #[test]
    fn report_counts_catalog_executability_and_session_activity() {
        let catalog = McpCatalog {
            server_info: McpServerInfo {
                name: "demo".to_owned(),
                version: "1.0.0".to_owned(),
            },
            tools: vec![
                McpTool {
                    name: "lookup".to_owned(),
                    description: None,
                    input_schema: json!({}),
                    output_schema: Some(json!({ "type": "object" })),
                },
                McpTool {
                    name: "write_note".to_owned(),
                    description: None,
                    input_schema: json!({}),
                    output_schema: Some(json!({ "type": "object" })),
                },
            ],
            resources: Vec::new(),
            prompts: Vec::new(),
            security_schemes: Vec::new(),
            required_auth_schemes: Vec::new(),
            catalog_hash: "sha256:test".to_owned(),
            source: McpCatalogSource::Manifest {
                path: "mcp.json".to_owned(),
            },
        };
        let mut log = McpSessionLog::default();
        log.record(McpRequestLog {
            method: Some("tools/call".to_owned()),
            refusal: false,
            tool_call: Some(McpToolCallOutcome::Stubbable),
            protocol_version: None,
        });
        log.record(McpRequestLog {
            method: Some("tools/call".to_owned()),
            refusal: true,
            tool_call: Some(McpToolCallOutcome::Unsupported),
            protocol_version: None,
        });

        let report = McpReport::new(&catalog, log.summary());

        assert_eq!("manifest", source_label(&catalog.source));
        assert_eq!(1, report.catalog.tools_stubbable);
        assert_eq!(1, report.catalog.tools_unsupported);
        assert_eq!(2, report.session.request_count);
        assert_eq!(1, report.session.refusal_count);
        assert_eq!(1, report.session.tool_calls.stubbable);
        assert_eq!(1, report.session.tool_calls.unsupported);
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn report_warns_on_unsupported_protocol_versions() {
        let catalog = McpCatalog {
            server_info: McpServerInfo {
                name: "demo".to_owned(),
                version: "1.0.0".to_owned(),
            },
            tools: Vec::new(),
            resources: Vec::new(),
            prompts: Vec::new(),
            security_schemes: Vec::new(),
            required_auth_schemes: Vec::new(),
            catalog_hash: "sha256:test".to_owned(),
            source: McpCatalogSource::Manifest {
                path: "mcp.json".to_owned(),
            },
        };
        let mut log = McpSessionLog::default();
        log.record(McpRequestLog {
            method: Some("initialize".to_owned()),
            refusal: false,
            tool_call: None,
            protocol_version: Some(super::McpProtocolVersionLog {
                requested: Some("2099-01-01".to_owned()),
                negotiated: "2025-03-26".to_owned(),
                supported: false,
            }),
        });

        let report = McpReport::new(&catalog, log.summary());

        assert_eq!(
            Some(&1),
            report
                .session
                .protocol_versions
                .unsupported_requested
                .get("2099-01-01")
        );
        assert_eq!(1, report.warnings.len());
    }
}
