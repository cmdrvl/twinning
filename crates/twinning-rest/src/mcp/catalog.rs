//! MCP catalog loading from static manifests or live stdio servers.

use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};

use crate::{
    refusal::{RefusalEnvelope, RefusalResult},
    spec::SecurityScheme,
};

use super::version::default_protocol_version;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpTool {
    pub name: String,
    pub description: Option<String>,
    #[serde(default = "empty_json_object")]
    pub input_schema: JsonValue,
    pub output_schema: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpResource {
    pub uri: Option<String>,
    pub uri_template: Option<String>,
    pub name: String,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpPromptArgument {
    pub name: String,
    pub description: Option<String>,
    pub required: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpPrompt {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub arguments: Vec<McpPromptArgument>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct McpServerInfo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpCatalog {
    pub server_info: McpServerInfo,
    pub tools: Vec<McpTool>,
    pub resources: Vec<McpResource>,
    pub prompts: Vec<McpPrompt>,
    #[serde(default)]
    pub security_schemes: Vec<SecurityScheme>,
    #[serde(default)]
    pub required_auth_schemes: Vec<String>,
    pub catalog_hash: String,
    pub source: McpCatalogSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpCatalogSource {
    LiveServer { command: String },
    Manifest { path: String },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpManifest {
    server_info: McpServerInfo,
    #[serde(default)]
    tools: Vec<McpTool>,
    #[serde(default)]
    resources: Vec<McpResource>,
    #[serde(default)]
    prompts: Vec<McpPrompt>,
    #[serde(default)]
    security_schemes: Vec<SecurityScheme>,
    #[serde(default)]
    required_auth_schemes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InitializeResult {
    #[serde(default)]
    server_info: McpServerInfo,
}

#[derive(Debug, Deserialize)]
struct ToolsListResult {
    #[serde(default)]
    tools: Vec<McpTool>,
}

#[derive(Debug, Deserialize)]
struct ResourcesListResult {
    #[serde(default)]
    resources: Vec<McpResource>,
}

#[derive(Debug, Deserialize)]
struct PromptsListResult {
    #[serde(default)]
    prompts: Vec<McpPrompt>,
}

pub fn load_mcp_catalog_from_manifest(path: &Path) -> RefusalResult<McpCatalog> {
    let raw = fs::read(path).map_err(|error| Box::new(refusal_io_read(path, &error)))?;
    let manifest = serde_json::from_slice::<McpManifest>(&raw).map_err(|error| {
        Box::new(catalog_load_failed(
            "manifest_parse",
            path.display().to_string(),
            error.to_string(),
        ))
    })?;

    build_catalog(
        manifest.server_info,
        manifest.tools,
        manifest.resources,
        manifest.prompts,
        manifest.security_schemes,
        manifest.required_auth_schemes,
        McpCatalogSource::Manifest {
            path: path.display().to_string(),
        },
    )
}

pub fn load_mcp_catalog_from_server(command: &str) -> RefusalResult<McpCatalog> {
    let mut child = shell_command(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            Box::new(catalog_load_failed(
                "server_spawn",
                command.to_owned(),
                error.to_string(),
            ))
        })?;

    let result = load_mcp_catalog_from_child(command, &mut child);
    terminate_child(&mut child);
    result
}

fn load_mcp_catalog_from_child(command: &str, child: &mut Child) -> RefusalResult<McpCatalog> {
    let mut stdin = child.stdin.take().ok_or_else(|| {
        Box::new(catalog_load_failed(
            "server_stdio",
            command.to_owned(),
            "child stdin was not available".to_owned(),
        ))
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        Box::new(catalog_load_failed(
            "server_stdio",
            command.to_owned(),
            "child stdout was not available".to_owned(),
        ))
    })?;
    let response_rx = spawn_stdout_reader(stdout);
    let mut pending = BTreeMap::<i64, JsonValue>::new();

    write_jsonrpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": default_protocol_version(),
                "capabilities": {},
                "clientInfo": {
                    "name": "twinning",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }
        }),
        command,
    )?;
    let initialize_result = response_result(
        read_response_by_id(&response_rx, &mut pending, 1, command)?,
        "initialize",
        command,
    )?;
    let initialize =
        deserialize_result::<InitializeResult>(initialize_result, "initialize", command)?;

    write_jsonrpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }),
        command,
    )?;
    write_jsonrpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list"
        }),
        command,
    )?;
    write_jsonrpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "resources/list"
        }),
        command,
    )?;
    write_jsonrpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "prompts/list"
        }),
        command,
    )?;

    let tools_result = response_result(
        read_response_by_id(&response_rx, &mut pending, 2, command)?,
        "tools/list",
        command,
    )?;
    let tools = deserialize_result::<ToolsListResult>(tools_result, "tools/list", command)?.tools;

    let resources = optional_list_result(
        read_response_by_id(&response_rx, &mut pending, 3, command)?,
        "resources/list",
        command,
        |value| {
            deserialize_result::<ResourcesListResult>(value, "resources/list", command)
                .map(|result| result.resources)
        },
    )?;
    let prompts = optional_list_result(
        read_response_by_id(&response_rx, &mut pending, 4, command)?,
        "prompts/list",
        command,
        |value| {
            deserialize_result::<PromptsListResult>(value, "prompts/list", command)
                .map(|result| result.prompts)
        },
    )?;

    build_catalog(
        normalize_server_info(initialize.server_info),
        tools,
        resources,
        prompts,
        Vec::new(),
        Vec::new(),
        McpCatalogSource::LiveServer {
            command: command.to_owned(),
        },
    )
}

fn build_catalog(
    server_info: McpServerInfo,
    mut tools: Vec<McpTool>,
    mut resources: Vec<McpResource>,
    mut prompts: Vec<McpPrompt>,
    mut security_schemes: Vec<SecurityScheme>,
    mut required_auth_schemes: Vec<String>,
    source: McpCatalogSource,
) -> RefusalResult<McpCatalog> {
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    resources.sort_by(|left, right| {
        (&left.name, &left.uri, &left.uri_template).cmp(&(
            &right.name,
            &right.uri,
            &right.uri_template,
        ))
    });
    prompts.sort_by(|left, right| left.name.cmp(&right.name));
    for prompt in &mut prompts {
        prompt
            .arguments
            .sort_by(|left, right| left.name.cmp(&right.name));
    }
    security_schemes.sort_by(|left, right| left.name.cmp(&right.name));
    required_auth_schemes.sort();
    required_auth_schemes.dedup();

    let catalog_hash = catalog_hash(&tools, &resources, &prompts)?;

    Ok(McpCatalog {
        server_info: normalize_server_info(server_info),
        tools,
        resources,
        prompts,
        security_schemes,
        required_auth_schemes,
        catalog_hash,
        source,
    })
}

fn catalog_hash(
    tools: &[McpTool],
    resources: &[McpResource],
    prompts: &[McpPrompt],
) -> RefusalResult<String> {
    let canonical = json!({
        "tools": tools,
        "resources": resources,
        "prompts": prompts,
    });
    let rendered = serde_json::to_vec(&canonical).map_err(|error| {
        Box::new(catalog_load_failed(
            "hash_render",
            "catalog",
            error.to_string(),
        ))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(rendered);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn spawn_stdout_reader(stdout: std::process::ChildStdout) -> Receiver<Result<String, String>> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            if tx.send(line.map_err(|error| error.to_string())).is_err() {
                break;
            }
        }
    });
    rx
}

fn write_jsonrpc<W: Write>(writer: &mut W, value: &JsonValue, command: &str) -> RefusalResult<()> {
    serde_json::to_writer(&mut *writer, value).map_err(|error| {
        Box::new(catalog_load_failed(
            "server_write",
            command.to_owned(),
            error.to_string(),
        ))
    })?;
    writer.write_all(b"\n").map_err(|error| {
        Box::new(catalog_load_failed(
            "server_write",
            command.to_owned(),
            error.to_string(),
        ))
    })?;
    writer.flush().map_err(|error| {
        Box::new(catalog_load_failed(
            "server_write",
            command.to_owned(),
            error.to_string(),
        ))
    })
}

fn read_response_by_id(
    rx: &Receiver<Result<String, String>>,
    pending: &mut BTreeMap<i64, JsonValue>,
    expected_id: i64,
    command: &str,
) -> RefusalResult<JsonValue> {
    if let Some(response) = pending.remove(&expected_id) {
        return Ok(response);
    }

    loop {
        let line = rx.recv_timeout(RESPONSE_TIMEOUT).map_err(|error| {
            Box::new(catalog_load_failed(
                "server_read_timeout",
                command.to_owned(),
                format!(
                    "timed out after {}s waiting for response id {expected_id}: {error}",
                    RESPONSE_TIMEOUT.as_secs()
                ),
            ))
        })?;
        let line = line.map_err(|error| {
            Box::new(catalog_load_failed(
                "server_read",
                command.to_owned(),
                error,
            ))
        })?;
        let value = serde_json::from_str::<JsonValue>(&line).map_err(|error| {
            Box::new(catalog_load_failed(
                "server_response_parse",
                command.to_owned(),
                format!("invalid JSON-RPC response line `{line}`: {error}"),
            ))
        })?;

        if value.get("method").is_some() && value.get("id").is_none() {
            continue;
        }

        let Some(id) = value.get("id").and_then(JsonValue::as_i64) else {
            return Err(Box::new(catalog_load_failed(
                "server_response_parse",
                command.to_owned(),
                format!("JSON-RPC response did not include a numeric id: {value}"),
            )));
        };

        if id == expected_id {
            return Ok(value);
        }

        pending.insert(id, value);
    }
}

fn response_result(response: JsonValue, method: &str, command: &str) -> RefusalResult<JsonValue> {
    if let Some(error) = response.get("error") {
        return Err(Box::new(catalog_load_failed(
            method,
            command.to_owned(),
            format!("server returned JSON-RPC error: {error}"),
        )));
    }
    response.get("result").cloned().ok_or_else(|| {
        Box::new(catalog_load_failed(
            method,
            command.to_owned(),
            "server response did not include result".to_owned(),
        ))
    })
}

fn optional_list_result<T, F>(
    response: JsonValue,
    method: &str,
    command: &str,
    deserialize: F,
) -> RefusalResult<Vec<T>>
where
    F: FnOnce(JsonValue) -> RefusalResult<Vec<T>>,
{
    if is_method_not_found(&response) {
        return Ok(Vec::new());
    }
    let result = response_result(response, method, command)?;
    deserialize(result)
}

fn deserialize_result<T: for<'de> Deserialize<'de>>(
    value: JsonValue,
    method: &str,
    command: &str,
) -> RefusalResult<T> {
    serde_json::from_value(value).map_err(|error| {
        Box::new(catalog_load_failed(
            method,
            command.to_owned(),
            error.to_string(),
        ))
    })
}

fn is_method_not_found(response: &JsonValue) -> bool {
    response
        .get("error")
        .and_then(|error| error.get("code"))
        .and_then(JsonValue::as_i64)
        == Some(-32601)
}

fn terminate_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill();
    }
    let _ = child.wait();
}

fn shell_command(command: &str) -> Command {
    #[cfg(unix)]
    {
        let mut child_command = Command::new("sh");
        child_command.arg("-c").arg(command);
        child_command
    }

    #[cfg(windows)]
    {
        let mut child_command = Command::new("cmd");
        child_command.arg("/C").arg(command);
        child_command
    }
}

fn empty_json_object() -> JsonValue {
    json!({})
}

fn normalize_server_info(mut server_info: McpServerInfo) -> McpServerInfo {
    if server_info.name.is_empty() {
        server_info.name = "unknown".to_owned();
    }
    if server_info.version.is_empty() {
        server_info.version = "unknown".to_owned();
    }
    server_info
}

fn refusal_io_read(path: &Path, error: &std::io::Error) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_IO_READ",
        format!("Failed to read `{}`.", path.display()),
        json!({ "path": path.display().to_string(), "error": error.to_string() }),
        None,
    )
}

fn catalog_load_failed(
    stage: impl Into<String>,
    source: impl Into<String>,
    error: impl Into<String>,
) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_CATALOG_LOAD_FAILED",
        "MCP catalog loading failed.",
        json!({
            "kind": "catalog_load_failed",
            "stage": stage.into(),
            "source": source.into(),
            "error": error.into(),
        }),
        None,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        process::{Command, Stdio},
    };

    use serde_json::json;
    use tempfile::tempdir;

    use super::{load_mcp_catalog_from_manifest, load_mcp_catalog_from_server};

    #[test]
    fn manifest_catalog_loads_and_hash_is_stable() {
        let dir = tempdir().expect("tempdir");
        let manifest_path = dir.path().join("mcp.json");
        write_manifest(&manifest_path);

        let first = load_mcp_catalog_from_manifest(&manifest_path).expect("manifest should load");
        let second =
            load_mcp_catalog_from_manifest(&manifest_path).expect("manifest should reload");

        assert_eq!("demo-server", first.server_info.name);
        assert_eq!(2, first.tools.len());
        assert_eq!(1, first.resources.len());
        assert_eq!(1, first.prompts.len());
        assert_eq!(first.catalog_hash, second.catalog_hash);
        assert!(first.catalog_hash.starts_with("sha256:"));
        assert_eq!(
            vec!["lookup".to_owned(), "write_note".to_owned()],
            first
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn manifest_hash_ignores_manifest_source_path() {
        let dir = tempdir().expect("tempdir");
        let first_path = dir.path().join("first.json");
        let second_path = dir.path().join("second.json");
        write_manifest(&first_path);
        write_manifest(&second_path);

        let first = load_mcp_catalog_from_manifest(&first_path).expect("first manifest");
        let second = load_mcp_catalog_from_manifest(&second_path).expect("second manifest");

        assert_eq!(first.catalog_hash, second.catalog_hash);
        assert_ne!(first.source, second.source);
    }

    #[test]
    fn live_server_catalog_loads_from_stdio_jsonrpc() {
        let dir = tempdir().expect("tempdir");
        let script_path = dir.path().join("server.py");
        fs::write(
            &script_path,
            r#"
import json
import sys

for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    if method == "initialize":
        result = {"serverInfo": {"name": "fake-mcp", "version": "1.0.0"}}
    elif method == "tools/list":
        result = {"tools": [{"name": "lookup", "inputSchema": {"type": "object"}, "outputSchema": {"type": "object"}}]}
    elif method == "resources/list":
        result = {"resources": [{"uri": "file:///tmp/demo", "name": "demo", "mimeType": "text/plain"}]}
    elif method == "prompts/list":
        result = {"prompts": [{"name": "summarize", "arguments": [{"name": "topic", "required": True}]}]}
    else:
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": msg.get("id"), "result": result}), flush=True)
"#,
        )
        .expect("write script");

        let catalog = load_mcp_catalog_from_server(&format!(
            "{} {}",
            python_command(),
            script_path.display()
        ))
        .expect("live server catalog should load");

        assert_eq!("fake-mcp", catalog.server_info.name);
        assert_eq!(1, catalog.tools.len());
        assert_eq!(1, catalog.resources.len());
        assert_eq!(1, catalog.prompts.len());
    }

    #[test]
    fn real_npx_filesystem_server_catalog_loads_when_enabled() {
        if std::env::var("TWINNING_MCP_NPX_TEST").ok().as_deref() != Some("1") {
            return;
        }
        if !command_exists("npx") {
            return;
        }

        let catalog =
            load_mcp_catalog_from_server("npx -y @modelcontextprotocol/server-filesystem /tmp")
                .expect("npx filesystem MCP server should expose a catalog");

        assert!(!catalog.tools.is_empty());
    }

    fn write_manifest(path: &Path) {
        let manifest = json!({
            "serverInfo": {
                "name": "demo-server",
                "version": "0.1.0"
            },
            "tools": [
                {
                    "name": "write_note",
                    "description": "stateful tool",
                    "inputSchema": { "type": "object" }
                },
                {
                    "name": "lookup",
                    "description": "read-only lookup",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" }
                        }
                    },
                    "outputSchema": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" }
                        }
                    }
                }
            ],
            "resources": [
                {
                    "uri": "file:///tmp/demo",
                    "name": "demo",
                    "mimeType": "text/plain"
                }
            ],
            "prompts": [
                {
                    "name": "summarize",
                    "description": "Summarize a topic",
                    "arguments": [
                        {
                            "name": "topic",
                            "description": "Topic to summarize",
                            "required": true
                        }
                    ]
                }
            ]
        });
        fs::write(
            path,
            serde_json::to_string_pretty(&manifest).expect("render manifest"),
        )
        .expect("write manifest");
    }

    fn command_exists(command: &str) -> bool {
        Command::new("which")
            .arg(command)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn python_command() -> &'static str {
        if command_exists("python3") {
            "python3"
        } else {
            "python"
        }
    }
}
