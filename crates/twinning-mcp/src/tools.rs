//! Stable tool descriptors shared by `tools/list` and CLI capabilities.

use serde_json::{Value, json};

pub fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "twin_doctor",
            "description": "Check the twinning binary and the configured work directory.",
            "inputSchema": empty_object_schema(),
            "annotations": { "readOnlyHint": true }
        }),
        json!({
            "name": "twin_list",
            "description": "List the retained Postgres twins managed by this server.",
            "inputSchema": empty_object_schema(),
            "annotations": { "readOnlyHint": true }
        }),
        json!({
            "name": "twin_report",
            "description": "Read the report for a stopped Postgres twin.",
            "inputSchema": object_schema(
                json!({ "twin_id": { "type": "string", "description": "Twin id returned by twin_start." } }),
                vec!["twin_id"]
            ),
            "annotations": { "readOnlyHint": true }
        }),
        json!({
            "name": "twin_seed_contract",
            "description": "Create a seed contract from a Postgres schema.",
            "inputSchema": object_schema(
                json!({
                    "kind": { "type": "string", "enum": ["postgres"] },
                    "schema_path": { "type": "string", "description": "Path to the schema file." }
                }),
                vec!["kind", "schema_path"]
            ),
            "annotations": { "readOnlyHint": false }
        }),
        json!({
            "name": "twin_start",
            "description": "Start a disposable Postgres twin from a schema file.",
            "inputSchema": object_schema(
                json!({
                    "kind": { "type": "string", "enum": ["postgres"] },
                    "schema_path": { "type": "string", "description": "Path to the schema file." },
                    "verify_path": { "type": "string", "description": "Optional verify artifact path." },
                    "seed_path": { "type": "string", "description": "Optional seed file path." },
                    "restore_path": { "type": "string", "description": "Optional snapshot path to restore." },
                    "ttl_minutes": { "type": "integer", "minimum": 1, "maximum": 1440, "default": 60 }
                }),
                vec!["kind", "schema_path"]
            ),
            "annotations": { "readOnlyHint": false, "idempotentHint": false }
        }),
        json!({
            "name": "twin_stop",
            "description": "Stop a managed Postgres twin and return its run summary.",
            "inputSchema": object_schema(
                json!({ "twin_id": { "type": "string", "description": "Twin id returned by twin_start." } }),
                vec!["twin_id"]
            ),
            "annotations": { "readOnlyHint": false, "destructiveHint": false }
        }),
    ]
}

pub fn tool_list_result() -> Value {
    json!({ "tools": tool_definitions() })
}

pub fn tool_names() -> Vec<String> {
    tool_definitions()
        .iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect()
}

fn empty_object_schema() -> Value {
    object_schema(json!({}), Vec::new())
}

fn object_schema(properties: Value, required: Vec<&str>) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use super::{tool_definitions, tool_names};

    #[test]
    fn tool_names_follow_sorted_tool_descriptors() {
        let names = tool_names();
        assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            tool_definitions()
                .iter()
                .all(|tool| { tool["inputSchema"]["additionalProperties"] == false })
        );
    }
}
