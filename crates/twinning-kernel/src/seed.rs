//! Shared JSONL seed contract primitives.
//!
//! The seed contract is an artifact for an external agent to fill. The
//! twinning runtime never generates mock data, calls a model, samples values,
//! or evaluates protocol-specific semantics in this shared layer.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

pub const SEED_CONTRACT_VERSION: &str = "twinning.seed-contract.v0";
pub const SEED_DATA_VERSION: &str = "twinning.seed-data.v0";
pub const SEED_CONTRACT_KIND_TARGET: &str = "target";
pub const SEED_DATA_KIND_ROW: &str = "row";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeedContractTarget {
    pub version: String,
    pub kind: String,
    pub twin: String,
    pub target_kind: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<SeedField>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seedable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub payload: BTreeMap<String, JsonValue>,
}

impl SeedContractTarget {
    pub fn new(
        twin: impl Into<String>,
        target_kind: impl Into<String>,
        target: impl Into<String>,
        fields: Vec<SeedField>,
    ) -> Self {
        Self {
            version: SEED_CONTRACT_VERSION.to_owned(),
            kind: SEED_CONTRACT_KIND_TARGET.to_owned(),
            twin: twin.into(),
            target_kind: target_kind.into(),
            target: target.into(),
            fields,
            seedable: None,
            reason: None,
            payload: BTreeMap::new(),
        }
    }

    pub fn unsupported(
        twin: impl Into<String>,
        target_kind: impl Into<String>,
        target: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            seedable: Some(false),
            reason: Some(reason.into()),
            ..Self::new(twin, target_kind, target, Vec::new())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeedField {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: String,
    pub required: bool,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub payload: BTreeMap<String, JsonValue>,
}

impl SeedField {
    pub fn new(
        name: impl Into<String>,
        value_type: impl Into<String>,
        required: bool,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            value_type: value_type.into(),
            required,
            nullable,
            payload: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeedDataRow {
    pub version: String,
    pub kind: String,
    pub twin: String,
    pub target_kind: String,
    pub target: String,
    pub row: BTreeMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub payload: BTreeMap<String, JsonValue>,
}

impl SeedDataRow {
    pub fn new(
        twin: impl Into<String>,
        target_kind: impl Into<String>,
        target: impl Into<String>,
        row: BTreeMap<String, JsonValue>,
    ) -> Self {
        Self {
            version: SEED_DATA_VERSION.to_owned(),
            kind: SEED_DATA_KIND_ROW.to_owned(),
            twin: twin.into(),
            target_kind: target_kind.into(),
            target: target.into(),
            row,
            payload: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SeedJsonlDocument {
    pub contract_targets: Vec<SeedContractTarget>,
    pub data_rows: Vec<SeedDataRow>,
}

impl SeedJsonlDocument {
    pub fn is_empty(&self) -> bool {
        self.contract_targets.is_empty() && self.data_rows.is_empty()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum SeedJsonlError {
    #[error("seed JSONL line {line} is blank; expected one JSON object per line")]
    BlankLine { line: usize },
    #[error("seed JSONL line {line} could not be parsed as a seed envelope: {error}")]
    Parse { line: usize, error: String },
    #[error("seed JSONL line {line} uses unknown version `{version}`")]
    UnknownVersion { line: usize, version: String },
    #[error(
        "seed JSONL line {line} uses kind `{kind}` with version `{version}`; expected `{expected}`"
    )]
    UnexpectedKind {
        line: usize,
        version: String,
        kind: String,
        expected: &'static str,
    },
    #[error("seed JSONL line {line} is missing required field `{field}`")]
    MissingField { line: usize, field: &'static str },
    #[error("seed JSONL line {line} field `{field}` is not allowed for {version}/{kind}")]
    UnexpectedField {
        line: usize,
        version: String,
        kind: String,
        field: &'static str,
    },
    #[error(
        "seed contract line {line} duplicates target `{twin}:{target_kind}:{target}` first declared on line {first_line}"
    )]
    DuplicateTarget {
        line: usize,
        first_line: usize,
        twin: String,
        target_kind: String,
        target: String,
    },
    #[error("seed JSONL serialization failed: {error}")]
    Serialize { error: String },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSeedLine {
    version: String,
    kind: String,
    twin: String,
    target_kind: String,
    target: String,
    fields: Option<Vec<SeedField>>,
    seedable: Option<bool>,
    reason: Option<String>,
    row: Option<BTreeMap<String, JsonValue>>,
    #[serde(default)]
    payload: BTreeMap<String, JsonValue>,
}

pub fn parse_seed_jsonl(input: &str) -> Result<SeedJsonlDocument, SeedJsonlError> {
    let mut document = SeedJsonlDocument::default();
    let mut contract_targets = BTreeMap::<(String, String, String), usize>::new();

    for (index, line) in input.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            return Err(SeedJsonlError::BlankLine { line: line_number });
        }

        let raw =
            serde_json::from_str::<RawSeedLine>(line).map_err(|source| SeedJsonlError::Parse {
                line: line_number,
                error: source.to_string(),
            })?;

        match raw.version.as_str() {
            SEED_CONTRACT_VERSION => {
                let target = contract_target_from_raw(raw, line_number, &mut contract_targets)?;
                document.contract_targets.push(target);
            }
            SEED_DATA_VERSION => document
                .data_rows
                .push(data_row_from_raw(raw, line_number)?),
            _ => {
                return Err(SeedJsonlError::UnknownVersion {
                    line: line_number,
                    version: raw.version,
                });
            }
        }
    }

    Ok(document)
}

pub fn render_seed_contract_jsonl(
    targets: &[SeedContractTarget],
) -> Result<String, SeedJsonlError> {
    let mut targets = targets.to_vec();
    for target in &mut targets {
        target.version = SEED_CONTRACT_VERSION.to_owned();
        target.kind = SEED_CONTRACT_KIND_TARGET.to_owned();
        target
            .fields
            .sort_by(|left, right| left.name.cmp(&right.name));
    }
    targets.sort_by(|left, right| {
        (
            left.twin.as_str(),
            left.target_kind.as_str(),
            left.target.as_str(),
        )
            .cmp(&(
                right.twin.as_str(),
                right.target_kind.as_str(),
                right.target.as_str(),
            ))
    });
    ensure_unique_render_targets(&targets)?;

    let mut rendered = String::new();
    for target in targets {
        rendered.push_str(&serde_json::to_string(&target).map_err(|source| {
            SeedJsonlError::Serialize {
                error: source.to_string(),
            }
        })?);
        rendered.push('\n');
    }
    Ok(rendered)
}

pub fn render_seed_data_jsonl(rows: &[SeedDataRow]) -> Result<String, SeedJsonlError> {
    let mut rendered = String::new();
    for row in rows {
        let mut row = row.clone();
        row.version = SEED_DATA_VERSION.to_owned();
        row.kind = SEED_DATA_KIND_ROW.to_owned();
        rendered.push_str(&serde_json::to_string(&row).map_err(|source| {
            SeedJsonlError::Serialize {
                error: source.to_string(),
            }
        })?);
        rendered.push('\n');
    }
    Ok(rendered)
}

fn contract_target_from_raw(
    raw: RawSeedLine,
    line: usize,
    seen: &mut BTreeMap<(String, String, String), usize>,
) -> Result<SeedContractTarget, SeedJsonlError> {
    if raw.kind != SEED_CONTRACT_KIND_TARGET {
        return Err(SeedJsonlError::UnexpectedKind {
            line,
            version: raw.version,
            kind: raw.kind,
            expected: SEED_CONTRACT_KIND_TARGET,
        });
    }
    if raw.row.is_some() {
        return Err(SeedJsonlError::UnexpectedField {
            line,
            version: raw.version,
            kind: raw.kind,
            field: "row",
        });
    }

    let key = (
        raw.twin.clone(),
        raw.target_kind.clone(),
        raw.target.clone(),
    );
    if let Some(first_line) = seen.insert(key.clone(), line) {
        return Err(SeedJsonlError::DuplicateTarget {
            line,
            first_line,
            twin: key.0,
            target_kind: key.1,
            target: key.2,
        });
    }

    Ok(SeedContractTarget {
        version: raw.version,
        kind: raw.kind,
        twin: raw.twin,
        target_kind: raw.target_kind,
        target: raw.target,
        fields: raw.fields.unwrap_or_default(),
        seedable: raw.seedable,
        reason: raw.reason,
        payload: raw.payload,
    })
}

fn data_row_from_raw(raw: RawSeedLine, line: usize) -> Result<SeedDataRow, SeedJsonlError> {
    if raw.kind != SEED_DATA_KIND_ROW {
        return Err(SeedJsonlError::UnexpectedKind {
            line,
            version: raw.version,
            kind: raw.kind,
            expected: SEED_DATA_KIND_ROW,
        });
    }
    if raw.fields.is_some() {
        return Err(SeedJsonlError::UnexpectedField {
            line,
            version: raw.version,
            kind: raw.kind,
            field: "fields",
        });
    }
    if raw.seedable.is_some() {
        return Err(SeedJsonlError::UnexpectedField {
            line,
            version: raw.version,
            kind: raw.kind,
            field: "seedable",
        });
    }
    if raw.reason.is_some() {
        return Err(SeedJsonlError::UnexpectedField {
            line,
            version: raw.version,
            kind: raw.kind,
            field: "reason",
        });
    }

    let row = raw
        .row
        .ok_or(SeedJsonlError::MissingField { line, field: "row" })?;

    Ok(SeedDataRow {
        version: raw.version,
        kind: raw.kind,
        twin: raw.twin,
        target_kind: raw.target_kind,
        target: raw.target,
        row,
        payload: raw.payload,
    })
}

fn ensure_unique_render_targets(targets: &[SeedContractTarget]) -> Result<(), SeedJsonlError> {
    let mut seen = BTreeMap::<(String, String, String), usize>::new();
    for (index, target) in targets.iter().enumerate() {
        let line = index + 1;
        let key = (
            target.twin.clone(),
            target.target_kind.clone(),
            target.target.clone(),
        );
        if let Some(first_line) = seen.insert(key.clone(), line) {
            return Err(SeedJsonlError::DuplicateTarget {
                line,
                first_line,
                twin: key.0,
                target_kind: key.1,
                target: key.2,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        SeedContractTarget, SeedDataRow, SeedField, SeedJsonlError, parse_seed_jsonl,
        render_seed_contract_jsonl, render_seed_data_jsonl,
    };

    #[test]
    fn contract_renderer_pins_deterministic_jsonl_ordering() {
        let mut deals = SeedContractTarget::new(
            "postgres",
            "relation",
            "public.deals",
            vec![
                SeedField::new("deal_id", "text", true, false),
                SeedField::new("amount", "numeric", true, false),
            ],
        );
        deals
            .payload
            .insert("primary_key".to_owned(), json!(["deal_id"]));

        let tenants = SeedContractTarget::new(
            "postgres",
            "relation",
            "public.tenants",
            vec![SeedField::new("tenant_id", "text", true, false)],
        );

        let rendered = render_seed_contract_jsonl(&[tenants, deals]).expect("render seed contract");

        assert_eq!(
            rendered,
            concat!(
                "{\"version\":\"twinning.seed-contract.v0\",\"kind\":\"target\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.deals\",\"fields\":[{\"name\":\"amount\",\"type\":\"numeric\",\"required\":true,\"nullable\":false},{\"name\":\"deal_id\",\"type\":\"text\",\"required\":true,\"nullable\":false}],\"payload\":{\"primary_key\":[\"deal_id\"]}}\n",
                "{\"version\":\"twinning.seed-contract.v0\",\"kind\":\"target\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"fields\":[{\"name\":\"tenant_id\",\"type\":\"text\",\"required\":true,\"nullable\":false}]}\n",
            )
        );
    }

    #[test]
    fn parser_accepts_contract_and_data_lines() {
        let input = concat!(
            "{\"version\":\"twinning.seed-contract.v0\",\"kind\":\"target\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"fields\":[{\"name\":\"tenant_id\",\"type\":\"text\",\"required\":true,\"nullable\":false}]}\n",
            "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"name\":\"Acme\",\"tenant_id\":\"tenant-001\"}}\n",
        );

        let parsed = parse_seed_jsonl(input).expect("parse seed JSONL");

        assert_eq!(parsed.contract_targets.len(), 1);
        assert_eq!(parsed.contract_targets[0].target, "public.tenants");
        assert_eq!(parsed.data_rows.len(), 1);
        assert_eq!(parsed.data_rows[0].row["tenant_id"], json!("tenant-001"));
    }

    #[test]
    fn empty_file_parses_to_empty_document() {
        let parsed = parse_seed_jsonl("").expect("parse empty seed JSONL");
        assert!(parsed.is_empty());
    }

    #[test]
    fn parser_rejects_malformed_or_unknown_envelopes() {
        assert!(matches!(
            parse_seed_jsonl("\n"),
            Err(SeedJsonlError::BlankLine { line: 1 })
        ));
        assert!(matches!(
            parse_seed_jsonl(
                "{\"version\":\"other\",\"kind\":\"target\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\"}\n"
            ),
            Err(SeedJsonlError::UnknownVersion { line: 1, .. })
        ));
        assert!(matches!(
            parse_seed_jsonl(
                "{\"version\":\"twinning.seed-contract.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\"}\n"
            ),
            Err(SeedJsonlError::UnexpectedKind { line: 1, .. })
        ));
        assert!(matches!(
            parse_seed_jsonl(
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\"}\n"
            ),
            Err(SeedJsonlError::MissingField {
                line: 1,
                field: "row"
            })
        ));
    }

    #[test]
    fn parser_rejects_duplicate_contract_targets_but_allows_multiple_data_rows() {
        let duplicate_contract = concat!(
            "{\"version\":\"twinning.seed-contract.v0\",\"kind\":\"target\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\"}\n",
            "{\"version\":\"twinning.seed-contract.v0\",\"kind\":\"target\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\"}\n",
        );
        assert!(matches!(
            parse_seed_jsonl(duplicate_contract),
            Err(SeedJsonlError::DuplicateTarget {
                line: 2,
                first_line: 1,
                ..
            })
        ));

        let rows = concat!(
            "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-001\"}}\n",
            "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-002\"}}\n",
        );
        let parsed = parse_seed_jsonl(rows).expect("multiple seed rows for a target");
        assert_eq!(parsed.data_rows.len(), 2);
    }

    #[test]
    fn data_renderer_preserves_row_order_and_pins_kind() {
        let first = SeedDataRow::new(
            "postgres",
            "relation",
            "public.tenants",
            [("tenant_id".to_owned(), json!("tenant-001"))].into(),
        );
        let second = SeedDataRow::new(
            "postgres",
            "relation",
            "public.tenants",
            [("tenant_id".to_owned(), json!("tenant-002"))].into(),
        );

        let rendered = render_seed_data_jsonl(&[first, second]).expect("render seed data");

        assert_eq!(
            rendered,
            concat!(
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-001\"}}\n",
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-002\"}}\n",
            )
        );
    }
}
