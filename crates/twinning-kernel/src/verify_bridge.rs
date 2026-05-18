use std::collections::BTreeMap;

use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use verify_engine::{
    Relation,
    embedded::{EmbeddedBinding, EmbeddedBindings, EmbeddedExecutor},
};

use crate::{
    backend::SessionOverlayManager,
    catalog::Catalog,
    kernel::storage::TableStorage,
    refusal,
    refusal::RefusalResult,
    report::LiveVerifyArtifact,
    snapshot::{SnapshotRelations, SnapshotRow, relations_from_committed_tables},
};

const VERIFY_BINDER_SESSION_ID: &str = "__twinning_verify__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyRelationBindings {
    relations: SnapshotRelations,
}

impl VerifyRelationBindings {
    pub fn relation(&self, relation_name: &str) -> Option<&Vec<SnapshotRow>> {
        self.relations.get(relation_name)
    }

    pub fn relations(&self) -> &SnapshotRelations {
        &self.relations
    }
}

pub fn bind_committed_relations(
    catalog: &Catalog,
    committed_tables: impl IntoIterator<Item = TableStorage>,
) -> RefusalResult<VerifyRelationBindings> {
    Ok(VerifyRelationBindings {
        relations: relations_from_committed_tables(catalog, committed_tables)?,
    })
}

pub fn bind_overlay_committed_relations(
    catalog: &Catalog,
    overlays: &SessionOverlayManager,
) -> RefusalResult<VerifyRelationBindings> {
    let committed_tables = catalog
        .tables
        .iter()
        .map(|table| {
            overlays
                .snapshot_visible_table(VERIFY_BINDER_SESSION_ID, &table.name)
                .map_err(|error| {
                    Box::new(refusal::serialization(format!(
                        "failed to snapshot committed verify relation `{}`: {error}",
                        table.name
                    )))
                })
        })
        .collect::<RefusalResult<Vec<_>>>()?;

    bind_committed_relations(catalog, committed_tables)
}

pub fn execute_embedded_verify(
    artifact: &LiveVerifyArtifact,
    bindings: &VerifyRelationBindings,
) -> RefusalResult<JsonValue> {
    let mut embedded_bindings = EmbeddedBindings::new();

    for binding in &artifact.constraint_set.bindings {
        let Some(rows) = bindings.relation(&binding.name) else {
            continue;
        };

        let relation_rows = rows
            .iter()
            .map(snapshot_row_to_verify_row)
            .collect::<RefusalResult<Vec<_>>>()?;
        let content_hash = relation_content_hash(&relation_rows)?;
        embedded_bindings.insert(
            binding.name.clone(),
            EmbeddedBinding::new(
                binding.name.clone(),
                content_hash,
                Relation::new(binding.key_fields.clone(), relation_rows),
            ),
        );
    }

    serde_json::to_value(EmbeddedExecutor::evaluate(
        &artifact.constraint_set,
        artifact.report.hash.clone(),
        &embedded_bindings,
    ))
    .map_err(|error| Box::new(refusal::serialization(error.to_string())))
}

fn snapshot_row_to_verify_row(row: &SnapshotRow) -> RefusalResult<BTreeMap<String, JsonValue>> {
    row.iter()
        .map(|(column, value)| Ok((column.clone(), kernel_json_to_verify_value(value)?)))
        .collect()
}

fn kernel_json_to_verify_value(value: &JsonValue) -> RefusalResult<JsonValue> {
    let Some(kind) = value
        .as_object()
        .and_then(|object| object.get("kind"))
        .and_then(JsonValue::as_str)
    else {
        return Ok(value.clone());
    };

    let object = value
        .as_object()
        .expect("kind-bearing kernel values should be objects");
    let raw_value = object.get("value");

    match kind {
        "null" => Ok(JsonValue::Null),
        "bigint" | "integer" | "smallint" | "float" | "boolean" | "timestamp" | "date"
        | "bytes" | "json" | "text" => raw_value.cloned().ok_or_else(|| {
            Box::new(refusal::serialization(format!(
                "kernel value `{kind}` is missing `value`"
            )))
        }),
        "numeric" => match raw_value.and_then(JsonValue::as_str) {
            Some(number_text) => match serde_json::from_str::<JsonValue>(number_text) {
                Ok(JsonValue::Number(number)) => Ok(JsonValue::Number(number)),
                Ok(other) => Ok(other),
                Err(_) => Ok(JsonValue::String(number_text.to_owned())),
            },
            None => Err(Box::new(refusal::serialization(
                "kernel value `numeric` is missing string `value`",
            ))),
        },
        "array" => raw_value
            .and_then(JsonValue::as_array)
            .ok_or_else(|| {
                Box::new(refusal::serialization(
                    "kernel value `array` is missing array `value`",
                ))
            })?
            .iter()
            .map(kernel_json_to_verify_value)
            .collect::<RefusalResult<Vec<_>>>()
            .map(JsonValue::Array),
        other => Err(Box::new(refusal::serialization(format!(
            "kernel value kind `{other}` cannot be converted into embedded verify input"
        )))),
    }
}

fn relation_content_hash(rows: &[BTreeMap<String, JsonValue>]) -> RefusalResult<String> {
    let bytes = serde_json::to_vec(rows)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
    let mut digest = Sha256::new();
    digest.update(bytes);
    Ok(format!("sha256:{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        backend::{BaseSnapshotBackend, SessionOverlayManager},
        catalog::parse_postgres_schema,
        kernel::{storage::TableStorage, value::KernelValue},
        report::LiveVerifyArtifact,
    };

    use super::{
        bind_committed_relations, bind_overlay_committed_relations, execute_embedded_verify,
    };

    fn seeded_catalog() -> crate::catalog::Catalog {
        parse_postgres_schema(
            r#"
            CREATE TABLE public.tenants (
                tenant_id TEXT PRIMARY KEY,
                tenant_name TEXT NOT NULL
            );

            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                deal_name TEXT NOT NULL,
                CONSTRAINT deals_tenant_fk
                    FOREIGN KEY (tenant_id) REFERENCES public.tenants (tenant_id)
            );
            "#,
        )
        .expect("schema should parse")
    }

    fn seeded_committed_tables(catalog: &crate::catalog::Catalog) -> Vec<TableStorage> {
        let mut tenants = TableStorage::new(
            catalog
                .table("public.tenants")
                .expect("tenants table should exist"),
        )
        .expect("tenants storage should build");
        tenants
            .insert_row(vec![
                KernelValue::Text(String::from("tenant-a")),
                KernelValue::Text(String::from("Tenant A")),
            ])
            .expect("insert tenant");

        let mut deals = TableStorage::new(
            catalog
                .table("public.deals")
                .expect("deals table should exist"),
        )
        .expect("deals storage should build");
        deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("tenant-a")),
                KernelValue::Text(String::from("Alpha")),
            ])
            .expect("insert deal");

        vec![deals, tenants]
    }

    #[test]
    fn committed_relations_bind_under_canonical_catalog_names() {
        let catalog = seeded_catalog();
        let bindings =
            bind_committed_relations(&catalog, seeded_committed_tables(&catalog)).expect("bind");

        assert_eq!(
            bindings
                .relations()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["public.deals", "public.tenants"]
        );
        assert_eq!(
            bindings
                .relation("public.deals")
                .expect("deals relation should exist"),
            &vec![
                json!({
                    "deal_id": {"kind": "text", "value": "deal-1"},
                    "tenant_id": {"kind": "text", "value": "tenant-a"},
                    "deal_name": {"kind": "text", "value": "Alpha"}
                })
                .as_object()
                .expect("row object")
                .clone()
                .into_iter()
                .collect()
            ]
        );
        assert_eq!(
            bindings
                .relation("public.tenants")
                .expect("tenants relation should exist"),
            &vec![
                json!({
                    "tenant_id": {"kind": "text", "value": "tenant-a"},
                    "tenant_name": {"kind": "text", "value": "Tenant A"}
                })
                .as_object()
                .expect("row object")
                .clone()
                .into_iter()
                .collect()
            ]
        );
    }

    #[test]
    fn overlay_binding_uses_last_committed_state_only() {
        let catalog = seeded_catalog();
        let committed_tables = seeded_committed_tables(&catalog);
        let backend = BaseSnapshotBackend::new(committed_tables).expect("backend should build");
        let mut overlays = SessionOverlayManager::new(backend);

        overlays.begin_write("writer").expect("begin writer");
        let mut writer_deals = overlays
            .snapshot_visible_table("writer", "public.deals")
            .expect("clone writer deals");
        writer_deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("tenant-a")),
                KernelValue::Text(String::from("Beta")),
            ])
            .expect("insert transient row");
        overlays
            .write_overlay_table("writer", writer_deals)
            .expect("persist transient row");

        let bindings =
            bind_overlay_committed_relations(&catalog, &overlays).expect("bind committed view");

        assert_eq!(
            bindings
                .relation("public.deals")
                .expect("deals relation should exist")
                .len(),
            1
        );
        assert_eq!(
            bindings
                .relation("public.deals")
                .expect("deals relation should exist")[0]["deal_name"],
            json!({"kind": "text", "value": "Alpha"})
        );
    }

    #[test]
    fn embedded_verify_emits_schema_correct_pass_report() {
        let catalog = seeded_catalog();
        let bindings =
            bind_committed_relations(&catalog, seeded_committed_tables(&catalog)).expect("bind");
        let artifact = LiveVerifyArtifact::parse(
            "constraints.verify.json",
            br#"{
              "version": "verify.constraint.v1",
              "constraint_set_id": "fixtures.public.deals",
              "bindings": [
                { "name": "public.deals", "kind": "relation", "key_fields": ["deal_id"] }
              ],
              "rules": [
                {
                  "id": "DEAL_ID_PRESENT",
                  "severity": "error",
                  "portability": "portable",
                  "check": { "op": "not_null", "binding": "public.deals", "columns": ["deal_id"] }
                },
                {
                  "id": "AT_LEAST_ONE_DEAL",
                  "severity": "warn",
                  "portability": "portable",
                  "check": { "op": "row_count", "binding": "public.deals", "compare": { "gte": 1 } }
                }
              ]
            }"#,
        )
        .expect("parse verify artifact");

        let verify = execute_embedded_verify(&artifact, &bindings).expect("execute verify");

        assert_eq!(verify["tool"], "verify");
        assert_eq!(verify["version"], "verify.report.v1");
        assert_eq!(verify["execution_mode"], "embedded");
        assert_eq!(verify["outcome"], "PASS");
        assert_eq!(verify["constraint_set_id"], "fixtures.public.deals");
        assert_eq!(verify["summary"]["total_rules"], 2);
        assert_eq!(verify["summary"]["passed_rules"], 2);
        assert_eq!(verify["summary"]["failed_rules"], 0);
        assert_eq!(verify["policy_signals"]["severity_band"], "CLEAN");
        assert_eq!(verify["bindings"]["public.deals"]["kind"], "relation");
        assert_eq!(verify["bindings"]["public.deals"]["source"], "public.deals");
        assert!(verify["bindings"]["public.deals"]["content_hash"].is_string());
        assert!(verify["refusal"].is_null());
        assert_eq!(
            verify["results"].as_array().expect("results array").len(),
            2
        );
    }

    #[test]
    fn embedded_verify_localizes_not_null_failures_with_relation_keys() {
        let catalog = seeded_catalog();
        let mut committed_tables = seeded_committed_tables(&catalog);
        let deals = committed_tables
            .iter_mut()
            .find(|table| table.table_name() == "public.deals")
            .expect("deals table");
        deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("tenant-a")),
                KernelValue::Null,
            ])
            .expect("insert invalid deal");
        let bindings = bind_committed_relations(&catalog, committed_tables).expect("bind");
        let artifact = LiveVerifyArtifact::parse(
            "constraints.verify.json",
            br#"{
              "version": "verify.constraint.v1",
              "constraint_set_id": "fixtures.public.deals.not_null",
              "bindings": [
                { "name": "public.deals", "kind": "relation", "key_fields": ["deal_id"] }
              ],
              "rules": [
                {
                  "id": "DEAL_NAME_PRESENT",
                  "severity": "error",
                  "portability": "portable",
                  "check": { "op": "not_null", "binding": "public.deals", "columns": ["deal_name"] }
                }
              ]
            }"#,
        )
        .expect("parse verify artifact");

        let verify = execute_embedded_verify(&artifact, &bindings).expect("execute verify");

        assert_eq!(verify["outcome"], "FAIL");
        assert_eq!(verify["summary"]["failed_rules"], 1);
        assert_eq!(verify["policy_signals"]["severity_band"], "ERROR_PRESENT");
        assert_eq!(verify["results"][0]["rule_id"], "DEAL_NAME_PRESENT");
        assert_eq!(verify["results"][0]["status"], "fail");
        assert_eq!(verify["results"][0]["violation_count"], 1);
        assert_eq!(
            verify["results"][0]["affected"][0]["binding"],
            "public.deals"
        );
        assert_eq!(
            verify["results"][0]["affected"][0]["key"]["deal_id"],
            "deal-2"
        );
        assert_eq!(verify["results"][0]["affected"][0]["field"], "deal_name");
        assert!(verify["results"][0]["affected"][0]["value"].is_null());
    }
}
