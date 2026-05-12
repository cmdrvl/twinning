use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    catalog::Catalog,
    refusal::{self, RefusalEnvelope, RefusalResult},
};

pub const CATALOG_DECLARATION_VERSION: &str = "twinning.catalog-declaration.v0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDeclaration {
    pub version: String,
    pub declaration_id: String,
    pub catalog_version: String,
    pub source_deployment_fingerprint: String,
    pub outcome_tags: Vec<String>,
    pub schema_hash: String,
    pub tables: Vec<CatalogDeclarationTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDeclarationTable {
    pub name: String,
    pub catalog_table_key: String,
    pub primary_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<CatalogDeclarationForeignKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDeclarationForeignKey {
    pub columns: Vec<String>,
    pub references_table: String,
    pub references_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDeclarationIdentity {
    pub source: String,
    pub hash: String,
    pub version: String,
    pub declaration_id: String,
    pub catalog_version: String,
    pub source_deployment_fingerprint: String,
    pub outcome_tags: Vec<String>,
    pub table_keys: BTreeMap<String, String>,
}

impl CatalogDeclaration {
    pub fn identity(self, source: String, hash: String) -> CatalogDeclarationIdentity {
        let table_keys = self
            .tables
            .into_iter()
            .map(|table| (table.name, table.catalog_table_key))
            .collect();

        CatalogDeclarationIdentity {
            source,
            hash,
            version: self.version,
            declaration_id: self.declaration_id,
            catalog_version: self.catalog_version,
            source_deployment_fingerprint: self.source_deployment_fingerprint,
            outcome_tags: self.outcome_tags,
            table_keys,
        }
    }
}

pub fn load_catalog_declaration(
    path: &Path,
    expected_schema_hash: &str,
    catalog: &Catalog,
) -> RefusalResult<CatalogDeclarationIdentity> {
    let bytes = std::fs::read(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
    let declaration_hash = sha256_prefixed(&bytes);
    let declaration: CatalogDeclaration = serde_json::from_slice(&bytes)
        .map_err(|error| Box::new(declaration_parse(path, error.to_string())))?;

    validate_declaration(path, &declaration, expected_schema_hash, catalog)?;
    Ok(declaration.identity(path.display().to_string(), declaration_hash))
}

fn validate_declaration(
    path: &Path,
    declaration: &CatalogDeclaration,
    expected_schema_hash: &str,
    catalog: &Catalog,
) -> RefusalResult<()> {
    validate_identity_fields(path, declaration)?;

    if declaration.version != CATALOG_DECLARATION_VERSION {
        return Err(Box::new(declaration_parse(
            path,
            format!(
                "unsupported declaration version `{}` (expected `{CATALOG_DECLARATION_VERSION}`)",
                declaration.version
            ),
        )));
    }

    if declaration.schema_hash != expected_schema_hash {
        return Err(Box::new(declaration_parse(
            path,
            format!(
                "declaration schema_hash `{}` does not match loaded schema `{expected_schema_hash}`",
                declaration.schema_hash
            ),
        )));
    }

    let declared_tables = declaration
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    if declared_tables.len() != declaration.tables.len() {
        return Err(Box::new(declaration_parse(
            path,
            "declaration contains duplicate table entries",
        )));
    }

    let catalog_tables = catalog
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    if declared_tables != catalog_tables {
        let missing = catalog_tables
            .difference(&declared_tables)
            .cloned()
            .collect::<Vec<_>>();
        let unexpected = declared_tables
            .difference(&catalog_tables)
            .cloned()
            .collect::<Vec<_>>();
        return Err(Box::new(declaration_parse(
            path,
            format!(
                "declaration tables must match loaded catalog exactly; missing {:?}, unexpected {:?}",
                missing, unexpected
            ),
        )));
    }

    for declared in &declaration.tables {
        if declared.catalog_table_key.is_empty() {
            return Err(Box::new(declaration_parse(
                path,
                format!(
                    "declaration table `{}` has an empty catalog_table_key",
                    declared.name
                ),
            )));
        }

        let Some(table) = catalog.table(&declared.name) else {
            continue;
        };
        let catalog_primary_key = table
            .primary_key
            .as_ref()
            .map(|key| key.columns.as_slice())
            .unwrap_or(&[]);
        if declared.primary_key.as_slice() != catalog_primary_key {
            return Err(Box::new(declaration_parse(
                path,
                format!(
                    "declaration primary key for `{}` does not match loaded catalog",
                    declared.name
                ),
            )));
        }

        let declared_foreign_keys = declared
            .foreign_keys
            .iter()
            .map(|foreign_key| {
                (
                    foreign_key.columns.clone(),
                    foreign_key.references_table.clone(),
                    foreign_key.references_columns.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        let catalog_foreign_keys = table
            .foreign_keys
            .iter()
            .map(|foreign_key| {
                (
                    foreign_key.columns.clone(),
                    foreign_key.foreign_table.clone(),
                    foreign_key.referred_columns.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        if declared_foreign_keys != catalog_foreign_keys {
            return Err(Box::new(declaration_parse(
                path,
                format!(
                    "declaration foreign keys for `{}` do not match loaded catalog",
                    declared.name
                ),
            )));
        }
    }

    Ok(())
}

fn validate_identity_fields(path: &Path, declaration: &CatalogDeclaration) -> RefusalResult<()> {
    if declaration.declaration_id.is_empty() {
        return Err(Box::new(declaration_parse(
            path,
            "declaration_id must not be empty",
        )));
    }
    if declaration.catalog_version.is_empty() {
        return Err(Box::new(declaration_parse(
            path,
            "catalog_version must not be empty",
        )));
    }
    if declaration.outcome_tags.is_empty()
        || declaration.outcome_tags.iter().any(|tag| tag.is_empty())
    {
        return Err(Box::new(declaration_parse(
            path,
            "outcome_tags must contain at least one non-empty tag",
        )));
    }
    if !is_sha256_prefixed(&declaration.source_deployment_fingerprint) {
        return Err(Box::new(declaration_parse(
            path,
            "source_deployment_fingerprint must be a sha256:<64 lowercase hex> value",
        )));
    }

    Ok(())
}

fn declaration_parse(path: &Path, message: impl Into<String>) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_DECLARATION_PARSE",
        format!(
            "Catalog declaration import failed for `{}`.",
            path.display()
        ),
        json!({ "path": path.display().to_string(), "error": message.into() }),
        None,
    )
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

fn is_sha256_prefixed(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    })
}
