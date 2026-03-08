use std::{collections::BTreeMap, path::Path};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    catalog::Catalog, cli::Engine, refusal, refusal::RefusalResult, report::VerifyArtifactReport,
};

pub const SNAPSHOT_VERSION: &str = "twinning.snapshot.v0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinSnapshot {
    pub version: String,
    pub snapshot_hash: String,
    pub created_at: String,
    pub engine: Engine,
    pub mode: String,
    pub schema_source: String,
    pub schema_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_snapshot_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify_artifact: Option<VerifyArtifactReport>,
    pub catalog: Catalog,
    pub table_rows: BTreeMap<String, u64>,
}

impl TwinSnapshot {
    pub fn new(
        engine: Engine,
        schema_source: String,
        schema_hash: String,
        base_snapshot_hash: Option<String>,
        verify_artifact: Option<VerifyArtifactReport>,
        catalog: Catalog,
    ) -> RefusalResult<Self> {
        let table_rows = catalog
            .tables
            .iter()
            .map(|table| (table.name.clone(), 0))
            .collect();

        let mut snapshot = Self {
            version: SNAPSHOT_VERSION.to_owned(),
            snapshot_hash: String::new(),
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            engine,
            mode: "catalog_only".to_owned(),
            schema_source,
            schema_hash,
            base_snapshot_hash,
            verify_artifact,
            catalog,
            table_rows,
        };
        snapshot.snapshot_hash = snapshot.compute_hash()?;
        Ok(snapshot)
    }

    pub fn compute_hash(&self) -> RefusalResult<String> {
        let mut clone = self.clone();
        clone.snapshot_hash.clear();
        clone.created_at.clear();
        let bytes = serde_json::to_vec(&clone)
            .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
        let mut digest = Sha256::new();
        digest.update(bytes);
        Ok(format!("sha256:{:x}", digest.finalize()))
    }
}

pub fn write_snapshot(path: &Path, snapshot: &TwinSnapshot) -> RefusalResult<String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
    }

    let rendered = serde_json::to_string_pretty(snapshot)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
    std::fs::write(path, format!("{rendered}\n"))
        .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
    Ok(snapshot.snapshot_hash.clone())
}

pub fn read_snapshot(path: &Path) -> RefusalResult<TwinSnapshot> {
    let raw =
        std::fs::read_to_string(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
    let snapshot: TwinSnapshot = serde_json::from_str(&raw)
        .map_err(|error| Box::new(refusal::snapshot_verify(path, error.to_string())))?;

    if snapshot.version != SNAPSHOT_VERSION {
        return Err(Box::new(refusal::snapshot_verify(
            path,
            format!(
                "unsupported snapshot version `{}` (expected `{SNAPSHOT_VERSION}`)",
                snapshot.version
            ),
        )));
    }

    let computed_hash = snapshot.compute_hash()?;
    if computed_hash != snapshot.snapshot_hash {
        return Err(Box::new(refusal::snapshot_verify(
            path,
            format!(
                "snapshot hash mismatch: expected {}, computed {}",
                snapshot.snapshot_hash, computed_hash
            ),
        )));
    }

    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use crate::{
        catalog::{Catalog, TableCatalog},
        cli::Engine,
    };

    use super::{TwinSnapshot, read_snapshot, write_snapshot};

    #[test]
    fn snapshot_round_trips_and_verifies_hash() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("bootstrap.twin");

        let catalog = Catalog {
            dialect: "postgres".to_owned(),
            tables: vec![TableCatalog {
                name: "public.deals".to_owned(),
                columns: Vec::new(),
                primary_key: None,
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                checks: Vec::new(),
                indexes: Vec::new(),
            }],
            table_count: 1,
            column_count: 0,
            index_count: 0,
            constraint_count: 0,
        };

        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            None,
            None,
            catalog,
        )
        .expect("snapshot");

        let hash = write_snapshot(&path, &snapshot).expect("write snapshot");
        let restored = read_snapshot(&path).expect("read snapshot");

        assert_eq!(hash, restored.snapshot_hash);
        assert_eq!(
            restored.table_rows,
            BTreeMap::from([(String::from("public.deals"), 0)])
        );
    }

    #[test]
    fn snapshot_hash_ignores_created_at_but_includes_base_snapshot_hash() {
        let catalog = Catalog {
            dialect: "postgres".to_owned(),
            tables: Vec::new(),
            table_count: 0,
            column_count: 0,
            index_count: 0,
            constraint_count: 0,
        };

        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            Some("sha256:parent".to_owned()),
            None,
            catalog,
        )
        .expect("snapshot");

        let mut changed_timestamp = snapshot.clone();
        changed_timestamp.created_at = "2030-01-01T00:00:00Z".to_owned();
        assert_eq!(
            snapshot.compute_hash().expect("hash"),
            changed_timestamp.compute_hash().expect("hash")
        );

        let mut changed_parent = snapshot;
        changed_parent.base_snapshot_hash = Some("sha256:other".to_owned());
        assert_ne!(
            changed_parent.compute_hash().expect("hash"),
            changed_timestamp.compute_hash().expect("hash")
        );
    }
}
