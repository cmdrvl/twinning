use std::collections::BTreeMap;

use thiserror::Error;

use crate::backend::base::{Backend, BackendError, BaseSnapshotBackend};
use crate::kernel::storage::TableStorage;

pub type SessionId = String;

#[derive(Debug, Clone)]
pub struct SessionOverlayManager {
    committed: BaseSnapshotBackend,
    active_writer: Option<ActiveWriter>,
}

impl SessionOverlayManager {
    pub fn new(committed: BaseSnapshotBackend) -> Self {
        Self {
            committed,
            active_writer: None,
        }
    }

    pub fn writer_session_id(&self) -> Option<&str> {
        self.active_writer
            .as_ref()
            .map(|writer| writer.session_id.as_str())
    }

    pub fn begin_write(&mut self, session_id: impl Into<SessionId>) -> Result<(), OverlayError> {
        let session_id = session_id.into();
        match &self.active_writer {
            Some(writer) if writer.session_id == session_id => Ok(()),
            Some(writer) => Err(OverlayError::WriterBusy {
                active_session: writer.session_id.clone(),
            }),
            None => {
                self.active_writer = Some(ActiveWriter::new(session_id));
                Ok(())
            }
        }
    }

    pub fn visible_table(&self, session_id: &str, table_name: &str) -> Option<&TableStorage> {
        if let Some(writer) = &self.active_writer
            && writer.session_id == session_id
            && let Some(table) = writer.overlay_tables.get(table_name)
        {
            return Some(table);
        }

        self.committed.base_table(table_name)
    }

    pub fn snapshot_visible_table(
        &self,
        session_id: &str,
        table_name: &str,
    ) -> Result<TableStorage, OverlayError> {
        if let Some(writer) = &self.active_writer
            && writer.session_id == session_id
            && let Some(table) = writer.overlay_tables.get(table_name)
        {
            return Ok(table.clone());
        }

        self.committed
            .snapshot_base_table(table_name)
            .map_err(OverlayError::from)
    }

    pub fn write_overlay_table(
        &mut self,
        session_id: &str,
        table: TableStorage,
    ) -> Result<(), OverlayError> {
        let table_name = table.table_name().to_owned();
        if self.committed.base_table(&table_name).is_none() {
            return Err(OverlayError::UnknownTable { table: table_name });
        }

        let writer = self.require_writer(session_id)?;
        writer.overlay_tables.insert(table_name, table);
        Ok(())
    }

    pub fn commit(&mut self, session_id: &str) -> Result<(), OverlayError> {
        let writer = self.take_writer(session_id)?;
        let table_names = self.committed.table_names();
        let mut committed_tables = Vec::with_capacity(table_names.len());

        for table_name in table_names {
            if let Some(table) = writer.overlay_tables.get(table_name) {
                committed_tables.push(table.clone());
            } else {
                committed_tables.push(
                    self.committed
                        .snapshot_base_table(table_name)
                        .map_err(OverlayError::from)?,
                );
            }
        }

        self.committed = BaseSnapshotBackend::new(committed_tables).map_err(OverlayError::from)?;
        Ok(())
    }

    pub fn rollback(&mut self, session_id: &str) -> Result<(), OverlayError> {
        self.take_writer(session_id)?;
        Ok(())
    }

    fn require_writer(&mut self, session_id: &str) -> Result<&mut ActiveWriter, OverlayError> {
        match self.active_writer.as_mut() {
            Some(writer) if writer.session_id == session_id => Ok(writer),
            Some(writer) => Err(OverlayError::SessionNotWriter {
                session_id: session_id.to_owned(),
                active_session: Some(writer.session_id.clone()),
            }),
            None => Err(OverlayError::SessionNotWriter {
                session_id: session_id.to_owned(),
                active_session: None,
            }),
        }
    }

    fn take_writer(&mut self, session_id: &str) -> Result<ActiveWriter, OverlayError> {
        match self.active_writer.take() {
            Some(writer) if writer.session_id == session_id => Ok(writer),
            Some(writer) => {
                let active_session = writer.session_id.clone();
                self.active_writer = Some(writer);
                Err(OverlayError::SessionNotWriter {
                    session_id: session_id.to_owned(),
                    active_session: Some(active_session),
                })
            }
            None => Err(OverlayError::SessionNotWriter {
                session_id: session_id.to_owned(),
                active_session: None,
            }),
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveWriter {
    session_id: SessionId,
    overlay_tables: BTreeMap<String, TableStorage>,
}

impl ActiveWriter {
    fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            overlay_tables: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OverlayError {
    #[error("session `{active_session}` already owns the mutable overlay")]
    WriterBusy { active_session: SessionId },
    #[error("session `{session_id}` does not own the mutable overlay")]
    SessionNotWriter {
        session_id: SessionId,
        active_session: Option<SessionId>,
    },
    #[error("overlay state has no table named `{table}`")]
    UnknownTable { table: String },
    #[error(transparent)]
    Backend(#[from] BackendError),
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::{
            base::BaseSnapshotBackend,
            overlay::{OverlayError, SessionOverlayManager},
        },
        catalog::parse_postgres_schema,
        kernel::{storage::TableStorage, value::KernelValue},
    };

    fn committed_backend() -> BaseSnapshotBackend {
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT NOT NULL
            );
            "#,
        )
        .expect("schema should parse");

        let mut deals = TableStorage::new(
            catalog
                .table("public.deals")
                .expect("deals table should exist"),
        )
        .expect("deals storage should build");
        deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("Alpha")),
            ])
            .expect("insert committed row");

        BaseSnapshotBackend::new([deals]).expect("build backend")
    }

    #[test]
    fn writer_sees_overlay_while_other_sessions_see_last_committed_state() {
        let mut overlays = SessionOverlayManager::new(committed_backend());
        overlays.begin_write("writer").expect("begin writer");

        let mut overlay_deals = overlays
            .snapshot_visible_table("writer", "public.deals")
            .expect("clone deals");
        overlay_deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("Beta")),
            ])
            .expect("insert overlay row");
        overlays
            .write_overlay_table("writer", overlay_deals)
            .expect("write overlay table");

        assert_eq!(
            overlays
                .visible_table("writer", "public.deals")
                .expect("writer visible deals")
                .row_count(),
            2
        );
        assert_eq!(
            overlays
                .visible_table("reader", "public.deals")
                .expect("reader visible deals")
                .row_count(),
            1
        );
    }

    #[test]
    fn commit_publishes_overlay_and_clears_writer_lease() {
        let mut overlays = SessionOverlayManager::new(committed_backend());
        overlays.begin_write("writer").expect("begin writer");

        let mut overlay_deals = overlays
            .snapshot_visible_table("writer", "public.deals")
            .expect("clone deals");
        overlay_deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("Beta")),
            ])
            .expect("insert overlay row");
        overlays
            .write_overlay_table("writer", overlay_deals)
            .expect("write overlay");

        overlays.commit("writer").expect("commit overlay");

        assert!(overlays.writer_session_id().is_none());
        assert_eq!(
            overlays
                .visible_table("reader", "public.deals")
                .expect("reader visible deals")
                .row_count(),
            2
        );
    }

    #[test]
    fn rollback_discards_overlay_and_one_writer_rule_stays_explicit() {
        let mut overlays = SessionOverlayManager::new(committed_backend());
        overlays.begin_write("writer").expect("begin writer");

        let busy = overlays
            .begin_write("reader")
            .expect_err("second writer should be refused");
        assert_eq!(
            busy,
            OverlayError::WriterBusy {
                active_session: String::from("writer"),
            }
        );

        let mut overlay_deals = overlays
            .snapshot_visible_table("writer", "public.deals")
            .expect("clone deals");
        overlay_deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("Beta")),
            ])
            .expect("insert overlay row");
        overlays
            .write_overlay_table("writer", overlay_deals)
            .expect("write overlay");

        overlays.rollback("writer").expect("rollback overlay");

        assert!(overlays.writer_session_id().is_none());
        assert_eq!(
            overlays
                .visible_table("reader", "public.deals")
                .expect("reader visible deals after rollback")
                .row_count(),
            1
        );
    }
}
