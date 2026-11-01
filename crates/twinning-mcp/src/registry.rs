//! In-memory twin registry, TTL validation and connection-string construction.

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::errors::ToolError;

pub const DEFAULT_MAX_TWINS: usize = 5;
pub const MAX_STOPPED_RECORDS: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwinStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Expired,
}

impl TwinStatus {
    pub fn is_live(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Stopping)
    }
}

#[derive(Debug, Clone)]
pub struct TwinRecord {
    pub id: String,
    pub state_dir: PathBuf,
    pub report_path: PathBuf,
    pub trace_path: PathBuf,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
    pub child_pid: Option<u32>,
    pub port: u16,
    pub status: TwinStatus,
    pub ttl_deadline: Instant,
}

#[derive(Debug)]
pub struct Registry {
    max_live: usize,
    records: Vec<TwinRecord>,
    issued_ids: HashSet<String>,
}

impl Registry {
    pub fn new(max_live: usize) -> Self {
        Self {
            max_live: max_live.max(1),
            records: Vec::new(),
            issued_ids: HashSet::new(),
        }
    }

    pub fn max_live_from_env(value: Option<&str>) -> Result<usize, ToolError> {
        parse_max_twins(value.unwrap_or("5"))
    }

    pub fn reserve(
        &mut self,
        state_root: &std::path::Path,
        port: u16,
        ttl: Duration,
    ) -> Result<TwinRecord, ToolError> {
        if self.live_count() >= self.max_live {
            return Err(ToolError {
                code: "E_TWIN_CAP_REACHED",
                message: format!("twin cap: at most {} twins may run", self.max_live),
                hint: "Stop a running twin and try again",
            });
        }
        let id = self.unique_id()?;
        self.issued_ids.insert(id.clone());
        let state_dir = state_root.join(&id);
        let record = TwinRecord {
            id,
            report_path: state_dir.join("report.json"),
            trace_path: state_dir.join("trace.json"),
            stdout_path: state_dir.join("stdout.json"),
            stderr_path: state_dir.join("stderr.log"),
            state_dir,
            child_pid: None,
            port,
            status: TwinStatus::Starting,
            ttl_deadline: Instant::now() + ttl,
        };
        self.records.push(record.clone());
        Ok(record)
    }

    pub fn live_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| record.status.is_live())
            .count()
    }

    pub fn records(&self) -> &[TwinRecord] {
        &self.records
    }

    pub fn get(&self, id: &str) -> Option<&TwinRecord> {
        self.records.iter().find(|record| record.id == id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut TwinRecord> {
        self.records.iter_mut().find(|record| record.id == id)
    }

    pub fn mark_stopped(&mut self, id: &str, expired: bool) -> bool {
        let Some(record) = self.get_mut(id) else {
            return false;
        };
        record.status = if expired {
            TwinStatus::Expired
        } else {
            TwinStatus::Stopped
        };
        self.evict_stopped();
        true
    }

    fn unique_id(&self) -> Result<String, ToolError> {
        for _ in 0..128 {
            let mut bytes = [0_u8; 3];
            File::open("/dev/urandom")
                .and_then(|mut random| random.read_exact(&mut bytes))
                .map_err(|error| ToolError {
                    code: "E_INTERNAL",
                    message: format!("twin id: unable to read system randomness: {error}"),
                    hint: "Run twinning-mcp doctor",
                })?;
            let id = format!("tw_{:02x}{:02x}{:02x}", bytes[0], bytes[1], bytes[2]);
            if !self.issued_ids.contains(&id) {
                return Ok(id);
            }
        }
        Err(ToolError {
            code: "E_INTERNAL",
            message: "twin id: repeated collision while generating an id".into(),
            hint: "Run twinning-mcp doctor",
        })
    }

    fn evict_stopped(&mut self) {
        let mut stopped_indices: Vec<usize> = self
            .records
            .iter()
            .enumerate()
            .filter_map(|(index, record)| (!record.status.is_live()).then_some(index))
            .collect();
        let excess = stopped_indices.len().saturating_sub(MAX_STOPPED_RECORDS);
        if excess == 0 {
            return;
        }
        stopped_indices.truncate(excess);
        for index in stopped_indices.into_iter().rev() {
            self.records.remove(index);
        }
    }
}

pub fn parse_ttl_minutes(value: &serde_json::Value) -> Result<u64, ToolError> {
    let minutes = value.as_u64().ok_or_else(|| ToolError {
        code: "E_ARGUMENT_INVALID",
        message: "ttl_minutes: must be an integer".into(),
        hint: "Set ttl_minutes to an integer from 1 through 1440",
    })?;
    if !(1..=1440).contains(&minutes) {
        return Err(ToolError {
            code: "E_ARGUMENT_INVALID",
            message: "ttl_minutes: must be from 1 through 1440".into(),
            hint: "Set ttl_minutes to an integer from 1 through 1440",
        });
    }
    Ok(minutes)
}

pub fn parse_max_twins(value: &str) -> Result<usize, ToolError> {
    let count = value.parse::<usize>().ok().filter(|count| *count > 0);
    count.ok_or_else(|| ToolError {
        code: "E_ARGUMENT_INVALID",
        message: "max_twins: must be a positive integer".into(),
        hint: "Set TWINNING_MCP_MAX_TWINS to a positive integer",
    })
}

pub fn connection_string(port: u16) -> String {
    format!("postgresql://u@127.0.0.1:{port}/db?sslmode=disable&gssencmode=disable")
}

#[cfg(test)]
mod tests {
    use super::{MAX_STOPPED_RECORDS, Registry, TwinStatus, connection_string, parse_ttl_minutes};
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn u07_ttl_range_rejects_zero_and_above_limit() {
        assert_eq!(
            "E_ARGUMENT_INVALID",
            parse_ttl_minutes(&json!(0)).unwrap_err().code
        );
        assert_eq!(1, parse_ttl_minutes(&json!(1)).unwrap());
        assert_eq!(1440, parse_ttl_minutes(&json!(1440)).unwrap());
        assert_eq!(
            "E_ARGUMENT_INVALID",
            parse_ttl_minutes(&json!(1441)).unwrap_err().code
        );
        assert_eq!(
            "E_ARGUMENT_INVALID",
            parse_ttl_minutes(&json!("1")).unwrap_err().code
        );
    }

    #[test]
    fn u08_ten_thousand_ids_are_unique_and_match_contract() {
        let mut registry = Registry::new(10_000);
        let root = std::env::temp_dir();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let record = registry
                .reserve(&root, 5432, Duration::from_secs(60))
                .unwrap();
            assert!(record.id.starts_with("tw_"));
            assert_eq!(9, record.id.len());
            assert!(
                record.id[3..]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            );
            assert!(seen.insert(record.id.clone()));
            registry.mark_stopped(&record.id, false);
        }
        assert_eq!(10_000, seen.len());
    }

    #[test]
    fn u09_registry_enforces_live_cap_and_evicts_oldest_stopped_records() {
        let mut registry = Registry::new(5);
        let root = std::env::temp_dir();
        let mut active = Vec::new();
        for _ in 0..5 {
            active.push(
                registry
                    .reserve(&root, 5432, Duration::from_secs(60))
                    .unwrap()
                    .id,
            );
        }
        assert_eq!(
            "E_TWIN_CAP_REACHED",
            registry
                .reserve(&root, 5432, Duration::from_secs(60))
                .unwrap_err()
                .code
        );
        for id in active {
            registry.mark_stopped(&id, false);
        }
        for _ in 0..MAX_STOPPED_RECORDS + 7 {
            let id = registry
                .reserve(&root, 5432, Duration::from_secs(60))
                .unwrap()
                .id;
            registry.get_mut(&id).unwrap().status = TwinStatus::Running;
            registry.mark_stopped(&id, false);
        }
        assert_eq!(MAX_STOPPED_RECORDS, registry.records().len());
        assert_eq!(
            MAX_STOPPED_RECORDS,
            registry
                .records()
                .iter()
                .filter(|r| !r.status.is_live())
                .count()
        );
    }

    #[test]
    fn u10_connection_string_has_exact_driver_flags() {
        assert_eq!(
            "postgresql://u@127.0.0.1:54321/db?sslmode=disable&gssencmode=disable",
            connection_string(54321)
        );
    }
}
