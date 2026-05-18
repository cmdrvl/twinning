use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    catalog::SnowflakeCatalog,
    report::{SnowflakeSessionMetrics, snowflake_report_value},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct SnowflakeSession {
    pub token: String,
    pub master_token: String,
    pub session_id: i64,
    pub database_name: String,
    pub schema_name: String,
    pub warehouse_name: String,
    pub role_name: String,
    pub created_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResult {
    pub status: String,
    pub response: serde_json::Value,
}

impl QueryResult {
    pub fn success(response: serde_json::Value) -> Self {
        Self {
            status: "SUCCESS".to_owned(),
            response,
        }
    }
}

#[derive(Debug)]
pub struct SnowflakeSharedState {
    pub catalog: Arc<SnowflakeCatalog>,
    pub sessions: Arc<Mutex<HashMap<String, SnowflakeSession>>>,
    pub results_cache: Arc<Mutex<HashMap<String, QueryResult>>>,
    pub metrics: Arc<Mutex<SnowflakeSessionMetrics>>,
}

impl SnowflakeSharedState {
    pub fn new(catalog: SnowflakeCatalog) -> Self {
        Self {
            catalog: Arc::new(catalog),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            results_cache: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(SnowflakeSessionMetrics::default())),
        }
    }

    pub fn report_value(&self) -> serde_json::Value {
        let metrics = self
            .metrics
            .lock()
            .map(|metrics| metrics.clone())
            .unwrap_or_default();
        snowflake_report_value(&self.catalog, &metrics)
    }

    pub fn record_request(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.record_request();
        }
    }

    pub fn record_query(&self, sql: &str) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.record_query(sql);
        }
    }

    pub fn record_error(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.record_error();
        }
    }
}
