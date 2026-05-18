//! Snowflake report model and session counters.

use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::catalog::SnowflakeCatalog;

pub const SNOWFLAKE_REPORT_VERSION: &str = "twinning.snowflake-report.v0";
pub const NEXT_STEP: &str = "Connect with snowflake-connector-python, JDBC, dbt, or SnowSQL to account=fakesnow host=127.0.0.1 port=9876 protocol=http";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeSessionMetrics {
    pub request_count: u64,
    pub query_count: u64,
    pub show_count: u64,
    pub ddl_count: u64,
    pub error_count: u64,
}

impl SnowflakeSessionMetrics {
    pub fn record_request(&mut self) {
        self.request_count += 1;
    }

    pub fn record_query(&mut self, sql: &str) {
        self.query_count += 1;
        let upper = sql.trim().trim_start_matches('(').to_ascii_uppercase();
        let normalized = upper.split_whitespace().collect::<Vec<_>>();
        match normalized.as_slice() {
            ["SHOW", ..] | ["DESCRIBE", ..] | ["DESC", ..] => self.show_count += 1,
            ["CREATE", ..] | ["ALTER", ..] | ["DROP", ..] | ["TRUNCATE", ..] => self.ddl_count += 1,
            _ => {}
        }
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
    }
}

pub fn snowflake_report_value(
    catalog: &SnowflakeCatalog,
    metrics: &SnowflakeSessionMetrics,
) -> JsonValue {
    let mut catalog_value = json!({
        "source": catalog.source,
        "database_name": catalog.database_name,
        "schema_name": catalog.schema_name,
        "catalog_hash": catalog.catalog_hash,
        "table_count": catalog.table_count(),
        "total_rows_materialized": catalog.row_count(),
    });

    if let Some(materialization) = &catalog.source_materialization {
        catalog_value["source_materialization"] = json!(materialization);
    }

    json!({
        "version": SNOWFLAKE_REPORT_VERSION,
        "outcome": "PASS",
        "catalog": catalog_value,
        "session": {
            "request_count": metrics.request_count,
            "query_count": metrics.query_count,
            "show_count": metrics.show_count,
            "ddl_count": metrics.ddl_count,
            "error_count": metrics.error_count,
        },
        "next_step": NEXT_STEP,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SnowflakeCatalog;

    #[test]
    fn report_includes_catalog_session_counts_and_next_step() {
        let catalog = SnowflakeCatalog::from_ddl(
            "CREATE TABLE t (id int); INSERT INTO t (id) VALUES (1), (2);",
        )
        .expect("catalog");
        let metrics = SnowflakeSessionMetrics {
            request_count: 3,
            query_count: 2,
            show_count: 1,
            ddl_count: 0,
            error_count: 1,
        };

        let report = snowflake_report_value(&catalog, &metrics);

        assert_eq!(report["version"], SNOWFLAKE_REPORT_VERSION);
        assert_eq!(report["catalog"]["source"], "schema_file");
        assert_eq!(report["catalog"]["table_count"], 1);
        assert_eq!(report["catalog"]["total_rows_materialized"], 2);
        assert_eq!(report["session"]["request_count"], 3);
        assert_eq!(report["session"]["show_count"], 1);
        assert!(
            report["next_step"]
                .as_str()
                .unwrap()
                .contains("protocol=http")
        );
    }
}
