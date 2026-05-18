//! Snowflake query-request dispatch and response encoding.

use std::{
    collections::HashMap,
    io::Read,
    sync::{Arc, MutexGuard},
};

use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
};
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use sqlparser::{
    ast::{Expr, ObjectName, Select, SelectItem, SetExpr, Statement, TableFactor},
    dialect::SnowflakeDialect,
    parser::Parser,
};
use uuid::Uuid;

use crate::{
    arrow::encode_query_result,
    catalog::{
        DEFAULT_USER_NAME, DEFAULT_VERSION, SnowflakeCatalog, SnowflakeColumn, SnowflakeTable,
        SnowflakeType, normalize_identifier, normalize_lookup_identifier,
    },
    session::{QueryResult, SnowflakeSession, SnowflakeSharedState},
    show::{
        SnowflakeResultColumn, SnowflakeResultSet, metadata_result_for_sql, strip_sql_comments,
    },
};

const STATEMENT_TYPE_SELECT: i64 = 4096;
const STATEMENT_TYPE_DML: i64 = 12288;

type JsonResponse = (StatusCode, Json<JsonValue>);

#[derive(Debug, Deserialize)]
struct QueryRequest {
    #[serde(rename = "sqlText")]
    sql_text: String,
    #[serde(rename = "asyncExec", default)]
    async_exec: bool,
    #[serde(rename = "sequenceId", default)]
    sequence_id: u64,
    #[serde(rename = "bindings", default)]
    bindings: HashMap<String, BindingValue>,
    #[serde(rename = "queryContextDTO", default)]
    query_context_dto: JsonValue,
    #[serde(rename = "parameters", default)]
    parameters: HashMap<String, JsonValue>,
}

#[derive(Debug, Deserialize)]
struct BindingValue {
    #[serde(rename = "type")]
    binding_type: String,
    value: String,
}

#[derive(Debug)]
struct QueryOutcome {
    result_set: SnowflakeResultSet,
    statement_type_id: i64,
}

#[derive(Debug)]
struct QueryError {
    object_name: String,
}

#[derive(Debug, Clone)]
struct ProjectedColumn {
    source_index: usize,
    result_column: SnowflakeResultColumn,
}

pub async fn query_request(
    State(state): State<Arc<SnowflakeSharedState>>,
    headers: HeaderMap,
    body: Bytes,
) -> JsonResponse {
    state.record_request();
    let Some(token) = extract_token(&headers).map(str::to_owned) else {
        state.record_error();
        return missing_token_response();
    };
    if let Err(response) = require_session(&state, &headers) {
        state.record_error();
        return response;
    }
    let decoded = match decode_body(&headers, body) {
        Ok(decoded) => decoded,
        Err(response) => {
            state.record_error();
            return response;
        }
    };
    let request = match parse_query_request(&decoded) {
        Ok(request) => request,
        Err(response) => {
            state.record_error();
            return response;
        }
    };

    let QueryRequest {
        sql_text,
        async_exec,
        sequence_id,
        bindings,
        query_context_dto,
        parameters,
    } = request;
    let _ignored_wire_fields = (sequence_id, query_context_dto, parameters);
    state.record_query(&sql_text);
    if let Err(error) = convert_bindings(&bindings) {
        state.record_error();
        return query_bad_request(format!("Invalid binding: {error}"));
    }

    let query_id = Uuid::new_v4().to_string();
    let response = match execute_sql(&state, &token, &sql_text, &query_id) {
        Ok(response) => response,
        Err(error) => {
            state.record_error();
            return object_not_found_response(&error.object_name);
        }
    };

    if let Err(response) = cache_response(&state, query_id.clone(), response.clone()) {
        state.record_error();
        return response;
    }

    if async_exec {
        return (
            StatusCode::ACCEPTED,
            Json(json!({
                "data": {
                    "getResultUrl": format!("/queries/{query_id}/result"),
                    "queryId": query_id,
                    "queryAbortsAfterSecs": 300
                },
                "code": "333334",
                "message": "Query execution in progress.",
                "success": true
            })),
        );
    }

    (StatusCode::OK, Json(response))
}

pub async fn query_result(
    State(state): State<Arc<SnowflakeSharedState>>,
    Path(query_id): Path<String>,
    headers: HeaderMap,
) -> JsonResponse {
    state.record_request();
    if let Err(response) = require_session(&state, &headers) {
        state.record_error();
        return response;
    }

    let results = match state.results_cache.lock() {
        Ok(results) => results,
        Err(_) => {
            state.record_error();
            return server_error("results cache lock poisoned");
        }
    };
    let Some(result) = results.get(&query_id) else {
        state.record_error();
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "data": JsonValue::Null,
                "code": "390111",
                "message": "Query result not found",
                "success": false
            })),
        );
    };

    (StatusCode::OK, Json(result.response.clone()))
}

fn execute_sql(
    state: &SnowflakeSharedState,
    token: &str,
    sql: &str,
    query_id: &str,
) -> Result<JsonValue, QueryError> {
    let normalized_sql = normalize_sql(sql);
    if normalized_sql.is_empty() {
        let session = session_for_token(state, token)?;
        return success_response(
            query_id,
            empty_result_set(),
            STATEMENT_TYPE_SELECT,
            &session,
        );
    }

    if is_use_statement(&normalized_sql) {
        let session = update_session_context(state, token, &normalized_sql)?;
        return success_response(
            query_id,
            empty_result_set(),
            STATEMENT_TYPE_SELECT,
            &session,
        );
    }

    let session = session_for_token(state, token)?;
    let outcome = route_sql(&normalized_sql, &state.catalog, &session)?;
    success_response(
        query_id,
        outcome.result_set,
        outcome.statement_type_id,
        &session,
    )
}

fn route_sql(
    sql: &str,
    catalog: &SnowflakeCatalog,
    session: &SnowflakeSession,
) -> Result<QueryOutcome, QueryError> {
    if let Some(result_set) = current_function_result(sql, catalog, session) {
        return Ok(select_outcome(result_set));
    }

    if let Some(result_set) = metadata_result_for_sql(sql, catalog) {
        return Ok(select_outcome(result_set));
    }

    let upper = sql.to_ascii_uppercase();
    let keywords = upper.split_whitespace().collect::<Vec<_>>();
    match keywords.as_slice() {
        ["SELECT", ..] => select_outcome_result(select_from_catalog(sql, catalog)),
        ["CREATE", ..] | ["ALTER", ..] | ["DROP", ..] | ["TRUNCATE", ..] => {
            Ok(select_outcome(ddl_result(sql)))
        }
        ["INSERT", ..] | ["UPDATE", ..] | ["DELETE", ..] | ["MERGE", ..] | ["COPY", ..] => {
            Ok(QueryOutcome {
                result_set: dml_result(sql),
                statement_type_id: STATEMENT_TYPE_DML,
            })
        }
        ["BEGIN", ..]
        | ["START", "TRANSACTION", ..]
        | ["COMMIT", ..]
        | ["ROLLBACK", ..]
        | ["SET", ..]
        | ["UNSET", ..] => Ok(select_outcome(status_result(
            "Statement executed successfully.",
        ))),
        ["SHOW", "FUNCTIONS", ..]
        | ["SHOW", "PROCEDURES", ..]
        | ["SHOW", "STAGES", ..]
        | ["SHOW", "USERS", ..]
        | ["SHOW", "PARAMETERS", ..] => Ok(select_outcome(empty_result_set())),
        _ => Ok(select_outcome(status_result(
            "Statement executed successfully.",
        ))),
    }
}

fn select_outcome_result(
    result: Result<SnowflakeResultSet, QueryError>,
) -> Result<QueryOutcome, QueryError> {
    result.map(select_outcome)
}

fn select_outcome(result_set: SnowflakeResultSet) -> QueryOutcome {
    QueryOutcome {
        result_set,
        statement_type_id: STATEMENT_TYPE_SELECT,
    }
}

fn select_from_catalog(
    sql: &str,
    catalog: &SnowflakeCatalog,
) -> Result<SnowflakeResultSet, QueryError> {
    let dialect = SnowflakeDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|_| QueryError {
        object_name: sql.to_owned(),
    })?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Ok(status_result("Statement executed successfully."));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(status_result("Statement executed successfully."));
    };

    if select.from.is_empty() {
        return Ok(constant_select_result(select).unwrap_or_else(empty_result_set));
    }

    let Some(table_name) = selected_table_name(select) else {
        return Ok(status_result("Statement executed successfully."));
    };
    let table = catalog.table(&table_name).ok_or_else(|| QueryError {
        object_name: normalize_lookup_identifier(&table_name),
    })?;
    let projected_columns = selected_columns(select, table)?;
    let columns = projected_columns
        .iter()
        .map(|column| column.result_column.clone())
        .collect();
    let rows = table
        .rows
        .iter()
        .map(|row| {
            projected_columns
                .iter()
                .map(|column| {
                    row.get(column.source_index)
                        .cloned()
                        .unwrap_or(JsonValue::Null)
                })
                .collect()
        })
        .collect();

    Ok(SnowflakeResultSet { columns, rows })
}

fn selected_table_name(select: &Select) -> Option<String> {
    let relation = &select.from.first()?.relation;
    match relation {
        TableFactor::Table { name, .. } => object_name_leaf(name),
        _ => None,
    }
}

fn selected_columns(
    select: &Select,
    table: &SnowflakeTable,
) -> Result<Vec<ProjectedColumn>, QueryError> {
    let mut columns = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                columns.extend(table.columns.iter().enumerate().map(|(index, column)| {
                    ProjectedColumn {
                        source_index: index,
                        result_column: result_column_from_catalog(column),
                    }
                }));
            }
            SelectItem::UnnamedExpr(expr) => {
                let column_name = expression_column_name(expr).ok_or_else(|| QueryError {
                    object_name: expr.to_string(),
                })?;
                columns.push(projected_column(table, &column_name, None)?);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let column_name = expression_column_name(expr).ok_or_else(|| QueryError {
                    object_name: expr.to_string(),
                })?;
                columns.push(projected_column(
                    table,
                    &column_name,
                    Some(normalize_identifier(alias)),
                )?);
            }
        }
    }

    Ok(columns)
}

fn expression_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(normalize_identifier(ident)),
        Expr::CompoundIdentifier(idents) => idents.last().map(normalize_identifier),
        _ => None,
    }
}

fn projected_column(
    table: &SnowflakeTable,
    column_name: &str,
    alias: Option<String>,
) -> Result<ProjectedColumn, QueryError> {
    let source_index = table.column_index(column_name).ok_or_else(|| QueryError {
        object_name: format!(
            "{}.{}",
            table.name,
            normalize_lookup_identifier(column_name)
        ),
    })?;
    let column = table.column(column_name).ok_or_else(|| QueryError {
        object_name: format!(
            "{}.{}",
            table.name,
            normalize_lookup_identifier(column_name)
        ),
    })?;
    let mut result = result_column_from_catalog(column);
    if let Some(alias) = alias {
        result.name = alias;
    }
    Ok(ProjectedColumn {
        source_index,
        result_column: result,
    })
}

fn result_column_from_catalog(column: &SnowflakeColumn) -> SnowflakeResultColumn {
    SnowflakeResultColumn {
        name: column.name.clone(),
        sf_type: column.sf_type.clone(),
        nullable: column.nullable,
    }
}

fn constant_select_result(select: &Select) -> Option<SnowflakeResultSet> {
    let mut columns = Vec::new();
    let mut row = Vec::new();

    for item in &select.projection {
        let (name, expr) = match item {
            SelectItem::UnnamedExpr(expr) => (expr.to_string(), expr),
            SelectItem::ExprWithAlias { expr, alias } => (normalize_identifier(alias), expr),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
        };
        let (sf_type, value) = scalar_expr_value(expr)?;
        columns.push(SnowflakeResultColumn {
            name,
            sf_type,
            nullable: value.is_null(),
        });
        row.push(value);
    }

    Some(SnowflakeResultSet {
        columns,
        rows: vec![row],
    })
}

fn scalar_expr_value(expr: &Expr) -> Option<(SnowflakeType, JsonValue)> {
    match expr {
        Expr::Value(sqlparser::ast::Value::Number(raw, _)) => {
            if raw.contains('.') {
                Some((SnowflakeType::Real, json!(raw.parse::<f64>().ok()?)))
            } else {
                Some((
                    SnowflakeType::Fixed {
                        precision: 38,
                        scale: 0,
                    },
                    json!(raw.parse::<i64>().ok()?),
                ))
            }
        }
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(value))
        | Expr::Value(sqlparser::ast::Value::DoubleQuotedString(value))
        | Expr::Value(sqlparser::ast::Value::NationalStringLiteral(value)) => {
            Some((default_text_type(), json!(value)))
        }
        Expr::Value(sqlparser::ast::Value::Boolean(value)) => {
            Some((SnowflakeType::Boolean, json!(value)))
        }
        Expr::Value(sqlparser::ast::Value::Null) => Some((default_text_type(), JsonValue::Null)),
        _ => None,
    }
}

fn current_function_result(
    sql: &str,
    catalog: &SnowflakeCatalog,
    session: &SnowflakeSession,
) -> Option<SnowflakeResultSet> {
    let compact = sql
        .to_ascii_uppercase()
        .split_whitespace()
        .collect::<String>();
    match compact.as_str() {
        "SELECTCURRENT_VERSION()" => Some(scalar_result(
            "CURRENT_VERSION()",
            default_text_type(),
            json!(DEFAULT_VERSION),
        )),
        "SELECTCURRENT_DATABASE()" => Some(scalar_result(
            "CURRENT_DATABASE()",
            default_text_type(),
            json!(session.database_name),
        )),
        "SELECTCURRENT_SCHEMA()" => Some(scalar_result(
            "CURRENT_SCHEMA()",
            default_text_type(),
            json!(session.schema_name),
        )),
        "SELECTCURRENT_USER()" => Some(scalar_result(
            "CURRENT_USER()",
            default_text_type(),
            json!(DEFAULT_USER_NAME),
        )),
        "SELECTCURRENT_ROLE()" => Some(scalar_result(
            "CURRENT_ROLE()",
            default_text_type(),
            json!(session.role_name),
        )),
        "SELECTCURRENT_WAREHOUSE()" => Some(scalar_result(
            "CURRENT_WAREHOUSE()",
            default_text_type(),
            json!(session.warehouse_name),
        )),
        _ if compact == "SELECTCURRENT_CATALOG()" => Some(scalar_result(
            "CURRENT_CATALOG()",
            default_text_type(),
            json!(catalog.database_name),
        )),
        _ => None,
    }
}

fn ddl_result(sql: &str) -> SnowflakeResultSet {
    let upper = sql.to_ascii_uppercase();
    if upper.starts_with("CREATE") && upper.contains(" TABLE ") {
        let table_name = object_after_keyword(sql, "TABLE").unwrap_or_else(|| "UNKNOWN".to_owned());
        return status_result(&format!("Table {table_name} successfully created."));
    }
    if upper.starts_with("DROP") && upper.contains(" TABLE ") {
        let table_name = object_after_keyword(sql, "TABLE").unwrap_or_else(|| "UNKNOWN".to_owned());
        return status_result(&format!("{table_name} successfully dropped."));
    }
    if upper.starts_with("CREATE") && upper.contains(" SCHEMA ") {
        let schema_name =
            object_after_keyword(sql, "SCHEMA").unwrap_or_else(|| "PUBLIC".to_owned());
        return status_result(&format!("Schema {schema_name} successfully created."));
    }

    status_result("Statement executed successfully.")
}

fn dml_result(sql: &str) -> SnowflakeResultSet {
    let upper = sql.to_ascii_uppercase();
    if upper.starts_with("INSERT") {
        return fixed_result("number of rows inserted", insert_row_count(sql));
    }
    if upper.starts_with("UPDATE") {
        return fixed_result("number of rows updated", 0);
    }
    if upper.starts_with("DELETE") {
        return fixed_result("number of rows deleted", 0);
    }
    fixed_result("number of rows affected", 0)
}

fn status_result(status: &str) -> SnowflakeResultSet {
    SnowflakeResultSet {
        columns: vec![SnowflakeResultColumn {
            name: "status".to_owned(),
            sf_type: default_text_type(),
            nullable: false,
        }],
        rows: vec![vec![json!(status)]],
    }
}

fn fixed_result(column_name: &str, value: i64) -> SnowflakeResultSet {
    SnowflakeResultSet {
        columns: vec![SnowflakeResultColumn {
            name: column_name.to_owned(),
            sf_type: SnowflakeType::Fixed {
                precision: 38,
                scale: 0,
            },
            nullable: false,
        }],
        rows: vec![vec![json!(value)]],
    }
}

fn scalar_result(
    column_name: &str,
    sf_type: SnowflakeType,
    value: JsonValue,
) -> SnowflakeResultSet {
    SnowflakeResultSet {
        columns: vec![SnowflakeResultColumn {
            name: column_name.to_owned(),
            sf_type,
            nullable: value.is_null(),
        }],
        rows: vec![vec![value]],
    }
}

fn empty_result_set() -> SnowflakeResultSet {
    SnowflakeResultSet {
        columns: Vec::new(),
        rows: Vec::new(),
    }
}

fn success_response(
    query_id: &str,
    result_set: SnowflakeResultSet,
    statement_type_id: i64,
    session: &SnowflakeSession,
) -> Result<JsonValue, QueryError> {
    let encoded = encode_query_result(&result_set.columns, &result_set.rows)
        .map_err(|error| QueryError { object_name: error })?;
    let total = result_set.rows.len();

    Ok(json!({
        "Data": {
            "parameters": [
                {"name": "TIMEZONE", "value": "UTC"}
            ],
            "rowtype": encoded.rowtype,
            "rowsetBase64": encoded.rowset_base64,
            "total": total,
            "returned": total,
            "queryId": query_id,
            "sqlState": "00000",
            "statementTypeId": statement_type_id,
            "version": 1,
            "queryResultFormat": "arrow",
            "finalDatabaseName": session.database_name,
            "finalSchemaName": session.schema_name,
            "finalWarehouseName": session.warehouse_name,
            "finalRoleName": session.role_name
        },
        "code": JsonValue::Null,
        "message": JsonValue::Null,
        "success": true
    }))
}

fn update_session_context(
    state: &SnowflakeSharedState,
    token: &str,
    sql: &str,
) -> Result<SnowflakeSession, QueryError> {
    let mut sessions = sessions_lock(state)?;
    let session = sessions.get_mut(token).ok_or_else(|| QueryError {
        object_name: token.to_owned(),
    })?;
    let parts = sql.split_whitespace().collect::<Vec<_>>();
    if parts.len() >= 3 {
        let target = normalize_lookup_identifier(parts[2]);
        match parts[1].to_ascii_uppercase().as_str() {
            "DATABASE" => session.database_name = target,
            "SCHEMA" => session.schema_name = target,
            "WAREHOUSE" => session.warehouse_name = target,
            "ROLE" => session.role_name = target,
            _ => {}
        }
    }
    Ok(session.clone())
}

fn session_for_token(
    state: &SnowflakeSharedState,
    token: &str,
) -> Result<SnowflakeSession, QueryError> {
    let sessions = sessions_lock(state)?;
    sessions.get(token).cloned().ok_or_else(|| QueryError {
        object_name: token.to_owned(),
    })
}

fn sessions_lock(
    state: &SnowflakeSharedState,
) -> Result<MutexGuard<'_, HashMap<String, SnowflakeSession>>, QueryError> {
    state.sessions.lock().map_err(|_| QueryError {
        object_name: "session store lock poisoned".to_owned(),
    })
}

fn require_session(
    state: &SnowflakeSharedState,
    headers: &HeaderMap,
) -> Result<SnowflakeSession, JsonResponse> {
    let Some(token) = extract_token(headers) else {
        return Err(missing_token_response());
    };
    let sessions = state
        .sessions
        .lock()
        .map_err(|_| server_error("session store lock poisoned"))?;
    sessions
        .get(token)
        .cloned()
        .ok_or_else(expired_token_response)
}

fn cache_response(
    state: &SnowflakeSharedState,
    query_id: String,
    response: JsonValue,
) -> Result<(), JsonResponse> {
    let mut results = state
        .results_cache
        .lock()
        .map_err(|_| server_error("results cache lock poisoned"))?;
    results.insert(query_id, QueryResult::success(response));
    Ok(())
}

fn parse_query_request(body: &[u8]) -> Result<QueryRequest, JsonResponse> {
    serde_json::from_slice(body).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "data": JsonValue::Null,
                "code": "390100",
                "message": format!("Invalid query request body: {error}"),
                "success": false
            })),
        )
    })
}

fn convert_bindings(bindings: &HashMap<String, BindingValue>) -> Result<Vec<JsonValue>, String> {
    let mut ordered = bindings.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|(key, _)| key.parse::<usize>().unwrap_or(usize::MAX));

    ordered
        .into_iter()
        .map(
            |(_, value)| match value.binding_type.to_ascii_uppercase().as_str() {
                "FIXED" => value
                    .value
                    .parse::<i64>()
                    .map(JsonValue::from)
                    .map_err(|error| error.to_string()),
                "REAL" => value
                    .value
                    .parse::<f64>()
                    .map(JsonValue::from)
                    .map_err(|error| error.to_string()),
                "TEXT" => Ok(json!(value.value)),
                "BOOLEAN" => Ok(json!(value.value.eq_ignore_ascii_case("true"))),
                "DATE" | "TIME" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => value
                    .value
                    .parse::<i64>()
                    .map(JsonValue::from)
                    .map_err(|error| error.to_string()),
                "BINARY" => decode_hex(&value.value).map(JsonValue::from),
                other => Err(format!("unsupported binding type `{other}`")),
            },
        )
        .collect()
}

fn normalize_sql(sql: &str) -> String {
    strip_sql_comments(sql)
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_owned()
}

fn is_use_statement(sql: &str) -> bool {
    sql.split_whitespace()
        .next()
        .map(|keyword| keyword.eq_ignore_ascii_case("USE"))
        .unwrap_or(false)
}

fn object_name_leaf(name: &ObjectName) -> Option<String> {
    name.0.last().map(normalize_identifier)
}

fn object_after_keyword(sql: &str, keyword: &str) -> Option<String> {
    let upper_parts = sql.split_whitespace().collect::<Vec<_>>();
    let keyword_index = upper_parts
        .iter()
        .position(|part| part.eq_ignore_ascii_case(keyword))?;
    let mut index = keyword_index + 1;
    while matches!(
        upper_parts.get(index).map(|part| part.to_ascii_uppercase()),
        Some(token) if token == "IF" || token == "NOT" || token == "EXISTS"
    ) {
        index += 1;
    }
    upper_parts
        .get(index)
        .map(|value| value.trim_matches(|ch| ch == '(' || ch == ';' || ch == ','))
        .map(normalize_lookup_identifier)
}

fn insert_row_count(sql: &str) -> i64 {
    let upper = sql.to_ascii_uppercase();
    let Some(values_index) = upper.find("VALUES") else {
        return 0;
    };
    let mut count = 0_i64;
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut previous = '\0';
    for ch in sql[values_index + "VALUES".len()..].chars() {
        if ch == '\'' && previous != '\\' {
            in_string = !in_string;
        } else if !in_string && ch == '(' {
            if depth == 0 {
                count += 1;
            }
            depth += 1;
        } else if !in_string && ch == ')' {
            depth -= 1;
        }
        previous = ch;
    }
    count
}

fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    let value = value.trim();
    if !value.len().is_multiple_of(2) {
        return Err(format!("hex string `{value}` has odd length"));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|error| format!("hex string `{value}` did not parse: {error}"))
        })
        .collect()
}

fn extract_token(headers: &HeaderMap) -> Option<&str> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    auth.strip_prefix("Snowflake Token=\"")?.strip_suffix('"')
}

fn decode_body(headers: &HeaderMap, body: Bytes) -> Result<Vec<u8>, JsonResponse> {
    let gzip = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("gzip"))
        .unwrap_or(false);

    if !gzip {
        return Ok(body.to_vec());
    }

    let mut decoder = GzDecoder::new(body.as_ref());
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "data": JsonValue::Null,
                "code": "390100",
                "message": format!("Invalid gzip request body: {error}"),
                "success": false
            })),
        )
    })?;
    Ok(decompressed)
}

fn missing_token_response() -> JsonResponse {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "data": JsonValue::Null,
            "code": "390101",
            "message": "No token provided",
            "success": false
        })),
    )
}

fn expired_token_response() -> JsonResponse {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "data": JsonValue::Null,
            "code": "390104",
            "message": "Token expired",
            "success": false
        })),
    )
}

fn query_bad_request(message: String) -> JsonResponse {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "data": JsonValue::Null,
            "code": "390100",
            "message": message,
            "success": false
        })),
    )
}

fn object_not_found_response(name: &str) -> JsonResponse {
    let object_name = normalize_lookup_identifier(name);
    (
        StatusCode::OK,
        Json(json!({
            "data": {
                "errorCode": "002003",
                "sqlState": "42S02"
            },
            "code": "002003",
            "message": format!("Object '{object_name}' does not exist or not authorized."),
            "success": false
        })),
    )
}

fn server_error(message: &str) -> JsonResponse {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "data": JsonValue::Null,
            "code": "390100",
            "message": message,
            "success": false
        })),
    )
}

fn default_text_type() -> SnowflakeType {
    SnowflakeType::Text {
        char_length: 16_777_216,
        byte_length: 16_777_216,
    }
}

#[cfg(test)]
mod tests {
    use arrow_ipc::reader::StreamReader;
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

    use crate::catalog::{DEFAULT_ROLE_NAME, DEFAULT_WAREHOUSE_NAME};

    use super::*;

    #[test]
    fn routes_show_tables_to_arrow_exec_response_with_uppercase_data_key() {
        let catalog = SnowflakeCatalog::from_ddl("CREATE TABLE deals (id int, name varchar);")
            .expect("catalog");
        let state = SnowflakeSharedState::new(catalog);
        let token = insert_session(&state);

        let response =
            execute_sql(&state, &token, "show tables", "query-1").expect("query response");

        assert!(response.get("Data").is_some());
        assert!(response.get("data").is_none());
        assert_eq!(true, response["success"]);
        assert_eq!("arrow", response["Data"]["queryResultFormat"]);
        assert_eq!(1, response["Data"]["total"]);
        assert_eq!(
            response["Data"]["rowtype"][1]["name"],
            JsonValue::String("name".to_owned())
        );
        assert_arrow_rows(&response, 1);
    }

    #[test]
    fn select_from_catalog_table_returns_projected_rowtype_and_empty_arrow_batch() {
        let catalog = SnowflakeCatalog::from_ddl("CREATE TABLE deals (id int, name varchar);")
            .expect("catalog");
        let state = SnowflakeSharedState::new(catalog);
        let token = insert_session(&state);

        let response =
            execute_sql(&state, &token, "select id, name from deals", "query-2").unwrap();

        assert_eq!(0, response["Data"]["total"]);
        assert_eq!("ID", response["Data"]["rowtype"][0]["name"]);
        assert_eq!("NAME", response["Data"]["rowtype"][1]["name"]);
        assert_arrow_rows(&response, 0);
    }

    #[test]
    fn select_from_catalog_table_projects_materialized_rows() {
        let catalog = SnowflakeCatalog::from_ddl(
            "CREATE TABLE deals (id int, name varchar); INSERT INTO deals (id, name) VALUES (1, 'Alpha'), (2, 'Beta');",
        )
        .expect("catalog");
        let state = SnowflakeSharedState::new(catalog);
        let token = insert_session(&state);

        let response = execute_sql(&state, &token, "select name from deals", "query-rows").unwrap();

        assert_eq!(2, response["Data"]["total"]);
        assert_eq!("NAME", response["Data"]["rowtype"][0]["name"]);
        assert_arrow_rows(&response, 2);
    }

    #[test]
    fn use_database_updates_session_final_context_and_current_database() {
        let state = SnowflakeSharedState::new(SnowflakeCatalog::empty());
        let token = insert_session(&state);

        let use_response =
            execute_sql(&state, &token, "use database analytics", "query-3").expect("use response");
        assert_eq!("ANALYTICS", use_response["Data"]["finalDatabaseName"]);

        let current = execute_sql(&state, &token, "select current_database()", "query-4")
            .expect("current response");
        assert_eq!("ANALYTICS", current["Data"]["finalDatabaseName"]);
        assert_arrow_rows(&current, 1);
    }

    #[test]
    fn unknown_select_table_maps_to_snowflake_error() {
        let state = SnowflakeSharedState::new(SnowflakeCatalog::empty());
        let token = insert_session(&state);

        let error = execute_sql(&state, &token, "select * from missing_table", "query-5")
            .expect_err("unknown table");

        assert_eq!("MISSING_TABLE", error.object_name);
    }

    #[test]
    fn ddl_and_dml_stub_status_rows_match_snowflake_shapes() {
        let state = SnowflakeSharedState::new(SnowflakeCatalog::empty());
        let token = insert_session(&state);

        let create = execute_sql(&state, &token, "create table foo (id int)", "query-6").unwrap();
        assert_eq!("status", create["Data"]["rowtype"][0]["name"]);
        assert_arrow_rows(&create, 1);

        let insert =
            execute_sql(&state, &token, "insert into foo values (1), (2)", "query-7").unwrap();
        assert_eq!(12288, insert["Data"]["statementTypeId"]);
        assert_eq!(
            "number of rows inserted",
            insert["Data"]["rowtype"][0]["name"]
        );
        assert_arrow_rows(&insert, 1);
    }

    fn insert_session(state: &SnowflakeSharedState) -> String {
        let token = "token".to_owned();
        state.sessions.lock().unwrap().insert(
            token.clone(),
            SnowflakeSession {
                token: token.clone(),
                master_token: "master".to_owned(),
                session_id: 1,
                database_name: "TWINDB".to_owned(),
                schema_name: "PUBLIC".to_owned(),
                warehouse_name: DEFAULT_WAREHOUSE_NAME.to_owned(),
                role_name: DEFAULT_ROLE_NAME.to_owned(),
                created_at: std::time::Instant::now(),
            },
        );
        token
    }

    fn assert_arrow_rows(response: &JsonValue, expected: usize) {
        let encoded = response["Data"]["rowsetBase64"]
            .as_str()
            .expect("rowsetBase64");
        assert!(!encoded.is_empty());
        let raw = BASE64.decode(encoded).expect("base64 decodes");
        let mut reader = StreamReader::try_new(std::io::Cursor::new(raw), None).expect("reader");
        let batch = reader
            .next()
            .expect("one batch")
            .expect("batch reads from stream");
        assert_eq!(expected, batch.num_rows());
    }
}
