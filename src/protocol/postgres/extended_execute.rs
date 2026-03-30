use crate::{
    backend::Backend,
    catalog::{Catalog, ColumnCatalog},
    ir::{
        AggregateKind, MutationKind, Operation, ReadOp, RefusalOp, RefusalScope,
        normalize_mutation_sql, normalize_read_sql,
    },
    kernel::{mutation::execute_insert, read::execute_read},
    result::{AckResult, KernelResult, RefusalResult, ResultTag},
};

use super::{
    catalog::MetadataQuery,
    extended_parse::{ExtendedParseState, PortalState, PreparedStatementState},
    frames::{ResultFrameMetadata, encode_kernel_result_frames},
    session::{SessionCycle, SessionLoop},
};

const BOOL_OID: u32 = 16;
const BYTEA_OID: u32 = 17;
const INT2_OID: u32 = 21;
const INT4_OID: u32 = 23;
const INT8_OID: u32 = 20;
const TEXT_OID: u32 = 25;
const JSON_OID: u32 = 114;
const FLOAT8_OID: u32 = 701;
const NUMERIC_OID: u32 = 1700;
const DATE_OID: u32 = 1082;
const TIMESTAMP_OID: u32 = 1114;
const TEXT_ARRAY_OID: u32 = 1009;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeTarget {
    Statement(String),
    Portal(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteRequest {
    pub portal_name: String,
    pub max_rows: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ExtendedExecuteState {
    pending_result: Option<PendingResult>,
}

#[derive(Debug, Clone)]
struct PendingResult {
    result: KernelResult,
    returning_columns: Vec<String>,
}

impl ExtendedExecuteState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn process_describe(
        &self,
        catalog: &Catalog,
        parse_state: &ExtendedParseState,
        target: DescribeTarget,
    ) -> Vec<Vec<u8>> {
        match self.describe(catalog, parse_state, target) {
            Ok(frames) => frames,
            Err(refusal) => encode_result_frames(&KernelResult::Refusal(refusal), &[]),
        }
    }

    pub fn process_execute<B: Backend>(
        &mut self,
        catalog: &Catalog,
        backend: &mut B,
        parse_state: &ExtendedParseState,
        request: ExecuteRequest,
    ) -> Vec<Vec<u8>> {
        let pending = match self.execute(catalog, backend, parse_state, request) {
            Ok(pending) => pending,
            Err(refusal) => PendingResult {
                result: KernelResult::Refusal(refusal),
                returning_columns: Vec::new(),
            },
        };

        let frames = encode_result_frames(&pending.result, &pending.returning_columns);
        self.pending_result = Some(pending);
        frames
    }

    pub fn process_sync(&mut self, session: &mut SessionLoop) -> SessionCycle {
        if let Some(pending) = self.pending_result.take() {
            let metadata = ResultFrameMetadata {
                returning_columns: (!pending.returning_columns.is_empty())
                    .then_some(pending.returning_columns.as_slice()),
            };
            let cycle = session.process_result(&pending.result, metadata);
            return SessionCycle {
                frames: vec![cycle.frames.last().expect("ready frame").clone()],
                transaction_status: cycle.transaction_status,
            };
        }

        session.process_result(
            &KernelResult::Ack(AckResult {
                tag: ResultTag::Sync,
                rows_affected: 0,
            }),
            ResultFrameMetadata::default(),
        )
    }

    fn describe(
        &self,
        catalog: &Catalog,
        parse_state: &ExtendedParseState,
        target: DescribeTarget,
    ) -> Result<Vec<Vec<u8>>, RefusalResult> {
        match target {
            DescribeTarget::Statement(statement_name) => {
                let prepared = parse_state
                    .prepared_statement(statement_name.as_str())
                    .ok_or_else(|| missing_statement_refusal(&statement_name))?;
                if let Some(metadata_query) = prepared.metadata_query {
                    let mut frames = vec![parameter_description_frame(prepared)];
                    frames.push(row_description_frame(&metadata_field_descriptors(
                        metadata_query,
                    )));
                    return Ok(frames);
                }
                let operation = describe_operation_from_prepared(catalog, prepared)?;
                let mut frames = vec![parameter_description_frame(prepared)];
                frames.extend(match operation {
                    Operation::Mutation(mutation) => {
                        describe_mutation_frames(catalog, &mutation.table, &mutation.returning)?
                    }
                    Operation::Read(read) => describe_read_frames(catalog, &read)?,
                    Operation::Refusal(refusal) => {
                        return Err(operation_refusal_result(
                            refusal,
                            "describe",
                            Some(("statement_name", statement_name)),
                        ));
                    }
                    Operation::Session(_) | Operation::Prepare(_) => {
                        return Err(unsupported_execute_refusal(
                            "describe_statement_kind",
                            &[("statement_name", statement_name)],
                        ));
                    }
                });
                Ok(frames)
            }
            DescribeTarget::Portal(portal_name) => {
                let portal = parse_state
                    .portal(portal_name.as_str())
                    .ok_or_else(|| missing_portal_refusal(&portal_name))?;
                if let Some(metadata_query) = portal.metadata_query {
                    return Ok(vec![row_description_frame(&metadata_field_descriptors(
                        metadata_query,
                    ))]);
                }
                match normalized_operation_from_portal(catalog, portal)? {
                    Operation::Mutation(mutation) => {
                        describe_mutation_frames(catalog, &mutation.table, &mutation.returning)
                    }
                    Operation::Read(read) => describe_read_frames(catalog, &read),
                    Operation::Refusal(refusal) => Err(operation_refusal_result(
                        refusal,
                        "describe",
                        Some(("portal_name", portal_name)),
                    )),
                    Operation::Session(_) | Operation::Prepare(_) => {
                        Err(unsupported_execute_refusal(
                            "describe_portal_kind",
                            &[("portal_name", portal_name)],
                        ))
                    }
                }
            }
        }
    }

    fn execute<B: Backend>(
        &mut self,
        catalog: &Catalog,
        backend: &mut B,
        parse_state: &ExtendedParseState,
        request: ExecuteRequest,
    ) -> Result<PendingResult, RefusalResult> {
        if self.pending_result.is_some() {
            return Err(unsupported_execute_refusal("execute_before_sync", &[]));
        }

        let portal = parse_state
            .portal(request.portal_name.as_str())
            .ok_or_else(|| missing_portal_refusal(&request.portal_name))?;
        if let Some(metadata_query) = portal.metadata_query {
            return Ok(PendingResult {
                result: KernelResult::Read(metadata_query.result()),
                returning_columns: Vec::new(),
            });
        }

        if request.max_rows != 0 {
            return Err(unsupported_execute_refusal(
                "execute_max_rows",
                &[
                    ("portal_name", request.portal_name),
                    ("max_rows", request.max_rows.to_string()),
                ],
            ));
        }

        match normalized_operation_from_portal(catalog, portal)? {
            Operation::Mutation(mutation) => {
                let returning_columns = mutation.returning.clone();
                let result = if matches!(mutation.kind, MutationKind::Insert | MutationKind::Upsert)
                {
                    execute_insert(catalog, backend, &mutation)
                } else {
                    KernelResult::Refusal(unsupported_execute_refusal(
                        "mutation_kind",
                        &[(
                            "kind",
                            serde_json::to_value(mutation.kind)
                                .expect("serialize mutation kind")
                                .as_str()
                                .expect("mutation kind token")
                                .to_owned(),
                        )],
                    ))
                };

                Ok(PendingResult {
                    result,
                    returning_columns,
                })
            }
            Operation::Read(read) => Ok(PendingResult {
                result: execute_read(catalog, backend, &read),
                returning_columns: Vec::new(),
            }),
            Operation::Refusal(refusal) => Ok(PendingResult {
                result: KernelResult::Refusal(operation_refusal_result(
                    refusal,
                    "execute",
                    Some(("portal_name", request.portal_name)),
                )),
                returning_columns: Vec::new(),
            }),
            Operation::Session(_) | Operation::Prepare(_) => Err(unsupported_execute_refusal(
                "execute_portal_kind",
                &[("portal_name", request.portal_name)],
            )),
        }
    }
}

fn describe_operation_from_prepared(
    catalog: &Catalog,
    prepared: &PreparedStatementState,
) -> Result<Operation, RefusalResult> {
    let params = dummy_parameter_values(prepared);
    normalized_operation(
        catalog,
        prepared
            .prepare
            .as_ref()
            .expect("prepared statement should exist for normalized operations")
            .session_id
            .as_str(),
        prepared.sql.as_str(),
        prepared
            .prepare
            .as_ref()
            .expect("prepared statement should exist for normalized operations")
            .param_types
            .as_slice(),
        params.as_slice(),
    )
}

fn normalized_operation_from_portal(
    catalog: &Catalog,
    portal: &PortalState,
) -> Result<Operation, RefusalResult> {
    let params = portal
        .params
        .iter()
        .map(|param| param.value.clone())
        .collect::<Vec<_>>();
    normalized_operation(
        catalog,
        portal.statement_name.as_str(),
        portal.sql.as_str(),
        portal.param_types.as_slice(),
        params.as_slice(),
    )
}

fn normalized_operation(
    catalog: &Catalog,
    session_id: &str,
    sql: &str,
    param_types: &[String],
    params: &[Option<String>],
) -> Result<Operation, RefusalResult> {
    let bound_sql = bind_sql_parameters(sql, param_types, params)?;
    match statement_kind(sql) {
        StatementKind::Mutation => Ok(normalize_mutation_sql(catalog, session_id, &bound_sql)),
        StatementKind::Read => Ok(normalize_read_sql(catalog, session_id, &bound_sql)),
        StatementKind::Unsupported => Err(unsupported_execute_refusal(
            "statement_kind",
            &[("sql", sql.to_owned())],
        )),
    }
}

fn encode_result_frames(result: &KernelResult, returning_columns: &[String]) -> Vec<Vec<u8>> {
    encode_kernel_result_frames(
        result,
        ResultFrameMetadata {
            returning_columns: (!returning_columns.is_empty()).then_some(returning_columns),
        },
    )
}

fn parameter_description_frame(prepared: &PreparedStatementState) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(prepared.parameter_count as i16).to_be_bytes());

    for index in 0..prepared.parameter_count {
        let oid = prepared
            .prepare
            .as_ref()
            .and_then(|prepare| prepare.param_types.get(index))
            .map_or(0_u32, |param_type| type_name_to_oid(param_type));
        body.extend_from_slice(&oid.to_be_bytes());
    }

    framed_message(b't', body)
}

fn metadata_field_descriptors(metadata_query: MetadataQuery) -> Vec<FieldDescriptor> {
    metadata_query
        .columns()
        .into_iter()
        .map(|name| FieldDescriptor {
            name,
            type_oid: TEXT_OID,
            type_size: -1,
        })
        .collect()
}

fn describe_mutation_frames(
    catalog: &Catalog,
    table_name: &str,
    returning_columns: &[String],
) -> Result<Vec<Vec<u8>>, RefusalResult> {
    if returning_columns.is_empty() {
        return Ok(vec![no_data_frame()]);
    }

    let fields = lookup_fields(
        catalog,
        returning_columns
            .iter()
            .map(|column| (table_name, column.clone()))
            .collect::<Vec<_>>()
            .as_slice(),
    )?;
    Ok(vec![row_description_frame(&fields)])
}

fn describe_read_frames(catalog: &Catalog, read: &ReadOp) -> Result<Vec<Vec<u8>>, RefusalResult> {
    let table = catalog.table(&read.table).ok_or_else(|| {
        unsupported_execute_refusal("unknown_table", &[("table", read.table.clone())])
    })?;

    let mut fields = read
        .projection
        .iter()
        .map(|column| {
            lookup_column(table.columns.as_slice(), column).map(|column| {
                field_descriptor(column.name.clone(), column.normalized_type.as_str())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if read.aggregate.kind != AggregateKind::None {
        fields.push(aggregate_field_descriptor(read));
    }

    Ok(vec![row_description_frame(&fields)])
}

fn lookup_fields(
    catalog: &Catalog,
    columns: &[(impl AsRef<str>, String)],
) -> Result<Vec<FieldDescriptor>, RefusalResult> {
    columns
        .iter()
        .map(|(table_name, column_name)| {
            let table = catalog.table(table_name.as_ref()).ok_or_else(|| {
                unsupported_execute_refusal(
                    "unknown_table",
                    &[("table", table_name.as_ref().to_owned())],
                )
            })?;
            let column = lookup_column(table.columns.as_slice(), column_name)?;
            Ok(field_descriptor(
                column.name.clone(),
                column.normalized_type.as_str(),
            ))
        })
        .collect()
}

fn lookup_column<'a>(
    columns: &'a [ColumnCatalog],
    column_name: &str,
) -> Result<&'a ColumnCatalog, RefusalResult> {
    columns
        .iter()
        .find(|column| column.name == column_name)
        .ok_or_else(|| RefusalResult {
            code: String::from("undefined_column"),
            message: format!("column `{column_name}` does not exist"),
            sqlstate: String::from("42703"),
            detail: [(String::from("column"), column_name.to_owned())]
                .into_iter()
                .collect(),
        })
}

fn aggregate_field_descriptor(read: &ReadOp) -> FieldDescriptor {
    FieldDescriptor {
        name: read
            .aggregate
            .alias
            .clone()
            .unwrap_or_else(|| String::from("count")),
        type_oid: INT8_OID,
        type_size: 8,
    }
}

fn field_descriptor(name: String, normalized_type: &str) -> FieldDescriptor {
    let (type_oid, type_size) = type_metadata(normalized_type);
    FieldDescriptor {
        name,
        type_oid,
        type_size,
    }
}

fn row_description_frame(fields: &[FieldDescriptor]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(fields.len() as i16).to_be_bytes());
    for field in fields {
        body.extend_from_slice(field.name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_u32.to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
        body.extend_from_slice(&field.type_oid.to_be_bytes());
        body.extend_from_slice(&field.type_size.to_be_bytes());
        body.extend_from_slice(&(-1_i32).to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
    }
    framed_message(b'T', body)
}

fn no_data_frame() -> Vec<u8> {
    vec![b'n', 0, 0, 0, 4]
}

fn framed_message(tag: u8, body: Vec<u8>) -> Vec<u8> {
    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(tag);
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn bind_sql_parameters(
    sql: &str,
    param_types: &[String],
    params: &[Option<String>],
) -> Result<String, RefusalResult> {
    let bytes = sql.as_bytes();
    let mut index = 0usize;
    let mut rendered = String::with_capacity(sql.len() + 16);

    while index < bytes.len() {
        if bytes[index] == b'$' {
            let start = index + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start {
                let position = sql[start..end]
                    .parse::<usize>()
                    .expect("placeholder digits should parse");
                let value = params.get(position - 1).ok_or_else(|| {
                    unsupported_execute_refusal(
                        "missing_parameter",
                        &[("position", position.to_string())],
                    )
                })?;
                let param_type = param_types.get(position - 1).map(String::as_str);
                rendered.push_str(sql_literal(value.as_ref(), param_type).as_str());
                index = end;
                continue;
            }
        }

        rendered.push(bytes[index] as char);
        index += 1;
    }

    Ok(rendered)
}

fn sql_literal(value: Option<&String>, param_type: Option<&str>) -> String {
    match value {
        None => String::from("NULL"),
        Some(value) => {
            let param_type = param_type.unwrap_or_default().to_ascii_lowercase();
            if matches!(
                param_type.as_str(),
                "bigint" | "integer" | "smallint" | "numeric" | "float"
            ) && value.parse::<f64>().is_ok()
            {
                return value.clone();
            }
            if param_type == "boolean" {
                if value.eq_ignore_ascii_case("true") || value == "t" {
                    return String::from("TRUE");
                }
                if value.eq_ignore_ascii_case("false") || value == "f" {
                    return String::from("FALSE");
                }
            }

            format!("'{}'", value.replace('\'', "''"))
        }
    }
}

fn dummy_parameter_values(prepared: &PreparedStatementState) -> Vec<Option<String>> {
    const DUMMY_TEXT_PARAMETER: &str = "placeholder_text";

    (0..prepared.parameter_count)
        .map(|index| {
            Some(
                match prepared
                    .prepare
                    .as_ref()
                    .and_then(|prepare| prepare.param_types.get(index))
                    .map(String::as_str)
                {
                    Some("bigint" | "integer" | "smallint" | "numeric" | "float") => {
                        String::from("0")
                    }
                    Some("boolean") => String::from("false"),
                    _ => String::from(DUMMY_TEXT_PARAMETER),
                },
            )
        })
        .collect()
}

fn statement_kind(statement_text: &str) -> StatementKind {
    let leading_keyword = statement_text
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    match leading_keyword.as_str() {
        "insert" => StatementKind::Mutation,
        "select" => StatementKind::Read,
        _ => StatementKind::Unsupported,
    }
}

fn operation_refusal_result(
    refusal: RefusalOp,
    phase: &str,
    extra: Option<(&str, String)>,
) -> RefusalResult {
    let shape = refusal
        .detail
        .get("shape")
        .cloned()
        .unwrap_or_else(|| refusal.code.clone());
    let mut detail = refusal.detail;
    detail.insert(String::from("phase"), phase.to_owned());
    detail.insert(String::from("transport"), String::from("extended_query"));
    if let Some((key, value)) = extra {
        detail.insert(key.to_owned(), value);
    }

    RefusalResult {
        code: refusal.code,
        message: format!(
            "extended query {phase} shape `{shape}` is outside the declared {} subset",
            refusal_scope_token(refusal.scope)
        ),
        sqlstate: String::from("0A000"),
        detail,
    }
}

fn unsupported_execute_refusal(shape: &str, extras: &[(&str, String)]) -> RefusalResult {
    let mut detail = std::collections::BTreeMap::from([
        (String::from("shape"), shape.to_owned()),
        (String::from("transport"), String::from("extended_query")),
    ]);
    for (key, value) in extras {
        detail.insert((*key).to_owned(), value.clone());
    }

    RefusalResult {
        code: String::from("unsupported_shape"),
        message: format!("extended query shape `{shape}` is outside the declared execute subset"),
        sqlstate: String::from("0A000"),
        detail,
    }
}

fn missing_statement_refusal(statement_name: &str) -> RefusalResult {
    RefusalResult {
        code: String::from("undefined_prepared_statement"),
        message: format!("prepared statement `{statement_name}` does not exist"),
        sqlstate: String::from("26000"),
        detail: [
            (String::from("phase"), String::from("describe")),
            (String::from("transport"), String::from("extended_query")),
            (String::from("statement_name"), statement_name.to_owned()),
        ]
        .into_iter()
        .collect(),
    }
}

fn missing_portal_refusal(portal_name: &str) -> RefusalResult {
    RefusalResult {
        code: String::from("invalid_cursor_name"),
        message: format!("portal `{portal_name}` does not exist"),
        sqlstate: String::from("34000"),
        detail: [
            (String::from("transport"), String::from("extended_query")),
            (String::from("portal_name"), portal_name.to_owned()),
        ]
        .into_iter()
        .collect(),
    }
}

fn refusal_scope_token(scope: RefusalScope) -> &'static str {
    match scope {
        RefusalScope::Session => "session",
        RefusalScope::Prepare => "prepare",
        RefusalScope::Mutation => "mutation",
        RefusalScope::Read => "read",
    }
}

fn type_name_to_oid(type_name: &str) -> u32 {
    match type_name.to_ascii_lowercase().as_str() {
        "boolean" => BOOL_OID,
        "bigint" => INT8_OID,
        "integer" => INT4_OID,
        "smallint" => INT2_OID,
        "numeric" => NUMERIC_OID,
        "float" => FLOAT8_OID,
        "timestamp" => TIMESTAMP_OID,
        "date" => DATE_OID,
        "bytes" => BYTEA_OID,
        "json" => JSON_OID,
        "array" => TEXT_ARRAY_OID,
        _ => TEXT_OID,
    }
}

fn type_metadata(normalized_type: &str) -> (u32, i16) {
    match normalized_type {
        "boolean" => (BOOL_OID, 1),
        "bigint" => (INT8_OID, 8),
        "integer" => (INT4_OID, 4),
        "smallint" => (INT2_OID, 2),
        "numeric" => (NUMERIC_OID, -1),
        "float" => (FLOAT8_OID, 8),
        "timestamp" => (TIMESTAMP_OID, 8),
        "date" => (DATE_OID, 4),
        "bytes" => (BYTEA_OID, -1),
        "json" => (JSON_OID, -1),
        "array" => (TEXT_ARRAY_OID, -1),
        _ => (TEXT_OID, -1),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementKind {
    Mutation,
    Read,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldDescriptor {
    name: String,
    type_oid: u32,
    type_size: i16,
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::{Backend, BaseSnapshotBackend},
        catalog::{Catalog, parse_postgres_schema},
        kernel::{storage::TableStorage, value::KernelValue},
        protocol::postgres::{
            extended_parse::{BindRequest, ParseRequest, PreparedStatementState},
            session::{SessionLoop, TransactionStatus},
        },
        result::{AckResult, ResultTag},
    };

    use super::{
        DescribeTarget, ExecuteRequest, ExtendedExecuteState, ExtendedParseState, TEXT_OID,
        dummy_parameter_values,
    };

    #[test]
    fn dummy_parameters_use_neutral_text_placeholder_for_unknown_types() {
        let prepared = PreparedStatementState {
            statement_name: String::from("stmt-placeholder"),
            sql: String::from("SELECT $1"),
            prepare: None,
            metadata_query: None,
            parameter_count: 1,
        };

        assert_eq!(
            dummy_parameter_values(&prepared),
            vec![Some(String::from("placeholder_text"))]
        );
    }

    #[test]
    fn describe_statement_returns_parameter_and_row_metadata_for_select() {
        let (catalog, _) = deals_backend();
        let mut parse_state = ExtendedParseState::new();
        parse_state.process_parse(
            "session-1",
            ParseRequest {
                statement_name: String::from("stmt-select"),
                sql: String::from("SELECT deal_id, deal_name FROM public.deals WHERE deal_id = $1"),
                param_types: vec![String::from("text")],
            },
        );

        let execute_state = ExtendedExecuteState::new();
        let frames = execute_state.process_describe(
            &catalog,
            &parse_state,
            DescribeTarget::Statement(String::from("stmt-select")),
        );

        assert_eq!(decode_parameter_description(&frames[0]), vec![25]);
        assert_eq!(
            decode_row_description(&frames[1]),
            vec![
                (String::from("deal_id"), TEXT_OID),
                (String::from("deal_name"), TEXT_OID),
            ]
        );
    }

    #[test]
    fn describe_portal_refuses_unsupported_query_shapes() {
        let (catalog, _) = deals_backend();
        let mut parse_state = ExtendedParseState::new();
        parse_state.process_parse(
            "session-2",
            ParseRequest {
                statement_name: String::from("stmt-lock"),
                sql: String::from("SELECT deal_id FROM public.deals FOR UPDATE"),
                param_types: Vec::new(),
            },
        );
        parse_state.process_bind(BindRequest {
            portal_name: String::from("portal-lock"),
            statement_name: String::from("stmt-lock"),
            params: Vec::new(),
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });

        let execute_state = ExtendedExecuteState::new();
        let frames = execute_state.process_describe(
            &catalog,
            &parse_state,
            DescribeTarget::Portal(String::from("portal-lock")),
        );

        assert_eq!(
            decode_error_fields(&frames[0]),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from("0A000")),
                (
                    'M',
                    String::from(
                        "extended query describe shape `select_for_update` is outside the declared read subset",
                    ),
                ),
                ('V', String::from("unsupported_shape")),
                (
                    'D',
                    String::from(
                        "phase=describe; portal_name=portal-lock; query=SELECT deal_id FROM public.deals FOR UPDATE; shape=select_for_update; transport=extended_query",
                    ),
                ),
            ]
        );
    }

    #[test]
    fn execute_select_portal_reads_rows_and_sync_emits_ready_for_query() {
        let (catalog, mut backend) = deals_backend();
        let mut parse_state = ExtendedParseState::new();
        parse_state.process_parse(
            "session-3",
            ParseRequest {
                statement_name: String::from("stmt-select"),
                sql: String::from("SELECT deal_id, deal_name FROM public.deals WHERE deal_id = $1"),
                param_types: Vec::new(),
            },
        );
        parse_state.process_bind(BindRequest {
            portal_name: String::from("portal-select"),
            statement_name: String::from("stmt-select"),
            params: vec![Some(String::from("deal-1"))],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });

        let mut session = SessionLoop::new();
        session.process_result(
            &crate::result::KernelResult::Ack(AckResult {
                tag: ResultTag::Begin,
                rows_affected: 0,
            }),
            Default::default(),
        );

        let mut execute_state = ExtendedExecuteState::new();
        let execute_frames = execute_state.process_execute(
            &catalog,
            &mut backend,
            &parse_state,
            ExecuteRequest {
                portal_name: String::from("portal-select"),
                max_rows: 0,
            },
        );

        assert_eq!(
            decode_row_description(&execute_frames[0]),
            vec![
                (String::from("deal_id"), TEXT_OID),
                (String::from("deal_name"), TEXT_OID),
            ]
        );
        assert_eq!(
            decode_data_row(&execute_frames[1]),
            vec![Some(String::from("deal-1")), Some(String::from("Alpha"))]
        );
        assert_eq!(decode_command_complete(&execute_frames[2]), "SELECT 1");

        let sync = execute_state.process_sync(&mut session);
        assert_eq!(sync.transaction_status, TransactionStatus::InTransaction);
        assert_eq!(decode_ready_status(&sync.frames[0]), b'T');
    }

    #[test]
    fn execute_insert_portal_writes_overlay_and_sync_returns_idle_ready() {
        let (catalog, mut backend) = deals_backend();
        let mut parse_state = ExtendedParseState::new();
        parse_state.process_parse(
            "session-4",
            ParseRequest {
                statement_name: String::from("stmt-insert"),
                sql: String::from("INSERT INTO public.deals (deal_id, deal_name) VALUES ($1, $2)"),
                param_types: vec![String::from("text"), String::from("text")],
            },
        );
        parse_state.process_bind(BindRequest {
            portal_name: String::from("portal-insert"),
            statement_name: String::from("stmt-insert"),
            params: vec![Some(String::from("deal-2")), Some(String::from("Beta"))],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });

        let mut session = SessionLoop::new();
        let mut execute_state = ExtendedExecuteState::new();
        let execute_frames = execute_state.process_execute(
            &catalog,
            &mut backend,
            &parse_state,
            ExecuteRequest {
                portal_name: String::from("portal-insert"),
                max_rows: 0,
            },
        );

        assert_eq!(decode_command_complete(&execute_frames[0]), "INSERT 0 1");
        assert_eq!(
            backend
                .visible_table("public.deals")
                .expect("visible deals")
                .row_count(),
            2
        );

        let sync = execute_state.process_sync(&mut session);
        assert_eq!(sync.transaction_status, TransactionStatus::Idle);
        assert_eq!(decode_ready_status(&sync.frames[0]), b'I');
    }

    #[test]
    fn execute_extractor_insert_shape_writes_overlay_for_external_key_schema() {
        let (catalog, mut backend) = deals_with_external_key_backend();
        let mut parse_state = ExtendedParseState::new();
        parse_state.process_parse(
            "session-extractor-insert",
            ParseRequest {
                statement_name: String::from("stmt-extractor-insert"),
                sql: String::from(
                    "INSERT INTO public.deals (deal_id, external_key, deal_name) VALUES ($1, $2, $3)",
                ),
                param_types: vec![
                    String::from("text"),
                    String::from("text"),
                    String::from("text"),
                ],
            },
        );
        parse_state.process_bind(BindRequest {
            portal_name: String::from("portal-extractor-insert"),
            statement_name: String::from("stmt-extractor-insert"),
            params: vec![
                Some(String::from("deal-001")),
                Some(String::from("alpha-001")),
                Some(String::from("Alpha")),
            ],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });

        let mut session = SessionLoop::new();
        let mut execute_state = ExtendedExecuteState::new();
        let execute_frames = execute_state.process_execute(
            &catalog,
            &mut backend,
            &parse_state,
            ExecuteRequest {
                portal_name: String::from("portal-extractor-insert"),
                max_rows: 0,
            },
        );

        assert_eq!(decode_command_complete(&execute_frames[0]), "INSERT 0 1");
        assert_eq!(
            backend
                .visible_table("public.deals")
                .expect("visible deals")
                .row_count(),
            1
        );

        let sync = execute_state.process_sync(&mut session);
        assert_eq!(sync.transaction_status, TransactionStatus::Idle);
        assert_eq!(decode_ready_status(&sync.frames[0]), b'I');
    }

    #[test]
    fn execute_extractor_upsert_pk_shape_updates_existing_primary_key_row() {
        let (catalog, mut backend) = seeded_deals_with_external_key_backend();
        let mut parse_state = ExtendedParseState::new();
        parse_state.process_parse(
            "session-extractor-upsert-pk",
            ParseRequest {
                statement_name: String::from("stmt-extractor-upsert-pk"),
                sql: String::from(
                    "INSERT INTO public.deals (deal_id, external_key, deal_name) VALUES ($1, $2, $3) ON CONFLICT (deal_id) DO UPDATE SET external_key = EXCLUDED.external_key, deal_name = EXCLUDED.deal_name",
                ),
                param_types: vec![
                    String::from("text"),
                    String::from("text"),
                    String::from("text"),
                ],
            },
        );
        parse_state.process_bind(BindRequest {
            portal_name: String::from("portal-extractor-upsert-pk"),
            statement_name: String::from("stmt-extractor-upsert-pk"),
            params: vec![
                Some(String::from("deal-001")),
                Some(String::from("alpha-001")),
                Some(String::from("Alpha Updated")),
            ],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });

        let mut session = SessionLoop::new();
        let mut execute_state = ExtendedExecuteState::new();
        let execute_frames = execute_state.process_execute(
            &catalog,
            &mut backend,
            &parse_state,
            ExecuteRequest {
                portal_name: String::from("portal-extractor-upsert-pk"),
                max_rows: 0,
            },
        );

        assert_eq!(decode_command_complete(&execute_frames[0]), "INSERT 0 1");
        let visible = backend
            .visible_table("public.deals")
            .expect("visible deals");
        assert_eq!(visible.row_count(), 1);
        assert_eq!(
            visible
                .lookup_primary_key(&[KernelValue::Text(String::from("deal-001"))])
                .expect("lookup primary key")
                .expect("row by primary key")
                .values,
            vec![
                KernelValue::Text(String::from("deal-001")),
                KernelValue::Text(String::from("alpha-001")),
                KernelValue::Text(String::from("Alpha Updated")),
            ]
        );

        let sync = execute_state.process_sync(&mut session);
        assert_eq!(sync.transaction_status, TransactionStatus::Idle);
        assert_eq!(decode_ready_status(&sync.frames[0]), b'I');
    }

    #[test]
    fn execute_extractor_upsert_unique_shape_updates_named_constraint_row() {
        let (catalog, mut backend) = seeded_deals_with_external_key_backend();
        let mut parse_state = ExtendedParseState::new();
        parse_state.process_parse(
            "session-extractor-upsert-unique",
            ParseRequest {
                statement_name: String::from("stmt-extractor-upsert-unique"),
                sql: String::from(
                    "INSERT INTO public.deals (deal_id, external_key, deal_name) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT deals_external_key_key DO UPDATE SET deal_id = EXCLUDED.deal_id, deal_name = EXCLUDED.deal_name",
                ),
                param_types: vec![
                    String::from("text"),
                    String::from("text"),
                    String::from("text"),
                ],
            },
        );
        parse_state.process_bind(BindRequest {
            portal_name: String::from("portal-extractor-upsert-unique"),
            statement_name: String::from("stmt-extractor-upsert-unique"),
            params: vec![
                Some(String::from("deal-002")),
                Some(String::from("alpha-001")),
                Some(String::from("Alpha Unique Rewrite")),
            ],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });

        let mut session = SessionLoop::new();
        let mut execute_state = ExtendedExecuteState::new();
        let execute_frames = execute_state.process_execute(
            &catalog,
            &mut backend,
            &parse_state,
            ExecuteRequest {
                portal_name: String::from("portal-extractor-upsert-unique"),
                max_rows: 0,
            },
        );

        assert_eq!(decode_command_complete(&execute_frames[0]), "INSERT 0 1");
        let visible = backend
            .visible_table("public.deals")
            .expect("visible deals");
        assert_eq!(visible.row_count(), 1);
        assert!(
            visible
                .lookup_primary_key(&[KernelValue::Text(String::from("deal-001"))])
                .expect("lookup old primary key")
                .is_none()
        );
        assert_eq!(
            visible
                .lookup_primary_key(&[KernelValue::Text(String::from("deal-002"))])
                .expect("lookup rewritten primary key")
                .expect("row by rewritten primary key")
                .values,
            vec![
                KernelValue::Text(String::from("deal-002")),
                KernelValue::Text(String::from("alpha-001")),
                KernelValue::Text(String::from("Alpha Unique Rewrite")),
            ]
        );

        let sync = execute_state.process_sync(&mut session);
        assert_eq!(sync.transaction_status, TransactionStatus::Idle);
        assert_eq!(decode_ready_status(&sync.frames[0]), b'I');
    }

    #[test]
    fn unsupported_execute_shape_returns_error_and_sync_marks_failed_tx() {
        let (catalog, mut backend) = deals_backend();
        let mut parse_state = ExtendedParseState::new();
        parse_state.process_parse(
            "session-5",
            ParseRequest {
                statement_name: String::from("stmt-lock"),
                sql: String::from("SELECT deal_id FROM public.deals FOR UPDATE"),
                param_types: Vec::new(),
            },
        );
        parse_state.process_bind(BindRequest {
            portal_name: String::from("portal-lock"),
            statement_name: String::from("stmt-lock"),
            params: Vec::new(),
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });

        let mut session = SessionLoop::new();
        session.process_result(
            &crate::result::KernelResult::Ack(AckResult {
                tag: ResultTag::Begin,
                rows_affected: 0,
            }),
            Default::default(),
        );

        let mut execute_state = ExtendedExecuteState::new();
        let execute_frames = execute_state.process_execute(
            &catalog,
            &mut backend,
            &parse_state,
            ExecuteRequest {
                portal_name: String::from("portal-lock"),
                max_rows: 0,
            },
        );

        assert_eq!(
            decode_error_fields(&execute_frames[0]),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from("0A000")),
                (
                    'M',
                    String::from(
                        "extended query execute shape `select_for_update` is outside the declared read subset",
                    ),
                ),
                ('V', String::from("unsupported_shape")),
                (
                    'D',
                    String::from(
                        "phase=execute; portal_name=portal-lock; query=SELECT deal_id FROM public.deals FOR UPDATE; shape=select_for_update; transport=extended_query",
                    ),
                ),
            ]
        );

        let sync = execute_state.process_sync(&mut session);
        assert_eq!(
            sync.transaction_status,
            TransactionStatus::FailedTransaction
        );
        assert_eq!(decode_ready_status(&sync.frames[0]), b'E');
    }

    fn deals_backend() -> (Catalog, BaseSnapshotBackend) {
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
            .expect("insert seed row");

        let backend = BaseSnapshotBackend::new([deals]).expect("build backend");
        (catalog, backend)
    }

    fn deals_with_external_key_backend() -> (Catalog, BaseSnapshotBackend) {
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                external_key TEXT NOT NULL,
                deal_name TEXT NOT NULL,
                CONSTRAINT deals_external_key_key UNIQUE (external_key)
            );
            "#,
        )
        .expect("schema should parse");

        let deals = TableStorage::new(
            catalog
                .table("public.deals")
                .expect("deals table should exist"),
        )
        .expect("deals storage should build");

        let backend = BaseSnapshotBackend::new([deals]).expect("build backend");
        (catalog, backend)
    }

    fn seeded_deals_with_external_key_backend() -> (Catalog, BaseSnapshotBackend) {
        let (catalog, mut backend) = deals_with_external_key_backend();
        let mut deals = backend
            .snapshot_base_table("public.deals")
            .expect("snapshot deals");
        deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-001")),
                KernelValue::Text(String::from("alpha-001")),
                KernelValue::Text(String::from("Alpha")),
            ])
            .expect("insert seed row");
        backend = BaseSnapshotBackend::new([deals]).expect("build backend");
        (catalog, backend)
    }

    fn decode_command_complete(frame: &[u8]) -> String {
        assert_eq!(frame[0], b'C');
        String::from_utf8(frame[5..frame.len() - 1].to_vec()).expect("command tag")
    }

    fn decode_ready_status(frame: &[u8]) -> u8 {
        assert_eq!(frame[0], b'Z');
        frame[5]
    }

    fn decode_parameter_description(frame: &[u8]) -> Vec<u32> {
        assert_eq!(frame[0], b't');
        let body = &frame[5..];
        let count = i16::from_be_bytes([body[0], body[1]]) as usize;
        let mut cursor = 2usize;
        let mut oids = Vec::with_capacity(count);

        for _ in 0..count {
            oids.push(u32::from_be_bytes([
                body[cursor],
                body[cursor + 1],
                body[cursor + 2],
                body[cursor + 3],
            ]));
            cursor += 4;
        }

        oids
    }

    fn decode_row_description(frame: &[u8]) -> Vec<(String, u32)> {
        assert_eq!(frame[0], b'T');
        let body = &frame[5..];
        let field_count = i16::from_be_bytes([body[0], body[1]]) as usize;
        let mut cursor = 2usize;
        let mut fields = Vec::with_capacity(field_count);

        for _ in 0..field_count {
            let name_end = body[cursor..]
                .iter()
                .position(|byte| *byte == 0)
                .expect("field name terminator");
            let name =
                String::from_utf8(body[cursor..cursor + name_end].to_vec()).expect("field name");
            cursor += name_end + 1;
            cursor += 4;
            cursor += 2;
            let type_oid = u32::from_be_bytes([
                body[cursor],
                body[cursor + 1],
                body[cursor + 2],
                body[cursor + 3],
            ]);
            cursor += 4;
            cursor += 2;
            cursor += 4;
            cursor += 2;
            fields.push((name, type_oid));
        }

        fields
    }

    fn decode_data_row(frame: &[u8]) -> Vec<Option<String>> {
        assert_eq!(frame[0], b'D');
        let body = &frame[5..];
        let field_count = i16::from_be_bytes([body[0], body[1]]) as usize;
        let mut cursor = 2usize;
        let mut values = Vec::with_capacity(field_count);

        for _ in 0..field_count {
            let length = i32::from_be_bytes([
                body[cursor],
                body[cursor + 1],
                body[cursor + 2],
                body[cursor + 3],
            ]);
            cursor += 4;
            if length < 0 {
                values.push(None);
                continue;
            }
            let length = length as usize;
            values.push(Some(
                String::from_utf8(body[cursor..cursor + length].to_vec()).expect("value"),
            ));
            cursor += length;
        }

        values
    }

    fn decode_error_fields(frame: &[u8]) -> Vec<(char, String)> {
        assert_eq!(frame[0], b'E');
        let body = &frame[5..];
        let mut cursor = 0usize;
        let mut fields = Vec::new();

        while body[cursor] != 0 {
            let code = body[cursor] as char;
            cursor += 1;
            let value_end = body[cursor..]
                .iter()
                .position(|byte| *byte == 0)
                .expect("value terminator");
            let value =
                String::from_utf8(body[cursor..cursor + value_end].to_vec()).expect("field value");
            cursor += value_end + 1;
            fields.push((code, value));
        }

        fields
    }
}
