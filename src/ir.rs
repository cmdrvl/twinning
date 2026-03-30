use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlparser::{
    ast::{
        Assignment, AssignmentTarget, BinaryOperator, ConflictTarget as SqlConflictTarget, Expr,
        Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Insert, ObjectName,
        OnConflictAction, OnInsert, OneOrManyWithParens, Query, Select, SelectItem, SetExpr,
        Statement, TableFactor, TableWithJoins, UnaryOperator, Value as SqlValue,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::catalog::{Catalog, TableCatalog};

pub type SessionId = String;
pub type StatementId = String;
pub type SqlHash = String;
pub type TableName = String;
pub type ColumnName = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op_type", rename_all = "snake_case")]
pub enum Operation {
    Session(SessionOp),
    Prepare(PrepareOp),
    Mutation(MutationOp),
    Read(ReadOp),
    Refusal(RefusalOp),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOp {
    pub session_id: SessionId,
    pub op: SessionOpKind,
    pub tracked_params: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionOpKind {
    SetParam,
    Begin,
    Commit,
    Rollback,
    Sync,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareOp {
    pub session_id: SessionId,
    pub statement_id: StatementId,
    pub sql_hash: SqlHash,
    pub param_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationOp {
    pub session_id: SessionId,
    pub table: TableName,
    pub kind: MutationKind,
    pub columns: Vec<ColumnName>,
    pub rows: Vec<Vec<ScalarValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_target: Option<ConflictTarget>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub update_columns: Vec<ColumnName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<PredicateExpr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<ColumnName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationKind {
    Insert,
    Upsert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictTarget {
    PrimaryKey,
    Columns(Vec<ColumnName>),
    NamedConstraint(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadOp {
    pub session_id: SessionId,
    pub table: TableName,
    pub shape: ReadShape,
    pub projection: Vec<ColumnName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<PredicateExpr>,
    pub aggregate: AggregateSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<ColumnName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadShape {
    PointLookup,
    FilteredScan,
    AggregateScan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateSpec {
    pub kind: AggregateKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<ColumnName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

impl Default for AggregateSpec {
    fn default() -> Self {
        Self {
            kind: AggregateKind::None,
            column: None,
            alias: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateKind {
    None,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefusalOp {
    pub scope: RefusalScope,
    pub code: String,
    pub detail: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalScope {
    Session,
    Prepare,
    Mutation,
    Read,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateExpr {
    Comparison(PredicateComparison),
    Conjunction(Vec<PredicateComparison>),
    Disjunction(Vec<PredicateComparison>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredicateComparison {
    pub column: ColumnName,
    pub operator: PredicateOperator,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<ScalarValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    IsNull,
    InList,
    Between,
}

impl PredicateOperator {
    pub fn value_arity(self) -> PredicateValueArity {
        match self {
            Self::IsNull => PredicateValueArity::Zero,
            Self::Eq | Self::Neq | Self::Lt | Self::Lte | Self::Gt | Self::Gte => {
                PredicateValueArity::One
            }
            Self::InList => PredicateValueArity::OneOrMore,
            Self::Between => PredicateValueArity::Two,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateValueArity {
    Zero,
    One,
    OneOrMore,
    Two,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
}

type NormalizationResult<T> = Result<T, RefusalOp>;
type RefusalBuilder = fn(&str, &[(&str, String)]) -> RefusalOp;

const UNSUPPORTED_SHAPE_CODE: &str = "unsupported_shape";
const TRACKED_SESSION_PARAM_APPLICATION_NAME: &str = "application_name";

pub fn normalize_session_sql(session_id: impl Into<SessionId>, sql: &str) -> Operation {
    let session_id = session_id.into();
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(error) => {
            return Operation::Refusal(session_refusal(
                "parse_error",
                &[("error", error.to_string())],
            ));
        }
    };

    if statements.len() != 1 {
        return Operation::Refusal(session_refusal(
            "multiple_statements",
            &[("count", statements.len().to_string())],
        ));
    }

    normalize_session_statement(session_id, &statements.remove(0))
}

pub fn normalize_session_statement(
    session_id: impl Into<SessionId>,
    statement: &Statement,
) -> Operation {
    match normalize_session_statement_inner(session_id.into(), statement) {
        Ok(session) => Operation::Session(session),
        Err(refusal) => Operation::Refusal(refusal),
    }
}

pub fn normalize_sync(session_id: impl Into<SessionId>) -> Operation {
    Operation::Session(SessionOp {
        session_id: session_id.into(),
        op: SessionOpKind::Sync,
        tracked_params: BTreeMap::new(),
    })
}

pub fn normalize_prepare_sql<I, S>(
    session_id: impl Into<SessionId>,
    statement_id: impl Into<StatementId>,
    sql: &str,
    param_types: I,
) -> Operation
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let session_id = session_id.into();
    let statement_id = statement_id.into();
    let param_types = normalize_param_types(param_types);
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(error) => {
            return Operation::Refusal(prepare_refusal(
                "parse_error",
                &[("error", error.to_string())],
            ));
        }
    };

    if statements.len() != 1 {
        return Operation::Refusal(prepare_refusal(
            "multiple_statements",
            &[("count", statements.len().to_string())],
        ));
    }

    normalize_prepare_statement(session_id, statement_id, &statements.remove(0), param_types)
}

pub fn normalize_prepare_statement(
    session_id: impl Into<SessionId>,
    statement_id: impl Into<StatementId>,
    statement: &Statement,
    param_types: Vec<String>,
) -> Operation {
    match normalize_prepare_statement_inner(
        session_id.into(),
        statement_id.into(),
        statement,
        param_types,
    ) {
        Ok(prepare) => Operation::Prepare(prepare),
        Err(refusal) => Operation::Refusal(refusal),
    }
}

fn normalize_session_statement_inner(
    session_id: SessionId,
    statement: &Statement,
) -> NormalizationResult<SessionOp> {
    match statement {
        Statement::StartTransaction {
            modes, modifier, ..
        } if modes.is_empty() && modifier.is_none() => Ok(SessionOp {
            session_id,
            op: SessionOpKind::Begin,
            tracked_params: BTreeMap::new(),
        }),
        Statement::Commit { chain: false } => Ok(SessionOp {
            session_id,
            op: SessionOpKind::Commit,
            tracked_params: BTreeMap::new(),
        }),
        Statement::Rollback {
            chain: false,
            savepoint: None,
        } => Ok(SessionOp {
            session_id,
            op: SessionOpKind::Rollback,
            tracked_params: BTreeMap::new(),
        }),
        Statement::SetVariable {
            local: false,
            hivevar: false,
            variables,
            value,
        } => normalize_tracked_set_statement(session_id, variables, value),
        Statement::ShowVariable { variable } => Err(session_refusal(
            show_variable_shape(variable),
            &[("statement", statement.to_string())],
        )),
        Statement::Savepoint { name } => Err(session_refusal(
            "savepoint",
            &[("name", name.value.clone())],
        )),
        Statement::ReleaseSavepoint { name } => Err(session_refusal(
            "release_savepoint",
            &[("name", name.value.clone())],
        )),
        Statement::SetTransaction { .. } => Err(session_refusal(
            "set_transaction",
            &[("statement", statement.to_string())],
        )),
        Statement::Commit { chain: true } => Err(session_refusal(
            "commit_and_chain",
            &[("statement", statement.to_string())],
        )),
        Statement::Rollback {
            chain: true,
            savepoint,
        } => Err(session_refusal(
            if savepoint.is_some() {
                "rollback_savepoint_and_chain"
            } else {
                "rollback_and_chain"
            },
            &[("statement", statement.to_string())],
        )),
        Statement::Rollback {
            savepoint: Some(name),
            ..
        } => Err(session_refusal(
            "rollback_savepoint",
            &[("name", name.value.clone())],
        )),
        _ => Err(session_refusal(
            "unsupported_statement",
            &[("statement", statement.to_string())],
        )),
    }
}

fn normalize_tracked_set_statement(
    session_id: SessionId,
    variables: &OneOrManyWithParens<ObjectName>,
    values: &[Expr],
) -> NormalizationResult<SessionOp> {
    if variables.len() != 1 || values.len() != 1 {
        return Err(session_refusal(
            "set_shape",
            &[(
                "statement",
                format!("variables={}, values={}", variables.len(), values.len()),
            )],
        ));
    }

    let variable = object_name_to_string(&variables[0]);
    if variable != TRACKED_SESSION_PARAM_APPLICATION_NAME {
        return Err(session_refusal(
            &session_set_shape(&variable),
            &[("variable", variable)],
        ));
    }

    let value = normalize_session_setting_value(&values[0])?;
    Ok(SessionOp {
        session_id,
        op: SessionOpKind::SetParam,
        tracked_params: BTreeMap::from([(
            String::from(TRACKED_SESSION_PARAM_APPLICATION_NAME),
            value,
        )]),
    })
}

fn normalize_session_setting_value(expr: &Expr) -> NormalizationResult<String> {
    match expr {
        Expr::Value(SqlValue::SingleQuotedString(value))
        | Expr::Value(SqlValue::DoubleQuotedString(value))
        | Expr::Value(SqlValue::EscapedStringLiteral(value))
        | Expr::Value(SqlValue::UnicodeStringLiteral(value))
        | Expr::Value(SqlValue::NationalStringLiteral(value))
        | Expr::Value(SqlValue::TripleSingleQuotedString(value))
        | Expr::Value(SqlValue::TripleDoubleQuotedString(value))
        | Expr::Value(SqlValue::SingleQuotedRawStringLiteral(value))
        | Expr::Value(SqlValue::DoubleQuotedRawStringLiteral(value))
        | Expr::Value(SqlValue::TripleSingleQuotedRawStringLiteral(value))
        | Expr::Value(SqlValue::TripleDoubleQuotedRawStringLiteral(value)) => Ok(value.clone()),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(|ident| ident.value.as_str())
            .collect::<Vec<_>>()
            .join(".")),
        Expr::Value(SqlValue::Number(value, _)) => Ok(value.clone()),
        Expr::Value(SqlValue::Boolean(value)) => Ok(value.to_string()),
        _ => Err(session_refusal("set_value", &[("expr", expr.to_string())])),
    }
}

fn normalize_prepare_statement_inner(
    session_id: SessionId,
    statement_id: StatementId,
    statement: &Statement,
    param_types: Vec<String>,
) -> NormalizationResult<PrepareOp> {
    match statement {
        Statement::Insert(_) | Statement::Query(_) => Ok(PrepareOp {
            session_id,
            statement_id,
            sql_hash: statement_hash(statement),
            param_types,
        }),
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::SetVariable { .. }
        | Statement::SetTransaction { .. }
        | Statement::ShowVariable { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. } => Err(prepare_refusal(
            "prepare_session_control",
            &[("statement", statement.to_string())],
        )),
        _ => Err(prepare_refusal(
            "unsupported_prepare_statement",
            &[("statement", statement.to_string())],
        )),
    }
}

fn normalize_param_types<I, S>(param_types: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    param_types
        .into_iter()
        .map(|param_type| param_type.as_ref().to_ascii_lowercase())
        .collect()
}

fn statement_hash(statement: &Statement) -> SqlHash {
    let mut digest = Sha256::new();
    digest.update(statement.to_string().as_bytes());
    format!("sha256:{:x}", digest.finalize())
}

pub fn normalize_read_sql(
    catalog: &Catalog,
    session_id: impl Into<SessionId>,
    sql: &str,
) -> Operation {
    let session_id = session_id.into();
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(error) => {
            return Operation::Refusal(read_refusal(
                "parse_error",
                &[("error", error.to_string())],
            ));
        }
    };

    if statements.len() != 1 {
        return Operation::Refusal(read_refusal(
            "multiple_statements",
            &[("count", statements.len().to_string())],
        ));
    }

    normalize_read_statement(catalog, session_id, &statements.remove(0))
}

pub fn normalize_read_statement(
    catalog: &Catalog,
    session_id: impl Into<SessionId>,
    statement: &Statement,
) -> Operation {
    match normalize_read_statement_inner(catalog, session_id.into(), statement) {
        Ok(read) => Operation::Read(read),
        Err(refusal) => Operation::Refusal(refusal),
    }
}

fn normalize_read_statement_inner(
    catalog: &Catalog,
    session_id: SessionId,
    statement: &Statement,
) -> NormalizationResult<ReadOp> {
    match statement {
        Statement::Query(query) => normalize_query_statement(catalog, session_id, query),
        other => Err(read_refusal(
            "unsupported_statement",
            &[("statement", other.to_string())],
        )),
    }
}

fn normalize_query_statement(
    catalog: &Catalog,
    session_id: SessionId,
    query: &Query,
) -> NormalizationResult<ReadOp> {
    if query.with.is_some() {
        return Err(read_refusal("select_with", &[("query", query.to_string())]));
    }
    if query.order_by.is_some() {
        return Err(read_refusal(
            "select_order_by",
            &[("query", query.to_string())],
        ));
    }
    if !query.limit_by.is_empty() {
        return Err(read_refusal(
            "select_limit_by",
            &[("query", query.to_string())],
        ));
    }
    if query.offset.is_some() {
        return Err(read_refusal(
            "select_offset",
            &[("query", query.to_string())],
        ));
    }
    if query.fetch.is_some() {
        return Err(read_refusal(
            "select_fetch",
            &[("query", query.to_string())],
        ));
    }
    if !query.locks.is_empty() {
        return Err(read_refusal(
            "select_for_update",
            &[("query", query.to_string())],
        ));
    }
    if query.for_clause.is_some() {
        return Err(read_refusal(
            "select_for_clause",
            &[("query", query.to_string())],
        ));
    }
    if query.settings.is_some() {
        return Err(read_refusal(
            "select_settings",
            &[("query", query.to_string())],
        ));
    }
    if query.format_clause.is_some() {
        return Err(read_refusal(
            "select_format",
            &[("query", query.to_string())],
        ));
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(read_refusal(
            "select_set_operation",
            &[("query", query.to_string())],
        ));
    };

    normalize_select_statement(catalog, session_id, select, query.limit.as_ref())
}

fn normalize_select_statement(
    catalog: &Catalog,
    session_id: SessionId,
    select: &Select,
    limit: Option<&Expr>,
) -> NormalizationResult<ReadOp> {
    if select.distinct.is_some() {
        return Err(read_refusal(
            "select_distinct",
            &[("query", select.to_string())],
        ));
    }
    if select.top.is_some() {
        return Err(read_refusal("select_top", &[("query", select.to_string())]));
    }
    if select.into.is_some() {
        return Err(read_refusal(
            "select_into",
            &[("query", select.to_string())],
        ));
    }
    if !select.lateral_views.is_empty() {
        return Err(read_refusal(
            "select_lateral_view",
            &[("query", select.to_string())],
        ));
    }
    if select.prewhere.is_some() {
        return Err(read_refusal(
            "select_prewhere",
            &[("query", select.to_string())],
        ));
    }
    if !select.cluster_by.is_empty() {
        return Err(read_refusal(
            "select_cluster_by",
            &[("query", select.to_string())],
        ));
    }
    if !select.distribute_by.is_empty() {
        return Err(read_refusal(
            "select_distribute_by",
            &[("query", select.to_string())],
        ));
    }
    if !select.sort_by.is_empty() {
        return Err(read_refusal(
            "select_sort_by",
            &[("query", select.to_string())],
        ));
    }
    if select.having.is_some() {
        return Err(read_refusal(
            "select_having",
            &[("query", select.to_string())],
        ));
    }
    if !select.named_window.is_empty() {
        return Err(read_refusal(
            "select_window",
            &[("query", select.to_string())],
        ));
    }
    if select.qualify.is_some() {
        return Err(read_refusal(
            "select_qualify",
            &[("query", select.to_string())],
        ));
    }
    if select.value_table_mode.is_some() {
        return Err(read_refusal(
            "select_value_table",
            &[("query", select.to_string())],
        ));
    }
    if select.connect_by.is_some() {
        return Err(read_refusal(
            "select_connect_by",
            &[("query", select.to_string())],
        ));
    }

    let (table, table_name) = normalize_read_from(catalog, &select.from)?;
    let read_projection = normalize_read_projection(table, &select.projection)?;
    let group_by = normalize_read_group_by(table, &select.group_by)?;

    if read_projection.aggregate.kind == AggregateKind::None {
        if !group_by.is_empty() {
            return Err(read_refusal(
                "aggregate_group_by",
                &[("query", select.to_string())],
            ));
        }
    } else if read_projection.projection != group_by {
        return Err(read_refusal(
            "aggregate_projection",
            &[("query", select.to_string())],
        ));
    }

    let predicate = select
        .selection
        .as_ref()
        .map(|expr| normalize_read_predicate(table, expr))
        .transpose()?;
    let limit = normalize_read_limit(limit)?;
    let shape = determine_read_shape(table, predicate.as_ref(), &read_projection.aggregate);

    Ok(ReadOp {
        session_id,
        table: table_name,
        shape,
        projection: read_projection.projection,
        predicate,
        aggregate: read_projection.aggregate,
        group_by,
        limit,
    })
}

fn normalize_read_from<'a>(
    catalog: &'a Catalog,
    from: &[TableWithJoins],
) -> NormalizationResult<(&'a TableCatalog, TableName)> {
    if from.len() != 1 {
        return Err(read_refusal(
            "select_from",
            &[("from_count", from.len().to_string())],
        ));
    }

    let from = &from[0];
    if !from.joins.is_empty() {
        return Err(read_refusal("select_join", &[("from", from.to_string())]));
    }

    match &from.relation {
        TableFactor::Table {
            name,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            ..
        } if args.is_none()
            && with_hints.is_empty()
            && version.is_none()
            && !with_ordinality
            && partitions.is_empty()
            && json_path.is_none() =>
        {
            resolve_table_with(catalog, name, read_refusal)
        }
        _ => Err(read_refusal("select_from", &[("from", from.to_string())])),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadProjection {
    projection: Vec<ColumnName>,
    aggregate: AggregateSpec,
}

enum ProjectionItem {
    Column(ColumnName),
    Aggregate(AggregateSpec),
}

fn normalize_read_projection(
    table: &TableCatalog,
    projection: &[SelectItem],
) -> NormalizationResult<ReadProjection> {
    let mut columns = Vec::new();
    let mut aggregate = AggregateSpec::default();

    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => match normalize_read_projection_expr(table, expr)? {
                ProjectionItem::Column(column) => columns.push(column),
                ProjectionItem::Aggregate(candidate) => {
                    if aggregate.kind != AggregateKind::None {
                        return Err(read_refusal(
                            "aggregate_arity",
                            &[("projection", item.to_string())],
                        ));
                    }
                    aggregate = candidate;
                }
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                match normalize_read_projection_expr(table, expr)? {
                    ProjectionItem::Column(_) => {
                        return Err(read_refusal(
                            "projection_alias",
                            &[("projection", item.to_string())],
                        ));
                    }
                    ProjectionItem::Aggregate(mut candidate) => {
                        if aggregate.kind != AggregateKind::None {
                            return Err(read_refusal(
                                "aggregate_arity",
                                &[("projection", item.to_string())],
                            ));
                        }
                        candidate.alias = Some(alias.value.clone());
                        aggregate = candidate;
                    }
                }
            }
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {
                return Err(read_refusal(
                    "select_wildcard",
                    &[("projection", item.to_string())],
                ));
            }
        }
    }

    Ok(ReadProjection {
        projection: columns,
        aggregate,
    })
}

fn normalize_read_projection_expr(
    table: &TableCatalog,
    expr: &Expr,
) -> NormalizationResult<ProjectionItem> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            resolve_column_expr_with(table, expr, read_refusal).map(ProjectionItem::Column)
        }
        Expr::Function(function) => {
            normalize_aggregate_function(table, function).map(ProjectionItem::Aggregate)
        }
        Expr::Nested(expr) => normalize_read_projection_expr(table, expr),
        _ => Err(read_refusal(
            "projection_expression",
            &[("expr", expr.to_string())],
        )),
    }
}

fn normalize_aggregate_function(
    table: &TableCatalog,
    function: &Function,
) -> NormalizationResult<AggregateSpec> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(read_refusal(
            "aggregate_shape",
            &[("function", function.to_string())],
        ));
    }

    let FunctionArguments::List(arguments) = &function.args else {
        return Err(read_refusal(
            "aggregate_shape",
            &[("function", function.to_string())],
        ));
    };

    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(read_refusal(
            "aggregate_shape",
            &[("function", function.to_string())],
        ));
    }

    let name = object_name_to_string(&function.name).to_ascii_lowercase();
    let args = &arguments.args;
    let (kind, column) = match name.as_str() {
        "count" => normalize_count_aggregate_arg(table, args)?,
        "sum" => normalize_single_column_aggregate_arg(table, args, AggregateKind::Sum)?,
        "avg" => normalize_single_column_aggregate_arg(table, args, AggregateKind::Avg)?,
        "min" => normalize_single_column_aggregate_arg(table, args, AggregateKind::Min)?,
        "max" => normalize_single_column_aggregate_arg(table, args, AggregateKind::Max)?,
        _ => {
            return Err(read_refusal(
                "aggregate_function",
                &[("function", function.name.to_string())],
            ));
        }
    };

    Ok(AggregateSpec {
        kind,
        column,
        alias: None,
    })
}

fn normalize_count_aggregate_arg(
    table: &TableCatalog,
    args: &[FunctionArg],
) -> NormalizationResult<(AggregateKind, Option<ColumnName>)> {
    match args {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => Ok((AggregateKind::Count, None)),
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))] => Ok((
            AggregateKind::Count,
            Some(resolve_column_expr_with(table, expr, read_refusal)?),
        )),
        _ => Err(read_refusal(
            "aggregate_shape",
            &[("function", String::from("count"))],
        )),
    }
}

fn normalize_single_column_aggregate_arg(
    table: &TableCatalog,
    args: &[FunctionArg],
    kind: AggregateKind,
) -> NormalizationResult<(AggregateKind, Option<ColumnName>)> {
    match args {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))] => Ok((
            kind,
            Some(resolve_column_expr_with(table, expr, read_refusal)?),
        )),
        _ => Err(read_refusal(
            "aggregate_shape",
            &[("aggregate", aggregate_kind_token(kind))],
        )),
    }
}

fn normalize_read_group_by(
    table: &TableCatalog,
    group_by: &GroupByExpr,
) -> NormalizationResult<Vec<ColumnName>> {
    match group_by {
        GroupByExpr::All(_) => Err(read_refusal(
            "aggregate_group_by_all",
            &[("group_by", group_by.to_string())],
        )),
        GroupByExpr::Expressions(expressions, modifiers) => {
            if !modifiers.is_empty() {
                return Err(read_refusal(
                    "aggregate_group_by",
                    &[("group_by", group_by.to_string())],
                ));
            }

            expressions
                .iter()
                .map(|expr| resolve_column_expr_with(table, expr, read_refusal))
                .collect()
        }
    }
}

fn normalize_read_predicate(
    table: &TableCatalog,
    expr: &Expr,
) -> NormalizationResult<PredicateExpr> {
    match strip_nested_expr(expr) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Ok(PredicateExpr::Conjunction(flatten_predicate(
            table,
            left,
            right,
            BinaryOperator::And,
        )?)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Ok(PredicateExpr::Disjunction(flatten_predicate(
            table,
            left,
            right,
            BinaryOperator::Or,
        )?)),
        expr => normalize_read_comparison(table, expr).map(PredicateExpr::Comparison),
    }
}

fn flatten_predicate(
    table: &TableCatalog,
    left: &Expr,
    right: &Expr,
    operator: BinaryOperator,
) -> NormalizationResult<Vec<PredicateComparison>> {
    let mut comparisons = Vec::new();
    collect_predicate_comparisons(table, left, &operator, &mut comparisons)?;
    collect_predicate_comparisons(table, right, &operator, &mut comparisons)?;
    Ok(comparisons)
}

fn collect_predicate_comparisons(
    table: &TableCatalog,
    expr: &Expr,
    operator: &BinaryOperator,
    comparisons: &mut Vec<PredicateComparison>,
) -> NormalizationResult<()> {
    match strip_nested_expr(expr) {
        Expr::BinaryOp { left, op, right } if op == operator => {
            collect_predicate_comparisons(table, left, operator, comparisons)?;
            collect_predicate_comparisons(table, right, operator, comparisons)
        }
        Expr::BinaryOp {
            op: BinaryOperator::And | BinaryOperator::Or,
            ..
        } => Err(read_refusal(
            "predicate_boolean_mixed",
            &[("predicate", expr.to_string())],
        )),
        expr => {
            comparisons.push(normalize_read_comparison(table, expr)?);
            Ok(())
        }
    }
}

fn normalize_read_comparison(
    table: &TableCatalog,
    expr: &Expr,
) -> NormalizationResult<PredicateComparison> {
    match strip_nested_expr(expr) {
        Expr::BinaryOp { left, op, right } => {
            normalize_binary_read_comparison(table, left, op, right)
        }
        Expr::IsNull(expr) => Ok(PredicateComparison {
            column: normalize_read_column_expr(table, expr)?,
            operator: PredicateOperator::IsNull,
            values: Vec::new(),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                return Err(read_refusal(
                    "predicate_not_in",
                    &[("predicate", expr.to_string())],
                ));
            }

            let column = normalize_read_column_expr(table, expr)?;
            let values = list
                .iter()
                .map(normalize_read_scalar_expr)
                .collect::<NormalizationResult<Vec<_>>>()?;
            validate_predicate_values(expr, &values)?;
            Ok(PredicateComparison {
                column,
                operator: PredicateOperator::InList,
                values,
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            if *negated {
                return Err(read_refusal(
                    "predicate_not_between",
                    &[("predicate", expr.to_string())],
                ));
            }

            let column = normalize_read_column_expr(table, expr)?;
            let values = [
                normalize_read_scalar_expr(low)?,
                normalize_read_scalar_expr(high)?,
            ];
            validate_predicate_values(expr, &values)?;
            Ok(PredicateComparison {
                column,
                operator: PredicateOperator::Between,
                values: values.into(),
            })
        }
        expr => Err(read_refusal(
            "predicate_expression",
            &[("expr", expr.to_string())],
        )),
    }
}

fn normalize_binary_read_comparison(
    table: &TableCatalog,
    left: &Expr,
    operator: &BinaryOperator,
    right: &Expr,
) -> NormalizationResult<PredicateComparison> {
    let normalized_operator = binary_operator_to_predicate(operator.clone())
        .ok_or_else(|| read_refusal("predicate_operator", &[("operator", operator.to_string())]))?;

    if is_column_expr(left) {
        let column = normalize_read_column_expr(table, left)?;
        let value = normalize_read_scalar_expr(right)?;
        validate_predicate_values(left, std::slice::from_ref(&value))?;
        return Ok(PredicateComparison {
            column,
            operator: normalized_operator,
            values: vec![value],
        });
    }

    if is_column_expr(right) {
        let column = normalize_read_column_expr(table, right)?;
        let value = normalize_read_scalar_expr(left)?;
        validate_predicate_values(right, std::slice::from_ref(&value))?;
        return Ok(PredicateComparison {
            column,
            operator: reverse_predicate_operator(normalized_operator),
            values: vec![value],
        });
    }

    Err(read_refusal(
        "predicate_expression",
        &[("expr", format!("{left} {operator} {right}"))],
    ))
}

fn normalize_read_column_expr(
    table: &TableCatalog,
    expr: &Expr,
) -> NormalizationResult<ColumnName> {
    resolve_column_expr_with(table, strip_nested_expr(expr), read_refusal)
}

fn normalize_read_scalar_expr(expr: &Expr) -> NormalizationResult<ScalarValue> {
    normalize_scalar_expr_with(expr, read_refusal)
}

fn validate_predicate_values(expr: &Expr, values: &[ScalarValue]) -> NormalizationResult<()> {
    if values
        .iter()
        .any(|value| matches!(value, ScalarValue::Null))
    {
        return Err(read_refusal(
            "predicate_null",
            &[("expr", expr.to_string())],
        ));
    }

    Ok(())
}

fn normalize_read_limit(limit: Option<&Expr>) -> NormalizationResult<Option<u64>> {
    let Some(limit) = limit else {
        return Ok(None);
    };

    match normalize_read_scalar_expr(limit)? {
        ScalarValue::Integer(value) if value >= 0 => Ok(Some(value as u64)),
        _ => Err(read_refusal(
            "select_limit",
            &[("limit", limit.to_string())],
        )),
    }
}

fn determine_read_shape(
    table: &TableCatalog,
    predicate: Option<&PredicateExpr>,
    aggregate: &AggregateSpec,
) -> ReadShape {
    if aggregate.kind != AggregateKind::None {
        return ReadShape::AggregateScan;
    }

    if predicate_matches_primary_key(table, predicate) {
        ReadShape::PointLookup
    } else {
        ReadShape::FilteredScan
    }
}

fn predicate_matches_primary_key(table: &TableCatalog, predicate: Option<&PredicateExpr>) -> bool {
    let Some(primary_key) = &table.primary_key else {
        return false;
    };

    let comparisons = match predicate {
        Some(PredicateExpr::Comparison(comparison)) => std::slice::from_ref(comparison),
        Some(PredicateExpr::Conjunction(comparisons)) => comparisons.as_slice(),
        Some(PredicateExpr::Disjunction(_)) | None => return false,
    };

    if comparisons.len() != primary_key.columns.len() {
        return false;
    }

    let columns = comparisons
        .iter()
        .filter(|comparison| {
            comparison.operator == PredicateOperator::Eq && comparison.values.len() == 1
        })
        .map(|comparison| comparison.column.as_str())
        .collect::<BTreeSet<_>>();
    let primary_key_columns = primary_key
        .columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    columns == primary_key_columns
}

fn binary_operator_to_predicate(operator: BinaryOperator) -> Option<PredicateOperator> {
    match operator {
        BinaryOperator::Eq => Some(PredicateOperator::Eq),
        BinaryOperator::NotEq => Some(PredicateOperator::Neq),
        BinaryOperator::Lt => Some(PredicateOperator::Lt),
        BinaryOperator::LtEq => Some(PredicateOperator::Lte),
        BinaryOperator::Gt => Some(PredicateOperator::Gt),
        BinaryOperator::GtEq => Some(PredicateOperator::Gte),
        _ => None,
    }
}

fn aggregate_kind_token(kind: AggregateKind) -> String {
    match kind {
        AggregateKind::None => String::from("none"),
        AggregateKind::Count => String::from("count"),
        AggregateKind::Sum => String::from("sum"),
        AggregateKind::Avg => String::from("avg"),
        AggregateKind::Min => String::from("min"),
        AggregateKind::Max => String::from("max"),
    }
}

fn reverse_predicate_operator(operator: PredicateOperator) -> PredicateOperator {
    match operator {
        PredicateOperator::Eq => PredicateOperator::Eq,
        PredicateOperator::Neq => PredicateOperator::Neq,
        PredicateOperator::Lt => PredicateOperator::Gt,
        PredicateOperator::Lte => PredicateOperator::Gte,
        PredicateOperator::Gt => PredicateOperator::Lt,
        PredicateOperator::Gte => PredicateOperator::Lte,
        other => other,
    }
}

fn is_column_expr(expr: &Expr) -> bool {
    matches!(
        strip_nested_expr(expr),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
    )
}

fn strip_nested_expr(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }

    expr
}

pub fn normalize_mutation_sql(
    catalog: &Catalog,
    session_id: impl Into<SessionId>,
    sql: &str,
) -> Operation {
    let session_id = session_id.into();
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(error) => {
            return Operation::Refusal(mutation_refusal(
                "parse_error",
                &[("error", error.to_string())],
            ));
        }
    };

    if statements.len() != 1 {
        return Operation::Refusal(mutation_refusal(
            "multiple_statements",
            &[("count", statements.len().to_string())],
        ));
    }

    normalize_mutation_statement(catalog, session_id, &statements.remove(0))
}

pub fn normalize_mutation_statement(
    catalog: &Catalog,
    session_id: impl Into<SessionId>,
    statement: &Statement,
) -> Operation {
    let session_id = session_id.into();

    match statement {
        Statement::Insert(insert) => {
            match normalize_insert_statement(catalog, session_id, insert) {
                Ok(mutation) => Operation::Mutation(mutation),
                Err(refusal) => Operation::Refusal(refusal),
            }
        }
        other => Operation::Refusal(mutation_refusal(
            "unsupported_statement",
            &[("statement", other.to_string())],
        )),
    }
}

fn normalize_insert_statement(
    catalog: &Catalog,
    session_id: SessionId,
    insert: &Insert,
) -> NormalizationResult<MutationOp> {
    let (table, table_name) = resolve_table(catalog, &insert.table_name)?;
    let resolved_columns = resolve_insert_columns(table, &insert.columns)?;
    let normalized_columns = canonicalize_columns(table, &resolved_columns);
    let rows = normalize_insert_rows(
        insert.source.as_deref(),
        &resolved_columns,
        &normalized_columns,
    )?;
    let returning = normalize_returning_columns(table, insert.returning.as_deref())?;
    let (kind, conflict_target, update_columns) =
        normalize_conflict_clause(table, &normalized_columns, insert.on.as_ref())?;

    Ok(MutationOp {
        session_id,
        table: table_name,
        kind,
        columns: normalized_columns,
        rows,
        conflict_target,
        update_columns,
        predicate: None,
        returning,
    })
}

fn resolve_table<'a>(
    catalog: &'a Catalog,
    object_name: &ObjectName,
) -> NormalizationResult<(&'a TableCatalog, TableName)> {
    resolve_table_with(catalog, object_name, mutation_refusal)
}

fn resolve_table_with<'a>(
    catalog: &'a Catalog,
    object_name: &ObjectName,
    refusal: RefusalBuilder,
) -> Result<(&'a TableCatalog, TableName), RefusalOp> {
    let rendered = object_name_to_string(object_name);
    if let Some(table) = catalog.table(&rendered) {
        return Ok((table, table.name.clone()));
    }

    if object_name.0.len() == 1 {
        let short_name = &object_name.0[0].value;
        let mut matches = catalog.tables.iter().filter(|table| {
            table
                .name
                .rsplit('.')
                .next()
                .is_some_and(|segment| segment == short_name)
        });

        if let (Some(table), None) = (matches.next(), matches.next()) {
            return Ok((table, table.name.clone()));
        }
    }

    Err(refusal("unknown_table", &[("table", rendered)]))
}

fn resolve_insert_columns(
    table: &TableCatalog,
    columns: &[sqlparser::ast::Ident],
) -> NormalizationResult<Vec<ColumnName>> {
    if columns.is_empty() {
        return Ok(table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect());
    }

    let mut resolved = Vec::with_capacity(columns.len());
    let mut seen = BTreeSet::new();

    for ident in columns {
        let column = resolve_column(table, &ident.value)?;
        if !seen.insert(column.clone()) {
            return Err(mutation_refusal("duplicate_column", &[("column", column)]));
        }
        resolved.push(column);
    }

    Ok(resolved)
}

fn canonicalize_columns(table: &TableCatalog, columns: &[ColumnName]) -> Vec<ColumnName> {
    let wanted = columns.iter().collect::<BTreeSet<_>>();
    table
        .columns
        .iter()
        .filter(|column| wanted.contains(&column.name))
        .map(|column| column.name.clone())
        .collect()
}

fn normalize_insert_rows(
    source: Option<&Query>,
    input_columns: &[ColumnName],
    normalized_columns: &[ColumnName],
) -> NormalizationResult<Vec<Vec<ScalarValue>>> {
    let Some(source) = source else {
        return Err(mutation_refusal(
            "mutation_source",
            &[("source", String::from("missing"))],
        ));
    };

    if source.with.is_some()
        || source.order_by.is_some()
        || source.limit.is_some()
        || !source.limit_by.is_empty()
        || source.offset.is_some()
        || source.fetch.is_some()
        || !source.locks.is_empty()
        || source.for_clause.is_some()
        || source.settings.is_some()
        || source.format_clause.is_some()
    {
        return Err(mutation_refusal(
            "mutation_source",
            &[("source", source.to_string())],
        ));
    }

    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(mutation_refusal(
            "mutation_source",
            &[("source", source.to_string())],
        ));
    };

    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        if row.len() != input_columns.len() {
            return Err(mutation_refusal(
                "row_arity",
                &[
                    ("expected", input_columns.len().to_string()),
                    ("actual", row.len().to_string()),
                ],
            ));
        }

        let resolved_values = row
            .iter()
            .map(normalize_scalar_expr)
            .collect::<NormalizationResult<Vec<_>>>()?;

        let mut by_column = BTreeMap::new();
        for (column, value) in input_columns.iter().zip(resolved_values) {
            by_column.insert(column.clone(), value);
        }

        let normalized_row = normalized_columns
            .iter()
            .map(|column| {
                by_column
                    .get(column)
                    .cloned()
                    .expect("normalized columns should be present in row mapping")
            })
            .collect();
        rows.push(normalized_row);
    }

    Ok(rows)
}

fn normalize_scalar_expr(expr: &Expr) -> NormalizationResult<ScalarValue> {
    normalize_scalar_expr_with(expr, mutation_refusal)
}

fn normalize_scalar_expr_with(
    expr: &Expr,
    refusal: RefusalBuilder,
) -> NormalizationResult<ScalarValue> {
    match expr {
        Expr::Value(value) => normalize_scalar_value_with(value, refusal),
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Minus => match normalize_scalar_expr_with(expr, refusal)? {
                ScalarValue::Integer(value) => Ok(ScalarValue::Integer(-value)),
                _ => Err(refusal("literal_value", &[("expr", expr.to_string())])),
            },
            UnaryOperator::Plus => normalize_scalar_expr_with(expr, refusal),
            _ => Err(refusal("literal_value", &[("expr", expr.to_string())])),
        },
        _ => Err(refusal("literal_value", &[("expr", expr.to_string())])),
    }
}

fn normalize_scalar_value_with(
    value: &SqlValue,
    refusal: RefusalBuilder,
) -> NormalizationResult<ScalarValue> {
    match value {
        SqlValue::Null => Ok(ScalarValue::Null),
        SqlValue::Boolean(value) => Ok(ScalarValue::Boolean(*value)),
        SqlValue::Number(value, _) => value
            .parse::<i64>()
            .map(ScalarValue::Integer)
            .map_err(|_| refusal("literal_value", &[("value", value.clone())])),
        SqlValue::SingleQuotedString(value)
        | SqlValue::DoubleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value)
        | SqlValue::NationalStringLiteral(value)
        | SqlValue::TripleSingleQuotedString(value)
        | SqlValue::TripleDoubleQuotedString(value)
        | SqlValue::SingleQuotedRawStringLiteral(value)
        | SqlValue::DoubleQuotedRawStringLiteral(value)
        | SqlValue::TripleSingleQuotedRawStringLiteral(value)
        | SqlValue::TripleDoubleQuotedRawStringLiteral(value) => {
            Ok(ScalarValue::Text(value.clone()))
        }
        SqlValue::Placeholder(value) => {
            Err(refusal("placeholder_value", &[("value", value.clone())]))
        }
        other => Err(refusal("literal_value", &[("value", other.to_string())])),
    }
}

fn normalize_returning_columns(
    table: &TableCatalog,
    returning: Option<&[SelectItem]>,
) -> NormalizationResult<Vec<ColumnName>> {
    let Some(returning) = returning else {
        return Ok(Vec::new());
    };

    returning
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => resolve_column_expr(table, expr),
            SelectItem::ExprWithAlias { .. } => Err(mutation_refusal(
                "returning_alias",
                &[("item", item.to_string())],
            )),
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => Err(mutation_refusal(
                "returning_wildcard",
                &[("item", item.to_string())],
            )),
        })
        .collect()
}

fn normalize_conflict_clause(
    table: &TableCatalog,
    normalized_columns: &[ColumnName],
    on_insert: Option<&OnInsert>,
) -> NormalizationResult<(MutationKind, Option<ConflictTarget>, Vec<ColumnName>)> {
    let Some(on_insert) = on_insert else {
        return Ok((MutationKind::Insert, None, Vec::new()));
    };

    match on_insert {
        OnInsert::DuplicateKeyUpdate(_) => Err(mutation_refusal(
            "unsupported_conflict_clause",
            &[("clause", on_insert.to_string())],
        )),
        OnInsert::OnConflict(conflict) => {
            let target = normalize_conflict_target(table, conflict.conflict_target.as_ref())?;
            match &conflict.action {
                OnConflictAction::DoNothing => Err(mutation_refusal(
                    "on_conflict_action",
                    &[("action", String::from("do_nothing"))],
                )),
                OnConflictAction::DoUpdate(update) => {
                    let update_columns = validate_upsert_assignments(
                        table,
                        normalized_columns,
                        &target,
                        &update.assignments,
                        update.selection.as_ref(),
                    )?;
                    Ok((MutationKind::Upsert, Some(target), update_columns))
                }
            }
        }
        _ => Err(mutation_refusal(
            "unsupported_conflict_clause",
            &[("clause", on_insert.to_string())],
        )),
    }
}

fn normalize_conflict_target(
    table: &TableCatalog,
    target: Option<&SqlConflictTarget>,
) -> NormalizationResult<ConflictTarget> {
    let Some(target) = target else {
        return Err(mutation_refusal("missing_conflict_target", &[]));
    };

    match target {
        SqlConflictTarget::Columns(columns) => {
            let resolved = columns
                .iter()
                .map(|column| resolve_column(table, &column.value))
                .collect::<NormalizationResult<Vec<_>>>()?;

            if table
                .primary_key
                .as_ref()
                .is_some_and(|key| key.columns == resolved)
            {
                return Ok(ConflictTarget::PrimaryKey);
            }

            if table
                .unique_constraints
                .iter()
                .any(|constraint| constraint.columns == resolved)
            {
                return Ok(ConflictTarget::Columns(resolved));
            }

            Err(mutation_refusal(
                "unknown_conflict_target",
                &[("target", columns_to_string(&resolved))],
            ))
        }
        SqlConflictTarget::OnConstraint(name) => {
            let rendered = object_name_to_string(name);
            if table
                .primary_key
                .as_ref()
                .and_then(|key| key.name.as_deref())
                .is_some_and(|name| name == rendered)
            {
                return Ok(ConflictTarget::PrimaryKey);
            }

            if table
                .unique_constraints
                .iter()
                .filter_map(|constraint| constraint.name.as_ref())
                .any(|name| name == &rendered)
            {
                return Ok(ConflictTarget::NamedConstraint(rendered));
            }

            Err(mutation_refusal(
                "unknown_conflict_target",
                &[("target", rendered)],
            ))
        }
    }
}

fn validate_upsert_assignments(
    table: &TableCatalog,
    normalized_columns: &[ColumnName],
    conflict_target: &ConflictTarget,
    assignments: &[Assignment],
    selection: Option<&Expr>,
) -> NormalizationResult<Vec<ColumnName>> {
    if let Some(selection) = selection {
        return Err(mutation_refusal(
            "on_conflict_where",
            &[("selection", selection.to_string())],
        ));
    }

    let target_columns = conflict_target_columns(table, conflict_target)?;
    let expected = normalized_columns
        .iter()
        .filter(|column| !target_columns.contains(*column))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();

    for assignment in assignments {
        let column = assignment_target_column(table, &assignment.target)?;
        let Expr::CompoundIdentifier(parts) = &assignment.value else {
            return Err(mutation_refusal(
                "on_conflict_assignment",
                &[("assignment", assignment.to_string())],
            ));
        };

        if parts.len() != 2 || !parts[0].value.eq_ignore_ascii_case("excluded") {
            return Err(mutation_refusal(
                "on_conflict_assignment",
                &[("assignment", assignment.to_string())],
            ));
        }

        let value_column = resolve_column(table, &parts[1].value)?;
        if value_column != column {
            return Err(mutation_refusal(
                "on_conflict_assignment",
                &[("assignment", assignment.to_string())],
            ));
        }

        actual.insert(column);
    }

    if !actual.is_subset(&expected) {
        return Err(mutation_refusal(
            "on_conflict_assignment_set",
            &[
                ("expected", columns_to_string(expected.iter())),
                ("actual", columns_to_string(actual.iter())),
            ],
        ));
    }

    Ok(actual.into_iter().collect())
}

fn conflict_target_columns(
    table: &TableCatalog,
    conflict_target: &ConflictTarget,
) -> NormalizationResult<Vec<ColumnName>> {
    match conflict_target {
        ConflictTarget::PrimaryKey => table
            .primary_key
            .as_ref()
            .map(|key| key.columns.clone())
            .ok_or_else(|| mutation_refusal("unknown_conflict_target", &[])),
        ConflictTarget::Columns(columns) => Ok(columns.clone()),
        ConflictTarget::NamedConstraint(name) => table
            .unique_constraints
            .iter()
            .find(|constraint| constraint.name.as_deref() == Some(name.as_str()))
            .map(|constraint| constraint.columns.clone())
            .ok_or_else(|| {
                mutation_refusal("unknown_conflict_target", &[("target", name.clone())])
            }),
    }
}

fn assignment_target_column(
    table: &TableCatalog,
    target: &AssignmentTarget,
) -> NormalizationResult<ColumnName> {
    match target {
        AssignmentTarget::ColumnName(name) => resolve_object_name_column(table, name),
        AssignmentTarget::Tuple(columns) => Err(mutation_refusal(
            "on_conflict_assignment",
            &[(
                "assignment",
                columns_to_string(columns.iter().map(object_name_to_string)),
            )],
        )),
    }
}

fn resolve_column_expr(table: &TableCatalog, expr: &Expr) -> NormalizationResult<ColumnName> {
    resolve_column_expr_with(table, expr, mutation_refusal)
}

fn resolve_column_expr_with(
    table: &TableCatalog,
    expr: &Expr,
    refusal: RefusalBuilder,
) -> Result<ColumnName, RefusalOp> {
    match expr {
        Expr::Identifier(ident) => resolve_column_with(table, &ident.value, refusal),
        Expr::CompoundIdentifier(parts) => resolve_compound_column_with(table, parts, refusal),
        _ => Err(refusal(
            "returning_expression",
            &[("expr", expr.to_string())],
        )),
    }
}

fn resolve_object_name_column(
    table: &TableCatalog,
    object_name: &ObjectName,
) -> NormalizationResult<ColumnName> {
    resolve_object_name_column_with(table, object_name, mutation_refusal)
}

fn resolve_object_name_column_with(
    table: &TableCatalog,
    object_name: &ObjectName,
    refusal: RefusalBuilder,
) -> Result<ColumnName, RefusalOp> {
    if object_name.0.is_empty() {
        return Err(refusal("unknown_column", &[]));
    }

    if object_name.0.len() == 1 {
        return resolve_column_with(table, &object_name.0[0].value, refusal);
    }

    resolve_compound_column_with(table, &object_name.0, refusal)
}

fn resolve_compound_column_with(
    table: &TableCatalog,
    parts: &[sqlparser::ast::Ident],
    refusal: RefusalBuilder,
) -> Result<ColumnName, RefusalOp> {
    if parts.is_empty() {
        return Err(refusal("unknown_column", &[]));
    }

    let column = parts
        .last()
        .map(|ident| ident.value.as_str())
        .expect("compound identifier should have a last segment");
    let prefix = parts[..parts.len() - 1]
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>();

    if prefix.is_empty() {
        return resolve_column(table, column);
    }

    let table_parts = table.name.split('.').collect::<Vec<_>>();
    if prefix == table_parts || prefix == vec![*table_parts.last().expect("table name segment")] {
        return resolve_column_with(table, column, refusal);
    }

    Err(refusal(
        "unknown_column",
        &[(
            "column",
            parts
                .iter()
                .map(|ident| ident.value.as_str())
                .collect::<Vec<_>>()
                .join("."),
        )],
    ))
}

fn resolve_column(table: &TableCatalog, name: &str) -> NormalizationResult<ColumnName> {
    resolve_column_with(table, name, mutation_refusal)
}

fn resolve_column_with(
    table: &TableCatalog,
    name: &str,
    refusal: RefusalBuilder,
) -> Result<ColumnName, RefusalOp> {
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| column.name.clone())
        .ok_or_else(|| refusal("unknown_column", &[("column", name.to_owned())]))
}

fn mutation_refusal(reason: &str, extras: &[(&str, String)]) -> RefusalOp {
    let mut detail = BTreeMap::from([(String::from("reason"), reason.to_owned())]);
    for (key, value) in extras {
        detail.insert((*key).to_owned(), value.clone());
    }

    RefusalOp {
        scope: RefusalScope::Mutation,
        code: String::from(UNSUPPORTED_SHAPE_CODE),
        detail,
    }
}

fn session_refusal(shape: &str, extras: &[(&str, String)]) -> RefusalOp {
    let mut detail = BTreeMap::from([(String::from("shape"), shape.to_owned())]);
    for (key, value) in extras {
        detail.insert((*key).to_owned(), value.clone());
    }

    RefusalOp {
        scope: RefusalScope::Session,
        code: String::from(UNSUPPORTED_SHAPE_CODE),
        detail,
    }
}

fn prepare_refusal(shape: &str, extras: &[(&str, String)]) -> RefusalOp {
    let mut detail = BTreeMap::from([(String::from("shape"), shape.to_owned())]);
    for (key, value) in extras {
        detail.insert((*key).to_owned(), value.clone());
    }

    RefusalOp {
        scope: RefusalScope::Prepare,
        code: String::from(UNSUPPORTED_SHAPE_CODE),
        detail,
    }
}

fn read_refusal(shape: &str, extras: &[(&str, String)]) -> RefusalOp {
    let mut detail = BTreeMap::from([(String::from("shape"), shape.to_owned())]);
    for (key, value) in extras {
        detail.insert((*key).to_owned(), value.clone());
    }

    RefusalOp {
        scope: RefusalScope::Read,
        code: String::from(UNSUPPORTED_SHAPE_CODE),
        detail,
    }
}

fn show_variable_shape(variable: &[sqlparser::ast::Ident]) -> &'static str {
    if variable.len() == 1 && variable[0].value.eq_ignore_ascii_case("all") {
        "show_all"
    } else {
        "show_variable"
    }
}

fn session_set_shape(variable: &str) -> String {
    format!("set_{}", variable.replace('.', "_"))
}

fn columns_to_string<I, S>(columns: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    columns
        .into_iter()
        .map(|column| column.as_ref().to_owned())
        .collect::<Vec<_>>()
        .join(",")
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::{Value, json};

    use crate::catalog::parse_postgres_schema;
    use crate::result::{
        AckResult, KernelResult, MutationResult, ReadResult, RefusalResult, ResultTag,
    };

    use super::{
        AggregateKind, AggregateSpec, ConflictTarget, MutationKind, MutationOp, Operation,
        PredicateComparison, PredicateExpr, PredicateOperator, PredicateValueArity, PrepareOp,
        ReadOp, ReadShape, RefusalOp, RefusalScope, ScalarValue, SessionOp, SessionOpKind,
        normalize_mutation_sql, normalize_prepare_sql, normalize_read_sql, normalize_session_sql,
        normalize_sync,
    };

    fn object_keys(value: &Value) -> BTreeSet<String> {
        value
            .as_object()
            .expect("json object")
            .keys()
            .cloned()
            .collect()
    }

    fn enum_token<T: serde::Serialize>(value: T) -> String {
        serde_json::to_value(value)
            .expect("serialize enum")
            .as_str()
            .expect("enum string")
            .to_owned()
    }

    #[test]
    fn predicate_operators_report_expected_value_arity() {
        assert_eq!(
            PredicateOperator::Eq.value_arity(),
            PredicateValueArity::One
        );
        assert_eq!(
            PredicateOperator::IsNull.value_arity(),
            PredicateValueArity::Zero
        );
        assert_eq!(
            PredicateOperator::InList.value_arity(),
            PredicateValueArity::OneOrMore
        );
        assert_eq!(
            PredicateOperator::Between.value_arity(),
            PredicateValueArity::Two
        );
    }

    #[test]
    fn aggregate_spec_defaults_to_no_aggregate() {
        assert_eq!(
            AggregateSpec::default(),
            AggregateSpec {
                kind: AggregateKind::None,
                column: None,
                alias: None,
            }
        );
    }

    #[test]
    fn controlled_ir_vocab_matches_plan() {
        assert_eq!(
            vec![
                enum_token(SessionOpKind::SetParam),
                enum_token(SessionOpKind::Begin),
                enum_token(SessionOpKind::Commit),
                enum_token(SessionOpKind::Rollback),
                enum_token(SessionOpKind::Sync),
            ],
            vec!["set_param", "begin", "commit", "rollback", "sync"]
        );
        assert_eq!(
            vec![
                enum_token(MutationKind::Insert),
                enum_token(MutationKind::Upsert),
                enum_token(MutationKind::Update),
                enum_token(MutationKind::Delete),
            ],
            vec!["insert", "upsert", "update", "delete"]
        );
        assert_eq!(
            vec![
                enum_token(ReadShape::PointLookup),
                enum_token(ReadShape::FilteredScan),
                enum_token(ReadShape::AggregateScan),
            ],
            vec!["point_lookup", "filtered_scan", "aggregate_scan"]
        );
        assert_eq!(
            vec![
                enum_token(AggregateKind::None),
                enum_token(AggregateKind::Count),
                enum_token(AggregateKind::Sum),
                enum_token(AggregateKind::Avg),
                enum_token(AggregateKind::Min),
                enum_token(AggregateKind::Max),
            ],
            vec!["none", "count", "sum", "avg", "min", "max"]
        );
        assert_eq!(
            vec![
                enum_token(PredicateOperator::Eq),
                enum_token(PredicateOperator::Neq),
                enum_token(PredicateOperator::Lt),
                enum_token(PredicateOperator::Lte),
                enum_token(PredicateOperator::Gt),
                enum_token(PredicateOperator::Gte),
                enum_token(PredicateOperator::IsNull),
                enum_token(PredicateOperator::InList),
                enum_token(PredicateOperator::Between),
            ],
            vec![
                "eq", "neq", "lt", "lte", "gt", "gte", "is_null", "in_list", "between",
            ]
        );
        assert_eq!(
            vec![
                enum_token(RefusalScope::Session),
                enum_token(RefusalScope::Prepare),
                enum_token(RefusalScope::Mutation),
                enum_token(RefusalScope::Read),
            ],
            vec!["session", "prepare", "mutation", "read"]
        );
    }

    #[test]
    fn operation_ir_serializes_with_snake_case_vocab() {
        let operation = Operation::Mutation(MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Upsert,
            columns: vec![String::from("deal_id"), String::from("deal_name")],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-1")),
                ScalarValue::Text(String::from("Alpha")),
            ]],
            conflict_target: None,
            update_columns: vec![String::from("deal_name")],
            predicate: Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("deal_id"),
                operator: PredicateOperator::Eq,
                values: vec![ScalarValue::Text(String::from("deal-1"))],
            })),
            returning: vec![String::from("deal_id")],
        });

        let json = serde_json::to_value(operation).expect("serialize operation");
        assert_eq!(json["op_type"], "mutation");
        assert_eq!(json["kind"], "upsert");
        assert_eq!(json["update_columns"], json!(["deal_name"]));
        assert_eq!(
            json["predicate"],
            json!({
                "comparison": {
                    "column": "deal_id",
                    "operator": "eq",
                    "values": [{"text": "deal-1"}]
                }
            })
        );
    }

    #[test]
    fn normalized_ops_keep_required_fields_and_omit_absent_optionals() {
        let session = serde_json::to_value(SessionOp {
            session_id: String::from("session-1"),
            op: SessionOpKind::Begin,
            tracked_params: BTreeMap::new(),
        })
        .expect("serialize session op");
        assert_eq!(
            object_keys(&session),
            BTreeSet::from([
                String::from("op"),
                String::from("session_id"),
                String::from("tracked_params"),
            ])
        );

        let prepare = serde_json::to_value(PrepareOp {
            session_id: String::from("session-1"),
            statement_id: String::from("stmt-1"),
            sql_hash: String::from("sha256:abc"),
            param_types: vec![String::from("text")],
        })
        .expect("serialize prepare op");
        assert_eq!(
            object_keys(&prepare),
            BTreeSet::from([
                String::from("param_types"),
                String::from("session_id"),
                String::from("sql_hash"),
                String::from("statement_id"),
            ])
        );

        let mutation = serde_json::to_value(MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Insert,
            columns: vec![String::from("deal_id")],
            rows: vec![vec![ScalarValue::Text(String::from("deal-1"))]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: None,
            returning: Vec::new(),
        })
        .expect("serialize mutation op");
        assert_eq!(
            object_keys(&mutation),
            BTreeSet::from([
                String::from("columns"),
                String::from("kind"),
                String::from("rows"),
                String::from("session_id"),
                String::from("table"),
            ])
        );

        let read = serde_json::to_value(ReadOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            shape: ReadShape::FilteredScan,
            projection: vec![String::from("deal_id")],
            predicate: None,
            aggregate: AggregateSpec::default(),
            group_by: Vec::new(),
            limit: None,
        })
        .expect("serialize read op");
        assert_eq!(
            object_keys(&read),
            BTreeSet::from([
                String::from("aggregate"),
                String::from("projection"),
                String::from("session_id"),
                String::from("shape"),
                String::from("table"),
            ])
        );
        assert_eq!(read["aggregate"], json!({"kind": "none"}));

        let refusal = serde_json::to_value(RefusalOp {
            scope: RefusalScope::Read,
            code: String::from("unsupported_shape"),
            detail: BTreeMap::new(),
        })
        .expect("serialize refusal op");
        assert_eq!(
            object_keys(&refusal),
            BTreeSet::from([
                String::from("code"),
                String::from("detail"),
                String::from("scope"),
            ])
        );
    }

    #[test]
    fn read_and_refusal_ops_stay_protocol_agnostic() {
        let read = ReadOp {
            session_id: String::from("session-2"),
            table: String::from("public.deals"),
            shape: ReadShape::AggregateScan,
            projection: vec![String::from("tenant_id")],
            predicate: None,
            aggregate: AggregateSpec {
                kind: AggregateKind::Count,
                column: None,
                alias: Some(String::from("row_count")),
            },
            group_by: vec![String::from("tenant_id")],
            limit: Some(100),
        };
        assert_eq!(read.aggregate.kind, AggregateKind::Count);
        assert!(read.predicate.is_none());

        let refusal = RefusalOp {
            scope: RefusalScope::Read,
            code: String::from("unsupported_shape"),
            detail: BTreeMap::from([(
                String::from("shape"),
                String::from("aggregate_basic_group_by"),
            )]),
        };
        assert_eq!(refusal.scope, RefusalScope::Read);
        assert!(!refusal.detail.contains_key("sqlstate"));
    }

    #[test]
    fn session_op_tracks_only_acknowledged_params() {
        let session = SessionOp {
            session_id: String::from("session-3"),
            op: SessionOpKind::SetParam,
            tracked_params: BTreeMap::from([(
                String::from("application_name"),
                String::from("extractor_canary"),
            )]),
        };

        assert_eq!(session.op, SessionOpKind::SetParam);
        assert_eq!(
            session.tracked_params.get("application_name"),
            Some(&String::from("extractor_canary"))
        );
    }

    #[test]
    fn session_control_sql_normalizes_into_declared_session_ops() {
        let begin = expect_session(normalize_session_sql("session-4", "BEGIN"))
            .expect("BEGIN should normalize into a session op");
        let commit = expect_session(normalize_session_sql("session-4", "COMMIT"))
            .expect("COMMIT should normalize into a session op");
        let rollback = expect_session(normalize_session_sql("session-4", "ROLLBACK"))
            .expect("ROLLBACK should normalize into a session op");
        let set_application_name = expect_session(normalize_session_sql(
            "session-4",
            "SET application_name = 'psql'",
        ))
        .expect("tracked SET should normalize into a session op");
        let sync = expect_session(normalize_sync("session-4"))
            .expect("SYNC should normalize into a session op");

        assert_eq!(begin.op, SessionOpKind::Begin);
        assert_eq!(commit.op, SessionOpKind::Commit);
        assert_eq!(rollback.op, SessionOpKind::Rollback);
        assert_eq!(sync.op, SessionOpKind::Sync);
        assert_eq!(
            set_application_name.tracked_params,
            BTreeMap::from([(String::from("application_name"), String::from("psql"))])
        );
    }

    #[test]
    fn prepare_surface_normalizes_statement_identity_and_param_types() {
        let select = expect_prepare(normalize_prepare_sql(
            "session-5",
            "stmt-select",
            "SELECT deal_id FROM public.deals WHERE deal_id = $1",
            ["TEXT"],
        ))
        .expect("SELECT should normalize into a prepare op");
        let insert = expect_prepare(normalize_prepare_sql(
            "session-5",
            "stmt-insert",
            "INSERT INTO public.deals (deal_id, deal_name) VALUES ($1, $2)",
            ["text", "text"],
        ))
        .expect("INSERT should normalize into a prepare op");
        let normalized_whitespace = expect_prepare(normalize_prepare_sql(
            "session-5",
            "stmt-select-2",
            " SELECT  deal_id  FROM public.deals WHERE deal_id = $1 ",
            ["text"],
        ))
        .expect("normalized SELECT should normalize into a prepare op");

        assert_eq!(select.param_types, vec![String::from("text")]);
        assert_eq!(
            insert.param_types,
            vec![String::from("text"), String::from("text")]
        );
        assert!(select.sql_hash.starts_with("sha256:"));
        assert_eq!(select.sql_hash, normalized_whitespace.sql_hash);
    }

    #[test]
    fn unsupported_session_and_prepare_near_misses_become_refusals() {
        let savepoint = expect_refusal(normalize_session_sql("session-6", "SAVEPOINT retry"))
            .expect("SAVEPOINT should refuse");
        let show_all = expect_refusal(normalize_session_sql("session-6", "SHOW ALL"))
            .expect("SHOW ALL should refuse");
        let untracked_set = expect_refusal(normalize_session_sql(
            "session-6",
            "SET client_encoding = 'UTF8'",
        ))
        .expect("untracked SET should refuse");
        let prepared_begin = expect_refusal(normalize_prepare_sql(
            "session-6",
            "stmt-begin",
            "BEGIN",
            Vec::<String>::new(),
        ))
        .expect("prepared BEGIN should refuse");

        assert_eq!(savepoint.scope, RefusalScope::Session);
        assert_eq!(
            savepoint.detail.get("shape"),
            Some(&String::from("savepoint"))
        );
        assert_eq!(
            show_all.detail.get("shape"),
            Some(&String::from("show_all"))
        );
        assert_eq!(
            untracked_set.detail.get("shape"),
            Some(&String::from("set_client_encoding"))
        );
        assert_eq!(prepared_begin.scope, RefusalScope::Prepare);
        assert_eq!(
            prepared_begin.detail.get("shape"),
            Some(&String::from("prepare_session_control"))
        );
    }

    #[test]
    fn kernel_results_serialize_with_snake_case_vocab() {
        let mutation = KernelResult::Mutation(MutationResult {
            tag: ResultTag::Upsert,
            rows_affected: 1,
            returning_rows: vec![vec![ScalarValue::Text(String::from("deal-1"))]],
        });
        let refusal = KernelResult::Refusal(RefusalResult {
            code: String::from("unsupported_shape"),
            message: String::from("window functions are outside the declared subset"),
            sqlstate: String::from("0A000"),
            detail: BTreeMap::from([(String::from("shape"), String::from("window_function"))]),
        });

        let mutation_json = serde_json::to_value(mutation).expect("serialize mutation result");
        assert_eq!(mutation_json["result_kind"], "mutation");
        assert_eq!(mutation_json["tag"], "upsert");
        assert_eq!(mutation_json["rows_affected"], 1);

        let refusal_json = serde_json::to_value(refusal).expect("serialize refusal result");
        assert_eq!(refusal_json["result_kind"], "refusal");
        assert_eq!(refusal_json["sqlstate"], "0A000");
        assert_eq!(
            refusal_json["detail"],
            json!({
                "shape": "window_function"
            })
        );
    }

    #[test]
    fn read_and_ack_results_stay_protocol_agnostic() {
        let read = ReadResult {
            columns: vec![String::from("deal_id"), String::from("deal_name")],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-1")),
                ScalarValue::Text(String::from("Alpha")),
            ]],
        };
        assert_eq!(read.columns.len(), 2);
        assert_eq!(read.rows.len(), 1);

        let ack = AckResult {
            tag: ResultTag::Begin,
            rows_affected: 0,
        };
        assert_eq!(ack.tag, ResultTag::Begin);
        assert_eq!(ack.rows_affected, 0);
    }

    #[test]
    fn equivalent_insert_shapes_normalize_to_same_mutation() {
        let catalog = mutation_catalog();
        let implicit = expect_mutation(normalize_mutation_sql(
            &catalog,
            "session-1",
            "INSERT INTO deals VALUES ('deal-1', 'alpha-1', 'Alpha') RETURNING deal_id",
        ))
        .expect("implicit insert should normalize into a mutation op");
        let explicit = expect_mutation(normalize_mutation_sql(
            &catalog,
            "session-1",
            "INSERT INTO public.deals (deal_name, external_key, deal_id) VALUES ('Alpha', 'alpha-1', 'deal-1') RETURNING public.deals.deal_id",
        ))
        .expect("explicit insert should normalize into a mutation op");

        assert_eq!(implicit, explicit);
        assert_eq!(
            implicit.columns,
            vec![
                String::from("deal_id"),
                String::from("external_key"),
                String::from("deal_name"),
            ]
        );
        assert_eq!(
            implicit.rows,
            vec![vec![
                ScalarValue::Text(String::from("deal-1")),
                ScalarValue::Text(String::from("alpha-1")),
                ScalarValue::Text(String::from("Alpha")),
            ]]
        );
        assert_eq!(implicit.returning, vec![String::from("deal_id")]);
        assert_eq!(implicit.kind, MutationKind::Insert);
    }

    #[test]
    fn upsert_conflict_targets_normalize_to_declared_ir_surface() {
        let catalog = mutation_catalog();
        let pk_upsert = expect_mutation(normalize_mutation_sql(
            &catalog,
            "session-2",
            "INSERT INTO deals (deal_name, external_key, deal_id) VALUES ('Alpha Updated', 'alpha-1', 'deal-1') ON CONFLICT (deal_id) DO UPDATE SET external_key = EXCLUDED.external_key, deal_name = EXCLUDED.deal_name RETURNING deal_id",
        ))
        .expect("primary-key upsert should normalize into a mutation op");
        let unique_upsert = expect_mutation(normalize_mutation_sql(
            &catalog,
            "session-2",
            "INSERT INTO public.deals (external_key, deal_id, deal_name) VALUES ('alpha-1', 'deal-2', 'Alpha Unique Rewrite') ON CONFLICT ON CONSTRAINT deals_external_key_key DO UPDATE SET deal_id = EXCLUDED.deal_id, deal_name = EXCLUDED.deal_name",
        ))
        .expect("named-constraint upsert should normalize into a mutation op");

        assert_eq!(pk_upsert.kind, MutationKind::Upsert);
        assert_eq!(pk_upsert.conflict_target, Some(ConflictTarget::PrimaryKey));
        assert_eq!(
            pk_upsert.update_columns,
            vec![String::from("deal_name"), String::from("external_key")]
        );
        assert_eq!(pk_upsert.returning, vec![String::from("deal_id")]);
        assert_eq!(
            unique_upsert.conflict_target,
            Some(ConflictTarget::NamedConstraint(String::from(
                "deals_external_key_key"
            )))
        );
        assert_eq!(
            unique_upsert.update_columns,
            vec![String::from("deal_id"), String::from("deal_name")]
        );
        assert_eq!(
            unique_upsert.columns,
            vec![
                String::from("deal_id"),
                String::from("external_key"),
                String::from("deal_name"),
            ]
        );
        assert_eq!(
            unique_upsert.rows,
            vec![vec![
                ScalarValue::Text(String::from("deal-2")),
                ScalarValue::Text(String::from("alpha-1")),
                ScalarValue::Text(String::from("Alpha Unique Rewrite")),
            ]]
        );
    }

    #[test]
    fn upsert_allows_omitted_unchanged_non_target_columns() {
        let catalog = mutation_catalog();
        let upsert = expect_mutation(normalize_mutation_sql(
            &catalog,
            "session-2",
            "INSERT INTO public.deals (deal_id, external_key, deal_name) VALUES ('deal-1', 'alpha-9', 'Alpha Updated') ON CONFLICT (deal_id) DO UPDATE SET external_key = EXCLUDED.external_key",
        ))
        .expect("omitted-column upsert should normalize into a mutation op");

        assert_eq!(upsert.kind, MutationKind::Upsert);
        assert_eq!(upsert.conflict_target, Some(ConflictTarget::PrimaryKey));
        assert_eq!(
            upsert.columns,
            vec![
                String::from("deal_id"),
                String::from("external_key"),
                String::from("deal_name"),
            ]
        );
        assert_eq!(upsert.update_columns, vec![String::from("external_key")]);
    }

    #[test]
    fn unsupported_mutation_near_misses_become_refusals() {
        let catalog = mutation_catalog();
        let do_nothing = expect_refusal(normalize_mutation_sql(
            &catalog,
            "session-3",
            "INSERT INTO deals (deal_id, external_key, deal_name) VALUES ('deal-1', 'alpha-1', 'Alpha') ON CONFLICT (deal_id) DO NOTHING",
        ))
        .expect("DO NOTHING should refuse");
        let insert_select = expect_refusal(normalize_mutation_sql(
            &catalog,
            "session-3",
            "INSERT INTO deals (deal_id, external_key, deal_name) SELECT deal_id, external_key, deal_name FROM deals",
        ))
        .expect("INSERT ... SELECT should refuse");
        let placeholder = expect_refusal(normalize_mutation_sql(
            &catalog,
            "session-3",
            "INSERT INTO deals (deal_id, external_key, deal_name) VALUES ($1, 'alpha-1', 'Alpha')",
        ))
        .expect("placeholder insert should refuse");

        assert_eq!(
            do_nothing.detail.get("reason"),
            Some(&String::from("on_conflict_action"))
        );
        assert_eq!(
            insert_select.detail.get("reason"),
            Some(&String::from("mutation_source"))
        );
        assert_eq!(
            placeholder.detail.get("reason"),
            Some(&String::from("placeholder_value"))
        );
    }

    #[test]
    fn declared_read_subset_normalizes_point_lookup_and_filtered_scan_shapes() {
        let catalog = read_catalog();
        let point_lookup = expect_read(normalize_read_sql(
            &catalog,
            "session-7",
            "SELECT public.deals.deal_id, public.deals.deal_name FROM public.deals WHERE public.deals.deal_id = 'deal-1'",
        ))
        .expect("point lookup should normalize into a read op");
        let filtered_scan = expect_read(normalize_read_sql(
            &catalog,
            "session-7",
            "SELECT deal_id FROM public.deals WHERE tenant_id = 'tenant-a' LIMIT 50",
        ))
        .expect("filtered scan should normalize into a read op");
        let is_null = expect_read(normalize_read_sql(
            &catalog,
            "session-7",
            "SELECT deal_id FROM deals WHERE status IS NULL",
        ))
        .expect("IS NULL read should normalize into a read op");
        let in_list = expect_read(normalize_read_sql(
            &catalog,
            "session-7",
            "SELECT deal_id FROM public.deals WHERE status IN ('open', 'won')",
        ))
        .expect("IN-list read should normalize into a read op");
        let between = expect_read(normalize_read_sql(
            &catalog,
            "session-7",
            "SELECT deal_id FROM public.deals WHERE amount_cents BETWEEN 100 AND 200",
        ))
        .expect("BETWEEN read should normalize into a read op");

        assert_eq!(point_lookup.shape, ReadShape::PointLookup);
        assert_eq!(
            point_lookup.projection,
            vec![String::from("deal_id"), String::from("deal_name")]
        );
        assert_eq!(
            point_lookup.predicate,
            Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("deal_id"),
                operator: PredicateOperator::Eq,
                values: vec![ScalarValue::Text(String::from("deal-1"))],
            }))
        );
        assert_eq!(point_lookup.aggregate, AggregateSpec::default());

        assert_eq!(filtered_scan.shape, ReadShape::FilteredScan);
        assert_eq!(filtered_scan.projection, vec![String::from("deal_id")]);
        assert_eq!(filtered_scan.limit, Some(50));
        assert_eq!(
            filtered_scan.predicate,
            Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("tenant_id"),
                operator: PredicateOperator::Eq,
                values: vec![ScalarValue::Text(String::from("tenant-a"))],
            }))
        );

        assert_eq!(is_null.shape, ReadShape::FilteredScan);
        assert_eq!(
            is_null.predicate,
            Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("status"),
                operator: PredicateOperator::IsNull,
                values: Vec::new(),
            }))
        );

        assert_eq!(
            in_list.predicate,
            Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("status"),
                operator: PredicateOperator::InList,
                values: vec![
                    ScalarValue::Text(String::from("open")),
                    ScalarValue::Text(String::from("won")),
                ],
            }))
        );
        assert_eq!(
            between.predicate,
            Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("amount_cents"),
                operator: PredicateOperator::Between,
                values: vec![ScalarValue::Integer(100), ScalarValue::Integer(200)],
            }))
        );
    }

    #[test]
    fn declared_read_subset_normalizes_bounded_aggregate_shapes() {
        let catalog = read_catalog();
        let aggregate_count = expect_read(normalize_read_sql(
            &catalog,
            "session-8",
            "SELECT COUNT(*) AS row_count FROM public.deals WHERE tenant_id = 'tenant-a'",
        ))
        .expect("COUNT aggregate should normalize into a read op");
        let aggregate_group_by = expect_read(normalize_read_sql(
            &catalog,
            "session-8",
            "SELECT tenant_id, COUNT(*) AS row_count FROM public.deals GROUP BY tenant_id LIMIT 10",
        ))
        .expect("GROUP BY aggregate should normalize into a read op");

        assert_eq!(aggregate_count.shape, ReadShape::AggregateScan);
        assert!(aggregate_count.projection.is_empty());
        assert_eq!(
            aggregate_count.aggregate,
            AggregateSpec {
                kind: AggregateKind::Count,
                column: None,
                alias: Some(String::from("row_count")),
            }
        );

        assert_eq!(aggregate_group_by.shape, ReadShape::AggregateScan);
        assert_eq!(
            aggregate_group_by.projection,
            vec![String::from("tenant_id")]
        );
        assert_eq!(aggregate_group_by.group_by, vec![String::from("tenant_id")]);
        assert_eq!(aggregate_group_by.limit, Some(10));
        assert_eq!(
            aggregate_group_by.aggregate,
            AggregateSpec {
                kind: AggregateKind::Count,
                column: None,
                alias: Some(String::from("row_count")),
            }
        );
    }

    #[test]
    fn unsupported_read_near_misses_become_refusals() {
        let catalog = read_catalog();
        let wildcard = expect_refusal(normalize_read_sql(
            &catalog,
            "session-9",
            "SELECT * FROM public.deals WHERE deal_id = 'deal-1'",
        ))
        .expect("wildcard read should refuse");
        let for_update = expect_refusal(normalize_read_sql(
            &catalog,
            "session-9",
            "SELECT deal_id FROM public.deals FOR UPDATE",
        ))
        .expect("FOR UPDATE should refuse");
        let join = expect_refusal(normalize_read_sql(
            &catalog,
            "session-9",
            "SELECT d.deal_id FROM public.deals AS d JOIN public.deals AS other ON other.deal_id = d.deal_id",
        ))
        .expect("JOIN should refuse");
        let mixed_boolean = expect_refusal(normalize_read_sql(
            &catalog,
            "session-9",
            "SELECT deal_id FROM public.deals WHERE deal_id = 'deal-1' OR (tenant_id = 'tenant-a' AND status IS NULL)",
        ))
        .expect("mixed boolean predicate should refuse");

        assert_eq!(wildcard.scope, RefusalScope::Read);
        assert_eq!(
            wildcard.detail.get("shape"),
            Some(&String::from("select_wildcard"))
        );
        assert_eq!(
            for_update.detail.get("shape"),
            Some(&String::from("select_for_update"))
        );
        assert_eq!(join.detail.get("shape"), Some(&String::from("select_join")));
        assert_eq!(
            mixed_boolean.detail.get("shape"),
            Some(&String::from("predicate_boolean_mixed"))
        );
    }

    fn mutation_catalog() -> crate::catalog::Catalog {
        parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                external_key TEXT NOT NULL,
                deal_name TEXT NOT NULL,
                CONSTRAINT deals_external_key_key UNIQUE (external_key)
            );
        "#,
        )
        .expect("mutation schema should parse")
    }

    fn read_catalog() -> crate::catalog::Catalog {
        parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                external_key TEXT NOT NULL,
                deal_name TEXT NOT NULL,
                status TEXT,
                amount_cents BIGINT,
                CONSTRAINT deals_external_key_key UNIQUE (external_key)
            );
        "#,
        )
        .expect("read schema should parse")
    }

    fn expect_mutation(operation: Operation) -> Result<MutationOp, String> {
        match operation {
            Operation::Mutation(mutation) => Ok(mutation),
            other => Err(format!("expected mutation op, got {other:?}")),
        }
    }

    fn expect_read(operation: Operation) -> Result<ReadOp, String> {
        match operation {
            Operation::Read(read) => Ok(read),
            other => Err(format!("expected read op, got {other:?}")),
        }
    }

    fn expect_session(operation: Operation) -> Result<SessionOp, String> {
        match operation {
            Operation::Session(session) => Ok(session),
            other => Err(format!("expected session op, got {other:?}")),
        }
    }

    fn expect_prepare(operation: Operation) -> Result<PrepareOp, String> {
        match operation {
            Operation::Prepare(prepare) => Ok(prepare),
            other => Err(format!("expected prepare op, got {other:?}")),
        }
    }

    fn expect_refusal(operation: Operation) -> Result<RefusalOp, String> {
        match operation {
            Operation::Refusal(refusal) => Ok(refusal),
            other => Err(format!("expected refusal op, got {other:?}")),
        }
    }
}
