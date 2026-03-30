use std::collections::BTreeMap;

use crate::{
    ir::{Operation, PrepareOp, normalize_prepare_sql},
    protocol::postgres::frames::DEFAULT_UNSUPPORTED_LIVE_SQLSTATE,
    result::{KernelResult, RefusalResult},
};

use super::{
    catalog::{MetadataQuery, classify_metadata_query},
    frames::{ResultFrameMetadata, encode_kernel_result_frames},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExtendedParseState {
    prepared_statements: BTreeMap<String, PreparedStatementState>,
    portals: BTreeMap<String, PortalState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseRequest {
    pub statement_name: String,
    pub sql: String,
    pub param_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindRequest {
    pub portal_name: String,
    pub statement_name: String,
    pub params: Vec<Option<String>>,
    pub parameter_formats: Vec<i16>,
    pub result_formats: Vec<i16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStatementState {
    pub statement_name: String,
    pub sql: String,
    pub prepare: Option<PrepareOp>,
    pub(crate) metadata_query: Option<MetadataQuery>,
    pub parameter_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortalState {
    pub portal_name: String,
    pub statement_name: String,
    pub sql: String,
    pub sql_hash: String,
    pub param_types: Vec<String>,
    pub parameter_count: usize,
    pub params: Vec<BoundParameter>,
    pub result_formats: Vec<ValueFormat>,
    pub(crate) metadata_query: Option<MetadataQuery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundParameter {
    pub format: ValueFormat,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueFormat {
    Text,
}

impl ExtendedParseState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn prepared_statement(&self, statement_name: &str) -> Option<&PreparedStatementState> {
        self.prepared_statements.get(statement_name)
    }

    pub fn portal(&self, portal_name: &str) -> Option<&PortalState> {
        self.portals.get(portal_name)
    }

    pub fn process_parse(
        &mut self,
        session_id: impl Into<String>,
        request: ParseRequest,
    ) -> Vec<Vec<u8>> {
        match self.parse(session_id, request) {
            Ok(_) => vec![parse_complete_frame()],
            Err(refusal) => encode_refusal_frames(&refusal),
        }
    }

    pub fn process_bind(&mut self, request: BindRequest) -> Vec<Vec<u8>> {
        match self.bind(request) {
            Ok(_) => vec![bind_complete_frame()],
            Err(refusal) => encode_refusal_frames(&refusal),
        }
    }

    fn parse(
        &mut self,
        session_id: impl Into<String>,
        request: ParseRequest,
    ) -> Result<&PreparedStatementState, RefusalResult> {
        let parameter_count = infer_parameter_count(&request.sql);
        if !request.param_types.is_empty() && request.param_types.len() != parameter_count {
            return Err(parameter_arity_refusal(
                "parse",
                parameter_count,
                request.param_types.len(),
                &[
                    ("statement_name", request.statement_name.clone()),
                    ("sql", request.sql.clone()),
                ],
            ));
        }

        let (prepare, metadata_query) = match classify_metadata_query(&request.sql) {
            Some(metadata_query) => (None, Some(metadata_query)),
            None => {
                let prepare = match normalize_prepare_sql(
                    session_id,
                    request.statement_name.clone(),
                    &request.sql,
                    request.param_types.iter().map(String::as_str),
                ) {
                    Operation::Prepare(prepare) => prepare,
                    Operation::Refusal(refusal) => {
                        return Err(parse_refusal_result(
                            refusal.code,
                            refusal
                                .detail
                                .get("shape")
                                .cloned()
                                .unwrap_or_else(|| String::from("unsupported_shape")),
                            refusal.detail,
                            &request.statement_name,
                        ));
                    }
                    Operation::Session(_) | Operation::Mutation(_) | Operation::Read(_) => {
                        unreachable!("prepare normalization returns either Prepare or Refusal")
                    }
                };
                (Some(prepare), None)
            }
        };

        self.portals
            .retain(|_, portal| portal.statement_name != request.statement_name);
        self.prepared_statements.insert(
            request.statement_name.clone(),
            PreparedStatementState {
                statement_name: request.statement_name.clone(),
                sql: request.sql,
                prepare,
                metadata_query,
                parameter_count,
            },
        );

        Ok(self
            .prepared_statements
            .get(request.statement_name.as_str())
            .expect("prepared statement should be inserted"))
    }

    fn bind(&mut self, request: BindRequest) -> Result<&PortalState, RefusalResult> {
        let prepared = self
            .prepared_statements
            .get(request.statement_name.as_str())
            .ok_or_else(|| missing_prepared_statement_refusal(&request.statement_name))?
            .clone();

        if request.params.len() != prepared.parameter_count {
            return Err(parameter_arity_refusal(
                "bind",
                prepared.parameter_count,
                request.params.len(),
                &[
                    ("portal_name", request.portal_name.clone()),
                    ("statement_name", request.statement_name.clone()),
                ],
            ));
        }

        let parameter_formats =
            resolve_parameter_formats(&request.parameter_formats, prepared.parameter_count)?;
        let result_formats = resolve_result_formats(&request.result_formats)?;

        let params = request
            .params
            .into_iter()
            .zip(parameter_formats)
            .map(|(value, format)| BoundParameter { format, value })
            .collect();

        self.portals.insert(
            request.portal_name.clone(),
            PortalState {
                portal_name: request.portal_name.clone(),
                statement_name: request.statement_name.clone(),
                sql: prepared.sql.clone(),
                sql_hash: prepared
                    .prepare
                    .as_ref()
                    .map_or_else(String::new, |prepare| prepare.sql_hash.clone()),
                param_types: prepared
                    .prepare
                    .as_ref()
                    .map_or_else(Vec::new, |prepare| prepare.param_types.clone()),
                parameter_count: prepared.parameter_count,
                params,
                result_formats,
                metadata_query: prepared.metadata_query,
            },
        );

        Ok(self
            .portals
            .get(request.portal_name.as_str())
            .expect("portal should be inserted"))
    }
}

pub fn parse_complete_frame() -> Vec<u8> {
    vec![b'1', 0, 0, 0, 4]
}

pub fn bind_complete_frame() -> Vec<u8> {
    vec![b'2', 0, 0, 0, 4]
}

fn encode_refusal_frames(refusal: &RefusalResult) -> Vec<Vec<u8>> {
    encode_kernel_result_frames(
        &KernelResult::Refusal(refusal.clone()),
        ResultFrameMetadata::default(),
    )
}

fn parse_refusal_result(
    code: String,
    shape: String,
    mut detail: BTreeMap<String, String>,
    statement_name: &str,
) -> RefusalResult {
    detail.insert(String::from("phase"), String::from("parse"));
    detail.insert(String::from("transport"), String::from("extended_query"));
    detail.insert(String::from("statement_name"), statement_name.to_owned());

    RefusalResult {
        code,
        message: format!(
            "extended query parse shape `{shape}` is outside the declared prepare subset"
        ),
        sqlstate: String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE),
        detail,
    }
}

fn missing_prepared_statement_refusal(statement_name: &str) -> RefusalResult {
    RefusalResult {
        code: String::from("undefined_prepared_statement"),
        message: format!("prepared statement `{statement_name}` does not exist"),
        sqlstate: String::from("26000"),
        detail: BTreeMap::from([
            (String::from("phase"), String::from("bind")),
            (String::from("transport"), String::from("extended_query")),
            (String::from("statement_name"), statement_name.to_owned()),
        ]),
    }
}

fn parameter_arity_refusal(
    phase: &str,
    expected: usize,
    actual: usize,
    extras: &[(&str, String)],
) -> RefusalResult {
    let mut detail = BTreeMap::from([
        (String::from("phase"), phase.to_owned()),
        (String::from("transport"), String::from("extended_query")),
        (String::from("expected"), expected.to_string()),
        (String::from("actual"), actual.to_string()),
    ]);
    for (key, value) in extras {
        detail.insert((*key).to_owned(), value.clone());
    }

    RefusalResult {
        code: String::from("parameter_arity_mismatch"),
        message: format!(
            "{phase} received {actual} parameter values for a statement requiring {expected}"
        ),
        sqlstate: String::from("08P01"),
        detail,
    }
}

fn unsupported_bind_shape_refusal(shape: &str, extras: &[(&str, String)]) -> RefusalResult {
    let mut detail = BTreeMap::from([
        (String::from("phase"), String::from("bind")),
        (String::from("transport"), String::from("extended_query")),
        (String::from("shape"), shape.to_owned()),
    ]);
    for (key, value) in extras {
        detail.insert((*key).to_owned(), value.clone());
    }

    RefusalResult {
        code: String::from("unsupported_shape"),
        message: format!("extended query bind shape `{shape}` is outside the declared subset"),
        sqlstate: String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE),
        detail,
    }
}

fn resolve_parameter_formats(
    parameter_formats: &[i16],
    parameter_count: usize,
) -> Result<Vec<ValueFormat>, RefusalResult> {
    match parameter_formats {
        [] => Ok(vec![ValueFormat::Text; parameter_count]),
        [format] => Ok(vec![
            resolve_format_code(*format, "parameter_format")?;
            parameter_count
        ]),
        formats if formats.len() == parameter_count => formats
            .iter()
            .map(|format| resolve_format_code(*format, "parameter_format"))
            .collect(),
        formats => Err(unsupported_bind_shape_refusal(
            "parameter_format_arity",
            &[
                ("declared_formats", formats.len().to_string()),
                ("parameter_count", parameter_count.to_string()),
            ],
        )),
    }
}

fn resolve_result_formats(result_formats: &[i16]) -> Result<Vec<ValueFormat>, RefusalResult> {
    match result_formats {
        [] => Ok(Vec::new()),
        [format] => Ok(vec![resolve_format_code(*format, "result_format")?]),
        formats => Err(unsupported_bind_shape_refusal(
            "result_format_arity",
            &[("declared_formats", formats.len().to_string())],
        )),
    }
}

fn resolve_format_code(code: i16, shape: &str) -> Result<ValueFormat, RefusalResult> {
    match code {
        0 => Ok(ValueFormat::Text),
        other => Err(unsupported_bind_shape_refusal(
            shape,
            &[("format_code", other.to_string())],
        )),
    }
}

fn infer_parameter_count(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut index = 0usize;
    let mut max_parameter = 0usize;

    while index < bytes.len() {
        if bytes[index] == b'$' {
            let start = index + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start {
                let parameter = sql[start..end]
                    .parse::<usize>()
                    .expect("placeholder digits should parse");
                max_parameter = max_parameter.max(parameter);
                index = end;
                continue;
            }
        }
        index += 1;
    }

    max_parameter
}

#[cfg(test)]
mod tests {
    use super::{
        BindRequest, ExtendedParseState, ParseRequest, bind_complete_frame, parse_complete_frame,
    };

    #[test]
    fn parse_stores_prepared_statement_state_and_returns_parse_complete() {
        let mut state = ExtendedParseState::new();

        let frames = state.process_parse(
            "session-1",
            ParseRequest {
                statement_name: String::from("stmt-select"),
                sql: String::from("SELECT deal_id FROM public.deals WHERE deal_id = $1"),
                param_types: Vec::new(),
            },
        );

        assert_eq!(frames, vec![parse_complete_frame()]);
        let prepared = state
            .prepared_statement("stmt-select")
            .expect("prepared state should be stored");
        assert_eq!(prepared.parameter_count, 1);
        assert_eq!(
            prepared
                .prepare
                .as_ref()
                .expect("prepared state should exist")
                .statement_id,
            "stmt-select"
        );
        assert!(
            prepared
                .prepare
                .as_ref()
                .expect("prepared state should exist")
                .sql_hash
                .starts_with("sha256:")
        );
    }

    #[test]
    fn parse_refuses_unsupported_prepare_shapes_with_protocol_visible_error() {
        let mut state = ExtendedParseState::new();

        let frames = state.process_parse(
            "session-2",
            ParseRequest {
                statement_name: String::from("stmt-begin"),
                sql: String::from("BEGIN"),
                param_types: Vec::new(),
            },
        );

        assert_eq!(
            decode_error_fields(&frames[0]),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from("0A000")),
                (
                    'M',
                    String::from(
                        "extended query parse shape `prepare_session_control` is outside the declared prepare subset",
                    ),
                ),
                ('V', String::from("unsupported_shape")),
                (
                    'D',
                    String::from(
                        "phase=parse; shape=prepare_session_control; statement=BEGIN; statement_name=stmt-begin; transport=extended_query",
                    ),
                ),
            ]
        );
        assert!(state.prepared_statement("stmt-begin").is_none());
    }

    #[test]
    fn bind_stores_portal_state_and_returns_bind_complete() {
        let mut state = ExtendedParseState::new();
        state.process_parse(
            "session-3",
            ParseRequest {
                statement_name: String::from("stmt-insert"),
                sql: String::from("INSERT INTO public.deals (deal_id, deal_name) VALUES ($1, $2)"),
                param_types: vec![String::from("text"), String::from("text")],
            },
        );

        let frames = state.process_bind(BindRequest {
            portal_name: String::from("portal-1"),
            statement_name: String::from("stmt-insert"),
            params: vec![Some(String::from("deal-1")), Some(String::from("Alpha"))],
            parameter_formats: Vec::new(),
            result_formats: vec![0],
        });

        assert_eq!(frames, vec![bind_complete_frame()]);
        let portal = state
            .portal("portal-1")
            .expect("portal state should be stored");
        assert_eq!(portal.parameter_count, 2);
        assert_eq!(
            portal.param_types,
            vec![String::from("text"), String::from("text")]
        );
        assert_eq!(portal.params[0].value.as_deref(), Some("deal-1"));
        assert_eq!(portal.result_formats.len(), 1);
    }

    #[test]
    fn bind_refuses_parameter_arity_mismatch_with_protocol_violation() {
        let mut state = ExtendedParseState::new();
        state.process_parse(
            "session-4",
            ParseRequest {
                statement_name: String::from("stmt-select"),
                sql: String::from("SELECT deal_id FROM public.deals WHERE deal_id = $1"),
                param_types: Vec::new(),
            },
        );

        let frames = state.process_bind(BindRequest {
            portal_name: String::from("portal-2"),
            statement_name: String::from("stmt-select"),
            params: Vec::new(),
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });

        assert_eq!(
            decode_error_fields(&frames[0]),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from("08P01")),
                (
                    'M',
                    String::from("bind received 0 parameter values for a statement requiring 1"),
                ),
                ('V', String::from("parameter_arity_mismatch")),
                (
                    'D',
                    String::from(
                        "actual=0; expected=1; phase=bind; portal_name=portal-2; statement_name=stmt-select; transport=extended_query",
                    ),
                ),
            ]
        );
        assert!(state.portal("portal-2").is_none());
    }

    #[test]
    fn bind_refuses_binary_parameter_formats_without_killing_prepared_state() {
        let mut state = ExtendedParseState::new();
        state.process_parse(
            "session-5",
            ParseRequest {
                statement_name: String::from("stmt-select"),
                sql: String::from("SELECT deal_id FROM public.deals WHERE deal_id = $1"),
                param_types: Vec::new(),
            },
        );

        let frames = state.process_bind(BindRequest {
            portal_name: String::from("portal-3"),
            statement_name: String::from("stmt-select"),
            params: vec![Some(String::from("deal-1"))],
            parameter_formats: vec![1],
            result_formats: Vec::new(),
        });

        assert_eq!(
            decode_error_fields(&frames[0]),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from("0A000")),
                (
                    'M',
                    String::from(
                        "extended query bind shape `parameter_format` is outside the declared subset",
                    ),
                ),
                ('V', String::from("unsupported_shape")),
                (
                    'D',
                    String::from(
                        "format_code=1; phase=bind; shape=parameter_format; transport=extended_query",
                    ),
                ),
            ]
        );
        assert!(state.portal("portal-3").is_none());
        assert!(state.prepared_statement("stmt-select").is_some());
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
