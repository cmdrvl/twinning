use crate::{
    ir::{Operation, RefusalOp, RefusalScope, normalize_session_sql},
    result::{AckResult, KernelResult, RefusalResult, ResultTag},
};

use super::{
    catalog::classify_metadata_query,
    frames::{DEFAULT_UNSUPPORTED_LIVE_SQLSTATE, ResultFrameMetadata},
    session::{SessionCycle, SessionLoop},
};

pub fn dispatch_simple_query(
    session: &mut SessionLoop,
    session_id: impl Into<String>,
    sql: &str,
) -> SessionCycle {
    let result = dispatch_simple_query_result(session_id, sql);
    session.process_result(&result, ResultFrameMetadata::default())
}

pub fn dispatch_simple_query_result(session_id: impl Into<String>, sql: &str) -> KernelResult {
    if let Some(metadata_result) = metadata_query_result(sql) {
        return metadata_result;
    }

    match normalize_session_sql(session_id, sql) {
        Operation::Session(session) => KernelResult::Ack(AckResult {
            tag: ResultTag::from(session.op),
            rows_affected: 0,
        }),
        Operation::Refusal(refusal) => KernelResult::Refusal(refusal_into_result(refusal)),
        Operation::Prepare(_) | Operation::Mutation(_) | Operation::Read(_) => {
            unreachable!("simple-query dispatch normalizes only session SQL in this lane")
        }
    }
}

fn metadata_query_result(sql: &str) -> Option<KernelResult> {
    classify_metadata_query(sql).map(|metadata_query| KernelResult::Read(metadata_query.result()))
}

fn refusal_into_result(refusal: RefusalOp) -> RefusalResult {
    let shape = refusal
        .detail
        .get("shape")
        .cloned()
        .unwrap_or_else(|| refusal.code.clone());
    let mut detail = refusal.detail;
    detail.insert(String::from("transport"), String::from("simple_query"));

    RefusalResult {
        code: refusal.code,
        message: refusal_message(refusal.scope, &shape),
        sqlstate: String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE),
        detail,
    }
}

fn refusal_message(scope: RefusalScope, shape: &str) -> String {
    match shape {
        "parse_error" => String::from("simple query parse failed against the declared live subset"),
        "multiple_statements" => {
            String::from("simple query must contain exactly one declared statement")
        }
        other => format!(
            "simple query shape `{other}` is outside the declared {} subset",
            refusal_scope_token(scope)
        ),
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

#[cfg(test)]
mod tests {
    use crate::ir::SessionOpKind;
    use crate::protocol::postgres::session::TransactionStatus;

    use super::dispatch_simple_query;
    use super::{DEFAULT_UNSUPPORTED_LIVE_SQLSTATE, SessionLoop};

    #[test]
    fn declared_session_simple_queries_render_command_complete_frames() {
        let mut session = SessionLoop::new();

        let begin = dispatch_simple_query(&mut session, "psql-1", "BEGIN");
        assert_eq!(decode_command_complete(&begin.frames[0]), "BEGIN");
        assert_eq!(
            decode_ready_status(begin.frames.last().expect("ready frame")),
            b'T'
        );
        assert_eq!(begin.transaction_status, TransactionStatus::InTransaction);

        let set_application_name =
            dispatch_simple_query(&mut session, "psql-1", "SET application_name = 'psql'");
        assert_eq!(
            decode_command_complete(&set_application_name.frames[0]),
            "SET"
        );
        assert_eq!(
            decode_ready_status(set_application_name.frames.last().expect("ready frame")),
            b'T'
        );

        let rollback = dispatch_simple_query(&mut session, "psql-1", "ROLLBACK");
        assert_eq!(decode_command_complete(&rollback.frames[0]), "ROLLBACK");
        assert_eq!(
            decode_ready_status(rollback.frames.last().expect("ready frame")),
            b'I'
        );
        assert_eq!(rollback.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            ResultTag::from(SessionOpKind::SetParam),
            ResultTag::SetParam
        );
    }

    #[test]
    fn metadata_bootstrap_queries_render_read_rows_without_changing_idle_session() {
        let mut session = SessionLoop::new();

        let version = dispatch_simple_query(&mut session, "sqlalchemy-1", "SELECT version()");
        assert_eq!(
            decode_row_description(version.frames.first().expect("row description")),
            vec![(String::from("version"), String::from("text"))]
        );
        assert_eq!(
            decode_data_row(version.frames.get(1).expect("data row")),
            vec![Some(String::from("PostgreSQL 16.0 (twinning)"))]
        );
        assert_eq!(decode_command_complete(&version.frames[2]), "SELECT 1");
        assert_eq!(version.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            decode_ready_status(version.frames.last().expect("ready frame")),
            b'I'
        );

        let current_schema =
            dispatch_simple_query(&mut session, "sqlalchemy-1", "SELECT current_schema()");
        assert_eq!(
            decode_data_row(current_schema.frames.get(1).expect("data row")),
            vec![Some(String::from("public"))]
        );
        assert_eq!(current_schema.transaction_status, TransactionStatus::Idle);

        let isolation = dispatch_simple_query(
            &mut session,
            "sqlalchemy-1",
            "SHOW transaction isolation level",
        );
        assert_eq!(
            decode_data_row(isolation.frames.get(1).expect("data row")),
            vec![Some(String::from("read committed"))]
        );

        let standard_conforming_strings = dispatch_simple_query(
            &mut session,
            "sqlalchemy-1",
            "SHOW standard_conforming_strings",
        );
        assert_eq!(
            decode_data_row(standard_conforming_strings.frames.get(1).expect("data row")),
            vec![Some(String::from("on"))]
        );
    }

    #[test]
    fn unsupported_show_all_simple_query_stays_protocol_visible_and_recoverable() {
        let mut session = SessionLoop::new();
        dispatch_simple_query(&mut session, "psql-2", "BEGIN");

        let refusal = dispatch_simple_query(&mut session, "psql-2", "SHOW ALL");
        assert_eq!(
            refusal.transaction_status,
            TransactionStatus::FailedTransaction
        );
        assert_eq!(
            decode_error_fields(refusal.frames.first().expect("error frame")),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE)),
                (
                    'M',
                    String::from(
                        "simple query shape `show_all` is outside the declared session subset",
                    ),
                ),
                ('V', String::from("unsupported_shape")),
                (
                    'D',
                    String::from("shape=show_all; statement=SHOW ALL; transport=simple_query"),
                ),
            ]
        );
        assert_eq!(
            decode_ready_status(refusal.frames.last().expect("ready frame")),
            b'E'
        );

        let rollback = dispatch_simple_query(&mut session, "psql-2", "ROLLBACK");
        assert_eq!(decode_command_complete(&rollback.frames[0]), "ROLLBACK");
        assert_eq!(rollback.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            decode_ready_status(rollback.frames.last().expect("ready frame")),
            b'I'
        );
    }

    #[test]
    fn multi_statement_simple_query_refuses_without_killing_idle_session() {
        let mut session = SessionLoop::new();

        let refusal = dispatch_simple_query(&mut session, "psql-3", "BEGIN; ROLLBACK");
        assert_eq!(refusal.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            decode_error_fields(refusal.frames.first().expect("error frame")),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE)),
                (
                    'M',
                    String::from("simple query must contain exactly one declared statement"),
                ),
                ('V', String::from("unsupported_shape")),
                (
                    'D',
                    String::from("count=2; shape=multiple_statements; transport=simple_query"),
                ),
            ]
        );
        assert_eq!(
            decode_ready_status(refusal.frames.last().expect("ready frame")),
            b'I'
        );
    }

    #[test]
    fn unsupported_reflection_query_stays_protocol_visible() {
        let mut session = SessionLoop::new();

        let refusal = dispatch_simple_query(
            &mut session,
            "sqlalchemy-2",
            "SELECT table_name FROM information_schema.tables",
        );
        assert_eq!(refusal.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            decode_error_fields(refusal.frames.first().expect("error frame"))[1],
            ('C', String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE))
        );
        assert_eq!(
            decode_error_fields(refusal.frames.first().expect("error frame"))[3],
            ('V', String::from("unsupported_shape"))
        );
        assert_eq!(
            decode_ready_status(refusal.frames.last().expect("ready frame")),
            b'I'
        );
    }

    fn decode_command_complete(frame: &[u8]) -> String {
        assert_eq!(frame[0], b'C');
        String::from_utf8(frame[5..frame.len() - 1].to_vec()).expect("command tag")
    }

    fn decode_ready_status(frame: &[u8]) -> u8 {
        assert_eq!(frame[0], b'Z');
        frame[5]
    }

    fn decode_row_description(frame: &[u8]) -> Vec<(String, String)> {
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
            cursor += 4;
            cursor += 2;
            cursor += 4;
            cursor += 2;
            fields.push((name, String::from("text")));
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
                String::from_utf8(body[cursor..cursor + length].to_vec()).expect("field value"),
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

    use crate::result::ResultTag;
}
