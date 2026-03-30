use std::collections::BTreeMap;

use crate::{
    ir::{ColumnName, ScalarValue},
    result::{AckResult, KernelResult, MutationResult, RefusalResult, ResultRow, ResultTag},
};

const BOOL_OID: u32 = 16;
const INT8_OID: u32 = 20;
const TEXT_OID: u32 = 25;
pub const DEFAULT_UNSUPPORTED_LIVE_SQLSTATE: &str = "0A000";

#[derive(Debug, Clone, Copy, Default)]
pub struct ResultFrameMetadata<'a> {
    pub returning_columns: Option<&'a [ColumnName]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolRefusalOverride<'a> {
    pub code: &'a str,
    pub sqlstate: &'a str,
    pub message: &'a str,
}

pub fn encode_kernel_result_frames(
    result: &KernelResult,
    metadata: ResultFrameMetadata<'_>,
) -> Vec<Vec<u8>> {
    match result {
        KernelResult::Ack(ack) => encode_ack_result_frames(ack),
        KernelResult::Read(read) => encode_read_result_frames(
            read.columns.as_slice(),
            &read.rows,
            &format!("SELECT {}", read.rows.len()),
        ),
        KernelResult::Mutation(mutation) => encode_mutation_result_frames(mutation, metadata),
        KernelResult::Refusal(refusal) => encode_refusal_result_frames(refusal),
    }
}

pub fn unsupported_live_shape_result(
    scope: &str,
    shape: impl Into<String>,
    override_detail: Option<ProtocolRefusalOverride<'_>>,
) -> KernelResult {
    let shape = shape.into();
    let (code, sqlstate, message) = match override_detail {
        Some(override_detail) => (
            override_detail.code.to_owned(),
            override_detail.sqlstate.to_owned(),
            override_detail.message.to_owned(),
        ),
        None => (
            String::from("unsupported_live_shape"),
            String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE),
            format!("`{shape}` is outside the declared live subset"),
        ),
    };

    KernelResult::Refusal(RefusalResult {
        code,
        message,
        sqlstate,
        detail: BTreeMap::from([
            (String::from("scope"), scope.to_owned()),
            (String::from("shape"), shape),
        ]),
    })
}

fn encode_ack_result_frames(ack: &AckResult) -> Vec<Vec<u8>> {
    match ack.tag {
        ResultTag::Sync => Vec::new(),
        _ => vec![command_complete_frame(&command_tag_for_ack(ack))],
    }
}

fn encode_mutation_result_frames(
    mutation: &MutationResult,
    metadata: ResultFrameMetadata<'_>,
) -> Vec<Vec<u8>> {
    let command_tag = command_tag_for_mutation(mutation);
    if mutation.returning_rows.is_empty() {
        return vec![command_complete_frame(&command_tag)];
    }

    match metadata.returning_columns {
        Some(returning_columns) => {
            encode_read_result_frames(returning_columns, &mutation.returning_rows, &command_tag)
        }
        None => encode_refusal_result_frames(&RefusalResult {
            code: String::from("mutation_returning_columns_required"),
            message: String::from(
                "mutation result contains RETURNING rows without declared column metadata",
            ),
            sqlstate: String::from("0A000"),
            detail: BTreeMap::from([(
                String::from("tag"),
                serde_json::to_value(mutation.tag)
                    .expect("serialize result tag")
                    .as_str()
                    .expect("result tag string")
                    .to_owned(),
            )]),
        }),
    }
}

fn encode_read_result_frames(
    columns: &[ColumnName],
    rows: &[ResultRow],
    command_tag: &str,
) -> Vec<Vec<u8>> {
    let mut frames = vec![row_description_frame(columns, rows)];
    frames.extend(rows.iter().map(data_row_frame));
    frames.push(command_complete_frame(command_tag));
    frames
}

fn encode_refusal_result_frames(refusal: &RefusalResult) -> Vec<Vec<u8>> {
    vec![error_response_frame(
        "ERROR",
        refusal.sqlstate.as_str(),
        refusal.message.as_str(),
        refusal.code.as_str(),
        Some(&refusal.detail),
    )]
}

fn command_tag_for_ack(ack: &AckResult) -> String {
    match ack.tag {
        ResultTag::SetParam => String::from("SET"),
        ResultTag::Begin => String::from("BEGIN"),
        ResultTag::Commit => String::from("COMMIT"),
        ResultTag::Rollback => String::from("ROLLBACK"),
        ResultTag::Sync => String::from("SYNC"),
        ResultTag::Insert | ResultTag::Upsert | ResultTag::Update | ResultTag::Delete => {
            format!("{} {}", command_tag_prefix(ack.tag), ack.rows_affected)
        }
    }
}

fn command_tag_for_mutation(mutation: &MutationResult) -> String {
    match mutation.tag {
        ResultTag::Insert | ResultTag::Upsert => format!("INSERT 0 {}", mutation.rows_affected),
        ResultTag::Update | ResultTag::Delete => {
            format!(
                "{} {}",
                command_tag_prefix(mutation.tag),
                mutation.rows_affected
            )
        }
        ResultTag::SetParam
        | ResultTag::Begin
        | ResultTag::Commit
        | ResultTag::Rollback
        | ResultTag::Sync => {
            format!(
                "{} {}",
                command_tag_prefix(mutation.tag),
                mutation.rows_affected
            )
        }
    }
}

fn command_tag_prefix(tag: ResultTag) -> &'static str {
    match tag {
        ResultTag::SetParam => "SET",
        ResultTag::Begin => "BEGIN",
        ResultTag::Commit => "COMMIT",
        ResultTag::Rollback => "ROLLBACK",
        ResultTag::Sync => "SYNC",
        ResultTag::Insert | ResultTag::Upsert => "INSERT",
        ResultTag::Update => "UPDATE",
        ResultTag::Delete => "DELETE",
    }
}

fn row_description_frame(columns: &[ColumnName], rows: &[ResultRow]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());

    for (column_index, column_name) in columns.iter().enumerate() {
        let (type_oid, type_size) = infer_column_type(rows, column_index);
        body.extend_from_slice(column_name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_u32.to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
        body.extend_from_slice(&type_oid.to_be_bytes());
        body.extend_from_slice(&type_size.to_be_bytes());
        body.extend_from_slice(&(-1_i32).to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
    }

    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(b'T');
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn data_row_frame(row: &ResultRow) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(row.len() as i16).to_be_bytes());
    for value in row {
        match encode_scalar_text(value) {
            Some(text) => {
                body.extend_from_slice(&(text.len() as i32).to_be_bytes());
                body.extend_from_slice(text.as_bytes());
            }
            None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }

    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(b'D');
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn command_complete_frame(command_tag: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(command_tag.as_bytes());
    body.push(0);

    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(b'C');
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn error_response_frame(
    severity: &str,
    sqlstate: &str,
    message: &str,
    code: &str,
    detail: Option<&BTreeMap<String, String>>,
) -> Vec<u8> {
    let mut body = Vec::new();
    write_error_field(&mut body, b'S', severity);
    write_error_field(&mut body, b'C', sqlstate);
    write_error_field(&mut body, b'M', message);
    write_error_field(&mut body, b'V', code);
    if let Some(detail) = detail.filter(|detail| !detail.is_empty()) {
        let detail_string = detail
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        write_error_field(&mut body, b'D', &detail_string);
    }
    body.push(0);

    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(b'E');
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn write_error_field(buffer: &mut Vec<u8>, code: u8, value: &str) {
    buffer.push(code);
    buffer.extend_from_slice(value.as_bytes());
    buffer.push(0);
}

fn infer_column_type(rows: &[ResultRow], column_index: usize) -> (u32, i16) {
    for row in rows {
        if let Some(value) = row.get(column_index) {
            match value {
                ScalarValue::Null => continue,
                ScalarValue::Boolean(_) => return (BOOL_OID, 1),
                ScalarValue::Integer(_) => return (INT8_OID, 8),
                ScalarValue::Text(_) => return (TEXT_OID, -1),
            }
        }
    }

    (TEXT_OID, -1)
}

fn encode_scalar_text(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Boolean(value) => Some(if *value {
            String::from("t")
        } else {
            String::from("f")
        }),
        ScalarValue::Integer(value) => Some(value.to_string()),
        ScalarValue::Text(value) => Some(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        ir::ScalarValue,
        protocol::postgres::session::{SessionLoop, TransactionStatus},
        result::{AckResult, KernelResult, MutationResult, ReadResult, RefusalResult, ResultTag},
    };

    use super::{
        BOOL_OID, DEFAULT_UNSUPPORTED_LIVE_SQLSTATE, INT8_OID, ProtocolRefusalOverride,
        ResultFrameMetadata, TEXT_OID, encode_kernel_result_frames, unsupported_live_shape_result,
    };

    #[test]
    fn ack_and_mutation_results_render_expected_command_complete_tags() {
        let begin = KernelResult::Ack(AckResult {
            tag: ResultTag::Begin,
            rows_affected: 0,
        });
        let insert = KernelResult::Mutation(MutationResult {
            tag: ResultTag::Insert,
            rows_affected: 2,
            returning_rows: Vec::new(),
        });
        let sync = KernelResult::Ack(AckResult {
            tag: ResultTag::Sync,
            rows_affected: 0,
        });

        assert_eq!(
            decode_command_complete(
                &encode_kernel_result_frames(&begin, ResultFrameMetadata::default())[0]
            ),
            "BEGIN"
        );
        assert_eq!(
            decode_command_complete(
                &encode_kernel_result_frames(&insert, ResultFrameMetadata::default())[0]
            ),
            "INSERT 0 2"
        );
        assert!(encode_kernel_result_frames(&sync, ResultFrameMetadata::default()).is_empty());
    }

    #[test]
    fn read_results_render_row_description_data_rows_and_select_tag() {
        let read = KernelResult::Read(ReadResult {
            columns: vec![
                String::from("deal_id"),
                String::from("active"),
                String::from("batch_count"),
                String::from("note"),
            ],
            rows: vec![
                vec![
                    ScalarValue::Text(String::from("deal-1")),
                    ScalarValue::Boolean(true),
                    ScalarValue::Integer(7),
                    ScalarValue::Null,
                ],
                vec![
                    ScalarValue::Text(String::from("deal-2")),
                    ScalarValue::Boolean(false),
                    ScalarValue::Integer(9),
                    ScalarValue::Text(String::from("ok")),
                ],
            ],
        });

        let frames = encode_kernel_result_frames(&read, ResultFrameMetadata::default());
        assert_eq!(
            decode_row_description(&frames[0]),
            vec![
                (String::from("deal_id"), TEXT_OID),
                (String::from("active"), BOOL_OID),
                (String::from("batch_count"), INT8_OID),
                (String::from("note"), TEXT_OID),
            ]
        );
        assert_eq!(
            decode_data_row(&frames[1]),
            vec![
                Some(String::from("deal-1")),
                Some(String::from("t")),
                Some(String::from("7")),
                None,
            ]
        );
        assert_eq!(decode_data_row(&frames[2])[1], Some(String::from("f")));
        assert_eq!(decode_command_complete(&frames[3]), "SELECT 2");
    }

    #[test]
    fn mutation_returning_rows_use_supplied_column_metadata() {
        let mutation = KernelResult::Mutation(MutationResult {
            tag: ResultTag::Insert,
            rows_affected: 1,
            returning_rows: vec![vec![
                ScalarValue::Text(String::from("deal-1")),
                ScalarValue::Integer(7),
            ]],
        });
        let returning_columns = vec![String::from("deal_id"), String::from("batch_count")];

        let frames = encode_kernel_result_frames(
            &mutation,
            ResultFrameMetadata {
                returning_columns: Some(returning_columns.as_slice()),
            },
        );

        assert_eq!(
            decode_row_description(&frames[0]),
            vec![
                (String::from("deal_id"), TEXT_OID),
                (String::from("batch_count"), INT8_OID),
            ]
        );
        assert_eq!(
            decode_data_row(&frames[1]),
            vec![Some(String::from("deal-1")), Some(String::from("7"))]
        );
        assert_eq!(decode_command_complete(&frames[2]), "INSERT 0 1");
    }

    #[test]
    fn refusal_results_render_deterministic_error_fields() {
        let refusal = KernelResult::Refusal(RefusalResult {
            code: String::from("unsupported_live_shape"),
            message: String::from("COPY is outside the declared live subset"),
            sqlstate: String::from("0A000"),
            detail: BTreeMap::from([
                (String::from("scope"), String::from("read")),
                (String::from("statement"), String::from("copy")),
            ]),
        });

        let frames = encode_kernel_result_frames(&refusal, ResultFrameMetadata::default());
        assert_eq!(
            decode_error_fields(&frames[0]),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from("0A000")),
                (
                    'M',
                    String::from("COPY is outside the declared live subset")
                ),
                ('V', String::from("unsupported_live_shape")),
                ('D', String::from("scope=read; statement=copy")),
            ]
        );
    }

    #[test]
    fn unsupported_live_shape_defaults_to_0a000_with_stable_detail() {
        let refusal = unsupported_live_shape_result("read", "copy", None);

        let frames = encode_kernel_result_frames(&refusal, ResultFrameMetadata::default());
        assert_eq!(
            decode_error_fields(&frames[0]),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE)),
                (
                    'M',
                    String::from("`copy` is outside the declared live subset")
                ),
                ('V', String::from("unsupported_live_shape")),
                ('D', String::from("scope=read; shape=copy")),
            ]
        );
    }

    #[test]
    fn narrower_protocol_refusal_override_is_preserved_in_error_frame() {
        let refusal = unsupported_live_shape_result(
            "read",
            "missing_column",
            Some(ProtocolRefusalOverride {
                code: "undefined_column",
                sqlstate: "42703",
                message: "column `missing_column` does not exist",
            }),
        );

        let frames = encode_kernel_result_frames(&refusal, ResultFrameMetadata::default());
        assert_eq!(
            decode_error_fields(&frames[0]),
            vec![
                ('S', String::from("ERROR")),
                ('C', String::from("42703")),
                ('M', String::from("column `missing_column` does not exist")),
                ('V', String::from("undefined_column")),
                ('D', String::from("scope=read; shape=missing_column")),
            ]
        );
    }

    #[test]
    fn protocol_visible_refusals_keep_session_alive_after_error_framing() {
        let begin = KernelResult::Ack(AckResult {
            tag: ResultTag::Begin,
            rows_affected: 0,
        });
        let refusal = unsupported_live_shape_result("read", "copy", None);
        let mut session = SessionLoop::new();

        session.process_result(&begin, ResultFrameMetadata::default());
        let cycle = session.process_result(&refusal, ResultFrameMetadata::default());

        assert_eq!(
            cycle.transaction_status,
            TransactionStatus::FailedTransaction
        );
        assert_eq!(
            decode_error_fields(cycle.frames.first().expect("error frame"))[1],
            ('C', String::from(DEFAULT_UNSUPPORTED_LIVE_SQLSTATE))
        );
        assert_eq!(cycle.frames.last().expect("ready frame")[0], b'Z');
        assert_eq!(cycle.frames.last().expect("ready frame")[5], b'E');
    }

    fn decode_command_complete(frame: &[u8]) -> String {
        assert_eq!(frame[0], b'C');
        String::from_utf8(frame[5..frame.len() - 1].to_vec()).expect("command tag")
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
