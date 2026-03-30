#![forbid(unsafe_code)]

use twinning::{
    backend::BaseSnapshotBackend,
    catalog::parse_postgres_schema,
    kernel::{storage::TableStorage, value::KernelValue},
    protocol::postgres::{
        extended_execute::{DescribeTarget, ExecuteRequest, ExtendedExecuteState},
        extended_parse::{BindRequest, ExtendedParseState, ParseRequest},
        session::SessionLoop,
    },
};

const TEXT_OID: u32 = 25;

#[test]
fn extended_query_metadata_probes_return_declared_single_text_rows() {
    let catalog = parse_postgres_schema(
        r#"
        CREATE TABLE public.widgets (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        "#,
    )
    .expect("schema should parse");

    let mut storage = TableStorage::new(
        catalog
            .table("public.widgets")
            .expect("widgets table should exist"),
    )
    .expect("widgets storage should build");
    storage
        .insert_row(vec![
            KernelValue::Integer(1),
            KernelValue::Text(String::from("Seed")),
        ])
        .expect("seed row should insert");
    let mut backend = BaseSnapshotBackend::new([storage]).expect("backend should build");

    let mut parse_state = ExtendedParseState::new();
    let mut execute_state = ExtendedExecuteState::new();
    let mut session = SessionLoop::new();

    for (statement_name, sql, column_name, value) in [
        (
            "stmt-version",
            "SELECT pg_catalog.version()",
            "version",
            "PostgreSQL 16.0 (twinning)",
        ),
        (
            "stmt-schema",
            "SELECT current_schema()",
            "current_schema",
            "public",
        ),
        (
            "stmt-isolation",
            "SHOW transaction isolation level",
            "transaction_isolation",
            "read committed",
        ),
        (
            "stmt-strings",
            "SHOW standard_conforming_strings",
            "standard_conforming_strings",
            "on",
        ),
    ] {
        let parse = parse_state.process_parse(
            "session-metadata",
            ParseRequest {
                statement_name: statement_name.to_owned(),
                sql: sql.to_owned(),
                param_types: Vec::new(),
            },
        );
        assert_eq!(parse, vec![parse_complete_frame()]);

        let statement_describe = execute_state.process_describe(
            &catalog,
            &parse_state,
            DescribeTarget::Statement(statement_name.to_owned()),
        );
        assert_eq!(
            decode_parameter_description(&statement_describe[0]),
            Vec::<u32>::new()
        );
        assert_eq!(
            decode_row_description(&statement_describe[1]),
            vec![(String::from(column_name), TEXT_OID)]
        );

        let bind = parse_state.process_bind(BindRequest {
            portal_name: format!("{statement_name}-portal"),
            statement_name: statement_name.to_owned(),
            params: Vec::new(),
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        });
        assert_eq!(bind, vec![bind_complete_frame()]);

        let portal_describe = execute_state.process_describe(
            &catalog,
            &parse_state,
            DescribeTarget::Portal(format!("{statement_name}-portal")),
        );
        assert_eq!(
            decode_row_description(&portal_describe[0]),
            vec![(String::from(column_name), TEXT_OID)]
        );

        let execute = execute_state.process_execute(
            &catalog,
            &mut backend,
            &parse_state,
            ExecuteRequest {
                portal_name: format!("{statement_name}-portal"),
                max_rows: 0,
            },
        );
        assert_eq!(
            decode_row_description(&execute[0]),
            vec![(String::from(column_name), TEXT_OID)]
        );
        assert_eq!(
            decode_data_row(&execute[1]),
            vec![Some(String::from(value))]
        );
        assert_eq!(decode_command_complete(&execute[2]), "SELECT 1");

        let sync = execute_state.process_sync(&mut session);
        assert_eq!(decode_ready_status(&sync.frames[0]), b'I');
    }
}

#[test]
fn broader_reflection_queries_stay_explicitly_unsupported() {
    let catalog = parse_postgres_schema(
        r#"
        CREATE TABLE public.widgets (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        "#,
    )
    .expect("schema should parse");

    let mut parse_state = ExtendedParseState::new();
    let parse = parse_state.process_parse(
        "session-reflect",
        ParseRequest {
            statement_name: String::from("stmt-reflect"),
            sql: String::from("SELECT table_name FROM information_schema.tables"),
            param_types: Vec::new(),
        },
    );
    assert_eq!(parse, vec![parse_complete_frame()]);

    let describe = ExtendedExecuteState::new().process_describe(
        &catalog,
        &parse_state,
        DescribeTarget::Statement(String::from("stmt-reflect")),
    );
    assert_eq!(
        decode_error_fields(&describe[0]),
        vec![
            ('S', String::from("ERROR")),
            ('C', String::from("0A000")),
            (
                'M',
                String::from(
                    "extended query describe shape `unknown_table` is outside the declared read subset"
                ),
            ),
            ('V', String::from("unsupported_shape")),
            (
                'D',
                String::from(
                    "phase=describe; shape=unknown_table; statement_name=stmt-reflect; table=information_schema.tables; transport=extended_query",
                ),
            ),
        ]
    );
}

fn parse_complete_frame() -> Vec<u8> {
    vec![b'1', 0, 0, 0, 4]
}

fn bind_complete_frame() -> Vec<u8> {
    vec![b'2', 0, 0, 0, 4]
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
        let name = String::from_utf8(body[cursor..cursor + name_end].to_vec())
            .expect("field name should decode");
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
        if length == -1 {
            values.push(None);
            continue;
        }

        let length = length as usize;
        let value = String::from_utf8(body[cursor..cursor + length].to_vec())
            .expect("field value should decode");
        cursor += length;
        values.push(Some(value));
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
