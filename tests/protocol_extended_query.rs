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

const INT4_OID: u32 = 23;
const TEXT_OID: u32 = 25;

#[test]
fn extended_query_flow_round_trips_parameterized_insert_returning_and_select() {
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

    let insert_parse = parse_state.process_parse(
        "session-extended",
        ParseRequest {
            statement_name: String::from("insert_widget"),
            sql: String::from(
                "INSERT INTO public.widgets (id, name) VALUES ($1, $2) RETURNING name",
            ),
            param_types: vec![String::from("integer"), String::from("text")],
        },
    );
    assert_eq!(insert_parse, vec![parse_complete_frame()]);

    let insert_statement_describe = execute_state.process_describe(
        &catalog,
        &parse_state,
        DescribeTarget::Statement(String::from("insert_widget")),
    );
    assert_eq!(
        decode_parameter_description(&insert_statement_describe[0]),
        vec![INT4_OID, TEXT_OID]
    );
    assert_eq!(
        decode_row_description(&insert_statement_describe[1]),
        vec![(String::from("name"), TEXT_OID)]
    );

    let insert_bind = parse_state.process_bind(BindRequest {
        portal_name: String::from("insert_widget_portal"),
        statement_name: String::from("insert_widget"),
        params: vec![Some(String::from("7")), Some(String::from("Alpha"))],
        parameter_formats: Vec::new(),
        result_formats: Vec::new(),
    });
    assert_eq!(insert_bind, vec![bind_complete_frame()]);

    let insert_portal_describe = execute_state.process_describe(
        &catalog,
        &parse_state,
        DescribeTarget::Portal(String::from("insert_widget_portal")),
    );
    assert_eq!(
        decode_row_description(&insert_portal_describe[0]),
        vec![(String::from("name"), TEXT_OID)]
    );

    let insert_execute = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("insert_widget_portal"),
            max_rows: 0,
        },
    );
    assert_eq!(
        decode_row_description(&insert_execute[0]),
        vec![(String::from("name"), TEXT_OID)]
    );
    assert_eq!(
        decode_data_row(&insert_execute[1]),
        vec![Some(String::from("Alpha"))]
    );
    assert_eq!(decode_command_complete(&insert_execute[2]), "INSERT 0 1");

    let insert_sync = execute_state.process_sync(&mut session);
    assert_eq!(decode_ready_status(&insert_sync.frames[0]), b'I');

    let select_parse = parse_state.process_parse(
        "session-extended",
        ParseRequest {
            statement_name: String::from("select_widget"),
            sql: String::from("SELECT name FROM public.widgets WHERE id = $1"),
            param_types: vec![String::from("integer")],
        },
    );
    assert_eq!(select_parse, vec![parse_complete_frame()]);

    let select_statement_describe = execute_state.process_describe(
        &catalog,
        &parse_state,
        DescribeTarget::Statement(String::from("select_widget")),
    );
    assert_eq!(
        decode_parameter_description(&select_statement_describe[0]),
        vec![INT4_OID]
    );
    assert_eq!(
        decode_row_description(&select_statement_describe[1]),
        vec![(String::from("name"), TEXT_OID)]
    );

    let select_bind = parse_state.process_bind(BindRequest {
        portal_name: String::from("select_widget_portal"),
        statement_name: String::from("select_widget"),
        params: vec![Some(String::from("1"))],
        parameter_formats: Vec::new(),
        result_formats: Vec::new(),
    });
    assert_eq!(select_bind, vec![bind_complete_frame()]);

    let select_portal_describe = execute_state.process_describe(
        &catalog,
        &parse_state,
        DescribeTarget::Portal(String::from("select_widget_portal")),
    );
    assert_eq!(
        decode_row_description(&select_portal_describe[0]),
        vec![(String::from("name"), TEXT_OID)]
    );

    let select_execute = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("select_widget_portal"),
            max_rows: 0,
        },
    );
    assert_eq!(
        decode_row_description(&select_execute[0]),
        vec![(String::from("name"), TEXT_OID)]
    );
    assert_eq!(
        decode_data_row(&select_execute[1]),
        vec![Some(String::from("Seed"))]
    );
    assert_eq!(decode_command_complete(&select_execute[2]), "SELECT 1");

    let select_sync = execute_state.process_sync(&mut session);
    assert_eq!(decode_ready_status(&select_sync.frames[0]), b'I');
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
        if length < 0 {
            values.push(None);
            continue;
        }

        let length = length as usize;
        values.push(Some(
            String::from_utf8(body[cursor..cursor + length].to_vec()).expect("value should decode"),
        ));
        cursor += length;
    }

    values
}
