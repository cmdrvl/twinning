#![forbid(unsafe_code)]

use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use twinning::{
    backend::BaseSnapshotBackend,
    catalog::{Catalog, parse_postgres_schema},
    kernel::{storage::TableStorage, value::KernelValue},
    protocol::postgres::{
        extended_execute::{DescribeTarget, ExecuteRequest, ExtendedExecuteState},
        extended_parse::{BindRequest, ExtendedParseState, ParseRequest},
        listener::PgwireListener,
        session::SessionLoop,
    },
};

const INT4_OID: u32 = 23;
const TEXT_OID: u32 = 25;

#[test]
fn extended_query_flow_round_trips_parameterized_insert_returning_and_select() {
    let (catalog, mut backend) = widgets_catalog_and_backend();

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

#[test]
fn live_listener_dispatches_extended_query_messages_over_socket() {
    let (catalog, backend) = widgets_catalog_and_backend();
    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let handle =
        thread::spawn(move || listener.accept_with_backend("socket-extended", catalog, backend));

    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set timeout");

    write_startup_packet(
        &mut client,
        &[("user", "postgres"), ("database", "postgres")],
    );
    let startup_frames = read_until_ready(&mut client).expect("startup frames");
    assert!(startup_frames.iter().any(|frame| frame[0] == b'R'));
    assert_eq!(
        decode_ready_status(startup_frames.last().expect("startup ready")),
        b'I'
    );

    write_parse_message(
        &mut client,
        "insert_widget",
        "INSERT INTO public.widgets (id, name) VALUES ($1, $2) RETURNING name",
        &[INT4_OID, TEXT_OID],
    );
    write_describe_statement_message(&mut client, "insert_widget");
    write_sync_message(&mut client);
    let insert_describe = read_until_ready(&mut client).expect("insert describe frames");
    assert_eq!(insert_describe[0], parse_complete_frame());
    assert_eq!(
        decode_parameter_description(&insert_describe[1]),
        vec![INT4_OID, TEXT_OID]
    );
    assert_eq!(
        decode_row_description(&insert_describe[2]),
        vec![(String::from("name"), TEXT_OID)]
    );

    write_query_message(&mut client, "BEGIN");
    let begin = read_until_ready(&mut client).expect("begin frames");
    assert_eq!(decode_command_complete(&begin[0]), "BEGIN");

    write_bind_message(
        &mut client,
        "insert_widget_portal",
        "insert_widget",
        &[Some("7"), Some("Alpha")],
    );
    write_describe_portal_message(&mut client, "insert_widget_portal");
    write_execute_message(&mut client, "insert_widget_portal", 0);
    write_sync_message(&mut client);
    let insert_execute = read_until_ready(&mut client).expect("insert execute frames");
    assert_eq!(insert_execute[0], bind_complete_frame());
    assert_eq!(
        decode_row_description(&insert_execute[1]),
        vec![(String::from("name"), TEXT_OID)]
    );
    assert_eq!(
        decode_row_description(&insert_execute[2]),
        vec![(String::from("name"), TEXT_OID)]
    );
    assert_eq!(
        decode_data_row(&insert_execute[3]),
        vec![Some(String::from("Alpha"))]
    );
    assert_eq!(decode_command_complete(&insert_execute[4]), "INSERT 0 1");
    assert_eq!(
        decode_ready_status(insert_execute.last().expect("insert ready")),
        b'T'
    );

    write_query_message(&mut client, "COMMIT");
    let commit = read_until_ready(&mut client).expect("commit frames");
    assert_eq!(decode_command_complete(&commit[0]), "COMMIT");

    write_parse_message(
        &mut client,
        "select_widget",
        "SELECT name FROM public.widgets WHERE id = $1",
        &[INT4_OID],
    );
    write_bind_message(
        &mut client,
        "select_widget_portal",
        "select_widget",
        &[Some("7")],
    );
    write_execute_message(&mut client, "select_widget_portal", 0);
    write_sync_message(&mut client);
    let select_execute = read_until_ready(&mut client).expect("select execute frames");
    assert_eq!(select_execute[0], parse_complete_frame());
    assert_eq!(select_execute[1], bind_complete_frame());
    assert_eq!(
        decode_row_description(&select_execute[2]),
        vec![(String::from("name"), TEXT_OID)]
    );
    assert_eq!(
        decode_data_row(&select_execute[3]),
        vec![Some(String::from("Alpha"))]
    );
    assert_eq!(decode_command_complete(&select_execute[4]), "SELECT 1");

    write_parse_message(&mut client, "bad_begin", "BEGIN", &[]);
    write_sync_message(&mut client);
    let refusal_frames = read_until_ready(&mut client).expect("parse refusal frames");
    assert_eq!(
        decode_error_sqlstate(&refusal_frames[0]).expect("SQLSTATE field"),
        "0A000"
    );
    assert_eq!(
        decode_ready_status(refusal_frames.last().expect("refusal ready")),
        b'I'
    );

    write_terminate_message(&mut client);
    handle
        .join()
        .expect("listener thread")
        .expect("serve extended connection");
}

fn widgets_catalog_and_backend() -> (Catalog, BaseSnapshotBackend) {
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
    let backend = BaseSnapshotBackend::new([storage]).expect("backend should build");

    (catalog, backend)
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

fn decode_error_sqlstate(frame: &[u8]) -> Result<String, &'static str> {
    assert_eq!(frame[0], b'E');
    let mut cursor = 5usize;
    while cursor < frame.len() {
        let field_type = frame[cursor];
        cursor += 1;
        if field_type == 0 {
            break;
        }

        let value_end = frame[cursor..]
            .iter()
            .position(|byte| *byte == 0)
            .ok_or("field terminator")?;
        let value = String::from_utf8(frame[cursor..cursor + value_end].to_vec())
            .map_err(|_| "field value")?;
        cursor += value_end + 1;

        if field_type == b'C' {
            return Ok(value);
        }
    }

    Err("missing SQLSTATE field")
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

fn write_startup_packet(stream: &mut TcpStream, params: &[(&str, &str)]) {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608_u32.to_be_bytes());
    for (name, value) in params {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);

    let mut frame = Vec::new();
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    stream.write_all(&frame).expect("write startup");
    stream.flush().expect("flush startup");
}

fn write_parse_message(
    stream: &mut TcpStream,
    statement_name: &str,
    sql: &str,
    param_oids: &[u32],
) {
    let mut body = Vec::new();
    body.extend_from_slice(statement_name.as_bytes());
    body.push(0);
    body.extend_from_slice(sql.as_bytes());
    body.push(0);
    body.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
    for oid in param_oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    write_framed_message(stream, b'P', &body);
}

fn write_query_message(stream: &mut TcpStream, sql: &str) {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    write_framed_message(stream, b'Q', &body);
}

fn write_bind_message(
    stream: &mut TcpStream,
    portal_name: &str,
    statement_name: &str,
    params: &[Option<&str>],
) {
    let mut body = Vec::new();
    body.extend_from_slice(portal_name.as_bytes());
    body.push(0);
    body.extend_from_slice(statement_name.as_bytes());
    body.push(0);
    body.extend_from_slice(&0_i16.to_be_bytes());
    body.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        match param {
            Some(value) => {
                body.extend_from_slice(&(value.len() as i32).to_be_bytes());
                body.extend_from_slice(value.as_bytes());
            }
            None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
    body.extend_from_slice(&0_i16.to_be_bytes());
    write_framed_message(stream, b'B', &body);
}

fn write_describe_statement_message(stream: &mut TcpStream, statement_name: &str) {
    write_describe_message(stream, b'S', statement_name);
}

fn write_describe_portal_message(stream: &mut TcpStream, portal_name: &str) {
    write_describe_message(stream, b'P', portal_name);
}

fn write_describe_message(stream: &mut TcpStream, target: u8, name: &str) {
    let mut body = Vec::new();
    body.push(target);
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    write_framed_message(stream, b'D', &body);
}

fn write_execute_message(stream: &mut TcpStream, portal_name: &str, max_rows: i32) {
    let mut body = Vec::new();
    body.extend_from_slice(portal_name.as_bytes());
    body.push(0);
    body.extend_from_slice(&max_rows.to_be_bytes());
    write_framed_message(stream, b'E', &body);
}

fn write_sync_message(stream: &mut TcpStream) {
    write_framed_message(stream, b'S', &[]);
}

fn write_terminate_message(stream: &mut TcpStream) {
    write_framed_message(stream, b'X', &[]);
}

fn write_framed_message(stream: &mut TcpStream, tag: u8, body: &[u8]) {
    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(tag);
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(body);
    stream.write_all(&frame).expect("write message");
    stream.flush().expect("flush message");
}

fn read_until_ready(stream: &mut TcpStream) -> std::io::Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    loop {
        let frame = read_backend_frame(stream)?;
        let done = frame[0] == b'Z';
        frames.push(frame);
        if done {
            return Ok(frames);
        }
    }
}

fn read_backend_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag)?;
    let mut length_bytes = [0_u8; 4];
    stream.read_exact(&mut length_bytes)?;
    let length = i32::from_be_bytes(length_bytes);
    let mut body = vec![0_u8; length as usize - 4];
    stream.read_exact(&mut body)?;

    let mut frame = Vec::with_capacity(length as usize + 1);
    frame.push(tag[0]);
    frame.extend_from_slice(&length_bytes);
    frame.extend_from_slice(&body);
    Ok(frame)
}
