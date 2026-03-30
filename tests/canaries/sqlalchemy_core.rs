use std::{
    collections::BTreeSet,
    fs,
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use serde_json::Value;
use twinning::{
    backend::BaseSnapshotBackend,
    catalog::parse_postgres_schema,
    kernel::{storage::TableStorage, value::KernelValue},
    protocol::postgres::{
        extended_execute::{DescribeTarget, ExecuteRequest, ExtendedExecuteState},
        extended_parse::{BindRequest, ExtendedParseState, ParseRequest},
        listener::PgwireListener,
        session::SessionLoop,
        simple_query::dispatch_simple_query,
    },
};

use super::assertions::assert_unsupported_refusal_sqlstate;

#[test]
fn sqlalchemy_core_metadata_subset_is_supported() {
    let fixture_dir = crate::support::canary_fixture_dir_for_test(stringify!(sqlalchemy_core));
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for sqlalchemy_core"
    );

    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let handle = thread::spawn(move || listener.accept("sqlalchemy-core"));

    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set timeout");

    write_startup_packet(
        &mut client,
        &[
            ("user", "postgres"),
            ("database", "postgres"),
            ("application_name", "sqlalchemy"),
        ],
    );
    let startup_frames = read_until_ready(&mut client).expect("startup frames");
    assert!(startup_frames.iter().any(|frame| frame[0] == b'R'));
    assert_eq!(
        decode_ready_status(startup_frames.last().expect("ready frame")),
        b'I'
    );

    write_query_message(&mut client, "SELECT pg_catalog.version()");
    let version_frames = read_until_ready(&mut client).expect("version frames");
    assert_eq!(
        decode_row_description(version_frames.first().expect("row description")),
        vec![String::from("version")]
    );
    assert_eq!(
        decode_data_row(version_frames.get(1).expect("data row")),
        vec![Some(String::from("PostgreSQL 16.0 (twinning)"))]
    );
    assert_eq!(decode_command_complete(&version_frames[2]), "SELECT 1");
    assert_eq!(
        decode_ready_status(version_frames.last().expect("ready frame")),
        b'I'
    );

    write_query_message(&mut client, "SELECT current_schema()");
    let current_schema_frames = read_until_ready(&mut client).expect("current schema frames");
    assert_eq!(
        decode_row_description(current_schema_frames.first().expect("row description")),
        vec![String::from("current_schema")]
    );
    assert_eq!(
        decode_data_row(current_schema_frames.get(1).expect("data row")),
        vec![Some(String::from("public"))]
    );

    write_query_message(&mut client, "SHOW transaction isolation level");
    let isolation_frames = read_until_ready(&mut client).expect("isolation frames");
    assert_eq!(
        decode_row_description(isolation_frames.first().expect("row description")),
        vec![String::from("transaction_isolation")]
    );
    assert_eq!(
        decode_data_row(isolation_frames.get(1).expect("data row")),
        vec![Some(String::from("read committed"))]
    );

    write_query_message(&mut client, "SHOW standard_conforming_strings");
    let standard_conforming_strings_frames =
        read_until_ready(&mut client).expect("standard conforming strings frames");
    assert_eq!(
        decode_row_description(
            standard_conforming_strings_frames
                .first()
                .expect("row description")
        ),
        vec![String::from("standard_conforming_strings")]
    );
    assert_eq!(
        decode_data_row(standard_conforming_strings_frames.get(1).expect("data row")),
        vec![Some(String::from("on"))]
    );

    write_query_message(
        &mut client,
        "SELECT table_name FROM information_schema.tables",
    );
    let refusal_frames = read_until_ready(&mut client).expect("refusal frames");
    assert_unsupported_refusal_sqlstate(
        Some(decode_error_sqlstate(&refusal_frames[0]).as_str()),
        "sqlalchemy_core reflection refusal",
    );
    assert_eq!(
        decode_error_field(&refusal_frames[0], b'V').as_deref(),
        Some("unsupported_shape")
    );
    assert_eq!(
        decode_ready_status(refusal_frames.last().expect("ready frame")),
        b'I'
    );

    write_terminate_message(&mut client);
    handle
        .join()
        .expect("listener thread")
        .expect("serve connection");
}

#[test]
fn sqlalchemy_core() {
    let fixture_dir = crate::support::canary_fixture_dir_for_test(stringify!(sqlalchemy_core));
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for sqlalchemy_core"
    );

    let fixture_path = fixture_dir.join("ir_equivalence.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(&fixture_path).expect("read sqlalchemy ir equivalence fixture"),
    )
    .expect("parse sqlalchemy ir equivalence fixture");
    let manifest = crate::support::canary_by_id(stringify!(sqlalchemy_core));

    let covered_shapes: BTreeSet<String> = fixture["supported_cases"]
        .as_array()
        .expect("supported_cases array")
        .iter()
        .map(|case| {
            case["covers_shape"]
                .as_str()
                .unwrap_or_else(|| {
                    panic!(
                        "covers_shape should be a string in `{}`",
                        fixture_path.display()
                    )
                })
                .to_owned()
        })
        .collect();
    let refusal_shapes: BTreeSet<String> = fixture["refusal_cases"]
        .as_array()
        .expect("refusal_cases array")
        .iter()
        .map(|case| {
            case["near_miss_shape"]
                .as_str()
                .unwrap_or_else(|| {
                    panic!(
                        "near_miss_shape should be a string in `{}`",
                        fixture_path.display()
                    )
                })
                .to_owned()
        })
        .collect();

    assert_eq!(
        covered_shapes,
        manifest
            .session_shapes
            .iter()
            .filter(|shape| **shape == "tx_begin" || **shape == "tx_commit")
            .cloned()
            .chain(manifest.write_shapes.iter().cloned())
            .chain(manifest.read_shapes.iter().cloned())
            .collect()
    );
    assert!(
        refusal_shapes.contains("select_for_update"),
        "fixture should preserve the explicit SQLAlchemy FOR UPDATE refusal near miss"
    );

    let catalog = parse_postgres_schema(
        r#"
        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            deal_name TEXT NOT NULL
        );
        "#,
    )
    .expect("schema should parse");
    let mut storage = TableStorage::new(
        catalog
            .table("public.deals")
            .expect("deals table should exist"),
    )
    .expect("deals storage should build");
    storage
        .insert_row(vec![
            KernelValue::Text(String::from("deal-1")),
            KernelValue::Text(String::from("Alpha")),
        ])
        .expect("seed row should insert");
    let mut backend = BaseSnapshotBackend::new([storage]).expect("backend should build");

    let mut parse_state = ExtendedParseState::new();
    let mut execute_state = ExtendedExecuteState::new();
    let mut session = SessionLoop::new();

    let begin = dispatch_simple_query(&mut session, "sqlalchemy-1", "BEGIN");
    assert_eq!(decode_command_complete(&begin.frames[0]), "BEGIN");
    assert_eq!(
        decode_ready_status(begin.frames.last().expect("ready frame")),
        b'T'
    );

    assert_eq!(
        parse_state.process_parse(
            "sqlalchemy-1",
            ParseRequest {
                statement_name: String::from("sqlalchemy_insert_values"),
                sql: String::from("INSERT INTO public.deals (deal_id, deal_name) VALUES ($1, $2)"),
                param_types: vec![String::from("text"), String::from("text")],
            },
        ),
        vec![parse_complete_frame()]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("sqlalchemy_insert_values_portal"),
            statement_name: String::from("sqlalchemy_insert_values"),
            params: vec![Some(String::from("deal-2")), Some(String::from("Beta"))],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );
    let insert_frames = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("sqlalchemy_insert_values_portal"),
            max_rows: 0,
        },
    );
    assert_eq!(decode_command_complete(&insert_frames[0]), "INSERT 0 1");
    assert_eq!(
        decode_ready_status(&execute_state.process_sync(&mut session).frames[0]),
        b'T'
    );

    assert_eq!(
        parse_state.process_parse(
            "sqlalchemy-1",
            ParseRequest {
                statement_name: String::from("sqlalchemy_select_by_pk"),
                sql: String::from(
                    "SELECT deal_id, deal_name FROM public.deals WHERE deal_id = $1",
                ),
                param_types: vec![String::from("text")],
            },
        ),
        vec![parse_complete_frame()]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("sqlalchemy_select_by_pk_portal"),
            statement_name: String::from("sqlalchemy_select_by_pk"),
            params: vec![Some(String::from("deal-1"))],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );
    let select_frames = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("sqlalchemy_select_by_pk_portal"),
            max_rows: 0,
        },
    );
    assert_eq!(
        decode_row_description(select_frames.first().expect("row description")),
        vec![String::from("deal_id"), String::from("deal_name")]
    );
    assert_eq!(
        decode_data_row(select_frames.get(1).expect("data row")),
        vec![Some(String::from("deal-1")), Some(String::from("Alpha"))]
    );
    assert_eq!(decode_command_complete(&select_frames[2]), "SELECT 1");
    assert_eq!(
        decode_ready_status(&execute_state.process_sync(&mut session).frames[0]),
        b'T'
    );

    assert_eq!(
        parse_state.process_parse(
            "sqlalchemy-1",
            ParseRequest {
                statement_name: String::from("sqlalchemy_select_for_update"),
                sql: String::from("SELECT deal_id, deal_name FROM public.deals FOR UPDATE"),
                param_types: Vec::new(),
            },
        ),
        vec![parse_complete_frame()]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("sqlalchemy_select_for_update_portal"),
            statement_name: String::from("sqlalchemy_select_for_update"),
            params: Vec::new(),
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );
    let refusal_frames = execute_state.process_describe(
        &catalog,
        &parse_state,
        DescribeTarget::Portal(String::from("sqlalchemy_select_for_update_portal")),
    );
    assert_unsupported_refusal_sqlstate(
        Some(decode_error_sqlstate(&refusal_frames[0]).as_str()),
        "sqlalchemy_core select_for_update refusal",
    );
    assert_eq!(
        decode_error_field(&refusal_frames[0], b'V').as_deref(),
        Some("unsupported_shape")
    );

    let commit = dispatch_simple_query(&mut session, "sqlalchemy-1", "COMMIT");
    assert_eq!(decode_command_complete(&commit.frames[0]), "COMMIT");
    assert_eq!(
        decode_ready_status(commit.frames.last().expect("ready frame")),
        b'I'
    );
}

fn parse_complete_frame() -> Vec<u8> {
    vec![b'1', 0, 0, 0, 4]
}

fn bind_complete_frame() -> Vec<u8> {
    vec![b'2', 0, 0, 0, 4]
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

fn write_query_message(stream: &mut TcpStream, sql: &str) {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    write_framed_message(stream, b'Q', &body);
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

fn decode_row_description(frame: &[u8]) -> Vec<String> {
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
        let name = String::from_utf8(body[cursor..cursor + name_end].to_vec()).expect("field name");
        cursor += name_end + 1;
        cursor += 4;
        cursor += 2;
        cursor += 4;
        cursor += 2;
        cursor += 4;
        cursor += 2;
        fields.push(name);
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

fn decode_command_complete(frame: &[u8]) -> String {
    assert_eq!(frame[0], b'C');
    String::from_utf8(frame[5..frame.len() - 1].to_vec()).expect("command tag")
}

fn decode_ready_status(frame: &[u8]) -> u8 {
    assert_eq!(frame[0], b'Z');
    frame[5]
}

fn decode_error_sqlstate(frame: &[u8]) -> String {
    decode_error_field(frame, b'C').expect("sqlstate field")
}

fn decode_error_field(frame: &[u8], field: u8) -> Option<String> {
    assert_eq!(frame[0], b'E');

    let mut offset = 5usize;
    while offset < frame.len() {
        let field_type = frame[offset];
        offset += 1;
        if field_type == 0 {
            break;
        }

        let terminator = frame[offset..]
            .iter()
            .position(|byte| *byte == 0)
            .expect("nul-terminated field");
        let value =
            String::from_utf8(frame[offset..offset + terminator].to_vec()).expect("utf8 field");
        offset += terminator + 1;

        if field_type == field {
            return Some(value);
        }
    }

    None
}
