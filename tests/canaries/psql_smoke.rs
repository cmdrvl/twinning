use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use twinning::protocol::postgres::listener::PgwireListener;

use super::assertions::assert_unsupported_refusal_sqlstate;

#[test]
fn psql_smoke() {
    let fixture_dir = crate::support::canary_fixture_dir_for_test(stringify!(psql_smoke));
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for psql_smoke"
    );

    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let handle = thread::spawn(move || listener.accept("psql-smoke"));

    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set timeout");

    write_startup_packet(
        &mut client,
        &[
            ("user", "postgres"),
            ("database", "postgres"),
            ("application_name", "psql"),
        ],
    );
    let startup_frames = read_until_ready(&mut client).expect("startup frames");
    assert!(startup_frames.iter().any(|frame| frame[0] == b'R'));
    assert!(startup_frames.iter().any(|frame| frame[0] == b'K'));
    assert_eq!(
        decode_parameter_statuses(&startup_frames),
        BTreeMap::from([
            (String::from("DateStyle"), String::from("ISO, MDY")),
            (String::from("application_name"), String::from("psql")),
            (String::from("client_encoding"), String::from("UTF8")),
            (String::from("integer_datetimes"), String::from("on")),
            (String::from("server_encoding"), String::from("UTF8")),
            (String::from("server_version"), String::from("16.0")),
            (
                String::from("standard_conforming_strings"),
                String::from("on"),
            ),
        ])
    );
    assert_eq!(
        decode_ready_status(startup_frames.last().expect("ready frame")),
        b'I'
    );

    write_query_message(&mut client, "SET application_name = 'psql'");
    let set_frames = read_until_ready(&mut client).expect("set frames");
    assert_eq!(decode_command_complete(&set_frames[0]), "SET");
    assert_eq!(
        decode_ready_status(set_frames.last().expect("ready frame")),
        b'I'
    );

    write_query_message(&mut client, "BEGIN");
    let begin_frames = read_until_ready(&mut client).expect("begin frames");
    assert_eq!(decode_command_complete(&begin_frames[0]), "BEGIN");
    assert_eq!(
        decode_ready_status(begin_frames.last().expect("ready frame")),
        b'T'
    );

    write_query_message(&mut client, "SHOW ALL");
    let refusal_frames = read_until_ready(&mut client).expect("refusal frames");
    assert_unsupported_refusal_sqlstate(
        Some(
            decode_error_sqlstate(&refusal_frames[0])
                .expect("sqlstate field")
                .as_str(),
        ),
        "psql_smoke unsupported SHOW ALL",
    );
    assert_eq!(
        decode_error_field(&refusal_frames[0], b'V')
            .expect("decode variant field")
            .as_deref(),
        Some("unsupported_shape")
    );
    assert_eq!(
        decode_error_field(&refusal_frames[0], b'D')
            .expect("decode detail field")
            .as_deref(),
        Some("shape=show_all; statement=SHOW ALL; transport=simple_query")
    );
    assert_eq!(
        decode_ready_status(refusal_frames.last().expect("ready frame")),
        b'E'
    );

    write_query_message(&mut client, "ROLLBACK");
    let rollback_frames = read_until_ready(&mut client).expect("rollback frames");
    assert_eq!(decode_command_complete(&rollback_frames[0]), "ROLLBACK");
    assert_eq!(
        decode_ready_status(rollback_frames.last().expect("ready frame")),
        b'I'
    );

    write_terminate_message(&mut client);
    handle
        .join()
        .expect("listener thread")
        .expect("serve connection");
}

#[test]
fn decode_error_sqlstate_returns_err_when_field_is_missing() {
    let frame = [b'E', 0, 0, 0, 4, 0];

    assert_eq!(
        decode_error_sqlstate(&frame),
        Err("missing requested error field")
    );
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

fn decode_parameter_statuses(frames: &[Vec<u8>]) -> BTreeMap<String, String> {
    frames
        .iter()
        .filter(|frame| frame[0] == b'S')
        .map(|frame| {
            let body = &frame[5..frame.len() - 1];
            let separator = body
                .iter()
                .position(|byte| *byte == 0)
                .expect("parameter status separator");
            let name = String::from_utf8(body[..separator].to_vec()).expect("status name");
            let value = String::from_utf8(body[separator + 1..].to_vec()).expect("status value");
            (name, value)
        })
        .collect()
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
    decode_error_field(frame, b'C')?.ok_or("missing requested error field")
}

fn decode_error_field(frame: &[u8], field: u8) -> Result<Option<String>, &'static str> {
    if frame.first() != Some(&b'E') {
        return Err("frame is not an ErrorResponse");
    }

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
            .ok_or("missing field terminator")?;
        let value = String::from_utf8(frame[offset..offset + terminator].to_vec())
            .map_err(|_| "field is not valid UTF-8")?;
        offset += terminator + 1;

        if field_type == field {
            return Ok(Some(value));
        }
    }

    Ok(None)
}
