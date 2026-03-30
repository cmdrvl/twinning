#![forbid(unsafe_code)]

use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use twinning::protocol::postgres::listener::{PgwireListener, ShutdownHook};

#[test]
fn listener_accepts_startup_and_simple_query_round_trip() {
    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");

    let handle = thread::spawn(move || listener.accept("psql-1"));

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
    assert!(startup_frames.iter().any(|frame| frame[0] == b'Z'));

    write_query_message(&mut client, "BEGIN");
    let begin_frames = read_until_ready(&mut client).expect("begin frames");
    assert_eq!(decode_command_complete(&begin_frames[0]), "BEGIN");
    assert_eq!(
        decode_ready_status(begin_frames.last().expect("ready")),
        b'T'
    );

    write_query_message(&mut client, "ROLLBACK");
    let rollback_frames = read_until_ready(&mut client).expect("rollback frames");
    assert_eq!(decode_command_complete(&rollback_frames[0]), "ROLLBACK");
    assert_eq!(
        decode_ready_status(rollback_frames.last().expect("ready")),
        b'I'
    );

    write_terminate_message(&mut client);
    handle
        .join()
        .expect("listener thread")
        .expect("serve connection");
}

#[test]
fn unsupported_frontend_message_stays_protocol_visible_and_session_recovers() {
    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");

    let handle = thread::spawn(move || listener.accept("psql-2"));

    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set timeout");

    write_startup_packet(&mut client, &[("user", "postgres")]);
    let _ = read_until_ready(&mut client).expect("startup frames");

    write_unsupported_message(&mut client, b'P');
    let refusal_frames = read_until_ready(&mut client).expect("refusal frames");
    assert_eq!(
        decode_error_sqlstate(&refusal_frames[0]).expect("SQLSTATE field"),
        "0A000"
    );
    assert_eq!(
        decode_ready_status(refusal_frames.last().expect("ready frame")),
        b'I'
    );

    write_query_message(&mut client, "BEGIN");
    let begin_frames = read_until_ready(&mut client).expect("begin frames");
    assert_eq!(decode_command_complete(&begin_frames[0]), "BEGIN");
    assert_eq!(
        decode_ready_status(begin_frames.last().expect("ready")),
        b'T'
    );

    write_terminate_message(&mut client);
    handle
        .join()
        .expect("listener thread")
        .expect("serve connection");
}

#[test]
fn shutdown_hook_stops_accept_loop_without_any_connection() {
    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let shutdown = ShutdownHook::new();
    let server_shutdown = shutdown.clone();

    let handle =
        thread::spawn(move || listener.accept_until_shutdown("interactive", &server_shutdown));

    thread::sleep(Duration::from_millis(100));
    shutdown.request_shutdown();

    handle
        .join()
        .expect("listener thread")
        .expect("shutdown accept loop");
}

#[test]
fn shutdown_hook_terminates_active_connection_without_client_terminate() {
    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let shutdown = ShutdownHook::new();
    let server_shutdown = shutdown.clone();

    let handle =
        thread::spawn(move || listener.accept_until_shutdown("interactive", &server_shutdown));

    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set timeout");

    write_startup_packet(&mut client, &[("user", "postgres")]);
    let startup_frames = read_until_ready(&mut client).expect("startup frames");
    assert!(startup_frames.iter().any(|frame| frame[0] == b'Z'));

    shutdown.request_shutdown();

    handle
        .join()
        .expect("listener thread")
        .expect("shutdown active connection");
}

#[test]
fn decode_error_sqlstate_returns_err_when_field_is_missing() {
    let frame = [b'E', 0, 0, 0, 4, 0];

    assert_eq!(decode_error_sqlstate(&frame), Err("missing SQLSTATE field"));
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

fn write_unsupported_message(stream: &mut TcpStream, tag: u8) {
    write_framed_message(stream, tag, &[]);
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

fn decode_command_complete(frame: &[u8]) -> String {
    assert_eq!(frame[0], b'C');
    String::from_utf8(frame[5..frame.len() - 1].to_vec()).expect("command tag")
}

fn decode_ready_status(frame: &[u8]) -> u8 {
    assert_eq!(frame[0], b'Z');
    frame[5]
}

fn decode_error_sqlstate(frame: &[u8]) -> Result<String, &'static str> {
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

        if field_type == b'C' {
            return Ok(value);
        }
    }

    Err("missing SQLSTATE field")
}
