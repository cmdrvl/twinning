#![forbid(unsafe_code)]
#![cfg(feature = "postgres")]

use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use twinning::protocol::postgres::listener::{PgwireListener, ShutdownHook};

#[test]
fn m0a_ssl_request_then_startup_is_accepted() {
    run_encryption_request_then_startup(
        "M0A-SSL",
        "cargo test --test pgwire_listener m0a_ssl_request_then_startup_is_accepted -- --exact --nocapture",
        80_877_103,
    );
}

#[test]
fn m0a_gssenc_request_then_startup_is_accepted() {
    run_encryption_request_then_startup(
        "M0A-GSSENC",
        "cargo test --test pgwire_listener m0a_gssenc_request_then_startup_is_accepted -- --exact --nocapture",
        80_877_104,
    );
}

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

    write_unsupported_message(&mut client, b'Y');
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
    let frame = startup_packet_bytes(params);
    stream.write_all(&frame).expect("write startup");
    stream.flush().expect("flush startup");
}

fn startup_packet_bytes(params: &[(&str, &str)]) -> Vec<u8> {
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
    frame
}

fn run_encryption_request_then_startup(label: &str, command_line: &str, request_code: u32) {
    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let port = addr.port();
    let handle = thread::spawn(move || listener.accept("m0a-encryption-request"));

    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set 2 s read timeout");

    let request = startup_request_frame(request_code);
    client
        .write_all(&request)
        .expect("write encryption request");
    let mut bytes_sent = request.len();
    let first_response = read_startup_response(&mut client);
    let response_tag = first_response
        .as_ref()
        .ok()
        .and_then(|(frame, _)| frame.first())
        .copied();
    let mut bytes_received = first_response
        .as_ref()
        .map(|(_, byte_count)| *byte_count)
        .unwrap_or_default();

    let mut startup_write_error = None;
    let mut startup_result = None;
    if response_tag == Some(b'N') {
        let startup = startup_packet_bytes(&[("user", "postgres")]);
        bytes_sent += startup.len();
        match client.write_all(&startup) {
            Ok(()) => {
                startup_result = Some(read_until_ready(&mut client));
                if let Some(Ok(frames)) = &startup_result {
                    bytes_received += frames.iter().map(Vec::len).sum::<usize>();
                }
            }
            Err(error) => startup_write_error = Some(error.kind()),
        }
    }

    let startup_is_ready = startup_result.as_ref().is_some_and(|result| {
        result.as_ref().is_ok_and(|frames| {
            frames.iter().any(|frame| frame.first() == Some(&b'R'))
                && frames
                    .last()
                    .is_some_and(|frame| frame.first() == Some(&b'Z'))
        })
    });
    if startup_is_ready {
        write_terminate_message(&mut client);
        bytes_sent += 5;
    }

    let response_observation = match &first_response {
        Ok((frame, _)) if frame.first() == Some(&b'E') => {
            format!(
                "tag=0x{:02X} sqlstate={:?}",
                frame[0],
                decode_error_sqlstate(frame).ok()
            )
        }
        Ok((frame, _)) => format!("tag=0x{:02X}", frame[0]),
        Err(error) => format!("error={:?}", error.kind()),
    };
    let startup_observation = match &startup_result {
        Some(Ok(frames)) => format!("frames={},ready={startup_is_ready}", frames.len()),
        Some(Err(error)) => format!("error={:?}", error.kind()),
        None => "not-sent".to_owned(),
    };
    drop(client);
    let server_result = handle.join();
    let server_exit_code = match &server_result {
        Ok(Ok(())) => 0,
        _ => 1,
    };
    let server_observation = match &server_result {
        Ok(Ok(())) => "ok".to_owned(),
        Ok(Err(error)) => format!("error={:?}", error.kind()),
        Err(_) => "panic".to_owned(),
    };

    println!(
        "[{label}] command={command_line:?} port={port} request_hex={} bytes_sent={bytes_sent} first_response={response_observation} startup={startup_observation} bytes_received={bytes_received} server={server_observation} server_exit_code={server_exit_code}",
        request
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>()
    );

    assert_eq!(
        response_tag,
        Some(b'N'),
        "{label} encryption request must receive the one-byte N denial"
    );
    assert!(
        startup_write_error.is_none(),
        "{label} startup write failed: {startup_write_error:?}"
    );
    let Some(startup_result) = startup_result else {
        panic!("{label} startup packet must be sent after N");
    };
    let startup_frames = startup_result.unwrap_or_else(|error| {
        panic!("{label} startup response must reach ReadyForQuery: {error:?}");
    });
    assert!(
        startup_frames
            .iter()
            .any(|frame| frame.first() == Some(&b'R')),
        "{label} startup response must authenticate"
    );
    assert_eq!(
        startup_frames.last().map(|frame| frame.get(5)),
        Some(Some(&b'I')),
        "{label} startup response must end with ReadyForQuery"
    );
    assert!(
        matches!(server_result, Ok(Ok(()))),
        "{label} server connection must exit cleanly"
    );
}

fn startup_request_frame(request_code: u32) -> Vec<u8> {
    let mut frame = Vec::with_capacity(8);
    frame.extend_from_slice(&8_i32.to_be_bytes());
    frame.extend_from_slice(&request_code.to_be_bytes());
    frame
}

fn read_startup_response(stream: &mut TcpStream) -> std::io::Result<(Vec<u8>, usize)> {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag)?;
    if tag[0] == b'N' {
        return Ok((tag.to_vec(), tag.len()));
    }

    let mut length_bytes = [0_u8; 4];
    stream.read_exact(&mut length_bytes)?;
    let length = i32::from_be_bytes(length_bytes);
    if length < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("startup response has invalid length {length}"),
        ));
    }
    let mut body = vec![0_u8; length as usize - 4];
    stream.read_exact(&mut body)?;
    let mut frame = Vec::with_capacity(length as usize + 1);
    frame.push(tag[0]);
    frame.extend_from_slice(&length_bytes);
    frame.extend_from_slice(&body);
    let bytes_read = frame.len();
    Ok((frame, bytes_read))
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
