use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use crate::{backend::BaseSnapshotBackend, catalog::Catalog};

#[cfg(unix)]
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    flag,
};

use super::{
    extended_execute::{DescribeTarget, ExecuteRequest, ExtendedExecuteState},
    extended_parse::{BindRequest, ExtendedParseState, ParseRequest},
    frames::{ResultFrameMetadata, unsupported_live_shape_result},
    session::{SessionLoop, ready_for_query_frame},
    simple_query::dispatch_simple_query,
    startup::{StartupOutcome, encode_startup_response, negotiate_startup, parse_startup_packet},
};

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const CONNECTION_POLL_INTERVAL: Duration = Duration::from_millis(25);
const NO_SHUTDOWN: usize = 0;
const OPERATOR_STOP: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownReason {
    OperatorStop,
    Signal(i32),
}

#[derive(Debug, Clone, Default)]
pub struct ShutdownHook {
    requested: Arc<AtomicUsize>,
}

impl ShutdownHook {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(unix)]
    pub fn install() -> io::Result<Self> {
        let hook = Self::new();
        flag::register_usize(SIGINT, Arc::clone(&hook.requested), SIGINT as usize)
            .map_err(io::Error::other)?;
        flag::register_usize(SIGTERM, Arc::clone(&hook.requested), SIGTERM as usize)
            .map_err(io::Error::other)?;
        Ok(hook)
    }

    #[cfg(not(unix))]
    pub fn install() -> io::Result<Self> {
        Ok(Self::new())
    }

    pub fn request_shutdown(&self) {
        let _ = self.requested.compare_exchange(
            NO_SHUTDOWN,
            OPERATOR_STOP,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst) != NO_SHUTDOWN
    }

    pub fn shutdown_reason(&self) -> Option<ShutdownReason> {
        match self.requested.load(Ordering::SeqCst) {
            NO_SHUTDOWN => None,
            OPERATOR_STOP => Some(ShutdownReason::OperatorStop),
            signal => Some(ShutdownReason::Signal(signal as i32)),
        }
    }

    #[cfg(test)]
    fn record_signal_for_test(&self, signal: i32) {
        if signal <= 0 {
            return;
        }

        let _ = self.requested.compare_exchange(
            NO_SHUTDOWN,
            signal as usize,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }
}

#[derive(Debug)]
pub struct PgwireListener {
    listener: TcpListener,
}

impl PgwireListener {
    pub fn bind(host: &str, port: u16) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind((host, port))?,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn accept(&self, session_id: impl Into<String>) -> io::Result<()> {
        let (stream, _) = self.listener.accept()?;
        serve_connection(stream, session_id)
    }

    pub fn accept_with_backend(
        &self,
        session_id: impl Into<String>,
        catalog: Catalog,
        backend: BaseSnapshotBackend,
    ) -> io::Result<()> {
        let (stream, _) = self.listener.accept()?;
        serve_connection_with_backend(stream, session_id, catalog, backend)
    }

    pub fn accept_until_shutdown(
        &self,
        session_id_prefix: impl AsRef<str>,
        shutdown: &ShutdownHook,
    ) -> io::Result<()> {
        self.listener.set_nonblocking(true)?;
        let result =
            accept_until_shutdown_loop(&self.listener, session_id_prefix.as_ref(), shutdown);
        self.listener.set_nonblocking(false)?;
        result
    }
}

pub fn serve_connection(mut stream: TcpStream, session_id: impl Into<String>) -> io::Result<()> {
    serve_connection_with_shutdown(&mut stream, session_id, None, None)
}

pub fn serve_connection_with_backend(
    mut stream: TcpStream,
    session_id: impl Into<String>,
    catalog: Catalog,
    backend: BaseSnapshotBackend,
) -> io::Result<()> {
    serve_connection_with_shutdown(
        &mut stream,
        session_id,
        None,
        Some(LiveQueryState { catalog, backend }),
    )
}

pub fn serve_connection_until_shutdown(
    mut stream: TcpStream,
    session_id: impl Into<String>,
    shutdown: &ShutdownHook,
) -> io::Result<()> {
    stream.set_read_timeout(Some(CONNECTION_POLL_INTERVAL))?;
    serve_connection_with_shutdown(&mut stream, session_id, Some(shutdown), None)
}

fn serve_connection_with_shutdown(
    stream: &mut TcpStream,
    session_id: impl Into<String>,
    shutdown: Option<&ShutdownHook>,
    live_state: Option<LiveQueryState>,
) -> io::Result<()> {
    let session_id = session_id.into();
    let startup_packet = match read_startup_packet(stream, shutdown) {
        Ok(packet) => packet,
        Err(error) => {
            let frames = encode_startup_response(&Err(startup_error_refusal(&error)));
            write_frames(stream, &frames)?;
            return Err(error);
        }
    };

    let startup = match parse_startup_packet(&startup_packet) {
        Ok(packet) => packet,
        Err(refusal) => {
            let error = io::Error::new(io::ErrorKind::InvalidData, refusal.message.clone());
            let frames = encode_startup_response(&Err(refusal));
            write_frames(stream, &frames)?;
            return Err(error);
        }
    };

    let outcome = match negotiate_startup(startup) {
        Ok(outcome) => outcome,
        Err(error) => {
            let io_error = io::Error::new(io::ErrorKind::InvalidData, error.message.clone());
            let frames = encode_startup_response(&Err(error));
            write_frames(stream, &frames)?;
            return Err(io_error);
        }
    };

    let mut startup_frames = encode_startup_response(&Ok(outcome.clone()));
    if matches!(outcome, StartupOutcome::Authenticated(_)) {
        startup_frames.push(ready_for_query_frame(Default::default()));
    }
    write_frames(stream, &startup_frames)?;

    let mut session = SessionLoop::new();
    let mut parse_state = ExtendedParseState::new();
    let mut execute_state = ExtendedExecuteState::new();
    let mut live_state = live_state;
    while let Some(message) = read_frontend_message(stream, shutdown)? {
        match message {
            FrontendMessage::Terminate => break,
            FrontendMessage::Flush => {
                stream.flush()?;
            }
            FrontendMessage::Sync => {
                let cycle = execute_state.process_sync(&mut session);
                write_frames(stream, &cycle.frames)?;
            }
            FrontendMessage::Query(sql) => {
                let cycle = dispatch_simple_query(&mut session, session_id.as_str(), &sql);
                write_frames(stream, &cycle.frames)?;
            }
            FrontendMessage::Parse(request) => {
                let frames = parse_state.process_parse(session_id.as_str(), request);
                write_frames(stream, &frames)?;
            }
            FrontendMessage::Bind(request) => {
                let frames = parse_state.process_bind(request);
                write_frames(stream, &frames)?;
            }
            FrontendMessage::Describe(target) => {
                let frames = match live_state.as_ref() {
                    Some(state) => {
                        execute_state.process_describe(&state.catalog, &parse_state, target)
                    }
                    None => {
                        session
                            .process_result(
                                &unsupported_live_shape_result(
                                    "protocol",
                                    "extended_query_without_backend",
                                    None,
                                ),
                                ResultFrameMetadata::default(),
                            )
                            .frames
                    }
                };
                write_frames(stream, &frames)?;
            }
            FrontendMessage::Execute(request) => {
                let frames = match live_state.as_mut() {
                    Some(state) => execute_state.process_execute(
                        &state.catalog,
                        &mut state.backend,
                        &parse_state,
                        request,
                    ),
                    None => {
                        session
                            .process_result(
                                &unsupported_live_shape_result(
                                    "protocol",
                                    "extended_query_without_backend",
                                    None,
                                ),
                                ResultFrameMetadata::default(),
                            )
                            .frames
                    }
                };
                write_frames(stream, &frames)?;
            }
            FrontendMessage::Unsupported { shape } => {
                let result = unsupported_live_shape_result("protocol", shape, None);
                let cycle = session.process_result(&result, ResultFrameMetadata::default());
                write_frames(stream, &cycle.frames)?;
            }
        }
    }

    if shutdown.is_some_and(ShutdownHook::is_shutdown_requested) {
        let _ = stream.shutdown(Shutdown::Both);
    }

    Ok(())
}

#[derive(Debug)]
struct LiveQueryState {
    catalog: Catalog,
    backend: BaseSnapshotBackend,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FrontendMessage {
    Bind(BindRequest),
    Describe(DescribeTarget),
    Execute(ExecuteRequest),
    Flush,
    Parse(ParseRequest),
    Query(String),
    Sync,
    Terminate,
    Unsupported { shape: String },
}

fn accept_until_shutdown_loop(
    listener: &TcpListener,
    session_id_prefix: &str,
    shutdown: &ShutdownHook,
) -> io::Result<()> {
    let mut connection_count = 0_u64;

    while !shutdown.is_shutdown_requested() {
        match listener.accept() {
            Ok((stream, _)) => {
                connection_count += 1;
                let session_id = format!("{session_id_prefix}-{connection_count}");
                serve_connection_until_shutdown(stream, session_id, shutdown)?;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

fn read_startup_packet(
    reader: &mut impl Read,
    shutdown: Option<&ShutdownHook>,
) -> io::Result<Vec<u8>> {
    let mut length_bytes = [0_u8; 4];
    read_exact_until_shutdown(reader, &mut length_bytes, shutdown)?;
    let length = i32::from_be_bytes(length_bytes);
    if length < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "startup packet length must be at least 8 bytes",
        ));
    }

    let mut frame = Vec::with_capacity(length as usize);
    frame.extend_from_slice(&length_bytes);
    let mut body = vec![0_u8; length as usize - 4];
    read_exact_until_shutdown(reader, &mut body, shutdown)?;
    frame.extend_from_slice(&body);
    Ok(frame)
}

fn read_frontend_message(
    reader: &mut impl Read,
    shutdown: Option<&ShutdownHook>,
) -> io::Result<Option<FrontendMessage>> {
    let mut tag = [0_u8; 1];
    if let Err(error) = read_exact_until_shutdown(reader, &mut tag, shutdown) {
        return match error.kind() {
            io::ErrorKind::UnexpectedEof => Ok(None),
            io::ErrorKind::Interrupted => Ok(None),
            _ => Err(error),
        };
    }

    let mut length_bytes = [0_u8; 4];
    read_exact_until_shutdown(reader, &mut length_bytes, shutdown)?;
    let length = i32::from_be_bytes(length_bytes);
    if length < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "frontend message `{}` has invalid length {length}",
                tag[0] as char
            ),
        ));
    }

    let mut body = vec![0_u8; length as usize - 4];
    read_exact_until_shutdown(reader, &mut body, shutdown)?;

    Ok(Some(match tag[0] {
        b'Q' => FrontendMessage::Query(parse_cstring_message(&body, "simple_query")?),
        b'P' => FrontendMessage::Parse(parse_parse_message(&body)?),
        b'B' => FrontendMessage::Bind(parse_bind_message(&body)?),
        b'D' => FrontendMessage::Describe(parse_describe_message(&body)?),
        b'E' => FrontendMessage::Execute(parse_execute_message(&body)?),
        b'H' => FrontendMessage::Flush,
        b'S' => FrontendMessage::Sync,
        b'X' => FrontendMessage::Terminate,
        other => FrontendMessage::Unsupported {
            shape: format!("frontend_message_{}", other as char),
        },
    }))
}

fn parse_parse_message(body: &[u8]) -> io::Result<ParseRequest> {
    let mut cursor = 0usize;
    let statement_name = read_cstring(body, &mut cursor, "parse statement name")?;
    let sql = read_cstring(body, &mut cursor, "parse statement SQL")?;
    let param_count = read_count(body, &mut cursor, "parse parameter type count")?;
    let param_oids = (0..param_count)
        .map(|_| read_u32(body, &mut cursor, "parse parameter type OID"))
        .collect::<io::Result<Vec<_>>>()?;
    ensure_fully_consumed(body, cursor, "parse")?;

    let param_types = if param_oids.iter().all(|oid| *oid == 0) {
        Vec::new()
    } else {
        param_oids
            .into_iter()
            .map(type_oid_to_param_type)
            .collect::<io::Result<Vec<_>>>()?
    };

    Ok(ParseRequest {
        statement_name,
        sql,
        param_types,
    })
}

fn parse_bind_message(body: &[u8]) -> io::Result<BindRequest> {
    let mut cursor = 0usize;
    let portal_name = read_cstring(body, &mut cursor, "bind portal name")?;
    let statement_name = read_cstring(body, &mut cursor, "bind statement name")?;

    let format_count = read_count(body, &mut cursor, "bind parameter format count")?;
    let parameter_formats = (0..format_count)
        .map(|_| read_i16(body, &mut cursor, "bind parameter format"))
        .collect::<io::Result<Vec<_>>>()?;

    let param_count = read_count(body, &mut cursor, "bind parameter value count")?;
    let params = (0..param_count)
        .map(|_| read_parameter_value(body, &mut cursor))
        .collect::<io::Result<Vec<_>>>()?;

    let result_format_count = read_count(body, &mut cursor, "bind result format count")?;
    let result_formats = (0..result_format_count)
        .map(|_| read_i16(body, &mut cursor, "bind result format"))
        .collect::<io::Result<Vec<_>>>()?;
    ensure_fully_consumed(body, cursor, "bind")?;

    Ok(BindRequest {
        portal_name,
        statement_name,
        params,
        parameter_formats,
        result_formats,
    })
}

fn parse_describe_message(body: &[u8]) -> io::Result<DescribeTarget> {
    let mut cursor = 0usize;
    let target = read_u8(body, &mut cursor, "describe target kind")?;
    let name = read_cstring(body, &mut cursor, "describe target name")?;
    ensure_fully_consumed(body, cursor, "describe")?;

    match target {
        b'S' => Ok(DescribeTarget::Statement(name)),
        b'P' => Ok(DescribeTarget::Portal(name)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("describe target `{}` is not supported", other as char),
        )),
    }
}

fn parse_execute_message(body: &[u8]) -> io::Result<ExecuteRequest> {
    let mut cursor = 0usize;
    let portal_name = read_cstring(body, &mut cursor, "execute portal name")?;
    let max_rows = read_i32(body, &mut cursor, "execute max rows")?;
    ensure_fully_consumed(body, cursor, "execute")?;
    if max_rows < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "execute max rows must be non-negative",
        ));
    }

    Ok(ExecuteRequest {
        portal_name,
        max_rows: max_rows as u32,
    })
}

fn read_parameter_value(body: &[u8], cursor: &mut usize) -> io::Result<Option<String>> {
    let length = read_i32(body, cursor, "bind parameter length")?;
    if length == -1 {
        return Ok(None);
    }
    if length < -1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bind parameter length must be -1 or non-negative",
        ));
    }

    let bytes = take_bytes(body, cursor, length as usize, "bind parameter value")?;
    String::from_utf8(bytes.to_vec())
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn read_exact_until_shutdown(
    reader: &mut impl Read,
    mut buf: &mut [u8],
    shutdown: Option<&ShutdownHook>,
) -> io::Result<()> {
    while !buf.is_empty() {
        match reader.read(buf) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed while reading frame",
                ));
            }
            Ok(read) => {
                let (_, rest) = std::mem::take(&mut buf).split_at_mut(read);
                buf = rest;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if shutdown.is_some_and(ShutdownHook::is_shutdown_requested) {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "shutdown requested",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

fn parse_cstring_message(body: &[u8], context: &str) -> io::Result<String> {
    let mut cursor = 0usize;
    let message = read_cstring(body, &mut cursor, context)?;
    ensure_fully_consumed(body, cursor, context)?;
    Ok(message)
}

fn read_cstring(body: &[u8], cursor: &mut usize, context: &str) -> io::Result<String> {
    let start = *cursor;
    let terminator = body[start..]
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{context} must be nul-terminated"),
            )
        })?;
    let end = start + terminator;
    *cursor = end + 1;

    String::from_utf8(body[start..end].to_vec()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{context} must be valid UTF-8: {error}"),
        )
    })
}

fn read_count(body: &[u8], cursor: &mut usize, context: &str) -> io::Result<usize> {
    let value = read_i16(body, cursor, context)?;
    if value < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{context} must be non-negative"),
        ));
    }
    Ok(value as usize)
}

fn read_u8(body: &[u8], cursor: &mut usize, context: &str) -> io::Result<u8> {
    Ok(*take_bytes(body, cursor, 1, context)?
        .first()
        .expect("one byte should be available"))
}

fn read_i16(body: &[u8], cursor: &mut usize, context: &str) -> io::Result<i16> {
    let bytes = take_bytes(body, cursor, 2, context)?;
    Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_i32(body: &[u8], cursor: &mut usize, context: &str) -> io::Result<i32> {
    let bytes = take_bytes(body, cursor, 4, context)?;
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u32(body: &[u8], cursor: &mut usize, context: &str) -> io::Result<u32> {
    let bytes = take_bytes(body, cursor, 4, context)?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn take_bytes<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    length: usize,
    context: &str,
) -> io::Result<&'a [u8]> {
    let end = cursor.checked_add(length).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{context} length overflowed"),
        )
    })?;
    if end > body.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{context} extends past message body"),
        ));
    }

    let bytes = &body[*cursor..end];
    *cursor = end;
    Ok(bytes)
}

fn ensure_fully_consumed(body: &[u8], cursor: usize, context: &str) -> io::Result<()> {
    if cursor == body.len() {
        return Ok(());
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{context} body has {} trailing byte(s)",
            body.len().saturating_sub(cursor)
        ),
    ))
}

fn type_oid_to_param_type(oid: u32) -> io::Result<String> {
    match oid {
        0 => Ok(String::new()),
        16 => Ok(String::from("boolean")),
        17 => Ok(String::from("bytes")),
        20 => Ok(String::from("bigint")),
        21 => Ok(String::from("smallint")),
        23 => Ok(String::from("integer")),
        25 => Ok(String::from("text")),
        114 => Ok(String::from("json")),
        701 => Ok(String::from("float")),
        1082 => Ok(String::from("date")),
        1114 => Ok(String::from("timestamp")),
        1700 => Ok(String::from("numeric")),
        1009 => Ok(String::from("array")),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parameter type OID {other} is outside the declared subset"),
        )),
    }
}

fn write_frames(writer: &mut impl Write, frames: &[Vec<u8>]) -> io::Result<()> {
    for frame in frames {
        writer.write_all(frame)?;
    }
    writer.flush()
}

fn startup_error_refusal(error: &io::Error) -> super::startup::StartupRefusal {
    super::startup::StartupRefusal {
        code: "invalid_startup_packet",
        message: error.to_string(),
        sqlstate: "08P01",
        detail: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::{ShutdownHook, ShutdownReason};

    #[test]
    fn shutdown_hook_reports_explicit_operator_stop() {
        let shutdown = ShutdownHook::new();
        assert_eq!(shutdown.shutdown_reason(), None);

        shutdown.request_shutdown();

        assert_eq!(
            shutdown.shutdown_reason(),
            Some(ShutdownReason::OperatorStop)
        );
    }

    #[test]
    fn shutdown_hook_preserves_first_signal_reason() {
        let shutdown = ShutdownHook::new();
        shutdown.record_signal_for_test(15);
        shutdown.request_shutdown();

        assert_eq!(shutdown.shutdown_reason(), Some(ShutdownReason::Signal(15)));
    }
}
