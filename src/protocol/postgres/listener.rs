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

#[cfg(unix)]
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    flag,
};

use super::{
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
    serve_connection_with_shutdown(&mut stream, session_id, None)
}

pub fn serve_connection_until_shutdown(
    mut stream: TcpStream,
    session_id: impl Into<String>,
    shutdown: &ShutdownHook,
) -> io::Result<()> {
    stream.set_read_timeout(Some(CONNECTION_POLL_INTERVAL))?;
    serve_connection_with_shutdown(&mut stream, session_id, Some(shutdown))
}

fn serve_connection_with_shutdown(
    stream: &mut TcpStream,
    session_id: impl Into<String>,
    shutdown: Option<&ShutdownHook>,
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
    while let Some(message) = read_frontend_message(stream, shutdown)? {
        match message {
            FrontendMessage::Terminate => break,
            FrontendMessage::Query(sql) => {
                let cycle = dispatch_simple_query(&mut session, session_id.as_str(), &sql);
                write_frames(stream, &cycle.frames)?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum FrontendMessage {
    Query(String),
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
        b'X' => FrontendMessage::Terminate,
        other => FrontendMessage::Unsupported {
            shape: format!("frontend_message_{}", other as char),
        },
    }))
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
    let Some(&0) = body.last() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{context} body must be nul-terminated"),
        ));
    };
    String::from_utf8(body[..body.len() - 1].to_vec()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{context} body must be valid UTF-8: {error}"),
        )
    })
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
