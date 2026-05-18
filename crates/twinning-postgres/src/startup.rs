use std::collections::BTreeMap;

pub const PROTOCOL_VERSION_3: u32 = 196_608;
pub const SSL_REQUEST_CODE: u32 = 80_877_103;
pub const CANCEL_REQUEST_CODE: u32 = 80_877_102;
pub const DECLARED_BACKEND_PROCESS_ID: i32 = 1;
pub const DECLARED_BACKEND_SECRET_KEY: i32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    SslRequest,
    CancelRequest { process_id: i32, secret_key: i32 },
    Startup(StartupMessage),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupMessage {
    pub protocol_version: u32,
    pub params: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupOutcome {
    SslDenied,
    Authenticated(AuthenticatedStartup),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedStartup {
    pub params: BTreeMap<String, String>,
    pub auth_mode: AuthMode,
    pub parameter_statuses: Vec<ParameterStatus>,
    pub backend_key_data: BackendKeyData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    Trust,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterStatus {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendKeyData {
    pub process_id: i32,
    pub secret_key: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupRefusal {
    pub code: &'static str,
    pub message: String,
    pub sqlstate: &'static str,
    pub detail: BTreeMap<String, String>,
}

impl std::fmt::Display for StartupRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl StartupRefusal {
    fn invalid_startup_packet(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_startup_packet",
            message: message.into(),
            sqlstate: "08P01",
            detail: BTreeMap::new(),
        }
    }

    fn unsupported_protocol_version(protocol_version: u32) -> Self {
        Self {
            code: "unsupported_protocol_version",
            message: format!(
                "protocol version `{protocol_version}` is outside the declared pgwire startup subset"
            ),
            sqlstate: "0A000",
            detail: BTreeMap::from([(
                String::from("protocol_version"),
                protocol_version.to_string(),
            )]),
        }
    }

    fn unsupported_startup_request(request: &'static str) -> Self {
        Self {
            code: "unsupported_startup_request",
            message: format!("startup request `{request}` is outside the declared local/dev shell"),
            sqlstate: "0A000",
            detail: BTreeMap::from([(String::from("request"), request.to_owned())]),
        }
    }

    fn invalid_authorization(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_authorization_specification",
            message: message.into(),
            sqlstate: "28000",
            detail: BTreeMap::new(),
        }
    }
}

pub fn parse_startup_packet(frame: &[u8]) -> Result<StartupPacket, StartupRefusal> {
    if frame.len() < 8 {
        return Err(StartupRefusal::invalid_startup_packet(
            "startup packet must be at least 8 bytes",
        ));
    }

    let declared_length = i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
    if declared_length < 8 {
        return Err(StartupRefusal::invalid_startup_packet(
            "startup packet length must be at least 8 bytes",
        ));
    }
    if declared_length as usize != frame.len() {
        return Err(StartupRefusal::invalid_startup_packet(format!(
            "startup packet length mismatch: declared {declared_length}, actual {}",
            frame.len()
        )));
    }

    let code = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    match code {
        SSL_REQUEST_CODE => {
            if frame.len() != 8 {
                return Err(StartupRefusal::invalid_startup_packet(
                    "SSLRequest packet must be exactly 8 bytes",
                ));
            }
            Ok(StartupPacket::SslRequest)
        }
        CANCEL_REQUEST_CODE => {
            if frame.len() != 16 {
                return Err(StartupRefusal::invalid_startup_packet(
                    "CancelRequest packet must be exactly 16 bytes",
                ));
            }
            Ok(StartupPacket::CancelRequest {
                process_id: i32::from_be_bytes([frame[8], frame[9], frame[10], frame[11]]),
                secret_key: i32::from_be_bytes([frame[12], frame[13], frame[14], frame[15]]),
            })
        }
        PROTOCOL_VERSION_3 => Ok(StartupPacket::Startup(StartupMessage {
            protocol_version: code,
            params: parse_startup_params(&frame[8..])?,
        })),
        other => Err(StartupRefusal::unsupported_protocol_version(other)),
    }
}

pub fn negotiate_startup(packet: StartupPacket) -> Result<StartupOutcome, StartupRefusal> {
    match packet {
        StartupPacket::SslRequest => Ok(StartupOutcome::SslDenied),
        StartupPacket::CancelRequest { .. } => Err(StartupRefusal::unsupported_startup_request(
            "cancel_request",
        )),
        StartupPacket::Startup(startup) => {
            let user = startup
                .params
                .get("user")
                .map(String::as_str)
                .filter(|value| !value.is_empty());
            if user.is_none() {
                return Err(StartupRefusal::invalid_authorization(
                    "startup packet must include a non-empty `user` parameter",
                ));
            }

            let parameter_statuses = declared_parameter_statuses(&startup.params);
            let params = startup.params;
            Ok(StartupOutcome::Authenticated(AuthenticatedStartup {
                params,
                auth_mode: AuthMode::Trust,
                parameter_statuses,
                backend_key_data: declared_backend_key_data(),
            }))
        }
    }
}

pub fn encode_startup_response(outcome: &Result<StartupOutcome, StartupRefusal>) -> Vec<Vec<u8>> {
    match outcome {
        Ok(StartupOutcome::SslDenied) => vec![vec![b'N']],
        Ok(StartupOutcome::Authenticated(startup)) => {
            let mut frames = vec![authentication_ok_frame()];
            frames.extend(
                startup
                    .parameter_statuses
                    .iter()
                    .map(parameter_status_frame),
            );
            frames.push(backend_key_data_frame(startup.backend_key_data));
            frames
        }
        Err(refusal) => vec![error_response_frame(refusal)],
    }
}

pub fn declared_parameter_statuses(
    startup_params: &BTreeMap<String, String>,
) -> Vec<ParameterStatus> {
    let mut parameter_statuses = vec![
        ParameterStatus {
            name: String::from("client_encoding"),
            value: String::from("UTF8"),
        },
        ParameterStatus {
            name: String::from("DateStyle"),
            value: String::from("ISO, MDY"),
        },
        ParameterStatus {
            name: String::from("integer_datetimes"),
            value: String::from("on"),
        },
        ParameterStatus {
            name: String::from("server_encoding"),
            value: String::from("UTF8"),
        },
        ParameterStatus {
            name: String::from("server_version"),
            value: String::from("16.0"),
        },
        ParameterStatus {
            name: String::from("standard_conforming_strings"),
            value: String::from("on"),
        },
    ];

    if let Some(application_name) = startup_params.get("application_name") {
        parameter_statuses.push(ParameterStatus {
            name: String::from("application_name"),
            value: application_name.clone(),
        });
    }

    parameter_statuses
}

pub fn declared_backend_key_data() -> BackendKeyData {
    BackendKeyData {
        process_id: DECLARED_BACKEND_PROCESS_ID,
        secret_key: DECLARED_BACKEND_SECRET_KEY,
    }
}

fn parse_startup_params(bytes: &[u8]) -> Result<BTreeMap<String, String>, StartupRefusal> {
    if bytes.is_empty() || *bytes.last().unwrap_or(&1) != 0 {
        return Err(StartupRefusal::invalid_startup_packet(
            "startup parameters must end with a zero byte",
        ));
    }

    let mut segments = Vec::new();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == 0 {
            let segment = String::from_utf8(bytes[start..index].to_vec()).map_err(|_| {
                StartupRefusal::invalid_startup_packet("startup parameters must be valid UTF-8")
            })?;
            segments.push(segment);
            start = index + 1;
        }
    }

    if segments.is_empty() || segments.last().is_none_or(|segment| !segment.is_empty()) {
        return Err(StartupRefusal::invalid_startup_packet(
            "startup parameters must end with a terminator pair",
        ));
    }
    segments.pop();

    if segments.len() % 2 != 0 {
        return Err(StartupRefusal::invalid_startup_packet(
            "startup parameters must be key/value pairs",
        ));
    }

    let mut params = BTreeMap::new();
    for pair in segments.chunks_exact(2) {
        let key = &pair[0];
        let value = &pair[1];
        if key.is_empty() {
            return Err(StartupRefusal::invalid_startup_packet(
                "startup parameter keys must be non-empty",
            ));
        }
        if params.insert(key.clone(), value.clone()).is_some() {
            return Err(StartupRefusal::invalid_startup_packet(format!(
                "startup parameter `{key}` appears more than once"
            )));
        }
    }

    Ok(params)
}

fn authentication_ok_frame() -> Vec<u8> {
    let mut frame = Vec::with_capacity(9);
    frame.push(b'R');
    frame.extend_from_slice(&8_i32.to_be_bytes());
    frame.extend_from_slice(&0_i32.to_be_bytes());
    frame
}

fn parameter_status_frame(parameter_status: &ParameterStatus) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(parameter_status.name.as_bytes());
    body.push(0);
    body.extend_from_slice(parameter_status.value.as_bytes());
    body.push(0);

    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(b'S');
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn backend_key_data_frame(backend_key_data: BackendKeyData) -> Vec<u8> {
    let mut frame = Vec::with_capacity(13);
    frame.push(b'K');
    frame.extend_from_slice(&12_i32.to_be_bytes());
    frame.extend_from_slice(&backend_key_data.process_id.to_be_bytes());
    frame.extend_from_slice(&backend_key_data.secret_key.to_be_bytes());
    frame
}

fn error_response_frame(refusal: &StartupRefusal) -> Vec<u8> {
    let mut body = Vec::new();
    write_error_field(&mut body, b'S', "FATAL");
    write_error_field(&mut body, b'C', refusal.sqlstate);
    write_error_field(&mut body, b'M', &refusal.message);
    write_error_field(&mut body, b'V', refusal.code);
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        AuthMode, BackendKeyData, DECLARED_BACKEND_PROCESS_ID, DECLARED_BACKEND_SECRET_KEY,
        PROTOCOL_VERSION_3, ParameterStatus, SSL_REQUEST_CODE, StartupMessage, StartupOutcome,
        StartupPacket, declared_backend_key_data, declared_parameter_statuses,
        encode_startup_response, negotiate_startup, parse_startup_packet,
    };

    #[test]
    fn parses_protocol_v3_startup_and_accepts_trust_auth_shell() {
        let packet = parse_startup_packet(&startup_frame(
            PROTOCOL_VERSION_3,
            &[
                ("user", "zac"),
                ("database", "twinning"),
                ("application_name", "psql"),
            ],
        ))
        .expect("parse startup");

        let startup = expect_startup_packet(packet).expect("expected startup packet");
        assert_eq!(startup.protocol_version, PROTOCOL_VERSION_3);
        assert_eq!(
            startup.params,
            BTreeMap::from([
                (String::from("application_name"), String::from("psql")),
                (String::from("database"), String::from("twinning")),
                (String::from("user"), String::from("zac")),
            ])
        );

        let outcome =
            negotiate_startup(StartupPacket::Startup(startup.clone())).expect("startup accepted");
        assert_eq!(
            outcome,
            StartupOutcome::Authenticated(super::AuthenticatedStartup {
                params: startup.params,
                auth_mode: AuthMode::Trust,
                parameter_statuses: expected_parameter_statuses(Some("psql")),
                backend_key_data: declared_backend_key_data(),
            })
        );

        let frames = encode_startup_response(&Ok(outcome));
        assert_eq!(frames[0], vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
        assert_eq!(
            decode_parameter_statuses(&frames[1..frames.len() - 1]),
            expected_parameter_statuses(Some("psql"))
        );
        assert_eq!(
            decode_backend_key_data(frames.last().expect("backend key frame")),
            BackendKeyData {
                process_id: DECLARED_BACKEND_PROCESS_ID,
                secret_key: DECLARED_BACKEND_SECRET_KEY,
            }
        );
    }

    #[test]
    fn ssl_request_is_denied_without_fatal_refusal() {
        let packet =
            parse_startup_packet(&raw_frame(SSL_REQUEST_CODE, &[])).expect("parse ssl request");
        assert_eq!(packet, StartupPacket::SslRequest);

        let outcome = negotiate_startup(packet).expect("ssl negotiation");
        assert_eq!(outcome, StartupOutcome::SslDenied);
        assert_eq!(encode_startup_response(&Ok(outcome)), vec![vec![b'N']]);
    }

    #[test]
    fn cancel_request_is_cleanly_refused() {
        let packet = parse_startup_packet(&cancel_request_frame(11, 22)).expect("parse cancel");
        let refusal = negotiate_startup(packet).expect_err("cancel should be refused");

        assert_eq!(refusal.code, "unsupported_startup_request");
        assert_eq!(refusal.sqlstate, "0A000");
        assert_eq!(refusal.detail["request"], "cancel_request");

        let frames = encode_startup_response(&Err(refusal));
        let frame = &frames[0];
        assert_eq!(frame[0], b'E');
        assert!(String::from_utf8_lossy(frame).contains("0A000"));
    }

    #[test]
    fn malformed_packets_refuse_with_protocol_violation() {
        let refusal =
            parse_startup_packet(&[0, 0, 0, 7, 0, 3, 0]).expect_err("packet is too short");
        assert_eq!(refusal.code, "invalid_startup_packet");
        assert_eq!(refusal.sqlstate, "08P01");

        let refusal = parse_startup_packet(&startup_frame_bytes(
            PROTOCOL_VERSION_3,
            b"user\0zac\0application_name\0psql",
        ))
        .expect_err("missing terminator");
        assert_eq!(refusal.code, "invalid_startup_packet");
        assert_eq!(refusal.sqlstate, "08P01");
    }

    #[test]
    fn unsupported_protocol_versions_and_missing_user_are_refused() {
        let refusal = parse_startup_packet(&raw_frame(196_609, &[])).expect_err("unsupported");
        assert_eq!(refusal.code, "unsupported_protocol_version");
        assert_eq!(refusal.sqlstate, "0A000");

        let startup = parse_startup_packet(&startup_frame(
            PROTOCOL_VERSION_3,
            &[("database", "twinning")],
        ))
        .expect("parse startup");
        let refusal = negotiate_startup(startup).expect_err("missing user should refuse");
        assert_eq!(refusal.code, "invalid_authorization_specification");
        assert_eq!(refusal.sqlstate, "28000");
    }

    #[test]
    fn declared_parameter_status_baseline_is_stable_without_application_name() {
        assert_eq!(
            declared_parameter_statuses(&BTreeMap::new()),
            expected_parameter_statuses(None)
        );
    }

    fn startup_frame(protocol_version: u32, params: &[(&str, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (key, value) in params {
            body.extend_from_slice(key.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        startup_frame_bytes(protocol_version, &body)
    }

    fn startup_frame_bytes(protocol_version: u32, body: &[u8]) -> Vec<u8> {
        raw_frame(protocol_version, body)
    }

    fn cancel_request_frame(process_id: i32, secret_key: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&process_id.to_be_bytes());
        body.extend_from_slice(&secret_key.to_be_bytes());
        raw_frame(super::CANCEL_REQUEST_CODE, &body)
    }

    fn raw_frame(code: u32, body: &[u8]) -> Vec<u8> {
        let length = (8 + body.len()) as i32;
        let mut frame = Vec::with_capacity(length as usize);
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&code.to_be_bytes());
        frame.extend_from_slice(body);
        frame
    }

    fn expected_parameter_statuses(application_name: Option<&str>) -> Vec<ParameterStatus> {
        let mut statuses = vec![
            ParameterStatus {
                name: String::from("client_encoding"),
                value: String::from("UTF8"),
            },
            ParameterStatus {
                name: String::from("DateStyle"),
                value: String::from("ISO, MDY"),
            },
            ParameterStatus {
                name: String::from("integer_datetimes"),
                value: String::from("on"),
            },
            ParameterStatus {
                name: String::from("server_encoding"),
                value: String::from("UTF8"),
            },
            ParameterStatus {
                name: String::from("server_version"),
                value: String::from("16.0"),
            },
            ParameterStatus {
                name: String::from("standard_conforming_strings"),
                value: String::from("on"),
            },
        ];

        if let Some(application_name) = application_name {
            statuses.push(ParameterStatus {
                name: String::from("application_name"),
                value: String::from(application_name),
            });
        }

        statuses
    }

    fn expect_startup_packet(packet: StartupPacket) -> Result<StartupMessage, &'static str> {
        match packet {
            StartupPacket::Startup(startup) => Ok(startup),
            StartupPacket::SslRequest | StartupPacket::CancelRequest { .. } => {
                Err("expected startup packet")
            }
        }
    }

    fn decode_parameter_statuses(frames: &[Vec<u8>]) -> Vec<ParameterStatus> {
        frames
            .iter()
            .map(|frame| {
                assert_eq!(frame[0], b'S');
                let body = &frame[5..];
                let mut parts = body.split(|byte| *byte == 0);
                let name = parts.next().expect("name");
                let value = parts.next().expect("value");
                assert_eq!(parts.next(), Some([].as_slice()));
                ParameterStatus {
                    name: String::from_utf8(name.to_vec()).expect("utf8 name"),
                    value: String::from_utf8(value.to_vec()).expect("utf8 value"),
                }
            })
            .collect()
    }

    fn decode_backend_key_data(frame: &[u8]) -> BackendKeyData {
        assert_eq!(frame[0], b'K');
        assert_eq!(
            i32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]),
            12
        );
        BackendKeyData {
            process_id: i32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]),
            secret_key: i32::from_be_bytes([frame[9], frame[10], frame[11], frame[12]]),
        }
    }
}
