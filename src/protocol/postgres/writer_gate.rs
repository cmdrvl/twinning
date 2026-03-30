use std::collections::BTreeMap;

use thiserror::Error;

use crate::result::RefusalResult;

pub const WRITER_CONTENTION_SQLSTATE: &str = "55P03";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WriterGate {
    active_writer: Option<String>,
}

impl WriterGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn active_writer(&self) -> Option<&str> {
        self.active_writer.as_deref()
    }

    pub fn try_admit(&mut self, session_id: impl Into<String>) -> Result<(), WriterGateError> {
        let session_id = session_id.into();
        match self.active_writer.as_deref() {
            Some(active_writer) if active_writer == session_id => Ok(()),
            Some(active_writer) => Err(WriterGateError::WriterBusy {
                active_session: active_writer.to_owned(),
            }),
            None => {
                self.active_writer = Some(session_id);
                Ok(())
            }
        }
    }

    pub fn release(&mut self, session_id: &str) -> bool {
        match self.active_writer.as_deref() {
            Some(active_writer) if active_writer == session_id => {
                self.active_writer = None;
                true
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WriterGateError {
    #[error("session `{active_session}` already owns the mutable writer slot")]
    WriterBusy { active_session: String },
}

impl WriterGateError {
    pub fn sqlstate(&self) -> &'static str {
        WRITER_CONTENTION_SQLSTATE
    }

    pub fn into_refusal_result(self, requesting_session: impl Into<String>) -> RefusalResult {
        let requesting_session = requesting_session.into();
        let sqlstate = self.sqlstate();
        match self {
            Self::WriterBusy { active_session } => RefusalResult {
                code: String::from("writer_contention"),
                message: format!(
                    "session `{requesting_session}` cannot acquire the mutable writer slot while `{active_session}` owns it"
                ),
                sqlstate: String::from(sqlstate),
                detail: BTreeMap::from([
                    (String::from("requesting_session"), requesting_session),
                    (String::from("active_session"), active_session),
                ]),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        protocol::postgres::{
            session::{SessionLoop, TransactionStatus},
            writer_gate::{WRITER_CONTENTION_SQLSTATE, WriterGate, WriterGateError},
        },
        result::KernelResult,
    };

    #[test]
    fn second_writer_receives_55p03_and_current_writer_stays_admitted() {
        let mut gate = WriterGate::new();
        gate.try_admit("writer")
            .expect("first writer should be admitted");

        let error = gate
            .try_admit("reader")
            .expect_err("second writer should be refused");
        assert_eq!(
            error,
            WriterGateError::WriterBusy {
                active_session: String::from("writer"),
            }
        );
        assert_eq!(error.sqlstate(), WRITER_CONTENTION_SQLSTATE);
        assert_eq!(gate.active_writer(), Some("writer"));

        let mut reader_session = SessionLoop::new();
        let contention_cycle = reader_session.process_result(
            &KernelResult::Refusal(error.into_refusal_result("reader")),
            Default::default(),
        );
        assert_eq!(reader_session.transaction_status(), TransactionStatus::Idle);
        assert_eq!(
            decode_error_sqlstate(contention_cycle.frames.first().expect("error frame"))
                .expect("writer contention error frame should include SQLSTATE"),
            WRITER_CONTENTION_SQLSTATE
        );
        assert_eq!(
            decode_ready_status(contention_cycle.frames.last().expect("ready frame")),
            b'I'
        );
    }

    #[test]
    fn releasing_writer_slot_allows_next_session_to_acquire_it() {
        let mut gate = WriterGate::new();
        gate.try_admit("writer")
            .expect("first writer should be admitted");

        assert!(!gate.release("reader"));
        assert_eq!(gate.active_writer(), Some("writer"));

        assert!(gate.release("writer"));
        assert!(gate.active_writer().is_none());

        gate.try_admit("reader")
            .expect("released writer slot should be reusable");
        assert_eq!(gate.active_writer(), Some("reader"));
    }

    #[test]
    fn same_writer_can_reenter_without_spurious_contention() {
        let mut gate = WriterGate::new();
        gate.try_admit("writer")
            .expect("first writer should be admitted");
        gate.try_admit("writer")
            .expect("same writer should reenter cleanly");

        assert_eq!(gate.active_writer(), Some("writer"));
    }

    fn decode_error_sqlstate(frame: &[u8]) -> Result<String, &'static str> {
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

            if field_type == b'C' {
                return Ok(value);
            }
        }

        Err("missing SQLSTATE field")
    }

    fn decode_ready_status(frame: &[u8]) -> u8 {
        assert_eq!(frame[0], b'Z');
        frame[5]
    }
}
