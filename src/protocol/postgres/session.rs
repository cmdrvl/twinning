use thiserror::Error;

use crate::result::{AckResult, KernelResult, ResultTag};

use super::frames::{ResultFrameMetadata, encode_kernel_result_frames};

pub const DEFAULT_MAX_CYCLES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionStatus {
    #[default]
    Idle,
    InTransaction,
    FailedTransaction,
}

impl TransactionStatus {
    pub fn ready_byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::FailedTransaction => b'E',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCycle {
    pub frames: Vec<Vec<u8>>,
    pub transaction_status: TransactionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SessionLoop {
    transaction_status: TransactionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionLoopError {
    #[error("declared session loop exhausted cycle budget of {max_cycles}")]
    CycleLimitExceeded { max_cycles: usize },
}

impl SessionLoop {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    pub fn process_result(
        &mut self,
        result: &KernelResult,
        metadata: ResultFrameMetadata<'_>,
    ) -> SessionCycle {
        self.transaction_status = next_transaction_status(self.transaction_status, result);

        let mut frames = encode_kernel_result_frames(result, metadata);
        frames.push(ready_for_query_frame(self.transaction_status));

        SessionCycle {
            frames,
            transaction_status: self.transaction_status,
        }
    }

    pub fn run_script<'a, I>(
        &mut self,
        results: I,
        max_cycles: usize,
    ) -> Result<Vec<SessionCycle>, SessionLoopError>
    where
        I: IntoIterator<Item = (&'a KernelResult, ResultFrameMetadata<'a>)>,
    {
        let mut cycles = Vec::new();

        for (index, (result, metadata)) in results.into_iter().enumerate() {
            if index >= max_cycles {
                return Err(SessionLoopError::CycleLimitExceeded { max_cycles });
            }
            cycles.push(self.process_result(result, metadata));
        }

        Ok(cycles)
    }
}

pub fn ready_for_query_frame(status: TransactionStatus) -> Vec<u8> {
    let mut frame = Vec::with_capacity(6);
    frame.push(b'Z');
    frame.extend_from_slice(&5_i32.to_be_bytes());
    frame.push(status.ready_byte());
    frame
}

fn next_transaction_status(current: TransactionStatus, result: &KernelResult) -> TransactionStatus {
    match result {
        KernelResult::Ack(AckResult {
            tag: ResultTag::Begin,
            ..
        }) => TransactionStatus::InTransaction,
        KernelResult::Ack(AckResult {
            tag: ResultTag::Commit | ResultTag::Rollback,
            ..
        }) => TransactionStatus::Idle,
        KernelResult::Refusal(_) if current == TransactionStatus::InTransaction => {
            TransactionStatus::FailedTransaction
        }
        _ => current,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        ir::ScalarValue,
        result::{AckResult, KernelResult, ReadResult, RefusalResult, ResultTag},
    };

    use super::{
        DEFAULT_MAX_CYCLES, SessionLoop, SessionLoopError, TransactionStatus, ready_for_query_frame,
    };

    #[test]
    fn begin_commit_and_rollback_drive_ready_for_query_status() {
        let mut session = SessionLoop::new();

        let begin = session.process_result(
            &KernelResult::Ack(AckResult {
                tag: ResultTag::Begin,
                rows_affected: 0,
            }),
            Default::default(),
        );
        assert_eq!(begin.transaction_status, TransactionStatus::InTransaction);
        assert_eq!(
            decode_ready_status(begin.frames.last().expect("ready frame")),
            b'T'
        );

        let commit = session.process_result(
            &KernelResult::Ack(AckResult {
                tag: ResultTag::Commit,
                rows_affected: 0,
            }),
            Default::default(),
        );
        assert_eq!(commit.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            decode_ready_status(commit.frames.last().expect("ready frame")),
            b'I'
        );

        let begin_again = session.process_result(
            &KernelResult::Ack(AckResult {
                tag: ResultTag::Begin,
                rows_affected: 0,
            }),
            Default::default(),
        );
        assert_eq!(
            begin_again.transaction_status,
            TransactionStatus::InTransaction
        );

        let rollback = session.process_result(
            &KernelResult::Ack(AckResult {
                tag: ResultTag::Rollback,
                rows_affected: 0,
            }),
            Default::default(),
        );
        assert_eq!(rollback.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            decode_ready_status(rollback.frames.last().expect("ready frame")),
            b'I'
        );
    }

    #[test]
    fn protocol_visible_refusal_keeps_connection_alive_and_marks_failed_tx() {
        let mut session = SessionLoop::new();
        session.process_result(
            &KernelResult::Ack(AckResult {
                tag: ResultTag::Begin,
                rows_affected: 0,
            }),
            Default::default(),
        );

        let refusal = session.process_result(
            &KernelResult::Refusal(RefusalResult {
                code: String::from("unsupported_shape"),
                message: String::from("window functions are outside the declared subset"),
                sqlstate: String::from("0A000"),
                detail: BTreeMap::from([(String::from("shape"), String::from("window_function"))]),
            }),
            Default::default(),
        );

        assert_eq!(
            refusal.transaction_status,
            TransactionStatus::FailedTransaction
        );
        assert_eq!(
            decode_error_frame(refusal.frames.first().expect("error frame"))
                .expect("error frame should contain SQLSTATE"),
            "0A000"
        );
        assert_eq!(
            decode_ready_status(refusal.frames.last().expect("ready frame")),
            b'E'
        );

        let rollback = session.process_result(
            &KernelResult::Ack(AckResult {
                tag: ResultTag::Rollback,
                rows_affected: 0,
            }),
            Default::default(),
        );
        assert_eq!(rollback.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            decode_ready_status(rollback.frames.last().expect("ready frame")),
            b'I'
        );
    }

    #[test]
    fn read_results_and_sync_append_ready_for_query_without_extra_disconnect_logic() {
        let mut session = SessionLoop::new();

        let read = session.process_result(
            &KernelResult::Read(ReadResult {
                columns: vec![String::from("deal_id"), String::from("deal_name")],
                rows: vec![vec![
                    ScalarValue::Text(String::from("deal-1")),
                    ScalarValue::Text(String::from("Alpha")),
                ]],
            }),
            Default::default(),
        );
        assert_eq!(read.transaction_status, TransactionStatus::Idle);
        assert_eq!(
            decode_ready_status(read.frames.last().expect("ready frame")),
            b'I'
        );
        assert_eq!(read.frames.first().expect("row description")[0], b'T');

        let sync = session.process_result(
            &KernelResult::Ack(AckResult {
                tag: ResultTag::Sync,
                rows_affected: 0,
            }),
            Default::default(),
        );
        assert_eq!(sync.frames.len(), 1);
        assert_eq!(
            decode_ready_status(sync.frames.last().expect("ready frame")),
            b'I'
        );
    }

    #[test]
    fn bounded_session_loop_refuses_to_run_past_declared_cycle_limit() {
        let begin = KernelResult::Ack(AckResult {
            tag: ResultTag::Begin,
            rows_affected: 0,
        });
        let commit = KernelResult::Ack(AckResult {
            tag: ResultTag::Commit,
            rows_affected: 0,
        });

        let mut session = SessionLoop::new();
        let error = session
            .run_script(
                [(&begin, Default::default()), (&commit, Default::default())],
                1,
            )
            .expect_err("cycle budget should be enforced");

        assert_eq!(
            error,
            SessionLoopError::CycleLimitExceeded { max_cycles: 1 }
        );
        assert_eq!(DEFAULT_MAX_CYCLES, 1024);
    }

    #[test]
    fn ready_for_query_frame_uses_postgres_status_bytes() {
        assert_eq!(
            ready_for_query_frame(TransactionStatus::Idle),
            vec![b'Z', 0, 0, 0, 5, b'I']
        );
        assert_eq!(
            ready_for_query_frame(TransactionStatus::InTransaction),
            vec![b'Z', 0, 0, 0, 5, b'T']
        );
        assert_eq!(
            ready_for_query_frame(TransactionStatus::FailedTransaction),
            vec![b'Z', 0, 0, 0, 5, b'E']
        );
    }

    fn decode_ready_status(frame: &[u8]) -> u8 {
        assert_eq!(frame[0], b'Z');
        assert_eq!(
            i32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]),
            5
        );
        frame[5]
    }

    fn decode_error_frame(frame: &[u8]) -> Result<String, String> {
        if frame.first().copied() != Some(b'E') {
            return Err(String::from("frame must begin with `E`"));
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
                .ok_or_else(|| String::from("error field must be nul-terminated"))?;
            let value = String::from_utf8(frame[offset..offset + terminator].to_vec())
                .map_err(|error| format!("error field must be valid UTF-8: {error}"))?;
            offset += terminator + 1;

            if field_type == b'C' {
                return Ok(value);
            }
        }

        Err(String::from("missing SQLSTATE field"))
    }
}
