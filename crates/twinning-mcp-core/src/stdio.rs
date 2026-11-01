//! Newline-delimited JSON-RPC stdio transport.

use std::io::{self, BufRead, Write};

use serde_json::Value as JsonValue;

use crate::jsonrpc::{
    JsonRpcError, JsonRpcRequest, JsonRpcResponse, error_response, parse_request,
};

/// Reads one request per line and writes and flushes each response before
/// reading the next line. Notifications do not produce a response.
pub fn serve<R, W, F>(reader: R, writer: W, mut dispatch: F) -> io::Result<()>
where
    R: BufRead,
    W: Write,
    F: FnMut(JsonRpcRequest) -> Option<JsonRpcResponse>,
{
    serve_until(reader, writer, &mut dispatch, || false)
}

/// Like [`serve`], with a stop predicate used after an interrupted read.
pub fn serve_until<R, W, F, S>(
    mut reader: R,
    mut writer: W,
    mut dispatch: F,
    mut stopping: S,
) -> io::Result<()>
where
    R: BufRead,
    W: Write,
    F: FnMut(JsonRpcRequest) -> Option<JsonRpcResponse>,
    S: FnMut() -> bool,
{
    let mut line = String::new();
    loop {
        line.clear();
        let count = match reader.read_line(&mut line) {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted && stopping() => break,
            Err(error) => return Err(error),
        };
        if count == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let response = match parse_request(&line) {
            Ok(request) => dispatch(request),
            Err(error) => Some(protocol_error_response(error)),
        };
        let Some(response) = response else {
            continue;
        };
        let mut rendered = serde_json::to_vec(&response)?;
        rendered.push(b'\n');
        writer.write_all(&rendered)?;
        writer.flush()?;
    }
    Ok(())
}

fn protocol_error_response(error: JsonRpcError) -> JsonRpcResponse {
    error_response(JsonValue::Null, error.code, error.message, error.data)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Write};

    use serde_json::json;

    use crate::jsonrpc::{JsonRpcRequest, result_response};

    use super::serve;

    #[derive(Default)]
    struct FlushCounter {
        bytes: Vec<u8>,
        writes: usize,
        flushes: usize,
    }

    impl Write for FlushCounter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn each_response_is_a_line_and_is_flushed_before_next_request() {
        let input = Cursor::new(
            b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
        );
        let mut output = FlushCounter::default();
        serve(input, &mut output, |request: JsonRpcRequest| {
            request.id.map(|id| result_response(id, json!({})))
        })
        .expect("stdio loop succeeds");

        assert_eq!(1, output.writes);
        assert_eq!(1, output.flushes);
        assert_eq!(
            1,
            output.bytes.iter().filter(|byte| **byte == b'\n').count()
        );
        let response: serde_json::Value = serde_json::from_slice(
            output
                .bytes
                .strip_suffix(b"\n")
                .expect("newline terminated"),
        )
        .expect("one JSON response");
        assert_eq!(json!({"jsonrpc":"2.0","id":1,"result":{}}), response);
    }

    #[test]
    fn parse_errors_are_protocol_responses_and_empty_lines_are_ignored() {
        let input = Cursor::new(b"\n{\n".as_slice());
        let mut output = Vec::new();
        serve(input, &mut output, |_| None).expect("stdio loop succeeds");
        let response: serde_json::Value =
            serde_json::from_slice(output.strip_suffix(b"\n").expect("newline terminated"))
                .expect("one error response");
        assert_eq!(-32700, response["error"]["code"]);
    }
}
