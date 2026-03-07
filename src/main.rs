#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    match twinning::run() {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("twinning: {error}");
            ExitCode::from(2)
        }
    }
}
