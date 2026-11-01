//! Read-only diagnostics for the `twinning` executable and server configuration.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::{
    locate,
    log::{self, Level},
    paths::Workdir,
};

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_POLL_INTERVAL: Duration = Duration::from_millis(20);

pub fn diagnose(
    explicit_bin: Option<&Path>,
    env_bin: Option<&Path>,
    workdir: &Result<Workdir, crate::errors::ToolError>,
    cap: usize,
    restricted: bool,
) -> Value {
    let workdir_status = match workdir {
        Ok(workdir) => json!({
            "path": workdir.path,
            "safe": !workdir.unsafe_default,
            "explicit": workdir.explicit
        }),
        Err(error) => json!({
            "path": Value::Null,
            "safe": false,
            "error": error.code
        }),
    };

    let binary = locate::locate(explicit_bin, env_bin).ok();
    let Some(binary) = binary else {
        return unavailable(workdir_status, cap, restricted);
    };

    let describe = run_json(&binary, &["--describe"]);
    let Some(describe) = describe.filter(|value| value["name"] == "twinning") else {
        return unavailable(workdir_status, cap, restricted);
    };
    let Some(version) = describe["version"].as_str() else {
        return unavailable(workdir_status, cap, restricted);
    };

    let Some(health) = run_json(&binary, &["doctor", "health", "--json"]) else {
        return unavailable(workdir_status, cap, restricted);
    };
    let Some(capabilities) = run_json(&binary, &["doctor", "capabilities", "--json"]) else {
        return unavailable(workdir_status, cap, restricted);
    };
    if health["version"] != "twinning.v0"
        || health["status"].as_str().is_none()
        || health["outcome"].as_str().is_none()
        || capabilities["version"] != "twinning.v0"
        || !capabilities["commands"].is_array()
    {
        return unavailable(workdir_status, cap, restricted);
    }
    let health = json!({"status":health["status"],"outcome":health["outcome"]});
    let capabilities = capabilities["commands"].clone();

    json!({
        "twinning_bin": binary,
        "twinning_version": version,
        "health": health,
        "capabilities": capabilities,
        "workdir": workdir_status,
        "cap": cap,
        "restricted": restricted
    })
}

fn unavailable(workdir: Value, cap: usize, restricted: bool) -> Value {
    json!({
        "twinning_bin": Value::Null,
        "twinning_version": Value::Null,
        "health": "unavailable",
        "capabilities": [],
        "workdir": workdir,
        "cap": cap,
        "restricted": restricted
    })
}

fn run_json(binary: &Path, args: &[&str]) -> Option<Value> {
    let mut command = Command::new(binary);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        command.process_group(0);
    }
    let mut child = command.spawn().ok()?;
    let pid = child.id();
    let mut stdout = child.stdout.take()?;
    let mut stderr = child.stderr.take()?;
    let stdout_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stdout.read_to_end(&mut bytes);
        (bytes, result)
    });
    let stderr_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stderr.read_to_end(&mut bytes);
        (bytes, result)
    });

    let deadline = Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(PROBE_POLL_INTERVAL);
            }
            Ok(None) => {
                kill_probe(pid);
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                log::emit(
                    Level::Warn,
                    None,
                    "twinning_probe_timeout",
                    &[("command", args.join(" "))],
                );
                return None;
            }
            Err(_) => {
                kill_probe(pid);
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                log::emit(
                    Level::Warn,
                    None,
                    "twinning_probe_failed",
                    &[("command", args.join(" "))],
                );
                return None;
            }
        }
    };
    let (stdout, stdout_result) = stdout_reader.join().ok()?;
    let (stderr, stderr_result) = stderr_reader.join().ok()?;
    if stdout_result.is_err() || stderr_result.is_err() || !status.success() {
        log::emit(
            Level::Warn,
            None,
            "twinning_probe_failed",
            &[
                ("command", args.join(" ")),
                ("exit_code", status.code().unwrap_or(-1).to_string()),
                ("stdout_bytes", stdout.len().to_string()),
                ("stderr_bytes", stderr.len().to_string()),
            ],
        );
        return None;
    }
    serde_json::from_slice(&stdout).ok().or_else(|| {
        log::emit(
            Level::Warn,
            None,
            "twinning_probe_invalid_json",
            &[
                ("command", args.join(" ")),
                ("stdout_bytes", stdout.len().to_string()),
            ],
        );
        None
    })
}

#[cfg(unix)]
fn kill_probe(pid: u32) {
    // SAFETY: the probe was started in a fresh process group with this PID.
    let _ = unsafe { libc::killpg(pid as libc::pid_t, libc::SIGKILL) };
}
