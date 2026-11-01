//! Remove state left by dead MCP servers and terminate only recorded twinning children.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use crate::log::{self, Level};

const CHILD_TERM_GRACE: Duration = Duration::from_secs(2);
const CHILD_KILL_GRACE: Duration = Duration::from_secs(2);
const CHILD_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    pub server_dirs_removed: Vec<PathBuf>,
    pub child_pids_signaled: Vec<u32>,
}

pub fn sweep_stale(root: &Path, current_server_pid: u32) -> std::io::Result<SweepReport> {
    let mut report = SweepReport::default();
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(report),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let Some(server_pid) = entry.file_name().to_string_lossy().parse::<u32>().ok() else {
            continue;
        };
        if server_pid == current_server_pid || process_alive(server_pid) {
            continue;
        }
        for child_dir in fs::read_dir(entry.path())? {
            let child_dir = child_dir?;
            if !child_dir.file_type()?.is_dir() || child_dir.file_type()?.is_symlink() {
                continue;
            }
            let pid_path = child_dir.path().join("child.pid");
            let Ok(pid_text) = fs::read_to_string(pid_path) else {
                continue;
            };
            let Ok(child_pid) = pid_text.trim().parse::<u32>() else {
                continue;
            };
            if terminate_recorded_child(child_pid)? {
                report.child_pids_signaled.push(child_pid);
                log::emit(
                    Level::Warn,
                    None,
                    "sweep_killed",
                    &[("pid", child_pid.to_string())],
                );
            }
        }
        fs::remove_dir_all(entry.path())?;
        report.server_dirs_removed.push(entry.path());
    }
    Ok(report)
}

fn terminate_recorded_child(pid: u32) -> io::Result<bool> {
    if !child_process_running(pid) {
        return Ok(false);
    }
    if !signal_process(pid, libc::SIGTERM)? {
        return Ok(false);
    }
    if wait_for_child_exit(pid, CHILD_TERM_GRACE) {
        return Ok(true);
    }

    if child_process_running(pid) {
        let _ = signal_process(pid, libc::SIGKILL)?;
    }
    if !wait_for_child_exit(pid, CHILD_KILL_GRACE) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("recorded twinning child {pid} did not exit after SIGKILL"),
        ));
    }
    Ok(true)
}

fn signal_process(pid: u32, signal: libc::c_int) -> io::Result<bool> {
    // SAFETY: pid was read from a server-owned child.pid file and checked against the
    // process command line before this signal is sent.
    let result = unsafe { libc::kill(pid as libc::pid_t, signal) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else {
        Err(error)
    }
}

fn child_process_running(pid: u32) -> bool {
    process_alive(pid) && command_line_mentions_twinning(pid)
}

fn wait_for_child_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while child_process_running(pid) {
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(CHILD_POLL_INTERVAL);
    }
    true
}

fn process_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // SAFETY: signal zero only performs an existence/permission check.
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(target_os = "linux")]
fn command_line_mentions_twinning(pid: u32) -> bool {
    fs::read(format!("/proc/{pid}/cmdline"))
        .map(|bytes| {
            String::from_utf8_lossy(&bytes)
                .to_ascii_lowercase()
                .contains("twinning")
        })
        .unwrap_or(false)
}

#[cfg(target_os = "macos")]
fn command_line_mentions_twinning(pid: u32) -> bool {
    let pid_text = pid.to_string();
    std::process::Command::new("ps")
        .args(["-p", pid_text.as_str(), "-o", "command="])
        .output()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .to_ascii_lowercase()
                .contains("twinning")
        })
        .unwrap_or(false)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn command_line_mentions_twinning(_pid: u32) -> bool {
    false
}
