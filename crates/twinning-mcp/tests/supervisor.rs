pub mod common;

use std::fs;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;

use common::{ProcessGroupDropGuard, poll_until, scaled_deadline};
use twinning_mcp::registry::TwinStatus;
use twinning_mcp::stale::sweep_stale;
use twinning_mcp::supervisor::{StartRequest, Supervisor, SupervisorConfig};

static MODE_LOCK: Mutex<()> = Mutex::new(());
const FAKE_TWINNING: &str = env!("CARGO_BIN_EXE_fake_twinning");

fn make_supervisor(temp: &tempfile::TempDir) -> Supervisor {
    let schema = temp.path().join("schema.sql");
    fs::write(&schema, "CREATE TABLE t (id INT PRIMARY KEY);").unwrap();
    Supervisor::new(SupervisorConfig {
        twinning_bin: FAKE_TWINNING.into(),
        state_root: temp
            .path()
            .join("twinning-mcp")
            .join(std::process::id().to_string()),
        max_twins: 5,
        ready_timeout: Some(Duration::from_secs(3)),
        stop_timeout: Some(Duration::from_secs(3)),
    })
    .unwrap()
}

fn start_request(temp: &tempfile::TempDir) -> StartRequest {
    StartRequest {
        workdir: temp.path().to_path_buf(),
        schema: temp.path().join("schema.sql"),
        verify: None,
        seed: None,
        restore: None,
        ttl: Duration::from_secs(60),
    }
}

#[test]
fn supervisor_starts_and_stops_fake_twinning() {
    let _mode = MODE_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let _artifacts = common::artifacts_dir("twinning_mcp_supervisor");
    let temp = tempfile::tempdir().unwrap();
    let fake = std::path::PathBuf::from(FAKE_TWINNING);
    assert!(
        fake.is_file(),
        "fake_twinning binary missing at {}",
        fake.display()
    );
    // SAFETY: this test owns the child mode setting for its isolated test process.
    unsafe { std::env::set_var("FAKE_TWINNING_MODE", "ok") };
    let supervisor = make_supervisor(&temp);
    let record = supervisor.start(start_request(&temp)).unwrap();
    assert_ne!(0, record.port);
    assert!(record.stdout_path.is_file());
    assert!(record.stderr_path.is_file());
    let stopped = supervisor.stop(&record.id).unwrap();
    assert_eq!(0, stopped.exit_code);
    assert_eq!("twinning.v0", stopped.report["version"]);
    assert_eq!(TwinStatus::Stopped, stopped.status);
    let second_stop = supervisor.stop(&record.id).unwrap();
    assert_eq!(stopped, second_stop);
}

#[test]
fn supervisor_retries_the_listener_bind_refusal_once() {
    let _mode = MODE_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let _artifacts = common::artifacts_dir("twinning_mcp_supervisor");
    let temp = tempfile::tempdir().unwrap();
    let fake = std::path::PathBuf::from(FAKE_TWINNING);
    assert!(
        fake.is_file(),
        "fake_twinning binary missing at {}",
        fake.display()
    );
    // SAFETY: this test owns the child mode setting for its isolated test process.
    unsafe { std::env::set_var("FAKE_TWINNING_MODE", "addr_in_use_once") };
    let supervisor = make_supervisor(&temp);
    let record = supervisor.start(start_request(&temp)).unwrap();
    assert_eq!(
        1,
        fs::read_to_string(record.state_dir.join("addr_in_use_once.counter"))
            .unwrap()
            .trim()
            .parse::<u8>()
            .unwrap()
    );
    assert!(record.port > 0);
    assert_eq!(0, supervisor.stop(&record.id).unwrap().exit_code);
}

#[test]
fn supervisor_includes_child_stderr_when_report_is_unreadable() {
    let _mode = MODE_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let _artifacts = common::artifacts_dir("twinning_mcp_supervisor");
    let temp = tempfile::tempdir().unwrap();
    let fake = std::path::PathBuf::from(FAKE_TWINNING);
    assert!(
        fake.is_file(),
        "fake_twinning binary missing at {}",
        fake.display()
    );
    // SAFETY: this test owns the child mode setting for its isolated test process.
    unsafe { std::env::set_var("FAKE_TWINNING_MODE", "crash_on_stop") };
    let supervisor = make_supervisor(&temp);
    let record = supervisor.start(start_request(&temp)).unwrap();
    let error = supervisor.stop(&record.id).unwrap_err();
    assert_eq!("E_REPORT_UNREADABLE", error.code);
    assert!(error.message.contains("fake crash on stop"));
}

#[test]
fn stale_sweep_terminates_recorded_twinning_child_and_removes_dead_server_directory() {
    let _artifacts = common::artifacts_dir("twinning_mcp_supervisor");
    let temp = tempfile::tempdir().unwrap();
    let dead = temp.path().join(u32::MAX.to_string());
    let current = temp.path().join(std::process::id().to_string());
    fs::create_dir_all(dead.join("tw_abcdef")).unwrap();
    fs::create_dir_all(current.join("tw_fedcba")).unwrap();
    let fake = std::path::PathBuf::from(FAKE_TWINNING);
    assert!(
        fake.is_file(),
        "fake_twinning binary missing at {}",
        fake.display()
    );
    let started = temp.path().join("fake_started");
    let child = Command::new(fake)
        .env("FAKE_TWINNING_MODE", "never_listen")
        .env("FAKE_TWINNING_STARTED_FILE", &started)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .process_group(0)
        .spawn()
        .unwrap();
    let child_pid = child.id();
    let mut guard = ProcessGroupDropGuard::new(child);
    poll_until(
        scaled_deadline(Duration::from_secs(2)),
        Duration::from_millis(20),
        "fake child signal handler installed",
        || started.is_file().then_some(()),
    );
    fs::write(dead.join("tw_abcdef/child.pid"), format!("{child_pid}\n")).unwrap();
    let report = sweep_stale(temp.path(), std::process::id()).unwrap();
    assert!(report.server_dirs_removed.contains(&dead));
    assert!(!dead.exists());
    assert!(current.exists());
    assert_eq!(vec![child_pid], report.child_pids_signaled);
    let exited = poll_until(
        scaled_deadline(Duration::from_secs(2)),
        Duration::from_millis(20),
        "stale child exit",
        || guard.child_mut().try_wait().ok().flatten(),
    );
    assert!(exited.success());
}
