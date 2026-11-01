//! Shared diagnostics and process helpers for M3+ integration tests.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Child;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

static ARTIFACTS: OnceLock<PathBuf> = OnceLock::new();

pub fn artifacts_dir(binary: &str) -> &'static PathBuf {
    ARTIFACTS.get_or_init(|| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_millis();
        let path = std::env::temp_dir()
            .join("twinning-mcp-tests")
            .join(format!("{binary}-{}-{now}", std::process::id()));
        fs::create_dir_all(&path).expect("create test artifacts directory");
        println!("ARTIFACTS: {}", path.display());
        path
    })
}

pub fn poll_until<T>(
    deadline: Instant,
    interval: Duration,
    what: &str,
    mut observe: impl FnMut() -> Option<T>,
) -> T {
    let start = Instant::now();
    let label = test_label();
    loop {
        if let Some(value) = observe() {
            println!(
                "[{label}] poll what={what:?} elapsed_ms={}",
                start.elapsed().as_millis()
            );
            return value;
        }
        let last = format!("not observed after {} ms", start.elapsed().as_millis());
        if Instant::now() >= deadline {
            panic!(
                "[{label}] poll timeout what={what:?} elapsed_ms={} last={last}",
                start.elapsed().as_millis()
            );
        }
        thread::sleep(interval);
    }
}

fn test_label() -> String {
    std::thread::current()
        .name()
        .and_then(|name| name.get(..3))
        .filter(|prefix| prefix.starts_with('s') && prefix[1..].chars().all(|c| c.is_ascii_digit()))
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| "TEST".into())
}

pub fn scaled_deadline(timeout: Duration) -> Instant {
    let scale = std::env::var("TWINNING_TEST_TIMEOUT_SCALE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(1.0);
    Instant::now() + timeout.mul_f64(scale)
}

pub fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .and_then(|listener| listener.local_addr())
        .expect("bind ephemeral loopback port")
        .port()
}

pub struct ProcessGroupDropGuard {
    child: Option<Child>,
}

impl ProcessGroupDropGuard {
    pub fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    pub fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("guard still owns child")
    }

    pub fn disarm(mut self) -> Child {
        self.child.take().expect("guard still owns child")
    }
}

impl Drop for ProcessGroupDropGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().ok().flatten().is_none() {
                // SAFETY: tests using this helper start their child with process_group(0).
                let _ = unsafe { libc::killpg(child.id() as libc::pid_t, libc::SIGKILL) };
            }
            let _ = child.wait();
        }
    }
}

pub fn skipped(id: &str, reason: &str, artifacts: &std::path::Path) {
    let line = format!("SKIPPED: {id}: {reason}");
    println!("{line}");
    let mut output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(artifacts.join("skips.txt"))
        .expect("open skips file");
    writeln!(output, "{line}").expect("write skip reason");
}
