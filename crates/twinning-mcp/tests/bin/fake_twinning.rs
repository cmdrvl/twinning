//! Process stub for supervisor tests. Its success report is derived from a real captured report.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::{Value, json};
use signal_hook::iterator::Signals;

const READY_REPORT: &str = include_str!("../fixtures/report_ready.json");

fn main() {
    if let Err(error) = run() {
        eprintln!("fake_twinning: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--describe") {
        println!(
            "{{\"version\":\"operator.v0\",\"name\":\"twinning\",\"description\":\"declared-subset Postgres twin test stub\"}}"
        );
        return Ok(());
    }
    if args.starts_with(&["doctor".into(), "health".into()]) {
        println!("{{\"version\":\"twinning.v0\",\"status\":\"healthy\",\"outcome\":\"READY\"}}");
        return Ok(());
    }
    if args.starts_with(&["doctor".into(), "capabilities".into()]) {
        println!(
            "{{\"version\":\"twinning.v0\",\"commands\":[\"postgres\",\"doctor health\",\"doctor capabilities\"]}}"
        );
        return Ok(());
    }
    let mode = std::env::var("FAKE_TWINNING_MODE").unwrap_or_else(|_| "ok".into());
    let report = value_after(&args, "--report").map(PathBuf::from);
    let trace = value_after(&args, "--query-trace").map(PathBuf::from);
    let contract = value_after(&args, "--export-seed-contract").map(PathBuf::from);
    if mode == "exit_early" {
        eprintln!("fake exited early");
        std::process::exit(3);
    }
    if mode == "contract_fail" {
        return Err("seed contract export failed".into());
    }
    if mode == "addr_in_use_once" && first_bind_attempt(report.as_deref())? {
        print_bind_refusal();
        std::process::exit(2);
    }
    if mode == "always_bind_refuse" {
        print_bind_refusal();
        std::process::exit(2);
    }
    if let Some(path) = contract.as_deref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut output = fs::File::create(path)?;
        writeln!(
            output,
            "{}",
            json!({"version":"twinning.seed-contract.v0","kind":"target","twin":"postgres","target_kind":"relation","target":"public.stub","fields":[]})
        )?;
        return Ok(());
    }
    if mode == "never_listen" || mode == "delayed_shutdown" {
        let mut signals = term_signals()?;
        if let Some(started) = std::env::var_os("FAKE_TWINNING_STARTED_FILE") {
            fs::write(started, "ready\n")?;
        }
        signals.forever().next();
        if mode == "delayed_shutdown" {
            let delay = std::env::var("FAKE_TWINNING_SHUTDOWN_DELAY_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(150);
            std::thread::sleep(Duration::from_millis(delay));
            if let Some(path) = std::env::var_os("FAKE_TWINNING_SHUTDOWN_WRITE_PATH") {
                let path = PathBuf::from(path);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, "shutdown report\n")?;
            }
            if let Some(marker) = std::env::var_os("FAKE_TWINNING_SHUTDOWN_MARKER") {
                fs::write(marker, "shutdown complete\n")?;
            }
        }
        return Ok(());
    }
    if mode == "listen_late" {
        let delay = std::env::var("FAKE_TWINNING_LISTEN_DELAY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(250);
        let (wake, wait) = mpsc::channel::<()>();
        let _ = wait.recv_timeout(Duration::from_millis(delay));
        drop(wake);
    }
    let port = value_after(&args, "--port")
        .ok_or("--port is required")?
        .parse::<u16>()?;
    let _listener = TcpListener::bind(("127.0.0.1", port))?;
    let mut signals = term_signals()?;
    let ignore_term = mode == "ignore_sigterm";
    if ignore_term {
        for _signal in signals.forever() {}
    } else {
        signals.forever().next();
    }
    if mode == "crash_on_stop" {
        eprintln!("fake crash on stop");
        std::process::exit(2);
    }
    if let Some(path) = report {
        let schema = value_after(&args, "--schema").unwrap_or_default();
        let value: Value = serde_json::from_str(READY_REPORT)?;
        let mut value = value;
        substitute_report(
            &mut value,
            schema,
            port,
            path.parent().unwrap_or(Path::new(".")),
        );
        write_json(&path, &value)?;
    }
    if let Some(path) = trace {
        write_json(
            &path,
            &json!({"version":"twinning.query-trace.v0","policy":"redacted","events":[]}),
        )?;
    }
    if mode == "exit1_with_report" {
        std::process::exit(1);
    }
    Ok(())
}

fn print_bind_refusal() {
    println!(
        "{}",
        json!({
            "version": "twinning.v0",
            "outcome": "REFUSAL",
            "refusal": {
                "code": "E_RUNTIME_IO",
                "message": "Live runtime failed during `listener_bind`.",
                "detail": { "stage": "listener_bind", "error": "127.0.0.1:0: Address already in use" },
                "next_command": null
            }
        })
    );
}

fn first_bind_attempt(report: Option<&Path>) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(parent) = report.and_then(Path::parent) else {
        return Ok(false);
    };
    let counter = parent.join("addr_in_use_once.counter");
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(counter)
    {
        Ok(mut file) => {
            writeln!(file, "1")?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn term_signals() -> Result<Signals, Box<dyn std::error::Error>> {
    Ok(Signals::new([signal_hook::consts::SIGTERM])?)
}

fn value_after<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].as_str())
}

fn substitute_report(value: &mut Value, schema: &str, port: u16, state_dir: &Path) {
    match value {
        Value::Object(object) => {
            for (key, item) in object.iter_mut() {
                match key.as_str() {
                    "port" => *item = Value::from(port),
                    "host" => *item = Value::from("127.0.0.1"),
                    "source" => *item = Value::from(schema),
                    "written_to" => {
                        *item = Value::from(state_dir.join("snapshot.twin").display().to_string())
                    }
                    _ => substitute_report(item, schema, port, state_dir),
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                substitute_report(item, schema, port, state_dir);
            }
        }
        _ => {}
    }
}

fn write_json(path: &Path, value: &Value) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
