#[allow(dead_code)]
mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

struct TestHome {
    home: TempDir,
    workdir: TempDir,
}

impl TestHome {
    fn new() -> Self {
        let home = tempfile::tempdir().expect("temporary HOME is created");
        let workdir = tempfile::tempdir().expect("temporary workdir is created");
        Self { home, workdir }
    }

    fn config(&self, client: &str) -> PathBuf {
        match client {
            "claude-desktop" => self
                .home
                .path()
                .join("Library/Application Support/Claude/claude_desktop_config.json"),
            "cursor" => self.home.path().join(".cursor/mcp.json"),
            other => panic!("no config path for {other}"),
        }
    }
}

fn run_cli(id: &str, area: &TestHome, config: Option<&Path>, args: &[&str]) -> Output {
    run_cli_with(id, area.home.path(), area.workdir.path(), config, args, &[])
}

fn run_cli_with(
    id: &str,
    home: &Path,
    cwd: &Path,
    config: Option<&Path>,
    args: &[&str],
    environment: &[(&str, &Path)],
) -> Output {
    let before = config
        .map(file_fingerprint)
        .unwrap_or_else(|| "not_applicable".into());
    let command = std::iter::once("twinning-mcp")
        .chain(args.iter().copied())
        .collect::<Vec<_>>()
        .join(" ");
    let started = Instant::now();
    let mut command_builder = Command::new(env!("CARGO_BIN_EXE_twinning-mcp"));
    command_builder
        .args(args)
        .current_dir(cwd)
        .env("HOME", home)
        .env_remove("TWINNING_BIN")
        .env_remove("TWINNING_MCP_WORKDIR")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (name, value) in environment {
        command_builder.env(name, value);
    }
    let output = command_builder.output().expect("twinning-mcp starts");
    let elapsed = started.elapsed();
    let after = config
        .map(file_fingerprint)
        .unwrap_or_else(|| "not_applicable".into());
    record_cli_run(id, &command, home, &before, &after, elapsed, &output);
    output
}

fn run_cli_from(id: &str, home: &Path, cwd: &Path, config: Option<&Path>, args: &[&str]) -> Output {
    run_cli_with(id, home, cwd, config, args, &[])
}

#[cfg(unix)]
fn write_twinning_script(path: &Path, health_exit: i32) {
    use std::os::unix::fs::PermissionsExt;

    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--describe\" ]; then\n  printf '%s\\n' '{{\"name\":\"twinning\",\"version\":\"1.0.0\"}}'\n  exit 0\nfi\nif [ \"$1\" = \"doctor\" ] && [ \"$2\" = \"health\" ]; then\n  printf '%s\\n' '{{\"version\":\"twinning.v0\",\"status\":\"healthy\",\"outcome\":\"READY\"}}'\n  exit {health_exit}\nfi\nif [ \"$1\" = \"doctor\" ] && [ \"$2\" = \"capabilities\" ]; then\n  printf '%s\\n' '{{\"version\":\"twinning.v0\",\"commands\":[\"postgres\",\"doctor health\",\"doctor capabilities\"]}}'\n  exit 0\nfi\nexit 1\n"
    );
    fs::write(path, script).expect("test twinning script writes inside temporary directory");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("test twinning script becomes executable");
}

fn assert_next_line(output: &Output) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr
            .lines()
            .last()
            .is_some_and(|line| line.starts_with("next: ")),
        "failure did not end with a next command: {stderr:?}"
    );
}

fn file_fingerprint(path: &Path) -> String {
    let Ok(bytes) = fs::read(path) else {
        return "missing".into();
    };
    format!("bytes={} sha256={:x}", bytes.len(), Sha256::digest(bytes))
}

fn record_cli_run(
    id: &str,
    command: &str,
    home: &Path,
    before: &str,
    after: &str,
    elapsed: Duration,
    output: &Output,
) {
    let stdout = safe_stream(&output.stdout);
    let stderr = safe_stream(&output.stderr);
    let log = format!(
        "[{id}] command={command:?}\n[{id}] home={} config_before={before:?} config_after={after:?}\n[{id}] elapsed_ms={} exit_code={:?}\n[{id}] stdout={stdout:?}\n[{id}] stderr={stderr:?}\n",
        home.display(),
        elapsed.as_millis(),
        output.status.code()
    );
    let artifacts = common::artifacts_dir("setup_configs");
    fs::write(artifacts.join(format!("{id}.log")), &log).expect("CLI log writes");
    print!("{log}");
}

fn safe_stream(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let contains_config_snippet = serde_json::from_slice::<Value>(bytes)
        .ok()
        .is_some_and(|value| value["mcpServers"].is_object());
    let is_diff = text.starts_with("--- ") && text.contains(" (proposed)\n");
    if !contains_config_snippet && !is_diff {
        return text.into_owned();
    }

    let digest = format!("{:x}", Sha256::digest(bytes));
    let mut redacted = format!(
        "<config {} redacted bytes={} sha256={}>",
        if is_diff { "diff" } else { "snippet" },
        bytes.len(),
        digest
    );
    if is_diff && let Some((_, summary)) = text.split_once("action: ") {
        redacted.push_str("\naction: ");
        redacted.push_str(summary);
    }
    redacted
}

fn config_value(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("config exists")).expect("config is valid JSON")
}

fn assert_twins_entry(config: &Value, expected_workdir: &Path) {
    let entry = &config["mcpServers"]["twins"];
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_twinning-mcp"))
        .canonicalize()
        .expect("test binary canonicalizes");
    assert_eq!(
        binary.to_str().expect("binary path is UTF-8"),
        entry["command"]
    );
    assert_eq!(
        json!([
            "--workdir",
            expected_workdir.canonicalize().unwrap().to_str().unwrap()
        ]),
        entry["args"]
    );
}

fn backup_files(config: &Path) -> Vec<PathBuf> {
    let parent = config.parent().expect("config has a parent");
    let prefix = format!(
        "{}.bak-",
        config
            .file_name()
            .expect("config has a name")
            .to_string_lossy()
    );
    fs::read_dir(parent)
        .expect("config parent exists")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
        })
        .collect()
}

#[test]
fn c01_claude_desktop_config_is_created() {
    let area = TestHome::new();
    let config = area.config("claude-desktop");
    let workdir = area.workdir.path();
    let output = run_cli(
        "C01",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "claude-desktop",
            "--workdir",
            workdir.to_str().unwrap(),
        ],
    );

    assert_eq!(Some(0), output.status.code());
    assert_twins_entry(&config_value(&config), workdir);
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("Restart Claude Desktop to load the server.")
    );
    assert!(backup_files(&config).is_empty());
}

#[test]
fn c02_existing_servers_are_preserved_and_backed_up() {
    let area = TestHome::new();
    let config = area.config("claude-desktop");
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    let original = br#"{
  "mcpServers": {
    "other": {"command": "/bin/true", "args": ["--keep"]}
  },
  "custom": {"enabled": true}
}
"#;
    fs::write(&config, original).unwrap();
    let other_before = config_value(&config)["mcpServers"]["other"].clone();
    let workdir = area.workdir.path();
    let output = run_cli(
        "C02",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "claude-desktop",
            "--workdir",
            workdir.to_str().unwrap(),
        ],
    );

    assert_eq!(Some(0), output.status.code());
    let after = config_value(&config);
    assert_eq!(other_before, after["mcpServers"]["other"]);
    assert_eq!(json!({"enabled": true}), after["custom"]);
    assert_twins_entry(&after, workdir);
    let backups = backup_files(&config);
    assert_eq!(1, backups.len());
    assert_eq!(original, fs::read(&backups[0]).unwrap().as_slice());
}

#[test]
fn c03_second_setup_reports_exact_no_change() {
    let area = TestHome::new();
    let config = area.config("claude-desktop");
    let args = [
        "setup",
        "--client",
        "claude-desktop",
        "--workdir",
        area.workdir.path().to_str().unwrap(),
    ];
    let first = run_cli("C03-first", &area, Some(&config), &args);
    assert_eq!(Some(0), first.status.code());
    let before_second = fs::read(&config).unwrap();
    let second = run_cli("C03-second", &area, Some(&config), &args);

    assert_eq!(Some(0), second.status.code());
    assert_eq!(b"no change\n", second.stdout.as_slice());
    assert_eq!(before_second, fs::read(&config).unwrap());
    assert!(backup_files(&config).is_empty());
}

#[test]
fn c04_invalid_json_is_refused_without_modifying_the_file() {
    let area = TestHome::new();
    let config = area.config("claude-desktop");
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    let invalid = b"{ this is not json\n";
    fs::write(&config, invalid).unwrap();
    let output = run_cli(
        "C04",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "claude-desktop",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
        ],
    );

    assert_eq!(Some(2), output.status.code());
    assert_eq!(invalid, fs::read(&config).unwrap().as_slice());
    assert!(backup_files(&config).is_empty());
    assert!(
        fs::read_dir(config.parent().unwrap())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp-"))
    );
}

#[test]
fn c05_cursor_config_is_created_and_second_run_is_no_change() {
    let area = TestHome::new();
    let config = area.config("cursor");
    let args = [
        "setup",
        "--client",
        "cursor",
        "--workdir",
        area.workdir.path().to_str().unwrap(),
    ];
    let first = run_cli("C05-first", &area, Some(&config), &args);
    assert_eq!(Some(0), first.status.code());
    assert_twins_entry(&config_value(&config), area.workdir.path());
    assert!(backup_files(&config).is_empty());
    let before_second = fs::read(&config).unwrap();
    let second = run_cli("C05-second", &area, Some(&config), &args);

    assert_eq!(Some(0), second.status.code());
    assert_eq!(b"no change\n", second.stdout.as_slice());
    assert_eq!(before_second, fs::read(&config).unwrap());
}

#[test]
fn c06_dry_run_shows_change_without_creating_config_or_directories() {
    let area = TestHome::new();
    let config = area.config("cursor");
    let output = run_cli(
        "C06",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "cursor",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
            "--dry-run",
        ],
    );

    assert_eq!(Some(0), output.status.code());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("mcpServers"));
    assert!(stdout.contains("twins"));
    assert!(!config.exists());
    assert!(!config.parent().unwrap().exists());
}

#[test]
fn c07_print_accepts_unknown_client_name_and_writes_nothing() {
    let area = TestHome::new();
    let output = run_cli(
        "C07",
        &area,
        None,
        &[
            "setup",
            "--client",
            "future-client",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
            "--print",
        ],
    );

    assert_eq!(Some(0), output.status.code());
    let snippet: Value = serde_json::from_slice(&output.stdout).expect("print is valid JSON");
    assert_twins_entry(&snippet, area.workdir.path());
    assert!(!area.home.path().join("Library").exists());
    assert!(!area.home.path().join(".cursor").exists());
}

#[test]
fn c08_read_only_directory_is_safe_and_successful_replace_is_atomic_and_keeps_mode() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let area = TestHome::new();
    let config = area.config("cursor");
    let parent = config.parent().unwrap();
    fs::create_dir_all(parent).unwrap();
    let original = br#"{"mcpServers":{"twins":{"command":"old","args":[]}}}"#;
    fs::write(&config, original).unwrap();
    let directory_mode = fs::metadata(parent).unwrap().permissions().mode();
    fs::set_permissions(parent, fs::Permissions::from_mode(directory_mode & !0o222)).unwrap();
    let readonly = run_cli(
        "C08-readonly",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "cursor",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
        ],
    );
    fs::set_permissions(parent, fs::Permissions::from_mode(directory_mode)).unwrap();

    assert_eq!(Some(3), readonly.status.code());
    assert_eq!(original, fs::read(&config).unwrap().as_slice());
    assert!(backup_files(&config).is_empty());
    assert!(fs::read_dir(parent).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp-")
    }));

    let before_metadata = fs::metadata(&config).unwrap();
    let before_inode = before_metadata.ino();
    let before_mode = before_metadata.permissions().mode() & 0o777;
    let updated = run_cli(
        "C08-atomic",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "cursor",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
        ],
    );

    assert_eq!(Some(0), updated.status.code());
    assert_twins_entry(&config_value(&config), area.workdir.path());
    let after_metadata = fs::metadata(&config).unwrap();
    assert_eq!(before_mode, after_metadata.permissions().mode() & 0o777);
    assert_ne!(
        before_inode,
        after_metadata.ino(),
        "rename replaced the file atomically"
    );
    assert_eq!(1, backup_files(&config).len());
    assert!(fs::read_dir(parent).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp-")
    }));
}

#[test]
fn c09_json_and_plain_text_outputs_keep_stdout_and_stderr_separate() {
    let area = TestHome::new();
    let config = area.config("cursor");
    let script = area.home.path().join("twinning-ok");
    write_twinning_script(&script, 0);
    let twin_env = [("TWINNING_BIN", script.as_path())];

    let doctor_json = run_cli_with(
        "C09-doctor-json",
        area.home.path(),
        area.workdir.path(),
        None,
        &["doctor", "--json"],
        &twin_env,
    );
    assert_eq!(Some(0), doctor_json.status.code());
    assert!(doctor_json.stderr.is_empty());
    let doctor: Value =
        serde_json::from_slice(&doctor_json.stdout).expect("one doctor JSON object");
    for key in [
        "twinning_bin",
        "twinning_version",
        "health",
        "capabilities",
        "workdir",
        "cap",
        "restricted",
        "checks",
    ] {
        assert!(doctor.get(key).is_some(), "missing doctor field {key}");
    }
    assert_eq!("healthy", doctor["health"]["status"]);
    assert!(
        doctor["checks"]
            .as_array()
            .is_some_and(|checks| !checks.is_empty())
    );

    let dry_run = run_cli(
        "C09-setup-dry-run-json",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "cursor",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
            "--dry-run",
            "--json",
        ],
    );
    assert_eq!(Some(0), dry_run.status.code());
    assert!(String::from_utf8_lossy(&dry_run.stderr).starts_with("--- "));
    let result: Value = serde_json::from_slice(&dry_run.stdout).expect("one setup JSON object");
    assert_eq!("cursor", result["client"]);
    assert_eq!("dry_run", result["action"]);
    assert!(result["config_path"].is_string());
    assert!(result["backup_path"].is_null());
    assert_eq!(false, result["restart_required"]);
    assert!(String::from_utf8_lossy(&dry_run.stderr).starts_with("--- "));
    assert!(!config.exists());

    let created = run_cli(
        "C09-setup-created-json",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "cursor",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
            "--json",
        ],
    );
    assert_eq!(Some(0), created.status.code());
    assert!(created.stderr.is_empty());
    let created_json: Value =
        serde_json::from_slice(&created.stdout).expect("one setup JSON object");
    assert_eq!("created", created_json["action"]);
    assert_eq!(true, created_json["restart_required"]);

    fs::write(
        &config,
        br#"{"mcpServers":{"twins":{"command":"old","args":[]}}}"#,
    )
    .expect("replace test config with an old twins entry");
    let updated = run_cli(
        "C09-setup-updated-json",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "cursor",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
            "--json",
        ],
    );
    assert_eq!(Some(0), updated.status.code());
    assert!(updated.stderr.is_empty());
    let updated_json: Value =
        serde_json::from_slice(&updated.stdout).expect("one setup JSON object");
    assert_eq!("updated", updated_json["action"]);
    assert!(updated_json["backup_path"].is_string());

    let printed = run_cli(
        "C09-setup-print-json",
        &area,
        None,
        &[
            "setup",
            "--client",
            "future-client",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
            "--print",
            "--json",
        ],
    );
    assert_eq!(Some(0), printed.status.code());
    assert!(printed.stderr.is_empty());
    let printed_json: Value =
        serde_json::from_slice(&printed.stdout).expect("one setup JSON object");
    assert_eq!("printed", printed_json["action"]);
    assert_eq!("future-client", printed_json["client"]);
    assert!(printed_json["config_path"].is_null());

    let plain_area = TestHome::new();
    let plain_config = plain_area.config("cursor");
    let plain = run_cli(
        "C09-setup-plain",
        &plain_area,
        Some(&plain_config),
        &[
            "setup",
            "--client",
            "cursor",
            "--workdir",
            plain_area.workdir.path().to_str().unwrap(),
            "--dry-run",
        ],
    );
    assert_eq!(Some(0), plain.status.code());
    assert!(plain.stderr.is_empty());
    let plain_text = String::from_utf8_lossy(&plain.stdout);
    assert!(plain_text.contains("action: dry_run"));
    assert!(plain_text.contains("restart_required: false"));
    assert!(plain_text.ends_with("No file was written.\n"));

    let doctor_plain = run_cli_with(
        "C09-doctor-plain",
        area.home.path(),
        area.workdir.path(),
        None,
        &["doctor"],
        &twin_env,
    );
    assert_eq!(Some(0), doctor_plain.status.code());
    assert!(doctor_plain.stderr.is_empty());
    assert!(String::from_utf8_lossy(&doctor_plain.stdout).contains("health: ok (healthy"));
}

#[test]
fn c10_cli_exit_codes_and_next_commands_match_the_contract() {
    let area = TestHome::new();

    let unknown_flag = run_cli(
        "C10-unknown-flag",
        &area,
        None,
        &["setup", "--unknown-flag"],
    );
    assert_eq!(Some(1), unknown_flag.status.code());
    assert_next_line(&unknown_flag);

    let config = area.config("cursor");
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    fs::write(&config, b"not json").unwrap();
    let invalid_json = run_cli(
        "C10-invalid-json",
        &area,
        Some(&config),
        &[
            "setup",
            "--client",
            "cursor",
            "--workdir",
            area.workdir.path().to_str().unwrap(),
        ],
    );
    assert_eq!(Some(2), invalid_json.status.code());
    assert_next_line(&invalid_json);
    assert_eq!(b"not json", fs::read(&config).unwrap().as_slice());

    let unsafe_area = TestHome::new();
    let unsafe_default = run_cli_from(
        "C10-unsafe-default-workdir",
        unsafe_area.home.path(),
        Path::new("/"),
        None,
        &["setup", "--client", "cursor"],
    );
    assert_eq!(Some(2), unsafe_default.status.code());
    assert_next_line(&unsafe_default);
    assert!(!unsafe_area.home.path().join(".cursor").exists());

    let missing = area.home.path().join("missing-twinning");
    let missing_doctor = run_cli(
        "C10-missing-twinning",
        &area,
        None,
        &["doctor", "--twinning-bin", missing.to_str().unwrap()],
    );
    assert_eq!(Some(3), missing_doctor.status.code());
    assert_next_line(&missing_doctor);

    let failed_health = area.home.path().join("twinning-health-fails");
    write_twinning_script(&failed_health, 1);
    let failed_doctor = run_cli(
        "C10-failed-health",
        &area,
        None,
        &["doctor", "--twinning-bin", failed_health.to_str().unwrap()],
    );
    assert_eq!(Some(4), failed_doctor.status.code());
    assert_next_line(&failed_doctor);

    let healthy = area.home.path().join("twinning-healthy");
    write_twinning_script(&healthy, 0);
    let success = run_cli(
        "C10-success",
        &area,
        None,
        &["doctor", "--twinning-bin", healthy.to_str().unwrap()],
    );
    assert_eq!(Some(0), success.status.code());
}
