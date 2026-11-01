//! Deterministic lookup and validation for the `twinning` executable.

use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::errors::ToolError;
use crate::log::{self, Level};

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

pub fn locate(explicit: Option<&Path>, env_override: Option<&Path>) -> Result<PathBuf, ToolError> {
    let executable = env::current_exe().ok();
    let path_entries: Vec<PathBuf> = env::var_os("PATH")
        .map(|value| env::split_paths(&value).collect())
        .unwrap_or_default();
    locate_with_probe(
        explicit,
        env_override,
        executable.as_deref(),
        &path_entries,
        probe,
    )
}

pub fn locate_with_probe<F>(
    explicit: Option<&Path>,
    env_override: Option<&Path>,
    current_exe: Option<&Path>,
    path_entries: &[PathBuf],
    mut is_valid: F,
) -> Result<PathBuf, ToolError>
where
    F: FnMut(&Path) -> bool,
{
    for candidate in candidates(explicit, env_override, current_exe, path_entries) {
        if is_valid(&candidate) {
            return Ok(candidate);
        }
    }
    Err(ToolError {
        code: "E_TWINNING_NOT_FOUND",
        message: "twinning binary: no candidate passed `twinning --describe`".into(),
        hint: "Set TWINNING_BIN or pass --twinning-bin <path>",
    })
}

fn candidates(
    explicit: Option<&Path>,
    env_override: Option<&Path>,
    current_exe: Option<&Path>,
    path_entries: &[PathBuf],
) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if let Some(path) = explicit {
        result.push(path.to_path_buf());
    }
    if let Some(path) = env_override {
        result.push(path.to_path_buf());
    }
    if let Some(executable) = current_exe
        && let Ok(executable) = executable.canonicalize()
        && let Some(parent) = executable.parent()
    {
        result.push(parent.join(binary_name()));
    }
    result.extend(path_entries.iter().map(|entry| entry.join(binary_name())));
    let mut deduped = Vec::new();
    for path in result {
        if !deduped.contains(&path) {
            deduped.push(path);
        }
    }
    deduped
}

fn binary_name() -> &'static str {
    if cfg!(windows) {
        "twinning.exe"
    } else {
        "twinning"
    }
}

fn probe(candidate: &Path) -> bool {
    let Ok(mut child) = Command::new(candidate)
        .arg("--describe")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    else {
        return false;
    };
    let deadline = Instant::now() + PROBE_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let Ok(output) = child.wait_with_output() else {
                    return false;
                };
                if !status.success() {
                    log::emit(
                        Level::Warn,
                        None,
                        "twinning_probe_failed",
                        &[
                            ("exit_code", status.code().unwrap_or(-1).to_string()),
                            ("stderr_bytes", output.stderr.len().to_string()),
                        ],
                    );
                    return false;
                }
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(&output.stdout) else {
                    return false;
                };
                return value["version"].as_str().is_some()
                    && value["name"].as_str() == Some("twinning");
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{locate_with_probe, probe};
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn u11_lookup_order_is_flag_env_sibling_then_path_and_skips_bad_probe() {
        let temp = tempfile::tempdir().unwrap();
        let exe = temp.path().join("install/bin/twinning-mcp");
        std::fs::create_dir_all(exe.parent().unwrap()).unwrap();
        std::fs::write(&exe, "test executable marker").unwrap();
        let flag = PathBuf::from("/flag/twinning");
        let env = PathBuf::from("/env/twinning");
        let path = vec![PathBuf::from("/path")];
        let sibling = exe
            .canonicalize()
            .unwrap()
            .parent()
            .unwrap()
            .join("twinning");
        let mut checked = Vec::new();
        let found = locate_with_probe(Some(&flag), Some(&env), Some(&exe), &path, |candidate| {
            checked.push(candidate.to_path_buf());
            candidate == sibling
        })
        .unwrap();
        assert_eq!(sibling, found);
        assert_eq!(vec![flag, env, sibling,], checked);
        let error = locate_with_probe::<_>(None, None, None, &path, |_| false).unwrap_err();
        assert_eq!("E_TWINNING_NOT_FOUND", error.code);
    }

    #[cfg(unix)]
    #[test]
    fn u11_probe_skips_nonzero_describe_candidate_and_accepts_valid_manifest() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let bad_dir = temp.path().join("bad");
        let good_dir = temp.path().join("good");
        fs::create_dir_all(&bad_dir).unwrap();
        fs::create_dir_all(&good_dir).unwrap();
        let bad = bad_dir.join("twinning");
        let good = good_dir.join("twinning");
        fs::write(&bad, "#!/bin/sh\nprintf 'probe failure\n' >&2\nexit 1\n").unwrap();
        fs::write(
            &good,
            "#!/bin/sh\nprintf '{\"name\":\"twinning\",\"version\":\"0.1.0\"}\\n'\n",
        )
        .unwrap();
        fs::set_permissions(&bad, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&good, fs::Permissions::from_mode(0o755)).unwrap();
        let found = locate_with_probe(None, None, None, &[bad_dir, good_dir], probe).unwrap();
        assert_eq!(good, found);
    }
}
