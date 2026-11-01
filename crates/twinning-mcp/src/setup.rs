//! Safe, local configuration writes for supported MCP clients.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use serde_json::{Map, Value, json};

use crate::paths::Workdir;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    User,
    Safety,
    Environment,
}

#[derive(Debug)]
pub struct SetupError {
    pub kind: FailureKind,
    pub message: String,
    pub next: String,
}

#[derive(Debug)]
pub struct SetupResult {
    pub client: String,
    pub config_path: Option<PathBuf>,
    pub action: &'static str,
    pub backup_path: Option<PathBuf>,
    pub restart_required: bool,
    pub snippet: Value,
    pub diff: Option<String>,
}

struct ConfigData {
    value: Value,
    original: Option<Vec<u8>>,
    mode: Option<u32>,
}

pub fn configure(
    client: Option<&str>,
    explicit_workdir: Option<&Path>,
    launch_dir: &Path,
    home: Option<&Path>,
    executable: &Path,
    dry_run: bool,
    print: bool,
) -> Result<SetupResult, SetupError> {
    if print && dry_run {
        return Err(user_error(
            "setup: --print cannot be combined with --dry-run",
            "twinning-mcp setup --help",
        ));
    }

    let client = match client {
        Some(client) if !client.trim().is_empty() => client.to_owned(),
        Some(_) => {
            return Err(user_error(
                "setup: --client must not be empty",
                "twinning-mcp setup --help",
            ));
        }
        None if cfg!(target_os = "macos") => "claude-desktop".to_owned(),
        None => {
            return Err(user_error(
                "setup: name a client with --client",
                "twinning-mcp setup --print --client <name>",
            ));
        }
    };

    let workdir_home = home.unwrap_or(Path::new("/"));
    let workdir =
        Workdir::resolve(explicit_workdir, None, launch_dir, workdir_home).map_err(|error| {
            let kind = if error.code == "E_ARGUMENT_INVALID" {
                FailureKind::User
            } else {
                FailureKind::Environment
            };
            setup_error(
                kind,
                format!("setup: could not resolve workdir: {}", error.message),
                "twinning-mcp setup --workdir <existing-directory>",
            )
        })?;
    if workdir.unsafe_default {
        return Err(safety_error(
            "setup: default workdir is / or the user's home directory",
            "twinning-mcp setup --workdir <safe-directory>",
        ));
    }

    let executable = executable.canonicalize().map_err(|error| {
        environment_error(
            format!("setup: cannot resolve the running executable: {error}"),
            "twinning-mcp doctor",
        )
    })?;
    let snippet = json!({
        "mcpServers": {
            "twins": {
                "command": executable,
                "args": ["--workdir", workdir.path]
            }
        }
    });

    if print {
        return Ok(SetupResult {
            client,
            config_path: None,
            action: "printed",
            backup_path: None,
            restart_required: false,
            snippet,
            diff: None,
        });
    }

    if !supports_file_config(&client) {
        return Err(user_error(
            &format!("setup: unsupported client for file configuration: {client}"),
            "twinning-mcp setup --print --client <name>",
        ));
    }
    let home = normalized_home(
        home.ok_or_else(|| {
            environment_error(
                "setup: HOME is not set".into(),
                "set HOME or use twinning-mcp setup --print --client <name>",
            )
        })?,
        launch_dir,
    )?;
    let config_path = config_path(&client, &home).ok_or_else(|| {
        user_error(
            &format!("setup: unsupported client for file configuration: {client}"),
            "twinning-mcp setup --print --client <name>",
        )
    })?;
    let config_path = absolute_config_path(&config_path, &home);
    let parent = config_path.parent().expect("client config has a parent");
    ensure_parent_stays_in_home(&home, parent)?;

    let ConfigData {
        value: mut current,
        original,
        mode,
    } = read_config(&config_path)?;
    let servers = current
        .as_object_mut()
        .expect("read_config guarantees a JSON object")
        .entry("mcpServers")
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(servers) = servers.as_object_mut() else {
        return Err(safety_error(
            "setup: config field mcpServers must be a JSON object",
            &format!(
                "edit {} to make mcpServers an object, then rerun setup",
                config_path.display()
            ),
        ));
    };
    let desired = snippet["mcpServers"]["twins"].clone();
    if servers.get("twins") == Some(&desired) {
        return Ok(SetupResult {
            client,
            config_path: Some(config_path),
            action: "no_change",
            backup_path: None,
            restart_required: false,
            snippet,
            diff: None,
        });
    }

    servers.insert("twins".into(), desired);
    let proposed = render_json(&current).map_err(|error| {
        environment_error(
            format!("setup: could not render config JSON: {error}"),
            "twinning-mcp doctor",
        )
    })?;
    let diff = make_diff(&config_path, original.as_deref().unwrap_or(b""), &proposed);
    if dry_run {
        return Ok(SetupResult {
            client,
            config_path: Some(config_path),
            action: "dry_run",
            backup_path: None,
            restart_required: false,
            snippet,
            diff: Some(diff),
        });
    }

    fs::create_dir_all(parent).map_err(|error| {
        environment_error(
            format!(
                "setup: cannot create config directory {}: {error}",
                parent.display()
            ),
            "check permissions on the client config directory, then rerun setup",
        )
    })?;
    ensure_parent_stays_in_home(&home, parent)?;
    let backup_path = match original.as_deref() {
        Some(bytes) => Some(write_backup(&config_path, bytes, mode)?),
        None => None,
    };
    atomic_replace(&config_path, &proposed, mode)?;

    Ok(SetupResult {
        client,
        config_path: Some(config_path),
        action: if original.is_some() {
            "updated"
        } else {
            "created"
        },
        backup_path,
        restart_required: true,
        snippet,
        diff: None,
    })
}

fn config_path(client: &str, home: &Path) -> Option<PathBuf> {
    match client {
        "cursor" => Some(home.join(".cursor/mcp.json")),
        "claude-desktop" if cfg!(target_os = "macos") => {
            Some(home.join("Library/Application Support/Claude/claude_desktop_config.json"))
        }
        _ => None,
    }
}

fn supports_file_config(client: &str) -> bool {
    client == "cursor" || (client == "claude-desktop" && cfg!(target_os = "macos"))
}

fn normalized_home(home: &Path, launch_dir: &Path) -> Result<PathBuf, SetupError> {
    let absolute = if home.is_absolute() {
        home.to_path_buf()
    } else {
        launch_dir.join(home)
    };
    absolute.canonicalize().map_err(|error| {
        environment_error(
            format!("setup: cannot resolve HOME {}: {error}", absolute.display()),
            "set HOME to an existing directory, then rerun setup",
        )
    })
}

fn absolute_config_path(path: &Path, home: &Path) -> PathBuf {
    let relative = path.strip_prefix(home).unwrap_or(path);
    home.join(relative)
}

fn ensure_parent_stays_in_home(home: &Path, parent: &Path) -> Result<(), SetupError> {
    let relative = parent.strip_prefix(home).map_err(|_| {
        safety_error(
            "setup: client config directory is outside HOME",
            "use a HOME directory containing the documented client config path",
        )
    })?;
    let mut cursor = home.to_path_buf();
    let canonical_home = home.canonicalize().map_err(|error| {
        environment_error(
            format!("setup: cannot resolve HOME {}: {error}", home.display()),
            "set HOME to an existing directory, then rerun setup",
        )
    })?;
    for component in relative.components() {
        if let Component::Normal(part) = component {
            cursor.push(part);
            match fs::symlink_metadata(&cursor) {
                Ok(_) => {
                    let canonical = cursor.canonicalize().map_err(|error| {
                        environment_error(
                            format!(
                                "setup: cannot resolve config directory {}: {error}",
                                cursor.display()
                            ),
                            "check the client config directory, then rerun setup",
                        )
                    })?;
                    if !canonical.starts_with(&canonical_home) {
                        return Err(safety_error(
                            "setup: client config directory resolves outside HOME",
                            "remove the external config-directory link and rerun setup",
                        ));
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(error) => {
                    return Err(environment_error(
                        format!(
                            "setup: cannot inspect config directory {}: {error}",
                            cursor.display()
                        ),
                        "check the client config directory, then rerun setup",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn read_config(path: &Path) -> Result<ConfigData, SetupError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ConfigData {
                value: Value::Object(Map::new()),
                original: None,
                mode: None,
            });
        }
        Err(error) => {
            return Err(environment_error(
                format!("setup: cannot read config {}: {error}", path.display()),
                "check permissions on the client config file, then rerun setup",
            ));
        }
    };
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        environment_error(
            format!("setup: cannot inspect config {}: {error}", path.display()),
            "check the client config file, then rerun setup",
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(safety_error(
            "setup: config must be a regular file, not a symlink or directory",
            &format!(
                "replace {} with a regular JSON file, then rerun setup",
                path.display()
            ),
        ));
    }
    let mode = unix_mode(&metadata);
    let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
        safety_error(
            &format!("setup: config is not valid JSON: {error}"),
            &format!("repair {} as valid JSON, then rerun setup", path.display()),
        )
    })?;
    if !value.is_object() {
        return Err(safety_error(
            "setup: config root must be a JSON object",
            &format!(
                "repair {} as a JSON object, then rerun setup",
                path.display()
            ),
        ));
    }
    Ok(ConfigData {
        value,
        original: Some(bytes),
        mode,
    })
}

#[cfg(unix)]
fn unix_mode(metadata: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;

    Some(metadata.mode() & 0o7777)
}

#[cfg(not(unix))]
fn unix_mode(_: &fs::Metadata) -> Option<u32> {
    None
}

fn render_json(value: &Value) -> io::Result<Vec<u8>> {
    let mut rendered = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    rendered.push(b'\n');
    Ok(rendered)
}

fn make_diff(path: &Path, current: &[u8], proposed: &[u8]) -> String {
    let current = String::from_utf8_lossy(current);
    let proposed = String::from_utf8_lossy(proposed);
    let mut diff = format!(
        "--- {}\n+++ {} (proposed)\n",
        path.display(),
        path.display()
    );
    for line in current.lines() {
        diff.push('-');
        diff.push_str(line);
        diff.push('\n');
    }
    for line in proposed.lines() {
        diff.push('+');
        diff.push_str(line);
        diff.push('\n');
    }
    diff
}

fn write_backup(path: &Path, bytes: &[u8], mode: Option<u32>) -> Result<PathBuf, SetupError> {
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let name = path
        .file_name()
        .expect("config path has a file name")
        .to_string_lossy();
    let backup = path.with_file_name(format!("{name}.bak-{stamp}"));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&backup)
        .map_err(|error| {
            environment_error(
                format!("setup: cannot create backup {}: {error}", backup.display()),
                "check permissions on the client config directory, then rerun setup",
            )
        })?;
    let result = mode
        .map(|mode| set_file_mode(&file, mode))
        .unwrap_or(Ok(()))
        .and_then(|()| file.write_all(bytes))
        .and_then(|()| file.sync_all());
    drop(file);
    if let Err(error) = result {
        let _ = fs::remove_file(&backup);
        return Err(environment_error(
            format!("setup: cannot write backup {}: {error}", backup.display()),
            "check permissions on the client config directory, then rerun setup",
        ));
    }
    Ok(backup)
}

fn atomic_replace(path: &Path, bytes: &[u8], mode: Option<u32>) -> Result<(), SetupError> {
    let parent = path.parent().expect("config path has a parent");
    let (temp_path, mut temp_file) = create_temp_file(parent, mode).map_err(|error| {
        environment_error(
            format!(
                "setup: cannot create temporary config in {}: {error}",
                parent.display()
            ),
            "check permissions on the client config directory, then rerun setup",
        )
    })?;
    let write_result = temp_file
        .write_all(bytes)
        .and_then(|()| temp_file.sync_all());
    drop(temp_file);
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(environment_error(
            format!(
                "setup: cannot write temporary config {}: {error}",
                temp_path.display()
            ),
            "check permissions on the client config directory, then rerun setup",
        ));
    }
    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(environment_error(
            format!("setup: cannot replace config {}: {error}", path.display()),
            "check permissions on the client config directory, then rerun setup",
        ));
    }
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            environment_error(
                format!("setup: config was replaced but directory sync failed: {error}"),
                "run twinning-mcp doctor to inspect the configuration",
            )
        })?;
    Ok(())
}

fn create_temp_file(parent: &Path, mode: Option<u32>) -> io::Result<(PathBuf, File)> {
    let config_tmp_prefix = ".twinning-mcp-setup.tmp-";
    for _ in 0..100 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            "{config_tmp_prefix}-{}-{sequence}",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.mode(mode.unwrap_or(0o600));
        }
        match options.open(&path) {
            Ok(file) => {
                if let Some(mode) = mode
                    && let Err(error) = set_file_mode(&file, mode)
                {
                    drop(file);
                    let _ = fs::remove_file(&path);
                    return Err(error);
                }
                return Ok((path, file));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique temporary config path",
    ))
}

#[cfg(unix)]
fn set_file_mode(file: &File, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_file_mode(_: &File, _: u32) -> io::Result<()> {
    Ok(())
}

pub fn client_name(client: &str) -> &str {
    match client {
        "claude-desktop" => "Claude Desktop",
        "cursor" => "Cursor",
        other => other,
    }
}

fn setup_error(kind: FailureKind, message: String, next: &str) -> SetupError {
    SetupError {
        kind,
        message,
        next: next.to_owned(),
    }
}

fn user_error(message: &str, next: &str) -> SetupError {
    setup_error(FailureKind::User, message.to_owned(), next)
}

fn safety_error(message: &str, next: &str) -> SetupError {
    setup_error(FailureKind::Safety, message.to_owned(), next)
}

fn environment_error(message: String, next: &str) -> SetupError {
    setup_error(FailureKind::Environment, message, next)
}
