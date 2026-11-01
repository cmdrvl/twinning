//! Workdir and caller-path validation used before any child process is spawned.

use std::fs;
use std::path::{Path, PathBuf};

use crate::errors::ToolError;
use serde_json::Value;

pub const MAX_INPUT_BYTES: u64 = 5 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workdir {
    pub path: PathBuf,
    pub unsafe_default: bool,
    pub explicit: bool,
}

impl Workdir {
    pub fn resolve(
        explicit: Option<&Path>,
        env_workdir: Option<&Path>,
        launch_dir: &Path,
        home: &Path,
    ) -> Result<Self, ToolError> {
        let (requested, was_explicit) = if let Some(path) = explicit {
            (path, true)
        } else if let Some(path) = env_workdir {
            (path, false)
        } else {
            (launch_dir, false)
        };
        let absolute = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            launch_dir.join(requested)
        };
        let path = fs::canonicalize(&absolute).map_err(|error| path_error("workdir", error))?;
        if !path.is_dir() {
            return Err(argument_error("workdir", "must be an existing directory"));
        }
        let root = fs::canonicalize(Path::new("/")).unwrap_or_else(|_| PathBuf::from("/"));
        let canonical_home = fs::canonicalize(home).unwrap_or_else(|_| home.to_path_buf());
        let unsafe_default = !was_explicit && (path == root || path == canonical_home);
        Ok(Self {
            path,
            unsafe_default,
            explicit: was_explicit,
        })
    }
}

pub fn resolve_input(path: &str, workdir: &Path) -> Result<PathBuf, ToolError> {
    let requested = Path::new(path);
    let absolute = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        workdir.join(requested)
    };
    let canonical = fs::canonicalize(&absolute).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ToolError {
                code: "E_PATH_NOT_FOUND",
                message: format!("path: {path}: {error}"),
                hint: "Check the path and try again",
            }
        } else {
            path_error(path, error)
        }
    })?;
    let root = fs::canonicalize(workdir).map_err(|error| path_error("workdir", error))?;
    if !canonical.starts_with(&root) {
        return Err(ToolError {
            code: "E_PATH_OUTSIDE_WORKDIR",
            message: format!("path: {path}: resolves outside the workdir"),
            hint: "Move the file inside the workdir or choose a different workdir",
        });
    }
    Ok(canonical)
}

pub fn resolve_input_file(path: &str, workdir: &Path) -> Result<PathBuf, ToolError> {
    let canonical = resolve_input(path, workdir)?;
    let metadata = fs::metadata(&canonical).map_err(|error| path_error(path, error))?;
    if !metadata.is_file() {
        return Err(argument_error("path", "must name a regular file"));
    }
    if metadata.len() > MAX_INPUT_BYTES {
        return Err(argument_error("path", "must be no larger than 5 MiB"));
    }
    Ok(canonical)
}

pub fn resolve_output_file(path: &str, workdir: &Path) -> Result<PathBuf, ToolError> {
    let requested = Path::new(path);
    let absolute = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        workdir.join(requested)
    };
    let Some(file_name) = absolute.file_name() else {
        return Err(argument_error("path", "must name a file"));
    };
    if file_name == "." || file_name == ".." {
        return Err(argument_error("path", "must name a file"));
    }
    let parent = absolute.parent().unwrap_or(workdir);
    let canonical_parent = fs::canonicalize(parent).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ToolError {
                code: "E_PATH_NOT_FOUND",
                message: format!("path: {path}: parent directory does not exist"),
                hint: "Create the parent directory inside the workdir and try again",
            }
        } else {
            path_error(path, error)
        }
    })?;
    let root = fs::canonicalize(workdir).map_err(|error| path_error("workdir", error))?;
    let candidate = canonical_parent.join(file_name);
    if !candidate.starts_with(&root) {
        return Err(ToolError {
            code: "E_PATH_OUTSIDE_WORKDIR",
            message: format!("path: {path}: resolves outside the workdir"),
            hint: "Write the output file inside the workdir",
        });
    }
    match fs::symlink_metadata(&candidate) {
        Ok(_) => {
            let canonical =
                fs::canonicalize(&candidate).map_err(|error| path_error(path, error))?;
            if !canonical.starts_with(&root) {
                return Err(ToolError {
                    code: "E_PATH_OUTSIDE_WORKDIR",
                    message: format!("path: {path}: resolves outside the workdir"),
                    hint: "Write the output file inside the workdir",
                });
            }
            if !canonical.is_file() {
                return Err(argument_error("path", "must name a regular file"));
            }
            return Ok(canonical);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(path_error(path, error)),
    }
    Ok(candidate)
}

pub fn reject_nonlocal_refs(spec_path: &Path) -> Result<(), ToolError> {
    let bytes = fs::read(spec_path).map_err(|_| ToolError {
        code: "E_ARGUMENT_INVALID",
        message: "spec_path: unable to inspect OpenAPI references".into(),
        hint: "Check spec_path and use only references that begin with #/",
    })?;
    let spec = std::str::from_utf8(&bytes).map_err(|_| ToolError {
        code: "E_ARGUMENT_INVALID",
        message: "spec_path: OpenAPI spec must be UTF-8".into(),
        hint: "Save spec_path as UTF-8 and use only references that begin with #/",
    })?;

    let contains_nonlocal_ref = match serde_json::from_slice::<Value>(&bytes) {
        Ok(document) => json_has_nonlocal_ref(&document),
        Err(_) => yaml_has_nonlocal_ref(spec),
    };
    if contains_nonlocal_ref {
        return Err(ToolError {
            code: "E_ARGUMENT_INVALID",
            message: "spec_path: contains a non-local $ref".into(),
            hint: "Replace external references with local #/ references before starting the twin",
        });
    }
    Ok(())
}

fn json_has_nonlocal_ref(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            (key == "$ref"
                && value
                    .as_str()
                    .is_some_and(|reference| !reference.starts_with('#')))
                || json_has_nonlocal_ref(value)
        }),
        Value::Array(values) => values.iter().any(json_has_nonlocal_ref),
        _ => false,
    }
}

fn yaml_has_nonlocal_ref(spec: &str) -> bool {
    let bytes = spec.as_bytes();
    let mut index = 0;
    let mut flow_depth = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'#' && (index == 0 || bytes[index - 1].is_ascii_whitespace()) {
            index = bytes[index..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |offset| index + offset + 1);
            continue;
        }

        if matches!(bytes[index], b'\'' | b'"') {
            let quote = bytes[index];
            let (end, content) = quoted_scalar(bytes, index, quote);
            let colon = skip_space(bytes, end);
            if content == "$ref"
                && yaml_key_position(bytes, index, flow_depth)
                && colon < bytes.len()
                && bytes[colon] == b':'
                && yaml_ref_value_is_nonlocal(bytes, colon + 1)
            {
                return true;
            }
            index = end;
            continue;
        }

        if bytes[index..].starts_with(b"$ref")
            && yaml_key_position(bytes, index, flow_depth)
            && skip_space(bytes, index + 4) < bytes.len()
            && bytes[skip_space(bytes, index + 4)] == b':'
            && yaml_ref_value_is_nonlocal(bytes, skip_space(bytes, index + 4) + 1)
        {
            return true;
        }
        match bytes[index] {
            b'{' | b'[' => flow_depth += 1,
            b'}' | b']' => flow_depth = flow_depth.saturating_sub(1),
            _ => {}
        }
        index += 1;
    }
    false
}

fn yaml_key_position(bytes: &[u8], index: usize, flow_depth: usize) -> bool {
    let line_start = bytes[..index]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |offset| offset + 1);
    let line_prefix = String::from_utf8_lossy(&bytes[line_start..index]);
    if matches!(line_prefix.trim(), "" | "-") {
        return true;
    }
    if flow_depth == 0 {
        return false;
    }
    let flow_segment_start = bytes[line_start..index]
        .iter()
        .rposition(|byte| matches!(byte, b'{' | b'[' | b','))
        .map_or(line_start, |offset| line_start + offset + 1);
    let flow_prefix = String::from_utf8_lossy(&bytes[flow_segment_start..index]);
    flow_prefix.trim().is_empty()
}

fn yaml_ref_value_is_nonlocal(bytes: &[u8], start: usize) -> bool {
    let start = skip_space(bytes, start);
    if start >= bytes.len() || matches!(bytes[start], b'\n' | b'\r' | b'}' | b']' | b',') {
        return false;
    }
    let value = if matches!(bytes[start], b'\'' | b'"') {
        let (_, value) = quoted_scalar(bytes, start, bytes[start]);
        value
    } else {
        let end = bytes[start..]
            .iter()
            .enumerate()
            .position(|(offset, byte)| {
                matches!(byte, b'\n' | b'\r' | b'}' | b']' | b',')
                    || (*byte == b'#'
                        && offset > 0
                        && bytes[start + offset - 1].is_ascii_whitespace())
            })
            .map_or(bytes.len(), |offset| start + offset);
        String::from_utf8_lossy(&bytes[start..end])
            .trim()
            .to_owned()
    };
    !value.is_empty() && !value.starts_with('#')
}

fn quoted_scalar(bytes: &[u8], start: usize, quote: u8) -> (usize, String) {
    let mut index = start + 1;
    let content_start = index;
    while index < bytes.len() {
        if bytes[index] == quote {
            if quote == b'\'' && bytes.get(index + 1) == Some(&b'\'') {
                index += 2;
                continue;
            }
            return (
                index + 1,
                String::from_utf8_lossy(&bytes[content_start..index]).into_owned(),
            );
        }
        if quote == b'"' && bytes[index] == b'\\' && index + 1 < bytes.len() {
            index += 2;
        } else {
            index += 1;
        }
    }
    (
        bytes.len(),
        String::from_utf8_lossy(&bytes[content_start..]).into_owned(),
    )
}

fn skip_space(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    index
}

pub fn validate_restricted(
    restricted: bool,
    verify_path: Option<&str>,
    seed_path: Option<&str>,
    restore_path: Option<&str>,
) -> Result<(), ToolError> {
    if restricted {
        for (field, value) in [
            ("verify_path", verify_path),
            ("seed_path", seed_path),
            ("restore_path", restore_path),
        ] {
            if value.is_some() {
                return Err(argument_error(field, "is not allowed in restricted mode"));
            }
        }
    }
    Ok(())
}

fn argument_error(field: &str, detail: &str) -> ToolError {
    ToolError {
        code: "E_ARGUMENT_INVALID",
        message: format!("{field}: {detail}"),
        hint: "Check the named field and try again",
    }
}

fn path_error(path: &str, error: std::io::Error) -> ToolError {
    ToolError {
        code: "E_ARGUMENT_INVALID",
        message: format!("{path}: {error}"),
        hint: "Check the path and try again",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_INPUT_BYTES, Workdir, reject_nonlocal_refs, resolve_input, resolve_input_file,
        resolve_output_file, validate_restricted,
    };
    use std::fs;
    use std::path::Path;

    #[test]
    fn u04_path_traversal_symlink_and_flag_like_path_are_canonicalized() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("work");
        let outside = temp.path().join("outside.sql");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("-schema.sql"), "CREATE TABLE t (id INT);").unwrap();
        fs::write(&outside, "outside").unwrap();
        assert_eq!(
            "E_PATH_OUTSIDE_WORKDIR",
            resolve_input("../outside.sql", &root).unwrap_err().code
        );
        assert_eq!(
            "E_PATH_OUTSIDE_WORKDIR",
            resolve_input(outside.to_str().unwrap(), &root)
                .unwrap_err()
                .code
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&outside, root.join("escape.sql")).unwrap();
            assert_eq!(
                "E_PATH_OUTSIDE_WORKDIR",
                resolve_input("escape.sql", &root).unwrap_err().code
            );
        }
        assert_eq!(
            root.join("-schema.sql").canonicalize().unwrap(),
            resolve_input_file("-schema.sql", &root).unwrap()
        );
    }

    #[test]
    fn output_paths_are_confined_and_allow_a_missing_leaf() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("work");
        let outside = temp.path().join("outside.json");
        fs::create_dir(&root).unwrap();
        fs::write(&outside, "outside").unwrap();
        assert_eq!(
            root.canonicalize().unwrap().join("proof.json"),
            resolve_output_file("proof.json", &root).unwrap()
        );
        assert_eq!(
            "E_PATH_OUTSIDE_WORKDIR",
            resolve_output_file("../outside.json", &root)
                .unwrap_err()
                .code
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&outside, root.join("escape.json")).unwrap();
            assert_eq!(
                "E_PATH_OUTSIDE_WORKDIR",
                resolve_output_file("escape.json", &root).unwrap_err().code
            );
        }
    }

    #[test]
    fn u05_workdir_safety_depends_on_explicit_flag() {
        let home = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .unwrap();
        let root = Path::new("/");
        let unsafe_root = Workdir::resolve(None, Some(root), root, &home).unwrap();
        assert!(unsafe_root.unsafe_default);
        let explicit_root = Workdir::resolve(Some(root), None, root, &home).unwrap();
        assert!(!explicit_root.unsafe_default);
        let default_home = Workdir::resolve(None, Some(&home), &home, &home).unwrap();
        assert!(default_home.unsafe_default);
        let explicit_home = Workdir::resolve(Some(&home), None, &home, &home).unwrap();
        assert!(!explicit_home.unsafe_default);
    }

    #[test]
    fn u03_input_size_limit_accepts_exactly_five_mibibytes() {
        let temp = tempfile::tempdir().unwrap();
        let accepted = temp.path().join("accepted.sql");
        let rejected = temp.path().join("rejected.sql");
        fs::write(&accepted, vec![b'x'; MAX_INPUT_BYTES as usize]).unwrap();
        fs::write(&rejected, vec![b'x'; MAX_INPUT_BYTES as usize + 1]).unwrap();
        assert!(resolve_input_file("accepted.sql", temp.path()).is_ok());
        assert_eq!(
            "E_ARGUMENT_INVALID",
            resolve_input_file("rejected.sql", temp.path())
                .unwrap_err()
                .code
        );
    }

    #[test]
    fn u15_restricted_mode_rejects_only_optional_assets() {
        assert!(validate_restricted(true, None, None, None).is_ok());
        assert_eq!(
            "verify_path",
            validate_restricted(true, Some("verify.json"), None, None)
                .unwrap_err()
                .message
                .split(':')
                .next()
                .unwrap()
        );
        assert_eq!(
            "seed_path",
            validate_restricted(true, None, Some("seed.jsonl"), None)
                .unwrap_err()
                .message
                .split(':')
                .next()
                .unwrap()
        );
        assert_eq!(
            "restore_path",
            validate_restricted(true, None, None, Some("snapshot.twin"))
                .unwrap_err()
                .message
                .split(':')
                .next()
                .unwrap()
        );
    }

    #[test]
    fn rest_spec_preflight_rejects_nonlocal_refs_and_allows_local_refs() {
        let temp = tempfile::tempdir().unwrap();
        let remote_yaml = temp.path().join("remote.yaml");
        fs::write(
            &remote_yaml,
            "openapi: 3.0.3\npaths:\n  /x:\n    get:\n      responses:\n        '200':\n          content:\n            application/json:\n              schema:\n                $ref: 'https://example.invalid/SECRET#/Schema'\n",
        )
        .unwrap();
        let error = reject_nonlocal_refs(&remote_yaml).unwrap_err();
        assert_eq!("E_ARGUMENT_INVALID", error.code);
        assert!(error.message.contains("spec_path"));
        assert!(!error.message.contains("SECRET"));

        let local_yaml = temp.path().join("local.yaml");
        fs::write(
            &local_yaml,
            "openapi: 3.0.3\ninfo:\n  description: '$ref: https://example.invalid/not-a-key'\npaths:\n  /x:\n    get:\n      responses:\n        '200':\n          content:\n            application/json:\n              schema:\n                $ref: '#/components/schemas/Item'\n",
        )
        .unwrap();
        assert!(reject_nonlocal_refs(&local_yaml).is_ok());

        let remote_json = temp.path().join("remote.json");
        fs::write(
            &remote_json,
            r#"{"openapi":"3.0.3","paths":{"/x":{"get":{"responses":{"200":{"content":{"application/json":{"schema":{"$ref":"https://example.invalid/schema.json"}}}}}}}}}"#,
        )
        .unwrap();
        assert_eq!(
            "E_ARGUMENT_INVALID",
            reject_nonlocal_refs(&remote_json).unwrap_err().code
        );
    }
}
