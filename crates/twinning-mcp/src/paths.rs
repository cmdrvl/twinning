//! Workdir and caller-path validation used before any child process is spawned.

use std::fs;
use std::path::{Path, PathBuf};

use crate::errors::ToolError;

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
    use super::{MAX_INPUT_BYTES, Workdir, resolve_input, resolve_input_file, validate_restricted};
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
}
