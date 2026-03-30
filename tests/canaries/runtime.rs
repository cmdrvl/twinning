use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use tempfile::TempDir;

const LOCALHOST: &str = "127.0.0.1";

#[derive(Debug)]
pub(crate) struct TwinRuntime {
    workspace: TempDir,
    port: u16,
    child: Option<Child>,
}

#[derive(Debug)]
pub(crate) enum TwinRuntimeError {
    Io(io::Error),
    ExitedBeforeReady {
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    TimedOut {
        port: u16,
        timeout: Duration,
    },
}

impl From<io::Error> for TwinRuntimeError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl std::fmt::Display for TwinRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::ExitedBeforeReady {
                exit_code,
                stdout,
                stderr,
            } => write!(
                f,
                "twinning exited before becoming ready: code={exit_code:?}, stdout={stdout}, stderr={stderr}"
            ),
            Self::TimedOut { port, timeout } => write!(
                f,
                "timed out waiting {:?} for twinning to accept connections on {}:{}",
                timeout, LOCALHOST, port
            ),
        }
    }
}

impl TwinRuntime {
    pub(crate) fn launch(schema_path: &Path, verify_path: Option<&Path>) -> io::Result<Self> {
        let workspace = tempfile::tempdir()?;
        let port = reserve_local_port()?;

        let mut command = Command::new(twinning_binary());
        command
            .arg("postgres")
            .arg("--schema")
            .arg(schema_path)
            .arg("--host")
            .arg(LOCALHOST)
            .arg("--port")
            .arg(port.to_string())
            .current_dir(workspace.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(verify_path) = verify_path {
            command.arg("--verify").arg(verify_path);
        }

        let child = command.spawn()?;

        Ok(Self {
            workspace,
            port,
            child: Some(child),
        })
    }

    pub(crate) fn workspace(&self) -> &Path {
        self.workspace.path()
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn wait_until_ready(&mut self, timeout: Duration) -> Result<(), TwinRuntimeError> {
        let deadline = Instant::now() + timeout;
        let address = SocketAddr::from(([127, 0, 0, 1], self.port));

        loop {
            if TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_ok() {
                return Ok(());
            }

            let Some(child) = self.child.as_mut() else {
                return Err(TwinRuntimeError::TimedOut {
                    port: self.port,
                    timeout,
                });
            };

            if child.try_wait()?.is_some() {
                let output = self
                    .child
                    .take()
                    .expect("child should exist when collecting process output")
                    .wait_with_output()?;
                return Err(TwinRuntimeError::ExitedBeforeReady {
                    exit_code: output.status.code(),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                });
            }

            if Instant::now() >= deadline {
                return Err(TwinRuntimeError::TimedOut {
                    port: self.port,
                    timeout,
                });
            }

            thread::sleep(Duration::from_millis(25));
        }
    }

    pub(crate) fn terminate(&mut self) -> io::Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };

        if child.try_wait()?.is_none() {
            child.kill()?;
            let _ = child.wait();
        }

        Ok(())
    }
}

impl Drop for TwinRuntime {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

fn reserve_local_port() -> io::Result<u16> {
    let listener = TcpListener::bind((LOCALHOST, 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn twinning_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

#[cfg(test)]
mod tests {
    use super::{TwinRuntime, TwinRuntimeError, reserve_local_port};
    use std::{fs, net::TcpListener, path::Path, time::Duration};

    fn write_schema(dir: &Path) -> std::path::PathBuf {
        let schema_path = dir.join("schema.sql");
        fs::write(
            &schema_path,
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT NOT NULL
            );
        "#,
        )
        .expect("write schema");
        schema_path
    }

    #[test]
    fn reserve_local_port_returns_bindable_port() {
        let port = reserve_local_port().expect("reserve port");
        assert!(port > 0);

        let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind reserved port");
        drop(listener);
    }

    #[test]
    fn launch_sets_up_workspace_and_reports_exit_before_ready() {
        let schema_dir = tempfile::tempdir().expect("schema tempdir");
        let schema_path = write_schema(schema_dir.path());

        let mut runtime = TwinRuntime::launch(&schema_path, None).expect("launch runtime");

        assert!(runtime.workspace().exists());
        assert!(runtime.port() > 0);

        let error = runtime
            .wait_until_ready(Duration::from_secs(2))
            .expect_err("Phase-0 bootstrap should exit before opening a live pgwire port");

        match error {
            TwinRuntimeError::ExitedBeforeReady {
                exit_code,
                stdout,
                stderr,
            } => {
                assert_eq!(exit_code, Some(0));
                assert!(stderr.is_empty(), "unexpected stderr: {stderr}");
                assert!(
                    stdout.contains("twinning postgres bootstrap ready"),
                    "bootstrap output should be preserved when readiness fails: {stdout}"
                );
            }
            other => panic!("expected early process exit, got {other:?}"),
        }

        runtime.terminate().expect("terminate after early exit");
    }
}
