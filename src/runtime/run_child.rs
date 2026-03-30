use std::{
    io,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::process::{CommandExt, ExitStatusExt};

const POLL_INTERVAL: Duration = Duration::from_millis(10);
const TERMINATION_GRACE: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunChildOutcome {
    pub command: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub timed_out: bool,
}

#[derive(Debug)]
pub struct RunChild {
    command: String,
    child: Child,
}

impl RunChild {
    pub fn launch(command: impl Into<String>) -> io::Result<Self> {
        let command = command.into();
        let mut child_command = shell_command(&command);
        child_command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        #[cfg(unix)]
        child_command.process_group(0);

        let child = child_command.spawn()?;
        Ok(Self { command, child })
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn wait_with_timeout(mut self, timeout: Duration) -> io::Result<RunChildOutcome> {
        let deadline = Instant::now() + timeout;

        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(outcome_from_status(self.command, status, false));
            }

            if Instant::now() >= deadline {
                return self.terminate_for_timeout();
            }

            thread::sleep(POLL_INTERVAL);
        }
    }

    fn terminate_for_timeout(mut self) -> io::Result<RunChildOutcome> {
        if let Some(status) = self.child.try_wait()? {
            return Ok(outcome_from_status(self.command, status, false));
        }

        if let Err(error) = terminate_process_tree(&mut self.child) {
            if let Some(status) = self.child.try_wait()? {
                return Ok(outcome_from_status(self.command, status, false));
            }
            return Err(error);
        }

        let deadline = Instant::now() + TERMINATION_GRACE;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(outcome_from_status(self.command, status, true));
            }

            if Instant::now() >= deadline {
                if let Err(error) = force_kill_process_tree(&mut self.child) {
                    if let Some(status) = self.child.try_wait()? {
                        return Ok(outcome_from_status(self.command, status, true));
                    }
                    return Err(error);
                }
                let status = self.child.wait()?;
                return Ok(outcome_from_status(self.command, status, true));
            }

            thread::sleep(POLL_INTERVAL);
        }
    }
}

pub fn orchestrate(command: impl Into<String>, timeout: Duration) -> io::Result<RunChildOutcome> {
    RunChild::launch(command)?.wait_with_timeout(timeout)
}

fn outcome_from_status(command: String, status: ExitStatus, timed_out: bool) -> RunChildOutcome {
    #[cfg(unix)]
    let signal = status.signal();
    #[cfg(not(unix))]
    let signal = None;

    RunChildOutcome {
        command,
        exit_code: status.code(),
        signal,
        timed_out,
    }
}

fn shell_command(command: &str) -> Command {
    #[cfg(unix)]
    {
        let mut child_command = Command::new("sh");
        child_command.arg("-c").arg(command);
        child_command
    }

    #[cfg(windows)]
    {
        let mut child_command = Command::new("cmd");
        child_command.arg("/C").arg(command);
        child_command
    }
}

#[cfg(unix)]
fn terminate_process_tree(child: &mut Child) -> io::Result<()> {
    signal_process_group(child.id(), "TERM")
}

#[cfg(not(unix))]
fn terminate_process_tree(child: &mut Child) -> io::Result<()> {
    child.kill()
}

#[cfg(unix)]
fn force_kill_process_tree(child: &mut Child) -> io::Result<()> {
    signal_process_group(child.id(), "KILL")
}

#[cfg(not(unix))]
fn force_kill_process_tree(child: &mut Child) -> io::Result<()> {
    child.kill()
}

#[cfg(unix)]
fn signal_process_group(pid: u32, signal: &str) -> io::Result<()> {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(format!("-{pid}"))
        .status()?;

    if status.success() {
        return Ok(());
    }

    Err(io::Error::other(format!(
        "kill -{signal} -{pid} exited with {status}"
    )))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{RunChild, orchestrate};

    #[test]
    fn normal_exit_is_reported_without_timeout() {
        let outcome = orchestrate("exit 7", Duration::from_secs(1)).expect("orchestrate child");

        assert_eq!(outcome.command, "exit 7");
        assert_eq!(outcome.exit_code, Some(7));
        assert_eq!(outcome.signal, None);
        assert!(!outcome.timed_out);
    }

    #[cfg(unix)]
    #[test]
    fn signal_exit_is_reported_without_timeout() {
        let child = RunChild::launch("sleep 30").expect("launch child");
        let pid = child.pid();

        let status = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(format!("-{pid}"))
            .status()
            .expect("send term");
        assert!(status.success(), "term failed with {status}");

        let outcome = child
            .wait_with_timeout(Duration::from_secs(1))
            .expect("wait for signal exit");

        assert_eq!(outcome.command, "sleep 30");
        assert_eq!(outcome.exit_code, None);
        assert_eq!(outcome.signal, Some(15));
        assert!(!outcome.timed_out);
    }

    #[cfg(unix)]
    #[test]
    fn timeout_marks_outcome_and_forces_termination_when_needed() {
        let outcome = orchestrate("trap '' TERM; sleep 30", Duration::from_millis(50))
            .expect("orchestrate timeout");

        assert_eq!(outcome.command, "trap '' TERM; sleep 30");
        assert_eq!(outcome.exit_code, None);
        assert_eq!(outcome.signal, Some(9));
        assert!(outcome.timed_out);
    }
}
