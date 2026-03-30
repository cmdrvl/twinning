use std::{io, net::TcpListener, path::PathBuf, process::Command};

use tempfile::{TempDir, tempdir};

pub(crate) const DIFFERENTIAL_POSTGRES_URL_ENV: &str = "TWINNING_DIFF_POSTGRES_URL";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CorpusKind {
    Read,
    Write,
}

impl CorpusKind {
    pub(crate) fn id(self) -> &'static str {
        match self {
            Self::Read => "read_corpus",
            Self::Write => "write_corpus",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DifferentialFixture {
    kind: CorpusKind,
    fixture_dir: PathBuf,
}

impl DifferentialFixture {
    pub(crate) fn load(kind: CorpusKind) -> io::Result<Self> {
        let fixture_dir = repo_root()
            .join("tests")
            .join("fixtures")
            .join("differential")
            .join(kind.id());

        if !fixture_dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "missing differential fixture dir `{}`",
                    fixture_dir.display()
                ),
            ));
        }

        Ok(Self { kind, fixture_dir })
    }

    pub(crate) fn id(&self) -> &'static str {
        self.kind.id()
    }

    pub(crate) fn fixture_dir(&self) -> &PathBuf {
        &self.fixture_dir
    }

    pub(crate) fn schema_path(&self) -> PathBuf {
        self.fixture_dir.join("schema.sql")
    }

    pub(crate) fn corpus_path(&self) -> PathBuf {
        self.fixture_dir.join("corpus.json")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostgresTarget {
    connection_url: String,
}

impl PostgresTarget {
    pub(crate) fn from_env() -> Option<Self> {
        std::env::var(DIFFERENTIAL_POSTGRES_URL_ENV)
            .ok()
            .map(|url| url.trim().to_owned())
            .filter(|url| !url.is_empty())
            .map(|connection_url| Self { connection_url })
    }
}

#[derive(Debug)]
pub(crate) struct DifferentialRunner {
    fixture: DifferentialFixture,
    scratch_dir: TempDir,
    twin_host: &'static str,
    twin_port: u16,
    postgres_target: Option<PostgresTarget>,
}

impl DifferentialRunner {
    pub(crate) fn prepare(kind: CorpusKind) -> io::Result<Self> {
        Ok(Self {
            fixture: DifferentialFixture::load(kind)?,
            scratch_dir: tempdir()?,
            twin_host: "127.0.0.1",
            twin_port: allocate_local_port()?,
            postgres_target: PostgresTarget::from_env(),
        })
    }

    pub(crate) fn fixture(&self) -> &DifferentialFixture {
        &self.fixture
    }

    pub(crate) fn scratch_dir(&self) -> &TempDir {
        &self.scratch_dir
    }

    pub(crate) fn twin_host(&self) -> &str {
        self.twin_host
    }

    pub(crate) fn twin_port(&self) -> u16 {
        self.twin_port
    }

    pub(crate) fn postgres_target(&self) -> Option<&PostgresTarget> {
        self.postgres_target.as_ref()
    }

    pub(crate) fn twinning_command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_twinning"));
        command
            .arg("postgres")
            .arg("--host")
            .arg(self.twin_host)
            .arg("--port")
            .arg(self.twin_port.to_string())
            .arg("--schema")
            .arg(self.fixture.schema_path());
        command
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn allocate_local_port() -> io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

#[cfg(test)]
mod tests {
    use super::{CorpusKind, DifferentialRunner};

    #[test]
    fn prepares_runner_with_fixture_scratch_dir_and_port() {
        let runner = DifferentialRunner::prepare(CorpusKind::Read).expect("prepare runner");

        assert_eq!(runner.fixture().id(), "read_corpus");
        assert!(runner.fixture().fixture_dir().exists());
        assert_eq!(
            runner.fixture().schema_path(),
            runner.fixture().fixture_dir().join("schema.sql")
        );
        assert_eq!(
            runner.fixture().corpus_path(),
            runner.fixture().fixture_dir().join("corpus.json")
        );
        assert!(runner.scratch_dir().path().exists());
        assert_eq!(runner.twin_host(), "127.0.0.1");
        assert!(runner.twin_port() > 0);
    }

    #[test]
    fn builds_standard_twinning_command_for_differential_runs() {
        let runner = DifferentialRunner::prepare(CorpusKind::Write).expect("prepare runner");
        let command = runner.twinning_command();
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert!(
            command.get_program().to_string_lossy().contains("twinning"),
            "expected twinning binary, got {}",
            command.get_program().to_string_lossy()
        );
        assert_eq!(
            args,
            vec![
                String::from("postgres"),
                String::from("--host"),
                String::from("127.0.0.1"),
                String::from("--port"),
                runner.twin_port().to_string(),
                String::from("--schema"),
                runner.fixture().schema_path().display().to_string(),
            ]
        );
        assert!(runner.postgres_target().is_none());
    }
}
