//! The single trusted builder for arguments passed to the `twinning` process.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::registry::TwinKind;

#[derive(Debug, Clone)]
pub struct TwinArgv {
    pub kind: TwinKind,
    pub input: PathBuf,
    pub verify: Option<PathBuf>,
    pub seed: Option<PathBuf>,
    pub restore: Option<PathBuf>,
    pub snapshot: Option<PathBuf>,
    pub declaration: Option<PathBuf>,
    pub auth_mode: Option<String>,
    pub server_variables: Vec<(String, String)>,
    pub report: PathBuf,
    pub trace: PathBuf,
    pub port: u16,
    pub capture_refused_statements: bool,
}

impl TwinArgv {
    /// Construct the complete and fixed argument list (excluding the executable).
    pub fn build(&self) -> Vec<OsString> {
        match self.kind {
            TwinKind::Postgres => {
                let mut args = vec![
                    OsString::from("postgres"),
                    OsString::from("--schema"),
                    self.input.as_os_str().to_owned(),
                ];
                push_optional(&mut args, "--verify", self.verify.as_deref());
                push_optional(&mut args, "--seed", self.seed.as_deref());
                push_optional(&mut args, "--restore", self.restore.as_deref());
                push_optional(&mut args, "--snapshot", self.snapshot.as_deref());
                push_optional(&mut args, "--declaration", self.declaration.as_deref());
                if self.capture_refused_statements {
                    args.push(OsString::from("--capture-refused-statements"));
                }
                args.extend([
                    OsString::from("--host"),
                    OsString::from("127.0.0.1"),
                    OsString::from("--port"),
                    OsString::from(self.port.to_string()),
                    OsString::from("--serve"),
                    OsString::from("--report"),
                    self.report.as_os_str().to_owned(),
                    OsString::from("--query-trace"),
                    self.trace.as_os_str().to_owned(),
                    OsString::from("--json"),
                ]);
                args
            }
            TwinKind::Rest => {
                let mut args = vec![
                    OsString::from("rest"),
                    OsString::from("--spec"),
                    self.input.as_os_str().to_owned(),
                    OsString::from("--host"),
                    OsString::from("127.0.0.1"),
                    OsString::from("--port"),
                    OsString::from(self.port.to_string()),
                    OsString::from("--serve"),
                    OsString::from("--report"),
                    self.report.as_os_str().to_owned(),
                    OsString::from("--auth-mode"),
                    OsString::from(self.auth_mode.as_deref().unwrap_or("shape")),
                ];
                for (name, value) in &self.server_variables {
                    args.push(OsString::from("--server-variable"));
                    args.push(OsString::from(format!("{name}={value}")));
                }
                args.push(OsString::from("--json"));
                args
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SeedContractArgv {
    pub schema: PathBuf,
    pub output: PathBuf,
}

impl SeedContractArgv {
    pub fn build(&self) -> Vec<OsString> {
        vec![
            OsString::from("postgres"),
            OsString::from("--schema"),
            self.schema.as_os_str().to_owned(),
            OsString::from("--export-seed-contract"),
            self.output.as_os_str().to_owned(),
            OsString::from("--json"),
        ]
    }
}

fn push_optional(args: &mut Vec<OsString>, flag: &str, path: Option<&Path>) {
    if let Some(path) = path {
        args.push(OsString::from(flag));
        args.push(path.as_os_str().to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::{SeedContractArgv, TwinArgv};
    use crate::registry::TwinKind;
    use std::path::PathBuf;

    #[test]
    fn u06_argv_builder_never_accepts_flags_or_hosts_from_callers() {
        let argv = TwinArgv {
            kind: TwinKind::Postgres,
            input: PathBuf::from("/work/--run;--host=0.0.0.0"),
            verify: Some(PathBuf::from("/work/--materialize-source-url")),
            seed: Some(PathBuf::from("/work/seed with spaces")),
            restore: Some(PathBuf::from("/work/restore")),
            snapshot: Some(PathBuf::from("/work/output snapshot.twin")),
            declaration: Some(PathBuf::from("/work/declaration.json")),
            auth_mode: None,
            server_variables: Vec::new(),
            report: PathBuf::from("/state/report.json"),
            trace: PathBuf::from("/state/trace.json"),
            port: 54321,
            capture_refused_statements: false,
        };
        let args = argv.build();
        let strings: Vec<_> = args.iter().map(|arg| arg.to_string_lossy()).collect();
        assert_eq!(strings[0], "postgres");
        assert!(strings.contains(&"--host".into()));
        assert!(strings.contains(&"127.0.0.1".into()));
        assert!(strings.contains(&"--schema".into()));
        assert!(strings.contains(&"/work/--run;--host=0.0.0.0".into()));
        assert!(strings.contains(&"/work/--materialize-source-url".into()));
        assert!(strings.contains(&"/work/output snapshot.twin".into()));
        assert!(strings.contains(&"/work/declaration.json".into()));
        assert!(!strings.iter().any(|arg| arg.as_ref() == "--run"));
        assert!(
            !strings
                .iter()
                .any(|arg| arg.as_ref() == "--materialize-source-url")
        );
        assert!(!strings.iter().any(|arg| arg.as_ref() == "0.0.0.0"));
        assert!(
            !strings
                .iter()
                .any(|arg| arg.as_ref() == "--capture-refused-statements")
        );
    }

    #[test]
    fn postgres_argv_passes_refused_statement_capture_only_when_enabled() {
        let mut argv = TwinArgv {
            kind: TwinKind::Postgres,
            input: PathBuf::from("/work/schema.sql"),
            verify: None,
            seed: None,
            restore: None,
            snapshot: None,
            declaration: None,
            auth_mode: None,
            server_variables: Vec::new(),
            report: PathBuf::from("/state/report.json"),
            trace: PathBuf::from("/state/trace.json"),
            port: 54321,
            capture_refused_statements: false,
        };
        assert!(
            !argv
                .build()
                .iter()
                .any(|arg| arg == "--capture-refused-statements")
        );
        argv.capture_refused_statements = true;
        assert!(
            argv.build()
                .iter()
                .any(|arg| arg == "--capture-refused-statements")
        );
    }

    #[test]
    fn rest_argv_uses_only_fixed_listener_flags_and_repeated_server_variables() {
        let args = TwinArgv {
            kind: TwinKind::Rest,
            input: PathBuf::from("/work/openapi.yaml"),
            verify: None,
            seed: None,
            restore: None,
            snapshot: None,
            declaration: None,
            auth_mode: Some("bypass".into()),
            server_variables: vec![
                ("basePath".into(), "v3".into()),
                ("region".into(), "us".into()),
            ],
            report: PathBuf::from("/state/report.json"),
            trace: PathBuf::from("/state/trace.json"),
            port: 54321,
            capture_refused_statements: false,
        }
        .build();
        let strings = args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!("rest", strings[0]);
        assert!(
            strings
                .windows(2)
                .any(|pair| pair == ["--spec", "/work/openapi.yaml"])
        );
        assert!(
            strings
                .windows(2)
                .any(|pair| pair == ["--host", "127.0.0.1"])
        );
        assert!(strings.windows(2).any(|pair| pair == ["--port", "54321"]));
        assert!(
            strings
                .windows(2)
                .any(|pair| pair == ["--auth-mode", "bypass"]) // ubs:ignore - test assertion on argv, not a secret
        );
        assert_eq!(
            2,
            strings
                .iter()
                .filter(|arg| *arg == "--server-variable")
                .count()
        );
        assert!(
            strings
                .windows(2)
                .any(|pair| pair == ["--server-variable", "basePath=v3"])
        );
        assert!(
            strings
                .windows(2)
                .any(|pair| pair == ["--server-variable", "region=us"])
        );
        for forbidden in [
            "--run",
            "--chaos",
            "--canary",
            "--strict",
            "--routing",
            "--base-prefix",
            "--query-trace",
        ] {
            assert!(
                !strings.iter().any(|arg| arg == forbidden),
                "unexpected flag {forbidden}"
            );
        }
    }

    #[test]
    fn seed_contract_argv_uses_only_the_fixed_export_command() {
        let args = SeedContractArgv {
            schema: PathBuf::from("/work/schema.sql"),
            output: PathBuf::from("/state/contract/seed-contract.jsonl"),
        }
        .build();
        let strings = args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            strings.iter().map(String::as_str).collect::<Vec<_>>(),
            [
                "postgres",
                "--schema",
                "/work/schema.sql",
                "--export-seed-contract",
                "/state/contract/seed-contract.jsonl",
                "--json"
            ]
        );
        assert!(
            !strings
                .iter()
                .any(|arg| arg == "--serve" || arg == "--host")
        );
    }
}
