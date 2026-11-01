//! The single trusted builder for arguments passed to the `twinning` process.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct TwinArgv {
    pub schema: PathBuf,
    pub verify: Option<PathBuf>,
    pub seed: Option<PathBuf>,
    pub restore: Option<PathBuf>,
    pub report: PathBuf,
    pub trace: PathBuf,
    pub port: u16,
}

impl TwinArgv {
    /// Construct the complete and fixed argument list (excluding the executable).
    pub fn build(&self) -> Vec<OsString> {
        let mut args = vec![
            OsString::from("postgres"),
            OsString::from("--schema"),
            self.schema.as_os_str().to_owned(),
        ];
        push_optional(&mut args, "--verify", self.verify.as_deref());
        push_optional(&mut args, "--seed", self.seed.as_deref());
        push_optional(&mut args, "--restore", self.restore.as_deref());
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
    use std::path::PathBuf;

    #[test]
    fn u06_argv_builder_never_accepts_flags_or_hosts_from_callers() {
        let argv = TwinArgv {
            schema: PathBuf::from("/work/--run;--host=0.0.0.0"),
            verify: Some(PathBuf::from("/work/--materialize-source-url")),
            seed: Some(PathBuf::from("/work/seed with spaces")),
            restore: Some(PathBuf::from("/work/restore")),
            report: PathBuf::from("/state/report.json"),
            trace: PathBuf::from("/state/trace.json"),
            port: 54321,
        };
        let args = argv.build();
        let strings: Vec<_> = args.iter().map(|arg| arg.to_string_lossy()).collect();
        assert_eq!(strings[0], "postgres");
        assert!(strings.contains(&"--host".into()));
        assert!(strings.contains(&"127.0.0.1".into()));
        assert!(strings.contains(&"--schema".into()));
        assert!(strings.contains(&"/work/--run;--host=0.0.0.0".into()));
        assert!(strings.contains(&"/work/--materialize-source-url".into()));
        assert!(!strings.iter().any(|arg| arg.as_ref() == "--run"));
        assert!(
            !strings
                .iter()
                .any(|arg| arg.as_ref() == "--materialize-source-url")
        );
        assert!(!strings.iter().any(|arg| arg.as_ref() == "0.0.0.0"));
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
