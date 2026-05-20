use crate::{
    cli::{Cli, Engine, TwinArgs},
    refusal,
    refusal::RefusalResult,
};

pub use twinning_kernel::TwinConfig;
#[cfg(feature = "rest")]
pub use twinning_rest::config::{RestConfig, RestConfigParts};
#[cfg(feature = "mcp")]
pub use twinning_rest::mcp::listener::{McpCatalogInput, McpConfig};
#[cfg(feature = "snowflake")]
pub use twinning_snowflake::config::{SnowflakeConfig, SnowflakeConfigParts};

pub fn twin_config_from_cli(cli: &Cli) -> RefusalResult<TwinConfig> {
    let Some(command) = &cli.command else {
        return Err(Box::new(refusal::missing_command()));
    };
    let engine = command
        .engine()
        .ok_or_else(|| Box::new(refusal::missing_command()))?;
    let args = command
        .twin_args()
        .ok_or_else(|| Box::new(refusal::missing_command()))?;

    twin_config_from_engine_args(engine, args, cli.json)
}

pub fn twin_config_from_engine_args(
    engine: Engine,
    args: &TwinArgs,
    json: bool,
) -> RefusalResult<TwinConfig> {
    if args.schema.is_none() && args.restore.is_none() {
        return Err(Box::new(refusal::missing_bootstrap_source(engine)));
    }

    if args.schema.is_some() && args.restore.is_some() {
        return Err(Box::new(refusal::ambiguous_bootstrap_source()));
    }

    if args.restore.is_some() && args.materialize_source_url.is_some() {
        return Err(Box::new(refusal::materialization_requires_schema()));
    }

    if args.restore.is_some() && args.export_seed_contract.is_some() {
        return Err(Box::new(refusal::seed_requires_schema(
            "--export-seed-contract",
        )));
    }

    if args.restore.is_some() && args.seed.is_some() {
        return Err(Box::new(refusal::seed_requires_schema("--seed")));
    }

    if args.seed.is_some() && args.materialize_source_url.is_some() {
        return Err(Box::new(refusal::seed_materialization_composition()));
    }

    if args.serve && args.run.is_some() {
        return Err(Box::new(refusal::ambiguous_live_mode(engine)));
    }

    Ok(TwinConfig {
        engine,
        host: args.host.clone(),
        port: args.port.unwrap_or_else(|| engine.default_port()),
        schema_path: args.schema.clone(),
        verify_path: args.verify.clone(),
        declaration_path: args.declaration.clone(),
        run_command: args.run.clone(),
        serve: args.serve,
        report_path: args.report.clone(),
        snapshot_path: args.snapshot.clone(),
        query_trace_path: args.query_trace.clone(),
        restore_path: args.restore.clone(),
        export_seed_contract_path: args.export_seed_contract.clone(),
        seed_path: args.seed.clone(),
        materialize_source_url: args.materialize_source_url.clone(),
        json,
    })
}

#[cfg(feature = "rest")]
pub fn rest_config_from_cli(cli: &Cli) -> RefusalResult<RestConfig> {
    let Some(command) = &cli.command else {
        return Err(Box::new(refusal::missing_command()));
    };
    let args = command
        .rest_args()
        .ok_or_else(|| Box::new(refusal::missing_command()))?;

    rest_config_from_args(args, cli.json)
}

#[cfg(feature = "rest")]
pub fn rest_config_from_args(args: &crate::cli::RestArgs, json: bool) -> RefusalResult<RestConfig> {
    let spec_path = args
        .spec
        .clone()
        .ok_or_else(|| Box::new(refusal::missing_rest_spec()))?;

    if args.serve && args.run.is_some() {
        return Err(Box::new(refusal::ambiguous_rest_live_mode()));
    }

    Ok(RestConfig::from_parts(
        RestConfigParts {
            spec_path,
            host: args.host.clone(),
            port: args.port,
            run_command: args.run.clone(),
            serve: args.serve,
            report_path: args.report.clone(),
            canary_path: args.canary.clone(),
            strict: args.strict,
            routing: args.routing,
            base_prefix: args.base_prefix.clone(),
            auth_mode: args.auth_mode,
            chaos: args.chaos,
        },
        json,
    ))
}

#[cfg(feature = "mcp")]
pub fn mcp_config_from_cli(cli: &Cli) -> RefusalResult<McpConfig> {
    let Some(command) = &cli.command else {
        return Err(Box::new(refusal::missing_command()));
    };
    let args = command
        .mcp_args()
        .ok_or_else(|| Box::new(refusal::missing_command()))?;

    mcp_config_from_args(args, cli.json)
}

#[cfg(feature = "mcp")]
pub fn mcp_config_from_args(args: &crate::cli::McpArgs, json: bool) -> RefusalResult<McpConfig> {
    let source = match (&args.server, &args.manifest) {
        (Some(command), None) => McpCatalogInput::LiveServer {
            command: command.clone(),
        },
        (None, Some(path)) => McpCatalogInput::Manifest { path: path.clone() },
        (None, None) => return Err(Box::new(refusal::missing_mcp_catalog_source())),
        (Some(_), Some(_)) => return Err(Box::new(refusal::ambiguous_mcp_catalog_source())),
    };

    if args.stdio && args.run.is_some() {
        return Err(Box::new(refusal::mcp_stdio_run_unsupported()));
    }

    Ok(McpConfig {
        source,
        host: args.host.clone(),
        port: args.port,
        auth_mode: args.auth_mode,
        stdio: args.stdio,
        run_command: args.run.clone(),
        report_path: args.report.clone(),
        json,
    })
}

#[cfg(feature = "snowflake")]
pub fn snowflake_config_from_cli(cli: &Cli) -> RefusalResult<SnowflakeConfig> {
    let Some(command) = &cli.command else {
        return Err(Box::new(refusal::missing_command()));
    };
    let args = command
        .snowflake_args()
        .ok_or_else(|| Box::new(refusal::missing_command()))?;

    snowflake_config_from_args(args, cli.json)
}

#[cfg(feature = "snowflake")]
pub fn snowflake_config_from_args(
    args: &crate::cli::SnowflakeArgs,
    json: bool,
) -> RefusalResult<SnowflakeConfig> {
    let schema_path = args
        .schema
        .clone()
        .ok_or_else(|| Box::new(refusal::missing_snowflake_schema()))?;

    if args.serve && args.run.is_some() {
        return Err(Box::new(refusal::ambiguous_snowflake_live_mode()));
    }

    Ok(SnowflakeConfig::from_parts(
        SnowflakeConfigParts {
            schema_path: Some(schema_path),
            host: args.host.clone(),
            port: args.port,
            run_command: args.run.clone(),
            serve: args.serve,
            report_path: args.report.clone(),
            materialize_source_url: args.materialize_source_url.clone(),
            max_rows_per_table: args.max_rows_per_table,
        },
        json,
    ))
}

#[cfg(all(test, feature = "rest"))]
mod tests {
    use clap::Parser;

    use crate::{
        cli::{Cli, Command, Engine, RestArgs, TwinArgs},
        protocol::rest::{
            auth::RestAuthMode,
            policy::{RoutingConfig, RoutingPolicy},
        },
    };

    #[cfg(feature = "mcp")]
    use super::mcp_config_from_args;
    use super::{rest_config_from_args, twin_config_from_engine_args};
    #[cfg(feature = "mcp")]
    use crate::cli::McpArgs;

    #[test]
    fn config_uses_engine_default_port() {
        let args = TwinArgs {
            schema: Some("schema.sql".into()),
            verify: None,
            declaration: None,
            host: "127.0.0.1".to_owned(),
            port: None,
            run: None,
            serve: false,
            report: None,
            snapshot: None,
            query_trace: None,
            restore: None,
            export_seed_contract: None,
            seed: None,
            materialize_source_url: None,
        };

        let config =
            twin_config_from_engine_args(Engine::Mysql, &args, false).expect("config should build");
        assert_eq!(config.port, 3306);
    }

    #[test]
    fn twin_config_refuses_seed_restore_and_materialization_composition() {
        let base = TwinArgs {
            schema: Some("schema.sql".into()),
            verify: None,
            declaration: None,
            host: "127.0.0.1".to_owned(),
            port: None,
            run: None,
            serve: false,
            report: None,
            snapshot: None,
            query_trace: None,
            restore: None,
            export_seed_contract: None,
            seed: None,
            materialize_source_url: None,
        };

        let restore_export = TwinArgs {
            schema: None,
            restore: Some("restore.twin".into()),
            export_seed_contract: Some("contract.jsonl".into()),
            ..base.clone()
        };
        let refusal = twin_config_from_engine_args(Engine::Postgres, &restore_export, true)
            .expect_err("restore/export seed contract should be refused");
        let rendered = refusal.render(true).expect("render refusal");
        assert!(rendered.contains("\"code\": \"E_SEED_BOOTSTRAP_SOURCE\""));
        assert!(rendered.contains("--export-seed-contract"));

        let restore_seed = TwinArgs {
            schema: None,
            restore: Some("restore.twin".into()),
            seed: Some("seed.jsonl".into()),
            ..base.clone()
        };
        let refusal = twin_config_from_engine_args(Engine::Postgres, &restore_seed, true)
            .expect_err("restore/seed should be refused");
        let rendered = refusal.render(true).expect("render refusal");
        assert!(rendered.contains("\"code\": \"E_SEED_BOOTSTRAP_SOURCE\""));
        assert!(rendered.contains("--seed"));

        let seed_materialization = TwinArgs {
            seed: Some("seed.jsonl".into()),
            materialize_source_url: Some("postgres://example".to_owned()),
            ..base
        };
        let refusal = twin_config_from_engine_args(Engine::Postgres, &seed_materialization, true)
            .expect_err("seed/materialization should be refused");
        let rendered = refusal.render(true).expect("render refusal");
        assert!(rendered.contains("\"code\": \"E_SEED_SOURCE_COMPOSITION\""));
    }

    #[test]
    fn rest_config_uses_http_defaults() {
        let args = RestArgs {
            spec: Some("api.yaml".into()),
            host: "127.0.0.1".to_owned(),
            port: 8080,
            run: None,
            serve: false,
            report: None,
            canary: None,
            strict: false,
            routing: None,
            base_prefix: None,
            auth_mode: None,
            chaos: None,
        };

        let config = rest_config_from_args(&args, true).expect("config should build");

        assert_eq!(config.spec_path, std::path::PathBuf::from("api.yaml"));
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert_eq!(config.run_command, None);
        assert!(config.serve);
        assert!(config.serve_defaulted);
        assert!(!config.strict);
        assert_eq!(config.routing, RoutingConfig::default());
        assert_eq!(config.auth_mode, None);
        assert_eq!(config.chaos, None);
        assert!(config.json);
    }

    #[test]
    fn rest_config_refuses_run_and_serve_together() {
        let args = RestArgs {
            spec: Some("api.yaml".into()),
            host: "127.0.0.1".to_owned(),
            port: 8080,
            run: Some("echo hi".to_owned()),
            serve: true,
            report: None,
            canary: None,
            strict: false,
            routing: None,
            base_prefix: None,
            auth_mode: None,
            chaos: None,
        };

        let refusal = rest_config_from_args(&args, true).expect_err("mode should be refused");
        let rendered = refusal.render(true).expect("refusal should render");

        assert!(rendered.contains("\"code\": \"E_AMBIGUOUS_LIVE_MODE\""));
        assert!(rendered.contains("\"protocol\": \"rest\""));
    }

    #[test]
    fn rest_config_requires_spec_path() {
        let args = RestArgs {
            spec: None,
            host: "127.0.0.1".to_owned(),
            port: 8080,
            run: None,
            serve: false,
            report: None,
            canary: None,
            strict: false,
            routing: None,
            base_prefix: None,
            auth_mode: None,
            chaos: None,
        };

        let refusal = rest_config_from_args(&args, true).expect_err("spec should be required");
        let rendered = refusal.render(true).expect("refusal should render");

        assert!(rendered.contains("\"code\": \"E_REST_SPEC_REQUIRED\""));
    }

    #[test]
    fn rest_config_parses_strict_future_hook_without_enforcing_it() {
        let args = RestArgs {
            spec: Some("api.yaml".into()),
            host: "127.0.0.1".to_owned(),
            port: 8080,
            run: None,
            serve: true,
            report: None,
            canary: None,
            strict: true,
            routing: None,
            base_prefix: None,
            auth_mode: None,
            chaos: None,
        };

        let config = rest_config_from_args(&args, true).expect("config should build");

        assert!(config.strict);
        assert!(!config.serve_defaulted);
    }

    #[test]
    fn routing_cli_override_is_carried_by_rest_config() {
        let args = RestArgs {
            spec: Some("api.yaml".into()),
            host: "127.0.0.1".to_owned(),
            port: 8080,
            run: None,
            serve: true,
            report: None,
            canary: None,
            strict: false,
            routing: Some(RoutingPolicy::FlatCrud),
            base_prefix: Some("/api".to_owned()),
            auth_mode: None,
            chaos: None,
        };

        let config = rest_config_from_args(&args, false).expect("config should build");

        assert_eq!(config.routing.policy, RoutingPolicy::FlatCrud);
        assert_eq!(config.routing.base_prefix.as_deref(), Some("/api"));
    }

    #[test]
    fn rest_auth_mode_cli_override_is_carried_by_rest_config() {
        let args = RestArgs {
            spec: Some("api.yaml".into()),
            host: "127.0.0.1".to_owned(),
            port: 8080,
            run: None,
            serve: true,
            report: None,
            canary: None,
            strict: false,
            routing: None,
            base_prefix: None,
            auth_mode: Some(RestAuthMode::Bypass),
            chaos: None,
        };

        let config = rest_config_from_args(&args, false).expect("config should build");

        assert_eq!(config.auth_mode, Some(RestAuthMode::Bypass));
    }

    #[test]
    fn routing_clap_parse_accepts_schema_first() {
        let cli = Cli::try_parse_from([
            "twinning",
            "rest",
            "--spec",
            "api.yaml",
            "--routing",
            "schema-first",
            "--base-prefix",
            "/api",
            "--auth-mode",
            "bypass",
            "--chaos",
            "rate_limit=0.1,server_error=0.05",
        ])
        .expect("rest args should parse");

        let command = cli.command.expect("command should parse");
        assert!(matches!(command, Command::Rest(_)), "expected rest command");
        let Command::Rest(args) = command else {
            return;
        };

        assert_eq!(args.routing, Some(RoutingPolicy::SchemaFirst));
        assert_eq!(args.base_prefix.as_deref(), Some("/api"));
        assert_eq!(args.auth_mode, Some(RestAuthMode::Bypass));
        let chaos = args.chaos.expect("chaos flag should parse");
        assert_eq!(chaos.rate_limit_per_million, 100_000);
        assert_eq!(chaos.server_error_per_million, 50_000);
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_config_requires_exactly_one_catalog_source() {
        let missing = McpArgs {
            server: None,
            manifest: None,
            host: "127.0.0.1".to_owned(),
            port: 9878,
            auth_mode: RestAuthMode::Shape,
            stdio: false,
            run: None,
            report: None,
        };
        let refusal = mcp_config_from_args(&missing, true).expect_err("source should be required");
        assert!(
            refusal
                .render(true)
                .expect("refusal JSON")
                .contains("\"code\": \"E_MCP_SOURCE_REQUIRED\"")
        );

        let ambiguous = McpArgs {
            server: Some("server".to_owned()),
            manifest: Some("manifest.json".into()),
            host: "127.0.0.1".to_owned(),
            port: 9878,
            auth_mode: RestAuthMode::Shape,
            stdio: false,
            run: None,
            report: None,
        };
        let refusal =
            mcp_config_from_args(&ambiguous, true).expect_err("source should be ambiguous");
        assert!(
            refusal
                .render(true)
                .expect("refusal JSON")
                .contains("\"code\": \"E_MCP_SOURCE_AMBIGUOUS\"")
        );
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_clap_parse_carries_manifest_source_and_run_options() {
        let cli = Cli::try_parse_from([
            "twinning",
            "mcp",
            "--manifest",
            "mcp.json",
            "--port",
            "0",
            "--auth-mode",
            "bypass",
            "--stdio",
            "--run",
            "echo ok",
            "--report",
            "mcp-report.json",
        ])
        .expect("mcp args should parse");

        let command = cli.command.expect("command should parse");
        assert!(matches!(command, Command::Mcp(_)), "expected mcp command");
        let Command::Mcp(args) = command else {
            return;
        };
        let refusal = mcp_config_from_args(&args, true).expect_err("stdio and run should conflict");
        assert!(
            refusal
                .render(true)
                .expect("refusal JSON")
                .contains("\"code\": \"E_MCP_STDIO_RUN_UNSUPPORTED\"")
        );

        let cli = Cli::try_parse_from([
            "twinning",
            "mcp",
            "--manifest",
            "mcp.json",
            "--port",
            "0",
            "--auth-mode",
            "bypass",
            "--stdio",
            "--report",
            "mcp-report.json",
        ])
        .expect("mcp args should parse");

        let command = cli.command.expect("command should parse");
        assert!(matches!(command, Command::Mcp(_)), "expected mcp command");
        let Command::Mcp(args) = command else {
            return;
        };
        let config = mcp_config_from_args(&args, true).expect("mcp config should build");

        assert_eq!(config.port, 0);
        assert_eq!(config.auth_mode, RestAuthMode::Bypass);
        assert!(config.stdio);
        assert_eq!(config.run_command.as_deref(), None);
        assert_eq!(
            config.report_path.as_deref(),
            Some(std::path::Path::new("mcp-report.json"))
        );
        assert!(matches!(
            config.source,
            super::McpCatalogInput::Manifest { .. }
        ));
        assert!(config.json);
    }
}

#[cfg(all(test, feature = "snowflake"))]
mod snowflake_tests {
    use clap::Parser;

    use super::snowflake_config_from_args;
    use crate::cli::{Cli, Command, SnowflakeArgs};

    #[test]
    fn snowflake_config_uses_http_defaults() {
        let args = SnowflakeArgs {
            schema: Some("schema.sql".into()),
            host: "127.0.0.1".to_owned(),
            port: 9876,
            run: None,
            serve: false,
            report: None,
            materialize_source_url: None,
            max_rows_per_table: 100_000,
        };

        let config = snowflake_config_from_args(&args, true).expect("config should build");

        assert_eq!(
            config.schema_path.as_deref(),
            Some(std::path::Path::new("schema.sql"))
        );
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 9876);
        assert_eq!(config.run_command, None);
        assert!(config.serve);
        assert!(config.serve_defaulted);
        assert!(config.json);
    }

    #[test]
    fn snowflake_config_requires_schema_path() {
        let args = SnowflakeArgs {
            schema: None,
            host: "127.0.0.1".to_owned(),
            port: 9876,
            run: None,
            serve: false,
            report: None,
            materialize_source_url: None,
            max_rows_per_table: 100_000,
        };

        let refusal =
            snowflake_config_from_args(&args, true).expect_err("schema should be required");
        let rendered = refusal.render(true).expect("refusal should render");

        assert!(rendered.contains("\"code\": \"E_SNOWFLAKE_SCHEMA_REQUIRED\""));
    }

    #[test]
    fn snowflake_config_refuses_run_and_serve_together() {
        let args = SnowflakeArgs {
            schema: Some("schema.sql".into()),
            host: "127.0.0.1".to_owned(),
            port: 9876,
            run: Some("echo ok".to_owned()),
            serve: true,
            report: None,
            materialize_source_url: None,
            max_rows_per_table: 100_000,
        };

        let refusal = snowflake_config_from_args(&args, true).expect_err("mode should be refused");
        let rendered = refusal.render(true).expect("refusal should render");

        assert!(rendered.contains("\"code\": \"E_AMBIGUOUS_LIVE_MODE\""));
        assert!(rendered.contains("\"protocol\": \"snowflake\""));
    }

    #[test]
    fn snowflake_clap_parse_carries_run_and_report_options() {
        let cli = Cli::try_parse_from([
            "twinning",
            "snowflake",
            "--schema",
            "schema.sql",
            "--port",
            "0",
            "--run",
            "echo ok",
            "--report",
            "snowflake-report.json",
        ])
        .expect("snowflake args should parse");

        let command = cli.command.expect("command should parse");
        assert!(
            matches!(command, Command::Snowflake(_)),
            "expected snowflake command"
        );
        let Command::Snowflake(args) = command else {
            return;
        };
        let config =
            snowflake_config_from_args(&args, true).expect("snowflake config should build");

        assert_eq!(config.port, 0);
        assert_eq!(config.run_command.as_deref(), Some("echo ok"));
        assert_eq!(
            config.report_path.as_deref(),
            Some(std::path::Path::new("snowflake-report.json"))
        );
        assert_eq!(config.materialize_source_url, None);
        assert_eq!(config.max_rows_per_table, 100_000);
        assert!(config.json);
    }

    #[test]
    fn snowflake_clap_parse_carries_materialization_options() {
        let cli = Cli::try_parse_from([
            "twinning",
            "snowflake",
            "--schema",
            "schema.sql",
            "--materialize-source-url",
            "snowflake://acct/db",
            "--max-rows-per-table",
            "17",
        ])
        .expect("snowflake args should parse");

        let command = cli.command.expect("command should parse");
        assert!(
            matches!(command, Command::Snowflake(_)),
            "expected snowflake command"
        );
        let Command::Snowflake(args) = command else {
            return;
        };
        let config =
            snowflake_config_from_args(&args, true).expect("snowflake config should build");

        assert_eq!(
            config.materialize_source_url.as_deref(),
            Some("snowflake://acct/db")
        );
        assert_eq!(config.max_rows_per_table, 17);
    }
}
