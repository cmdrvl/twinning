use std::{collections::BTreeMap, fmt, path::PathBuf, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{
    auth::RestAuthMode,
    policy::{RoutingConfig, RoutingPolicy, resolve_routing_config},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestConfig {
    pub spec_path: PathBuf,
    pub host: String,
    pub port: u16,
    pub run_command: Option<String>,
    pub serve: bool,
    pub serve_defaulted: bool,
    pub report_path: Option<PathBuf>,
    pub canary_path: Option<PathBuf>,
    pub strict: bool,
    pub routing: RoutingConfig,
    pub server_variables: BTreeMap<String, String>,
    pub auth_mode: Option<RestAuthMode>,
    pub chaos: Option<ChaosConfig>,
    pub json: bool,
}

impl RestConfig {
    pub fn from_parts(args: RestConfigParts, json: bool) -> Self {
        let serve_defaulted = !args.serve && args.run_command.is_none();
        let mut routing = resolve_routing_config(args.routing, args.base_prefix, None);
        routing.server_variables = args.server_variables.clone();

        Self {
            spec_path: args.spec_path,
            host: args.host,
            port: args.port,
            run_command: args.run_command,
            serve: args.serve || serve_defaulted,
            serve_defaulted,
            report_path: args.report_path,
            canary_path: args.canary_path,
            strict: args.strict,
            routing,
            server_variables: args.server_variables,
            auth_mode: args.auth_mode,
            chaos: args.chaos,
            json,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestConfigParts {
    pub spec_path: PathBuf,
    pub host: String,
    pub port: u16,
    pub run_command: Option<String>,
    pub serve: bool,
    pub report_path: Option<PathBuf>,
    pub canary_path: Option<PathBuf>,
    pub strict: bool,
    pub routing: Option<RoutingPolicy>,
    pub base_prefix: Option<String>,
    pub server_variables: BTreeMap<String, String>,
    pub auth_mode: Option<RestAuthMode>,
    pub chaos: Option<ChaosConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChaosConfig {
    pub rate_limit_per_million: u32,
    pub server_error_per_million: u32,
    pub timeout_per_million: u32,
}

impl ChaosConfig {
    pub const SCALE: u32 = 1_000_000;

    pub fn is_enabled(self) -> bool {
        self.rate_limit_per_million > 0
            || self.server_error_per_million > 0
            || self.timeout_per_million > 0
    }

    fn probability_label(value: u32) -> String {
        let mut rendered = format!("{:.6}", value as f64 / Self::SCALE as f64);
        while rendered.contains('.') && rendered.ends_with('0') {
            rendered.pop();
        }
        if rendered.ends_with('.') {
            rendered.push('0');
        }
        rendered
    }
}

impl fmt::Display for ChaosConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rate_limit={}, server_error={}, timeout={}",
            Self::probability_label(self.rate_limit_per_million),
            Self::probability_label(self.server_error_per_million),
            Self::probability_label(self.timeout_per_million)
        )
    }
}

impl FromStr for ChaosConfig {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let mut config = Self::default();
        let mut saw_pair = false;

        for pair in raw
            .split(',')
            .map(str::trim)
            .filter(|pair| !pair.is_empty())
        {
            saw_pair = true;
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| format!("chaos entry `{pair}` must be key=value"))?;
            let probability = parse_probability_per_million(value.trim())?;
            match key.trim() {
                "rate_limit" => config.rate_limit_per_million = probability,
                "server_error" => config.server_error_per_million = probability,
                "timeout" => config.timeout_per_million = probability,
                other => {
                    return Err(format!(
                        "unknown chaos key `{other}`; expected rate_limit, server_error, or timeout"
                    ));
                }
            }
        }

        if saw_pair {
            Ok(config)
        } else {
            Err(String::from(
                "chaos config must include at least one key=value pair",
            ))
        }
    }
}

fn parse_probability_per_million(raw: &str) -> Result<u32, String> {
    let value = raw
        .parse::<f64>()
        .map_err(|_| format!("chaos probability `{raw}` is not a number"))?;
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(format!(
            "chaos probability `{raw}` must be between 0.0 and 1.0"
        ));
    }

    Ok((value * ChaosConfig::SCALE as f64).round() as u32)
}

#[cfg(test)]
mod tests {
    use super::ChaosConfig;

    #[test]
    fn chaos_config_parses_probabilities() {
        let config: ChaosConfig = "rate_limit=0.1,server_error=0.05,timeout=0.03"
            .parse()
            .expect("chaos config should parse");

        assert_eq!(config.rate_limit_per_million, 100_000);
        assert_eq!(config.server_error_per_million, 50_000);
        assert_eq!(config.timeout_per_million, 30_000);
        assert!(config.is_enabled());
        assert_eq!(
            config.to_string(),
            "rate_limit=0.1, server_error=0.05, timeout=0.03"
        );
    }

    #[test]
    fn chaos_config_rejects_unknown_keys_and_out_of_range_probabilities() {
        assert!("latency=0.1".parse::<ChaosConfig>().is_err());
        assert!("rate_limit=1.1".parse::<ChaosConfig>().is_err());
        assert!("rate_limit=-0.1".parse::<ChaosConfig>().is_err());
    }
}
