use serde::Serialize;

pub const CANONICAL_ROOT: &str = "~/.cmdrvl";
pub const CONFIG_PATH: &str = "~/.cmdrvl/config/twinning/config.toml";
pub const STATE_DIR: &str = "~/.cmdrvl/state/twinning/";
pub const CACHE_DIR: &str = "~/.cmdrvl/cache/twinning/";
pub const MIGRATION_LOG: &str = "~/.cmdrvl/migrations/applied.jsonl";
pub const DEPRECATION_NOTICES: &str = "~/.cmdrvl/notices/deprecated-paths.jsonl";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigFootprint {
    pub canonical_root: &'static str,
    pub config_path: &'static str,
    pub state_dir: &'static str,
    pub cache_dir: &'static str,
    pub migration_log: &'static str,
    pub deprecation_notices: &'static str,
    pub legacy_paths: Vec<&'static str>,
    pub legacy_migration_required: bool,
    pub explicit_output_policy: &'static str,
}

pub fn config_footprint() -> ConfigFootprint {
    ConfigFootprint {
        canonical_root: CANONICAL_ROOT,
        config_path: CONFIG_PATH,
        state_dir: STATE_DIR,
        cache_dir: CACHE_DIR,
        migration_log: MIGRATION_LOG,
        deprecation_notices: DEPRECATION_NOTICES,
        legacy_paths: Vec::new(),
        legacy_migration_required: false,
        explicit_output_policy: "Reports, snapshots, query traces, seed contracts, proof bundles, and session reports are written only to explicit operator-supplied paths.",
    }
}

#[cfg(test)]
mod tests {
    use super::config_footprint;

    #[test]
    fn footprint_uses_cmdrvl_root_without_legacy_paths() {
        let footprint = config_footprint();

        assert_eq!(footprint.canonical_root, "~/.cmdrvl");
        assert_eq!(
            footprint.config_path,
            "~/.cmdrvl/config/twinning/config.toml"
        );
        assert_eq!(footprint.state_dir, "~/.cmdrvl/state/twinning/");
        assert_eq!(footprint.cache_dir, "~/.cmdrvl/cache/twinning/");
        assert!(footprint.legacy_paths.is_empty());
        assert!(!footprint.legacy_migration_required);
    }
}
