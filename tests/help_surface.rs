#![forbid(unsafe_code)]

//! Agent-ergonomics regression test (R-010).
//!
//! Pins the CLI's self-documentation surface so help text cannot silently rot
//! back to the blank-flag state that an earlier build shipped. For every
//! subcommand, asserts that each long option carries a non-empty description in
//! `--help`, and that the top-level help points agents at the read-only
//! discovery surfaces and the exit-code dictionary.
//!
//! Robust to both clap help layouts: the compact single-line layout used by
//! subcommands (`--flag <V>   Description`) and the expanded layout used by the
//! top-level command (`--flag` on one line, description indented below).

use std::{path::PathBuf, process::Command};

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn help_for(args: &[&str]) -> String {
    let output = Command::new(twinning_bin())
        .args(args)
        .arg("--help")
        .output()
        .expect("run twinning --help");
    assert!(
        output.status.success(),
        "`twinning {} --help` should exit 0, got {:?}\nstderr: {}",
        args.join(" "),
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout).expect("help stdout utf-8")
}

/// True if the trimmed line introduces an option (starts with `-`).
fn is_option_line(line: &str) -> bool {
    line.trim_start().starts_with('-')
}

/// Long flag tokens (`--name`) on an option line, excluding clap built-ins.
fn long_flags(option_line: &str) -> Vec<String> {
    option_line
        .split_whitespace()
        .filter(|t| t.starts_with("--"))
        .map(|t| t.trim_end_matches(',').to_string())
        .filter(|t| t != "--help" && t != "--version")
        .collect()
}

/// Inline description on an option line after stripping flag tokens, `<VALUE>`
/// placeholders, and `[default: ...]` / `[possible values: ...]` tags.
fn inline_description(option_line: &str) -> String {
    let mut s = option_line.trim().to_string();
    strip_spans(&mut s, '[', ']');
    strip_spans(&mut s, '<', '>');
    s.split_whitespace()
        .filter(|t| !t.starts_with('-'))
        .collect::<Vec<_>>()
        .join(" ")
}

fn strip_spans(s: &mut String, open: char, close: char) {
    while let (Some(a), Some(b)) = (s.find(open), s.find(close)) {
        if a < b {
            s.replace_range(a..=b, " ");
        } else {
            break;
        }
    }
}

fn has_words(s: &str) -> bool {
    s.chars().filter(|c| c.is_ascii_alphabetic()).count() >= 3
}

/// Asserts every long option in this help output carries a description, in
/// either clap layout.
fn assert_all_flags_documented(label: &str, help: &str) {
    let lines: Vec<&str> = help.lines().collect();
    let mut checked = 0usize;
    for (i, line) in lines.iter().enumerate() {
        if !is_option_line(line) {
            continue;
        }
        let flags = long_flags(line);
        if flags.is_empty() {
            continue;
        }
        let documented = if has_words(&inline_description(line)) {
            // Compact layout: description on the same line.
            true
        } else {
            // Expanded layout: description on the following indented, non-option line.
            lines
                .get(i + 1)
                .map(|next| !is_option_line(next) && has_words(next))
                .unwrap_or(false)
        };
        assert!(
            documented,
            "`{label}`: option `{}` has no description in --help (agent-ergonomics regression).\nline: {:?}",
            flags.join("/"),
            line,
        );
        checked += flags.len();
    }
    assert!(
        checked > 0,
        "`{label}`: expected at least one documented long option, found none",
    );
}

#[test]
fn top_level_help_documents_global_flags_and_discovery_surface() {
    let help = help_for(&[]);
    assert_all_flags_documented("twinning", &help);

    // Global flags are described.
    assert!(
        help.contains("machine-readable JSON"),
        "top-level --help must describe --json"
    );
    assert!(
        help.contains("operator manifest"),
        "top-level --help must describe --describe"
    );

    // Discovery surfaces are advertised so an agent need not read source or docs.
    for needle in [
        "twinning --describe",
        "doctor capabilities --json",
        "doctor --robot-triage",
    ] {
        assert!(
            help.contains(needle),
            "top-level --help must advertise discovery surface `{needle}`"
        );
    }

    // Exit-code dictionary is present.
    assert!(
        help.contains("Exit codes:") && help.contains("refusal"),
        "top-level --help must document the exit-code dictionary"
    );
}

#[test]
fn postgres_help_is_fully_documented_with_examples() {
    let help = help_for(&["postgres"]);
    assert_all_flags_documented("twinning postgres", &help);
    assert!(
        help.contains("Examples:") && help.contains("--schema schema.sql"),
        "postgres --help must include a worked example for the canonical task"
    );
    assert!(
        help.contains("exactly one bootstrap source"),
        "postgres --help must state the --schema/--restore exclusivity"
    );
}

#[test]
fn declared_engine_placeholders_are_documented() {
    assert_all_flags_documented("twinning mysql", &help_for(&["mysql"]));
    assert_all_flags_documented("twinning oracle", &help_for(&["oracle"]));
}

#[test]
fn doctor_and_proof_help_are_documented() {
    assert_all_flags_documented("twinning doctor", &help_for(&["doctor"]));
    assert_all_flags_documented(
        "twinning proof twin-pair",
        &help_for(&["proof", "twin-pair"]),
    );
    assert_all_flags_documented(
        "twinning proof twin-pair orchestrate",
        &help_for(&["proof", "twin-pair", "orchestrate"]),
    );
}

#[cfg(feature = "rest")]
#[test]
fn rest_and_port_help_are_documented() {
    assert_all_flags_documented("twinning rest", &help_for(&["rest"]));
    assert_all_flags_documented("twinning port", &help_for(&["port"]));
}

#[cfg(feature = "mcp")]
#[test]
fn mcp_help_is_documented() {
    assert_all_flags_documented("twinning mcp", &help_for(&["mcp"]));
}

#[cfg(feature = "snowflake")]
#[test]
fn snowflake_help_is_documented() {
    assert_all_flags_documented("twinning snowflake", &help_for(&["snowflake"]));
}
