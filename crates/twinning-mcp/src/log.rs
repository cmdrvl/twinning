//! One-line, secret-safe stderr event formatting shared by the server modules.

use std::io::{self, Write};
use std::sync::{Mutex, OnceLock};

use chrono::{SecondsFormat, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

pub fn format_line(
    level: Level,
    twin_id: Option<&str>,
    event: &str,
    fields: &[(&str, String)],
) -> String {
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut line = format!(
        "{timestamp} {} {} {}",
        level.as_str(),
        twin_id.unwrap_or("-"),
        safe(event)
    );
    for (key, value) in fields {
        line.push(' ');
        line.push_str(&safe(key));
        line.push('=');
        line.push_str(&safe(value));
    }
    line
}

pub fn emit(level: Level, twin_id: Option<&str>, event: &str, fields: &[(&str, String)]) {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _ = writeln!(
        io::stderr().lock(),
        "{}",
        format_line(level, twin_id, event, fields)
    );
}

fn safe(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\n' | '\r' | '\0' | '=' => '_',
            _ => character,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Level, format_line};

    #[test]
    fn log_line_has_expected_levels_and_sanitizes_values() {
        let line = format_line(
            Level::Info,
            Some("tw_abcdef"),
            "child_spawned",
            &[("detail", "secret\nvalue=x".into())],
        );
        assert!(line.contains(" INFO tw_abcdef child_spawned detail=secret_value_x"));
        assert!(!line.contains('\n'));
        assert!(format_line(Level::Warn, None, "stale", &[]).contains(" WARN - stale"));
        assert!(
            format_line(
                Level::Error,
                None,
                "tool_error",
                &[("code", "E_INTERNAL".into())]
            )
            .contains(" ERROR - tool_error code=E_INTERNAL")
        );
    }
}
