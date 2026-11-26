//! Default redaction of the embedded verify report (bd-uk5d).
//!
//! A SECOND COPY of the rules in verify's `crates/verify-cli/src/redact.rs`, as of verify
//! v0.7.5. verify applies them in its command line front end, after evaluation, so linking
//! `verify-engine` does not bring them. Keep the two the same. verify's own tests of these
//! rules are copied to `tests/redaction_rules.rs` in the root package (the one CI tests),
//! so a change to the markers, the variable or the numbering here fails a test.
//!
//! Without `--explicit`, the report never carries raw cell or key values from the twin.
//! Rule ids, severities, counts, binding names, column names and the shape of every entry
//! are kept; each affected key value and cell value, and each key value inside a refusal
//! detail, is replaced by a marker.
//!
//! - **Ordinal refs (default).** `redacted:ref:k1`, ... for key values and
//!   `redacted:ref:v1`, ... for cell values, numbered in order of first appearance. Keys and
//!   cells are separate namespaces. No hash of any value is computed. A ref reveals only the
//!   equality pattern inside one report.
//! - **Keyed markers (opt-in).** When `VERIFY_REDACTION_KEY` holds at least 16 bytes, each
//!   value becomes `redacted:hmac-sha256:<32 hex>`: HMAC-SHA256 over the canonical JSON of
//!   the value, truncated to 128 bits. An empty or short key is refused, never downgraded.
//!
//! One difference from verify: twinning always prints every affected entry, so verify's
//! numbering of sampled human output has no counterpart here.
//!
//! What this does NOT cover: column names, binding names, rule ids, the rule's own
//! predicate text, counts, and `bindings.<name>.content_hash`, an unsalted hash over the
//! whole relation.

use std::collections::BTreeMap;
use std::ffi::OsString;

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use verify_core::{refusal::RefusalCode, report::VerifyReport};

pub const ENV_KEY: &str = "VERIFY_REDACTION_KEY";
pub const MIN_KEY_BYTES: usize = 16;
pub const REF_PREFIX: &str = "redacted:ref:";
pub const HMAC_PREFIX: &str = "redacted:hmac-sha256:";
const REDACTED_TEXT: &str =
    "details withheld by default redaction; pass --explicit to include them";

/// Why `VERIFY_REDACTION_KEY` was refused. Never carries the key or its length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    Empty,
    TooShort,
}

impl KeyError {
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::TooShort => "too_short",
        }
    }
}

/// An operator-held redaction key. `Debug` never prints the bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct RedactionKey(Vec<u8>);

impl std::fmt::Debug for RedactionKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedactionKey(<withheld>)")
    }
}

impl RedactionKey {
    /// `None` when the variable is unset (ordinal refs). Set-but-empty and too-short values are
    /// errors, so a misconfigured key cannot quietly fall back.
    pub fn from_env_value(value: Option<OsString>) -> Result<Option<Self>, KeyError> {
        let Some(value) = value else {
            return Ok(None);
        };
        #[cfg(unix)]
        let bytes = std::os::unix::ffi::OsStringExt::into_vec(value);
        #[cfg(not(unix))]
        let bytes = value.to_string_lossy().into_owned().into_bytes();
        if bytes.is_empty() {
            return Err(KeyError::Empty);
        }
        if bytes.len() < MIN_KEY_BYTES {
            return Err(KeyError::TooShort);
        }
        Ok(Some(Self(bytes)))
    }

    /// `redacted:hmac-sha256:<32 hex>` over the canonical JSON of `value`.
    pub fn marker(&self, value: &Value) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC accepts a key of any length");
        mac.update(canonical(value).as_bytes());
        let digest = mac.finalize().into_bytes();
        let mut hex = String::with_capacity(32);
        for byte in &digest[..16] {
            hex.push_str(&format!("{byte:02x}"));
        }
        format!("{HMAC_PREFIX}{hex}")
    }
}

/// What the embedded verify report may carry. The default is the safe one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum VerifyRedaction {
    #[default]
    Ordinal,
    Keyed(RedactionKey),
    /// `--explicit`: raw values.
    Explicit,
}

impl VerifyRedaction {
    /// The key is validated even with `--explicit`, as standalone verify does.
    pub fn resolve(explicit: bool, key: Option<OsString>) -> Result<Self, KeyError> {
        let key = RedactionKey::from_env_value(key)?;
        Ok(match key {
            _ if explicit => Self::Explicit,
            Some(key) => Self::Keyed(key),
            None => Self::Ordinal,
        })
    }

    pub fn apply(&self, report: &mut VerifyReport) {
        match self {
            Self::Ordinal => redact_report(report, None),
            Self::Keyed(key) => redact_report(report, Some(key)),
            Self::Explicit => {}
        }
    }
}

fn canonical(value: &Value) -> String {
    serde_json::to_string(value).expect("a JSON value serializes")
}

#[derive(Clone, Copy)]
enum Namespace {
    Key,
    Cell,
}

struct Marker<'a> {
    key: Option<&'a RedactionKey>,
    keys: BTreeMap<String, usize>,
    cells: BTreeMap<String, usize>,
}

impl<'a> Marker<'a> {
    fn new(key: Option<&'a RedactionKey>) -> Self {
        Self {
            key,
            keys: BTreeMap::new(),
            cells: BTreeMap::new(),
        }
    }

    fn mark(&mut self, namespace: Namespace, value: &Value) -> Value {
        if let Some(key) = self.key {
            return Value::String(key.marker(value));
        }
        let (table, letter) = match namespace {
            Namespace::Key => (&mut self.keys, 'k'),
            Namespace::Cell => (&mut self.cells, 'v'),
        };
        let next = table.len() + 1;
        let ordinal = *table.entry(canonical(value)).or_insert(next);
        Value::String(format!("{REF_PREFIX}{letter}{ordinal}"))
    }
}

/// Redact a report in place. Runs after evaluation, so no verdict can depend on it.
pub fn redact_report(report: &mut VerifyReport, key: Option<&RedactionKey>) {
    let mut marker = Marker::new(key);
    for result in &mut report.results {
        for entry in &mut result.affected {
            if let Some(entry_key) = entry.key.as_mut() {
                for value in entry_key.values_mut() {
                    *value = marker.mark(Namespace::Key, value);
                }
            }
            if let Some(value) = entry.value.as_mut() {
                *value = marker.mark(Namespace::Cell, value);
            }
        }
    }
    if let Some(refusal) = report.refusal.as_mut() {
        redact_key_objects(&mut refusal.detail, &mut marker);
        // An SQL engine's own error text can quote data. The one SQL_ERROR that carries a
        // `column` field names only a type, so it stays readable. Embedded execution refuses
        // query rules at startup today; the rule is kept so the two copies stay the same.
        let names_column_only = refusal.detail.get("column").is_some_and(Value::is_string);
        if refusal.code == RefusalCode::SqlError && !names_column_only {
            refusal.message = format!("{:?}: {REDACTED_TEXT}", refusal.code);
            if let Value::Object(fields) = &mut refusal.detail {
                for name in ["message", "error"] {
                    if fields.contains_key(name) {
                        fields.insert(name.to_owned(), Value::String(REDACTED_TEXT.to_owned()));
                    }
                }
            }
        }
    }
}

/// Replace every value inside any object stored under a field named `key`.
fn redact_key_objects(value: &mut Value, marker: &mut Marker<'_>) {
    match value {
        Value::Object(fields) => {
            for (name, child) in fields.iter_mut() {
                if name == "key" {
                    match child {
                        Value::Object(inner) => {
                            for leaf in inner.values_mut() {
                                *leaf = marker.mark(Namespace::Key, leaf);
                            }
                        }
                        Value::Null => {}
                        other => *other = marker.mark(Namespace::Key, other),
                    }
                } else {
                    redact_key_objects(child, marker);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_key_objects(item, marker);
            }
        }
        _ => {}
    }
}
