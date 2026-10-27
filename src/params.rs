use jsonschema::Validator;
use log::debug;
use serde_json::{Map, Value};

use color_eyre::Section;
use color_eyre::eyre::Result;

use crate::cli::KVPair;
use crate::error::user_error;

const SCHEMA: &str = include_str!("params_schema.json");

// Launch-parameter key names.
//
// These are the wire names the API accepts (see params_schema.json). Production
// code references these constants so a typo fails to compile; tests that pin
// the wire format keep the raw literals on purpose.
pub const ANT_TEST_NAME: &str = "antithesis.test_name";
pub const ANT_DESCRIPTION: &str = "antithesis.description";
pub const ANT_DURATION: &str = "antithesis.duration";
pub const ANT_CONFIG_IMAGE: &str = "antithesis.config_image";
pub const ANT_IMAGES: &str = "antithesis.images";
pub const ANT_SOURCE: &str = "antithesis.source";
pub const ANT_IS_EPHEMERAL: &str = "antithesis.is_ephemeral";
pub const ANT_REPORT_RECIPIENTS: &str = "antithesis.report.recipients";
pub const ANT_EVENT_DESCRIPTION: &str = "antithesis.event_description";
pub const ANT_FILTER_LOGS_MATCHING: &str = "antithesis.filter_logs_matching";
pub const ANT_FILTER_SOURCE_MATCHING: &str = "antithesis.filter_source_matching";
pub const ANT_PERFORMANCE_TIER: &str = "antithesis.performance_tier";
pub const ANT_FAULT_TOLERANCE: &str = "antithesis.fault_tolerance";
pub const ANT_FAULT_EXCLUSIONS: &str = "antithesis.fault_exclusions";
/// The kind of tool that launched a run. The server shows it as the creator's
/// type. Observed on `GET /api/v0/runs/{run_id}` (tenant version 64): a run
/// launched with `run.caller_type=snouty` reports `creator.type` `snouty`.
pub const RUN_CALLER_TYPE: &str = "run.caller_type";
/// Prefix of the user-defined `attrs.<name>` parameters the server records on a run.
pub const ATTRS_PREFIX: &str = "attrs.";
pub const VCS_REPO_TYPE: &str = "vcs.repo_type";
pub const VCS_REPO_OWNER: &str = "vcs.repo_owner";
pub const VCS_REPO_NAME: &str = "vcs.repo_name";
pub const VCS_REPO_BRANCH: &str = "vcs.repo_branch";
pub const VCS_VERSION_ID: &str = "vcs.version_id";
pub const VCS_VERSION_LINK: &str = "vcs.version_link";
/// The `vcs.*` parameters that `runs show` lists, with their labels, in display
/// order. `launch` detects exactly these.
/// Observed as strings on `GET /api/v0/runs/{run_id}` (tenant version 63), on runs
/// launched by the Antithesis GitHub trigger action.
/// TODO: add `vcs.pr_*` and `vcs.system_name` when a run records them.
pub const VCS_PARAMS: &[(&str, &str)] = &[
    (VCS_REPO_TYPE, "Repo Type"),
    (VCS_REPO_OWNER, "Repo Owner"),
    (VCS_REPO_NAME, "Repo Name"),
    (VCS_REPO_BRANCH, "Branch"),
    (VCS_VERSION_ID, "Commit"),
    (VCS_VERSION_LINK, "Commit Link"),
];

pub const ANT_DEBUGGING_SESSION_ID: &str = "antithesis.debugging.session_id";
pub const ANT_DEBUGGING_RUN_ID: &str = "antithesis.debugging.run_id";
pub const ANT_DEBUGGING_INPUT_HASH: &str = "antithesis.debugging.input_hash";
pub const ANT_DEBUGGING_VTIME: &str = "antithesis.debugging.vtime";

/// The key that both sides of [`Params::extend_checked`] set.
#[derive(Debug)]
pub struct DuplicateKey(pub String);

/// Params parsed from CLI arguments and validated against the JSON schema.
#[derive(Debug, Clone, Default)]
pub struct Params {
    inner: Map<String, Value>,
}

impl Params {
    /// Create empty params for incremental building.
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn from_key_value_pairs<I, S>(pairs: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(pairs
            .into_iter()
            .map(|pair| pair.as_ref().parse())
            .collect::<std::result::Result<_, crate::cli::ParseKVPairError>>()?)
    }

    /// Validate params against the test params schema, and check the pattern
    /// params. The checks run here so no launch path can skip them.
    ///
    /// The platform checks the log filter patterns only after the run starts,
    /// so a bad pattern fails the run minutes later. The `regex` crate is close
    /// to RE2 but not identical, so the filter check is a pre-check only.
    pub fn validate_test_params(&self) -> Result<()> {
        validate_against_def(&self.inner, "testParams")?;
        for (key, suppressed) in [
            (ANT_FILTER_LOGS_MATCHING, "every log line"),
            (ANT_FILTER_SOURCE_MATCHING, "the output of every source"),
        ] {
            if let Some(value) = self.inner.get(key).and_then(Value::as_str) {
                validate_filter_pattern(key, value, suppressed)?;
            }
        }
        if let Some(value) = self.inner.get(ANT_FAULT_EXCLUSIONS).and_then(Value::as_str) {
            validate_fault_exclusions(value)?;
        }
        Ok(())
    }

    /// Validate params against the debugging params schema.
    pub fn validate_debugging_params(&self) -> Result<()> {
        validate_against_def(&self.inner, "debuggingParams")
    }

    /// Check if the params are empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Insert a key-value pair into the params.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.inner.insert(key.into(), Value::String(value.into()));
    }

    /// Check if a key exists in the params.
    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    /// Get a reference to the inner map.
    pub fn as_map(&self) -> &Map<String, Value> {
        &self.inner
    }

    /// Merge another Params into this one, with the other params taking priority.
    pub fn merge(&mut self, other: Params) {
        for (key, value) in other.inner {
            self.inner.insert(key, value);
        }
    }

    /// Merge `other` into this one, or merge nothing when `other` sets a key
    /// that this one already holds.
    pub fn extend_checked(&mut self, other: Params) -> std::result::Result<(), DuplicateKey> {
        if let Some(key) = other.inner.keys().find(|&key| self.inner.contains_key(key)) {
            return Err(DuplicateKey(key.clone()));
        }
        self.merge(other);
        Ok(())
    }

    /// Get a redacted copy of the params for safe display in logs/CI.
    /// Sensitive fields (tokens, emails) are replaced with "[REDACTED]".
    pub fn to_redacted_map(&self) -> Map<String, Value> {
        self.inner
            .iter()
            .map(|(k, v)| {
                let redacted = is_sensitive_key(k);
                let value = if redacted {
                    Value::String("[REDACTED]".to_string())
                } else {
                    v.clone()
                };
                (k.clone(), value)
            })
            .collect()
    }
}

impl FromIterator<KVPair> for Params {
    fn from_iter<I: IntoIterator<Item = KVPair>>(pairs: I) -> Self {
        let inner = pairs
            .into_iter()
            .map(|KVPair { key, value }| (key, Value::String(value)))
            .collect();
        Self { inner }
    }
}

// A key suffix, not a file extension.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
fn is_sensitive_key(key: &str) -> bool {
    key.ends_with(".token") || key == ANT_REPORT_RECIPIENTS
}

/// The platform compiles each filter pattern into a 1024-byte buffer
/// including the NUL terminator.
const MAX_FILTER_PATTERN_BYTES: usize = 1023;

/// `suppressed` names the output that an empty-string match suppresses, for
/// the error message.
fn validate_filter_pattern(key: &str, value: &str, suppressed: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(user_error(format!("{key} is empty"))
            .note("the platform skips filtering for an empty pattern")
            .suggestion("provide a pattern or drop the flag"));
    }
    if value.len() > MAX_FILTER_PATTERN_BYTES {
        return Err(user_error(format!(
            "{key} is too long: {} bytes (max {MAX_FILTER_PATTERN_BYTES})",
            value.len()
        )));
    }
    if compile_pattern(key, value)?.is_match("") {
        return Err(user_error(format!(
            "{key} matches the empty string, which would suppress {suppressed}"
        )));
    }
    Ok(())
}

/// The OpenAPI spec gives the pattern the `regex` crate's syntax, but the
/// server can run another version of the crate, so this is a pre-check only.
fn validate_fault_exclusions(value: &str) -> Result<()> {
    // A blank pattern is most likely an unset shell variable, and it would
    // exclude every container from faults.
    if value.trim().is_empty() {
        return Err(user_error(format!("{ANT_FAULT_EXCLUSIONS} is empty"))
            .suggestion("provide a pattern or drop the flag"));
    }
    compile_pattern(ANT_FAULT_EXCLUSIONS, value)?;
    Ok(())
}

fn compile_pattern(key: &str, value: &str) -> Result<regex::Regex> {
    regex::Regex::new(value)
        .map_err(|err| user_error(format!("{key} is not a valid regular expression: {err}")))
}

fn validate_against_def(params: &Map<String, Value>, def_name: &str) -> Result<()> {
    let notes = collect_validation_notes(params, def_name);
    if notes.is_empty() {
        debug!("validation passed");
        return Ok(());
    }

    debug!("validation failed with {} notes", notes.len());
    // The message states the failure; each schema violation is its own note.
    let mut report = user_error("validation failed");
    for note in notes {
        debug!("  - {note}");
        report = report.note(note);
    }
    Err(report)
}

fn collect_validation_notes(params: &Map<String, Value>, def_name: &str) -> Vec<String> {
    let schema: Value = serde_json::from_str(SCHEMA).expect("valid schema");

    // Build a schema that references the specific definition
    let def_schema = serde_json::json!({
        "$ref": format!("#/$defs/{}", def_name),
        "$defs": schema["$defs"]
    });

    let validator = Validator::new(&def_schema).expect("valid schema");
    let instance = Value::Object(params.clone());

    validator
        .iter_errors(&instance)
        .map(|error| note_for_error(&error))
        .collect()
}

/// Render a single validation error as a note, prefixed with the offending
/// field name when the error is attributable to a specific property.
///
/// jsonschema messages name the value and the violated constraint, but not
/// the field (e.g. `"asdf" does not match "^[0-9]+(\.[0-9]+)?$"`). An
/// object-level error, such as a missing required property, has an empty
/// instance path and already names the property, so it gets no prefix.
fn note_for_error(error: &jsonschema::ValidationError) -> String {
    let pointer = error.instance_path().as_str();
    match pointer.strip_prefix('/') {
        Some(field) if !field.is_empty() => {
            format!("{}: {error}", unescape_pointer_token(field))
        }
        _ => error.to_string(),
    }
}

/// Decode a JSON Pointer reference token back into the raw property name,
/// reversing the `~1`/`~0` escapes for `/` and `~` (RFC 6901). Param keys are
/// flat dotted strings that rarely need this, but keys carrying a literal `/`
/// or `~` would otherwise surface mangled.
fn unescape_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::fake_secrets::FAKE_GITHUB_TOKEN;
    use hegel::generators::{self, Generator};

    #[test]
    fn parse_values_as_strings() {
        let pairs = ["count=42", "enabled=true", "ratio=3.14"];
        let params = Params::from_key_value_pairs(pairs).unwrap();

        // Values are kept as strings (schema validates format)
        assert_eq!(params.as_map().get("count").unwrap(), "42");
        assert_eq!(params.as_map().get("enabled").unwrap(), "true");
        assert_eq!(params.as_map().get("ratio").unwrap(), "3.14");
    }

    #[test]
    fn parse_integration_params() {
        let pairs = [
            "antithesis.integrations.github.callback_url=https://github.com/cb".to_owned(),
            format!("antithesis.integrations.github.token={FAKE_GITHUB_TOKEN}"),
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();

        assert_eq!(
            params
                .as_map()
                .get("antithesis.integrations.github.callback_url")
                .unwrap(),
            "https://github.com/cb"
        );
        assert_eq!(
            params
                .as_map()
                .get("antithesis.integrations.github.token")
                .unwrap(),
            FAKE_GITHUB_TOKEN
        );
    }

    #[test]
    fn validate_test_params_success() {
        let pairs = ["antithesis.duration=30", "antithesis.is_ephemeral=true"];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_test_params().is_ok());
    }

    #[test]
    fn validate_test_params_checks_the_filter_patterns() {
        let params = Params::from_key_value_pairs([
            "antithesis.duration=30",
            "antithesis.filter_logs_matching=x*",
        ])
        .unwrap();
        assert!(params.validate_test_params().is_err());
    }

    #[test]
    fn validate_images_semicolon_delimited() {
        let params =
            Params::from_key_value_pairs(["antithesis.images=app@sha256:abc;db:latest"]).unwrap();
        assert!(params.validate_test_params().is_ok());
    }

    #[test]
    fn validate_images_semicolon_delimited_with_registries() {
        let pairs = [
            "antithesis.images=us-central1-docker.pkg.dev/myproject/repo/app:v1.2.3; registry.example.com:5000/team/service@sha256:abc123def456",
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_test_params().is_ok());
    }

    #[test]
    fn unknown_antithesis_param_is_allowed() {
        // The platform can add antithesis.* params before snouty learns about
        // them, so unknown antithesis.* keys must pass validation (#211).
        let params =
            Params::from_key_value_pairs(["antithesis.duration=30", "antithesis.new_param=x"])
                .unwrap();
        assert!(params.validate_test_params().is_ok());
    }

    #[test]
    fn pattern_violation_note_names_the_field() {
        // The jsonschema message only describes the value and the pattern; the
        // note must prepend the field name so the user can find the culprit.
        let params = Params::from_key_value_pairs(["antithesis.duration=asdf"]).unwrap();
        let notes = collect_validation_notes(params.as_map(), "testParams");
        assert_eq!(notes.len(), 1, "expected one error, got: {notes:?}");
        assert!(
            notes[0].starts_with("antithesis.duration: "),
            "note should be attributed to the field, got: {notes:?}"
        );
    }

    #[test]
    fn unescape_pointer_token_reverses_rfc6901_escapes() {
        assert_eq!(
            unescape_pointer_token("antithesis.duration"),
            "antithesis.duration"
        );
        assert_eq!(unescape_pointer_token("a~1b"), "a/b");
        assert_eq!(unescape_pointer_token("a~0b"), "a~b");
        // A literal "~1" is encoded as "~01" and must round-trip, not collapse.
        assert_eq!(unescape_pointer_token("~01"), "~1");
    }

    #[test]
    fn validate_images_rejects_comma_delimited() {
        let params =
            Params::from_key_value_pairs(["antithesis.images=app:latest,db:latest"]).unwrap();
        assert!(params.validate_test_params().is_err());
    }

    #[test]
    fn validate_test_params_with_custom_props() {
        let pairs = ["antithesis.duration=30", "my.custom.property=value"];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_test_params().is_ok());
    }

    #[test]
    fn validate_debugging_params_success() {
        let pairs = [
            "antithesis.debugging.input_hash=abc123",
            "antithesis.debugging.session_id=sess-456",
            "antithesis.debugging.vtime=1234567890",
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_debugging_params().is_ok());
    }

    #[test]
    fn validate_debugging_params_accepts_custom_props() {
        let pairs = [
            "antithesis.debugging.input_hash=abc123",
            "antithesis.debugging.session_id=sess-456",
            "antithesis.debugging.vtime=123",
            "my.custom.prop=value",
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_debugging_params().is_ok());
    }

    #[test]
    fn validate_debugging_params_run_id_success() {
        let pairs = [
            "antithesis.debugging.input_hash=abc123",
            "antithesis.debugging.run_id=9043254f65c9c65d63fe043a0abfc7fc-53-1",
            "antithesis.debugging.vtime=1234567890",
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_debugging_params().is_ok());
    }

    #[test]
    fn validate_debugging_params_does_not_require_an_identifier() {
        // clap enforces the exactly-one-of rule, not the schema: schema
        // validation only requires input_hash + vtime.
        let pairs = [
            "antithesis.debugging.input_hash=abc123",
            "antithesis.debugging.vtime=1234567890",
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_debugging_params().is_ok());
    }

    #[test]
    fn extend_checked_merges_nothing_on_a_duplicate_key() {
        let mut base = Params::from_key_value_pairs(["a=1"]).unwrap();
        let DuplicateKey(key) = base
            .extend_checked(Params::from_key_value_pairs(["b=2", "a=3"]).unwrap())
            .unwrap_err();
        assert_eq!(key, "a");
        assert_eq!(
            base.as_map(),
            Params::from_key_value_pairs(["a=1"]).unwrap().as_map()
        );

        base.extend_checked(Params::from_key_value_pairs(["b=2"]).unwrap())
            .unwrap();
        assert_eq!(base.as_map().get("b").unwrap(), "2");
    }

    #[test]
    fn merge_params_overwrites_existing_keys() {
        let mut base = Params::from_key_value_pairs([
            "antithesis.duration=30",
            "antithesis.description=base description",
        ])
        .unwrap();

        let overlay = Params::from_key_value_pairs([
            "antithesis.duration=60",
            "antithesis.report.recipients=team@example.com",
        ])
        .unwrap();

        base.merge(overlay);

        // Overlay value should overwrite base value
        assert_eq!(base.as_map().get("antithesis.duration").unwrap(), "60");
        // Base-only value should be preserved
        assert_eq!(
            base.as_map().get("antithesis.description").unwrap(),
            "base description"
        );
        // Overlay-only value should be added
        assert_eq!(
            base.as_map().get("antithesis.report.recipients").unwrap(),
            "team@example.com"
        );
    }

    /// `unescape_pointer_token` is the exact inverse of RFC 6901 escaping
    /// (`~` → `~0`, `/` → `~1`). For *any* string — including ones already
    /// containing `~0`/`~1` sequences, the case the order-sensitive replacement
    /// is most likely to mangle — escaping then unescaping is the identity.
    #[hegel::test]
    fn unescape_reverses_rfc6901_escaping(tc: hegel::TestCase) {
        let raw = tc.draw(generators::text());
        // RFC 6901: `~` must be escaped before `/`, so a literal `/` doesn't
        // collide with the `~1` it produces.
        let escaped = raw.replace('~', "~0").replace('/', "~1");
        assert_eq!(unescape_pointer_token(&escaped), raw);
    }

    /// `merge` is a right-biased union: the result agrees, key for key, with a
    /// reference `HashMap` extended by the overlay. This is the model test for
    /// the merge operation — the overlay wins on conflicts, base-only keys
    /// survive, and no key is invented or dropped.
    #[hegel::test]
    fn merge_agrees_with_map_union(tc: hegel::TestCase) {
        let kv = || {
            generators::hashmaps(
                generators::text().max_size(12),
                generators::text().max_size(12),
            )
            .max_size(8)
        };
        let base_model = tc.draw(kv());
        let overlay_model = tc.draw(kv());

        let mut base = Params::new();
        for (k, v) in &base_model {
            base.insert(k.clone(), v.clone());
        }
        let mut overlay = Params::new();
        for (k, v) in &overlay_model {
            overlay.insert(k.clone(), v.clone());
        }
        base.merge(overlay);

        // Reference: a plain right-biased union.
        let mut model = base_model;
        model.extend(overlay_model);

        assert_eq!(base.as_map().len(), model.len());
        for (k, v) in &model {
            assert_eq!(
                base.as_map().get(k).and_then(Value::as_str),
                Some(v.as_str())
            );
        }
    }

    /// For any non-empty key without `=` and any value, which may contain `=`,
    /// the parsed pair round-trips exactly.
    #[hegel::test]
    fn key_value_pair_round_trips(tc: hegel::TestCase) {
        let key = tc.draw(generators::text().min_size(1).filter(|k| !k.contains('=')));
        let value = tc.draw(generators::text());
        let pair: KVPair = format!("{key}={value}")
            .parse()
            .expect("a non-empty key with a value must parse");
        let params = Params::from_iter([pair]);
        assert_eq!(params.as_map().len(), 1);
        assert_eq!(
            params.as_map().get(&key).and_then(Value::as_str),
            Some(value.as_str())
        );
    }
    /// Redaction preserves the exact key set and leaves every value either
    /// untouched or replaced by the marker — it never drops, adds, or reorders
    /// keys, and only the documented sensitive keys are masked.
    // A key suffix, not a file extension.
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    #[hegel::test]
    fn redaction_preserves_keys_and_masks_only_sensitive(tc: hegel::TestCase) {
        let model = tc.draw(
            generators::hashmaps(
                generators::text().max_size(20),
                generators::text().max_size(20),
            )
            .max_size(8),
        );
        let mut params = Params::new();
        for (k, v) in &model {
            params.insert(k.clone(), v.clone());
        }
        let redacted = params.to_redacted_map();

        assert_eq!(redacted.len(), model.len());
        for (k, original) in &model {
            // Oracle for sensitivity, kept independent of `is_sensitive_key`.
            let sensitive = k.ends_with(".token") || k == "antithesis.report.recipients";
            let expected = if sensitive {
                "[REDACTED]"
            } else {
                original.as_str()
            };
            assert_eq!(redacted.get(k).and_then(Value::as_str), Some(expected));
        }
    }

    fn validate_filter(pattern: &str) -> Result<()> {
        validate_filter_pattern(ANT_FILTER_LOGS_MATCHING, pattern, "every log line")
    }

    #[test]
    fn validate_filter_pattern_accepts_valid_patterns() {
        for pattern in ["debug", "(?i)error", "panic|fatal"] {
            assert!(
                validate_filter(pattern).is_ok(),
                "pattern should be accepted: {pattern}"
            );
        }
    }

    #[test]
    fn validate_filter_pattern_rejects_a_blank_pattern() {
        for pattern in ["", "  "] {
            let err = validate_filter(pattern).unwrap_err();
            assert!(
                err.to_string().contains("is empty"),
                "unexpected error for {pattern:?}: {err}"
            );
        }
    }

    #[test]
    fn validate_filter_pattern_limits_bytes_not_chars() {
        // The buffer is 1024 bytes including the NUL terminator, so the
        // limit is 1023 *bytes*: 512 two-byte chars (1024 bytes) must fail
        // even though the char count is far below the limit.
        let err = validate_filter(&"é".repeat(512)).unwrap_err();
        assert!(
            err.to_string().contains("1024 bytes (max 1023)"),
            "unexpected error: {err}"
        );

        assert!(validate_filter(&"a".repeat(1023)).is_ok());
    }

    #[test]
    fn validate_filter_pattern_rejects_an_invalid_regex() {
        let err = validate_filter("(").unwrap_err();
        assert!(
            err.to_string()
                .contains("is not a valid regular expression"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_filter_pattern_rejects_an_empty_string_match() {
        // These compile fine and then suppress every log line.
        for pattern in ["x*", "(foo)?"] {
            let err = validate_filter(pattern).unwrap_err();
            assert!(
                err.to_string().contains("matches the empty string"),
                "unexpected error for {pattern}: {err}"
            );
        }
    }

    #[test]
    fn validate_test_params_limits_fault_exclusions_to_1024_chars() {
        let params = |len| {
            Params::from_key_value_pairs([format!("{ANT_FAULT_EXCLUSIONS}={}", "é".repeat(len))])
                .unwrap()
        };
        assert!(params(1024).validate_test_params().is_ok());
        assert!(params(1025).validate_test_params().is_err());
    }

    /// For *any* input — however far from a regex — validation returns
    /// instead of panicking.
    #[hegel::test]
    fn filter_validation_never_panics(tc: hegel::TestCase) {
        let pattern = tc.draw(generators::text());
        let _ = validate_filter(&pattern);
        let _ = validate_fault_exclusions(&pattern);
    }

    #[test]
    fn redacted_map_hides_sensitive_values() {
        let github_token = format!("antithesis.integrations.github.token={FAKE_GITHUB_TOKEN}");
        let pairs = [
            "antithesis.duration=30",
            &github_token,
            "antithesis.integrations.github.callback_url=https://example.com/callback",
            "antithesis.report.recipients=user@example.com;other@example.com",
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        let redacted = params.to_redacted_map();

        // Non-sensitive values should be preserved
        assert_eq!(redacted.get("antithesis.duration").unwrap(), "30");
        assert_eq!(
            redacted
                .get("antithesis.integrations.github.callback_url")
                .unwrap(),
            "https://example.com/callback"
        );

        // Sensitive values should be redacted
        assert_eq!(
            redacted
                .get("antithesis.integrations.github.token")
                .unwrap(),
            "[REDACTED]"
        );
        assert_eq!(
            redacted.get("antithesis.report.recipients").unwrap(),
            "[REDACTED]"
        );
    }
}
