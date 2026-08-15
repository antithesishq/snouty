use jsonschema::Validator;
use jsonschema::error::ValidationErrorKind;
use log::debug;
use serde_json::{Map, Value};

use color_eyre::Section;
use color_eyre::eyre::Result;

use crate::error::user_error;
use crate::vtime::{ParseVTimeError, VTime};

const SCHEMA: &str = include_str!("params_schema.json");

// Launch-parameter key names.
//
// These are the wire names the API accepts (see params_schema.json). Production
// code references these constants so a typo fails to compile; tests that pin
// the wire format keep the raw literals on purpose.
pub const KEY_TEST_NAME: &str = "antithesis.test_name";
pub const KEY_DESCRIPTION: &str = "antithesis.description";
pub const KEY_DURATION: &str = "antithesis.duration";
pub const KEY_CONFIG_IMAGE: &str = "antithesis.config_image";
pub const KEY_IMAGES: &str = "antithesis.images";
pub const KEY_SOURCE: &str = "antithesis.source";
pub const KEY_IS_EPHEMERAL: &str = "antithesis.is_ephemeral";
pub const KEY_REPORT_RECIPIENTS: &str = "antithesis.report.recipients";
pub const KEY_EVENT_DESCRIPTION: &str = "antithesis.event_description";
pub const KEY_FILTER_LOGS_MATCHING: &str = "antithesis.filter_logs_matching";

/// Prefix for the `antithesis.debugging.*` family. Moment.from parsing
/// synthesizes keys from it; the dedicated constants below name the members
/// production code addresses directly.
pub const DEBUGGING_KEY_PREFIX: &str = "antithesis.debugging.";
pub const KEY_DEBUGGING_SESSION_ID: &str = "antithesis.debugging.session_id";
pub const KEY_DEBUGGING_RUN_ID: &str = "antithesis.debugging.run_id";
pub const KEY_DEBUGGING_INPUT_HASH: &str = "antithesis.debugging.input_hash";
pub const KEY_DEBUGGING_VTIME: &str = "antithesis.debugging.vtime";

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

    /// Parse params from `key=value` pairs.
    ///
    /// Values may contain `=` (only the first `=` is used as the delimiter).
    /// Errors on missing `=` or empty key.
    pub fn from_key_value_pairs<I, S>(pairs: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut inner = Map::new();
        for pair in pairs {
            let pair = pair.as_ref();
            let (key, value) = pair.split_once('=').ok_or_else(|| {
                user_error(format!(
                    "invalid parameter: expected key=value, got: {pair}"
                ))
            })?;
            if key.is_empty() {
                return Err(user_error(format!(
                    "invalid parameter: empty key in: {pair}"
                )));
            }
            inner.insert(key.to_string(), Value::String(value.to_string()));
        }
        Ok(Self { inner })
    }

    /// Create params from a JSON value.
    ///
    /// The value must be a JSON object. A `{"params": {...}}` envelope (the
    /// shape of the API request body) is unwrapped so users can paste a
    /// captured request body back as input.
    pub fn from_json(value: &Value) -> Result<Self> {
        let obj = value
            .as_object()
            .ok_or_else(|| user_error("invalid arguments: expected JSON object"))?;

        let inner = if obj.len() == 1
            && let Some(Value::Object(nested)) = obj.get("params")
        {
            debug!("unwrapping {{\"params\": ...}} envelope");
            nested.clone()
        } else {
            obj.clone()
        };

        debug!("parsed {} params from JSON", inner.len());
        Ok(Self { inner })
    }

    /// Validate params against the test params schema.
    pub fn validate_test_params(&self) -> Result<()> {
        validate_against_def(&self.inner, "testParams")
    }

    /// Validate params against the debugging params schema.
    pub fn validate_debugging_params(&self) -> Result<()> {
        validate_against_def(&self.inner, "debuggingParams")
    }

    /// Rewrite `antithesis.debugging.vtime` as the exact print of the
    /// [`VTime`] it names, and reject one that isn't a vtime at all.
    ///
    /// A debug moment arrives by several routes — typed flags, a `Moment.from`
    /// on stdin, raw JSON on stdin — which spell the same moment differently
    /// (`402`, `402.0`, `"402"`). Normalizing once, after those routes have
    /// merged, is what makes them agree on the text sent to the API. Absent is
    /// fine: a missing required vtime is the schema's error to report.
    pub fn normalize_debug_vtime(&mut self) -> Result<()> {
        let key = KEY_DEBUGGING_VTIME;
        let Some(value) = self.inner.get(key) else {
            return Ok(());
        };
        let vtime = VTime::from_json(value).ok_or_else(|| {
            user_error(format!("invalid arguments: {ParseVTimeError}"))
                .note(format!("{key} = {value}"))
        })?;
        self.insert(key, vtime.to_string());
        Ok(())
    }

    /// Ensure the debugging target run is identified by exactly one of
    /// `antithesis.debugging.run_id` (preferred) or
    /// `antithesis.debugging.session_id`.
    ///
    /// The MVD launch API models its params as a `oneOf` over these two
    /// identifiers, so supplying both — or neither — is an error. This is
    /// checked here, after schema validation, so a missing `input_hash`/`vtime`
    /// surfaces first and the identifier error gets a tailored message.
    pub fn ensure_single_debug_target(&self) -> Result<()> {
        let has_run_id = self.inner.contains_key(KEY_DEBUGGING_RUN_ID);
        let has_session_id = self.inner.contains_key(KEY_DEBUGGING_SESSION_ID);
        match (has_run_id, has_session_id) {
            (true, false) | (false, true) => Ok(()),
            (true, true) => Err(user_error("specify exactly one of --run-id / --session-id")),
            (false, false) => Err(user_error("specify --run-id or --session-id")),
        }
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

fn is_sensitive_key(key: &str) -> bool {
    key.ends_with(".token") || key == KEY_REPORT_RECIPIENTS
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
        debug!("  - {}", note);
        report = report.note(note);
    }
    Err(report)
}

/// Collect the human-readable notes for every validation error, with the
/// misleading `unevaluatedProperties` cascade filtered out. Returns an empty
/// vec when the params are valid.
///
/// `unevaluatedProperties: false` reports every sibling property as
/// "unexpected" whenever a more specific subschema (e.g. the `duration`
/// pattern) fails: once the failing subschema is dropped, its properties are
/// left unevaluated and the catch-all fires on all of them. Those properties
/// are usually valid and required, so the note sends readers chasing fields
/// that aren't actually wrong. When a more specific violation already fired,
/// we drop the cascade and surface only the real error(s).
fn collect_validation_notes(params: &Map<String, Value>, def_name: &str) -> Vec<String> {
    let schema: Value = serde_json::from_str(SCHEMA).expect("valid schema");

    // Build a schema that references the specific definition
    let def_schema = serde_json::json!({
        "$ref": format!("#/$defs/{}", def_name),
        "$defs": schema["$defs"]
    });

    let validator = Validator::new(&def_schema).expect("valid schema");
    let instance = Value::Object(params.clone());

    let errors: Vec<_> = validator.iter_errors(&instance).collect();
    let is_cascade = |e: &jsonschema::ValidationError| {
        matches!(e.kind(), ValidationErrorKind::UnevaluatedProperties { .. })
    };
    let has_specific_error = errors.iter().any(|e| !is_cascade(e));

    errors
        .iter()
        .filter(|e| !(has_specific_error && is_cascade(e)))
        .map(note_for_error)
        .collect()
}

/// Render a single validation error as a note, prefixed with the offending
/// field name when the error is attributable to a specific property.
///
/// The jsonschema messages describe only the value and the violated
/// constraint (e.g. `"asdf" does not match "^[0-9]+(\.[0-9]+)?$"`); they never
/// name the field. The field lives in the error's instance path, so we pull it
/// from there and put it up front. Object-level errors (e.g. unevaluated
/// properties) carry an empty instance path and already name the property in
/// their message, so they pass through unprefixed.
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
    use serde_json::json;

    fn debug_vtime(params: &Params) -> Option<&str> {
        params
            .as_map()
            .get("antithesis.debugging.vtime")
            .and_then(Value::as_str)
    }

    #[test]
    fn normalize_debug_vtime_agrees_across_spellings() {
        // Every spelling of the same moment — integer, decimal, quoted, and a
        // JSON number straight off stdin — sends identical text.
        for value in [json!(402), json!(402.0), json!("402"), json!("402.0")] {
            let mut params =
                Params::from_json(&json!({"antithesis.debugging.vtime": value})).unwrap();
            params.normalize_debug_vtime().unwrap();
            assert_eq!(debug_vtime(&params), Some("402.0"));
        }

        // Full precision survives untouched — the property the type exists for.
        let mut params =
            Params::from_json(&json!({"antithesis.debugging.vtime": "329.8037810830865"})).unwrap();
        params.normalize_debug_vtime().unwrap();
        assert_eq!(debug_vtime(&params), Some("329.8037810830865"));
    }

    #[test]
    fn normalize_debug_vtime_rejects_a_non_vtime() {
        for value in [json!("oops"), json!("NaN"), json!(true), json!(null)] {
            let mut params =
                Params::from_json(&json!({"antithesis.debugging.vtime": value})).unwrap();
            let err = format!("{:?}", params.normalize_debug_vtime().unwrap_err());
            assert!(
                err.contains("vtime is not a finite decimal number")
                    && err.contains("antithesis.debugging.vtime"),
                "unexpected error for {value}: {err}"
            );
        }
    }

    #[test]
    fn normalize_debug_vtime_leaves_an_absent_vtime_to_the_schema() {
        // A missing required vtime is the schema's error to report, not this
        // function's, so normalizing without one must succeed.
        let mut params = Params::from_json(&json!({"antithesis.debugging.run_id": "r-1"})).unwrap();
        params.normalize_debug_vtime().unwrap();
        assert_eq!(debug_vtime(&params), None);
        assert!(params.validate_debugging_params().is_err());
    }

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
            "antithesis.integrations.github.callback_url=https://github.com/cb",
            "antithesis.integrations.github.token=secret",
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
            "secret"
        );
    }

    #[test]
    fn validate_test_params_success() {
        let pairs = ["antithesis.duration=30", "antithesis.is_ephemeral=true"];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_test_params().is_ok());
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
    fn invalid_duration_does_not_cascade_into_unevaluated_properties() {
        // A bad `antithesis.duration` value used to drag every sibling property
        // into a misleading "Unevaluated properties are not allowed" note. The
        // only real error is the duration pattern; the siblings are valid.
        let params = Params::from_key_value_pairs([
            "antithesis.duration=15m",
            "antithesis.test_name=my-test",
            "antithesis.source=my-source",
        ])
        .unwrap();

        // The cascade is surfaced as color_eyre notes, not in the report's
        // Display, so assert on the collected notes directly.
        let notes = collect_validation_notes(params.as_map(), "testParams");
        assert_eq!(
            notes.len(),
            1,
            "exactly one real error expected, got: {notes:?}"
        );
        // The real, specific failure is reported, and the note names the
        // offending field so the reader knows where to look.
        assert!(
            notes[0].contains("antithesis.duration") && notes[0].contains("15m"),
            "expected field-attributed duration note, got: {notes:?}"
        );
        // ...but no cascade naming the valid siblings.
        assert!(
            notes.iter().all(|n| !n.contains("Unevaluated properties")),
            "cascade should be suppressed, got: {notes:?}"
        );
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
    fn validate_debugging_params_rejects_custom_props() {
        let pairs = [
            "antithesis.debugging.input_hash=abc123",
            "antithesis.debugging.session_id=sess-456",
            "antithesis.debugging.vtime=123",
            "my.custom.prop=value",
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_debugging_params().is_err());
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
        // The exactly-one-of rule is enforced by ensure_single_debug_target, not
        // the schema: schema validation only requires input_hash + vtime.
        let pairs = [
            "antithesis.debugging.input_hash=abc123",
            "antithesis.debugging.vtime=1234567890",
        ];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert!(params.validate_debugging_params().is_ok());
    }

    #[test]
    fn ensure_single_debug_target_accepts_run_id_only() {
        let mut params = Params::new();
        params.insert("antithesis.debugging.run_id", "run-1");
        assert!(params.ensure_single_debug_target().is_ok());
    }

    #[test]
    fn ensure_single_debug_target_accepts_session_id_only() {
        let mut params = Params::new();
        params.insert("antithesis.debugging.session_id", "sess-1");
        assert!(params.ensure_single_debug_target().is_ok());
    }

    #[test]
    fn ensure_single_debug_target_rejects_both() {
        let mut params = Params::new();
        params.insert("antithesis.debugging.run_id", "run-1");
        params.insert("antithesis.debugging.session_id", "sess-1");
        let err = params.ensure_single_debug_target().unwrap_err().to_string();
        assert!(err.contains("specify exactly one of --run-id / --session-id"));
    }

    #[test]
    fn ensure_single_debug_target_rejects_neither() {
        let mut params = Params::new();
        params.insert("antithesis.debugging.input_hash", "abc");
        let err = params.ensure_single_debug_target().unwrap_err().to_string();
        assert!(err.contains("specify --run-id or --session-id"));
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

    #[test]
    fn from_key_value_pairs_valid() {
        let pairs = ["antithesis.duration=30", "my.key=hello"];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert_eq!(params.as_map().get("antithesis.duration").unwrap(), "30");
        assert_eq!(params.as_map().get("my.key").unwrap(), "hello");
    }

    #[test]
    fn from_key_value_pairs_value_containing_equals() {
        let pairs = ["key=value=with=equals"];
        let params = Params::from_key_value_pairs(pairs).unwrap();
        assert_eq!(params.as_map().get("key").unwrap(), "value=with=equals");
    }

    #[test]
    fn from_key_value_pairs_missing_equals() {
        let pairs = ["noequals"];
        let result = Params::from_key_value_pairs(pairs);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expected key=value")
        );
    }

    #[test]
    fn from_key_value_pairs_empty_key() {
        let pairs = ["=value"];
        let result = Params::from_key_value_pairs(pairs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty key"));
    }

    #[test]
    fn from_json_unwraps_params_envelope() {
        // Users sometimes paste in the `{"params": {...}}` shape of the API
        // request body. Unwrap it so the real keys land in the params map
        // (and so validation runs against the right thing).
        let input = serde_json::json!({
            "params": {
                "antithesis.description": "test run",
                "antithesis.duration": "0.05",
                "antithesis.is_ephemeral": "true",
                "antithesis.report.recipients": "user@example.com",
                "antithesis.source": "example",
            }
        });

        let params = Params::from_json(&input).unwrap();

        assert_eq!(params.as_map().get("antithesis.duration").unwrap(), "0.05");
        assert_eq!(params.as_map().len(), 5);
        params.validate_test_params().unwrap();
    }

    #[test]
    fn from_json_does_not_unwrap_string_params_key() {
        // A user property literally named "params" with a string value is
        // not the envelope shape and must be preserved as-is.
        let input = serde_json::json!({ "params": "literal" });
        let params = Params::from_json(&input).unwrap();
        assert_eq!(params.as_map().get("params").unwrap(), "literal");
    }

    #[test]
    fn from_json_does_not_unwrap_when_siblings_present() {
        // If the top level has siblings alongside "params", don't guess —
        // leave the structure alone.
        let input = serde_json::json!({
            "params": { "antithesis.duration": "30" },
            "antithesis.source": "example",
        });
        let params = Params::from_json(&input).unwrap();
        assert!(params.as_map().contains_key("params"));
        assert!(params.as_map().contains_key("antithesis.source"));
    }

    use hegel::generators::{self, Generator};

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

    /// `from_key_value_pairs` splits on the *first* `=` only, so the value may
    /// itself contain `=` and must come back verbatim. For any non-empty key
    /// without `=` and any value, the parsed pair round-trips exactly.
    #[hegel::test]
    fn key_value_pair_round_trips(tc: hegel::TestCase) {
        let key = tc.draw(generators::text().min_size(1).filter(|k| !k.contains('=')));
        let value = tc.draw(generators::text());
        let params = Params::from_key_value_pairs([format!("{key}={value}")])
            .expect("a non-empty key with a value must parse");
        assert_eq!(params.as_map().len(), 1);
        assert_eq!(
            params.as_map().get(&key).and_then(Value::as_str),
            Some(value.as_str())
        );
    }

    /// Redaction preserves the exact key set and leaves every value either
    /// untouched or replaced by the marker — it never drops, adds, or reorders
    /// keys, and only the documented sensitive keys are masked.
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

    #[test]
    fn redacted_map_hides_sensitive_values() {
        let pairs = [
            "antithesis.duration=30",
            "antithesis.integrations.github.token=secret_token_123",
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
