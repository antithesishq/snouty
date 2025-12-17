//! Parser for the Moment.from format used by Antithesis triage reports.

use log::debug;
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::params::Params;

/// Parse a Moment.from format string into Params.
///
/// The format is: `Moment.from({ session_id: "...", input_hash: "...", vtime: 123.456 })`
///
/// This is JSON5 object syntax with unquoted keys. The keys are converted
/// to `antithesis.debugging.*` format and numeric values are converted to strings.
pub fn parse(input: &str) -> Result<Params> {
    let input = input.trim();

    if !input.starts_with("Moment.from(") || !input.ends_with(")") {
        return Err(Error::InvalidArgs(
            "expected Moment.from({ ... }) format".to_string(),
        ));
    }

    let inner = &input[12..input.len() - 1];
    debug!("parsing Moment.from inner: {}", inner);

    let value: Value = json5::from_str(inner)
        .map_err(|e| Error::InvalidArgs(format!("invalid Moment.from format: {}", e)))?;

    let obj = value
        .as_object()
        .ok_or_else(|| Error::InvalidArgs("Moment.from must contain an object".to_string()))?;

    // Convert keys to antithesis.debugging.* format
    let mut map = Map::new();
    for (key, val) in obj {
        let new_key = format!("antithesis.debugging.{}", key);
        debug!("converting key {} -> {}", key, new_key);
        // Convert numbers to strings
        let string_val = match val {
            Value::Number(n) => Value::String(n.to_string()),
            other => other.clone(),
        };
        map.insert(new_key, string_val);
    }

    Params::from_json(&Value::Object(map))
}

/// Check if input looks like a Moment.from format.
pub fn is_moment_format(input: &str) -> bool {
    let input = input.trim();
    input.starts_with("Moment.from(") && input.ends_with(")")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_moment_format_detects_valid() {
        assert!(is_moment_format("Moment.from({ foo: 1 })"));
        assert!(is_moment_format("  Moment.from({ foo: 1 })  "));
    }

    #[test]
    fn is_moment_format_rejects_invalid() {
        assert!(!is_moment_format("{}"));
        assert!(!is_moment_format("Moment.from("));
        assert!(!is_moment_format("{ foo: 1 }"));
    }

    #[test]
    fn parse_simple() {
        let input = r#"Moment.from({ session_id: "abc123" })"#;
        let params = parse(input).unwrap();

        assert_eq!(
            params.as_map().get("antithesis.debugging.session_id"),
            Some(&Value::String("abc123".to_string()))
        );
    }

    #[test]
    fn parse_full_moment() {
        let input = r#"Moment.from({ session_id: "f89d5c11f5e3bf5e4bb3641809800cee-44-22", input_hash: "6057726200491963783", vtime: 329.8037810830865 })"#;
        let params = parse(input).unwrap();

        assert_eq!(
            params.as_map().get("antithesis.debugging.session_id"),
            Some(&Value::String(
                "f89d5c11f5e3bf5e4bb3641809800cee-44-22".to_string()
            ))
        );
        assert_eq!(
            params.as_map().get("antithesis.debugging.input_hash"),
            Some(&Value::String("6057726200491963783".to_string()))
        );
        // Numbers are converted to strings
        assert_eq!(
            params.as_map().get("antithesis.debugging.vtime"),
            Some(&Value::String("329.8037810830865".to_string()))
        );
    }

    #[test]
    fn parse_converts_numbers_to_strings() {
        let input = r#"Moment.from({ count: 42, ratio: 3.14 })"#;
        let params = parse(input).unwrap();

        assert_eq!(
            params.as_map().get("antithesis.debugging.count"),
            Some(&Value::String("42".to_string()))
        );
        assert_eq!(
            params.as_map().get("antithesis.debugging.ratio"),
            Some(&Value::String("3.14".to_string()))
        );
    }

    #[test]
    fn parse_rejects_invalid_format() {
        assert!(parse("{}").is_err());
        assert!(parse("not moment format").is_err());
        assert!(parse("Moment.from(").is_err());
    }

    #[test]
    fn parse_rejects_invalid_js() {
        assert!(parse("Moment.from({ invalid })").is_err());
    }
}
