//! Reading environment variables with snouty's "empty means unset" policy.

use std::env::{self, VarError};

use color_eyre::eyre::{Result, eyre};

/// Read environment variable `key`, treating it as **unset** when it is either
/// absent *or* present but empty.
///
/// An exported-but-empty value (`FOO=`) is common in CI and wrapper shells,
/// where it almost always means "not configured" rather than "configured to the
/// empty string". For every variable snouty reads — settings, auth, paths — the
/// empty string is never a meaningful value, so collapsing those two cases here
/// keeps that policy in exactly one place. Read all snouty environment
/// variables through this function so the rule can't drift.
///
/// **Contract:** an empty value is reported as `None` (unset). If snouty ever
/// gains a variable for which the empty string is a *meaningful* value, do not
/// reach for this function — read it directly with [`std::env::var`] (or
/// [`std::env::var_os`]) so the empty-as-unset collapse doesn't silently swallow
/// it.
///
/// A value that is present but not valid Unicode is an error.
pub(crate) fn var(key: &str) -> Result<Option<String>> {
    interpret(key, env::var(key))
}

/// The empty-as-unset policy, factored out of the environment read so it can be
/// unit-tested without mutating process-global state (which would race other
/// tests under threaded `cargo test`).
fn interpret(key: &str, raw: std::result::Result<String, VarError>) -> Result<Option<String>> {
    match raw {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(eyre!(
            "the value of environment variable {key} was not valid Unicode"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_variable_is_unset() {
        assert!(matches!(
            interpret("X", Err(VarError::NotPresent)),
            Ok(None)
        ));
    }

    #[test]
    fn empty_variable_is_treated_as_unset() {
        assert!(matches!(interpret("X", Ok(String::new())), Ok(None)));
    }

    #[test]
    fn present_variable_yields_its_value() {
        assert_eq!(
            interpret("X", Ok("value".to_string())).unwrap().as_deref(),
            Some("value")
        );
    }

    #[test]
    fn non_unicode_variable_is_an_error() {
        let err = interpret("MYVAR", Err(VarError::NotUnicode("bad".into()))).unwrap_err();
        assert!(err.to_string().contains("MYVAR"));
    }
}
