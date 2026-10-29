//! API key credentials, from `ANTITHESIS_API_KEY` or `snouty login`.

pub(crate) const API_KEY_VAR_NAME: &str = "ANTITHESIS_API_KEY";

/// Whether `api_key` is empty or only whitespace. Such a key cannot
/// authenticate.
pub(crate) fn is_blank_api_key(api_key: &str) -> bool {
    api_key.trim().is_empty()
}
