//! User-facing error tagging.
//!
//! Most failures snouty hits are the user's to fix: a bad flag, a missing
//! environment variable, a 4xx from the API. Those should print as a clean
//! message, not a color_eyre report with a "Backtrace omitted" footer. Wrapping
//! such errors in [`UserError`] lets `main` tell them apart from genuine
//! internal faults (which still get the full report).

use color_eyre::eyre::Report;

/// Marker error wrapping a user-facing message.
///
/// Construct reports with [`user_error`] and detect them with [`is_user_error`].
#[derive(Debug)]
pub struct UserError(pub String);

impl std::fmt::Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UserError {}

/// Build an `eyre` report that `main` will print as a clean user-facing message.
pub fn user_error(message: impl Into<String>) -> Report {
    Report::new(UserError(message.into()))
}

/// Returns `true` when any link in the report's chain is a [`UserError`].
///
/// Works through `wrap_err` context so callers can add context to a user error
/// without losing the tag.
pub fn is_user_error(report: &Report) -> bool {
    report.chain().any(|cause| cause.is::<UserError>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_report_is_detected() {
        let report = user_error("missing environment variable: ANTITHESIS_TENANT");
        assert!(is_user_error(&report));
    }

    #[test]
    fn tag_survives_added_context() {
        let report = user_error("API error: 400").wrap_err("while listing runs");
        assert!(is_user_error(&report));
        // Alternate Display renders the full chain without a backtrace footer.
        let rendered = format!("{report:#}");
        assert!(rendered.contains("while listing runs"));
        assert!(rendered.contains("API error: 400"));
    }

    #[test]
    fn plain_report_is_not_user_error() {
        let report = color_eyre::eyre::eyre!("internal explosion");
        assert!(!is_user_error(&report));
    }
}
