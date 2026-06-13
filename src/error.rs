//! User-facing errors.
//!
//! Most failures snouty hits are the user's to fix: a bad flag, a missing
//! environment variable, a 4xx from the API. Those should print as a clean
//! message, not a color_eyre report with a backtrace footer. We mark such
//! errors with [`Report::suppress_backtrace`] at the point they're built, so
//! `main` can render every error the same way and the report itself carries
//! whether it's worth a backtrace — no out-of-band classification needed.

use color_eyre::Section;
use color_eyre::eyre::Report;

/// Marker error wrapping a user-facing message.
///
/// Construct reports with [`user_error`].
#[derive(Debug)]
pub struct UserError(pub String);

impl std::fmt::Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UserError {}

/// Build an `eyre` report that prints as a clean user-facing message — no
/// backtrace, even under `RUST_BACKTRACE`. Attach `.note(...)`/`.suggestion(...)`
/// (the [`color_eyre::Section`] API) for follow-up hints; they render below the
/// message automatically.
pub fn user_error(message: impl Into<String>) -> Report {
    Report::new(UserError(message.into())).suppress_backtrace(true)
}

/// Error carrying the HTTP status of a failed API call structurally, so callers
/// can classify failures (e.g. "was this a 404?") without sniffing the rendered
/// message string. The `Display` text is the human-facing message built by the
/// api layer; the `status` is the raw HTTP status code.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ApiError {}

/// Returns the HTTP status of the first [`ApiError`] in the report's chain, if
/// any. Works through `wrap_err` context, so callers can add context without
/// losing the structured status.
pub fn api_error_status(report: &Report) -> Option<u16> {
    report
        .chain()
        .find_map(|cause| cause.downcast_ref::<ApiError>().map(|e| e.status))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_error_message_survives_added_context() {
        let report = user_error("API error: 400").wrap_err("while listing runs");
        // Alternate Display renders the full chain without a backtrace footer.
        let rendered = format!("{report:#}");
        assert!(rendered.contains("while listing runs"));
        assert!(rendered.contains("API error: 400"));
    }

    fn api_error(status: u16, message: &str) -> Report {
        Report::new(ApiError {
            status,
            message: message.to_string(),
        })
    }

    #[test]
    fn api_error_status_reads_structured_status() {
        let report = api_error(404, "API error: 404 Not Found — run missing");
        assert_eq!(api_error_status(&report), Some(404));
    }

    #[test]
    fn api_error_status_ignores_digits_in_the_message() {
        // A 500 whose body happens to mention 404 (proxy error pages routinely
        // do) must classify by its real status, not by string-sniffing.
        let report = api_error(500, "API error: 500 — upstream returned 404 page");
        assert_eq!(api_error_status(&report), Some(500));
    }

    #[test]
    fn api_error_status_survives_added_context() {
        let report = api_error(404, "run missing").wrap_err("while listing properties");
        assert_eq!(api_error_status(&report), Some(404));
    }

    #[test]
    fn api_error_status_is_none_for_plain_report() {
        let report = color_eyre::eyre::eyre!("internal explosion");
        assert_eq!(api_error_status(&report), None);
    }
}
