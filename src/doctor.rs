use std::env;

use color_eyre::eyre::Result;

use crate::container;
use crate::error::user_error;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    Ok,    // green ✓
    Warn,  // yellow ⚠ — surfaced, but does not fail doctor
    Error, // red ✗ — fails doctor
}

/// A single line in the doctor report: a status icon, a headline, and any
/// number of indented detail lines (e.g. `ERROR: ...`, `NOTE: ...`).
struct Check {
    status: Status,
    headline: String,
    notes: Vec<String>,
}

impl Check {
    fn new(status: Status, headline: impl Into<String>) -> Self {
        Self {
            status,
            headline: headline.into(),
            notes: Vec::new(),
        }
    }

    fn ok(headline: impl Into<String>) -> Self {
        Self::new(Status::Ok, headline)
    }

    fn warn(headline: impl Into<String>) -> Self {
        Self::new(Status::Warn, headline)
    }

    fn error(headline: impl Into<String>) -> Self {
        Self::new(Status::Error, headline)
    }

    /// Attach an indented detail line, returning `self` for chaining.
    fn note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    fn print(&self) {
        let icon = match self.status {
            Status::Ok => console::style("✓").green(),
            Status::Warn => console::style("⚠").yellow(),
            Status::Error => console::style("✗").red(),
        };
        eprintln!("  {} {}", icon, self.headline);
        for note in &self.notes {
            eprintln!("      {note}");
        }
    }
}

fn env_set(name: &str) -> bool {
    env::var(name).is_ok_and(|v| !v.is_empty())
}

/// A plain "is set / is not set" check for a single required env var.
fn env_check(name: &str) -> Check {
    if env_set(name) {
        Check::ok(format!("{name} is set"))
    } else {
        Check::error(format!("{name} is not set"))
    }
}

/// Authentication checks. snouty authenticates with an API key, which grants
/// access to the full Antithesis API. Username/password is a legacy fallback
/// that only works for `snouty launch` and `snouty debug`, so it never stands
/// in for a missing API key — it only softens the missing-key error into a
/// warning.
///
/// Pure over the three booleans so it can be unit-tested without touching the
/// process environment.
fn auth_checks(api_key: bool, username: bool, password: bool) -> Vec<Check> {
    if api_key {
        return vec![Check::ok("ANTITHESIS_API_KEY is set")];
    }

    // Legacy auth needs BOTH halves; a lone username or password is not usable.
    if username && password {
        return vec![
            Check::warn("ANTITHESIS_API_KEY is not set")
                .note("WARNING: `snouty runs` and other API commands require ANTITHESIS_API_KEY"),
            Check::ok("ANTITHESIS_USERNAME & ANTITHESIS_PASSWORD are set")
                .note(
                    "NOTE: username/password is legacy auth — only `snouty launch` and \
                     `snouty debug` accept it",
                )
                .note("NOTE: set ANTITHESIS_API_KEY for full API access"),
        ];
    }

    vec![
        Check::error("ANTITHESIS_API_KEY is not set")
            .note("ERROR: snouty requires an API key to authenticate with Antithesis"),
    ]
}

pub fn cmd_doctor() -> Result<()> {
    let mut checks: Vec<Check> = Vec::new();

    // Container runtime (for building/pushing images)
    match container::runtime() {
        Ok(rt) => checks.push(Check::ok(format!(
            "Container runtime: {} detected",
            rt.name()
        ))),
        Err(e) => checks.push(Check::error(format!("Container runtime: {e}"))),
    }

    // Docker Compose v2 (required for compose configs)
    match container::docker_compose_version() {
        Ok(version) => checks.push(Check::ok(format!("docker-compose: {version}"))),
        Err(e) => checks.push(Check::error(format!("docker-compose: {e}"))),
    }

    // Required environment
    checks.push(env_check("ANTITHESIS_TENANT"));
    checks.push(env_check("ANTITHESIS_REPOSITORY"));

    // Authentication
    checks.extend(auth_checks(
        env_set("ANTITHESIS_API_KEY"),
        env_set("ANTITHESIS_USERNAME"),
        env_set("ANTITHESIS_PASSWORD"),
    ));

    // Print all checks and check for failures
    for check in &checks {
        check.print();
    }

    eprintln!();
    if checks.iter().any(|c| c.status == Status::Error) {
        return Err(user_error("doctor found problems"));
    }
    let warnings = checks.iter().filter(|c| c.status == Status::Warn).count();
    if warnings > 0 {
        let plural = if warnings == 1 { "" } else { "s" };
        eprintln!("All required checks passed ({warnings} warning{plural})");
    } else {
        eprintln!("All checks passed");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_api_key_set_is_single_ok_check() {
        let checks = auth_checks(true, false, false);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Ok);
        assert!(checks[0].headline.contains("ANTITHESIS_API_KEY is set"));
    }

    #[test]
    fn auth_api_key_wins_and_ignores_basic_creds() {
        // An API key takes precedence; username/password is not even mentioned.
        let checks = auth_checks(true, true, true);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Ok);
        assert!(!checks[0].headline.contains("USERNAME"));
    }

    #[test]
    fn auth_legacy_basic_warns_on_key_and_notes_legacy() {
        let checks = auth_checks(false, true, true);
        assert_eq!(checks.len(), 2);

        // Missing API key is a warning, not a failure: launch/debug still work.
        assert_eq!(checks[0].status, Status::Warn);
        assert!(checks[0].headline.contains("ANTITHESIS_API_KEY is not set"));

        // The legacy creds get their own passing check with explanatory notes
        // that steer the user back toward an API key.
        assert_eq!(checks[1].status, Status::Ok);
        let notes = checks[1].notes.join(" ");
        assert!(notes.contains("legacy"));
        assert!(notes.contains("snouty launch"));
        assert!(notes.contains("ANTITHESIS_API_KEY"));
    }

    #[test]
    fn auth_nothing_set_errors_and_only_mentions_api_key() {
        let checks = auth_checks(false, false, false);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Error);
        assert!(checks[0].headline.contains("ANTITHESIS_API_KEY is not set"));

        let notes = checks[0].notes.join(" ");
        assert!(notes.contains("requires an API key"));
        // Nothing-set must steer to the API key only — no username/password noise.
        assert!(!checks[0].headline.contains("USERNAME"));
        assert!(!notes.contains("USERNAME"));
        assert!(!notes.contains("PASSWORD"));
    }

    #[test]
    fn auth_partial_basic_errors_like_nothing_set() {
        for (username, password) in [(true, false), (false, true)] {
            let checks = auth_checks(false, username, password);
            assert_eq!(checks.len(), 1);
            assert_eq!(checks[0].status, Status::Error);
            assert!(checks[0].headline.contains("ANTITHESIS_API_KEY is not set"));
        }
    }
}
