use std::env;

use color_eyre::eyre::Result;
use serde::Serialize;

use crate::api::{AntithesisApi, ApiVersion, VersionError};
use crate::container;

/// Outcome of a single check. Only `Error` fails doctor; `Warn` is surfaced but
/// the run still passes.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Ok,
    Warn,
    Error,
}

/// Severity of an explanatory line printed under a check. Independent of the
/// check's `Status`: an `Ok` check can still carry `Note`s (what a var does),
/// and a failing check pairs its `Error`/`Warning` line with `Note` how-tos.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum Level {
    Note,
    Warning,
    Error,
}

impl Level {
    fn label(self) -> &'static str {
        match self {
            Level::Note => "NOTE",
            Level::Warning => "WARNING",
            Level::Error => "ERROR",
        }
    }
}

#[derive(Serialize)]
struct Note {
    level: Level,
    text: String,
}

/// One line in the doctor report. The headline `message` states the bare fact
/// ("ANTITHESIS_API_KEY is not set"); the `notes` carry every explanation of
/// what the variable does, what it's required for, and what to do about it. The
/// `name` is a stable machine key for `--json` consumers.
#[derive(Serialize)]
struct Check {
    name: &'static str,
    status: Status,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    notes: Vec<Note>,
}

impl Check {
    fn new(name: &'static str, status: Status, message: impl Into<String>) -> Self {
        Self {
            name,
            status,
            message: message.into(),
            notes: Vec::new(),
        }
    }

    fn ok(name: &'static str, message: impl Into<String>) -> Self {
        Self::new(name, Status::Ok, message)
    }

    fn warn(name: &'static str, message: impl Into<String>) -> Self {
        Self::new(name, Status::Warn, message)
    }

    fn fail(name: &'static str, message: impl Into<String>) -> Self {
        Self::new(name, Status::Error, message)
    }

    /// Attach an explanatory line, returning `self` for chaining. Prefer several
    /// short, independent notes over one bundled sentence.
    fn note(mut self, level: Level, text: impl Into<String>) -> Self {
        self.notes.push(Note {
            level,
            text: text.into(),
        });
        self
    }

    fn print(&self) {
        let icon = match self.status {
            Status::Ok => console::style("✓").green(),
            Status::Warn => console::style("⚠").yellow(),
            Status::Error => console::style("✗").red(),
        };
        eprintln!("  {} {}", icon, self.message);
        for note in &self.notes {
            let label = match note.level {
                Level::Note => console::style(note.level.label()).dim(),
                Level::Warning => console::style(note.level.label()).yellow(),
                Level::Error => console::style(note.level.label()).red(),
            };
            eprintln!("      {}: {}", label, note.text);
        }
    }
}

/// The full doctor report, as emitted by `--json`.
#[derive(Serialize)]
struct Report<'a> {
    ok: bool,
    checks: &'a [Check],
}

fn env_set(name: &str) -> bool {
    env::var(name).is_ok_and(|v| !v.is_empty())
}

fn presence(var: &str, set: bool) -> String {
    format!("{var} {}", if set { "is set" } else { "is not set" })
}

/// `ANTITHESIS_TENANT` is required by every command, so a missing one is a hard
/// failure.
fn tenant_check(set: bool) -> Check {
    let message = presence("ANTITHESIS_TENANT", set);
    if set {
        Check::ok("tenant", message)
    } else {
        Check::fail("tenant", message).note(
            Level::Note,
            "your Antithesis tenant, required by every command",
        )
    }
}

/// `ANTITHESIS_REPOSITORY` is only needed to build and push a config image
/// (`snouty launch --config`), so a missing one is a warning, not a failure —
/// read-only use (`snouty runs`, `snouty debug`) doesn't need it.
fn repository_check(set: bool) -> Check {
    let message = presence("ANTITHESIS_REPOSITORY", set);
    if set {
        Check::ok("repository", message)
    } else {
        Check::warn("repository", message)
            .note(Level::Note, "container registry for pushing images")
            .note(Level::Note, "only required to launch with --config")
    }
}

/// Authentication checks. snouty authenticates with an API key, which grants
/// the full Antithesis API. Username/password is legacy auth accepted only by
/// `snouty launch` and `snouty debug`, so it never stands in for a missing API
/// key — it only softens the missing-key failure into a warning.
///
/// Pure over the three booleans so it can be unit-tested without touching the
/// environment.
fn auth_checks(api_key: bool, username: bool, password: bool) -> Vec<Check> {
    if api_key {
        return vec![Check::ok("api_key", presence("ANTITHESIS_API_KEY", true))];
    }

    // Legacy auth needs BOTH halves; a lone username or password is not usable.
    if username && password {
        return vec![
            Check::warn("api_key", presence("ANTITHESIS_API_KEY", false))
                .note(
                    Level::Warning,
                    "`snouty runs` and other API commands require an API key",
                )
                .note(
                    Level::Note,
                    "ask Antithesis support for an API key if you don't have one",
                ),
            Check::ok(
                "basic_auth",
                "ANTITHESIS_USERNAME & ANTITHESIS_PASSWORD are set",
            )
            .note(
                Level::Warning,
                "legacy authentication method, set ANTITHESIS_API_KEY for full API access",
            )
            .note(
                Level::Note,
                "only `snouty launch` and `snouty debug` accept it",
            ),
        ];
    }

    vec![
        Check::fail("api_key", presence("ANTITHESIS_API_KEY", false))
            .note(
                Level::Error,
                "snouty requires an API key to authenticate with Antithesis",
            )
            .note(
                Level::Note,
                "ask Antithesis support for an API key if you don't have one",
            ),
    ]
}

fn collect_checks() -> Vec<Check> {
    let mut checks: Vec<Check> = Vec::new();

    // Container runtime (for building/pushing images)
    match container::runtime() {
        Ok(rt) => checks.push(Check::ok(
            "container_runtime",
            format!("Container runtime: {} detected", rt.name()),
        )),
        Err(e) => checks.push(
            Check::fail("container_runtime", "Container runtime not detected")
                .note(Level::Error, e.to_string()),
        ),
    }

    // Docker Compose v2 (required for compose configs)
    match container::docker_compose_version() {
        Ok(version) => checks.push(Check::ok("docker_compose", version)),
        Err(e) => checks.push(
            Check::fail("docker_compose", "docker-compose not available")
                .note(Level::Error, e.to_string()),
        ),
    }

    // Required environment + authentication
    checks.push(tenant_check(env_set("ANTITHESIS_TENANT")));
    checks.push(repository_check(env_set("ANTITHESIS_REPOSITORY")));
    checks.extend(auth_checks(
        env_set("ANTITHESIS_API_KEY"),
        env_set("ANTITHESIS_USERNAME"),
        env_set("ANTITHESIS_PASSWORD"),
    ));

    checks
}

/// Map a `GET /api/version` probe into a check. Reaching the endpoint at all —
/// even a 404 on an older backend that lacks it — proves connectivity, so only
/// rejected auth, an API error, or a failure to reach the API is a problem.
/// Pure over the result so it can be unit-tested without the network.
fn version_check(host: &str, result: std::result::Result<ApiVersion, VersionError>) -> Check {
    match result {
        Ok(v) => Check::ok("api", "Antithesis API reachable")
            .note(
                Level::Note,
                format!("latest API version: {}", v.latest_api_version),
            )
            .note(
                Level::Note,
                format!("tenant release version: {}", v.release_version),
            ),
        // 404: the version endpoint was added in release 56, so an older tenant
        // 404s — but the request was served, so auth and connectivity are fine
        // and the API is the stable v1.
        Err(VersionError::Http(404)) => Check::ok("api", "Antithesis API reachable").note(
            Level::Warning,
            "tenant release version: older than v56; upgrade recommended",
        ),
        // 401/403: the request was rejected — authentication is broken.
        // Connectivity is only probably ok (a proxy can reject before the
        // request reaches the API), so we don't claim it.
        Err(VersionError::Http(status @ (401 | 403))) => {
            Check::fail("api", "Antithesis API rejected authentication")
                .note(Level::Error, format!("the API returned HTTP {status}"))
                .note(Level::Note, "check ANTITHESIS_API_KEY")
        }
        // 5xx: we reached something, but it's erroring — connectivity is broken
        // by a server error and auth status is unknown.
        Err(VersionError::Http(status)) if (500..=599).contains(&status) => {
            Check::fail("api", "Antithesis API unavailable").note(
                Level::Error,
                format!("the API returned HTTP {status} (server error)"),
            )
        }
        // Any other unexpected status.
        Err(VersionError::Http(status)) => Check::fail("api", "Antithesis API error").note(
            Level::Error,
            format!("the API returned an unexpected HTTP {status}"),
        ),
        // Couldn't connect at all — connectivity is broken.
        Err(VersionError::Unreachable(err)) => Check::fail("api", "Antithesis API unreachable")
            .note(Level::Error, format!("could not connect to {host}"))
            .note(Level::Error, err),
    }
}

pub async fn cmd_doctor(json: bool, verbose: bool, offline: bool) -> Result<()> {
    let mut checks = collect_checks();

    // Connectivity + version check (network). Skipped with --offline. Only runs
    // with an API key: /api/version, like every endpoint but launch, rejects
    // basic auth, so probing it under username/password would only yield a
    // misleading 403 — and the auth checks above already tell legacy and
    // unauthenticated users to set a key. `verbose` logs the request/response.
    if !offline && let Ok(api) = AntithesisApi::from_env_requiring_api_key(verbose) {
        let host = api.host();
        checks.push(version_check(&host, api.get_version().await));
    }

    let errors = checks.iter().filter(|c| c.status == Status::Error).count();
    let warnings = checks.iter().filter(|c| c.status == Status::Warn).count();

    if json {
        let report = Report {
            ok: errors == 0,
            checks: &checks,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        for check in &checks {
            check.print();
        }
        eprintln!();
        let wp = if warnings == 1 { "" } else { "s" };
        if errors > 0 {
            let ep = if errors == 1 { "" } else { "s" };
            if warnings > 0 {
                eprintln!(
                    "doctor found {errors} problem{ep} and {warnings} warning{wp} — \
                     see the ✗ and ⚠ checks above"
                );
            } else {
                eprintln!("doctor found {errors} problem{ep} — see the ✗ check{ep} above");
            }
        } else if warnings > 0 {
            eprintln!("doctor passed with {warnings} warning{wp}");
        } else {
            eprintln!("All checks passed");
        }
    }

    // Exit non-zero on failure without re-rendering an error report: the checks
    // above already say exactly what's wrong, so a generic "Error: doctor found
    // problems" footer would be redundant noise.
    if errors > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_api_key_set_is_a_single_bare_ok_check() {
        let checks = auth_checks(true, false, false);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Ok);
        assert!(checks[0].message.contains("ANTITHESIS_API_KEY is set"));
        // A configured key needs no explanation — keep the happy path quiet.
        assert!(checks[0].notes.is_empty());
    }

    #[test]
    fn auth_api_key_wins_and_ignores_basic_creds() {
        let checks = auth_checks(true, true, true);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Ok);
        assert!(!checks[0].message.contains("USERNAME"));
    }

    #[test]
    fn auth_legacy_basic_warns_on_key_and_notes_legacy() {
        let checks = auth_checks(false, true, true);
        assert_eq!(checks.len(), 2);

        // Missing API key is a warning, with a how-to-get-one note (issue #2).
        assert_eq!(checks[0].status, Status::Warn);
        assert!(checks[0].message.contains("ANTITHESIS_API_KEY is not set"));
        assert!(checks[0].notes.iter().any(|n| n.level == Level::Warning));
        assert!(
            checks[0]
                .notes
                .iter()
                .any(|n| n.text.contains("ask Antithesis support"))
        );

        // The legacy creds get their own passing check: a WARNING that flags the
        // legacy method and points at the API key, plus a NOTE on its scope.
        assert_eq!(checks[1].status, Status::Ok);
        assert_eq!(checks[1].notes.len(), 2);
        assert!(checks[1].notes.iter().any(|n| n.level == Level::Warning
            && n.text.contains("legacy authentication method")
            && n.text.contains("ANTITHESIS_API_KEY")));
        let notes = checks[1]
            .notes
            .iter()
            .map(|n| n.text.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(notes.contains("snouty launch"));
    }

    #[test]
    fn auth_nothing_set_errors_and_only_mentions_api_key() {
        let checks = auth_checks(false, false, false);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Error);
        assert!(checks[0].message.contains("ANTITHESIS_API_KEY is not set"));
        assert!(checks[0].notes.iter().any(|n| n.level == Level::Error));
        // Issue #2: tell the user where to get a key.
        assert!(
            checks[0]
                .notes
                .iter()
                .any(|n| n.text.contains("ask Antithesis support"))
        );
        // Nothing-set must steer to the API key only — no username/password noise.
        let all = format!(
            "{} {}",
            checks[0].message,
            checks[0]
                .notes
                .iter()
                .map(|n| n.text.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        );
        assert!(!all.contains("USERNAME"));
        assert!(!all.contains("PASSWORD"));
    }

    #[test]
    fn auth_partial_basic_errors_like_nothing_set() {
        for (username, password) in [(true, false), (false, true)] {
            let checks = auth_checks(false, username, password);
            assert_eq!(checks.len(), 1);
            assert_eq!(checks[0].status, Status::Error);
        }
    }

    #[test]
    fn tenant_missing_is_an_error_with_a_note() {
        let check = tenant_check(false);
        assert_eq!(check.status, Status::Error);
        assert!(!check.notes.is_empty());
        assert!(check.notes.iter().any(|n| n.text.contains("required")));
    }

    #[test]
    fn repository_missing_is_only_a_warning() {
        // Issue #3: REPOSITORY is launch-only, so a missing one must not fail doctor.
        let check = repository_check(false);
        assert_eq!(check.status, Status::Warn);
        assert!(check.notes.iter().any(|n| n.text.contains("--config")));
    }

    #[test]
    fn version_ok_reports_both_versions() {
        let check = version_check(
            "tenant.antithesis.com",
            Ok(ApiVersion {
                latest_api_version: "v1".into(),
                release_version: "56.0".into(),
            }),
        );
        assert_eq!(check.status, Status::Ok);
        let notes = check
            .notes
            .iter()
            .map(|n| n.text.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(notes.contains("v1"));
        assert!(notes.contains("56.0"));
    }

    #[test]
    fn version_404_is_reachable_but_warns_an_old_tenant() {
        // 404 means an older tenant that predates the endpoint, but the request
        // was served — so connectivity and auth are fine; doctor stays green and
        // warns that the tenant release is old.
        let check = version_check("tenant.antithesis.com", Err(VersionError::Http(404)));
        assert_eq!(check.status, Status::Ok);
        assert!(check.message.contains("reachable"));
        assert!(
            check
                .notes
                .iter()
                .any(|n| n.level == Level::Warning && n.text.contains("older than v56"))
        );
    }

    #[test]
    fn version_auth_rejection_reports_the_status_code() {
        for status in [401, 403] {
            let check = version_check("tenant.antithesis.com", Err(VersionError::Http(status)));
            assert_eq!(check.status, Status::Error);
            assert!(
                check
                    .notes
                    .iter()
                    .any(|n| n.text.contains(&status.to_string()))
            );
            assert!(
                check
                    .notes
                    .iter()
                    .any(|n| n.text.contains("ANTITHESIS_API_KEY"))
            );
        }
    }

    #[test]
    fn version_unreachable_names_the_host_and_includes_the_error() {
        let check = version_check(
            "tenant.antithesis.com",
            Err(VersionError::Unreachable("connection refused".into())),
        );
        assert_eq!(check.status, Status::Error);
        assert!(check.message.contains("unreachable"));
        // A clean host-named note, plus the raw error for debugging.
        assert!(check.notes.iter().any(|n| {
            n.text
                .contains("could not connect to tenant.antithesis.com")
        }));
        assert!(
            check
                .notes
                .iter()
                .any(|n| n.text.contains("connection refused"))
        );
    }

    #[test]
    fn version_5xx_is_unavailable_with_status() {
        // 5xx is connectivity broken by a server error (auth unknown).
        let check = version_check("tenant.antithesis.com", Err(VersionError::Http(503)));
        assert_eq!(check.status, Status::Error);
        assert!(check.message.contains("unavailable"));
        assert!(check.notes.iter().any(|n| n.text.contains("503")));
    }

    #[test]
    fn version_unexpected_status_is_an_error_with_status() {
        let check = version_check("tenant.antithesis.com", Err(VersionError::Http(429)));
        assert_eq!(check.status, Status::Error);
        assert!(check.notes.iter().any(|n| n.text.contains("429")));
    }

    #[test]
    fn json_report_carries_structured_status_levels_and_notes() {
        let checks = auth_checks(false, false, false);
        let report = Report {
            ok: false,
            checks: &checks,
        };
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["ok"], false);
        assert_eq!(value["checks"][0]["name"], "api_key");
        assert_eq!(value["checks"][0]["status"], "error");
        assert_eq!(
            value["checks"][0]["message"],
            "ANTITHESIS_API_KEY is not set"
        );
        assert_eq!(value["checks"][0]["notes"][0]["level"], "error");
        assert!(
            value["checks"][0]["notes"][0]["text"]
                .as_str()
                .unwrap()
                .contains("requires an API key")
        );
    }

    #[test]
    fn json_omits_notes_when_empty() {
        let check = Check::ok("docker_compose", "docker-compose: v2.0.0");
        let value = serde_json::to_value(&check).unwrap();
        assert!(value.get("notes").is_none());
    }
}
