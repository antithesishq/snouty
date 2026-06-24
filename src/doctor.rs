use color_eyre::eyre::Result;
use serde::Serialize;

use crate::api::{AntithesisApi, ApiVersion, VersionError};
use crate::attributed_value::AttributedValue;
use crate::container;
use crate::credentials::{Credentials, PasswordCredentials};
use crate::render::render_kv;
use crate::settings::Settings;

/// Outcome of a single health check. Only `Error` fails doctor; `Warn` is
/// surfaced but the run still passes.
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

fn status_icon(status: Status) -> console::StyledObject<&'static str> {
    match status {
        Status::Ok => console::style("✓").green(),
        Status::Warn => console::style("⚠").yellow(),
        Status::Error => console::style("✗").red(),
    }
}

fn print_notes(notes: &[Note]) {
    for note in notes {
        let label = match note.level {
            Level::Note => console::style(note.level.label()).dim(),
            Level::Warning => console::style(note.level.label()).yellow(),
            Level::Error => console::style(note.level.label()).red(),
        };
        eprintln!("      {}: {}", label, note.text);
    }
}

/// One health check: local tooling, settings validity, authentication, and
/// API connectivity. The headline `message` states the bare fact; the `notes`
/// carry explanations and how-tos. `name` is a stable machine key for `--json`.
/// Checks own all of doctor's pass/warn/fail semantics — the settings table is
/// purely informational.
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
        eprintln!("  {} {}", status_icon(self.status), self.message);
        print_notes(&self.notes);
    }
}

/// One row of the resolved-settings table: a setting and the value snouty
/// resolved for it. Purely informational — it carries no status; whether a value
/// is required or optional is a [`Check`] concern.
#[derive(Serialize)]
struct Setting {
    name: &'static str,
    value: String,
}

impl Setting {
    fn new(name: &'static str, value: impl Into<String>) -> Self {
        Self {
            name,
            value: value.into(),
        }
    }
}

/// The full doctor report, as emitted by `--json`: the binary `checks` and the
/// informational `settings` table snouty resolved.
#[derive(Serialize)]
struct Report<'a> {
    ok: bool,
    checks: &'a [Check],
    settings: &'a [Setting],
}

/// The tenant is required by every command, so a missing one fails doctor.
/// (A broken settings file never reaches here — it fails at startup, before
/// doctor runs — so the resolved value is simply present or absent.)
fn tenant_check(tenant: Option<&str>) -> Check {
    match tenant {
        Some(_) => Check::ok("tenant", "tenant is set"),
        None => Check::fail("tenant", "tenant is not set").note(
            Level::Note,
            "set ANTITHESIS_TENANT or add it to a settings file",
        ),
    }
}

/// The repository (a container registry) is only needed to build and push a
/// config image (`snouty launch --config`), so a missing one is a warning, not
/// a failure — read-only use (`snouty runs`, `snouty debug`) doesn't need it.
fn repository_check(repository: Option<&str>) -> Check {
    match repository {
        Some(_) => Check::ok("repository", "repository is set"),
        None => Check::warn("repository", "repository is not set")
            .note(Level::Warning, "repository is needed for `snouty launch`"),
    }
}

fn authn_checks(credentials: Result<AttributedValue<Credentials>>) -> Vec<Check> {
    match credentials {
        Ok(credentials) => match credentials.unwrap() {
            Credentials::ApiKey(_) => {
                vec![enrich(
                    Check::ok("api_key", "API key provided"),
                    credentials,
                )]
            }
            Credentials::Password(PasswordCredentials { username, .. }) => vec![
                Check::warn("api_key", "API key not provided")
                    .note(
                        Level::Warning,
                        "`snouty runs` and other API commands require an API key",
                    )
                    .note(
                        Level::Note,
                        "ask Antithesis support for an API key if you don't have one",
                    ),
                enrich(
                    Check::ok(
                        "basic_auth",
                        format!("Using password credentials for user [{username}]"),
                    )
                    .note(
                        Level::Warning,
                        "legacy authentication method, set ANTITHESIS_API_KEY for full API access",
                    )
                    .note(
                        Level::Note,
                        "username/password only enables `snouty launch` and `snouty debug`",
                    ),
                    credentials,
                ),
            ],
        },
        Err(err) => vec![
            Check::fail("api_key", err.to_string())
                .note(
                    Level::Error,
                    "snouty requires an API key to authenticate with Antithesis",
                )
                .note(
                    Level::Note,
                    "ask Antithesis support for an API key if you don't have one",
                ),
        ],
    }
}

fn enrich<T>(check: Check, attribution: AttributedValue<T>) -> Check {
    match attribution {
        AttributedValue::FromEnvironmentVariable {
            value: _,
            environment_variable_name,
        } => check.note(
            Level::Note,
            format!("read from the [{environment_variable_name}] environment variable"),
        ),
        AttributedValue::FromSettingsFile {
            value: _,
            settings_file_path,
            profile,
        } => check.note(
            Level::Note,
            format!(
                "read from settings file at [{:?}] {}",
                settings_file_path,
                match profile {
                    Some(profile_name) => format!("under the [{profile_name}] profile"),
                    None => "defaults".to_owned(),
                }
            ),
        ),
        AttributedValue::FromKeychain {
            value: _,
            entry_name,
        } => check.note(
            Level::Note,
            format!("read from system keychain entry named [{entry_name}]",),
        ),
    }
}

/// Binary health checks: local tooling, the required settings
/// (tenant/repository), and authentication. The resolved values themselves are
/// reported separately by [`resolve_settings`].
fn collect_checks(settings: &Settings) -> Vec<Check> {
    let mut checks: Vec<Check> = Vec::new();

    // Container runtime (for building/pushing images)
    match container::runtime(settings) {
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

    // Required settings. tenant is needed by every command; repository is
    // launch-only, so a missing one is a warning.
    checks.push(tenant_check(settings.tenant()));
    checks.push(repository_check(settings.repository()));

    // Authentication (environment-only by design).
    checks.extend(authn_checks(
        Credentials::for_ambient_credentials_with_attribution(settings.profile(), true),
    ));

    checks
}

/// The resolved-settings table: the value snouty resolved for each setting and
/// where it came from (env > profile > project/global file). Purely
/// informational — required/optional semantics are reported by [`collect_checks`].
fn resolve_settings(settings: &Settings) -> Vec<Setting> {
    vec![
        Setting::new("profile", settings.profile().unwrap_or("(none)")),
        Setting::new("tenant", settings.tenant().unwrap_or("not set")),
        Setting::new("repository", settings.repository().unwrap_or("not set")),
        // The explicit override, otherwise auto-detected.
        Setting::new(
            "container engine",
            settings.container_engine().unwrap_or("auto-detect"),
        ),
    ]
}

/// Print the resolved-settings table, aligned via the shared [`render_kv`] helper
/// (which also sanitizes the values). No status icons — the checks above own
/// pass/warn/fail; this table just reports what snouty resolved, indented to sit
/// under the "Resolved settings" heading.
fn print_settings(settings: &[Setting]) {
    let rows: Vec<(&str, String)> = settings.iter().map(|s| (s.name, s.value.clone())).collect();
    for line in render_kv(&rows, 0).lines() {
        eprintln!("  {line}");
    }
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
        // 404s — but the request was served, so auth and connectivity are fine.
        // A 404 can also come from a proxy/route in front of the tenant, so we
        // name that possibility rather than asserting the tenant is old.
        Err(VersionError::Http(404)) => Check::ok("api", "Antithesis API reachable").note(
            Level::Warning,
            "GET /api/version returned 404 — your tenant likely predates version \
             reporting (added in release 56); if you expect a current tenant, a \
             proxy or route may be intercepting the request",
        ),
        // 401/403: the request was rejected. Most often the API key is wrong,
        // but a proxy can also reject before the request reaches the API, so we
        // name both rather than blaming the key outright.
        Err(VersionError::Http(status @ (401 | 403))) => {
            Check::fail("api", "Antithesis API rejected authentication")
                .note(Level::Error, format!("the API returned HTTP {status}"))
                .note(
                    Level::Note,
                    "verify ANTITHESIS_API_KEY is valid; if it is, a proxy may be \
                     rejecting the request before it reaches Antithesis",
                )
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

pub async fn cmd_doctor(
    settings: &Settings,
    json: bool,
    verbose: bool,
    offline: bool,
) -> Result<()> {
    let mut checks = collect_checks(settings);

    // Connectivity + version check (network). Skipped with --offline. Only runs
    // with an API key: /api/version, like every endpoint but launch, rejects
    // basic auth, so probing it under username/password would only yield a
    // misleading 403 — and the auth checks above already tell legacy and
    // unauthenticated users to set a key. The client is built from the resolved
    // settings (base url / tenant), and `verbose` logs the request/response.
    if !offline && let Ok(api) = AntithesisApi::new_requiring_api_key(settings, verbose) {
        let host = api.host();
        checks.push(version_check(&host, api.get_version().await));
    }

    let settings_rows = resolve_settings(settings);

    // Only the checks carry pass/warn/fail; the settings table is informational.
    let errors = checks.iter().filter(|c| c.status == Status::Error).count();
    let warnings = checks.iter().filter(|c| c.status == Status::Warn).count();

    if json {
        let report = Report {
            ok: errors == 0,
            checks: &checks,
            settings: &settings_rows,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        eprintln!("Checks");
        for check in &checks {
            check.print();
        }
        eprintln!();
        eprintln!("Resolved settings");
        print_settings(&settings_rows);
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
    use color_eyre::eyre::eyre;

    use crate::credentials::{API_KEY_VAR_NAME, PASSWORD_VAR_NAME};

    use super::*;

    // ---- auth_checks (env-only auth) -----------------------------------

    #[test]
    fn auth_api_key_set_is_a_single_bare_ok_check() {
        let checks = authn_checks(Ok(AttributedValue::FromEnvironmentVariable {
            value: Credentials::for_api_key("api_key".to_owned()),
            environment_variable_name: API_KEY_VAR_NAME,
        }));
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Ok);
        assert!(checks[0].message.contains("API key provided"));
    }

    #[test]
    fn auth_legacy_basic_warns_on_key_and_notes_legacy() {
        let checks = authn_checks(Ok(AttributedValue::FromEnvironmentVariable {
            value: Credentials::for_password("user".to_owned(), "pass".to_owned()),
            environment_variable_name: PASSWORD_VAR_NAME,
        }));
        assert_eq!(checks.len(), 2);
        assert_eq!(checks[0].status, Status::Warn);
        assert!(checks[0].message.contains("API key not provided"));
        assert!(checks[0].notes.iter().any(|n| n.level == Level::Warning));
        assert!(
            checks[0]
                .notes
                .iter()
                .any(|n| n.text.contains("ask Antithesis support"))
        );
        assert_eq!(checks[1].status, Status::Ok);
        assert_eq!(checks[1].notes.len(), 3);
        assert!(checks[1].notes.iter().any(|n| n.level == Level::Warning
            && n.text.contains("legacy authentication method")
            && n.text.contains("ANTITHESIS_API_KEY")));
        // The legacy creds steer the user to the only commands they unlock.
        assert!(checks[1].notes.iter().any(|n| n.level == Level::Note
            && n.text.contains("snouty launch")
            && n.text.contains("snouty debug")));
    }

    #[test]
    fn auth_nothing_set_errors_and_only_mentions_api_key() {
        let checks = authn_checks(Err(eyre!("PANIC PANIC PANIC")));
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Error);
        assert!(checks[0].message.contains("PANIC PANIC PANIC"));
        assert!(checks[0].notes.iter().any(|n| n.level == Level::Error));
        assert!(
            checks[0]
                .notes
                .iter()
                .any(|n| n.text.contains("ask Antithesis support"))
        );
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

    // ---- required-settings checks --------------------------------------

    #[test]
    fn tenant_check_is_ok_when_resolved() {
        let check = tenant_check(Some("acme"));
        assert_eq!(check.status, Status::Ok);
    }

    #[test]
    fn tenant_check_fails_when_missing() {
        let check = tenant_check(None);
        assert_eq!(check.status, Status::Error);
        assert!(
            check
                .notes
                .iter()
                .any(|n| n.text.contains("ANTITHESIS_TENANT"))
        );
    }

    #[test]
    fn repository_check_is_ok_when_resolved() {
        let check = repository_check(Some("acme/repo"));
        assert_eq!(check.status, Status::Ok);
    }

    #[test]
    fn repository_check_only_warns_when_missing() {
        // Following main's #147 decision: repository is launch-only, so a missing
        // one is a warning, not a failure.
        let check = repository_check(None);
        assert_eq!(check.status, Status::Warn);
        assert!(check.notes.iter().any(|n| n.text.contains("snouty launch")));
    }

    // ---- resolved-settings table (informational, no status) ------------

    fn row<'a>(rows: &'a [Setting], name: &str) -> &'a Setting {
        rows.iter().find(|s| s.name == name).expect("row present")
    }

    #[test]
    fn tenant_row_shows_value() {
        let settings = Settings::for_test(None, Some("acme"), None, None, None);
        let rows = resolve_settings(&settings);
        assert_eq!(row(&rows, "tenant").value, "acme");
    }

    #[test]
    fn missing_settings_render_as_not_set() {
        let rows = resolve_settings(&Settings::for_test(None, None, None, None, None));
        assert_eq!(row(&rows, "tenant").value, "not set");
    }

    #[test]
    fn container_engine_row_auto_detects_when_unset() {
        let settings = Settings::for_test(None, Some("acme"), None, None, None);
        let rows = resolve_settings(&settings);
        assert_eq!(row(&rows, "container engine").value, "auto-detect");
    }

    #[test]
    fn profile_row_reflects_no_active_profile() {
        let rows = resolve_settings(&Settings::for_test(None, None, None, None, None));
        assert_eq!(row(&rows, "profile").value, "(none)");
    }

    // ---- version_check (network probe) ---------------------------------

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
    fn version_404_is_reachable_but_warns() {
        let check = version_check("tenant.antithesis.com", Err(VersionError::Http(404)));
        assert_eq!(check.status, Status::Ok);
        assert!(check.message.contains("reachable"));
        // The warning explains the 404 without definitively blaming an old
        // tenant — it names both the old-tenant and proxy possibilities.
        let warning = check
            .notes
            .iter()
            .find(|n| n.level == Level::Warning)
            .expect("a 404 should attach a warning note");
        assert!(warning.text.contains("404"));
        assert!(warning.text.contains("proxy"));
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

    // ---- JSON report ----------------------------------------------------

    #[test]
    fn json_report_carries_checks_and_informational_settings() {
        let checks = authn_checks(Err(eyre!("PANIC PANIC PANIC")));
        let settings = vec![Setting::new("tenant", "acme")];
        let report = Report {
            ok: false,
            checks: &checks,
            settings: &settings,
        };
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["ok"], false);
        assert_eq!(value["checks"][0]["name"], "api_key");
        assert_eq!(value["checks"][0]["status"], "error");
        assert_eq!(value["checks"][0]["notes"][0]["level"], "error");
        // Settings rows are informational: a name and a value, no status.
        assert_eq!(value["settings"][0]["name"], "tenant");
        assert_eq!(value["settings"][0]["value"], "acme");
        assert!(value["settings"][0].get("status").is_none());
    }
}
