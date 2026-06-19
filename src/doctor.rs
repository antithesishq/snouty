use std::env;

use color_eyre::eyre::Result;
use serde::Serialize;

use crate::api::{AntithesisApi, ApiVersion, VersionError};
use crate::container;
use crate::settings::{AttributedValue, Settings, SharedReport, ValueSource};

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

/// One health check: local tooling, configuration validity, authentication, and
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
/// resolved for it, plus where it came from. Purely informational — it carries
/// no status; whether a value is required or optional is a [`Check`] concern.
#[derive(Serialize)]
struct Setting {
    name: &'static str,
    value: String,
    /// Where the value came from: `env`, a profile name, `default`, or
    /// `derived`. `None` when there's nothing to attribute (unset / auto).
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
}

impl Setting {
    fn sourced(name: &'static str, value: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            name,
            value: value.into(),
            source: Some(source.into()),
        }
    }

    fn plain(name: &'static str, value: impl Into<String>) -> Self {
        Self {
            name,
            value: value.into(),
            source: None,
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

fn env_set(name: &str) -> bool {
    env::var(name).is_ok_and(|v| !v.is_empty())
}

fn presence(var: &str, set: bool) -> String {
    format!("{var} {}", if set { "is set" } else { "is not set" })
}

/// Human-readable source for a resolved setting: `env`, the active profile name
/// (when it came from a profile), or `default` (a top-level config-file value).
fn source_label(source: &ValueSource, profile: Option<&str>) -> String {
    match source {
        ValueSource::EnvironmentVariable => "env".to_string(),
        ValueSource::ProjectProfile | ValueSource::GlobalProfile => {
            profile.unwrap_or("profile").to_string()
        }
        ValueSource::ProjectDefault | ValueSource::GlobalDefault => "default".to_string(),
    }
}

/// The tenant is required by every command, so a missing one fails doctor.
fn tenant_check(resolved: &Result<Option<AttributedValue<&String>>, SharedReport>) -> Check {
    match resolved {
        Ok(Some(_)) => Check::ok("tenant", "tenant is set"),
        Ok(None) => Check::fail("tenant", "tenant is not set").note(
            Level::Note,
            "required by every command — set ANTITHESIS_TENANT or add it to a config file",
        ),
        Err(err) => Check::fail("tenant", "tenant could not be resolved")
            .note(Level::Error, err.to_string()),
    }
}

/// The repository (a container registry) is only needed to build and push a
/// config image (`snouty launch --config`), so a missing one is a warning, not
/// a failure — read-only use (`snouty runs`, `snouty debug`) doesn't need it.
fn repository_check(resolved: &Result<Option<AttributedValue<&String>>, SharedReport>) -> Check {
    match resolved {
        Ok(Some(_)) => Check::ok("repository", "repository is set"),
        Ok(None) => Check::warn("repository", "repository is not set").note(
            Level::Note,
            "only needed to build and push a config image (snouty launch --config)",
        ),
        Err(err) => Check::fail("repository", "repository could not be resolved")
            .note(Level::Error, err.to_string()),
    }
}

/// Authentication checks. snouty authenticates with an API key, which grants
/// the full Antithesis API. Username/password is legacy auth accepted only by
/// `snouty launch` and `snouty debug`, so it never stands in for a missing API
/// key — it only softens the missing-key failure into a warning.
///
/// Authentication is intentionally environment-only: secrets never live in a
/// config file. Pure over the three booleans so it can be unit-tested without
/// touching the environment.
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

/// Binary health checks: local tooling, config-file validity, the required
/// configuration (tenant/repository), and authentication. The resolved values
/// themselves are reported separately by [`resolve_settings`].
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

    // Config files: report the file in use, or flag one that exists but can't be
    // read or parsed. A missing file is normal (settings can come entirely from
    // the environment), so it produces no row.
    match settings.load_project_settings() {
        Ok(Some(file)) => checks.push(Check::ok(
            "project_config",
            format!("project config file: {}", file.resolved_path.display()),
        )),
        Ok(None) => {}
        Err(err) => checks.push(
            Check::fail("project_config", "project config file could not be read")
                .note(Level::Error, err.to_string()),
        ),
    }
    match settings.load_global_settings() {
        Ok(Some(file)) => checks.push(Check::ok(
            "global_config",
            format!("global config file: {}", file.resolved_path.display()),
        )),
        Ok(None) => {}
        Err(err) => checks.push(
            Check::fail("global_config", "global config file could not be read")
                .note(Level::Error, err.to_string()),
        ),
    }

    // Required configuration. tenant is needed by every command; repository is
    // launch-only, so a missing one is a warning.
    checks.push(tenant_check(&settings.try_resolve_tenant()));
    checks.push(repository_check(&settings.try_resolve_repository()));

    // Authentication (environment-only by design).
    checks.extend(auth_checks(
        env_set("ANTITHESIS_API_KEY"),
        env_set("ANTITHESIS_USERNAME"),
        env_set("ANTITHESIS_PASSWORD"),
    ));

    checks
}

/// The resolved-settings table: the value snouty resolved for each setting and
/// where it came from (env > profile > project/global file). Purely
/// informational — required/optional semantics are reported by [`collect_checks`].
fn resolve_settings(settings: &Settings) -> Vec<Setting> {
    let profile = settings.settings_profile();

    let resolved = |name: &'static str,
                    value: Result<Option<AttributedValue<&String>>, SharedReport>|
     -> Setting {
        match value {
            Ok(Some(av)) => Setting::sourced(
                name,
                av.value.clone(),
                source_label(&av.attribution, profile),
            ),
            Ok(None) => Setting::plain(name, "not set"),
            Err(_) => Setting::plain(name, "error"),
        }
    };

    vec![
        // Active profile.
        Setting::plain("profile", profile.unwrap_or("(none)")),
        resolved("tenant", settings.try_resolve_tenant()),
        resolved("repository", settings.try_resolve_repository()),
        // base url: the explicit value if set, otherwise derived from the tenant.
        match settings.try_resolve_base_url() {
            Ok(Some(av)) => Setting::sourced(
                "base url",
                av.value.clone(),
                source_label(&av.attribution, profile),
            ),
            Ok(None) => match settings.base_url() {
                Ok(url) => Setting::sourced("base url", url.to_string(), "derived"),
                Err(_) => Setting::plain("base url", "derives from tenant"),
            },
            Err(_) => Setting::plain("base url", "error"),
        },
        // container engine: explicit value, otherwise auto-detected.
        match settings.try_resolve_container_engine() {
            Ok(Some(av)) => Setting::sourced(
                "container engine",
                av.value.clone(),
                source_label(&av.attribution, profile),
            ),
            Ok(None) => Setting::plain("container engine", "auto-detect"),
            Err(_) => Setting::plain("container engine", "error"),
        },
    ]
}

/// Print the resolved-settings table with `SETTING / SOURCE / VALUE` headers,
/// columns aligned. No status icons — the checks above own pass/warn/fail; this
/// table just reports what snouty resolved and where from. The value is last
/// (and unpadded) so long URLs don't push later columns out of alignment.
fn print_settings(settings: &[Setting]) {
    fn source_col(s: &Setting) -> &str {
        s.source.as_deref().unwrap_or("-")
    }
    let name_w = settings
        .iter()
        .map(|s| s.name.len())
        .chain(["SETTING".len()])
        .max()
        .unwrap_or(0);
    let source_w = settings
        .iter()
        .map(|s| source_col(s).len())
        .chain(["SOURCE".len()])
        .max()
        .unwrap_or(0);
    eprintln!("  {:<name_w$}  {:<source_w$}  VALUE", "SETTING", "SOURCE");
    for s in settings {
        eprintln!(
            "  {:<name_w$}  {:<source_w$}  {}",
            s.name,
            source_col(s),
            s.value
        );
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
    use super::*;

    // ---- auth_checks (env-only auth) -----------------------------------

    #[test]
    fn auth_api_key_set_is_a_single_bare_ok_check() {
        let checks = auth_checks(true, false, false);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Ok);
        assert!(checks[0].message.contains("ANTITHESIS_API_KEY is set"));
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
        assert_eq!(checks[0].status, Status::Warn);
        assert!(checks[0].message.contains("ANTITHESIS_API_KEY is not set"));
        assert!(checks[0].notes.iter().any(|n| n.level == Level::Warning));
        assert!(
            checks[0]
                .notes
                .iter()
                .any(|n| n.text.contains("ask Antithesis support"))
        );
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

    #[test]
    fn auth_partial_basic_errors_like_nothing_set() {
        for (username, password) in [(true, false), (false, true)] {
            let checks = auth_checks(false, username, password);
            assert_eq!(checks.len(), 1);
            assert_eq!(checks[0].status, Status::Error);
        }
    }

    // ---- required-configuration checks ---------------------------------

    fn attributed(
        value: &String,
        attribution: ValueSource,
    ) -> Result<Option<AttributedValue<&String>>, SharedReport> {
        Ok(Some(AttributedValue { value, attribution }))
    }

    #[test]
    fn tenant_check_is_ok_when_resolved() {
        let value = "acme".to_string();
        let check = tenant_check(&attributed(&value, ValueSource::EnvironmentVariable));
        assert_eq!(check.status, Status::Ok);
    }

    #[test]
    fn tenant_check_fails_when_missing() {
        let check = tenant_check(&Ok(None));
        assert_eq!(check.status, Status::Error);
        assert!(check.notes.iter().any(|n| n.text.contains("required")));
    }

    #[test]
    fn repository_check_is_ok_when_resolved() {
        let value = "acme/repo".to_string();
        let check = repository_check(&attributed(&value, ValueSource::ProjectDefault));
        assert_eq!(check.status, Status::Ok);
    }

    #[test]
    fn repository_check_only_warns_when_missing() {
        // Following main's #147 decision: repository is launch-only, so a missing
        // one is a warning, not a failure.
        let check = repository_check(&Ok(None));
        assert_eq!(check.status, Status::Warn);
        assert!(check.notes.iter().any(|n| n.text.contains("--config")));
    }

    // ---- source_label --------------------------------------------------

    #[test]
    fn source_label_maps_every_value_source() {
        assert_eq!(source_label(&ValueSource::EnvironmentVariable, None), "env");
        assert_eq!(
            source_label(&ValueSource::ProjectProfile, Some("staging")),
            "staging"
        );
        assert_eq!(
            source_label(&ValueSource::GlobalProfile, Some("staging")),
            "staging"
        );
        assert_eq!(source_label(&ValueSource::ProjectDefault, None), "default");
        assert_eq!(source_label(&ValueSource::GlobalDefault, None), "default");
    }

    // ---- resolved-settings table (informational, no status) ------------

    fn row<'a>(rows: &'a [Setting], name: &str) -> &'a Setting {
        rows.iter().find(|s| s.name == name).expect("row present")
    }

    #[test]
    fn tenant_row_shows_value_and_source() {
        let env = crate::settings::LoadedSettings::for_test_tenant("acme");
        let rows = resolve_settings(&Settings::for_test(env, Ok(None), Ok(None)));
        let tenant = row(&rows, "tenant");
        assert_eq!(tenant.value, "acme");
        assert_eq!(tenant.source.as_deref(), Some("env"));
    }

    #[test]
    fn missing_settings_render_as_not_set_without_a_source() {
        let rows = resolve_settings(&Settings::for_test(
            crate::settings::LoadedSettings::empty(),
            Ok(None),
            Ok(None),
        ));
        let tenant = row(&rows, "tenant");
        assert_eq!(tenant.value, "not set");
        assert_eq!(tenant.source, None);
    }

    #[test]
    fn base_url_row_derives_from_tenant_when_unset() {
        let env = crate::settings::LoadedSettings::for_test_tenant("acme");
        let rows = resolve_settings(&Settings::for_test(env, Ok(None), Ok(None)));
        let base_url = row(&rows, "base url");
        assert_eq!(base_url.value, "https://acme.antithesis.com");
        assert_eq!(base_url.source.as_deref(), Some("derived"));
    }

    #[test]
    fn container_engine_row_auto_detects_when_unset() {
        let env = crate::settings::LoadedSettings::for_test_tenant("acme");
        let rows = resolve_settings(&Settings::for_test(env, Ok(None), Ok(None)));
        let engine = row(&rows, "container engine");
        assert_eq!(engine.value, "auto-detect");
        assert_eq!(engine.source, None);
    }

    #[test]
    fn profile_row_reflects_no_active_profile() {
        let rows = resolve_settings(&Settings::for_test(
            crate::settings::LoadedSettings::empty(),
            Ok(None),
            Ok(None),
        ));
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
    fn version_404_is_reachable_but_warns_an_old_tenant() {
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
        let checks = auth_checks(false, false, false);
        let settings = vec![Setting::sourced("tenant", "acme", "env")];
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
        // Settings rows are informational: a value and a source, no status.
        assert_eq!(value["settings"][0]["name"], "tenant");
        assert_eq!(value["settings"][0]["value"], "acme");
        assert_eq!(value["settings"][0]["source"], "env");
        assert!(value["settings"][0].get("status").is_none());
    }

    #[test]
    fn json_setting_omits_source_when_absent() {
        let setting = Setting::plain("profile", "(none)");
        let value = serde_json::to_value(&setting).unwrap();
        assert!(value.get("source").is_none());
    }
}
