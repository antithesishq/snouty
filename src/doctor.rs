use color_eyre::eyre::Result;
use log::debug;
use serde::Serialize;

use crate::api::{
    AntithesisApi, ApiVersion, HttpClient, MIN_CANCEL_RELEASE, MIN_EXEC_RELEASE,
    MIN_LAUNCH_PARAMS_RELEASE, MIN_SEARCH_RELEASE, MIN_STREAM_RELEASE, VersionError,
    normalize_base_url,
};
use crate::auth::{
    AuthenticationInfo, CredentialSource, CredentialStore, SourcedCredential, keychain_entry_name,
};
use crate::compose;
use crate::container;
use crate::login::{CliOAuthConfig, fetch_cli_config};
use crate::render::OutputOptions;
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
        // On a terminal, a long note wraps with its continuation lines hung
        // under the note's text, so the terminal never breaks it mid-word.
        let hang = 6 + note.level.label().len() + 2;
        let lines = match crate::render::prose_width() {
            Some(width) => crate::render::wrap_text(
                &note.text,
                width.saturating_sub(hang),
                crate::render::CodeSpans::Keep,
            ),
            None => vec![note.text.clone()],
        };
        let mut lines = lines.iter();
        eprintln!(
            "      {}: {}",
            label,
            lines.next().map_or("", |l| l.as_str())
        );
        for line in lines {
            eprintln!("{:hang$}{line}", "");
        }
    }
}

/// The kind of credential the credentials check found.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum CredentialKind {
    ApiKey,
    Oauth,
    GithubActionsOidc,
    Password,
}

impl From<&AuthenticationInfo> for CredentialKind {
    fn from(credential: &AuthenticationInfo) -> Self {
        match credential {
            AuthenticationInfo::ApiKey { .. } => Self::ApiKey,
            AuthenticationInfo::OAuth(_) => Self::Oauth,
            AuthenticationInfo::GithubActionsOidc(_) => Self::GithubActionsOidc,
            AuthenticationInfo::Password { .. } => Self::Password,
        }
    }
}

/// Which check a [`Check`] is. In `--json` it is the `id` key, which does not
/// change with the check's status.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(tag = "id", rename_all = "snake_case")]
enum CheckId {
    ContainerRuntime,
    DockerCompose,
    Tenant,
    Repository,
    /// `kind` is `None` when no credential is found.
    Credentials {
        #[serde(skip_serializing_if = "Option::is_none")]
        kind: Option<CredentialKind>,
    },
    CredentialSources,
    Api,
    EventsSearch,
    ExecuteCommand,
    CancelRun,
    LaunchParams,
    RunStream,
    OauthConfigured,
}

impl CheckId {
    /// The credentials check for the credential in use.
    fn credentials(in_use: &SourcedCredential) -> Self {
        Self::Credentials {
            kind: Some(CredentialKind::from(in_use.value())),
        }
    }
}

/// One health check: local tooling, settings validity, authentication, and
/// API connectivity. The headline `message` states the bare fact; the `notes`
/// carry explanations and how-tos. Checks own all of doctor's pass/warn/fail
/// semantics — the settings table is purely informational.
#[derive(Serialize)]
struct Check {
    #[serde(flatten)]
    id: CheckId,
    status: Status,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    notes: Vec<Note>,
}

impl Check {
    fn new(id: CheckId, status: Status, message: impl Into<String>) -> Self {
        Self {
            id,
            status,
            message: message.into(),
            notes: Vec::new(),
        }
    }

    fn ok(id: CheckId, message: impl Into<String>) -> Self {
        Self::new(id, Status::Ok, message)
    }

    fn warn(id: CheckId, message: impl Into<String>) -> Self {
        Self::new(id, Status::Warn, message)
    }

    fn fail(id: CheckId, message: impl Into<String>) -> Self {
        Self::new(id, Status::Error, message)
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
        // A long headline wraps like a note, hung under its text after the
        // `  ✓ ` prefix.
        let hang = 4;
        let lines = match crate::render::prose_width() {
            Some(width) => crate::render::wrap_text(
                &self.message,
                width.saturating_sub(hang),
                crate::render::CodeSpans::Keep,
            ),
            None => vec![self.message.clone()],
        };
        let mut lines = lines.iter();
        eprintln!(
            "  {} {}",
            status_icon(self.status),
            lines.next().map_or("", |l| l.as_str())
        );
        for line in lines {
            eprintln!("{:hang$}{line}", "");
        }
        print_notes(&self.notes);
    }
}

/// The full doctor report, as emitted by `--json`. The resolved settings
/// are `settings show`'s job.
#[derive(Serialize)]
struct Report<'a> {
    ok: bool,
    checks: &'a [Check],
}

/// The tenant is required by every command, so a missing one fails doctor.
/// (A broken settings file never reaches here — it fails at startup, before
/// doctor runs — so the resolved value is simply present or absent.)
/// `snouty login` sets tenant, repository, and credentials in one pass, so every
/// check that reports one of them missing points at it. Each says so in its own
/// words: on an unconfigured machine all three fire at once, and the same
/// sentence three times reads as noise rather than as advice.
fn tenant_check(tenant: Option<&str>) -> Check {
    match tenant {
        Some(_) => Check::ok(CheckId::Tenant, "tenant is set"),
        None => Check::fail(CheckId::Tenant, "tenant is not set").note(
            Level::Note,
            "run `snouty login` to set it, or set ANTITHESIS_TENANT or add it to a settings file",
        ),
    }
}

/// The repository (a container registry) is only needed to build and push a
/// config image (`snouty launch --config`), so a missing one is a warning, not
/// a failure — read-only use (`snouty runs`, `snouty debug`) doesn't need it.
fn repository_check(repository: Option<&str>) -> Check {
    match repository {
        Some(_) => Check::ok(CheckId::Repository, "repository is set"),
        None => Check::warn(CheckId::Repository, "repository is not set")
            .note(Level::Warning, "repository is needed for `snouty launch`")
            .note(
                Level::Note,
                "run `snouty login` to set it, or set ANTITHESIS_REPOSITORY",
            ),
    }
}

/// `selected` is the profile that the user selects, if any.
fn authn_checks(
    sources: &[SourcedCredential],
    selected: Option<&str>,
    github_actions: bool,
) -> Vec<Check> {
    let Some((credentials, behind)) = sources.split_first() else {
        return vec![missing_credentials_check(
            crate::auth::NO_CREDENTIALS_MESSAGE,
            github_actions,
        )];
    };

    // Only a credential in the environment hides one that the user expects:
    // a stored credential hides just its fallbacks, the default profile and
    // the GitHub Actions OIDC token. A blank API key cannot authenticate, so
    // it is nothing to fall back on.
    let (variables, hidden): (&[&str], Option<&SourcedCredential>) = match credentials.tag() {
        CredentialSource::Environment { variables } => (
            variables,
            behind.iter().find(|s| {
                matches!(s.tag(), CredentialSource::Stored(_)) && !s.value().has_blank_api_key()
            }),
        ),
        CredentialSource::Stored(_) => (&[], None),
    };

    let id = CheckId::credentials(credentials);
    let check = match credentials.value() {
        AuthenticationInfo::GithubActionsOidc(_) => enrich_with_source(
            Check::ok(id, "GitHub Actions OIDC token provided"),
            credentials,
            selected,
        ),
        AuthenticationInfo::OAuth(_) => enrich_with_source(
            Check::ok(id, "OAuth credentials used"),
            credentials,
            selected,
        ),
        credential if credential.has_blank_api_key() => {
            let check = Check::fail(id, "the API key is empty")
                .note(Level::Error, "an empty API key cannot authenticate");
            match credentials.tag() {
                // The lookup rejects a blank environment key with
                // `BlankApiKey`, which `collect_checks` reports.
                CredentialSource::Environment { .. } => {
                    enrich_with_source(check, credentials, selected)
                        .note(Level::Note, crate::auth::BlankApiKey::SUGGESTION)
                }
                CredentialSource::Stored(store) => enrich_with_source(check, credentials, selected)
                    .note(Level::Note, login_to_replace(store.profile())),
            }
        }
        AuthenticationInfo::ApiKey { .. } => {
            enrich_with_source(Check::ok(id, "API key provided"), credentials, selected)
        }
        // A username and password that hide a credential the API accepts:
        // the one thing to do is free that credential (issue #292).
        AuthenticationInfo::Password { .. }
            if let Some(hidden) = hidden
                && !matches!(hidden.value(), AuthenticationInfo::Password { .. }) =>
        {
            return vec![hidden_credential_check(
                credentials,
                variables,
                hidden,
                selected,
            )];
        }
        AuthenticationInfo::Password { username, .. } => {
            return vec![password_only_check(credentials, username, selected)];
        }
    };

    let mut checks = vec![check];
    if let Some(hidden) = hidden {
        checks.push(
            Check::warn(
                CheckId::CredentialSources,
                "the environment hides a stored credential",
            )
            .note(
                Level::Warning,
                format!(
                    "snouty is using the {} from {}",
                    credentials.value(),
                    describe_source(credentials.tag(), selected)
                ),
            )
            .note(Level::Note, unset_to_use(variables, hidden, selected)),
        );
    }
    checks
}

/// The check for a credential shortfall. Every credential kind except
/// username and password is accepted, so the notes name the ways to get one
/// rather than naming an API key alone.
fn missing_credentials_check(message: impl Into<String>, github_actions: bool) -> Check {
    Check::fail(CheckId::Credentials { kind: None }, message)
        .note(
            Level::Error,
            "snouty needs credentials to authenticate with Antithesis",
        )
        .note(
            Level::Note,
            crate::auth::no_credentials_suggestion(github_actions),
        )
}

/// The single check for a username and password that hide a credential the
/// API accepts. It replaces the shortfall, password, and conflict checks,
/// which together repeated the problem three ways and led with `snouty
/// login`, a step the user has already taken.
fn hidden_credential_check(
    in_use: &SourcedCredential,
    variables: &[&str],
    hidden: &SourcedCredential,
    selected: Option<&str>,
) -> Check {
    Check::warn(
        CheckId::credentials(in_use),
        format!(
            "the username and password are hiding your {}",
            hidden.value()
        ),
    )
    .note(
        Level::Warning,
        format!(
            "snouty uses the username and password from {}, which `snouty runs` refuses",
            describe_source(in_use.tag(), selected)
        ),
    )
    .note(Level::Note, unset_to_use(variables, hidden, selected))
}

/// The single check for a username and password with no credential the API
/// accepts behind them. It replaces the shortfall and password checks, which
/// together suggested `snouty login` twice. The next step depends on the
/// source: `snouty login` replaces a stored password, but an exported one
/// still hides the new credential until the user unsets it.
fn password_only_check(
    in_use: &SourcedCredential,
    username: &str,
    selected: Option<&str>,
) -> Check {
    let next_step = match in_use.tag() {
        CredentialSource::Environment { variables } => format!(
            "run `snouty login` to store an API key, then {}",
            unset(variables)
        ),
        CredentialSource::Stored(store) => login_to_replace(store.profile()),
    };
    enrich_with_source(
        Check::warn(
            CheckId::credentials(in_use),
            format!(
                "username/password for user [{username}] only works for `snouty launch` and `snouty debug`"
            ),
        )
        .note(
            Level::Warning,
            "username/password is deprecated, and `snouty runs` and other API commands refuse it",
        ),
        in_use,
        selected,
    )
    .note(Level::Note, next_step)
}

/// The next step for a stored password.
fn login_to_replace(profile: Option<&str>) -> String {
    format!(
        "run {} to replace it with an API key",
        login_command(profile)
    )
}

/// `snouty login` for `profile`. A bare `snouty login` writes the default
/// profile, so a named profile is passed on.
fn login_command(profile: Option<&str>) -> String {
    match profile {
        Some(p) => format!("`snouty login --profile {p}`"),
        None => "`snouty login`".to_owned(),
    }
}

fn unset(variables: &[&str]) -> String {
    format!("`unset {}`", variables.join(" "))
}

/// The one step that frees `hidden`, a stored credential that the
/// environment variables `variables` hide.
fn unset_to_use(variables: &[&str], hidden: &SourcedCredential, selected: Option<&str>) -> String {
    format!(
        "{} to use the {} in {}",
        unset(variables),
        hidden.value(),
        describe_source(hidden.tag(), selected)
    )
}

/// A phrase that reads after "read from" or after the name of a credential
/// kind.
fn describe_source(source: &CredentialSource, selected: Option<&str>) -> String {
    match source {
        CredentialSource::Environment { variables } => format!(
            "the `{}` environment variable{}",
            variables.join("` and `"),
            if variables.len() == 1 { "" } else { "s" }
        ),
        CredentialSource::Stored(CredentialStore::CredentialsFile { profile, .. }) => match profile
        {
            Some(p) => format!("the `{p}` profile of the credentials file"),
            // A selected profile with no credential falls back to these.
            None if selected.is_some() => "the default profile of the credentials file".to_owned(),
            None => "the credentials file".to_owned(),
        },
        CredentialSource::Stored(CredentialStore::Keychain { profile }) => format!(
            "system keychain entry named [{}]",
            keychain_entry_name(profile.as_deref())
        ),
    }
}

fn enrich_with_source(
    check: Check,
    credential: &SourcedCredential,
    selected: Option<&str>,
) -> Check {
    check.note(
        Level::Note,
        format!("read from {}", describe_source(credential.tag(), selected)),
    )
}

/// Binary health checks: local tooling, the required settings
/// (tenant/repository), and authentication. `settings show` reports the
/// resolved values themselves.
fn collect_checks(settings: &Settings) -> (Vec<Check>, Option<SourcedCredential>) {
    let mut checks: Vec<Check> = Vec::new();

    // Container runtime (for building/pushing images). Report the real engine
    // (engine_kind), not the invoking binary (name): for podman-in-disguise the
    // command is `docker` but the engine is podman — and this must agree with
    // the engine `launch`/`validate` announce, which also uses engine_kind.
    match container::runtime(settings) {
        Ok(rt) => checks.push(Check::ok(
            CheckId::ContainerRuntime,
            format!("Container runtime: {} detected", rt.engine_kind()),
        )),
        Err(e) => checks.push(
            Check::fail(CheckId::ContainerRuntime, "Container runtime not detected")
                .note(Level::Error, e.to_string()),
        ),
    }

    // Docker Compose v2 (required for compose configs). Resolves the standalone
    // `docker-compose` binary or the `docker compose` CLI plugin, and reports
    // which one was picked.
    match compose::DockerCompose::probe() {
        Ok((name, version)) => {
            checks.push(Check::ok(
                CheckId::DockerCompose,
                format!("{name}: {version}"),
            ));
        }
        Err(e) => checks.push(
            Check::fail(CheckId::DockerCompose, "no usable Docker Compose")
                .note(Level::Error, e.to_string()),
        ),
    }

    // Required settings. tenant is needed by every command; repository is
    // launch-only, so a missing one is a warning.
    checks.push(tenant_check(settings.tenant()));
    checks.push(repository_check(settings.repository()));

    // Authentication (synchronous-only by design).
    let github_actions = crate::auth::in_github_actions();
    let in_use = match AuthenticationInfo::available_ambient_credentials(settings.profile()) {
        Ok(sources) => {
            checks.extend(authn_checks(&sources, settings.profile(), github_actions));
            sources.into_iter().next()
        }
        // A blank environment key hides every other credential, so its fix
        // is the variable, not a way to get a credential.
        Err(err) if err.downcast_ref::<crate::auth::BlankApiKey>().is_some() => {
            checks.push(
                Check::fail(
                    CheckId::Credentials {
                        kind: Some(CredentialKind::ApiKey),
                    },
                    err.to_string(),
                )
                .note(Level::Note, crate::auth::BlankApiKey::SUGGESTION),
            );
            None
        }
        Err(err) => {
            checks.push(missing_credentials_check(err.to_string(), github_actions));
            None
        }
    };

    (checks, in_use)
}

/// An API or a flag that some commands assume the tenant serves, from a
/// minimum tenant release, instead of probing for it.
struct ReleaseFloor {
    id: CheckId,
    serves: &'static str,
    min: (u64, u64),
    consequence: &'static str,
}

const RELEASE_FLOORS: [ReleaseFloor; 5] = [
    ReleaseFloor {
        id: CheckId::EventsSearch,
        serves: "the events-search API",
        min: MIN_SEARCH_RELEASE,
        consequence: "`runs search` and `runs events` can fail or hang",
    },
    ReleaseFloor {
        id: CheckId::ExecuteCommand,
        serves: "the execute-command `container` field",
        min: MIN_EXEC_RELEASE,
        consequence: "`runs exec` fails",
    },
    ReleaseFloor {
        id: CheckId::CancelRun,
        serves: "the cancel-run API",
        min: MIN_CANCEL_RELEASE,
        consequence: "`runs cancel` fails",
    },
    ReleaseFloor {
        id: CheckId::LaunchParams,
        serves: "the compute-hours, setup-timeout and debug-duration params",
        min: MIN_LAUNCH_PARAMS_RELEASE,
        consequence: "`launch --max-compute-hours`, `launch --setup-timeout` and \
                      `debug --duration` have no effect",
    },
    ReleaseFloor {
        id: CheckId::RunStream,
        serves: "property streaming",
        min: MIN_STREAM_RELEASE,
        consequence: "`runs stream` is not available",
    },
];

fn release_floor_checks(version: &ApiVersion) -> Vec<Check> {
    RELEASE_FLOORS
        .iter()
        .filter_map(|floor| release_floor_check(version, floor))
        .collect()
}

/// Verify that the tenant release meets `floor`. An unparsable release
/// version reports nothing, because the check would guess. It warns rather
/// than fails, because every other command still works on such a tenant.
fn release_floor_check(version: &ApiVersion, floor: &ReleaseFloor) -> Option<Check> {
    if version.release? >= floor.min {
        return None;
    }
    let (major, minor) = floor.min;
    Some(
        Check::warn(
            floor.id,
            format!("tenant release is too old for {}", floor.serves),
        )
        .note(
            Level::Warning,
            format!(
                "tenant release {} predates {} (release {major}.{minor}) — {}",
                version.release_version, floor.serves, floor.consequence
            ),
        )
        .note(
            Level::Note,
            "request that your Antithesis tenant is upgraded",
        ),
    )
}

/// Map a `GET /api/version` probe into a check. Reaching the endpoint at all —
/// even a 404 on an older backend that lacks it — proves connectivity, so only
/// rejected auth, an API error, or a failure to reach the API is a problem.
/// Pure over the result so it can be unit-tested without the network.
fn version_check(
    host: &str,
    in_use: &SourcedCredential,
    profile: Option<&str>,
    result: std::result::Result<ApiVersion, VersionError>,
) -> Check {
    match result {
        Ok(v) => Check::ok(CheckId::Api, "Antithesis API reachable")
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
        Err(VersionError::Http(404)) => Check::ok(CheckId::Api, "Antithesis API reachable").note(
            Level::Warning,
            "GET /api/version returned 404 — your tenant likely predates version \
             reporting (added in release 56); if you expect a current tenant, a \
             proxy or route may be intercepting the request",
        ),
        Err(VersionError::Http(status @ (401 | 403))) => {
            Check::fail(CheckId::Api, "Antithesis API rejected authentication")
                .note(Level::Error, format!("the API returned HTTP {status}"))
                .note(Level::Note, rejected_credential_next_step(in_use, profile))
        }
        // 5xx: we reached something, but it's erroring — connectivity is broken
        // by a server error and auth status is unknown.
        Err(VersionError::Http(status)) if (500..=599).contains(&status) => {
            Check::fail(CheckId::Api, "Antithesis API unavailable").note(
                Level::Error,
                format!("the API returned HTTP {status} (server error)"),
            )
        }
        // Any other unexpected status.
        Err(VersionError::Http(status)) => Check::fail(CheckId::Api, "Antithesis API error").note(
            Level::Error,
            format!("the API returned an unexpected HTTP {status}"),
        ),
        // The server answered, but not with the version payload — the network
        // is fine, so don't send the user debugging connectivity.
        Err(VersionError::BadResponse(err)) => {
            Check::fail(CheckId::Api, "Antithesis API sent an unexpected response").note(
                Level::Error,
                format!("{host} answered, but the response was invalid: {err}"),
            )
        }
        // Couldn't connect at all — connectivity is broken.
        Err(VersionError::Unreachable(err)) => {
            Check::fail(CheckId::Api, "Antithesis API unreachable")
                .note(Level::Error, format!("could not connect to {host}: {err}"))
        }
    }
}

/// The one step that fixes `in_use` when the tenant of `profile` rejects it.
fn rejected_credential_next_step(in_use: &SourcedCredential, profile: Option<&str>) -> String {
    match (in_use.value(), in_use.tag()) {
        (AuthenticationInfo::GithubActionsOidc(_), _) => {
            "ask Antithesis support to trust GitHub Actions OIDC tokens from this repository"
                .to_owned()
        }
        // `snouty login` cannot help: the environment hides what it stores.
        (credential, CredentialSource::Environment { variables }) => format!(
            "set `{}` to a valid {credential}",
            variables.join("` and `")
        ),
        // A named profile with no credential uses the default profile's, so
        // the login goes to the checked profile.
        (_, CredentialSource::Stored(_)) => {
            format!("run {} to fix authentication", login_command(profile))
        }
    }
}

pub async fn cmd_doctor(
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
    offline: bool,
) -> Result<()> {
    let (mut checks, in_use) = collect_checks(settings);

    if !offline {
        // Connectivity + version check (network). Skipped with --offline. Only
        // runs when the resolved credentials work against the full API:
        // /api/version, like every endpoint but launch, rejects username/password
        // auth, so probing it with those credentials would only yield a misleading
        // 403 — and the auth checks above already tell deprecated-credential and
        // unauthenticated users to set a key. The client is built from the
        // resolved settings (base url / tenant), and `verbose` logs the
        // request/response.
        let mut api_failed = false;
        if let Ok(api) = AntithesisApi::new(settings, verbose)
            && let Some(in_use) = &in_use
        {
            let host = api.host();
            // Renew here so that a rejected renewal fails the credentials
            // check, not the API check. The version request would fail the same way, so
            // skip it.
            let renewal = if api.authentication().expired() {
                api.refresh_authentication().await
            } else {
                Ok(())
            };
            if let Err(err) = renewal {
                api_failed = true;
                let auth = api.authentication();
                // An OAuth token is refreshed by the tenant, so a request
                // that got no response means that the tenant is unreachable.
                if matches!(auth, AuthenticationInfo::OAuth(_))
                    && err
                        .downcast_ref::<reqwest::Error>()
                        .is_some_and(|e| e.status().is_none() && !e.is_decode())
                {
                    checks.push(version_check(
                        &host,
                        in_use,
                        settings.profile(),
                        Err(VersionError::Unreachable(err.root_cause().to_string())),
                    ));
                } else {
                    let check = checks
                        .iter_mut()
                        .find(|c| matches!(c.id, CheckId::Credentials { .. }))
                        .expect(
                            "the client uses the credential that the credentials check reports",
                        );
                    check.status = Status::Error;
                    check.message = match auth {
                        AuthenticationInfo::GithubActionsOidc(_) => {
                            "could not get a GitHub Actions OIDC token".to_owned()
                        }
                        _ => format!("the {auth} expired and could not be renewed"),
                    };
                    check.notes.insert(
                        0,
                        Note {
                            level: Level::Error,
                            text: format!("{err:#}"),
                        },
                    );
                }
            } else {
                let version = api.get_version().await;
                if let Ok(version) = &version {
                    checks.extend(release_floor_checks(version));
                }
                api_failed = matches!(version, Err(VersionError::Unreachable(_)));
                checks.push(version_check(&host, in_use, settings.profile(), version));
            }
        }

        // A client that fails to build (bad proxy URL or extra header) skips
        // the API check too. Skip the probe when the tenant could be
        // unreachable: the probe would only wait out its timeout. Skip it
        // when a credential works, too: that user does not need OAuth login.
        let credential_works = checks
            .iter()
            .any(|c| c.id == CheckId::Api && c.status == Status::Ok);
        if !api_failed
            && !credential_works
            && let Some(base_url) = settings.base_url()
            && let Ok(client) = HttpClient::new(settings, verbose)
        {
            match fetch_cli_config(&client, &normalize_base_url(base_url)).await {
                Ok(CliOAuthConfig::Disabled) => checks.push(
                    Check::warn(
                        CheckId::OauthConfigured,
                        "CLI OAuth login is not enabled for this tenant",
                    )
                    .note(
                        Level::Note,
                        "request that your tenant administrator enable OAuth login for the CLI",
                    ),
                ),
                Ok(CliOAuthConfig::Fixed { .. } | CliOAuthConfig::Ephemeral) => {}
                // An error does not tell whether OAuth is enabled.
                Err(err) => debug!("could not read the tenant's OAuth configuration: {err:#}"),
            }
        }
    }

    // Only the checks carry pass/warn/fail; the settings table is informational.
    let errors = checks.iter().filter(|c| c.status == Status::Error).count();
    let warnings = checks.iter().filter(|c| c.status == Status::Warn).count();

    if json {
        let report = Report {
            ok: errors == 0,
            checks: &checks,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        eprintln!("Checks");
        for check in &checks {
            check.print();
        }
        eprintln!();
        eprintln!("Run `snouty settings show` to see your settings.");
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
    use crate::auth::{
        API_KEY_VAR_NAME, GithubActionsOidcToken, PASSWORD_VAR_NAME, USERNAME_VAR_NAME,
    };
    use crate::testutils::auth::oauth_credential_in;
    use crate::testutils::fake_secrets::{
        FAKE_ACCESS_TOKEN_A, FAKE_API_KEY_A, FAKE_API_KEY_B, FAKE_OIDC_REQUEST_TOKEN, FAKE_PASSWORD,
    };

    use super::*;
    use crate::tag::Tag;

    // ---- auth_checks (env-only auth) -----------------------------------

    #[test]
    fn auth_api_key_set_is_a_single_bare_ok_check() {
        let checks = authn_checks(
            &[from_env(
                AuthenticationInfo::ApiKey {
                    api_key: FAKE_API_KEY_A.to_owned(),
                },
                vec![API_KEY_VAR_NAME],
            )],
            None,
            false,
        );
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Ok);
        assert!(checks[0].message.contains("API key provided"));
    }

    /// A credential read from a file names its profile, `default` included,
    /// and the default profile when a selected profile falls back to it. It
    /// does not name the path of the file.
    #[test]
    fn auth_from_a_file_names_a_named_profile_but_no_path() {
        let note_for = |profile: Option<&str>, selected: Option<&str>| {
            let checks = authn_checks(
                &[from_file(
                    AuthenticationInfo::ApiKey {
                        api_key: FAKE_API_KEY_A.to_owned(),
                    },
                    profile,
                )],
                selected,
                false,
            );
            checks[0]
                .notes
                .iter()
                .map(|n| n.text.clone())
                .collect::<Vec<_>>()
                .join(" ")
        };
        let note = |profile| note_for(profile, profile);
        assert_eq!(note(None), "read from the credentials file");
        // `--profile default` selects `[profile.default]`, not the top level.
        assert_eq!(
            note(Some("default")),
            "read from the `default` profile of the credentials file"
        );
        assert_eq!(
            note(Some("prod")),
            "read from the `prod` profile of the credentials file"
        );
        assert_eq!(
            note_for(None, Some("prod")),
            "read from the default profile of the credentials file"
        );
    }

    /// Only a username and password: one warning, whose next step depends on
    /// where they came from.
    #[test]
    fn a_password_alone_is_one_check_with_one_next_step() {
        let notes = |source: SourcedCredential| {
            let checks = authn_checks(&[source], None, false);
            assert_eq!(
                checks.len(),
                1,
                "got: {:?}",
                checks.iter().map(|c| c.id).collect::<Vec<_>>()
            );
            let check = &checks[0];
            assert_eq!(
                check.id,
                CheckId::Credentials {
                    kind: Some(CredentialKind::Password)
                }
            );
            assert_eq!(check.status, Status::Warn);
            assert_eq!(
                check.message,
                "username/password for user [user] only works for `snouty launch` and `snouty debug`"
            );
            check
                .notes
                .iter()
                .map(|n| (n.level, n.text.clone()))
                .collect::<Vec<_>>()
        };
        let deprecated = (
            Level::Warning,
            "username/password is deprecated, and `snouty runs` and other API commands refuse it"
                .to_owned(),
        );
        assert_eq!(
            notes(env_password_source()),
            [
                deprecated.clone(),
                (
                    Level::Note,
                    "read from the `ANTITHESIS_USERNAME` and `ANTITHESIS_PASSWORD` \
                    environment variables"
                        .to_owned()
                ),
                (
                    Level::Note,
                    "run `snouty login` to store an API key, then \
                    `unset ANTITHESIS_USERNAME ANTITHESIS_PASSWORD`"
                        .to_owned()
                ),
            ]
        );
        assert_eq!(
            notes(from_file(
                AuthenticationInfo::Password {
                    username: "user".to_owned(),
                    password: FAKE_PASSWORD.to_owned(),
                },
                None
            )),
            [
                deprecated,
                (Level::Note, "read from the credentials file".to_owned()),
                (
                    Level::Note,
                    "run `snouty login` to replace it with an API key".to_owned()
                ),
            ]
        );
        // A bare `snouty login` writes the default profile, so the next step
        // names the profile that holds the password.
        let keychain = notes(from_keychain(
            AuthenticationInfo::Password {
                username: "user".to_owned(),
                password: FAKE_PASSWORD.to_owned(),
            },
            Some("prod"),
        ));
        assert_eq!(
            keychain.last().unwrap(),
            &(
                Level::Note,
                "run `snouty login --profile prod` to replace it with an API key".to_owned()
            )
        );
    }

    #[test]
    fn auth_nothing_set_errors_and_only_mentions_api_key() {
        let checks = [missing_credentials_check("PANIC PANIC PANIC", false)];
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Error);
        assert!(checks[0].message.contains("PANIC PANIC PANIC"));
        assert!(checks[0].notes.iter().any(|n| n.level == Level::Error));
        assert!(
            checks[0]
                .notes
                .iter()
                .any(|n| n.text.contains("snouty login")),
            "an unconfigured machine must be told the command that configures it"
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

    /// A user who meets both messages reads one wording, not two.
    #[test]
    fn auth_no_source_reports_the_shared_no_credentials_message() {
        let checks = authn_checks(&[], None, false);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, Status::Error);
        assert_eq!(checks[0].message, crate::auth::NO_CREDENTIALS_MESSAGE);
    }

    /// `snouty login` cannot run in a GitHub Actions job, so there the
    /// notes name the OIDC permission instead.
    #[test]
    fn no_credential_in_github_actions_names_the_oidc_permission() {
        let notes = |github_actions| {
            authn_checks(&[], None, github_actions)[0]
                .notes
                .iter()
                .map(|n| n.text.clone())
                .collect::<Vec<_>>()
                .join(" ")
        };
        let ci = notes(true);
        assert!(ci.contains("id-token: write"), "{ci}");
        assert!(!ci.contains("snouty login"), "{ci}");
        let local = notes(false);
        assert!(local.contains("snouty login"), "{local}");
        assert!(!local.contains("id-token"), "{local}");
    }

    fn from_env(value: AuthenticationInfo, variables: Vec<&'static str>) -> SourcedCredential {
        value.with_tag(CredentialSource::Environment { variables })
    }

    fn from_file(value: AuthenticationInfo, profile: Option<&str>) -> SourcedCredential {
        value.with_tag(CredentialSource::Stored(CredentialStore::CredentialsFile {
            path: std::path::PathBuf::from("/tmp/credentials.toml"),
            profile: profile.map(str::to_owned),
        }))
    }

    fn from_keychain(value: AuthenticationInfo, profile: Option<&str>) -> SourcedCredential {
        value.with_tag(CredentialSource::Stored(CredentialStore::Keychain {
            profile: profile.map(str::to_owned),
        }))
    }

    fn env_password_source() -> SourcedCredential {
        from_env(
            AuthenticationInfo::Password {
                username: "user".to_owned(),
                password: FAKE_PASSWORD.to_owned(),
            },
            vec![USERNAME_VAR_NAME, PASSWORD_VAR_NAME],
        )
    }

    fn file_api_key_source() -> SourcedCredential {
        from_file(
            AuthenticationInfo::ApiKey {
                api_key: FAKE_API_KEY_A.to_owned(),
            },
            None,
        )
    }

    #[test]
    fn one_credential_source_reports_no_conflict() {
        let checks = authn_checks(&[file_api_key_source()], None, false);
        assert!(!checks.iter().any(|c| c.id == CheckId::CredentialSources));
    }

    /// The case issue #292 reported: `snouty login` stored an API key while a
    /// legacy username/password was still exported. One check names the
    /// problem and the one step that fixes it.
    #[test]
    fn a_password_hiding_an_api_key_is_one_check_with_one_next_step() {
        let checks = authn_checks(&[env_password_source(), file_api_key_source()], None, false);
        assert_eq!(
            checks.len(),
            1,
            "got: {:?}",
            checks.iter().map(|c| c.id).collect::<Vec<_>>()
        );
        let check = &checks[0];
        assert_eq!(
            check.id,
            CheckId::Credentials {
                kind: Some(CredentialKind::Password)
            }
        );
        assert_eq!(check.status, Status::Warn);
        assert_eq!(
            check.message,
            "the username and password are hiding your API key"
        );
        assert_eq!(
            check
                .notes
                .iter()
                .map(|n| (n.level, n.text.as_str()))
                .collect::<Vec<_>>(),
            [
                (
                    Level::Warning,
                    "snouty uses the username and password from the \
                    `ANTITHESIS_USERNAME` and `ANTITHESIS_PASSWORD` environment variables, \
                    which `snouty runs` refuses"
                ),
                (
                    Level::Note,
                    "`unset ANTITHESIS_USERNAME ANTITHESIS_PASSWORD` to use the API key in \
                    the credentials file"
                ),
            ]
        );
    }

    fn api_key(api_key: &str) -> AuthenticationInfo {
        AuthenticationInfo::ApiKey {
            api_key: api_key.to_owned(),
        }
    }

    fn env_api_key_source() -> SourcedCredential {
        from_env(api_key(FAKE_API_KEY_B), vec![API_KEY_VAR_NAME])
    }

    fn oidc_source() -> SourcedCredential {
        from_env(
            AuthenticationInfo::GithubActionsOidc(GithubActionsOidcToken::new(
                "http://127.0.0.1:1/token".to_owned(),
                FAKE_OIDC_REQUEST_TOKEN.to_owned(),
            )),
            vec![
                "ACTIONS_ID_TOKEN_REQUEST_URL",
                "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
            ],
        )
    }

    /// An exported credential that hides a stored one is the trap: the one
    /// step is to unset it.
    #[test]
    fn an_environment_credential_that_hides_a_stored_one_is_a_warning() {
        let checks = authn_checks(&[env_api_key_source(), file_api_key_source()], None, false);
        let check = checks
            .iter()
            .find(|c| c.id == CheckId::CredentialSources)
            .expect("the hidden credential is reported");
        assert_eq!(check.status, Status::Warn);
        assert_eq!(
            check.notes.last().unwrap().text,
            "`unset ANTITHESIS_API_KEY` to use the API key in \
            the credentials file"
        );
    }

    /// The default profile and the GitHub Actions OIDC token are fallbacks,
    /// so a credential that hides only them is no conflict.
    #[test]
    fn a_credential_that_hides_only_its_fallbacks_is_no_conflict() {
        for sources in [
            vec![
                from_keychain(api_key(FAKE_API_KEY_A), Some("prod")),
                from_keychain(api_key(FAKE_API_KEY_B), None),
                oidc_source(),
            ],
            vec![file_api_key_source(), oidc_source()],
            vec![env_api_key_source(), oidc_source()],
        ] {
            let checks = authn_checks(&sources, None, false);
            assert_eq!(
                checks.iter().map(|c| c.status).collect::<Vec<_>>(),
                [Status::Ok],
                "got: {:?}",
                checks.iter().map(|c| &c.message).collect::<Vec<_>>()
            );
        }
    }

    /// A stored credential that cannot work is replaced with `snouty login`
    /// for its profile, also when the default profile holds a working one.
    #[test]
    fn a_stored_credential_that_cannot_work_is_replaced_by_login() {
        let password = AuthenticationInfo::Password {
            username: "user".to_owned(),
            password: FAKE_PASSWORD.to_owned(),
        };
        for (in_use, status) in [(password, Status::Warn), (api_key(""), Status::Error)] {
            let checks = authn_checks(
                &[
                    from_file(in_use, Some("prod")),
                    from_file(api_key(FAKE_API_KEY_A), None),
                ],
                Some("prod"),
                false,
            );
            assert_eq!(checks.len(), 1);
            assert_eq!(checks[0].status, status);
            assert_eq!(
                checks[0].notes.last().unwrap().text,
                "run `snouty login --profile prod` to replace it with an API key"
            );
        }
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
        assert!(
            check.notes.iter().any(|n| n.text.contains("snouty login")),
            "an unset tenant must point at the command that sets it"
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
        assert!(check.notes.iter().any(|n| n.text.contains("snouty login")));
    }

    // ---- release_floor_check -------------------------------------------

    #[test]
    fn release_floor_check_fires_only_on_a_known_gap() {
        let version = |release: &str| ApiVersion::new("v1".into(), release.into());
        let [search, exec, cancel, launch_params, run_stream] = &RELEASE_FLOORS;
        assert!(release_floor_check(&version("62.2"), search).is_none());
        assert!(release_floor_check(&version("63.0"), search).is_none());
        // 58.11 ships the endpoint but not its contract, so it is too old.
        assert!(release_floor_check(&version("58.11"), search).is_some());
        let check = release_floor_check(&version("60.1"), search).unwrap();
        assert_eq!(check.status, Status::Warn);
        assert!(
            check.notes[0].text.contains("60.1"),
            "{}",
            check.notes[0].text
        );
        assert!(
            check.notes[0].text.contains("`runs search`"),
            "{}",
            check.notes[0].text
        );
        // An unparsable release says nothing rather than guessing.
        assert!(release_floor_check(&version("unknown"), search).is_none());

        assert!(release_floor_check(&version("64.0"), exec).is_none());
        let check = release_floor_check(&version("63.3"), exec).unwrap();
        assert!(
            check.notes[0].text.contains("`runs exec`"),
            "{}",
            check.notes[0].text
        );

        assert!(release_floor_check(&version("63.0"), cancel).is_none());
        let check = release_floor_check(&version("62.2"), cancel).unwrap();
        assert!(
            check.notes[0].text.contains("`runs cancel`"),
            "{}",
            check.notes[0].text
        );

        assert!(release_floor_check(&version("66.0"), launch_params).is_none());
        let check = release_floor_check(&version("65.9"), launch_params).unwrap();
        assert!(
            check.notes[0].text.contains("`debug --duration`"),
            "{}",
            check.notes[0].text
        );

        assert!(release_floor_check(&version("65.3"), run_stream).is_none());
        let check = release_floor_check(&version("65.2"), run_stream).unwrap();
        assert!(
            check.notes[0].text.contains("`runs stream`"),
            "{}",
            check.notes[0].text
        );
    }

    #[test]
    fn exec_release_floor_warns_below_64() {
        let ids = |release: String| -> Vec<CheckId> {
            release_floor_checks(&ApiVersion::new("v1".into(), release))
                .into_iter()
                .map(|check| check.id)
                .collect()
        };
        let (major, minor) = MIN_EXEC_RELEASE;
        let newer = [CheckId::LaunchParams, CheckId::RunStream];
        assert_eq!(
            ids(format!("{}.99", major - 1)),
            [&[CheckId::ExecuteCommand][..], &newer].concat()
        );
        assert_eq!(ids(format!("{major}.{minor}")), newer);
    }

    #[test]
    fn version_ok_reports_both_versions() {
        let check = version_check(
            "tenant.antithesis.com",
            &env_api_key_source(),
            None,
            Ok(ApiVersion::new("v1".into(), "56.0".into())),
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
        let check = version_check(
            "tenant.antithesis.com",
            &env_api_key_source(),
            None,
            Err(VersionError::Http(404)),
        );
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
            let check = version_check(
                "tenant.antithesis.com",
                &env_api_key_source(),
                None,
                Err(VersionError::Http(status)),
            );
            assert_eq!(check.status, Status::Error);
            assert!(
                check
                    .notes
                    .iter()
                    .any(|n| n.text.contains(&status.to_string()))
            );
        }
    }

    /// A stored credential gets `snouty login`, with `--profile` for a named
    /// profile. Another source gets the step that fixes it.
    #[test]
    fn version_auth_rejection_names_the_fix_for_the_source() {
        let note_for = |in_use: SourcedCredential, profile| {
            version_check(
                "tenant.antithesis.com",
                &in_use,
                profile,
                Err(VersionError::Http(401)),
            )
            .notes
            .into_iter()
            .find(|n| n.level == Level::Note)
            .expect("a rejection carries a next step")
            .text
        };
        let note = |profile| note_for(file_api_key_source(), profile);
        assert_eq!(note(None), "run `snouty login` to fix authentication");
        assert_eq!(
            note(Some("default")),
            "run `snouty login --profile default` to fix authentication"
        );
        assert_eq!(
            note(Some("staging")),
            "run `snouty login --profile staging` to fix authentication"
        );
        assert_eq!(
            note_for(env_api_key_source(), Some("staging")),
            "set `ANTITHESIS_API_KEY` to a valid API key"
        );
        assert_eq!(
            note_for(oidc_source(), None),
            "ask Antithesis support to trust GitHub Actions OIDC tokens from this repository"
        );
    }

    #[test]
    fn version_bad_response_is_not_reported_as_unreachable() {
        let check = version_check(
            "tenant.antithesis.com",
            &env_api_key_source(),
            None,
            Err(VersionError::BadResponse(
                "invalid type: null, expected struct ApiVersion".into(),
            )),
        );
        assert_eq!(check.status, Status::Error);
        assert!(check.message.contains("unexpected response"));
        assert!(!check.message.contains("unreachable"));
        assert!(check.notes.iter().any(|n| {
            n.text.contains("tenant.antithesis.com")
                && n.text
                    .contains("invalid type: null, expected struct ApiVersion")
        }));
    }

    #[test]
    fn version_unreachable_names_the_host_and_includes_the_error() {
        let check = version_check(
            "tenant.antithesis.com",
            &env_api_key_source(),
            None,
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
        let check = version_check(
            "tenant.antithesis.com",
            &env_api_key_source(),
            None,
            Err(VersionError::Http(503)),
        );
        assert_eq!(check.status, Status::Error);
        assert!(check.message.contains("unavailable"));
        assert!(check.notes.iter().any(|n| n.text.contains("503")));
    }

    #[test]
    fn version_unexpected_status_is_an_error_with_status() {
        let check = version_check(
            "tenant.antithesis.com",
            &env_api_key_source(),
            None,
            Err(VersionError::Http(429)),
        );
        assert_eq!(check.status, Status::Error);
        assert!(check.notes.iter().any(|n| n.text.contains("429")));
    }

    #[test]
    fn the_credentials_check_carries_the_kind_in_use() {
        let env = |value: AuthenticationInfo| from_env(value, vec![API_KEY_VAR_NAME]);
        let dir = tempfile::tempdir().unwrap();
        let cases = [
            (vec![], Status::Error, None),
            (
                vec![env(AuthenticationInfo::ApiKey {
                    api_key: FAKE_API_KEY_A.to_owned(),
                })],
                Status::Ok,
                Some(CredentialKind::ApiKey),
            ),
            (
                vec![env(AuthenticationInfo::ApiKey {
                    api_key: String::new(),
                })],
                Status::Error,
                Some(CredentialKind::ApiKey),
            ),
            (
                vec![env(oauth_credential_in(
                    dir.path(),
                    FAKE_ACCESS_TOKEN_A,
                    None,
                ))],
                Status::Ok,
                Some(CredentialKind::Oauth),
            ),
            (
                vec![env(AuthenticationInfo::GithubActionsOidc(
                    GithubActionsOidcToken::new(
                        "http://127.0.0.1:1/token".to_owned(),
                        FAKE_OIDC_REQUEST_TOKEN.to_owned(),
                    ),
                ))],
                Status::Ok,
                Some(CredentialKind::GithubActionsOidc),
            ),
            (
                vec![env_password_source()],
                Status::Warn,
                Some(CredentialKind::Password),
            ),
            (
                vec![env_password_source(), file_api_key_source()],
                Status::Warn,
                Some(CredentialKind::Password),
            ),
        ];
        for (sources, status, kind) in cases {
            let checks = authn_checks(&sources, None, false);
            assert_eq!(checks[0].id, CheckId::Credentials { kind });
            assert_eq!(checks[0].status, status);
        }
    }

    // ---- JSON report ----------------------------------------------------

    #[test]
    fn json_report_carries_checks() {
        let checks = vec![missing_credentials_check("PANIC PANIC PANIC", false)];
        let report = Report {
            ok: false,
            checks: &checks,
        };
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["ok"], false);
        assert_eq!(value["checks"][0]["id"], "credentials");
        assert!(value["checks"][0].get("kind").is_none());
        assert_eq!(value["checks"][0]["status"], "error");
        assert_eq!(value["checks"][0]["notes"][0]["level"], "error");
    }

    /// The id comes first, a credential kind sits beside it, and a check
    /// without one has no `kind` key.
    #[test]
    fn json_check_ids_flatten_into_the_check() {
        let api_key = Check::ok(
            CheckId::Credentials {
                kind: Some(CredentialKind::ApiKey),
            },
            "API key provided",
        );
        assert_eq!(
            serde_json::to_string(&api_key).unwrap(),
            r#"{"id":"credentials","kind":"api_key","status":"ok","message":"API key provided"}"#
        );
        let tenant = Check::ok(CheckId::Tenant, "tenant is set");
        assert_eq!(
            serde_json::to_string(&tenant).unwrap(),
            r#"{"id":"tenant","status":"ok","message":"tenant is set"}"#
        );
    }
}
