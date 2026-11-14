use std::io::{self, IsTerminal};
use std::time::Duration;

use askama::Template;
use axum::{
    Router,
    extract::RawQuery,
    response::{Html, IntoResponse},
    routing::get,
};
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use bytes::Bytes;
use color_eyre::Section;
use color_eyre::eyre::{Context, Report, Result, eyre};
use http::{StatusCode, header};
use inquire::validator::Validation;
use inquire::{Confirm, Password, PasswordDisplayMode, Select, Text};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::{
    api::{
        AntithesisApi, CONNECT_TIMEOUT, HttpClient, RegistryResponse, ResponseCache,
        normalize_base_url,
    },
    auth::{
        AuthenticationInfo, CredentialSource, CredentialStore, OIDC_REJECTED_NEXT_STEP,
        PersistableCredentials, in_github_actions, is_blank_api_key, no_credentials_suggestion,
        persist,
    },
    env,
    error::{api_error_status, rejects_credential, user_error},
    registry::{Registry, TenantRegistry},
    render::{OutputOptions, sanitize},
    settings::{
        ANTITHESIS_PROFILE_ENV_VAR_NAME, Settings, tenant_base_url, update_settings_in_global_file,
        validate_tenant_host,
    },
    tag::{Tag, Tagged},
    util::root_cause,
};

/// Longer than [`CONNECT_TIMEOUT`], so that a host that cannot be reached
/// fails with a connect error, and only a slow answer hits this limit.
const OAUTH_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const _: () = assert!(OAUTH_HTTP_TIMEOUT.as_secs() > CONNECT_TIMEOUT.as_secs());

/// The longest wait for the registry request. The API client has only a
/// connect timeout, and a tenant that accepts a connection but does not answer
/// would otherwise hold login or doctor forever.
pub(crate) const REGISTRY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait for the browser to redirect back to the loopback server
/// before giving up — generous enough for the user to complete an interactive
/// sign-in (including MFA).
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(300);

/// Wrapping our TUI (`inquire`) in a trait so that it can be subbed out for testing
trait Prompter {
    /// Whether we're attached to an interactive terminal.
    fn is_interactive(&self) -> bool;

    /// A free-text line, pre-filled with `default` when the user just hits enter.
    /// The prompt asks again on a blank answer, and on an answer that `check`
    /// refuses, with the message that `check` gives. The result is never
    /// blank, and `check` accepts it.
    fn input(&self, prompt: &str, default: Option<&str>, check: InputCheck) -> Result<String>;

    /// A single-choice menu over `items`, initially highlighting `default`.
    /// Returns `None` when the user cancels (Esc).
    fn select(&self, prompt: &str, items: Vec<String>, default: usize) -> Result<Option<usize>>;

    /// A masked secret (each character echoes as `*`). There is deliberately
    /// no confirmation round: an API key is a long generated string that is
    /// pasted, not typed twice. Returns `None` when the user skips it (Esc).
    ///
    /// `hint` is a display-only stand-in for a secret already stored (see
    /// [`secret_hint`]). When it is set, the prompt shows it and says that an
    /// empty answer keeps the stored secret. The secret itself never reaches
    /// the prompter.
    ///
    /// The prompt asks again when [`refuses_blank_secret`] refuses the answer.
    fn secret(&self, prompt: &str, hint: Option<&str>) -> Result<Option<String>>;

    /// A yes/no question that defaults to no. Esc also answers no.
    fn confirm(&self, prompt: &str) -> Result<bool>;
}

/// Refuses an answer to [`Prompter::input`] with a message, or accepts it.
type InputCheck = fn(&str) -> Result<(), String>;

/// The production [`Prompter`]: `inquire` prompts reading the real terminal.
struct InquirePrompter;

impl Prompter for InquirePrompter {
    fn is_interactive(&self) -> bool {
        io::stdin().is_terminal()
    }

    fn input(&self, prompt: &str, default: Option<&str>, check: InputCheck) -> Result<String> {
        // `inquire` gives the default for an empty answer before it runs the
        // validator.
        let mut text = Text::new(prompt).with_validator(move |input: &str| {
            Ok(if input.trim().is_empty() {
                Validation::Invalid("An empty value is not valid.".into())
            } else if let Err(message) = check(input.trim()) {
                Validation::Invalid(message.into())
            } else {
                Validation::Valid
            })
        });
        if let Some(default) = default {
            text = text.with_default(default);
        }
        Ok(text.prompt()?)
    }

    fn select(&self, prompt: &str, items: Vec<String>, default: usize) -> Result<Option<usize>> {
        Ok(Select::new(prompt, items)
            .with_starting_cursor(default)
            .raw_prompt_skippable()?
            .map(|choice| choice.index))
    }

    fn secret(&self, prompt: &str, hint: Option<&str>) -> Result<Option<String>> {
        // `inquire` has no default for a masked prompt, so the hint rides in
        // the message, in the same `(value)` shape `Text` gives a default.
        let message = match hint {
            Some(hint) => format!("{prompt} ({hint})"),
            None => prompt.to_owned(),
        };
        let help = match hint {
            Some(_) => "hit enter to keep the stored value, or Esc to skip",
            None => "hit Esc to skip",
        };
        let keeps_stored = hint.is_some();
        // The validator accepts an empty answer only when it keeps the stored
        // secret.
        let formatter = |input: &str| {
            if input.is_empty() {
                "kept".to_owned()
            } else {
                Password::DEFAULT_FORMATTER(input)
            }
        };
        let password = Password::new(&message)
            .with_display_mode(PasswordDisplayMode::Masked)
            .without_confirmation()
            .with_help_message(help)
            .with_formatter(&formatter)
            .with_validator(move |input: &str| {
                Ok(if refuses_blank_secret(input, keeps_stored) {
                    Validation::Invalid("An empty value is not valid.".into())
                } else {
                    Validation::Valid
                })
            });
        Ok(password.prompt_skippable()?)
    }

    fn confirm(&self, prompt: &str) -> Result<bool> {
        Ok(Confirm::new(prompt)
            .with_default(false)
            .prompt_skippable()?
            .unwrap_or(false))
    }
}

/// With `output.verbose`, login logs its requests to stderr.
pub async fn cmd_login(
    tenant: Option<String>,
    registry_flag: Option<Registry>,
    profile: Option<&str>,
    current_settings: &Settings,
    output: OutputOptions,
) -> Result<()> {
    do_cmd_login(
        tenant,
        registry_flag,
        profile,
        current_settings,
        output,
        &InquirePrompter,
    )
    .await
}

async fn do_cmd_login(
    tenant: Option<String>,
    registry_flag: Option<Registry>,
    profile: Option<&str>,
    current_settings: &Settings,
    OutputOptions { verbose, .. }: OutputOptions,
    prompter: &dyn Prompter,
) -> Result<()> {
    let profile_to_use = profile
        .map(str::to_owned)
        .or_else(|| env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME).ok().flatten());
    let interactive = prompter.is_interactive();

    let tenant_to_use = match tenant {
        Some(tenant) => tenant,
        None if !interactive => {
            return Err(user_error(
                "login needs a tenant, and cannot ask for one without a terminal",
            )
            .suggestion("run `snouty login --tenant <tenant>`"));
        }
        None => prompter.input(
            "What Antithesis tenant would you like to use?",
            current_settings.tenant(),
            |_| Ok(()),
        )?,
    };
    validate_tenant_host(&tenant_to_use)?;

    // The credential stored for the profile, which a new credential replaces.
    // A read error counts as nothing stored, because storing a credential
    // backs up an unparsable credentials file. A blank key cannot
    // authenticate, so it also counts as nothing stored.
    let stored = AuthenticationInfo::stored_for_profile(profile_to_use.as_deref())
        .inspect_err(|err| debug!("no stored credentials to read: {err:#}"))
        .ok()
        .flatten()
        .filter(|stored| !stored.value().has_blank_api_key());

    let api_settings = current_settings.with_tenant(&tenant_to_use);
    let tenant_url = TenantUrl::new(&api_settings, &tenant_to_use);

    let new_credentials = if interactive {
        let client = HttpClient::new(&api_settings, verbose)?;
        prompt_for_auth(
            prompter,
            &client,
            &tenant_url,
            stored.as_ref().map(Tagged::value),
        )
        .await?
    } else {
        warn!(
            "Cannot collect credentials unless running in a TTY. Please provide credentials via environment variables or rerun `snouty login` in an interactive session"
        );
        None
    };

    let tenant_registry = fetch_registry(
        &api_settings,
        &tenant_url,
        new_credentials.as_ref(),
        profile_to_use.as_deref(),
        interactive,
        verbose,
    )
    .await?;
    let saved_registry = current_settings
        .registry()
        .filter(|_| current_settings.tenant() == Some(tenant_to_use.as_str()));
    let registry_to_use = match (registry_flag, tenant_registry) {
        (Some(registry), _) => registry,
        (None, tenant_registry) => {
            let registry = match tenant_registry {
                Some(tenant_registry) => Some(tenant_registry.registry_url),
                // A tenant that does not report its registry: ask, or keep
                // the saved one. A saved registry of another tenant is
                // neither offered nor kept.
                None if interactive => Some(prompter.input(
                    "What container registry would you like to use?",
                    saved_registry,
                    |answer| answer.parse::<Registry>().map(drop),
                )?),
                None => saved_registry.map(str::to_owned),
            };
            let suggestion = "run `snouty login --registry <registry>`";
            registry
                .filter(|registry| !registry.trim().is_empty())
                .ok_or_else(|| {
                    user_error(format!(
                        "login needs a container registry for `{tenant_to_use}`; nothing was saved"
                    ))
                    .suggestion(suggestion)
                })?
                .parse::<Registry>()
                .map_err(|err| {
                    user_error(format!("{err}; nothing was saved")).suggestion(suggestion)
                })?
        }
    };

    // The settings go first, so that a settings file that login cannot
    // update leaves the stored credential alone. When the credential cannot
    // be stored, the settings go back, so that the profile does not name the
    // new tenant with the old credential.
    let previous_settings = update_settings_in_global_file(
        &tenant_to_use,
        &registry_to_use.to_string(),
        profile_to_use.as_deref(),
    )?;

    // The kind of credential login wrote, and where.
    let written = match new_credentials {
        Some(credentials) => {
            let kind = match &credentials {
                PersistableCredentials::ApiKey { .. } => "API key",
                PersistableCredentials::Password { .. } => "username and password",
                PersistableCredentials::OAuth { .. } => "OAuth credentials",
            };
            match persist(credentials, profile_to_use.as_deref()) {
                Ok(store) => Some(kind.with_tag(store)),
                Err(err) => {
                    return Err(match previous_settings.restore() {
                        Ok(()) => err.wrap_err("could not store the credential; nothing was saved"),
                        Err(restore_err) => err.wrap_err(format!(
                            "could not store the credential, and {restore_err:#}"
                        )),
                    });
                }
            }
        }
        None => None,
    };

    print_login_summary(
        &tenant_to_use,
        &registry_to_use,
        profile_to_use.as_deref(),
        written,
        stored.as_ref(),
    );

    Ok(())
}

/// Where login signs in: the `base_url` setting that later commands call, or
/// else the host of the tenant.
struct TenantUrl<'a> {
    url: String,
    tenant: &'a str,
    explicit: bool,
}

impl<'a> TenantUrl<'a> {
    fn new(settings: &Settings, tenant: &'a str) -> Self {
        let explicit = settings.explicit_base_url();
        Self {
            url: normalize_base_url(
                explicit.map_or_else(|| tenant_base_url(tenant), str::to_owned),
            ),
            tenant,
            explicit: explicit.is_some(),
        }
    }

    /// `err` as the error of a URL that login cannot reach, or `None` for any
    /// other error. A URL that does not parse or a connect error, which
    /// includes a connect timeout, most often means a misspelled tenant or
    /// `base_url`.
    fn unreachable(&self, err: &Report) -> Option<Report> {
        let transport = err
            .chain()
            .find_map(|cause| cause.downcast_ref::<reqwest::Error>())
            .filter(|transport| transport.is_builder() || transport.is_connect())?;
        let check = if self.explicit {
            "check that the base_url setting is correct".to_owned()
        } else {
            format!("check that the tenant name `{}` is correct", self.tenant)
        };
        Some(
            user_error(format!(
                "could not reach {}: {}; nothing was saved",
                self.url,
                root_cause(transport)
            ))
            .suggestion(check),
        )
    }
}

/// `GET /api/v1/registry` at `tenant_url` with `new_credentials`, or else
/// with the credential that later commands use. The request is also the check
/// that the credential works. `None` means that the tenant does not serve the
/// endpoint, which tenants before it answer with 404. The tenant
/// authenticates a request before it routes it, so a 404 also means that it
/// accepted the credential. `interactive` picks the next step that an error
/// suggests.
async fn fetch_registry(
    settings: &Settings,
    tenant_url: &TenantUrl<'_>,
    new_credentials: Option<&PersistableCredentials>,
    profile: Option<&str>,
    interactive: bool,
    verbose: bool,
) -> Result<Option<RegistryResponse>> {
    let tenant = tenant_url.tenant;
    // A read error of a stored credential passes through as it is.
    let ambient = match new_credentials {
        Some(_) => None,
        None => AuthenticationInfo::find_ambient(profile)?,
    };
    let next_step = match ambient.as_ref().map(|found| (found.value(), found.tag())) {
        // Every later command reads the environment first, so a credential
        // at the prompt would not replace one there. A GitHub Actions OIDC
        // token is also tagged Environment, but ANTITHESIS_API_KEY does not
        // hold it.
        Some((AuthenticationInfo::ApiKey { .. }, CredentialSource::Environment { .. })) => {
            "set `ANTITHESIS_API_KEY` to a valid API key"
        }
        Some((AuthenticationInfo::GithubActionsOidc(_), _)) => OIDC_REJECTED_NEXT_STEP,
        Some((AuthenticationInfo::Password { .. }, CredentialSource::Environment { .. })) => {
            "unset ANTITHESIS_USERNAME and ANTITHESIS_PASSWORD"
        }
        _ if interactive => {
            "run `snouty login` again, and enter an API key or sign in with single sign-on"
        }
        _ if in_github_actions() => no_credentials_suggestion(true),
        _ => "set ANTITHESIS_API_KEY, or run `snouty login` in a terminal",
    };
    let authentication = match (new_credentials, ambient) {
        (Some(credentials), _) => credentials.to_unrefreshable_authentication_info(),
        (None, Some(found)) if !matches!(found.value(), AuthenticationInfo::Password { .. }) => {
            found.untag()
        }
        (None, _) => {
            return Err(user_error(format!(
                "login needs an API key or a single sign-on to check with `{tenant}`; nothing was saved"
            ))
            .suggestion(next_step));
        }
    };
    // A cached answer would not check the credential.
    let api = AntithesisApi::build(settings, authentication, verbose, ResponseCache::Disabled)?;
    let registry = tokio::time::timeout(env::scaled_wait(REGISTRY_TIMEOUT), api.get_registry())
        .await
        .map_err(|_| {
            user_error(format!(
                "timed out loading the registry configuration from {}; nothing was saved",
                tenant_url.url
            ))
            .suggestion("check your network connection, and the base_url and https_proxy settings")
        })?;
    match registry {
        Ok(registry) => Ok(Some(registry.untag())),
        Err(err) => match api_error_status(&err) {
            Some(404) => {
                debug!("the tenant does not report its registry: {err:#}");
                Ok(None)
            }
            Some(status) if rejects_credential(status) => Err(user_error(format!(
                "`{tenant}` did not accept the credential; nothing was saved"
            ))
            .suggestion(next_step)),
            _ => Err(tenant_url.unreachable(&err).unwrap_or(err)),
        },
    }
}

/// Confirm what `snouty login` saved, and the obvious next step — otherwise a
/// successful login exits silently, leaving the user unsure it took.
fn print_login_summary(
    tenant: &str,
    registry: &Registry,
    profile: Option<&str>,
    written: Option<Tagged<&str, CredentialStore>>,
    stored: Option<&Tagged<AuthenticationInfo, CredentialStore>>,
) {
    // Wrap each line at word boundaries on a terminal, so that no word is cut.
    let say = |line: &str| println!("{}", crate::render::wrap_stdout_if_tty(line));
    let scope = match profile {
        Some(p) => format!(" under profile `{p}`"),
        None => String::new(),
    };
    say(&format!(
        "\nSaved tenant `{tenant}` and registry `{registry}`{scope}."
    ));
    match (written, stored) {
        (Some(written), _) => {
            say(&format!(
                "Stored your {}{scope} in {}{}.",
                written.value(),
                location(written.tag()),
                replacing(stored)
            ));
        }
        (None, Some(_)) => say(&format!("Your stored credentials{scope} are unchanged.")),
        (None, None) => {}
    }
    // The snouty credential signs in to the tenant's own registry. Any other
    // registry takes its own login.
    let own_registry = registry
        .host()
        .parse::<TenantRegistry>()
        .is_ok_and(|own| own.tenant().eq_ignore_ascii_case(tenant));
    if !own_registry {
        // `docker login` takes the host, not the path after it.
        let host = registry.host();
        say(&format!(
            "Run `docker login {host}` or `podman login {host}` to sign in to the registry."
        ));
    }
    say("Run `snouty doctor` to verify your setup.");
}

/// Where the summary says a credential is stored.
fn location(store: &CredentialStore) -> String {
    match store {
        CredentialStore::Keychain { .. } => "the system keychain".to_owned(),
        CredentialStore::CredentialsFile { .. } => "the credentials file".to_owned(),
    }
}

/// The summary's ", replacing your stored …" clause. Every write replaces
/// `stored`, because login writes nothing when the user keeps it.
fn replacing(stored: Option<&Tagged<AuthenticationInfo, CredentialStore>>) -> String {
    stored.map_or_default(|stored| format!(", replacing your stored {}", stored.value()))
}

/// The credential kinds `snouty login` sets up. A username and password comes
/// only from the environment or an older credentials file, because `snouty runs`
/// refuses it.
#[derive(Clone, Copy)]
enum AuthSetupType {
    ApiKey,
    OAuth,
}

impl std::fmt::Display for AuthSetupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthSetupType::ApiKey => f.write_str("API Key"),
            AuthSetupType::OAuth => f.write_str("Single sign-on (OAuth)"),
        }
    }
}

/// Returns the credential to store, or `None` when the user skips, keeps
/// `stored`, or declines to replace it. Signs in at `tenant_url`.
async fn prompt_for_auth(
    prompter: &dyn Prompter,
    client: &HttpClient,
    tenant_url: &TenantUrl<'_>,
    stored: Option<&AuthenticationInfo>,
) -> Result<Option<PersistableCredentials>> {
    let url = &tenant_url.url;
    let oauth_config = fetch_cli_config(client, url, OAUTH_HTTP_TIMEOUT).await;

    // The registry request after the prompt uses the same URL, so stop before
    // the prompt when it cannot be reached. Any other error, such as a slow
    // answer, means the host exists: login continues without single sign-on.
    if let Err(err) = &oauth_config
        && let Some(unreachable) = tenant_url.unreachable(err)
    {
        return Err(unreachable);
    }

    let oauth_offered = oauth_config
        .as_ref()
        .is_ok_and(|config| !matches!(config, CliOAuthConfig::Disabled));

    let key_to_keep = match stored {
        Some(AuthenticationInfo::ApiKey { api_key }) => Some(api_key.as_str()),
        _ => None,
    };

    // Single sign-on leads the menu and is the default, unless an API key is
    // stored: then "log in again the same way" stays one keystroke.
    let choice = if oauth_offered {
        let options = [AuthSetupType::OAuth, AuthSetupType::ApiKey];
        let labels = options.iter().map(ToString::to_string).collect();
        prompter
            .select(
                "What kind of credentials would you like to use? (Hit Esc to skip)",
                labels,
                usize::from(key_to_keep.is_some()),
            )?
            .map(|index| options[index])
    } else {
        Some(AuthSetupType::ApiKey)
    };

    match choice {
        None => Ok(None),
        Some(AuthSetupType::ApiKey) => {
            let hint = key_to_keep.map(secret_hint);
            let entered = prompter.secret("Please enter your API Key", hint.as_deref())?;
            // Trim a pasted key: an API key holds no whitespace.
            Ok(match entered.as_deref().map(str::trim) {
                // An empty answer, or the stored key typed again, keeps the
                // stored key.
                Some(api_key) if api_key.is_empty() || Some(api_key) == key_to_keep => None,
                Some(api_key) if confirm_replacement(prompter, stored, AuthSetupType::ApiKey)? => {
                    Some(PersistableCredentials::ApiKey {
                        api_key: api_key.to_owned(),
                    })
                }
                _ => None,
            })
        }
        // Ask before the browser sign-in, so that a no costs the user no sign-in.
        Some(AuthSetupType::OAuth)
            if !confirm_replacement(prompter, stored, AuthSetupType::OAuth)? =>
        {
            Ok(None)
        }
        Some(AuthSetupType::OAuth) => complete_oauth_login(client, url, &oauth_config?)
            .await
            .map(Some),
    }
}

/// Whether to store a new credential of kind `new` over `stored`. Asks the
/// user unless nothing or a username and password is stored.
fn confirm_replacement(
    prompter: &dyn Prompter,
    stored: Option<&AuthenticationInfo>,
    new: AuthSetupType,
) -> Result<bool> {
    match (stored, new) {
        // Signing in again only renews the session that the stored tokens hold.
        // Login cannot pass with a username and password, so keeping one is
        // no choice.
        (None | Some(AuthenticationInfo::Password { .. }), _)
        | (Some(AuthenticationInfo::OAuth(_)), AuthSetupType::OAuth) => Ok(true),
        (Some(stored), _) => prompter.confirm(&format!("Replace your stored {stored}?")),
    }
}

/// How many trailing characters of a stored secret its hint shows. Enough to
/// tell one key from another, far too few to reconstruct either.
const HINT_VISIBLE_CHARS: usize = 4;

/// How many stars stand in for the rest of a stored secret. A fixed count, so
/// the hint does not disclose how long the secret is.
const HINT_STARS: usize = 8;

/// A display-only stand-in for a stored secret — stars, then its last few
/// characters, e.g. `********9Pgw`. The prompt shows this so the user can see
/// that there is a stored value to keep, and recognize which one it is.
///
/// The tail, not the head: every Antithesis API key opens with the same
/// `antithesis_api_key_v2` prefix, so leading characters would distinguish
/// nothing.
fn secret_hint(secret: &str) -> String {
    let skip = secret.chars().count().saturating_sub(HINT_VISIBLE_CHARS);
    let tail: String = secret.chars().skip(skip).collect();
    format!("{}{tail}", "*".repeat(HINT_STARS))
}

/// A masked prompt refuses a blank answer, except the empty answer that keeps a
/// stored secret.
fn refuses_blank_secret(answer: &str, keeps_stored: bool) -> bool {
    is_blank_api_key(answer) && !(keeps_stored && answer.is_empty())
}

#[derive(Debug, Deserialize)]
#[serde(tag = "port_strategy", rename_all = "snake_case")]
pub(crate) enum CliOAuthConfig {
    /// Bind the first available port from `ports`, in order.
    Fixed { ports: Vec<u16> },
    /// Bind any available port (an OS-assigned ephemeral port).
    Ephemeral,
    /// CLI OAuth login is not usable: the tenant answers `port_strategy:
    /// disabled`, or 403.
    Disabled,
}

/// The pieces of the OAuth callback the token exchange needs: the authorization
/// `code` (RFC 6749) and the `flow_token`, which the proxy delivers in the
/// standard OAuth `state` query parameter.
#[derive(Debug, PartialEq, Eq)]
struct CallbackParams {
    auth_code: String,
    flow_token: String,
}

/// Response body of `POST /auth/cli/token` (and `/auth/cli/refresh`)
#[derive(Debug, Deserialize)]
struct TokenResponse {
    antithesis_token: String,
    refresh_token: Option<String>,
}

/// Drive the CLI OAuth (PKCE) login end-to-end and return the resulting tokens
/// as persistable credentials:
///
/// 1. `GET /auth/cli/config` — discover how to bind the loopback callback server.
/// 2. Bind a localhost-only HTTP server on the chosen port.
/// 3. `POST /auth/cli/login` — send the PKCE challenge + CSRF state, receive the
///    authorization URL.
/// 4. Send the user to a local sign-in page that links to that URL (open a
///    browser, best-effort) and wait for the browser to redirect to
///    `http://localhost:<port>/callback?code=…&state=…`.
/// 5. `POST /auth/cli/token` — exchange the authorization code (in the
///    `Authorization` header, alongside the PKCE verifier) and the flow token
///    (in the body) for an Antithesis token.
async fn complete_oauth_login(
    client: &HttpClient,
    base_url: &str,
    config: &CliOAuthConfig,
) -> Result<PersistableCredentials> {
    // Bind the callback server *before* initiating login so the port we tell the
    // proxy about is one we're already listening on.
    let listeners = bind_callback_listener(config).await?;
    let port = listeners.port;

    // PKCE: the verifier is the secret we keep; the challenge is what we hand to
    // the proxy. `cli_state` is opaque CSRF state the proxy validates server-side.
    let code_verifier = generate_verifier_or_state()?;
    let code_challenge = code_challenge_for(&code_verifier);
    let cli_state = generate_verifier_or_state()?;

    let idp_url =
        request_login_redirect(client, base_url, port, &code_challenge, &cli_state).await?;

    // A random token in the path hides the IdP URL from a local process that
    // can reach the loopback port but did not get this URL.
    let sign_in_path = format!("/sign-in/{}", generate_verifier_or_state()?);
    let sign_in_url = format!("http://localhost:{port}{sign_in_path}");

    // A failure to open a browser is not fatal: the URL is printed either way,
    // so a headless environment can still complete the flow by hand.
    println!();
    match crate::browser::open_in_browser(&sign_in_url) {
        Ok(()) => {
            println!("Opening login url in your browser");
            println!("If your browser didn't open, manually visit: {sign_in_url}");
        }
        Err(err) => {
            println!("Failed to open login url automatically ({err}).");
            println!("Open the following url in your browser on this machine: {sign_in_url}");
        }
    }
    println!("Waiting for you to complete sign-in in your browser...");

    let callback = wait_for_callback(listeners, &sign_in_path, &idp_url).await?;

    let tokens = exchange_code_for_tokens(
        client,
        base_url,
        &callback.auth_code,
        &code_verifier,
        &callback.flow_token,
    )
    .await?;

    Ok(PersistableCredentials::OAuth {
        antithesis_token: tokens.antithesis_token,
        refresh_token: tokens.refresh_token,
    })
}

/// `GET /auth/cli/config` — the redirect strategy for this tenant. The route is
/// feature-gated: a tenant without CLI OIDC answers 403, which maps to
/// [`CliOAuthConfig::Disabled`].
/// The request gives up after `timeout`.
pub(crate) async fn fetch_cli_config(
    client: &HttpClient,
    base_url: &str,
    timeout: Duration,
) -> Result<CliOAuthConfig> {
    let response = client
        .send_with_timeout(client.get(format!("{base_url}/auth/cli/config")), timeout)
        .await
        .wrap_err("failed to contact the tenant's OAuth configuration endpoint")?;

    if response.status() == StatusCode::FORBIDDEN {
        return Ok(CliOAuthConfig::Disabled);
    }

    response
        .error_for_status()
        .wrap_err("failed to fetch the tenant's OAuth configuration")?
        .json::<CliOAuthConfig>()
        .await
        .wrap_err("failed to parse the tenant's OAuth configuration")
}

/// Loopback listeners for the OAuth callback, bound to the same port on both
/// IPv4 (`127.0.0.1`) and — best-effort — IPv6 (`::1`). A browser resolves the
/// `localhost` in the redirect URI to one family or the other depending on the
/// platform and `/etc/hosts`, so listening on both keeps the callback from
/// landing on an address nobody is accepting on.
struct CallbackListeners {
    listeners: Vec<TcpListener>,
    port: u16,
}

/// Bind the localhost-only callback server per the tenant's strategy.
async fn bind_callback_listener(config: &CliOAuthConfig) -> Result<CallbackListeners> {
    match config {
        CliOAuthConfig::Disabled => Err(user_error(
            "this tenant has not enabled OAuth login for the CLI",
        )
        .suggestion("choose API key authentication instead")),
        CliOAuthConfig::Ephemeral => {
            // Let the OS assign the port on IPv4, then mirror it onto IPv6.
            let v4 = TcpListener::bind(("127.0.0.1", 0))
                .await
                .wrap_err("failed to bind a local OAuth callback server")?;
            let port = v4
                .local_addr()
                .wrap_err("failed to read the callback server's local address")?
                .port();
            Ok(with_ipv6_loopback(vec![v4], port).await)
        }
        CliOAuthConfig::Fixed { ports } => {
            if ports.is_empty() {
                return Err(eyre!(
                    "the tenant advertised a fixed callback-port strategy but listed no ports"
                ));
            }
            // Use the first port whose IPv4 loopback is free; IPv6 is added
            // best-effort on that same port.
            for &port in ports {
                if let Ok(v4) = TcpListener::bind(("127.0.0.1", port)).await {
                    return Ok(with_ipv6_loopback(vec![v4], port).await);
                }
            }
            Err(user_error(format!(
                "none of the tenant's configured callback ports were available: {ports:?}"
            ))
            .suggestion("free one of those ports (close whatever is using it) and try again"))
        }
    }
}

/// Add a best-effort IPv6 loopback listener on `port`. A failure to bind `::1`
/// (a host without IPv6 loopback, or the v6 port already taken) is non-fatal:
/// the IPv4 listener already covers the overwhelmingly common case.
async fn with_ipv6_loopback(mut listeners: Vec<TcpListener>, port: u16) -> CallbackListeners {
    if let Ok(v6) = TcpListener::bind(("::1", port)).await {
        listeners.push(v6);
    }
    CallbackListeners { listeners, port }
}

/// `POST /auth/cli/login` — hand the proxy the PKCE challenge and CSRF state and
/// the loopback port, and get back the IdP authorization URL.
async fn request_login_redirect(
    client: &HttpClient,
    base_url: &str,
    port: u16,
    code_challenge: &str,
    cli_state: &str,
) -> Result<reqwest::Url> {
    #[derive(Serialize)]
    struct LoginRequest<'a> {
        port: u16,
        code_challenge: &'a str,
        code_challenge_method: &'a str,
        cli_state: &'a str,
    }

    #[derive(Deserialize)]
    struct LoginRedirect {
        location: String,
    }

    let request = client
        .post(format!("{base_url}/auth/cli/login"))
        .json(&LoginRequest {
            port,
            code_challenge,
            code_challenge_method: "S256",
            cli_state,
        });
    let redirect: LoginRedirect = client
        .send_with_timeout(request, OAUTH_HTTP_TIMEOUT)
        .await
        .wrap_err("failed to initiate OAuth login")?
        .error_for_status()
        .wrap_err("the tenant rejected the OAuth login request")?
        .json()
        .await
        .wrap_err("failed to parse the OAuth login response")?;

    crate::browser::parse_http_url(&redirect.location)
        .wrap_err("the tenant returned an unusable OAuth login URL")
}

/// `POST /auth/cli/token` — exchange the authorization code for tokens. The
/// authorization code and PKCE verifier ride in the `Authorization` header as
/// `Bearer base64url-nopad(code:verifier)` (keeping the secrets out of the
/// body/URL); the flow token goes in the JSON body.
async fn exchange_code_for_tokens(
    client: &HttpClient,
    base_url: &str,
    auth_code: &str,
    code_verifier: &str,
    flow_token: &str,
) -> Result<TokenResponse> {
    #[derive(Serialize)]
    struct TokenExchangeBody<'a> {
        flow_token: &'a str,
    }

    let credentials = BASE64_URL_SAFE_NO_PAD.encode(format!("{auth_code}:{code_verifier}"));

    let request = client
        .post(format!("{base_url}/auth/cli/token"))
        .bearer_auth(credentials)
        .json(&TokenExchangeBody { flow_token });
    client
        .send_with_timeout(request, OAUTH_HTTP_TIMEOUT)
        .await
        .wrap_err("failed to exchange the authorization code for tokens")?
        .error_for_status()
        .wrap_err("the tenant rejected the token exchange")?
        .json::<TokenResponse>()
        .await
        .wrap_err("failed to parse the token exchange response")
}

/// A page the loopback server shows in the browser during OAuth login.
#[derive(Template)]
#[template(path = "login/page.html")]
enum LoginPage<'a> {
    SignIn { idp_url: &'a reqwest::Url },
    SignedIn,
    SignInFailed,
}

/// Serve the sign-in page for `idp_url` at `sign_in_path` until the OAuth
/// callback arrives, show the result page, and return the callback's
/// parameters. Other paths get a 404. Fails after `CALLBACK_TIMEOUT`.
async fn wait_for_callback(
    listeners: CallbackListeners,
    sign_in_path: &str,
    idp_url: &reqwest::Url,
) -> Result<CallbackParams> {
    let render = |page: LoginPage<'_>| {
        page.render()
            .map(Bytes::from)
            .wrap_err("failed to render a login page")
    };
    let sign_in_page = render(LoginPage::SignIn { idp_url })?;
    let signed_in_page = render(LoginPage::SignedIn)?;
    let sign_in_failed_page = render(LoginPage::SignInFailed)?;

    let (callback_tx, mut callback_rx) = mpsc::channel(1);
    let app = Router::new()
        .route(sign_in_path, get(move || async { html(sign_in_page) }))
        .route(
            "/callback",
            get(move |RawQuery(query): RawQuery| async move {
                let result = parse_callback_params(query.as_deref().unwrap_or_default());
                // Show a page regardless of the outcome so the user isn't left
                // staring at a spinner; details land in the terminal.
                let page = match &result {
                    Ok(_) => signed_in_page,
                    Err(_) => sign_in_failed_page,
                };
                // Only the first callback counts; the channel holds one.
                let _ = callback_tx.try_send(result);
                html(page)
            }),
        );
    let servers = futures_util::future::try_join_all(
        listeners
            .listeners
            .into_iter()
            .map(|listener| axum::serve(listener, app.clone()).into_future()),
    );

    // Dropping `servers` closes the listeners. axum serves each connection in
    // its own task, which still writes the result page.
    tokio::select! {
        callback = callback_rx.recv() => {
            callback.ok_or_else(|| eyre!("the OAuth callback server stopped"))?
        }
        served = servers => {
            served.wrap_err("the OAuth callback server failed")?;
            Err(eyre!("the OAuth callback server stopped"))
        }
        () = tokio::time::sleep(CALLBACK_TIMEOUT) => Err(user_error(format!(
            "timed out after {} seconds waiting for the browser to complete sign-in",
            CALLBACK_TIMEOUT.as_secs()
        ))),
    }
}

/// An HTML response that the browser must not cache: the sign-in page holds a
/// URL for this login attempt only.
fn html(body: Bytes) -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-store")], Html(body))
}

/// Parse the callback query string (e.g. `code=…&state=…`) into the
/// authorization code and flow token. The proxy carries the flow token in the
/// standard OAuth `state` parameter. An IdP-reported `error` surfaces as an error.
/// Any page that knows the port can send a callback, so the function escapes
/// `error` and `error_description` for the terminal.
fn parse_callback_params(query: &str) -> Result<CallbackParams> {
    let mut auth_code = None;
    let mut flow_token = None;
    let mut error = None;
    let mut error_description = None;
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "code" => auth_code = Some(value.into_owned()),
            "state" => flow_token = Some(value.into_owned()),
            "error" => error = Some(sanitize(&value)),
            "error_description" => error_description = Some(sanitize(&value)),
            _ => {}
        }
    }

    if let Some(error) = error {
        let detail = error_description.map_or_default(|d| format!(": {d}"));
        return Err(user_error(format!(
            "the identity provider reported an authorization error ({error}){detail}"
        )));
    }

    Ok(CallbackParams {
        auth_code: auth_code
            .ok_or_else(|| eyre!("the OAuth callback did not include an authorization code"))?,
        flow_token: flow_token
            .ok_or_else(|| eyre!("the OAuth callback did not include a flow token"))?,
    })
}

/// The PKCE S256 code challenge for a verifier: `base64url-nopad(SHA256(verifier))`.
fn code_challenge_for(code_verifier: &str) -> String {
    BASE64_URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()))
}

/// Generate a random value suitable for use as a PKCE code verifier or CSRF
/// state: 32 bytes of CSPRNG output, base64url-encoded to 43 unreserved
/// characters (satisfying both the 43–128 verifier and 32–256 state bounds).
fn generate_verifier_or_state() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|e| eyre!("failed to generate secure random bytes: {e}"))?;
    Ok(BASE64_URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    #[test]
    fn secret_hint_shows_a_prefix_and_a_fixed_run_of_stars() {
        // The tail is the key's own, so the user recognizes which key is
        // stored; the star run is fixed, so two keys of different lengths hint
        // identically. Antithesis keys share a constant prefix, which is why the
        // hint shows the end and not the start.
        assert_eq!(secret_hint(FAKE_API_KEY_A), "********9Pgw");
        assert_eq!(secret_hint(FAKE_API_KEY_B), "********7Qxz");
    }

    #[test]
    fn secret_hint_never_shows_more_than_it_has() {
        assert_eq!(secret_hint("ab"), "********ab");
        assert_eq!(secret_hint(""), "********");
    }

    #[test]
    fn cli_config_deserializes_all_three_strategies() {
        let fixed: CliOAuthConfig =
            serde_json::from_str(r#"{"port_strategy":"fixed","ports":[12345,12346,12347]}"#)
                .unwrap();
        assert!(matches!(fixed, CliOAuthConfig::Fixed { ports } if ports == [12345, 12346, 12347]));

        let ephemeral: CliOAuthConfig =
            serde_json::from_str(r#"{"port_strategy":"ephemeral"}"#).unwrap();
        assert!(matches!(ephemeral, CliOAuthConfig::Ephemeral));

        let disabled: CliOAuthConfig =
            serde_json::from_str(r#"{"port_strategy":"disabled"}"#).unwrap();
        assert!(matches!(disabled, CliOAuthConfig::Disabled));
    }

    #[test]
    fn code_challenge_matches_rfc7636_test_vector() {
        // RFC 7636 Appendix B.
        assert_eq!(
            code_challenge_for(FAKE_PKCE_VERIFIER),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn generated_verifier_meets_pkce_constraints() {
        let verifier = generate_verifier_or_state().unwrap();
        // 32 bytes base64url-nopad encode to 43 chars, within the 43–128 range,
        // and the challenge is always exactly 43 chars.
        assert_eq!(verifier.len(), 43);
        assert!(
            verifier
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
        );
        assert_eq!(code_challenge_for(&verifier).len(), 43);
    }

    #[test]
    fn parse_callback_extracts_code_and_flow_token() {
        let params = parse_callback_params(&format!(
            "code={FAKE_OAUTH_CODE}&state={FAKE_OAUTH_FLOW_TOKEN}"
        ))
        .unwrap();
        assert_eq!(
            params,
            CallbackParams {
                auth_code: FAKE_OAUTH_CODE.to_owned(),
                flow_token: FAKE_OAUTH_FLOW_TOKEN.to_owned(),
            }
        );
    }

    #[test]
    fn parse_callback_url_decodes_values() {
        let params = parse_callback_params("code=a%2Fb%2Bc&state=x%3Dy").unwrap();
        assert_eq!(params.auth_code, "a/b+c");
        assert_eq!(params.flow_token, "x=y");
    }

    #[test]
    fn parse_callback_surfaces_idp_error() {
        let err = parse_callback_params("error=access_denied&error_description=user%20said%20no")
            .unwrap_err();
        let rendered = format!("{err}");
        assert!(rendered.contains("access_denied"), "got: {rendered}");
        assert!(rendered.contains("user said no"), "got: {rendered}");
    }

    #[test]
    fn parse_callback_escapes_terminal_controls_in_the_idp_error() {
        let err = parse_callback_params(
            "error=x%1b%5b2J&error_description=%1b%5d0%3bPWNED%07%1b%5b31mRED",
        )
        .unwrap_err();
        let rendered = format!("{err}");
        assert!(!rendered.contains(['\x1b', '\x07']), "got: {rendered:?}");
        assert!(rendered.contains("PWNED"), "got: {rendered:?}");
    }

    #[hegel::test]
    fn parse_callback_idp_error_holds_no_controls_except_tab(tc: hegel::TestCase) {
        let error = tc.draw(hegel::generators::text());
        let description = tc.draw(hegel::generators::text());
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("error", &error)
            .append_pair("error_description", &description)
            .finish();
        let rendered = format!("{}", parse_callback_params(&query).unwrap_err());
        assert!(
            !rendered.chars().any(|ch| ch != '\t' && ch.is_control()),
            "got: {rendered:?}"
        );
    }

    #[test]
    fn parse_callback_requires_code_and_state() {
        assert!(parse_callback_params("state=only_state").is_err());
        assert!(parse_callback_params("code=only_code").is_err());
    }

    /// A masked prompt accepts any answer with a non-whitespace character,
    /// refuses any blank answer that is not empty, and accepts the empty
    /// answer only when it keeps a stored secret.
    #[hegel::test]
    fn masked_prompt_refuses_exactly_the_blank_answers(tc: hegel::TestCase) {
        let answer = tc.draw(hegel::generators::text());
        let keeps_stored = tc.draw(hegel::generators::booleans());
        let refused = refuses_blank_secret(&answer, keeps_stored);
        if answer.chars().any(|c| !c.is_whitespace()) {
            assert!(!refused, "{answer:?}");
        } else if answer.is_empty() {
            assert_eq!(refused, !keeps_stored);
        } else {
            assert!(refused, "{answer:?}");
        }
    }

    /// The `href` the sign-in page renders decodes back to the IdP URL, so
    /// no URL can break out of the attribute or reach the browser altered.
    #[hegel::test]
    fn sign_in_page_links_to_the_idp_url(tc: hegel::TestCase) {
        let query = tc.draw(hegel::generators::text());
        let mut idp_url = reqwest::Url::parse("https://idp.example.com/authorize").unwrap();
        idp_url.set_query(Some(&query));

        let page = LoginPage::SignIn { idp_url: &idp_url }.render().unwrap();
        let (_, rest) = page.split_once(r#"<a class="button" href=""#).unwrap();
        let (href, _) = rest.split_once('"').unwrap();
        assert_eq!(decode_html_entities(href), idp_url.as_str());
    }

    /// Decode the numeric character references (`&#38;`) that askama's HTML
    /// escaper writes.
    fn decode_html_entities(text: &str) -> String {
        let mut decoded = String::new();
        let mut rest = text;
        while let Some((before, after)) = rest.split_once("&#") {
            decoded.push_str(before);
            let (code, after) = after.split_once(';').expect("an unterminated entity");
            decoded.push(char::from_u32(code.parse().unwrap()).unwrap());
            rest = after;
        }
        decoded.push_str(rest);
        decoded
    }

    async fn get(port: u16, target: &str, headers: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let request = format!("GET {target} HTTP/1.1\r\nConnection: close\r\n{headers}\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    /// Spawn the loopback server with the sign-in page at `/sign-in/token`.
    async fn spawn_callback_server() -> (u16, tokio::task::JoinHandle<Result<CallbackParams>>) {
        let listeners = bind_callback_listener(&CliOAuthConfig::Ephemeral)
            .await
            .unwrap();
        let port = listeners.port;
        let idp_url = reqwest::Url::parse("https://idp.example.com/authorize?a=1&b=2").unwrap();
        let server =
            tokio::spawn(
                async move { wait_for_callback(listeners, "/sign-in/token", &idp_url).await },
            );
        (port, server)
    }

    #[tokio::test]
    async fn callback_server_serves_the_sign_in_page_until_the_callback() {
        let (port, server) = spawn_callback_server().await;

        // A connection that never sends a request (a browser preconnect)
        // must not hold up the requests behind it.
        let _silent = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();

        let not_found = get(port, "/", "").await;
        assert!(not_found.starts_with("HTTP/1.1 404 "), "got: {not_found}");
        let wrong_token = get(port, "/sign-in/guess", "").await;
        assert!(
            wrong_token.starts_with("HTTP/1.1 404 "),
            "got: {wrong_token}"
        );

        let sign_in = get(port, "/sign-in/token", "").await;
        assert!(sign_in.starts_with("HTTP/1.1 200 "), "got: {sign_in}");
        assert!(
            sign_in.contains(r#"href="https://idp.example.com/authorize?a=1&#38;b=2""#),
            "got: {sign_in}"
        );

        let callback_target =
            format!("/callback?code={FAKE_OAUTH_CODE}&state={FAKE_OAUTH_FLOW_TOKEN}");
        let signed_in = get(port, &callback_target, "").await;
        assert!(signed_in.starts_with("HTTP/1.1 200 "), "got: {signed_in}");
        assert!(signed_in.contains("You’re signed in"), "got: {signed_in}");

        let callback = server.await.unwrap().unwrap();
        assert_eq!(callback.auth_code, FAKE_OAUTH_CODE);
        assert_eq!(callback.flow_token, FAKE_OAUTH_FLOW_TOKEN);
    }

    #[tokio::test]
    async fn callback_server_serves_the_whole_page_to_a_request_with_large_headers() {
        let (port, server) = spawn_callback_server().await;

        let cookie = format!("Cookie: {}\r\n", "c".repeat(8 * 1024));
        let sign_in = get(port, "/sign-in/token", &cookie).await;
        assert!(sign_in.trim_end().ends_with("</html>"), "got: {sign_in}");
        server.abort();
    }

    #[tokio::test]
    async fn callback_server_shows_the_failed_page_for_an_idp_error() {
        let (port, server) = spawn_callback_server().await;

        let failed = get(port, "/callback?error=access_denied", "").await;
        assert!(failed.contains("Sign-in failed"), "got: {failed}");
        assert!(server.await.unwrap().is_err());
    }

    // --- Integration tests -----------------------------------------------------------

    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use crate::settings::Settings;
    use crate::testutils::fake_secrets::{
        FAKE_API_KEY_A, FAKE_API_KEY_B, FAKE_OAUTH_CODE, FAKE_OAUTH_FLOW_TOKEN, FAKE_PASSWORD,
        FAKE_PKCE_VERIFIER, FAKE_REFRESH_TOKEN_A,
    };
    use crate::testutils::{
        api_key_credentials, api_key_line, oauth_credentials, password_credentials,
        spawn_login_backend,
    };
    use color_eyre::eyre::Result;

    enum Answer {
        Input(String),
        Select(Option<usize>),
        Secret(Option<String>),
        Confirm(bool),
    }

    /// Fluent builder for a script of [`Answer`]s, in the order the flow will ask.
    #[derive(Default)]
    struct Script(Vec<Answer>);

    impl Script {
        fn input(mut self, value: &str) -> Self {
            self.0.push(Answer::Input(value.to_owned()));
            self
        }
        fn select(mut self, index: usize) -> Self {
            self.0.push(Answer::Select(Some(index)));
            self
        }
        /// Esc at a menu: the user skips it rather than choosing.
        fn skip_select(mut self) -> Self {
            self.0.push(Answer::Select(None));
            self
        }
        fn secret(mut self, value: &str) -> Self {
            self.0.push(Answer::Secret(Some(value.to_owned())));
            self
        }
        /// Esc at a masked prompt.
        fn skip_secret(mut self) -> Self {
            self.0.push(Answer::Secret(None));
            self
        }
        fn confirm(mut self, value: bool) -> Self {
            self.0.push(Answer::Confirm(value));
            self
        }
        fn build(self) -> ScriptedPrompter {
            ScriptedPrompter {
                answers: RefCell::new(self.0.into()),
                prompts: RefCell::new(Vec::new()),
                selects: RefCell::new(Vec::new()),
                secret_hints: RefCell::new(Vec::new()),
            }
        }
    }

    /// A [`Prompter`] that returns pre-programmed answers and records what it was
    /// asked, so tests can assert on prompt order and menu contents.
    struct ScriptedPrompter {
        answers: RefCell<VecDeque<Answer>>,
        prompts: RefCell<Vec<String>>,
        /// Each `select` call: its item list and its default index.
        selects: RefCell<Vec<(Vec<String>, usize)>>,
        /// The hint shown by each `secret` call, in order.
        secret_hints: RefCell<Vec<Option<String>>>,
    }

    impl ScriptedPrompter {
        fn next(&self, kind: &str, prompt: &str) -> Answer {
            self.prompts.borrow_mut().push(prompt.to_owned());
            self.answers.borrow_mut().pop_front().unwrap_or_else(|| {
                panic!("script ran out of answers at {kind}({prompt:?})");
            })
        }

        /// Every prompt string the flow asked, in order.
        fn prompts(&self) -> Vec<String> {
            self.prompts.borrow().clone()
        }

        /// The (items, default) pair passed to each `select`, in order.
        fn selects(&self) -> Vec<(Vec<String>, usize)> {
            self.selects.borrow().clone()
        }

        /// The hint shown by each `secret` prompt, in order.
        fn secret_hints(&self) -> Vec<Option<String>> {
            self.secret_hints.borrow().clone()
        }
    }

    impl Prompter for ScriptedPrompter {
        fn is_interactive(&self) -> bool {
            true
        }

        fn input(&self, prompt: &str, default: Option<&str>, check: InputCheck) -> Result<String> {
            let Answer::Input(value) = self.next("input", prompt) else {
                panic!("next scripted answer was not an input at {prompt:?}");
            };
            // Like `inquire`, an empty answer takes the default, and the
            // checks see the answer that results.
            let value = match default {
                Some(default) if value.is_empty() => default.to_owned(),
                _ => value,
            };
            assert!(
                !value.trim().is_empty(),
                "the real prompt refuses the blank answer {value:?} at {prompt:?}"
            );
            assert!(
                check(value.trim()).is_ok(),
                "the real prompt refuses the answer {value:?} at {prompt:?}"
            );
            Ok(value)
        }

        fn select(
            &self,
            prompt: &str,
            items: Vec<String>,
            default: usize,
        ) -> Result<Option<usize>> {
            self.selects.borrow_mut().push((items, default));
            match self.next("select", prompt) {
                Answer::Select(value) => Ok(value),
                _ => panic!("next scripted answer was not a select at {prompt:?}"),
            }
        }

        fn secret(&self, prompt: &str, hint: Option<&str>) -> Result<Option<String>> {
            self.secret_hints.borrow_mut().push(hint.map(str::to_owned));
            match self.next("secret", prompt) {
                Answer::Secret(Some(value)) if refuses_blank_secret(&value, hint.is_some()) => {
                    panic!("the real prompt refuses the blank answer {value:?} at {prompt:?}")
                }
                Answer::Secret(value) => Ok(value),
                _ => panic!("next scripted answer was not a secret at {prompt:?}"),
            }
        }

        fn confirm(&self, prompt: &str) -> Result<bool> {
            match self.next("confirm", prompt) {
                Answer::Confirm(value) => Ok(value),
                _ => panic!("next scripted answer was not a confirm at {prompt:?}"),
            }
        }
    }

    // --- Environment isolation -------------------------------------------------

    /// Serializes every test that mutates process-global env. Held for a test's
    /// whole body via the guard inside [`LoginEnv`].
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// `GET /auth/cli/config` body reporting OAuth *unavailable* — the default, so
    /// tests that don't care about OAuth never surface the option.
    const OAUTH_DISABLED: &str = r#"{"port_strategy":"disabled"}"#;
    /// A body reporting OAuth *available* (an ephemeral loopback port).
    const OAUTH_EPHEMERAL: &str = r#"{"port_strategy":"ephemeral"}"#;

    const REGISTRY_URL: &str = "registry.mytenant.antithesis.com";
    /// `GET /api/v1/registry` body for a registry that takes the Antithesis
    /// credential.
    const REGISTRY_ANTITHESIS: &str =
        r#"{"auth":"antithesis","registry_url":"registry.mytenant.antithesis.com"}"#;

    /// Credential-kind menu labels, matching the `Display` impl on `AuthSetupType`.
    const API_KEY: &str = "API Key";
    const OAUTH: &str = "Single sign-on (OAuth)";

    /// A per-test isolated environment: an exclusive env lock, a throwaway `$HOME`,
    /// and an in-process mock backend for the OAuth-config probe and the registry.
    struct LoginEnv {
        home: tempfile::TempDir,
        base_url: String,
        // Held for the test's lifetime so no other test mutates env concurrently.
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl LoginEnv {
        /// Isolated env whose `/auth/cli/config` reports OAuth disabled.
        fn new() -> Self {
            Self::with_oauth_config(OAUTH_DISABLED)
        }

        /// Isolated env whose `/auth/cli/config` returns `config_body`.
        fn with_oauth_config(config_body: &'static str) -> Self {
            Self::with_backend(config_body, 200, REGISTRY_ANTITHESIS)
        }

        /// Isolated env whose `/api/v1/registry` answers `status` and `body`.
        fn with_registry(status: u16, body: &'static str) -> Self {
            Self::with_backend(OAUTH_DISABLED, status, body)
        }

        fn with_backend(
            config_body: &'static str,
            registry_status: u16,
            registry_body: &'static str,
        ) -> Self {
            let guard = ENV_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let home = tempfile::TempDir::new().expect("create temp HOME");
            let base_url = spawn_login_backend(config_body, registry_status, registry_body);

            // SAFETY: the `ENV_LOCK` guard we hold serializes every test in this
            // binary that touches process-global env, so nothing else reads or
            // writes these vars while the guard is alive. They are (re)set from
            // scratch for each test, so no state leaks between tests.
            unsafe {
                std::env::set_var("HOME", home.path());
                std::env::set_var("ANTITHESIS_BASE_URL", &base_url);
                std::env::set_var("SNOUTY_API_CACHE_DIR", home.path().join("api-cache"));
                // A stray value for any of these in the developer's shell would leak
                // into settings resolution; clear them for a clean baseline. The
                // credential variables come from `auth::CREDENTIAL_ENV_VARS` so this
                // list cannot drift from what ambient resolution actually reads.
                for key in [
                    "XDG_CONFIG_HOME",
                    "ANTITHESIS_PROFILE",
                    "ANTITHESIS_TENANT",
                    "ANTITHESIS_REGISTRY",
                    "ANTITHESIS_REPOSITORY",
                    "ANTITHESIS_HTTPS_PROXY",
                    "CONTAINER_ENGINE",
                    "SNOUTY_SETTINGS_PATH",
                ]
                .into_iter()
                .chain(crate::auth::CREDENTIAL_ENV_VARS)
                {
                    std::env::remove_var(key);
                }
            }

            LoginEnv {
                home,
                base_url,
                _guard: guard,
            }
        }

        fn api_cache_dir(&self) -> PathBuf {
            self.home.path().join("api-cache")
        }

        fn config_dir(&self) -> PathBuf {
            self.home.path().join(".config").join("snouty")
        }

        fn settings(&self) -> String {
            std::fs::read_to_string(self.config_dir().join("settings.toml")).unwrap_or_default()
        }

        fn credentials(&self) -> String {
            std::fs::read_to_string(self.config_dir().join("credentials.toml")).unwrap_or_default()
        }

        /// Seed a file under the isolated `$HOME` (e.g. an unparsable
        /// `settings.toml`) before running login.
        fn seed(&self, rel: &str, contents: &str) {
            let path = self.home.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }

        /// Resolve settings the way `main` does, under this test's isolated `$HOME`.
        // `&self` keeps the call inside the environment's lifetime.
        #[allow(clippy::unused_self)]
        fn resolve_settings(&self, profile: Option<&str>) -> Result<Settings> {
            Settings::resolve(None, profile.map(str::to_owned))
        }
    }

    /// [`do_cmd_login`] without `--tenant`, `--registry`, or `--verbose`.
    async fn login(
        profile: Option<&str>,
        settings: &Settings,
        prompter: &dyn Prompter,
    ) -> Result<()> {
        do_cmd_login(
            None,
            None,
            profile,
            settings,
            OutputOptions::default(),
            prompter,
        )
        .await
    }

    /// [`do_cmd_login`] with `--tenant mytenant`, and `--registry` when
    /// `registry` is set.
    async fn login_with_flags(
        registry: Option<&str>,
        settings: &Settings,
        prompter: &dyn Prompter,
    ) -> Result<()> {
        do_cmd_login(
            Some("mytenant".to_owned()),
            registry.map(|registry| registry.parse().unwrap()),
            None,
            settings,
            OutputOptions::default(),
            prompter,
        )
        .await
    }

    /// A fresh login with no flags prompts for the tenant and the credential,
    /// then saves the tenant's registry and persists an API key.
    #[tokio::test]
    async fn login_collects_and_persists_an_api_key() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_A)
            .build();

        login(None, &settings, &prompter).await?;

        let stored = env.settings();
        assert!(stored.contains(r#"tenant = "mytenant""#), "{stored}");
        assert!(
            stored.contains(&format!(r#"registry = "{REGISTRY_URL}""#)),
            "{stored}"
        );
        let creds = env.credentials();
        assert!(creds.contains(r#"type = "ApiKey""#), "{creds}");
        assert!(creds.contains(&api_key_line(FAKE_API_KEY_A)), "{creds}");
        Ok(())
    }

    /// With `--tenant` and `--registry` supplied, only the credential prompt is
    /// shown, and the given registry takes the place of the tenant's registry.
    #[tokio::test]
    async fn login_flags_skip_the_tenant_prompt_and_override_the_registry() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().secret(FAKE_API_KEY_A).build();

        login_with_flags(Some("myrepo"), &settings, &prompter).await?;

        assert_eq!(
            prompter.prompts(),
            vec!["Please enter your API Key".to_owned()]
        );
        let stored = env.settings();
        assert!(stored.contains(r#"tenant = "mytenant""#), "{stored}");
        assert!(stored.contains(r#"registry = "myrepo""#), "{stored}");
        Ok(())
    }

    /// A `repository` key, an alias for `registry`, loads as the
    /// registry. Login writes `registry`, and removes `repository` from the
    /// table that it writes.
    #[tokio::test]
    async fn login_replaces_the_repository_alias_with_registry() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(
            ".config/snouty/settings.toml",
            "repository = \"old\"\n[profile.p]\nrepository = \"old-p\"\n",
        );
        let settings = env.resolve_settings(None)?;
        assert_eq!(settings.registry(), Some("old"));
        let prompter = Script::default().secret(FAKE_API_KEY_A).build();

        login_with_flags(Some("myrepo"), &settings, &prompter).await?;

        let stored: toml::Table = env.settings().parse()?;
        assert_eq!(stored["registry"].as_str(), Some("myrepo"), "{stored}");
        assert!(!stored.contains_key("repository"), "{stored}");
        // Login writes only the top-level table, so the profile keeps its key.
        assert_eq!(
            stored["profile"]["p"]["repository"].as_str(),
            Some("old-p"),
            "{stored}"
        );
        Ok(())
    }

    /// A credential that the tenant rejects fails the login, which saves
    /// nothing.
    #[tokio::test]
    async fn login_saves_nothing_when_the_tenant_rejects_the_credential() -> Result<()> {
        let env = LoginEnv::with_registry(401, r#"{"message":"Invalid or expired bearer token."}"#);
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().secret(FAKE_API_KEY_A).build();

        let err = login_with_flags(Some("myrepo"), &settings, &prompter)
            .await
            .expect_err("a rejected credential must fail the login");

        assert!(
            format!("{err:#}").contains("did not accept the credential"),
            "{err:#}"
        );
        assert!(env.settings().is_empty(), "{}", env.settings());
        assert!(env.credentials().is_empty(), "{}", env.credentials());
        Ok(())
    }

    /// Login never takes the registry from the response cache, which would
    /// not check the credential.
    #[tokio::test]
    async fn login_does_not_read_the_registry_from_the_response_cache() -> Result<()> {
        use crate::api_cache::{ApiCache, CachePolicy, DEFAULT_MAX_FILE_SIZE};

        let env = LoginEnv::with_registry(401, r#"{"message":"Invalid or expired bearer token."}"#);
        let cache = ApiCache::new(
            Some(env.api_cache_dir()),
            DEFAULT_MAX_FILE_SIZE,
            normalize_base_url(env.base_url.clone()),
            false,
        );
        let cached: RegistryResponse = serde_json::from_str(REGISTRY_ANTITHESIS)?;
        let lifetime = CachePolicy::Cacheable(Duration::from_secs(3600));
        cache
            .store_value(&cache.key("get_registry", &()), &cached.with_tag(lifetime))
            .await;
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().secret(FAKE_API_KEY_A).build();

        let err = login_with_flags(None, &settings, &prompter)
            .await
            .expect_err("a rejected credential must fail the login");

        assert!(
            format!("{err:#}").contains("did not accept the credential"),
            "{err:#}"
        );
        Ok(())
    }

    /// A credential that login cannot store puts the settings file back, so
    /// the profile does not name the new tenant with the old credential.
    #[tokio::test]
    async fn login_restores_the_settings_when_it_cannot_store_the_credential() -> Result<()> {
        let env = LoginEnv::new();
        let seeded = "tenant = \"oldtenant\"\nregistry = \"old.example.com/app\"\n";
        env.seed(".config/snouty/settings.toml", seeded);
        // A directory where the credentials file goes cannot be read or
        // replaced.
        std::fs::create_dir_all(env.config_dir().join("credentials.toml"))?;
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_A)
            .build();

        let err = login(None, &settings, &prompter)
            .await
            .expect_err("a credential that cannot be stored must fail the login");

        assert!(format!("{err:#}").contains("nothing was saved"), "{err:#}");
        assert_eq!(env.settings(), seeded);

        // With no settings file before, none is left behind.
        std::fs::remove_file(env.config_dir().join("settings.toml"))?;
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_A)
            .build();
        let err = login(None, &settings, &prompter)
            .await
            .expect_err("a credential that cannot be stored must fail the login");
        assert!(format!("{err:#}").contains("nothing was saved"), "{err:#}");
        assert!(!env.config_dir().join("settings.toml").exists());

        // A settings file that does not parse stays in its backup, and the
        // restore leaves no settings file.
        let unparsable = "tenant = \"oldtenant\n";
        env.seed(".config/snouty/settings.toml", unparsable);
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_A)
            .build();
        let err = login(None, &settings, &prompter)
            .await
            .expect_err("a credential that cannot be stored must fail the login");
        assert!(format!("{err:#}").contains("nothing was saved"), "{err:#}");
        assert!(!env.config_dir().join("settings.toml").exists());
        assert_eq!(
            std::fs::read_to_string(env.config_dir().join("settings.toml.bak"))?,
            unparsable
        );
        Ok(())
    }

    /// Login writes the settings before the credential, so a settings file
    /// that login cannot update leaves the stored credentials alone.
    #[tokio::test]
    async fn login_stores_no_credential_when_it_cannot_write_the_settings() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        env.seed(
            ".config/snouty/settings.toml",
            "profile = \"not a table\"\n",
        );
        let prompter = Script::default()
            .input("ptenant")
            .secret(FAKE_API_KEY_A)
            .build();

        let err = login(Some("prod"), &settings, &prompter)
            .await
            .expect_err("a malformed settings file must fail the login");

        assert!(format!("{err:#}").contains("malformed"), "{err:#}");
        assert!(env.credentials().is_empty(), "{}", env.credentials());
        Ok(())
    }

    /// A registry that takes the Docker login is saved as the registry.
    #[tokio::test]
    async fn login_saves_a_registry_that_takes_the_docker_login() -> Result<()> {
        let env = LoginEnv::with_registry(
            200,
            r#"{"auth":"docker","registry_url":"registry.example.com/acme"}"#,
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().secret(FAKE_API_KEY_A).build();

        login_with_flags(None, &settings, &prompter).await?;

        let stored = env.settings();
        assert!(
            stored.contains(r#"registry = "registry.example.com/acme""#),
            "{stored}"
        );
        Ok(())
    }

    /// A tenant that does not serve the registry endpoint answers 404, and
    /// login asks for the registry, offering the saved one.
    #[tokio::test]
    async fn login_asks_for_the_registry_when_the_tenant_does_not_report_it() -> Result<()> {
        let env = LoginEnv::with_registry(404, r#"{"message":"Resource not found"}"#);
        env.seed(
            ".config/snouty/settings.toml",
            "registry = \"saved.example.com/app\"\n",
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .secret(FAKE_API_KEY_A)
            .input("typed.example.com/app")
            .build();

        login_with_flags(None, &settings, &prompter).await?;

        assert_eq!(
            prompter.prompts().last().map(String::as_str),
            Some("What container registry would you like to use?")
        );
        let stored = env.settings();
        assert!(
            stored.contains(r#"registry = "typed.example.com/app""#),
            "{stored}"
        );
        assert!(env.credentials().contains(&api_key_line(FAKE_API_KEY_A)));
        Ok(())
    }

    /// An empty answer at the registry prompt takes the saved registry, which
    /// login saves without its scheme and trailing `/`, in its own case.
    #[tokio::test]
    async fn login_saves_the_registry_prompt_default_without_its_scheme() -> Result<()> {
        let env = LoginEnv::with_registry(404, r#"{"message":"Resource not found"}"#);
        env.seed(
            ".config/snouty/settings.toml",
            "tenant = \"mytenant\"\nregistry = \"https://saved.example.com/MyOrg/\"\n",
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().secret(FAKE_API_KEY_A).input("").build();

        login_with_flags(None, &settings, &prompter).await?;

        let stored = env.settings();
        assert!(
            stored.contains(r#"registry = "saved.example.com/MyOrg""#),
            "{stored}"
        );
        Ok(())
    }

    /// A registry that the tenant reports with a scheme and a trailing `/` is
    /// saved without them.
    #[tokio::test]
    async fn login_saves_the_tenant_registry_without_its_scheme() -> Result<()> {
        let env = LoginEnv::with_registry(
            200,
            r#"{"auth":"docker","registry_url":"https://registry.example.com/acme/"}"#,
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().secret(FAKE_API_KEY_A).build();

        login_with_flags(None, &settings, &prompter).await?;

        let stored = env.settings();
        assert!(
            stored.contains(r#"registry = "registry.example.com/acme""#),
            "{stored}"
        );
        Ok(())
    }

    /// A tenant that reports a blank registry fails the login, which saves
    /// nothing, unless --registry is given.
    #[tokio::test]
    async fn login_saves_nothing_when_the_tenant_reports_a_blank_registry() -> Result<()> {
        let env = LoginEnv::with_registry(200, r#"{"auth":"antithesis","registry_url":" "}"#);
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().secret(FAKE_API_KEY_A).build();

        let err = login_with_flags(None, &settings, &prompter)
            .await
            .expect_err("a blank registry must fail the login");

        assert!(
            format!("{err:#}").contains("login needs a container registry"),
            "{err:#}"
        );
        assert!(env.settings().is_empty(), "{}", env.settings());
        assert!(env.credentials().is_empty(), "{}", env.credentials());

        // --registry takes the place of the blank registry.
        let prompter = Script::default().secret(FAKE_API_KEY_A).build();
        login_with_flags(Some("myrepo"), &settings, &prompter).await?;
        assert!(env.settings().contains(r#"registry = "myrepo""#));
        Ok(())
    }

    /// Login replaces a username and password that an older snouty stored
    /// with an API key, without asking, because login cannot pass with them.
    #[tokio::test]
    async fn login_replaces_a_stored_username_and_password_with_an_api_key() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(".config/snouty/credentials.toml", &password_credentials());
        let settings = env.resolve_settings(None)?;
        // An empty script after the key panics if the flow asks anything more.
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_A)
            .build();

        login(None, &settings, &prompter).await?;

        let creds = env.credentials();
        assert!(creds.contains(r#"type = "ApiKey""#), "{creds}");
        assert!(!creds.contains(FAKE_PASSWORD), "{creds}");
        Ok(())
    }

    /// A stored credential of another profile is not replaced, so login does
    /// not ask.
    #[tokio::test]
    async fn login_does_not_ask_to_replace_another_profiles_credential() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(".config/snouty/credentials.toml", &password_credentials());
        let settings = env.resolve_settings(Some("prod"))?;
        let prompter = Script::default()
            .input("ptenant")
            .secret(FAKE_API_KEY_B)
            .build();

        login(Some("prod"), &settings, &prompter).await?;

        let creds = env.credentials();
        assert!(creds.contains(FAKE_PASSWORD), "{creds}");
        assert!(creds.contains(&api_key_line(FAKE_API_KEY_B)), "{creds}");
        Ok(())
    }

    /// The global `--profile` flag scopes the persisted login.
    #[tokio::test]
    async fn login_scopes_to_a_named_profile() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(Some("prod"))?;
        let prompter = Script::default()
            .input("ptenant")
            .secret(FAKE_API_KEY_B)
            .build();

        login(Some("prod"), &settings, &prompter).await?;

        let stored = env.settings();
        assert!(stored.contains("[profile.prod]"), "{stored}");
        Ok(())
    }

    /// A tenant that isn't a valid hostname is rejected after the prompt, before
    /// the registry/credential prompts and before anything is persisted.
    #[tokio::test]
    async fn login_rejects_an_invalid_tenant() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("underscores_are_not_allowed")
            .build();

        let result = login(None, &settings, &prompter).await;

        let err = result.expect_err("an invalid tenant must fail the login");
        assert!(
            format!("{err:#}").contains("a tenant must be a valid hostname"),
            "unexpected error: {err:#}"
        );
        // Only the tenant was prompted; the flow bailed before registry/credentials.
        assert_eq!(prompter.prompts().len(), 1, "{:?}", prompter.prompts());
        assert!(env.settings().is_empty(), "nothing should be persisted");
        Ok(())
    }

    /// Conversely, when the backend advertises an OAuth port strategy, the "Single
    /// sign-on (OAuth)" option *is* offered — first in the menu and as the
    /// default.
    #[tokio::test]
    async fn login_offers_oauth_first_when_backend_supports_it() -> Result<()> {
        let env = LoginEnv::with_oauth_config(OAUTH_EPHEMERAL);
        let settings = env.resolve_settings(None)?;
        // Finish via the API-key option rather than OAuth, which would start
        // the interactive browser flow.
        let prompter = Script::default()
            .input("mytenant")
            .select(1)
            .secret(FAKE_API_KEY_A)
            .build();

        login(None, &settings, &prompter).await?;

        let (menu, default) = &prompter.selects()[0];
        assert_eq!(
            menu,
            &[OAUTH.to_owned(), API_KEY.to_owned()],
            "OAuth must lead the menu when the backend advertises a port strategy"
        );
        assert_eq!(*default, 0, "OAuth must be the default for a first login");
        Ok(())
    }

    /// A stored API key moves the menu's default to the API key entry, so "log
    /// in again the same way" stays one keystroke.
    #[tokio::test]
    async fn login_defaults_to_the_previously_used_kind() -> Result<()> {
        let env = LoginEnv::with_oauth_config(OAUTH_EPHEMERAL);
        env.seed(
            ".config/snouty/credentials.toml",
            &api_key_credentials(FAKE_API_KEY_A),
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .select(1)
            .secret("")
            .build();

        login(None, &settings, &prompter).await?;

        let (menu, default) = &prompter.selects()[0];
        assert_eq!(menu[*default], API_KEY);
        Ok(())
    }

    /// A stored API key is offered back: the prompt shows a masked hint, and an
    /// empty answer keeps the stored key.
    #[tokio::test]
    async fn login_keeps_the_stored_api_key_on_an_empty_answer() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(
            ".config/snouty/credentials.toml",
            &api_key_credentials(FAKE_API_KEY_A),
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .secret("") // hit enter
            .build();

        login(None, &settings, &prompter).await?;

        assert_eq!(
            prompter.secret_hints(),
            vec![Some("********9Pgw".to_owned())],
            "the key prompt must show that a stored key is there to keep"
        );
        let creds = env.credentials();
        assert!(creds.contains(&api_key_line(FAKE_API_KEY_A)), "{creds}");
        Ok(())
    }

    /// Typing the stored key again keeps it: login asks nothing and writes
    /// nothing.
    #[tokio::test]
    async fn login_writes_nothing_when_the_stored_key_is_typed_again() -> Result<()> {
        let env = LoginEnv::new();
        let seeded = api_key_credentials(FAKE_API_KEY_A);
        env.seed(".config/snouty/credentials.toml", &seeded);
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_A)
            .build();

        login(None, &settings, &prompter).await?;

        assert_eq!(env.credentials(), seeded);
        Ok(())
    }

    /// Whitespace around a pasted key is dropped, so the stored key pasted
    /// with a trailing newline still keeps it.
    #[tokio::test]
    async fn login_trims_a_pasted_key() -> Result<()> {
        let env = LoginEnv::new();
        let seeded = api_key_credentials(FAKE_API_KEY_A);
        env.seed(".config/snouty/credentials.toml", &seeded);
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .secret(&format!(" {FAKE_API_KEY_A}\n"))
            .build();

        login(None, &settings, &prompter).await?;

        assert_eq!(env.credentials(), seeded);
        Ok(())
    }

    /// Login for a profile offers back only that profile's key, not the key of
    /// the default profile, and storing a key for it replaces nothing.
    #[tokio::test]
    async fn login_does_not_offer_another_profiles_key() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(
            ".config/snouty/credentials.toml",
            &api_key_credentials(FAKE_API_KEY_A),
        );
        let settings = env.resolve_settings(Some("prod"))?;
        let prompter = Script::default()
            .input("ptenant")
            .secret(FAKE_API_KEY_B)
            .build();

        login(Some("prod"), &settings, &prompter).await?;

        assert_eq!(prompter.secret_hints(), vec![None]);
        let creds = env.credentials();
        assert!(creds.contains(&api_key_line(FAKE_API_KEY_A)), "{creds}");
        assert!(creds.contains(&api_key_line(FAKE_API_KEY_B)), "{creds}");
        Ok(())
    }

    /// A key typed at the prompt replaces the stored one once the user confirms.
    #[tokio::test]
    async fn login_replaces_the_stored_api_key_when_one_is_typed() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(
            ".config/snouty/credentials.toml",
            &api_key_credentials(FAKE_API_KEY_A),
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_B)
            .confirm(true)
            .build();

        login(None, &settings, &prompter).await?;

        let creds = env.credentials();
        assert!(creds.contains(&api_key_line(FAKE_API_KEY_B)), "{creds}");
        Ok(())
    }

    /// A blank stored API key is not offered back, so an empty answer cannot
    /// keep it, and a new key replaces it without a question.
    #[tokio::test]
    async fn login_replaces_a_blank_stored_api_key_without_asking() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(".config/snouty/credentials.toml", &api_key_credentials(""));
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_A)
            .build();

        login(None, &settings, &prompter).await?;

        assert_eq!(prompter.secret_hints(), vec![None]);
        let creds = env.credentials();
        assert!(creds.contains(&api_key_line(FAKE_API_KEY_A)), "{creds}");
        Ok(())
    }

    /// A new single sign-on replaces stored OAuth tokens without a question,
    /// but an API key that would replace them still asks.
    #[test]
    fn only_a_new_sign_in_replaces_stored_oauth_tokens_without_asking() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(
            ".config/snouty/credentials.toml",
            &oauth_credentials(Some(FAKE_REFRESH_TOKEN_A)),
        );
        let stored = AuthenticationInfo::stored_for_profile(None)?;
        let stored = stored.as_ref().map(Tagged::value);

        // An empty script panics if the flow asks anything.
        let prompter = Script::default().build();
        assert!(confirm_replacement(
            &prompter,
            stored,
            AuthSetupType::OAuth
        )?);

        let prompter = Script::default().confirm(false).build();
        assert!(!confirm_replacement(
            &prompter,
            stored,
            AuthSetupType::ApiKey
        )?);
        assert_eq!(
            prompter.prompts(),
            vec!["Replace your stored OAuth credentials?".to_owned()]
        );
        Ok(())
    }

    /// Single sign-on asks before the browser sign-in starts, and a no keeps
    /// the stored credential without a sign-in.
    #[tokio::test]
    async fn login_asks_before_single_sign_on_replaces_a_stored_credential() -> Result<()> {
        let env = LoginEnv::with_oauth_config(OAUTH_EPHEMERAL);
        let seeded = api_key_credentials(FAKE_API_KEY_A);
        env.seed(".config/snouty/credentials.toml", &seeded);
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .select(0)
            .confirm(false)
            .build();

        login(None, &settings, &prompter).await?;

        assert_eq!(
            prompter.prompts().last().map(String::as_str),
            Some("Replace your stored API key?")
        );
        assert_eq!(env.credentials(), seeded);
        Ok(())
    }

    /// An API key in the environment is not offered back, because keeping it
    /// would copy an ambient secret into the credentials file.
    #[tokio::test]
    async fn login_does_not_offer_back_an_api_key_from_the_environment() -> Result<()> {
        let env = LoginEnv::new();
        // SAFETY: `LoginEnv` holds `ENV_LOCK` for this test's whole body, so no
        // other test reads or writes process env while this var is set.
        unsafe { std::env::set_var("ANTITHESIS_API_KEY", FAKE_API_KEY_B) };
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .secret(FAKE_API_KEY_A)
            .build();

        let result = login(None, &settings, &prompter).await;
        // SAFETY: as above.
        unsafe { std::env::remove_var("ANTITHESIS_API_KEY") };
        result?;

        assert_eq!(
            prompter.secret_hints(),
            vec![None],
            "an ambient key must not be offered as something to keep"
        );
        Ok(())
    }

    /// Esc at the API key prompt skips credential storage: login checks the
    /// credential from the environment, saves the tenant and registry, and
    /// writes no credentials file.
    #[tokio::test]
    async fn login_skips_credential_storage_on_esc() -> Result<()> {
        let env = LoginEnv::new();
        // SAFETY: `LoginEnv` holds `ENV_LOCK` for this test's whole body, so no
        // other test reads or writes process env while this var is set.
        unsafe { std::env::set_var("ANTITHESIS_API_KEY", FAKE_API_KEY_B) };
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().input("mytenant").skip_secret().build();

        let result = login(None, &settings, &prompter).await;
        // SAFETY: as above.
        unsafe { std::env::remove_var("ANTITHESIS_API_KEY") };
        result?;

        assert!(env.settings().contains(r#"tenant = "mytenant""#));
        assert!(
            !env.config_dir().join("credentials.toml").exists(),
            "Esc at the API key prompt must write no credentials file"
        );
        Ok(())
    }

    /// A stored credential that cannot be read fails the login with the read
    /// error, not with a claim that no credential exists.
    #[tokio::test]
    async fn login_reports_a_stored_credential_that_cannot_be_read() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(".config/snouty/credentials.toml", "not = [valid toml");
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().input("mytenant").skip_secret().build();

        let err = login(None, &settings, &prompter)
            .await
            .expect_err("an unreadable credential must fail the login");

        assert!(
            !format!("{err:#}").contains("login needs an API key"),
            "{err:#}"
        );
        assert!(env.settings().is_empty(), "{}", env.settings());
        Ok(())
    }

    /// Esc at the menu with no credential anywhere fails the login, which
    /// saves nothing.
    #[tokio::test]
    async fn login_fails_on_esc_at_the_menu_without_a_credential() -> Result<()> {
        let env = LoginEnv::with_oauth_config(OAUTH_EPHEMERAL);
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().input("mytenant").skip_select().build();

        assert!(
            login(None, &settings, &prompter).await.is_err(),
            "login without a credential must fail"
        );

        assert!(env.settings().is_empty(), "{}", env.settings());
        assert!(!env.config_dir().join("credentials.toml").exists());
        Ok(())
    }

    // --- Real macOS Keychain -------------------------------------------------
    /// Run `/usr/bin/security` under `home` so its keychain preferences (default
    /// keychain, search list) land in the same `$HOME/Library/Preferences`
    /// snouty reads back — see [`login_persists_to_real_macos_keychain`].
    fn security(home: &std::path::Path, args: &[&str]) -> std::process::Output {
        Command::new("/usr/bin/security")
            .args(args)
            .env("HOME", home)
            .output()
            .expect("run /usr/bin/security")
    }

    /// Restores the default keychain and deletes the throwaway one — even if the
    /// test panics partway through. Everything lives under the throwaway `$HOME`,
    /// so this is mostly belt-and-suspenders on top of the temp-tree cleanup.
    struct KeychainGuard {
        home: PathBuf,
        keychain: PathBuf,
        original_default: String,
    }

    impl Drop for KeychainGuard {
        fn drop(&mut self) {
            // A fresh temp HOME has no prior default, so only restore when we
            // actually captured one (otherwise `default-keychain -s ""` just
            // errors noisily).
            if !self.original_default.is_empty() {
                security(
                    &self.home,
                    &["default-keychain", "-s", &self.original_default],
                );
            }
            if let Some(path) = self.keychain.to_str() {
                security(&self.home, &["delete-keychain", path]);
            }
        }
    }

    /// Exercises `snouty login`'s **real macOS Keychain** path end-to-end.
    #[tokio::test]
    async fn login_persists_to_real_macos_keychain() -> Result<()> {
        if !cfg!(target_os = "macos") || std::env::var_os("GITHUB_ACTIONS").is_none() {
            eprintln!("skipping real-keychain test: not a macOS GitHub Actions runner");
            return Ok(());
        }

        #[cfg(target_os = "macos")]
        {
            keyring_core::set_default_store(apple_native_keyring_store::keychain::Store::new()?);
        }

        // `LoginEnv` sets an isolated `$HOME` + mock OAuth-config backend and
        // holds `ENV_LOCK` for the whole test. We run `security` under that same
        // HOME so snouty resolves the keychain we configure.
        let env = LoginEnv::new();
        let home = env.home.path().to_path_buf();

        // `security` writes its preferences under `$HOME/Library/Preferences`;
        // on a fresh temp HOME those directories don't exist yet.
        std::fs::create_dir_all(home.join("Library/Preferences"))
            .expect("create Library/Preferences");
        std::fs::create_dir_all(home.join("Library/Keychains")).expect("create Library/Keychains");
        let keychain = home.join("snouty-test.keychain-db");
        let kc = keychain.to_str().expect("keychain path utf-8");
        let password = FAKE_PASSWORD;

        // Create the throwaway keychain and capture the current default before we
        // repoint it, so the guard can put everything back.
        assert!(
            security(&home, &["create-keychain", "-p", password, kc])
                .status
                .success(),
            "create-keychain failed"
        );
        let original_default = {
            let out = security(&home, &["default-keychain"]);
            String::from_utf8_lossy(&out.stdout)
                .trim()
                .trim_matches('"')
                .to_string()
        };
        // From here on, always restore the default and delete the keychain. `env`
        // (declared first) drops last, so the temp HOME outlives this guard's
        // `delete-keychain`.
        let _guard = KeychainGuard {
            home: home.clone(),
            keychain: keychain.clone(),
            original_default,
        };
        // No auto-lock (so it can't relock mid-test), unlocked, and made default
        // so snouty's store resolves to it.
        assert!(
            security(&home, &["set-keychain-settings", kc])
                .status
                .success(),
            "set-keychain-settings failed"
        );
        assert!(
            security(&home, &["unlock-keychain", "-p", password, kc])
                .status
                .success(),
            "unlock-keychain failed"
        );
        assert!(
            security(&home, &["default-keychain", "-s", kc])
                .status
                .success(),
            "setting default keychain failed"
        );

        // --- Write an API key; it must land in the keychain, not a file. ---
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("acme")
            .secret(FAKE_API_KEY_A)
            .build();
        login(None, &settings, &prompter).await?;

        assert!(
            !env.config_dir().join("credentials.toml").exists(),
            "credentials.toml must not be written when the keychain is used"
        );
        // The credential is really in the keychain under snouty's service/account.
        // Attributes only (no `-w`/`-g`): reading the secret through the
        // `security` CLI (not the app that created it) trips macOS's "allow
        // access" ACL confirmation, which has no GUI to answer on CI and would
        // hang the test. The in-process read-back below proves the secret itself
        // round-trips.
        assert!(
            security(
                &home,
                &[
                    "find-generic-password",
                    "-s",
                    "snouty",
                    "-a",
                    "_default_",
                    kc
                ],
            )
            .status
            .success(),
            "credential not found in keychain under `_default_`"
        );

        // --- Read-back: an in-process read does not trip the ACL prompt that
        // the `security` CLI would. ---
        let resolved = AuthenticationInfo::stored_for_profile(None)?;
        match resolved.as_ref().map(|found| (found.value(), found.tag())) {
            Some((
                AuthenticationInfo::ApiKey { api_key },
                CredentialStore::Keychain { profile: None },
            )) => assert_eq!(api_key.as_str(), FAKE_API_KEY_A),
            _ => panic!("expected the API key to resolve from the keychain"),
        }

        // --- Profile scoping uses a distinct keychain entry (`profile_<name>`). ---
        let prof_settings = env.resolve_settings(Some("prod"))?;
        let prof_prompter = Script::default()
            .input("acme")
            .secret(FAKE_API_KEY_B)
            .build();
        login(Some("prod"), &prof_settings, &prof_prompter).await?;
        assert!(
            security(
                &home,
                &[
                    "find-generic-password",
                    "-s",
                    "snouty",
                    "-a",
                    "profile_prod",
                    kc,
                ],
            )
            .status
            .success(),
            "profile credential not found under `profile_prod`"
        );

        Ok(())
    }
}
