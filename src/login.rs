use std::io::{self, IsTerminal};
use std::path::Path;
use std::time::Duration;

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use color_eyre::Section;
use color_eyre::eyre::{Context, Result, eyre};
use inquire::{Password, PasswordDisplayMode, Select, Text};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::settings;
use crate::{
    attributed_value::AttributedValue,
    auth::{AuthenticationInfo, PasswordPolicy, PersistableCredentials, persist},
    env,
    error::user_error,
    settings::{
        ANTITHESIS_PROFILE_ENV_VAR_NAME, Settings, update_settings_in_global_file,
        validate_tenant_host,
    },
};

const OAUTH_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for the browser to redirect back to the loopback server
/// before giving up — generous enough for the user to complete an interactive
/// sign-in (including MFA).
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(300);

/// Wrapping our TUI (`inquire`) in a trait so that it can be subbed out for testing
trait Prompter {
    /// Whether we're attached to an interactive terminal.
    fn is_interactive(&self) -> bool;

    /// A free-text line, pre-filled with `default` when the user just hits enter.
    fn input(&self, prompt: &str, default: Option<&str>) -> Result<String>;

    /// A single-choice menu over `items`, initially highlighting `default`.
    /// Returns `None` when the user cancels (Esc).
    fn select(&self, prompt: &str, items: Vec<String>, default: usize) -> Result<Option<usize>>;

    /// A masked secret (each character echoes as `*`). There is deliberately
    /// no confirmation round: Antithesis passwords are long generated strings
    /// that are pasted like API keys, not typed twice.
    ///
    /// `hint` is a display-only stand-in for a secret already stored (see
    /// [`secret_hint`]). When it is set, the prompt shows it and says that an
    /// empty answer keeps the stored secret. The secret itself never reaches
    /// the prompter.
    fn password(&self, prompt: &str, hint: Option<&str>) -> Result<String>;
}

/// The production [`Prompter`]: `inquire` prompts reading the real terminal.
struct InquirePrompter;

impl Prompter for InquirePrompter {
    fn is_interactive(&self) -> bool {
        io::stdin().is_terminal()
    }

    fn input(&self, prompt: &str, default: Option<&str>) -> Result<String> {
        let mut text = Text::new(prompt);
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

    fn password(&self, prompt: &str, hint: Option<&str>) -> Result<String> {
        // `inquire` has no default for a masked prompt, so the hint rides in
        // the message, in the same `(value)` shape `Text` gives a default.
        let message = match hint {
            Some(hint) => format!("{prompt} ({hint})"),
            None => prompt.to_owned(),
        };
        let mut password = Password::new(&message)
            .with_display_mode(PasswordDisplayMode::Masked)
            .without_confirmation();
        if hint.is_some() {
            password = password.with_help_message("hit enter to keep the stored value");
        }
        Ok(password.prompt()?)
    }
}

pub async fn cmd_login(
    tenant: Option<String>,
    repository: Option<String>,
    profile: Option<&str>,
    current_settings: &Settings,
) -> Result<()> {
    do_cmd_login(
        tenant,
        repository,
        profile,
        current_settings,
        &InquirePrompter,
    )
    .await
}

async fn do_cmd_login(
    tenant: Option<String>,
    repository: Option<String>,
    profile: Option<&str>,
    current_settings: &Settings,
    prompter: &dyn Prompter,
) -> Result<()> {
    let profile_to_use = profile
        .map(|p| p.to_owned())
        .or_else(|| env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME).ok().flatten());

    let tenant_to_use = match tenant {
        Some(arg_value) if !arg_value.is_empty() => arg_value,
        Some(_) | None => {
            prompt_for_value(prompter, "Antithesis tenant", current_settings.tenant())?
        }
    };
    validate_tenant_host(&tenant_to_use)?;

    let repository_to_use = match repository {
        Some(arg_value) if !arg_value.is_empty() => arg_value,
        Some(_) | None => prompt_for_value(
            prompter,
            "container repository",
            current_settings.repository(),
        )?,
    };

    // Whatever credentials are already in reach, so the menu can default to the
    // kind last used and the key prompt can offer the stored key back. Having
    // none is the ordinary state on a first login rather than a failure, so the
    // error is logged at debug level and then dropped.
    let current_credentials = AuthenticationInfo::for_ambient_configuration_with_attribution(
        profile_to_use.as_deref(),
        PasswordPolicy::Inspect,
    )
    .inspect_err(|err| debug!("no ambient credentials to offer back: {err:#}"))
    .ok();

    // Capture the credential kind and where it was stored so the summary can name
    // both; `None` when the user chose to skip credential setup.
    let credential_summary = if prompter.is_interactive() {
        match prompt_for_auth(prompter, &tenant_to_use, current_credentials.as_ref()).await? {
            Some(credentials) => {
                let kind = match &credentials {
                    PersistableCredentials::ApiKey { .. } => "API key",
                    PersistableCredentials::Password { .. } => "username and password",
                    PersistableCredentials::OAuth { .. } => "OAuth credentials",
                };
                Some(persist(credentials, profile_to_use.as_deref())?.with_value(kind))
            }
            None => None,
        }
    } else {
        warn!(
            "Cannot collect credentials unless running in a TTY. Please provide credentials via environment variables or rerun `snouty login` in an interactive session"
        );
        None
    };

    let settings_path = update_settings_in_global_file(
        Some(tenant_to_use.clone()),
        Some(repository_to_use.clone()),
        None,
        None,
        profile_to_use.as_deref(),
    )?;

    print_login_summary(
        &tenant_to_use,
        &repository_to_use,
        profile_to_use.as_deref(),
        &settings_path,
        credential_summary,
        current_credentials,
    );

    Ok(())
}

/// Confirm what `snouty login` persisted, where, and the obvious next step —
/// otherwise a successful login exits silently, leaving the user unsure it took.
fn print_login_summary(
    tenant: &str,
    repository: &str,
    profile: Option<&str>,
    settings_path: &Path,
    credentials: Option<AttributedValue<&str>>,
    previous_credentials: Option<AttributedValue<AuthenticationInfo>>,
) {
    let scope = match profile {
        Some(p) => format!(" under profile `{p}`"),
        None => String::new(),
    };
    // Only mention what was actually recorded: a blank repository is intentionally
    // not persisted (see `insert_key_if_non_empty` in settings.rs), so don't claim
    // we saved one.
    let mut saved = format!("tenant `{tenant}`");
    if !repository.is_empty() {
        saved.push_str(&format!(" and repository `{repository}`"));
    }
    println!("\nSaved {saved}{scope} to {}.", settings_path.display());
    match credentials {
        Some(AttributedValue::Keychain {
            value: kind,
            entry_name: _,
        }) => {
            println!("Stored your {kind}{scope} in the system keychain.");
        }
        Some(AttributedValue::SettingsFile {
            value: kind,
            settings_file_path: path,
            profile: _,
        }) => {
            println!("Stored your {kind}{scope} in {}.", path.display());
        }
        _ => match previous_credentials {
            Some(AttributedValue::Keychain { .. }) => {
                println!(
                    "Retained your previously stored credentials{scope} in the system keychain."
                );
            }
            Some(AttributedValue::SettingsFile {
                settings_file_path, ..
            }) => {
                println!(
                    "Retained your previously stored credentials{scope} in {}.",
                    settings_file_path.display()
                );
            }
            _ => {
                println!(
                    "Skipped credential storage — snouty will use the ANTITHESIS_API_KEY or ANTITHESIS_USERNAME/PASSWORD environment variables."
                );
            }
        },
    }
    println!("Run `snouty doctor` to verify your setup.");
}

fn prompt_for_value(
    prompter: &dyn Prompter,
    value_name: &str,
    previous_value: Option<&str>,
) -> Result<String> {
    if !prompter.is_interactive() {
        return Err(eyre!("Cannot prompt for value when not running in a TTY"));
    }
    prompter.input(
        &format!("What {value_name} would you like to use?"),
        previous_value,
    )
}

#[derive(Clone, Copy, PartialEq)]
enum AuthSetupType {
    ApiKey,
    Password,
    OAuth,
}

impl AuthSetupType {
    /// The credential menu, in order: single sign-on leads and is the
    /// first-login default; the deprecated username/password option is last.
    const IN_PREFERENCE_ORDER: [Self; 3] = [Self::OAuth, Self::ApiKey, Self::Password];

    /// Whether this menu entry collects the same credential kind as `info`.
    fn collects(self, info: &AuthenticationInfo) -> bool {
        match info {
            AuthenticationInfo::ApiKey { .. } => self == Self::ApiKey,
            AuthenticationInfo::Password { .. } => self == Self::Password,
            AuthenticationInfo::OAuth { .. } => self == Self::OAuth,
            // No menu entry sets up GitHub Actions OIDC; it is ambient-only.
            AuthenticationInfo::GithubActionsOidc { .. } => false,
        }
    }
}

impl std::fmt::Display for AuthSetupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthSetupType::ApiKey => f.write_str("API Key"),
            AuthSetupType::Password => f.write_str("Username & password (deprecated)"),
            AuthSetupType::OAuth => f.write_str("Single sign-on (OAuth)"),
        }
    }
}

async fn prompt_for_auth(
    prompter: &dyn Prompter,
    tenant: &str,
    previous_value: Option<&AttributedValue<AuthenticationInfo>>,
) -> Result<Option<PersistableCredentials>> {
    // ANTITHESIS_BASE_URL trumps the supplied tenant because the former is used by spec tests
    let base_url = env::var(settings::ANTITHESIS_BASE_URL_VAR_NAME)?
        .unwrap_or_else(|| format!("https://{tenant}.antithesis.com"));
    let client = reqwest::Client::builder()
        .timeout(OAUTH_HTTP_TIMEOUT)
        .build()
        .wrap_err("failed to build the OAuth HTTP client")?;
    let oauth_config = fetch_cli_config(&client, &base_url).await;

    let oauth_offered = oauth_config
        .as_ref()
        .is_ok_and(|config| !matches!(config, CliOAuthConfig::Disabled));
    let credential_options: Vec<AuthSetupType> = AuthSetupType::IN_PREFERENCE_ORDER
        .into_iter()
        .filter(|option| oauth_offered || *option != AuthSetupType::OAuth)
        .collect();

    // Default the highlighted option to whatever kind was last stored, so the
    // common "log in again the same way" case is one keystroke; otherwise
    // highlight the first (preferred) option. Credentials from environment
    // variables are not stored, so they set no default.
    let previous_kind = match previous_value {
        Some(creds) if !matches!(creds, AttributedValue::EnvironmentVariable { .. }) => {
            Some(creds.value())
        }
        _ => None,
    };
    let default = previous_kind
        .and_then(|kind| {
            credential_options
                .iter()
                .position(|option| option.collects(kind))
        })
        .unwrap_or(0);

    let labels = credential_options.iter().map(ToString::to_string).collect();
    let selection = prompter.select(
        "What kind of credentials would you like to use? (Hit Esc to skip)",
        labels,
        default,
    )?;

    match selection {
        None => Ok(None),
        Some(index) => match credential_options[index] {
            AuthSetupType::ApiKey => {
                prompt_for_api_key(prompter, stored_api_key(previous_value)).map(Some)
            }

            AuthSetupType::Password => match previous_value.map(AttributedValue::value) {
                Some(AuthenticationInfo::Password { username, .. }) => {
                    prompt_for_username_password(prompter, Some(username))
                }
                _ => prompt_for_username_password(prompter, None),
            }
            .map(Some),
            AuthSetupType::OAuth => complete_oauth_login(&client, &base_url, &oauth_config?)
                .await
                .map(Some),
        },
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

/// The API key already in storage, when that is what the previous credentials
/// hold. Credentials read from the environment are left out on purpose: keeping
/// one would copy an ambient secret into the credentials file. The menu's own
/// default ignores them for the same reason.
fn stored_api_key(previous: Option<&AttributedValue<AuthenticationInfo>>) -> Option<&str> {
    match previous {
        None | Some(AttributedValue::EnvironmentVariable { .. }) => None,
        Some(credentials) => match credentials.value() {
            AuthenticationInfo::ApiKey { api_key } => Some(api_key),
            _ => None,
        },
    }
}

/// An empty answer at a masked prompt keeps the secret already stored — that is
/// what the prompt's hint offers. With nothing stored, the empty answer stands.
fn keep_stored_if_empty(entered: String, stored: Option<&str>) -> String {
    match stored {
        Some(stored) if entered.is_empty() => stored.to_owned(),
        _ => entered,
    }
}

fn prompt_for_api_key(
    prompter: &dyn Prompter,
    stored: Option<&str>,
) -> Result<PersistableCredentials> {
    let hint = stored.map(secret_hint);
    let entered = prompter.password("Please enter your API Key", hint.as_deref())?;
    Ok(PersistableCredentials::ApiKey {
        api_key: keep_stored_if_empty(entered, stored),
    })
}

fn prompt_for_username_password(
    prompter: &dyn Prompter,
    previous_username: Option<&str>,
) -> Result<PersistableCredentials> {
    let username = prompt_for_value(prompter, "username", previous_username)?;
    if username.is_empty() {
        return Err(eyre!("Username cannot be empty"));
    }
    // No confirmation round: like an API key, an Antithesis password is a
    // long generated string that is pasted, not typed.
    let password = prompter.password("Please enter your password", None)?;

    Ok(PersistableCredentials::Password { username, password })
}

#[derive(Debug, Deserialize)]
#[serde(tag = "port_strategy", rename_all = "snake_case")]
enum CliOAuthConfig {
    /// Bind the first available port from `ports`, in order.
    Fixed { ports: Vec<u16> },
    /// Bind any available port (an OS-assigned ephemeral port).
    Ephemeral,
    /// The tenant has CLI OIDC wired up but no redirect strategy resolved, so
    /// CLI OAuth login is not usable.
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
/// 2. Bind a localhost-only, single-request HTTP server on the chosen port.
/// 3. `POST /auth/cli/login` — send the PKCE challenge + CSRF state, receive the
///    authorization URL.
/// 4. Send the user to that URL (open a browser, best-effort) and wait for the
///    browser to redirect to `http://localhost:<port>/callback?code=…&state=…`.
/// 5. `POST /auth/cli/token` — exchange the authorization code (in the
///    `Authorization` header, alongside the PKCE verifier) and the flow token
///    (in the body) for an Antithesis token.
async fn complete_oauth_login(
    client: &reqwest::Client,
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

    let location =
        request_login_redirect(client, base_url, port, &code_challenge, &cli_state).await?;

    // Best-effort: any failure to open a browser (invalid URL, no opener) is
    // not fatal — the URL is printed either way, so a headless or opener-less
    // environment can still complete the flow by hand.
    println!();
    match crate::browser::open_in_browser(&location) {
        Ok(()) => {
            println!("Opening login url in your browser");
            println!("If your browser didn't open, manually visit: {location}");
        }
        Err(err) => {
            println!("Failed to open login url automatically ({err}).");
            println!("Open the following url in your browser on this machine: {location}");
        }
    }
    println!("Waiting for you to complete sign-in in your browser...");

    let callback = wait_for_callback(listeners).await?;

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

/// `GET /auth/cli/config` — the redirect strategy for this tenant. A 403 means
/// the tenant has not enabled CLI OIDC at all (the route is feature-gated).
async fn fetch_cli_config(client: &reqwest::Client, base_url: &str) -> Result<CliOAuthConfig> {
    let response = client
        .get(format!("{base_url}/auth/cli/config"))
        .send()
        .await
        .wrap_err("failed to contact the tenant's OAuth configuration endpoint")?;

    if response.status() == reqwest::StatusCode::FORBIDDEN {
        return Err(
            user_error("this tenant has not enabled OAuth login for the CLI")
                .suggestion("choose API key or username/password authentication instead"),
        );
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
        .suggestion("choose API key or username/password authentication instead")),
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
/// the loopback port, and get back the IdP authorization URL to open.
async fn request_login_redirect(
    client: &reqwest::Client,
    base_url: &str,
    port: u16,
    code_challenge: &str,
    cli_state: &str,
) -> Result<String> {
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

    let redirect: LoginRedirect = client
        .post(format!("{base_url}/auth/cli/login"))
        .json(&LoginRequest {
            port,
            code_challenge,
            code_challenge_method: "S256",
            cli_state,
        })
        .send()
        .await
        .wrap_err("failed to initiate OAuth login")?
        .error_for_status()
        .wrap_err("the tenant rejected the OAuth login request")?
        .json()
        .await
        .wrap_err("failed to parse the OAuth login response")?;

    Ok(redirect.location)
}

/// `POST /auth/cli/token` — exchange the authorization code for tokens. The
/// authorization code and PKCE verifier ride in the `Authorization` header as
/// `Bearer base64url-nopad(code:verifier)` (keeping the secrets out of the
/// body/URL); the flow token goes in the JSON body.
async fn exchange_code_for_tokens(
    client: &reqwest::Client,
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

    client
        .post(format!("{base_url}/auth/cli/token"))
        .bearer_auth(credentials)
        .json(&TokenExchangeBody { flow_token })
        .send()
        .await
        .wrap_err("failed to exchange the authorization code for tokens")?
        .error_for_status()
        .wrap_err("the tenant rejected the token exchange")?
        .json::<TokenResponse>()
        .await
        .wrap_err("failed to parse the token exchange response")
}

/// Accept exactly one loopback connection, read the OAuth callback request, ack
/// it in the browser, and return the parsed callback parameters.
async fn wait_for_callback(listeners: CallbackListeners) -> Result<CallbackParams> {
    let received = tokio::time::timeout(
        CALLBACK_TIMEOUT,
        receive_callback_request(&listeners.listeners),
    )
    .await;
    let (mut stream, request_line) = match received {
        Ok(result) => result?,
        Err(_elapsed) => {
            return Err(user_error(format!(
                "timed out after {} seconds waiting for the browser to complete sign-in",
                CALLBACK_TIMEOUT.as_secs()
            )));
        }
    };

    let target = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| eyre!("malformed OAuth callback request line: {request_line:?}"))?;
    let result = parse_callback_params(target);

    // Acknowledge the request in the browser regardless of the parse outcome so
    // the user isn't left staring at a spinner; details land in the terminal.
    let body = match &result {
        Ok(_) => {
            "<html><body><h2>Sign-in complete</h2><p>You can close this tab and return to your terminal.</p></body></html>"
        }
        Err(_) => {
            "<html><body><h2>Sign-in failed</h2><p>Return to your terminal for details.</p></body></html>"
        }
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;

    result
}

async fn receive_callback_request(
    listeners: &[TcpListener],
) -> Result<(tokio::net::TcpStream, String)> {
    let (mut stream, _addr) = accept_any(listeners)
        .await
        .wrap_err("failed to accept the OAuth callback connection")?;
    let request_line = read_request_line(&mut stream).await?;
    Ok((stream, request_line))
}

/// Accept the first connection to arrive on any of the loopback listeners
/// (IPv4 and, when bound, IPv6), cancelling the others.
async fn accept_any(
    listeners: &[TcpListener],
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    let accepts = listeners
        .iter()
        .map(|listener| Box::pin(listener.accept()))
        .collect::<Vec<_>>();
    let (result, _index, _remaining) = futures_util::future::select_all(accepts).await;
    result
}

/// Read just the HTTP request line (everything up to the first CRLF) from the
/// callback connection. A standard OAuth authorization-code redirect is a GET
/// with everything we need in the query string, so the request line is all we
/// parse; the request is capped so a misbehaving client can't stream forever.
async fn read_request_line(stream: &mut tokio::net::TcpStream) -> Result<String> {
    const MAX_REQUEST_LINE: usize = 16 * 1024;

    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream
            .read(&mut chunk)
            .await
            .wrap_err("failed to read the OAuth callback request")?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            return Ok(String::from_utf8_lossy(&buf[..pos]).into_owned());
        }
        if buf.len() > MAX_REQUEST_LINE {
            break;
        }
    }
    Err(eyre!(
        "the OAuth callback request did not contain a valid request line"
    ))
}

/// Parse the callback request target (e.g. `/callback?code=…&state=…`) into the
/// authorization code and flow token. The proxy carries the flow token in the
/// standard OAuth `state` parameter. An IdP-reported `error` surfaces as an error.
fn parse_callback_params(target: &str) -> Result<CallbackParams> {
    // The target is a relative request URI; resolve it against a dummy loopback
    // base purely so `Url` will parse the query string for us.
    let url = reqwest::Url::parse("http://localhost")
        .and_then(|base| base.join(target))
        .wrap_err("the OAuth callback request had an unparsable target")?;

    let mut auth_code = None;
    let mut flow_token = None;
    let mut error = None;
    let mut error_description = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => auth_code = Some(value.into_owned()),
            "state" => flow_token = Some(value.into_owned()),
            "error" => error = Some(value.into_owned()),
            "error_description" => error_description = Some(value.into_owned()),
            _ => {}
        }
    }

    if let Some(error) = error {
        let detail = error_description
            .map(|d| format!(": {d}"))
            .unwrap_or_default();
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
        assert_eq!(
            secret_hint("antithesis_api_key_v2_NOTREAL_9Pgw"),
            "********9Pgw"
        );
        assert_eq!(
            secret_hint("antithesis_api_key_v2_SHORTER_7Qxz"),
            "********7Qxz"
        );
    }

    #[test]
    fn secret_hint_never_shows_more_than_it_has() {
        assert_eq!(secret_hint("ab"), "********ab");
        assert_eq!(secret_hint(""), "********");
    }

    #[test]
    fn empty_answer_keeps_the_stored_secret() {
        assert_eq!(
            keep_stored_if_empty(String::new(), Some("stored")),
            "stored"
        );
    }

    #[test]
    fn a_typed_answer_replaces_the_stored_secret() {
        assert_eq!(
            keep_stored_if_empty("typed".to_owned(), Some("stored")),
            "typed"
        );
    }

    #[test]
    fn empty_answer_stands_when_nothing_is_stored() {
        assert_eq!(keep_stored_if_empty(String::new(), None), "");
    }

    #[test]
    fn stored_api_key_reads_a_key_out_of_storage() {
        let stored = Some(AttributedValue::SettingsFile {
            value: AuthenticationInfo::ApiKey {
                api_key: "antithesis_api_key_v2_NOTREAL_9Pgw".to_owned(),
            },
            settings_file_path: Path::new("/tmp/credentials.toml").to_path_buf(),
            profile: None,
        });
        assert_eq!(
            stored_api_key(stored.as_ref()),
            Some("antithesis_api_key_v2_NOTREAL_9Pgw")
        );
    }

    #[test]
    fn stored_api_key_ignores_a_key_from_the_environment() {
        // Keeping it would copy an ambient secret into the credentials file.
        let ambient = Some(AttributedValue::EnvironmentVariable {
            value: AuthenticationInfo::ApiKey {
                api_key: "antithesis_api_key_v2_NOTREAL_9Pgw".to_owned(),
            },
            environment_variable_names: vec!["ANTITHESIS_API_KEY"],
        });
        assert_eq!(stored_api_key(ambient.as_ref()), None);
    }

    #[test]
    fn stored_api_key_ignores_credentials_of_another_kind() {
        let password = Some(AttributedValue::SettingsFile {
            value: AuthenticationInfo::Password {
                username: "user".to_owned(),
                password: "FAKE-not-a-real-password".to_owned(),
            },
            settings_file_path: Path::new("/tmp/credentials.toml").to_path_buf(),
            profile: None,
        });
        assert_eq!(stored_api_key(password.as_ref()), None);
        assert_eq!(stored_api_key(None), None);
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
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        assert_eq!(
            code_challenge_for(verifier),
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
        let params = parse_callback_params("/callback?code=AUTH_CODE&state=FLOW_TOKEN").unwrap();
        assert_eq!(
            params,
            CallbackParams {
                auth_code: "AUTH_CODE".to_owned(),
                flow_token: "FLOW_TOKEN".to_owned(),
            }
        );
    }

    #[test]
    fn parse_callback_url_decodes_values() {
        let params = parse_callback_params("/callback?code=a%2Fb%2Bc&state=x%3Dy").unwrap();
        assert_eq!(params.auth_code, "a/b+c");
        assert_eq!(params.flow_token, "x=y");
    }

    #[test]
    fn parse_callback_surfaces_idp_error() {
        let err = parse_callback_params(
            "/callback?error=access_denied&error_description=user%20said%20no",
        )
        .unwrap_err();
        let rendered = format!("{err}");
        assert!(rendered.contains("access_denied"), "got: {rendered}");
        assert!(rendered.contains("user said no"), "got: {rendered}");
    }

    #[test]
    fn parse_callback_requires_code_and_state() {
        assert!(parse_callback_params("/callback?state=only_state").is_err());
        assert!(parse_callback_params("/callback?code=only_code").is_err());
    }

    #[tokio::test]
    async fn callback_read_is_interruptible_when_peer_connects_then_stalls() {
        use tokio::net::TcpStream;

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        // Connect but never send a request line, and hold the connection open so
        // the server-side read blocks on data that never arrives (no EOF).
        let _client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        let listeners = vec![listener];
        let outcome = tokio::time::timeout(
            Duration::from_millis(250),
            receive_callback_request(&listeners),
        )
        .await;

        assert!(
            outcome.is_err(),
            "a connected-but-silent peer must trip the timeout, not block indefinitely"
        );
    }

    // --- Integration tests -----------------------------------------------------------

    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::thread;

    use crate::settings::Settings;
    use color_eyre::eyre::Result;

    enum Answer {
        Input(String),
        Select(Option<usize>),
        Password(String),
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
        fn password(mut self, value: &str) -> Self {
            self.0.push(Answer::Password(value.to_owned()));
            self
        }
        fn build(self) -> ScriptedPrompter {
            ScriptedPrompter {
                answers: RefCell::new(self.0.into()),
                prompts: RefCell::new(Vec::new()),
                selects: RefCell::new(Vec::new()),
                password_hints: RefCell::new(Vec::new()),
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
        /// The hint shown by each `password` call, in order.
        password_hints: RefCell<Vec<Option<String>>>,
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

        /// The hint shown by each `password` prompt, in order.
        fn password_hints(&self) -> Vec<Option<String>> {
            self.password_hints.borrow().clone()
        }
    }

    impl Prompter for ScriptedPrompter {
        fn is_interactive(&self) -> bool {
            true
        }

        fn input(&self, prompt: &str, _default: Option<&str>) -> Result<String> {
            match self.next("input", prompt) {
                Answer::Input(value) => Ok(value),
                _ => panic!("next scripted answer was not an input at {prompt:?}"),
            }
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

        fn password(&self, prompt: &str, hint: Option<&str>) -> Result<String> {
            self.password_hints
                .borrow_mut()
                .push(hint.map(str::to_owned));
            match self.next("password", prompt) {
                Answer::Password(value) => Ok(value),
                _ => panic!("next scripted answer was not a password at {prompt:?}"),
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

    /// Credential-kind menu labels, matching the `Display` impl on `AuthSetupType`.
    const API_KEY: &str = "API Key";
    const USERNAME_PASSWORD: &str = "Username & password (deprecated)";
    const OAUTH: &str = "Single sign-on (OAuth)";

    /// A per-test isolated environment: an exclusive env lock, a throwaway `$HOME`,
    /// and an in-process mock backend for the OAuth-config probe.
    struct LoginEnv {
        home: tempfile::TempDir,
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
            let guard = ENV_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());
            let home = tempfile::TempDir::new().expect("create temp HOME");
            let base_url = spawn_mock_server(200, config_body);

            // SAFETY: the `ENV_LOCK` guard we hold serializes every test in this
            // binary that touches process-global env, so nothing else reads or
            // writes these vars while the guard is alive. They are (re)set from
            // scratch for each test, so no state leaks between tests.
            unsafe {
                std::env::set_var("HOME", home.path());
                std::env::set_var("ANTITHESIS_BASE_URL", &base_url);
                // A stray value for any of these in the developer's shell would leak
                // into settings resolution; clear them for a clean baseline. The
                // credential variables come from `auth::CREDENTIAL_ENV_VARS` so this
                // list cannot drift from what ambient resolution actually reads.
                for key in [
                    "XDG_CONFIG_HOME",
                    "ANTITHESIS_PROFILE",
                    "ANTITHESIS_TENANT",
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
                _guard: guard,
            }
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
        fn resolve_settings(&self, profile: Option<&str>) -> Result<Settings> {
            Settings::resolve(None, profile.map(str::to_owned))
        }
    }

    /// Start a TCP server that answers every request with `status` + JSON `body`,
    /// returning its base URL. The listener thread is intentionally leaked — it
    /// lives for the test process, which is fine for a test.
    fn spawn_mock_server(status: u16, body: &'static str) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock server addr");
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        format!("http://{addr}")
    }

    /// A fresh login with no flags prompts for tenant, repository, and credential
    /// kind, then persists an API key.
    #[tokio::test]
    async fn login_collects_and_persists_an_api_key() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0) // API Key
            .password("sk-test-key")
            .build();

        do_cmd_login(None, None, None, &settings, &prompter).await?;

        let stored = env.settings();
        assert!(stored.contains(r#"tenant = "mytenant""#), "{stored}");
        assert!(stored.contains(r#"repository = "myrepo""#), "{stored}");
        let creds = env.credentials();
        assert!(creds.contains(r#"type = "ApiKey""#), "{creds}");
        assert!(creds.contains(r#"api_key = "sk-test-key""#), "{creds}");
        Ok(())
    }

    /// With `--tenant` and `--repository` supplied, only the credential prompt is
    /// shown — the tenant/repository prompts are skipped.
    #[tokio::test]
    async fn login_flags_skip_the_tenant_and_repository_prompts() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default().select(0).password("sk-test-key").build();

        do_cmd_login(
            Some("mytenant".to_owned()),
            Some("myrepo".to_owned()),
            None,
            &settings,
            &prompter,
        )
        .await?;

        let prompts = prompter.prompts();
        assert!(
            !prompts
                .iter()
                .any(|p| p.contains("What Antithesis tenant would you like to use")),
            "tenant should not be prompted when --tenant is given: {prompts:?}"
        );
        assert!(
            !prompts
                .iter()
                .any(|p| p.contains("What container repository would you like to use")),
            "repository should not be prompted when --repository is given: {prompts:?}"
        );
        assert!(env.settings().contains(r#"tenant = "mytenant""#));
        Ok(())
    }

    /// Selecting "Username & password" collects a username and a password and
    /// persists them.
    #[tokio::test]
    async fn login_collects_a_username_and_password() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("ptenant")
            .input("prepo")
            .select(1) // Username & password
            .input("puser")
            .password("ppass")
            .build();

        do_cmd_login(None, None, None, &settings, &prompter).await?;

        let creds = env.credentials();
        assert!(creds.contains(r#"type = "Password""#), "{creds}");
        assert!(creds.contains(r#"username = "puser""#), "{creds}");
        assert!(creds.contains(r#"password = "ppass""#), "{creds}");
        Ok(())
    }

    /// The global `--profile` flag scopes the persisted login.
    #[tokio::test]
    async fn login_scopes_to_a_named_profile() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(Some("prod"))?;
        let prompter = Script::default()
            .input("ptenant")
            .input("prepo")
            .select(0)
            .password("pk-secret")
            .build();

        do_cmd_login(None, None, Some("prod"), &settings, &prompter).await?;

        let stored = env.settings();
        assert!(stored.contains("[profile.prod]"), "{stored}");
        Ok(())
    }

    /// A tenant that isn't a valid hostname is rejected after the prompt, before
    /// the repository/credential prompts and before anything is persisted.
    #[tokio::test]
    async fn login_rejects_an_invalid_tenant() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("underscores_are_not_allowed")
            .build();

        let result = do_cmd_login(None, None, None, &settings, &prompter).await;

        let err = result.expect_err("an invalid tenant must fail the login");
        assert!(
            format!("{err:#}").contains("a tenant must be a valid hostname"),
            "unexpected error: {err:#}"
        );
        // Only the tenant was prompted; the flow bailed before repository/credentials.
        assert_eq!(prompter.prompts().len(), 1, "{:?}", prompter.prompts());
        assert!(env.settings().is_empty(), "nothing should be persisted");
        Ok(())
    }

    /// When the backend reports OAuth is disabled for the CLI, the "Single sign-on
    /// (OAuth)" option is not offered in the credential menu.
    #[tokio::test]
    async fn login_hides_oauth_when_backend_disables_it() -> Result<()> {
        let env = LoginEnv::with_oauth_config(OAUTH_DISABLED);
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0)
            .password("sk-test-key")
            .build();

        do_cmd_login(None, None, None, &settings, &prompter).await?;

        let (menu, default) = &prompter.selects()[0];
        assert_eq!(
            menu,
            &[API_KEY.to_owned(), USERNAME_PASSWORD.to_owned()],
            "OAuth must be hidden when the backend disables it"
        );
        assert_eq!(
            *default, 0,
            "the first option must be highlighted when no credentials are stored"
        );
        Ok(())
    }

    /// Conversely, when the backend advertises an OAuth port strategy, the "Single
    /// sign-on (OAuth)" option *is* offered — first in the menu and as the
    /// default, with the deprecated username/password option last.
    #[tokio::test]
    async fn login_offers_oauth_first_when_backend_supports_it() -> Result<()> {
        let env = LoginEnv::with_oauth_config(OAUTH_EPHEMERAL);
        let settings = env.resolve_settings(None)?;
        // Finish via the API-key option rather than OAuth, which would start
        // the interactive browser flow.
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(1)
            .password("sk-test-key")
            .build();

        do_cmd_login(None, None, None, &settings, &prompter).await?;

        let (menu, default) = &prompter.selects()[0];
        assert_eq!(
            menu,
            &[
                OAUTH.to_owned(),
                API_KEY.to_owned(),
                USERNAME_PASSWORD.to_owned()
            ],
            "OAuth must lead the menu when the backend advertises a port strategy"
        );
        assert_eq!(*default, 0, "OAuth must be the default for a first login");
        Ok(())
    }

    /// Stored username/password credentials move the default to that (last)
    /// menu entry, so "log in again the same way" stays one keystroke.
    #[tokio::test]
    async fn login_defaults_to_the_previously_used_kind() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(
            ".config/snouty/credentials.toml",
            "[default]\ntype = \"Password\"\nusername = \"puser\"\npassword = \"ppass\"\n",
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0)
            .password("sk-test-key")
            .build();

        do_cmd_login(None, None, None, &settings, &prompter).await?;

        let (menu, default) = &prompter.selects()[0];
        let password_index = menu
            .iter()
            .position(|label| label == USERNAME_PASSWORD)
            .expect("menu must offer username/password");
        assert_eq!(password_index, menu.len() - 1, "password must come last");
        assert_eq!(*default, password_index);
        Ok(())
    }

    /// A stored API key is offered back: the prompt shows a masked hint, and an
    /// empty answer keeps the stored key rather than saving an empty one.
    #[tokio::test]
    async fn login_keeps_the_stored_api_key_on_an_empty_answer() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(
            ".config/snouty/credentials.toml",
            "[default]\ntype = \"ApiKey\"\napi_key = \"antithesis_api_key_v2_STORED_9Pgw\"\n",
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0) // API Key, the stored kind, is already the default
            .password("") // hit enter
            .build();

        do_cmd_login(None, None, None, &settings, &prompter).await?;

        assert_eq!(
            prompter.password_hints(),
            vec![Some("********9Pgw".to_owned())],
            "the key prompt must show that a stored key is there to keep"
        );
        let creds = env.credentials();
        assert!(
            creds.contains(r#"api_key = "antithesis_api_key_v2_STORED_9Pgw""#),
            "{creds}"
        );
        Ok(())
    }

    /// A key typed at the prompt replaces the stored one.
    #[tokio::test]
    async fn login_replaces_the_stored_api_key_when_one_is_typed() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(
            ".config/snouty/credentials.toml",
            "[default]\ntype = \"ApiKey\"\napi_key = \"antithesis_api_key_v2_STORED_9Pgw\"\n",
        );
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0)
            .password("antithesis_api_key_v2_NEW_7Qxz")
            .build();

        do_cmd_login(None, None, None, &settings, &prompter).await?;

        let creds = env.credentials();
        assert!(
            creds.contains(r#"api_key = "antithesis_api_key_v2_NEW_7Qxz""#),
            "{creds}"
        );
        Ok(())
    }

    /// An API key in the environment is not offered back, because keeping it
    /// would copy an ambient secret into the credentials file.
    #[tokio::test]
    async fn login_does_not_offer_back_an_api_key_from_the_environment() -> Result<()> {
        let env = LoginEnv::new();
        // SAFETY: `LoginEnv` holds `ENV_LOCK` for this test's whole body, so no
        // other test reads or writes process env while this var is set.
        unsafe { std::env::set_var("ANTITHESIS_API_KEY", "antithesis_api_key_v2_AMBIENT_5Kfn") };
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0)
            .password("sk-typed-key")
            .build();

        let result = do_cmd_login(None, None, None, &settings, &prompter).await;
        // SAFETY: as above.
        unsafe { std::env::remove_var("ANTITHESIS_API_KEY") };
        result?;

        assert_eq!(
            prompter.password_hints(),
            vec![None],
            "an ambient key must not be offered as something to keep"
        );
        Ok(())
    }

    /// Esc at the credential menu skips credential storage: the tenant and
    /// repository are still saved, and no credentials file is written.
    #[tokio::test]
    async fn login_skips_credential_storage_on_esc() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None)?;
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .skip_select()
            .build();

        do_cmd_login(None, None, None, &settings, &prompter).await?;

        assert!(env.settings().contains(r#"tenant = "mytenant""#));
        assert!(
            !env.config_dir().join("credentials.toml").exists(),
            "Esc at the menu must write no credentials file"
        );
        Ok(())
    }

    // --- Real macOS Keychain -------------------------------------------------
    /// Run `/usr/bin/security` under `home` so its keychain preferences (default
    /// keychain, search list) land in the same `$HOME/Library/Preferences`
    /// snouty reads back — see [`login_persists_to_real_macos_keychain`].
    fn security(home: &Path, args: &[&str]) -> std::process::Output {
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
        let password = "snouty-test-keychain";

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
            .input("registry.example.com/acme/app")
            .select(0) // API Key
            .password("sk-KEYCHAIN-TEST")
            .build();
        do_cmd_login(None, None, None, &settings, &prompter).await?;

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

        // --- Read-back: resolving ambient credentials returns the stored key
        // straight from the keychain, proving the secret round-trips. Reading in
        // the same process that wrote it doesn't trip the ACL prompt the
        // `security` CLI would. ---
        let resolved = AuthenticationInfo::for_ambient_configuration_with_attribution(
            None,
            PasswordPolicy::Inspect,
        )?;
        match &resolved {
            AttributedValue::Keychain {
                value: AuthenticationInfo::ApiKey { api_key },
                ..
            } => assert_eq!(api_key.as_str(), "sk-KEYCHAIN-TEST"),
            _ => panic!("expected the API key to resolve from the keychain"),
        }

        // --- Profile scoping uses a distinct keychain entry (`profile_<name>`). ---
        let prof_settings = env.resolve_settings(Some("prod"))?;
        let prof_prompter = Script::default()
            .input("acme")
            .input("registry.example.com/acme/app")
            .select(0)
            .password("sk-PROD-KEY")
            .build();
        do_cmd_login(None, None, Some("prod"), &prof_settings, &prof_prompter).await?;
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
