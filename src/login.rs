use std::io::{self, IsTerminal};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use color_eyre::Section;
use color_eyre::eyre::{Context, Result, eyre};
use dialoguer::{Confirm, Input, Password, Select};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::settings;
use crate::{
    attributed_value::AttributedValue,
    auth::{AuthenticationInfo, PersistableCredentials, persist},
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

/// Wrapping our TUI (`dialoguer`) in trait so that it can be subbed out for testing
trait Prompter {
    /// Whether we're attached to an interactive terminal.
    fn is_interactive(&self) -> bool;

    /// A yes/no confirmation with no default (the user must answer).
    fn confirm(&self, prompt: &str) -> Result<bool>;

    /// A free-text line, pre-filled with `default` when the user just hits enter.
    fn input(&self, prompt: &str, default: Option<&str>) -> Result<String>;

    /// A single-choice menu over `items`, initially highlighting `default`.
    /// Returns `None` when the user cancels (Esc/q), mirroring
    /// [`dialoguer::Select::interact_opt`].
    fn select(
        &self,
        prompt: &str,
        items: &[String],
        default: Option<usize>,
    ) -> Result<Option<usize>>;

    /// A no-echo secret. `allow_empty` permits an empty value; when
    /// `confirm_prompt` is `Some`, the value must be entered twice and matched.
    fn password(&self, prompt: &str, confirm_prompt: Option<&str>) -> Result<String>;
}

/// The production [`Prompter`]: `dialoguer` widgets reading the real terminal.
struct DialoguerPrompter;

impl Prompter for DialoguerPrompter {
    fn is_interactive(&self) -> bool {
        io::stdin().is_terminal()
    }

    fn confirm(&self, prompt: &str) -> Result<bool> {
        Ok(Confirm::new().with_prompt(prompt).interact()?)
    }

    fn input(&self, prompt: &str, default: Option<&str>) -> Result<String> {
        // T is inferred as `String` from the `String` return type — matching the
        // form dialoguer's own examples use.
        let mut input = Input::new().with_prompt(prompt);
        if let Some(default) = default {
            input = input.default(default.to_owned());
        }
        Ok(input.interact_text()?)
    }

    fn select(
        &self,
        prompt: &str,
        items: &[String],
        default: Option<usize>,
    ) -> Result<Option<usize>> {
        let mut select = Select::new().with_prompt(prompt).items(items);
        if let Some(default) = default {
            select = select.default(default);
        }
        Ok(select.interact_opt()?)
    }

    fn password(&self, prompt: &str, confirm_prompt: Option<&str>) -> Result<String> {
        let mut password = Password::new().with_prompt(prompt);
        if let Some(confirm_prompt) = confirm_prompt {
            password = password.with_confirmation(confirm_prompt, "Passwords did not match");
        }
        Ok(password.interact()?)
    }
}

pub async fn cmd_login(
    tenant: Option<String>,
    repository: Option<String>,
    profile: Option<&str>,
    current_settings: Result<Settings>,
) -> Result<()> {
    do_cmd_login(
        tenant,
        repository,
        profile,
        current_settings,
        &DialoguerPrompter,
    )
    .await
}

async fn do_cmd_login(
    tenant: Option<String>,
    repository: Option<String>,
    profile: Option<&str>,
    current_settings: Result<Settings>,
    prompter: &dyn Prompter,
) -> Result<()> {
    if let Err(report) = &current_settings {
        eprintln!("The current settings failed to load with the following error: {report:#}");
        if prompter.is_interactive()
            && !prompter.confirm(
                "Would you like to proceed with the login command? Doing so may cause your existing settings file to be replaced rather than updated.",
            )?
        {
            return Err(eyre!(
                "Exiting login command without completing per user request."
            ));
        }
    }

    let profile_to_use = profile
        .map(|p| p.to_owned())
        .or_else(|| env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME).ok().flatten());

    let tenant_to_use = match tenant {
        Some(arg_value) if !arg_value.is_empty() => arg_value,
        Some(_) | None => prompt_for_value(
            prompter,
            "Antithesis tenant",
            current_settings.as_ref().ok().and_then(|s| s.tenant()),
        )?,
    };
    validate_tenant_host(&tenant_to_use)?;

    let repository_to_use = match repository {
        Some(arg_value) if !arg_value.is_empty() => arg_value,
        Some(_) | None => prompt_for_value(
            prompter,
            "container repository",
            current_settings.as_ref().ok().and_then(|s| s.repository()),
        )?,
    };

    let current_credentials = AuthenticationInfo::for_ambient_configuration_with_attribution(
        profile_to_use.as_deref(),
        true,
    );

    // Capture the credential kind and where it was stored so the summary can name
    // both; `None` when the user chose to skip credential setup.
    let credential_summary = if prompter.is_interactive() {
        match prompt_for_auth(prompter, &tenant_to_use, &current_credentials).await? {
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
        eprintln!(
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
    previous_credentials: Result<AttributedValue<AuthenticationInfo>>,
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
            Ok(AttributedValue::Keychain { .. }) => {
                println!(
                    "Retained your previously stored credentials{scope} in the system keychain."
                );
            }
            Ok(AttributedValue::SettingsFile {
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
    if prompter.is_interactive() {
        prompter.input(
            &format!("What {value_name} would you like to use?"),
            previous_value,
        )
    } else {
        Err(eyre!("Cannot prompt for value when not running in a TTY"))
    }
}

enum AuthSetupType {
    ApiKey,
    Password,
    OAuth,
}

impl std::fmt::Display for AuthSetupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthSetupType::ApiKey => f.write_str("API Key"),
            AuthSetupType::Password => f.write_str("Username & password"),
            AuthSetupType::OAuth => f.write_str("Single sign-on (OAuth)"),
        }
    }
}

async fn prompt_for_auth(
    prompter: &dyn Prompter,
    tenant: &str,
    previous_value: &Result<AttributedValue<AuthenticationInfo>>,
) -> Result<Option<PersistableCredentials>> {
    // ANTITHESIS_BASE_URL trumps the supplied tenant because the former is used by spec tests
    let base_url = env::var(settings::ANTITHESIS_BASE_URL_VAR_NAME)?
        .unwrap_or_else(|| format!("https://{tenant}.antithesis.com"));
    let client = reqwest::Client::builder()
        .timeout(OAUTH_HTTP_TIMEOUT)
        .build()
        .wrap_err("failed to build the OAuth HTTP client")?;
    let oauth_config = fetch_cli_config(&client, &base_url).await;

    let mut credential_options = vec![AuthSetupType::ApiKey, AuthSetupType::Password];

    if oauth_config
        .as_ref()
        .is_ok_and(|config| !matches!(config, CliOAuthConfig::Disabled))
    {
        credential_options.push(AuthSetupType::OAuth);
    }

    let labels: Vec<String> = credential_options.iter().map(ToString::to_string).collect();

    // Default the highlighted option to whatever kind was last used, so the
    // common "log in again the same way" case is one keystroke.
    let default = match previous_value {
        Err(_) => None,
        Ok(creds) => match creds {
            AttributedValue::EnvironmentVariable { .. } => None,
            _ => match creds.value() {
                AuthenticationInfo::ApiKey { .. } => Some(0),
                AuthenticationInfo::Password { .. } => Some(1),
                AuthenticationInfo::OAuth { .. } if credential_options.len() >= 3 => Some(2),
                _ => None,
            },
        },
    };

    let selection = prompter.select(
        "What kind of credentials would you like to use? (Hit Esc to skip)",
        &labels,
        default,
    )?;

    match selection {
        None => Ok(None),
        Some(index) => match credential_options[index] {
            AuthSetupType::ApiKey => prompt_for_api_key(prompter).map(Some),
            AuthSetupType::Password => match previous_value.as_ref().map(AttributedValue::value) {
                Ok(AuthenticationInfo::Password { username, .. }) => {
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

fn prompt_for_api_key(prompter: &dyn Prompter) -> Result<PersistableCredentials> {
    Ok(PersistableCredentials::ApiKey {
        api_key: prompter.password("Please enter your API Key", None)?,
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
    let password = prompter.password(
        "Please enter your password",
        Some("Please reenter your password to confirm"),
    )?;

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

    println!("\nTo finish signing in, open the following URL in your browser:\n\n  {location}\n");
    open_in_browser(&location)?;
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

fn validate_browser_launch_url(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .filter(|parsed| matches!(parsed.scheme(), "http" | "https"))
        .map(|parsed| parsed.as_str().to_owned())
}

/// Best-effort open of `url` in the user's default browser. Failures are
/// intentionally ignored — the URL is always also printed, so a headless or
/// opener-less environment can still complete the flow by hand.
fn open_in_browser(url: &str) -> Result<()> {
    let Some(url) = validate_browser_launch_url(url) else {
        return Err(eyre!(
            "The supplied login URL is not a valid HTTP(S) URL: {url}"
        ));
    };
    let url = url.as_str();

    #[cfg(target_os = "macos")]
    let mut command = {
        let mut c = Command::new("open");
        c.arg(url);
        c
    };
    // If Snouty ever supports Windows, another declaration of `command` will be needed here.
    // Without it, Snouty will fail to compile on or for Windows
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut c = Command::new("xdg-open");
        c.arg(url);
        c
    };

    let _ = command.stdout(Stdio::null()).stderr(Stdio::null()).spawn();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn browser_launch_url_accepts_only_http_and_https() {
        // Real authorization URLs (http/https) pass through.
        assert!(validate_browser_launch_url("https://idp.example.com/authorize?a=1&b=2").is_some());
        assert!(validate_browser_launch_url("http://localhost:12345/callback").is_some());

        // Anything else is dropped rather than handed to an OS launcher.
        assert_eq!(validate_browser_launch_url("file:///etc/passwd"), None);
        assert_eq!(validate_browser_launch_url("javascript:alert(1)"), None);
        assert_eq!(validate_browser_launch_url("ftp://example.com/x"), None);
        assert_eq!(validate_browser_launch_url("not a url"), None);
        assert_eq!(validate_browser_launch_url(""), None);
    }

    #[test]
    fn browser_launch_url_preserves_ampersand_query_params() {
        // The `&`-separated params (what the unquoted Windows shell mangled)
        // survive normalization intact.
        let normalized = validate_browser_launch_url(
            "https://idp/authorize?response_type=code&client_id=x&state=y",
        )
        .expect("valid https URL");
        assert!(
            normalized.contains("response_type=code"),
            "got: {normalized}"
        );
        assert!(normalized.contains("&client_id=x"), "got: {normalized}");
        assert!(normalized.contains("&state=y"), "got: {normalized}");
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
        Confirm(bool),
        Input(String),
        Select(Option<usize>),
        Password(String),
    }

    /// Fluent builder for a script of [`Answer`]s, in the order the flow will ask.
    #[derive(Default)]
    struct Script(Vec<Answer>);

    impl Script {
        fn confirm(mut self, value: bool) -> Self {
            self.0.push(Answer::Confirm(value));
            self
        }
        fn input(mut self, value: &str) -> Self {
            self.0.push(Answer::Input(value.to_owned()));
            self
        }
        fn select(mut self, index: usize) -> Self {
            self.0.push(Answer::Select(Some(index)));
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
                menus: RefCell::new(Vec::new()),
            }
        }
    }

    /// A [`Prompter`] that returns pre-programmed answers and records what it was
    /// asked, so tests can assert on prompt order and menu contents.
    struct ScriptedPrompter {
        answers: RefCell<VecDeque<Answer>>,
        prompts: RefCell<Vec<String>>,
        menus: RefCell<Vec<Vec<String>>>,
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

        /// The item lists passed to each `select`, in order.
        fn menus(&self) -> Vec<Vec<String>> {
            self.menus.borrow().clone()
        }
    }

    impl Prompter for ScriptedPrompter {
        fn is_interactive(&self) -> bool {
            true
        }

        fn confirm(&self, prompt: &str) -> Result<bool> {
            match self.next("confirm", prompt) {
                Answer::Confirm(value) => Ok(value),
                _ => panic!("next scripted answer was not a confirm at {prompt:?}"),
            }
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
            items: &[String],
            _default: Option<usize>,
        ) -> Result<Option<usize>> {
            self.menus.borrow_mut().push(items.to_vec());
            match self.next("select", prompt) {
                Answer::Select(value) => Ok(value),
                _ => panic!("next scripted answer was not a select at {prompt:?}"),
            }
        }

        fn password(&self, prompt: &str, _confirm_prompt: Option<&str>) -> Result<String> {
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
    const USERNAME_PASSWORD: &str = "Username & password";
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
                // into settings resolution; clear them for a clean baseline.
                for key in [
                    "XDG_CONFIG_HOME",
                    "ANTITHESIS_PROFILE",
                    "ANTITHESIS_TENANT",
                    "ANTITHESIS_REPOSITORY",
                    "ANTITHESIS_HTTPS_PROXY",
                    "CONTAINER_ENGINE",
                    "SNOUTY_SETTINGS_PATH",
                ] {
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
        let settings = env.resolve_settings(None);
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0) // API Key
            .password("sk-test-key")
            .build();

        do_cmd_login(None, None, None, settings, &prompter).await?;

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
        let settings = env.resolve_settings(None);
        let prompter = Script::default().select(0).password("sk-test-key").build();

        do_cmd_login(
            Some("mytenant".to_owned()),
            Some("myrepo".to_owned()),
            None,
            settings,
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
        let settings = env.resolve_settings(None);
        let prompter = Script::default()
            .input("ptenant")
            .input("prepo")
            .select(1) // Username & password
            .input("puser")
            .password("ppass")
            .build();

        do_cmd_login(None, None, None, settings, &prompter).await?;

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
        let settings = env.resolve_settings(Some("prod"));
        let prompter = Script::default()
            .input("ptenant")
            .input("prepo")
            .select(0)
            .password("pk-secret")
            .build();

        do_cmd_login(None, None, Some("prod"), settings, &prompter).await?;

        let stored = env.settings();
        assert!(stored.contains("[profile.prod]"), "{stored}");
        Ok(())
    }

    /// A tenant that isn't a valid hostname is rejected after the prompt, before
    /// the repository/credential prompts and before anything is persisted.
    #[tokio::test]
    async fn login_rejects_an_invalid_tenant() -> Result<()> {
        let env = LoginEnv::new();
        let settings = env.resolve_settings(None);
        let prompter = Script::default()
            .input("underscores_are_not_allowed")
            .build();

        let result = do_cmd_login(None, None, None, settings, &prompter).await;

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

    /// When settings resolution fails (an unparsable settings file), login first
    /// asks the user to confirm before proceeding; answering yes repairs it.
    #[tokio::test]
    async fn login_confirms_before_proceeding_past_broken_settings() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(".config/snouty/settings.toml", "this is = = not valid toml");
        // Resolution now fails, exactly as it would for the real command.
        let settings = env.resolve_settings(None);
        assert!(settings.is_err(), "seeded settings should fail to parse");

        let prompter = Script::default()
            .confirm(true)
            .input("ptenant")
            .input("prepo")
            .select(0)
            .password("sk-test-key")
            .build();

        do_cmd_login(None, None, None, settings, &prompter).await?;

        let first_prompt = &prompter.prompts()[0];
        assert!(
            first_prompt.contains("Would you like to proceed with the login command"),
            "the confirmation must come first: {first_prompt:?}"
        );
        assert!(
            env.settings().contains(r#"tenant = "ptenant""#),
            "{}",
            env.settings()
        );
        Ok(())
    }

    /// Answering "no" at the broken-settings confirmation aborts the login without
    /// persisting anything.
    #[tokio::test]
    async fn login_aborts_when_user_declines_broken_settings() -> Result<()> {
        let env = LoginEnv::new();
        env.seed(".config/snouty/settings.toml", "this is = = not valid toml");
        let settings = env.resolve_settings(None);

        let prompter = Script::default().confirm(false).build();

        let result = do_cmd_login(None, None, None, settings, &prompter).await;

        assert!(
            result.is_err(),
            "declining the confirmation must abort login"
        );
        // Nothing beyond the confirmation was asked.
        assert_eq!(prompter.prompts().len(), 1, "{:?}", prompter.prompts());
        Ok(())
    }

    /// When the backend reports OAuth is disabled for the CLI, the "Single sign-on
    /// (OAuth)" option is not offered in the credential menu.
    #[tokio::test]
    async fn login_hides_oauth_when_backend_disables_it() -> Result<()> {
        let env = LoginEnv::with_oauth_config(OAUTH_DISABLED);
        let settings = env.resolve_settings(None);
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0)
            .password("sk-test-key")
            .build();

        do_cmd_login(None, None, None, settings, &prompter).await?;

        let menu = &prompter.menus()[0];
        assert_eq!(
            menu,
            &[API_KEY.to_owned(), USERNAME_PASSWORD.to_owned()],
            "OAuth must be hidden when the backend disables it"
        );
        Ok(())
    }

    /// Conversely, when the backend advertises an OAuth port strategy, the "Single
    /// sign-on (OAuth)" option *is* offered.
    #[tokio::test]
    async fn login_offers_oauth_when_backend_supports_it() -> Result<()> {
        let env = LoginEnv::with_oauth_config(OAUTH_EPHEMERAL);
        let settings = env.resolve_settings(None);
        // Finish via the default API-key option rather than OAuth, which would start
        // the interactive browser flow.
        let prompter = Script::default()
            .input("mytenant")
            .input("myrepo")
            .select(0)
            .password("sk-test-key")
            .build();

        do_cmd_login(None, None, None, settings, &prompter).await?;

        let menu = &prompter.menus()[0];
        assert!(
            menu.contains(&OAUTH.to_owned()),
            "OAuth must be offered when the backend advertises a port strategy: {menu:?}"
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
        let settings = env.resolve_settings(None);
        let prompter = Script::default()
            .input("acme")
            .input("registry.example.com/acme/app")
            .select(0) // API Key
            .password("sk-KEYCHAIN-TEST")
            .build();
        do_cmd_login(None, None, None, settings, &prompter).await?;

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
        let resolved = AuthenticationInfo::for_ambient_configuration_with_attribution(None, true)?;
        match &resolved {
            AttributedValue::Keychain {
                value: AuthenticationInfo::ApiKey { api_key },
                ..
            } => assert_eq!(api_key.as_str(), "sk-KEYCHAIN-TEST"),
            _ => panic!("expected the API key to resolve from the keychain"),
        }

        // --- Profile scoping uses a distinct keychain entry (`profile_<name>`). ---
        let prof_settings = env.resolve_settings(Some("prod"));
        let prof_prompter = Script::default()
            .input("acme")
            .input("registry.example.com/acme/app")
            .select(0)
            .password("sk-PROD-KEY")
            .build();
        do_cmd_login(None, None, Some("prod"), prof_settings, &prof_prompter).await?;
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
