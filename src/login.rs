use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use color_eyre::Section;
use color_eyre::eyre::{Context, Result, eyre};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::{
    attributed_value::AttributedValue,
    auth::{AuthenticationInfo, CredentialStorage, PersistableCredentials, persist},
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

pub async fn cmd_login(
    tenant: Option<String>,
    repository: Option<String>,
    profile: Option<&str>,
    current_settings: Result<Settings>,
) -> Result<()> {
    if let Err(report) = &current_settings {
        eprintln!("The current settings failed to load with the following error: {report:#}");
        eprintln!(
            "Would you like to proceed with the login command? Doing so may cause your existing settings file to be replaced rather than updated."
        );
        eprintln!("1. Yes, please proceed");
        eprintln!("2. No, please exit immediately");
        eprintln!(
            "Please enter either '1' or '2'. Any other input will cause the program to exit."
        );

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        match input.trim() {
            "1" => {}
            _ => {
                return Err(eyre!(
                    "Exiting login command without completing per user request."
                ));
            }
        }
    }

    let profile_to_use = profile
        .map(|p| p.to_owned())
        .or_else(|| env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME).ok().flatten());

    let tenant_to_use = match tenant {
        Some(arg_value) if !arg_value.is_empty() => arg_value,
        Some(_) | None => prompt_for_value(
            "Antithesis tenant",
            current_settings.as_ref().ok().and_then(|s| s.tenant()),
        )?,
    };
    validate_tenant_host(&tenant_to_use)?;

    let repository_to_use = match repository {
        Some(arg_value) if !arg_value.is_empty() => arg_value,
        Some(_) | None => prompt_for_value(
            "container repository",
            current_settings.as_ref().ok().and_then(|s| s.repository()),
        )?,
    };

    // Capture the credential kind and where it was stored so the summary can name
    // both; `None` when the user chose to skip credential setup.
    let credential_summary =
        match prompt_for_auth(profile_to_use.as_deref(), &tenant_to_use).await? {
            Some(credentials) => {
                let kind = match &credentials {
                    PersistableCredentials::ApiKey { .. } => "API key",
                    PersistableCredentials::Password { .. } => "username and password",
                    PersistableCredentials::OAuth { .. } => "OAuth",
                };
                Some((kind, persist(credentials, profile_to_use.as_deref())?))
            }
            None => None,
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
    credentials: Option<(&str, CredentialStorage)>,
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
        Some((kind, CredentialStorage::Keychain)) => {
            println!("Stored your {kind}{scope} in the system keychain.");
        }
        Some((kind, CredentialStorage::File(path))) => {
            println!("Stored your {kind}{scope} in {}.", path.display());
        }
        None => {
            println!(
                "Skipped credential storage — snouty will use the ANTITHESIS_API_KEY or ANTITHESIS_USERNAME/PASSWORD environment variables."
            );
        }
    }
    println!("Run `snouty doctor` to verify your setup.");
}

fn prompt_for_value(value_name: &str, previous_value: Option<&str>) -> Result<String> {
    println!("What {value_name} would you like to use?");
    if let Some(prev) = previous_value
        && !prev.is_empty()
    {
        println!("(Hit enter to use the previous value of [{prev}])");
    }

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    input = input.trim().to_owned();

    if input.is_empty()
        && let Some(prev) = previous_value
    {
        input.push_str(prev);
    }

    Ok(input)
}

enum AuthSetupType {
    Skip,
    ApiKey,
    Password,
    OAuth,
}

impl AuthSetupType {
    fn try_from_str(to_parse: &str) -> Option<Self> {
        match to_parse {
            "1" => Some(AuthSetupType::Skip),
            "2" => Some(AuthSetupType::ApiKey),
            "3" => Some(AuthSetupType::Password),
            "4" => Some(AuthSetupType::OAuth),
            _ => None,
        }
    }
}

async fn prompt_for_auth(
    profile: Option<&str>,
    tenant: &str,
) -> Result<Option<PersistableCredentials>> {
    let previous_value =
        AuthenticationInfo::for_ambient_configuration_with_attribution(profile, true);

    let default_selection = match &previous_value {
        Err(_) => '1',
        Ok(creds) => match creds {
            AttributedValue::EnvironmentVariable { .. } => '1',
            _ => match creds.value() {
                AuthenticationInfo::ApiKey { .. } => '2',
                AuthenticationInfo::Password { .. } => '3',
                _ => '1',
            },
        },
    };

    println!("What kind of credentials would you like to use?");
    println!(
        "1. Skip setup (Select this option if you wish to keep your current credentials or plan to use environment variables instead of persisted credentials.)"
    );
    println!("2. API key");
    println!("3. Username/password");
    println!("4. OAuth credentials");
    println!("(Hit enter to use the default value of [{default_selection}])");

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    input = input.trim().to_owned();

    if input.is_empty() {
        input.push(default_selection);
    }

    match AuthSetupType::try_from_str(&input) {
        None => Err(eyre!("Unrecognized input.")),
        Some(AuthSetupType::Skip) => Ok(None),
        Some(AuthSetupType::ApiKey) => match previous_value.map(|attr| attr.extract()) {
            Ok(AuthenticationInfo::ApiKey { api_key }) => prompt_for_api_key(Some(&api_key)),
            _ => prompt_for_api_key(None),
        }
        .map(Some),
        Some(AuthSetupType::Password) => match previous_value.map(|attr| attr.extract()) {
            Ok(AuthenticationInfo::Password { username, password }) => {
                prompt_for_username_password(Some(&username), Some(&password))
            }
            _ => prompt_for_username_password(None, None),
        }
        .map(Some),
        Some(AuthSetupType::OAuth) => complete_oauth_login(tenant).await.map(Some),
    }
}

fn prompt_for_api_key(previous_api_key: Option<&str>) -> Result<PersistableCredentials> {
    Ok(PersistableCredentials::ApiKey {
        api_key: prompt_for_sensitive_value("API key", previous_api_key)?,
    })
}

fn prompt_for_sensitive_value(value_name: &str, previous_value: Option<&str>) -> Result<String> {
    let prompt_str = match previous_value {
        Some(prev) if !prev.is_empty() => {
            format!("Please enter your {value_name} (leave blank to use previous value): ")
        }
        Some(_) | None => format!("Please enter your {value_name}: "),
    };

    let entered = read_secret(value_name, &prompt_str)?;
    if entered.is_empty() {
        match previous_value {
            Some(prev) if !prev.is_empty() => Ok(prev.to_owned()),
            Some(_) | None => Err(eyre!("{value_name} cannot be empty")),
        }
    } else {
        Ok(entered)
    }
}

/// Read a secret, hiding it from the terminal when one is attached.
///
/// Interactively (stdin is a TTY) the value is read with [`rpassword`] so it is
/// never echoed. When stdin is *not* a terminal — piped input from a script or
/// the spec tests — `rpassword` would try to open `/dev/tty` (failing, or
/// blocking on the real terminal) instead of reading the pipe, so we read the
/// secret as an ordinary line from stdin. There is no terminal echo to suppress
/// in that case, so nothing is lost.
fn read_secret(value_name: &str, prompt: &str) -> Result<String> {
    if io::stdin().is_terminal() {
        return rpassword::prompt_password(prompt).wrap_err(format!("Unable to read {value_name}"));
    }

    print!("{prompt}");
    io::stdout().flush().ok();

    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .wrap_err(format!("Unable to read {value_name}"))?;
    // Strip only the line terminator; a secret may legitimately contain
    // surrounding whitespace.
    Ok(line.trim_end_matches(['\r', '\n']).to_owned())
}

fn prompt_for_username_password(
    previous_username: Option<&str>,
    previous_password: Option<&str>,
) -> Result<PersistableCredentials> {
    let username = prompt_for_value("username", previous_username)?;
    if username.is_empty() {
        return Err(eyre!("Username cannot be empty"));
    }
    let password = prompt_for_sensitive_value("password", previous_password)?;

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
async fn complete_oauth_login(tenant: &str) -> Result<PersistableCredentials> {
    let base_url = format!("https://{tenant}.antithesis.com");
    let client = reqwest::Client::builder()
        .timeout(OAUTH_HTTP_TIMEOUT)
        .build()
        .wrap_err("failed to build the OAuth HTTP client")?;

    let config = fetch_cli_config(&client, &base_url).await?;
    // Bind the callback server *before* initiating login so the port we tell the
    // proxy about is one we're already listening on.
    let listeners = bind_callback_listener(&config).await?;
    let port = listeners.port;

    // PKCE: the verifier is the secret we keep; the challenge is what we hand to
    // the proxy. `cli_state` is opaque CSRF state the proxy validates server-side.
    let code_verifier = generate_verifier_or_state()?;
    let code_challenge = code_challenge_for(&code_verifier);
    let cli_state = generate_verifier_or_state()?;

    let location =
        request_login_redirect(&client, &base_url, port, &code_challenge, &cli_state).await?;

    println!("\nTo finish signing in, open the following URL in your browser:\n\n  {location}\n");
    open_in_browser(&location);
    println!("Waiting for you to complete sign-in in your browser...");

    let callback = wait_for_callback(listeners).await?;

    let tokens = exchange_code_for_tokens(
        &client,
        &base_url,
        &callback.auth_code,
        &code_verifier,
        &callback.flow_token,
    )
    .await?;

    let expiry = expiry_from_antithesis_token(&tokens.antithesis_token);

    Ok(PersistableCredentials::OAuth {
        antithesis_token: tokens.antithesis_token,
        refresh_token: tokens.refresh_token,
        expiry,
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
    let accept = tokio::time::timeout(CALLBACK_TIMEOUT, accept_any(&listeners.listeners)).await;
    let (mut stream, _addr) = accept
        .map_err(|_| {
            user_error(format!(
                "timed out after {} seconds waiting for the browser to complete sign-in",
                CALLBACK_TIMEOUT.as_secs()
            ))
        })?
        .wrap_err("failed to accept the OAuth callback connection")?;

    let request_line = read_request_line(&mut stream).await?;
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
        .wrap_err("the OAuth callback request had an unparseable target")?;

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

/// Best-effort extraction of the expiry from the Antithesis token
fn expiry_from_antithesis_token(token: &str) -> Option<DateTime<Utc>> {
    #[derive(Deserialize)]
    struct PasetoClaims {
        exp: Option<serde_json::Value>,
    }

    let mut parts = token.splitn(4, '.');
    let _version = parts.next()?;
    let purpose = parts.next()?;
    let payload = parts.next()?;
    if purpose != "public" {
        return None;
    }

    let bytes = BASE64_URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims = serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<PasetoClaims>()
        .next()?
        .ok()?;

    parse_exp_claim(&claims.exp?)
}

fn parse_exp_claim(exp: &serde_json::Value) -> Option<DateTime<Utc>> {
    if let Some(text) = exp.as_str() {
        return Some(DateTime::parse_from_rfc3339(text).ok()?.with_timezone(&Utc));
    }
    if let Some(secs) = exp.as_u64() {
        return DateTime::from_timestamp(secs as i64, 0);
    }
    None
}

/// Best-effort open of `url` in the user's default browser. Failures are
/// intentionally ignored — the URL is always also printed, so a headless or
/// opener-less environment can still complete the flow by hand.
fn open_in_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut c = Command::new("open");
        c.arg(url);
        c
    };
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut c = Command::new("cmd");
        // The empty "" is `start`'s window-title argument; without it a quoted
        // URL would be consumed as the title.
        c.args(["/C", "start", "", url]);
        c
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut c = Command::new("xdg-open");
        c.arg(url);
        c
    };

    let _ = command.stdout(Stdio::null()).stderr(Stdio::null()).spawn();
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

    /// Build a `v4.public` PASETO whose payload is `claims_json ‖ signature`,
    /// mirroring the real wire format (a 64-byte Ed25519 signature stand-in
    /// trails the JSON claims).
    fn public_paseto_with_claims(claims_json: &[u8]) -> String {
        let mut payload = claims_json.to_vec();
        payload.extend_from_slice(&[0u8; 64]);
        format!("v4.public.{}", BASE64_URL_SAFE_NO_PAD.encode(&payload))
    }

    #[test]
    fn expiry_parsed_from_public_paseto_rfc3339_exp() {
        let token =
            public_paseto_with_claims(br#"{"sub":"user","exp":"2039-01-01T00:00:00+00:00"}"#);
        let expected = DateTime::parse_from_rfc3339("2039-01-01T00:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(expiry_from_antithesis_token(&token), Some(expected));
    }

    #[test]
    fn expiry_parsed_from_numeric_exp() {
        let token = public_paseto_with_claims(br#"{"exp":2145916800}"#);
        assert_eq!(
            expiry_from_antithesis_token(&token),
            DateTime::from_timestamp(2_145_916_800, 0)
        );
    }

    #[test]
    fn expiry_is_none_for_local_paseto() {
        // A local token's payload is encrypted, so its claims are unreadable.
        let token = format!(
            "v4.local.{}",
            BASE64_URL_SAFE_NO_PAD.encode(b"opaque-ciphertext")
        );
        assert_eq!(expiry_from_antithesis_token(&token), None);
    }

    #[test]
    fn expiry_is_none_on_unparseable_or_missing_exp() {
        // Missing exp claim.
        assert_eq!(
            expiry_from_antithesis_token(&public_paseto_with_claims(br#"{"sub":"user"}"#)),
            None
        );
        // Not a PASETO at all.
        assert_eq!(expiry_from_antithesis_token("not-a-token"), None);
        // Public shape, but the payload isn't valid base64url / JSON.
        assert_eq!(expiry_from_antithesis_token("v4.public.@@@@"), None);
        // exp present but not an RFC 3339 string or a number.
        assert_eq!(
            expiry_from_antithesis_token(&public_paseto_with_claims(br#"{"exp":"whenever"}"#)),
            None
        );
    }

    // 2019-01-01T00:00:00+00:00, the `exp` claim in the canonical PASETO
    // v2.public test vectors below.
    const CANONICAL_VECTOR_EXP_UNIX: u64 = 1_546_300_800;

    #[test]
    fn expiry_parsed_from_canonical_v2_public_vector() {
        // Official PASETO 2-S-1 test vector: a real token with a genuine 64-byte
        // Ed25519 signature trailing the JSON claims (no footer). This exercises
        // the real base64url decode and the skip-the-signature parse — unlike the
        // synthetic tokens above whose "signature" is 64 zero bytes.
        let token = "v2.public.eyJkYXRhIjoidGhpcyBpcyBhIHNpZ25lZCBtZXNzYWdlIiwiZXhwIjoiMjAxOS0wMS0wMVQwMDowMDowMCswMDowMCJ9HQr8URrGntTu7Dz9J2IF23d1M7-9lH9xiqdGyJNvzp4angPW5Esc7C5huy_M8I8_DjJK2ZXC2SUYuOFM-Q_5Cw";
        assert_eq!(
            expiry_from_antithesis_token(token),
            DateTime::from_timestamp(CANONICAL_VECTOR_EXP_UNIX as i64, 0)
        );
    }

    #[test]
    fn expiry_parsed_from_canonical_v2_public_vector_with_footer() {
        // Official PASETO 2-S-2 test vector: same claims, but with a footer
        // (`UGFyYWdvbiBJbml0aWF0aXZlIEVudGVycHJpc2Vz` = "Paragon Initiative
        // Enterprises"). Confirms the 4th `.`-delimited segment is ignored.
        let token = "v2.public.eyJkYXRhIjoidGhpcyBpcyBhIHNpZ25lZCBtZXNzYWdlIiwiZXhwIjoiMjAxOS0wMS0wMVQwMDowMDowMCswMDowMCJ9flsZsx_gYCR0N_Ec2QxJFFpvQAs7h9HtKwbVK2n1MJ3Rz-hwe8KUqjnd8FAnIJZ601tp7lGkguU63oGbomhoBw.UGFyYWdvbiBJbml0aWF0aXZlIEVudGVycHJpc2Vz";
        assert_eq!(
            expiry_from_antithesis_token(token),
            DateTime::from_timestamp(CANONICAL_VECTOR_EXP_UNIX as i64, 0)
        );
    }
}
