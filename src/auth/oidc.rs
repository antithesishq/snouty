//! GitHub Actions OIDC credentials: a token that the Actions token endpoint
//! gives for the Antithesis audience.

use std::{sync::Arc, time::Duration};

use color_eyre::eyre::{Context, Result};
use serde::Deserialize;
use tokio::sync::OnceCell;

use super::AuthenticationInfo;
use crate::{attributed_value::AttributedValue, env};

pub(super) const OIDC_URL_VAR_NAME: &str = "ACTIONS_ID_TOKEN_REQUEST_URL";
pub(super) const OIDC_TOKEN_VAR_NAME: &str = "ACTIONS_ID_TOKEN_REQUEST_TOKEN";

const OIDC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

impl AuthenticationInfo {
    pub(super) fn try_from_github_actions_environment() -> Result<Option<AttributedValue<Self>>> {
        if let Some(actions_id_request_token) = env::var(OIDC_TOKEN_VAR_NAME)?
            && let Some(actions_id_url) = env::var(OIDC_URL_VAR_NAME)?
        {
            return Ok(Some(AttributedValue::EnvironmentVariable {
                value: Self::GithubActionsOidc {
                    url: actions_id_url,
                    request_token: actions_id_request_token,
                    cached: Arc::new(OnceCell::new()),
                },
                environment_variable_names: vec![OIDC_URL_VAR_NAME, OIDC_TOKEN_VAR_NAME],
            }));
        }

        Ok(None)
    }
}

/// The token of a `GithubActionsOidc` credential: fetched on the first call,
/// and kept in `cached` for the later calls.
pub(crate) async fn cached_github_actions_oidc_token<'a>(
    url: &str,
    request_token: &str,
    cached: &'a OnceCell<String>,
) -> Result<&'a str> {
    Ok(cached
        .get_or_try_init(|| fetch_github_actions_oidc_credentials(url, request_token))
        .await?)
}

/// Exchange the GitHub Actions OIDC *request* token for an Antithesis-audience
/// OIDC token by calling the Actions token endpoint.
///
/// Split out from [`Credentials::try_from_github_actions_environment`] so the
/// HTTP exchange can be unit-tested against a local server without mutating the
/// process environment (which would race other tests under threaded
/// `cargo test`). The request URL already carries a query string, so the
/// audience is appended with `&`.
async fn fetch_github_actions_oidc_credentials(
    actions_id_url: &str,
    actions_id_request_token: &str,
) -> Result<String> {
    #[derive(Deserialize)]
    struct OidcTokenResponse {
        value: String,
    }

    let client = crate::api::client_builder()
        .timeout(OIDC_REQUEST_TIMEOUT)
        .build()?;
    let response: OidcTokenResponse = client
        .get(format!("{actions_id_url}&audience=antithesis"))
        .bearer_auth(actions_id_request_token)
        .send()
        .await?
        .error_for_status()
        .wrap_err("failed to fetch a GitHub Actions OIDC token")?
        .json()
        .await?;

    Ok(response.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::fake_secrets::{FAKE_OIDC_REQUEST_TOKEN, FAKE_OIDC_TOKEN};
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::mpsc::{self, Receiver};
    use std::thread;

    /// The parts of an inbound HTTP request the OIDC exchange test asserts on.
    struct CapturedRequest {
        request_line: String,
        authorization: Option<String>,
    }

    /// Spawn a one-shot HTTP server that records the request it receives and
    /// answers it with `status` (e.g. `"200 OK"`) and a JSON `body`. Returns the
    /// request URL — already carrying a query string, like the real Actions
    /// endpoint — and a channel that yields the captured request once it arrives.
    fn spawn_oidc_token_server(
        status: &'static str,
        body: &str,
    ) -> (String, Receiver<CapturedRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock OIDC server");
        let addr = listener.local_addr().expect("mock server address");
        let (tx, rx) = mpsc::channel();
        let body = body.to_owned();

        thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let mut response_stream = stream.try_clone().expect("clone stream");
            let mut reader = BufReader::new(stream);

            let mut request_line = String::new();
            reader
                .read_line(&mut request_line)
                .expect("read request line");

            let mut authorization = None;
            loop {
                let mut line = String::new();
                let read = reader.read_line(&mut line).expect("read header line");
                if read == 0 || line == "\r\n" || line == "\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.trim().eq_ignore_ascii_case("authorization")
                {
                    authorization = Some(value.trim().to_owned());
                }
            }

            tx.send(CapturedRequest {
                request_line: request_line.trim().to_owned(),
                authorization,
            })
            .expect("send captured request");

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len(),
            );
            response_stream
                .write_all(response.as_bytes())
                .expect("write response");
            response_stream.flush().expect("flush response");
        });

        (format!("http://{addr}/token?api-version=2.0"), rx)
    }

    /// The JSON envelope that GitHub's Actions OIDC endpoint wraps the token in.
    fn oidc_token_body() -> String {
        serde_json::json!({"count": 1, "value": FAKE_OIDC_TOKEN}).to_string()
    }

    #[tokio::test]
    async fn github_actions_oidc_exchange_sends_bearer_token_and_audience() {
        // The endpoint returns the JWT wrapped in a JSON envelope, exactly as
        // GitHub's Actions OIDC endpoint does.
        let (url, requests) = spawn_oidc_token_server("200 OK", &oidc_token_body());

        let token = fetch_github_actions_oidc_credentials(&url, FAKE_OIDC_REQUEST_TOKEN)
            .await
            .unwrap();

        assert_eq!(token, FAKE_OIDC_TOKEN);

        let request = requests
            .recv_timeout(Duration::from_secs(5))
            .expect("server should have received a request");

        // The Antithesis audience is appended onto the (already query-bearing) URL.
        assert!(
            request.request_line.contains("audience=antithesis"),
            "request line missing audience: {:?}",
            request.request_line
        );
        // The Actions request token is presented as a bearer credential.
        assert_eq!(
            request.authorization.as_deref(),
            Some(format!("Bearer {FAKE_OIDC_REQUEST_TOKEN}").as_str())
        );
    }

    #[tokio::test]
    async fn github_actions_oidc_exchange_errors_on_non_success_status() {
        // A rejected request token (or any non-2xx) must surface as an error
        // rather than letting the error body be mistaken for a token.
        let (url, _requests) =
            spawn_oidc_token_server("403 Forbidden", r#"{"message":"bad credentials"}"#);

        let result = fetch_github_actions_oidc_credentials(&url, FAKE_OIDC_REQUEST_TOKEN).await;
        assert!(result.is_err(), "expected an error for a 403 response");
    }

    #[tokio::test]
    async fn github_actions_oidc_token_is_fetched_once_and_cached() {
        // The mock server accepts exactly one connection and then shuts down, so
        // a second exchange would fail with a connection error. Both
        // `auth_header` calls succeeding proves the token is reused from the
        // cache rather than re-fetched per request.
        let (url, requests) = spawn_oidc_token_server("200 OK", &oidc_token_body());

        let auth = AuthenticationInfo::GithubActionsOidc {
            url,
            request_token: FAKE_OIDC_REQUEST_TOKEN.to_owned(),
            cached: Arc::new(OnceCell::new()),
        };

        let first = auth.auth_header().await.unwrap();
        let second = auth.auth_header().await.unwrap();
        assert_eq!(first.to_str().unwrap(), format!("GHA {FAKE_OIDC_TOKEN}"));
        assert_eq!(first, second);

        // Exactly one request reached the server: the first fetch is captured,
        // and no second request ever arrives.
        requests
            .recv_timeout(Duration::from_secs(5))
            .expect("first fetch should hit the server");
        assert!(
            requests.recv_timeout(Duration::from_millis(200)).is_err(),
            "second auth_header call should be served from cache, not re-fetched"
        );
    }
}
