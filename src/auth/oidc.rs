//! GitHub Actions OIDC credentials: a token that the Actions token endpoint
//! gives for the Antithesis audience.

use std::{
    sync::{Arc, PoisonError, RwLock},
    time::Duration,
};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, Result};
use serde::Deserialize;

use super::{AuthenticationInfo, claims::try_get_expiry_from_jwt};
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
                value: Self::GithubActionsOidc(GithubActionsOidcToken::new(
                    actions_id_url,
                    actions_id_request_token,
                )),
                environment_variable_names: vec![OIDC_URL_VAR_NAME, OIDC_TOKEN_VAR_NAME],
            }));
        }

        Ok(None)
    }
}

/// A GitHub Actions OIDC credential: the request token that the Actions token
/// endpoint at `url` takes, and the last token that the endpoint gave. Clones
/// share the token: a refresh through any clone gives every clone the new
/// token.
#[derive(Clone)]
pub struct GithubActionsOidcToken {
    url: String,
    request_token: String,
    /// Never held across an `await`: a refresh fetches with no lock held,
    /// and then takes the write lock to store the token.
    fetched: Arc<RwLock<Option<FetchedToken>>>,
}

#[derive(Clone)]
struct FetchedToken {
    token: String,
    /// The `exp` claim of the token, when snouty can read it.
    expires_at: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for GithubActionsOidcToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubActionsOidcToken")
            .field("url", &self.url)
            .field("request_token", &"[REDACTED]")
            .field(
                "expires_at",
                &self.current().map(|fetched| fetched.expires_at),
            )
            .finish_non_exhaustive()
    }
}

impl GithubActionsOidcToken {
    pub(crate) fn new(url: String, request_token: String) -> Self {
        Self {
            url,
            request_token,
            fetched: Arc::default(),
        }
    }

    fn current(&self) -> Option<FetchedToken> {
        self.fetched
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The token that the last refresh fetched, or `None` before the first
    /// refresh.
    pub(crate) fn token(&self) -> Option<String> {
        self.current().map(|fetched| fetched.token)
    }

    /// Whether the credential needs a new token: it has none, its `exp` is in
    /// the past, or snouty cannot read its `exp`. A token with no readable
    /// `exp` is thus used for one request, and not again.
    pub(crate) fn expired(&self) -> bool {
        self.current().is_none_or(|fetched| {
            fetched
                .expires_at
                .is_none_or(|expires_at| Utc::now() >= expires_at)
        })
    }

    /// Replaces the token with a new one from the Actions token endpoint, in
    /// every clone. Concurrent refreshes each fetch, and the last to finish
    /// sets the token: a fetch is idempotent and costs nothing.
    pub(crate) async fn refresh(&self) -> Result<()> {
        let token = fetch_github_actions_oidc_credentials(&self.url, &self.request_token).await?;
        let expires_at = try_get_expiry_from_jwt(&token);
        *self.fetched.write().unwrap_or_else(PoisonError::into_inner) =
            Some(FetchedToken { token, expires_at });
        Ok(())
    }
}

/// Exchange the GitHub Actions OIDC *request* token for an Antithesis-audience
/// OIDC token by calling the Actions token endpoint.
///
/// Split out from [`GithubActionsOidcToken::refresh`] so the
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
    use crate::testutils::auth::{jwt_expiring_in, jwt_with_claims};
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

    /// A wiremock Actions token endpoint that gives `token`, and that
    /// expects `fetches` requests. It returns the endpoint and a credential
    /// that fetches from it.
    async fn oidc_credential_fetching(
        token: &str,
        fetches: u64,
    ) -> (wiremock::MockServer, AuthenticationInfo) {
        use wiremock::matchers::{header, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/token"))
            .and(query_param("audience", "antithesis"))
            .and(header(
                "authorization",
                format!("Bearer {FAKE_OIDC_REQUEST_TOKEN}"),
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"count": 1, "value": token})),
            )
            .expect(fetches)
            .mount(&server)
            .await;
        let auth = AuthenticationInfo::GithubActionsOidc(GithubActionsOidcToken::new(
            format!("{}/token?api-version=2.0", server.uri()),
            FAKE_OIDC_REQUEST_TOKEN.to_owned(),
        ));
        (server, auth)
    }

    /// The `Authorization` header of `auth` for one request, as each API
    /// request gets it: a refresh when the credential is expired, then the
    /// header.
    async fn header_for_one_request(auth: &AuthenticationInfo) -> String {
        if auth.expired() {
            // The OIDC refresh calls neither the API client nor the API.
            auth.refresh(&crate::testutils::test_client(), "http://unused.invalid")
                .await
                .unwrap();
        }
        auth.auth_header().unwrap().to_str().unwrap().to_owned()
    }

    /// Two requests with `auth`, which must both present `token`.
    async fn two_requests(auth: &AuthenticationInfo, token: &str) {
        for _ in 0..2 {
            assert_eq!(header_for_one_request(auth).await, format!("GHA {token}"));
        }
    }

    #[tokio::test]
    async fn github_actions_oidc_token_is_reused_before_its_exp() {
        let token = jwt_expiring_in(chrono::TimeDelta::minutes(5));
        let (server, auth) = oidc_credential_fetching(&token, 1).await;
        two_requests(&auth, &token).await;
        server.verify().await;
    }

    #[tokio::test]
    async fn github_actions_oidc_token_is_fetched_again_past_its_exp() {
        let token = jwt_expiring_in(-chrono::TimeDelta::minutes(1));
        let (server, auth) = oidc_credential_fetching(&token, 2).await;
        two_requests(&auth, &token).await;
        server.verify().await;
    }

    #[tokio::test]
    async fn github_actions_oidc_token_without_a_readable_exp_is_used_once() {
        for token in [
            FAKE_OIDC_TOKEN.to_owned(),
            jwt_with_claims(br#"{"sub":"repo:acme/app"}"#),
        ] {
            let (server, auth) = oidc_credential_fetching(&token, 2).await;
            two_requests(&auth, &token).await;
            server.verify().await;
        }
    }
}
