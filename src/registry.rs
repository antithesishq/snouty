//! The tenant's container registry, `registry.<tenant>.antithesis.com`, and
//! the credential that snouty gives it.
//!
//! The registry takes HTTP basic auth: the username [`USERNAME`], and an
//! Antithesis token or a GitHub Actions OIDC token as the password. The
//! Antithesis API team gave this contract before the registry went live, so
//! no response from it has been observed yet.

use std::{fmt, path::PathBuf, str::FromStr};

use chrono::{TimeDelta, Utc};
use color_eyre::eyre::Result;

use crate::api::AntithesisApi;
use crate::auth::AuthenticationInfo;
use crate::error::user_error;
use crate::settings::{Settings, SettingsRequest};

pub const USERNAME: &str = "snouty-token";

/// A registry client uses one password for a whole push, so a token that
/// [`password`] returns must outlive the push.
const TOKEN_MIN_LIFETIME: TimeDelta = TimeDelta::hours(2);

/// A registry host, such as `registry.acme.antithesis.com`, or
/// `localhost:5000` with its port. It holds no scheme and no path, and its
/// letters are lowercase, because a host name ignores case.
#[derive(Debug, PartialEq, Eq)]
pub struct RegistryHost(String);

impl RegistryHost {
    pub fn for_tenant(tenant: &str) -> Self {
        Self(format!("registry.{tenant}.antithesis.com").to_ascii_lowercase())
    }
}

impl fmt::Display for RegistryHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Parses a server URL as a registry client gives it: a bare host, or a URL
/// with a scheme, a path, or both.
impl FromStr for RegistryHost {
    type Err = &'static str;

    fn from_str(server_url: &str) -> Result<Self, Self::Err> {
        let rest = match server_url.split_once("://") {
            Some((scheme, rest)) if !scheme.contains('/') => rest,
            _ => server_url,
        };
        let host = rest.split('/').next().unwrap_or_default().trim();
        if host.is_empty() {
            return Err("the server URL names no host");
        }
        Ok(Self(host.to_ascii_lowercase()))
    }
}

/// The tenant registry that `settings` push to: the host of the repository,
/// when it is `registry.<tenant>.antithesis.com` for the tenant of the same
/// settings. `None` when the repository is on another registry, or when the
/// tenant or the repository is not set.
pub fn tenant_registry(settings: &Settings) -> Option<RegistryHost> {
    let host: RegistryHost = settings.repository()?.parse().ok()?;
    (host == RegistryHost::for_tenant(settings.tenant()?)).then_some(host)
}

/// The settings that give the credential for `host`. Settings files resolve
/// as for every snouty command: `--settings` or `SNOUTY_SETTINGS_PATH` takes
/// the place of `./.snouty.toml`, and the global settings file applies.
///
/// 1. When `--profile` or `ANTITHESIS_PROFILE` names a profile, use only that
///    profile. When it fails to resolve, fail with its error. When its tenant
///    registry is not `host`, return `None`: do not search the other
///    profiles.
/// 2. Otherwise, use the first profile whose tenant registry is `host` and
///    that has a credential, in the order of
///    [`SettingsRequest::resolve_every_profile`]: the top-level defaults
///    first, then the named profiles. A profile that fails to resolve is
///    skipped. When no profile matches, return `None`. When profiles match
///    but none has a credential, fail with an error that names them.
///
/// A profile has a credential when the lookup of every command finds one for
/// it: in the environment, or in the keychain or the credentials file, for
/// that profile or for the default profile. The search makes no network call.
pub fn select_settings(
    project_settings_path: Option<PathBuf>,
    profile: Option<String>,
    host: &RegistryHost,
) -> Result<Option<Settings>> {
    let uses_host = |settings: &Settings| tenant_registry(settings).as_ref() == Some(host);
    let request = SettingsRequest::from_flags(project_settings_path, profile)?;
    if request.profile().is_some() {
        return Ok(Some(request.resolve()?).filter(uses_host));
    }
    let mut without_credential = Vec::new();
    for settings in request
        .resolve_every_profile()?
        .into_iter()
        .filter(uses_host)
    {
        if AuthenticationInfo::find_ambient(settings.profile())?.is_some() {
            return Ok(Some(settings));
        }
        without_credential.push(settings.profile().map(str::to_owned));
    }
    if without_credential.is_empty() {
        return Ok(None);
    }
    let names: Vec<_> = without_credential
        .iter()
        .map(|profile| match profile {
            Some(name) => format!("`{name}`"),
            None => "the default profile".to_owned(),
        })
        .collect();
    let login = if without_credential.contains(&None) {
        "run `snouty login`, or `snouty login --profile <name>` for a named profile"
    } else {
        "run `snouty login --profile <name>` for one of them"
    };
    Err(user_error(format!(
        "no profile that uses {host} has an Antithesis credential: {}; {login}, or set ANTITHESIS_API_KEY",
        names.join(", ")
    )))
}

/// The password that the registry takes with [`USERNAME`]: the API key, the
/// OAuth access token, or the GitHub Actions OIDC token of the credential of
/// `api`. A username and password is an error.
///
/// An OAuth token that expires in more than [`TOKEN_MIN_LIFETIME`] is used as it
/// is. A token that expires sooner, or whose expiry is unknown, is refreshed.
/// When there is no refresh token, or the refresh fails, the stored token is
/// used until it expires.
pub async fn password(api: &AntithesisApi) -> Result<String> {
    let auth = api.authentication();
    if let Some(token) = auth.github_actions_oidc_token().await? {
        return Ok(token.to_owned());
    }
    let Some(stored) = auth.bearer_token() else {
        return Err(user_error(format!(
            "the Antithesis container registry does not accept a {auth}; set ANTITHESIS_API_KEY, or run `snouty login` to store an API key or OAuth credentials"
        )));
    };
    let expired = auth.expired();
    let near_expiry = auth
        .expires_at()
        .is_none_or(|expires_at| expires_at - Utc::now() <= TOKEN_MIN_LIFETIME);
    if near_expiry {
        // `refresh_authentication` gives `Ok(None)` or `Err` only while the
        // persisted token is still the stored one.
        match api.refresh_authentication().await {
            Ok(refreshed) => {
                if let Some(token) = refreshed.and_then(|refreshed| refreshed.bearer_token()) {
                    return Ok(token);
                }
            }
            Err(err) if expired => return Err(err),
            Err(err) => {
                // A bare eprintln, not log::warn!: the notice must reach
                // stderr independent of RUST_LOG.
                eprintln!(
                    "warning: the OAuth token refresh failed, so the stored token is used until it expires: {err:#}"
                );
                return Ok(stored);
            }
        }
    }
    if expired {
        return Err(user_error(
            "the stored OAuth token has expired, and there is no refresh token to replace it; run `snouty login`",
        ));
    }
    Ok(stored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ResponseCache;
    use crate::auth::{OAuthRefreshInfo, paseto_expiring_in};
    use crate::testutils::fake_secrets::{
        FAKE_ACCESS_TOKEN_A, FAKE_ACCESS_TOKEN_B, FAKE_API_KEY_A, FAKE_OIDC_REQUEST_TOKEN,
        FAKE_OIDC_TOKEN, FAKE_PASSWORD, FAKE_REFRESH_TOKEN_A, FAKE_REFRESH_TOKEN_B, FAKE_USERNAME,
    };
    use hegel::generators;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Nothing listens on this address, so a request to it fails.
    const NO_SERVER: &str = "http://127.0.0.1:9";

    fn api(base_url: &str, auth: AuthenticationInfo) -> AntithesisApi {
        AntithesisApi::build(
            &Settings::for_test_base_url(base_url),
            auth,
            false,
            ResponseCache::Disabled,
        )
        .unwrap()
    }

    /// An OAuth credential whose origin is a credentials file in `dir`.
    fn oauth(
        dir: &tempfile::TempDir,
        access_token: &str,
        refresh_token: Option<&str>,
    ) -> AuthenticationInfo {
        let refresh_info = OAuthRefreshInfo::CredentialsFile {
            path: dir.path().join("credentials.toml"),
            profile: None,
        };
        AuthenticationInfo::oauth_for_test(access_token, refresh_token, refresh_info)
    }

    /// Mount a refresh endpoint that [`FAKE_REFRESH_TOKEN_A`] must call
    /// `times` times, and that answers with `new_access_token` and
    /// [`FAKE_REFRESH_TOKEN_B`].
    async fn mount_refresh(server: &MockServer, new_access_token: &str, times: u64) {
        Mock::given(method("POST"))
            .and(path("/auth/cli/refresh"))
            .and(header(
                "authorization",
                format!("Bearer {FAKE_REFRESH_TOKEN_A}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "antithesis_token": new_access_token,
                "refresh_token": FAKE_REFRESH_TOKEN_B
            })))
            .expect(times)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn the_password_is_the_api_key() {
        let auth = AuthenticationInfo::ApiKey {
            api_key: FAKE_API_KEY_A.to_owned(),
        };
        assert_eq!(
            password(&api(NO_SERVER, auth)).await.unwrap(),
            FAKE_API_KEY_A
        );
    }

    #[tokio::test]
    async fn the_password_is_the_github_actions_oidc_token() {
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
                    .set_body_json(serde_json::json!({"count": 1, "value": FAKE_OIDC_TOKEN})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let auth = AuthenticationInfo::GithubActionsOidc {
            url: format!("{}/token?api-version=2.0", server.uri()),
            request_token: FAKE_OIDC_REQUEST_TOKEN.to_owned(),
            cached: std::sync::Arc::default(),
        };

        assert_eq!(
            password(&api(NO_SERVER, auth)).await.unwrap(),
            FAKE_OIDC_TOKEN
        );
    }

    #[tokio::test]
    async fn a_password_credential_is_rejected_with_the_fix() {
        let auth = AuthenticationInfo::Password {
            username: FAKE_USERNAME.to_owned(),
            password: FAKE_PASSWORD.to_owned(),
        };
        let err = password(&api(NO_SERVER, auth)).await.unwrap_err();
        assert!(err.to_string().contains("set ANTITHESIS_API_KEY"), "{err}");
    }

    #[tokio::test]
    async fn a_token_far_from_expiry_is_used_without_a_refresh() {
        let server = MockServer::start().await;
        mount_refresh(&server, FAKE_ACCESS_TOKEN_A, 0).await;
        let dir = tempfile::TempDir::new().unwrap();
        let fresh = paseto_expiring_in(TimeDelta::minutes(125));
        let auth = oauth(&dir, &fresh, Some(FAKE_REFRESH_TOKEN_A));

        assert_eq!(password(&api(&server.uri(), auth)).await.unwrap(), fresh);
    }

    #[tokio::test]
    async fn a_token_that_expires_within_the_margin_is_refreshed() {
        let server = MockServer::start().await;
        mount_refresh(&server, FAKE_ACCESS_TOKEN_A, 1).await;
        let dir = tempfile::TempDir::new().unwrap();
        let auth = oauth(
            &dir,
            &paseto_expiring_in(TimeDelta::minutes(119)),
            Some(FAKE_REFRESH_TOKEN_A),
        );

        let secret = password(&api(&server.uri(), auth)).await.unwrap();

        assert_eq!(secret, FAKE_ACCESS_TOKEN_A);
        let persisted = std::fs::read_to_string(dir.path().join("credentials.toml")).unwrap();
        assert!(persisted.contains(FAKE_ACCESS_TOKEN_A), "{persisted}");
        assert!(persisted.contains(FAKE_REFRESH_TOKEN_B), "{persisted}");
    }

    #[tokio::test]
    async fn a_token_whose_expiry_is_unknown_is_refreshed() {
        let server = MockServer::start().await;
        mount_refresh(&server, FAKE_ACCESS_TOKEN_B, 1).await;
        let dir = tempfile::TempDir::new().unwrap();
        let auth = oauth(&dir, FAKE_ACCESS_TOKEN_A, Some(FAKE_REFRESH_TOKEN_A));

        let secret = password(&api(&server.uri(), auth)).await.unwrap();

        assert_eq!(secret, FAKE_ACCESS_TOKEN_B);
    }

    #[tokio::test]
    async fn without_a_refresh_token_only_an_unexpired_token_is_used() {
        let dir = tempfile::TempDir::new().unwrap();
        for valid in [
            paseto_expiring_in(TimeDelta::days(1000)),
            paseto_expiring_in(TimeDelta::minutes(119)),
        ] {
            let auth = oauth(&dir, &valid, None);
            assert_eq!(password(&api(NO_SERVER, auth)).await.unwrap(), valid);
        }

        let auth = oauth(&dir, &paseto_expiring_in(-TimeDelta::days(1)), None);
        let err = password(&api(NO_SERVER, auth)).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "the stored OAuth token has expired, and there is no refresh token to replace it; run `snouty login`"
        );
    }

    #[tokio::test]
    async fn a_token_that_another_process_just_refreshed_is_used() {
        // A second refresh would spend the refresh token that the other
        // process already spent.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/auth/cli/refresh"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("credentials.toml"),
            format!(
                "[default]\ntype = \"OAuth\"\nantithesis_token = \"{FAKE_ACCESS_TOKEN_A}\"\nrefresh_token = \"{FAKE_REFRESH_TOKEN_B}\"\n"
            ),
        )
        .unwrap();
        let auth = oauth(
            &dir,
            &paseto_expiring_in(TimeDelta::minutes(119)),
            Some(FAKE_REFRESH_TOKEN_A),
        );

        let secret = password(&api(&server.uri(), auth)).await.unwrap();

        assert_eq!(secret, FAKE_ACCESS_TOKEN_A);
    }

    #[tokio::test]
    async fn a_failed_refresh_falls_back_only_to_an_unexpired_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/auth/cli/refresh"))
            .respond_with(ResponseTemplate::new(503))
            .expect(2)
            .mount(&server)
            .await;
        let dir = tempfile::TempDir::new().unwrap();

        let valid = paseto_expiring_in(TimeDelta::minutes(119));
        let auth = oauth(&dir, &valid, Some(FAKE_REFRESH_TOKEN_A));
        assert_eq!(password(&api(&server.uri(), auth)).await.unwrap(), valid);

        let expired = paseto_expiring_in(-TimeDelta::days(1));
        let auth = oauth(&dir, &expired, Some(FAKE_REFRESH_TOKEN_A));
        let err = password(&api(&server.uri(), auth)).await.unwrap_err();
        assert!(err.to_string().contains("snouty login"), "{err}");
    }

    fn settings(profile: Option<&str>, tenant: &str, repository: &str) -> Settings {
        let builder = Settings::builder().tenant(tenant).repository(repository);
        match profile {
            Some(profile) => builder.profile(profile),
            None => builder,
        }
        .build()
    }

    fn host(value: &str) -> RegistryHost {
        value.parse().unwrap()
    }

    /// A host as a registry reference spells it: DNS labels, and an optional
    /// port.
    fn hosts() -> impl hegel::PrintableGenerator<String> {
        generators::from_regex(r"[A-Za-z0-9-]{1,10}(\.[A-Za-z0-9-]{1,10}){0,3}(:[0-9]{1,5})?")
            .fullmatch(true)
    }

    /// A scheme, a path and the case of the letters do not change the host.
    #[hegel::test]
    fn a_server_url_names_its_host(tc: hegel::TestCase) {
        let bare = tc.draw(hosts());
        let scheme = tc.draw(generators::sampled_from(vec!["", "https://", "http://"]));
        let path = tc.draw(generators::from_regex(r"(/[a-z0-9/._-]*)?").fullmatch(true));
        let url = format!("{scheme}{bare}{path}");
        let parsed = host(&url);
        assert_eq!(parsed.to_string(), bare.to_ascii_lowercase(), "{url}");
        assert_eq!(parsed, host(&bare.to_ascii_uppercase()));
    }

    /// The host of any parsed text parses to itself, and holds no path.
    #[hegel::test]
    fn parsing_a_host_again_changes_nothing(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        let Ok(once) = text.parse::<RegistryHost>() else {
            return;
        };
        assert!(!once.0.contains('/'), "{text:?} gave {once}");
        assert_eq!(host(&once.to_string()), once);
    }

    #[test]
    fn a_server_url_with_no_host_is_an_error() {
        for url in ["", "  ", "https://", "https:///v2/", "/v2/"] {
            assert!(url.parse::<RegistryHost>().is_err(), "{url:?}");
        }
    }

    #[test]
    fn the_tenant_registry_is_the_repository_host_of_that_tenant() {
        let cases = [
            ("acme", "registry.acme.antithesis.com/app", true),
            ("acme", "registry.acme.antithesis.com", true),
            ("acme", "REGISTRY.acme.antithesis.com/app", true),
            ("acme", "registry.other.antithesis.com/app", false),
            ("acme", "us-docker.pkg.dev/acme/repo", false),
            ("acme", "registry.acme.antithesis.com:5000/app", false),
        ];
        for (tenant, repository, matches) in cases {
            assert_eq!(
                tenant_registry(&settings(None, tenant, repository)),
                matches.then(|| RegistryHost::for_tenant(tenant)),
                "{tenant} {repository}"
            );
        }
        assert_eq!(
            tenant_registry(&Settings::builder().tenant("acme").build()),
            None
        );
    }
}
