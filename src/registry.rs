//! The tenant's container registry, `registry.<tenant>.antithesis.com`, and
//! the credential that snouty gives it.
//!
//! The registry takes HTTP basic auth: the username [`USERNAME`], and an
//! Antithesis token or a GitHub Actions OIDC token as the password. The
//! Antithesis API team gave this contract before the registry went live, so
//! no response from it has been observed yet.

use std::{fmt, path::PathBuf, str::FromStr};

use chrono::{TimeDelta, Utc};
use color_eyre::eyre::{OptionExt, Result};

use crate::api::AntithesisApi;
use crate::auth::{AuthenticationInfo, PasswordPolicy, RefreshableOAuthToken};
use crate::error::user_error;
use crate::settings::{Settings, SettingsRequest, is_valid_tenant_host};

pub const USERNAME: &str = "snouty-token";

/// A registry client uses one secret for a whole push, so a token that
/// [`secret`] returns must outlive the push.
const TOKEN_MIN_LIFETIME: TimeDelta = TimeDelta::hours(2);

/// The container registry of a tenant, `registry.<tenant>.antithesis.com`.
/// The tenant is lowercase, because a host name ignores case.
#[derive(Debug, PartialEq, Eq)]
pub struct TenantRegistry {
    tenant: String,
}

impl TenantRegistry {
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

impl fmt::Display for TenantRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "registry.{}.antithesis.com", self.tenant)
    }
}

/// Why a text does not parse as a [`TenantRegistry`].
#[derive(Debug, PartialEq, Eq)]
pub enum TenantRegistryError {
    /// The text names no host.
    InvalidServerUrl,
    /// The text names a host, but not a tenant registry.
    NotTenantRegistry,
}

impl fmt::Display for TenantRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidServerUrl => "the server URL names no host",
            Self::NotTenantRegistry => "the server URL is not registry.<tenant>.antithesis.com",
        })
    }
}

impl std::error::Error for TenantRegistryError {}

/// Parses a server URL as a registry client gives it: a host, with an
/// optional scheme and path. A repository, such as
/// `registry.acme.antithesis.com/team/app`, parses to its registry. The host
/// ignores case, and may take the port 443, which a registry client uses when
/// it names no port.
impl FromStr for TenantRegistry {
    type Err = TenantRegistryError;

    fn from_str(server_url: &str) -> Result<Self, Self::Err> {
        let server_url = server_url.trim();
        let rest = server_url
            .split_once("://")
            .map_or(server_url, |(_, rest)| rest);
        let host = rest.split('/').next().unwrap_or_default();
        if host.is_empty() {
            return Err(TenantRegistryError::InvalidServerUrl);
        }
        let host = host
            .strip_suffix(":443")
            .unwrap_or(host)
            .to_ascii_lowercase();
        let tenant = host
            .strip_prefix("registry.")
            .and_then(|rest| rest.strip_suffix(".antithesis.com"))
            .filter(|tenant| is_valid_tenant_host(tenant))
            .ok_or(TenantRegistryError::NotTenantRegistry)?;
        Ok(Self {
            tenant: tenant.to_owned(),
        })
    }
}

/// The tenant registry that `settings` push to: the registry of the
/// repository, when it is the registry of the tenant of the same settings.
/// `None` when the repository is on another registry, or when the tenant or
/// the repository is not set.
pub fn tenant_registry(settings: &Settings) -> Option<TenantRegistry> {
    let registry: TenantRegistry = settings.repository()?.parse().ok()?;
    registry
        .tenant
        .eq_ignore_ascii_case(settings.tenant()?)
        .then_some(registry)
}

/// The settings and the credential for `registry`. Settings files resolve
/// as for every snouty command: `--settings` or `SNOUTY_SETTINGS_PATH` takes
/// the place of `./.snouty.toml`, and the global settings file applies.
///
/// 1. When `--profile` or `ANTITHESIS_PROFILE` names a profile, use only that
///    profile, with the credential that every command finds for it. When the
///    profile fails to resolve, fail with its error. When its tenant registry
///    is not `registry`, return `None`: do not search the other profiles.
/// 2. Otherwise, find the profiles whose tenant registry is `registry`, in
///    the order of [`SettingsRequest::resolve_every_profile`]: the top-level
///    defaults first, then the named profiles. A profile that fails to
///    resolve is skipped. Use the first of them that has its own credential
///    ([`AuthenticationInfo::find_own`]). A named profile does not use the
///    stored credential of the default profile, because that credential can
///    be for another tenant. When no profile matches, return `None`. When
///    profiles match but none has its own credential, fail.
///
/// The search makes no network call.
pub fn select_settings(
    project_settings_path: Option<PathBuf>,
    profile: Option<String>,
    registry: &TenantRegistry,
) -> Result<Option<(Settings, AuthenticationInfo)>> {
    let request = SettingsRequest::from_flags(project_settings_path, profile)?;
    if request.profile().is_some() {
        let settings = request.resolve()?;
        if tenant_registry(&settings).as_ref() != Some(registry) {
            return Ok(None);
        }
        let credential = AuthenticationInfo::for_ambient_configuration(
            settings.profile(),
            PasswordPolicy::Reject,
        )?;
        return Ok(Some((settings, credential)));
    }
    first_with_own_credential(request.resolve_every_profile()?, registry, |profile| {
        AuthenticationInfo::find_own(profile, PasswordPolicy::Reject)
    })
}

/// The first of `profiles` whose tenant registry is `registry` and for whose
/// profile `own_credential` finds a credential, with that credential.
fn first_with_own_credential(
    profiles: Vec<Settings>,
    registry: &TenantRegistry,
    mut own_credential: impl FnMut(Option<&str>) -> Result<Option<AuthenticationInfo>>,
) -> Result<Option<(Settings, AuthenticationInfo)>> {
    let mut matched = false;
    for settings in profiles {
        if tenant_registry(&settings).as_ref() != Some(registry) {
            continue;
        }
        if let Some(credential) = own_credential(settings.profile())? {
            return Ok(Some((settings, credential)));
        }
        matched = true;
    }
    if matched {
        return Err(user_error(
            "no credential found; run `snouty login` or set ANTITHESIS_API_KEY",
        ));
    }
    Ok(None)
}

/// The registry secret, which the credential helper returns as `Secret` with
/// [`USERNAME`], from the credential of `api`: the API key, the GitHub
/// Actions OIDC token, or the OAuth access token. A username and password is
/// an error.
pub async fn secret(api: &AntithesisApi) -> Result<String> {
    match api.authentication() {
        AuthenticationInfo::ApiKey { api_key } => Ok(api_key.clone()),
        // The same step as before each API request: a refresh when the
        // credential is expired, then its token.
        AuthenticationInfo::GithubActionsOidc(token) => {
            if token.expired() {
                api.refresh_authentication().await?;
            }
            token
                .token()
                .ok_or_eyre("no GitHub Actions OIDC token was fetched")
        }
        AuthenticationInfo::OAuth(token) => oauth_secret(api, token).await,
        auth @ AuthenticationInfo::Password { .. } => Err(user_error(format!(
            "the Antithesis container registry does not accept a {auth}; set ANTITHESIS_API_KEY, or run `snouty login`"
        ))),
    }
}

/// The OAuth access token of `token`, the credential of `api`.
///
/// A token that expires in more than [`TOKEN_MIN_LIFETIME`] is used as it is.
/// A token that expires sooner, or whose expiry is unknown, is refreshed.
/// When the refresh fails, the stored token is used until it expires.
async fn oauth_secret(api: &AntithesisApi, token: &RefreshableOAuthToken) -> Result<String> {
    let near_expiry = token
        .expires_at()
        .is_none_or(|expires_at| expires_at - Utc::now() <= TOKEN_MIN_LIFETIME);
    if near_expiry && let Err(err) = api.refresh_authentication().await {
        if token.expired() {
            return Err(err);
        }
        // A bare eprintln, not log::warn!: the notice must reach stderr
        // independent of RUST_LOG.
        eprintln!(
            "warning: the OAuth token refresh failed, so the stored token is used until it expires: {err:#}"
        );
    }
    Ok(token.access_token())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ResponseCache;
    use crate::auth::GithubActionsOidcToken;
    use crate::testutils::auth::{oauth_credential_in, paseto_expiring_in};
    use crate::testutils::fake_secrets::{
        FAKE_ACCESS_TOKEN_A, FAKE_ACCESS_TOKEN_B, FAKE_API_KEY_A, FAKE_API_KEY_B,
        FAKE_OIDC_REQUEST_TOKEN, FAKE_OIDC_TOKEN, FAKE_PASSWORD, FAKE_REFRESH_TOKEN_A,
        FAKE_REFRESH_TOKEN_B, FAKE_USERNAME,
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
    async fn the_secret_is_the_api_key() {
        let auth = AuthenticationInfo::ApiKey {
            api_key: FAKE_API_KEY_A.to_owned(),
        };
        assert_eq!(secret(&api(NO_SERVER, auth)).await.unwrap(), FAKE_API_KEY_A);
    }

    #[tokio::test]
    async fn the_secret_is_the_github_actions_oidc_token() {
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
        let auth = AuthenticationInfo::GithubActionsOidc(GithubActionsOidcToken::new(
            format!("{}/token?api-version=2.0", server.uri()),
            FAKE_OIDC_REQUEST_TOKEN.to_owned(),
        ));

        assert_eq!(
            secret(&api(NO_SERVER, auth)).await.unwrap(),
            FAKE_OIDC_TOKEN
        );
    }

    #[tokio::test]
    async fn a_password_credential_is_rejected_with_the_fix() {
        let auth = AuthenticationInfo::Password {
            username: FAKE_USERNAME.to_owned(),
            password: FAKE_PASSWORD.to_owned(),
        };
        let err = secret(&api(NO_SERVER, auth)).await.unwrap_err();
        assert!(err.to_string().contains("set ANTITHESIS_API_KEY"), "{err}");
    }

    #[tokio::test]
    async fn a_token_far_from_expiry_is_used_without_a_refresh() {
        let server = MockServer::start().await;
        mount_refresh(&server, FAKE_ACCESS_TOKEN_A, 0).await;
        let dir = tempfile::TempDir::new().unwrap();
        let fresh = paseto_expiring_in(TimeDelta::minutes(125));
        let auth = oauth_credential_in(dir.path(), &fresh, Some(FAKE_REFRESH_TOKEN_A));

        assert_eq!(secret(&api(&server.uri(), auth)).await.unwrap(), fresh);
    }

    #[tokio::test]
    async fn a_token_that_expires_within_the_margin_is_refreshed() {
        let server = MockServer::start().await;
        mount_refresh(&server, FAKE_ACCESS_TOKEN_A, 1).await;
        let dir = tempfile::TempDir::new().unwrap();
        let auth = oauth_credential_in(
            dir.path(),
            paseto_expiring_in(TimeDelta::minutes(119)),
            Some(FAKE_REFRESH_TOKEN_A),
        );

        let secret = secret(&api(&server.uri(), auth)).await.unwrap();

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
        let auth = oauth_credential_in(dir.path(), FAKE_ACCESS_TOKEN_A, Some(FAKE_REFRESH_TOKEN_A));

        let secret = secret(&api(&server.uri(), auth)).await.unwrap();

        assert_eq!(secret, FAKE_ACCESS_TOKEN_B);
    }

    #[tokio::test]
    async fn without_a_refresh_token_only_an_unexpired_token_is_used() {
        let dir = tempfile::TempDir::new().unwrap();
        for valid in [
            paseto_expiring_in(TimeDelta::days(1000)),
            paseto_expiring_in(TimeDelta::minutes(119)),
        ] {
            let auth = oauth_credential_in(dir.path(), &valid, None);
            assert_eq!(secret(&api(NO_SERVER, auth)).await.unwrap(), valid);
        }

        let auth = oauth_credential_in(dir.path(), paseto_expiring_in(-TimeDelta::days(1)), None);
        let err = secret(&api(NO_SERVER, auth)).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "the stored OAuth token cannot be refreshed, because there is no refresh token; run `snouty login`"
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
        let auth = oauth_credential_in(
            dir.path(),
            paseto_expiring_in(TimeDelta::minutes(119)),
            Some(FAKE_REFRESH_TOKEN_A),
        );

        let secret = secret(&api(&server.uri(), auth)).await.unwrap();

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
        let auth = oauth_credential_in(dir.path(), &valid, Some(FAKE_REFRESH_TOKEN_A));
        assert_eq!(secret(&api(&server.uri(), auth)).await.unwrap(), valid);

        let expired = paseto_expiring_in(-TimeDelta::days(1));
        let auth = oauth_credential_in(dir.path(), &expired, Some(FAKE_REFRESH_TOKEN_A));
        let err = secret(&api(&server.uri(), auth)).await.unwrap_err();
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

    fn registry(value: &str) -> TenantRegistry {
        value.parse().unwrap()
    }

    /// A tenant as a host name spells it: one or more DNS labels.
    fn tenants() -> impl hegel::PrintableGenerator<String> {
        generators::from_regex(
            r"[A-Za-z0-9]{1,5}(-[A-Za-z0-9]{1,4})?(\.[A-Za-z0-9]{1,5}(-[A-Za-z0-9]{1,4})?){0,2}",
        )
        .fullmatch(true)
    }

    /// A scheme, a path, the port 443 and the case of the
    /// letters do not change the registry.
    #[hegel::test]
    fn a_server_url_names_its_tenant_registry(tc: hegel::TestCase) {
        let tenant = tc.draw(tenants());
        let scheme = tc.draw(generators::sampled_from(vec!["", "https://", "http://"]));
        let port = tc.draw(generators::sampled_from(vec!["", ":443"]));
        let path = tc.draw(generators::from_regex(r"(/[a-z0-9/._-]*)?").fullmatch(true));
        let bare = format!("registry.{tenant}.antithesis.com");
        let url = format!("{scheme}{bare}{port}{path}");
        let parsed = registry(&url);
        assert_eq!(parsed.tenant(), tenant.to_ascii_lowercase(), "{url}");
        assert_eq!(parsed.to_string(), bare.to_ascii_lowercase(), "{url}");
        assert_eq!(parsed, registry(&bare.to_ascii_uppercase()));
    }

    #[test]
    fn a_tenant_registry_names_its_tenant() {
        let cases = [
            ("registry.acme.antithesis.com", "acme"),
            ("registry.acme.antithesis.com/team/app", "acme"),
            ("https://REGISTRY.Acme.antithesis.com/v2/", "acme"),
            ("https://registry.acme.antithesis.com:443/", "acme"),
            ("registry.acme.antithesis.com:443", "acme"),
            ("http://registry.acme.antithesis.com:443", "acme"),
            ("registry.eu.acme.antithesis.com", "eu.acme"),
            (" registry.acme.antithesis.com\n", "acme"),
        ];
        for (url, tenant) in cases {
            assert_eq!(registry(url).tenant(), tenant, "{url:?}");
        }
    }

    #[test]
    fn a_server_url_of_another_registry_is_not_a_tenant_registry() {
        for url in [
            "registry.acme.antithesis.com:5000",
            "http://registry.acme.antithesis.com:80/v2/",
            "acme.antithesis.com",
            "registry.antithesis.com",
            "registry..antithesis.com",
            "registry.a..b.antithesis.com",
            "registry.-acme.antithesis.com",
            "docker.acme.antithesis.com",
            "registry.acme.antithesis.com.example.com",
            "registry.acme.example.com",
            "https://index.docker.io/v1/",
            "localhost:5000",
            "127.0.0.1:5000/app",
            "[::1]:5000/app",
            "user@registry.acme.antithesis.com",
            "\\v2",
            "a b",
        ] {
            assert_eq!(
                url.parse::<TenantRegistry>(),
                Err(TenantRegistryError::NotTenantRegistry),
                "{url:?}"
            );
        }
    }

    /// Any text that parses prints as a registry that parses to itself.
    #[hegel::test]
    fn a_tenant_registry_parses_from_its_display(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        let Ok(once) = text.parse::<TenantRegistry>() else {
            return;
        };
        assert_eq!(registry(&once.to_string()), once, "{text:?}");
    }

    #[hegel::test]
    fn a_tenant_registry_round_trips_through_its_display(tc: hegel::TestCase) {
        let tenant = tc.draw(tenants()).to_ascii_lowercase();
        let parsed = registry(&format!("registry.{tenant}.antithesis.com"));
        assert_eq!(parsed.tenant(), tenant);
        assert_eq!(registry(&parsed.to_string()), parsed);
    }

    #[test]
    fn a_server_url_with_no_host_is_an_error() {
        for url in ["", "  ", "https://", "https:///v2/", "/v2/"] {
            assert_eq!(
                url.parse::<TenantRegistry>(),
                Err(TenantRegistryError::InvalidServerUrl),
                "{url:?}"
            );
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
            ("acme", "registry.acme.antithesis.com:443/app", true),
            ("ACME", "registry.acme.antithesis.com/app", true),
            ("eu.acme", "registry.eu.acme.antithesis.com/app", true),
        ];
        for (tenant, repository, matches) in cases {
            assert_eq!(
                tenant_registry(&settings(None, tenant, repository)),
                matches.then(|| registry(&format!("registry.{tenant}.antithesis.com"))),
                "{tenant} {repository}"
            );
        }
        assert_eq!(
            tenant_registry(&Settings::builder().tenant("acme").build()),
            None
        );
    }

    fn api_key(api_key: &str) -> AuthenticationInfo {
        AuthenticationInfo::ApiKey {
            api_key: api_key.to_owned(),
        }
    }

    /// The profile and the API key that the implicit search selects for the
    /// acme registry, from `profiles` and `own_credential`.
    fn select_for_acme(
        profiles: Vec<Settings>,
        own_credential: impl FnMut(Option<&str>) -> Result<Option<AuthenticationInfo>>,
    ) -> Result<Option<(Option<String>, String)>> {
        let selected = first_with_own_credential(
            profiles,
            &registry("registry.acme.antithesis.com"),
            own_credential,
        )?;
        Ok(selected.map(|(settings, credential)| {
            let AuthenticationInfo::ApiKey { api_key } = credential else {
                panic!("not an API key: {credential}");
            };
            (settings.profile().map(str::to_owned), api_key)
        }))
    }

    /// A stored credential for each profile in `stored`, and none for the
    /// other profiles.
    fn stored<'a>(
        stored: &'a [(Option<&'a str>, &'a str)],
    ) -> impl FnMut(Option<&str>) -> Result<Option<AuthenticationInfo>> + 'a {
        |profile| {
            Ok(stored
                .iter()
                .find(|(name, _)| *name == profile)
                .map(|(_, key)| api_key(key)))
        }
    }

    /// The default profile of another tenant, then two acme profiles.
    fn other_then_acme_dev_and_prod() -> Vec<Settings> {
        vec![
            settings(None, "other", "registry.other.antithesis.com/app"),
            settings(Some("acme-dev"), "acme", "registry.acme.antithesis.com/dev"),
            settings(
                Some("acme-prod"),
                "acme",
                "registry.acme.antithesis.com/prod",
            ),
        ]
    }

    #[test]
    fn the_first_matching_profile_with_its_own_credential_wins() {
        let stored = [(None, FAKE_API_KEY_A), (Some("acme-prod"), FAKE_API_KEY_B)];
        assert_eq!(
            select_for_acme(other_then_acme_dev_and_prod(), self::stored(&stored)).unwrap(),
            Some((Some("acme-prod".to_owned()), FAKE_API_KEY_B.to_owned())),
        );
    }

    #[test]
    fn a_matching_default_profile_uses_its_own_credential() {
        let profiles = vec![
            settings(None, "acme", "registry.acme.antithesis.com/app"),
            settings(
                Some("acme-prod"),
                "acme",
                "registry.acme.antithesis.com/prod",
            ),
        ];
        let stored = [(None, FAKE_API_KEY_A), (Some("acme-prod"), FAKE_API_KEY_B)];
        assert_eq!(
            select_for_acme(profiles, self::stored(&stored)).unwrap(),
            Some((None, FAKE_API_KEY_A.to_owned())),
        );
    }

    #[test]
    fn matching_profiles_without_their_own_credential_fail() {
        let stored = [(None, FAKE_API_KEY_A)];
        let err =
            select_for_acme(other_then_acme_dev_and_prod(), self::stored(&stored)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "no credential found; run `snouty login` or set ANTITHESIS_API_KEY"
        );
    }

    #[test]
    fn a_credential_for_every_profile_selects_the_first_match() {
        // As ANTITHESIS_API_KEY does: the environment applies to every profile.
        let every_profile = |_: Option<&str>| Ok(Some(api_key(FAKE_API_KEY_B)));
        assert_eq!(
            select_for_acme(other_then_acme_dev_and_prod(), every_profile).unwrap(),
            Some((Some("acme-dev".to_owned()), FAKE_API_KEY_B.to_owned())),
        );
    }

    #[test]
    fn no_matching_profile_is_not_found() {
        let profiles = vec![settings(None, "other", "registry.other.antithesis.com/app")];
        let stored = [(None, FAKE_API_KEY_A)];
        assert_eq!(
            select_for_acme(profiles, self::stored(&stored)).unwrap(),
            None
        );
    }
}
