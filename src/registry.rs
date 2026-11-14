//! Container registries: the registry that settings name, the tenant's own
//! registry, `registry.<tenant>.antithesis.com`, and the credential that
//! snouty gives it.
//!
//! The tenant's registry takes HTTP basic auth: the username [`USERNAME`], and an
//! Antithesis token or a GitHub Actions OIDC token as the password. The
//! Antithesis API team gave this contract before the registry went live, so
//! no response from it has been observed yet.

use std::{fmt, path::PathBuf, str::FromStr};

use chrono::{TimeDelta, Utc};
use color_eyre::eyre::{OptionExt, Result};
use url::Url;

use crate::api::AntithesisApi;
use crate::auth::{AuthenticationInfo, PasswordPolicy, RefreshableOAuthToken};
use crate::error::user_error;
use crate::render::sanitize;
use crate::settings::{Settings, SettingsRequest, is_valid_tenant_host};

pub const USERNAME: &str = "snouty-token";

/// A registry client uses one secret for a whole push, so a token that
/// [`secret`] returns must outlive the push.
const TOKEN_MIN_LIFETIME: TimeDelta = TimeDelta::hours(2);

/// A container registry to push images to: a host, with an optional port
/// and path, such as `registry.example.com:5000/team`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registry(Url);

impl Registry {
    /// The host and port, which `docker login` takes.
    pub fn host(&self) -> &str {
        self.0.authority()
    }

    /// The tenant whose registry this is, when the host is
    /// `registry.<tenant>.antithesis.com` with no port other than 443.
    pub fn antithesis_tenant(&self) -> Option<&str> {
        if self.0.port().is_some() {
            return None;
        }
        self.0
            .host_str()?
            .strip_prefix("registry.")?
            .strip_suffix(".antithesis.com")
            .filter(|tenant| is_valid_tenant_host(tenant))
    }
}

/// The registry without a scheme or a trailing `/`, so that `{registry}/`
/// opens an image reference.
impl fmt::Display for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}",
            self.0.authority(),
            self.0.path().trim_end_matches('/')
        )
    }
}

/// Parses a registry as `docker login` does: the whitespace around it and
/// any scheme are removed. The host is lowercase, because a container engine
/// keys credentials by the exact host. The path must be lowercase, because a
/// container engine refuses an image reference with an uppercase path.
impl FromStr for Registry {
    type Err = String;

    fn from_str(given: &str) -> Result<Self, Self::Err> {
        let given = given.trim();
        let invalid = |problem: &str| {
            format!(
                "`{}` is not a container registry: {problem}",
                sanitize(given)
            )
        };
        let rest = given.split_once("://").map_or(given, |(_, rest)| rest);
        if rest.is_empty() || rest.starts_with('/') || rest.contains(char::is_whitespace) {
            return Err(invalid(
                "give a host, optionally with a port and a path, such as `registry.example.com/team`",
            ));
        }
        let mut url =
            Url::parse(&format!("https://{rest}")).map_err(|err| invalid(&err.to_string()))?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid("give only a host, a port and a path"));
        }
        if url.path().contains(|c: char| c.is_ascii_uppercase()) {
            return Err(invalid("the path must be lowercase"));
        }
        // Without a trailing `/`, so that a registry equals the one that its
        // display parses to.
        let path = url.path().trim_end_matches('/').to_owned();
        url.set_path(&path);
        Ok(Self(url))
    }
}

/// The tenant registry that `settings` push to: the `registry` setting, when
/// it is the registry of the tenant of the same settings. `None` when the
/// setting names another registry or is not a registry, or when the tenant
/// or the registry is not set.
pub fn tenant_registry(settings: &Settings) -> Option<&Registry> {
    let registry = settings.registry()?.ok()?;
    registry
        .antithesis_tenant()?
        .eq_ignore_ascii_case(settings.tenant()?)
        .then_some(registry)
}

/// Whether the tenant registry of `settings` is the registry of `tenant`.
fn pushes_to_tenant(settings: &Settings, tenant: &str) -> bool {
    tenant_registry(settings).and_then(Registry::antithesis_tenant) == Some(tenant)
}

/// The settings and the credential for the registry of `tenant`. Settings
/// files resolve as for every snouty command: `--settings` or
/// `SNOUTY_SETTINGS_PATH` takes the place of `./.snouty.toml`, and the global
/// settings file applies.
///
/// 1. When `--profile` or `ANTITHESIS_PROFILE` names a profile, use only that
///    profile, with the credential that every command finds for it. When the
///    profile fails to resolve, fail with its error. When its tenant registry
///    is not the registry of `tenant`, return `None`: do not search the other
///    profiles.
/// 2. Otherwise, find the profiles whose tenant registry is the registry of
///    `tenant`, in the order of [`SettingsRequest::resolve_every_profile`]:
///    the top-level defaults first, then the named profiles. A profile that
///    fails to resolve is skipped. Use the first of them that has its own credential
///    ([`AuthenticationInfo::find_own`]). A named profile does not use the
///    stored credential of the default profile, because that credential can
///    be for another tenant. When no profile matches, return `None`. When
///    profiles match but none has its own credential, fail.
///
/// The search makes no network call.
pub fn select_settings(
    project_settings_path: Option<PathBuf>,
    profile: Option<String>,
    tenant: &str,
) -> Result<Option<(Settings, AuthenticationInfo)>> {
    let request = SettingsRequest::from_flags(project_settings_path, profile)?;
    if request.profile().is_some() {
        let settings = request.resolve()?;
        if !pushes_to_tenant(&settings, tenant) {
            return Ok(None);
        }
        let credential = AuthenticationInfo::for_ambient_configuration(
            settings.profile(),
            PasswordPolicy::Reject,
        )?;
        return Ok(Some((settings, credential)));
    }
    first_with_own_credential(request.resolve_every_profile()?, tenant, |profile| {
        AuthenticationInfo::find_own(profile, PasswordPolicy::Reject)
    })
}

/// The first of `profiles` whose tenant registry is the registry of `tenant`,
/// and for whose profile `own_credential` finds a credential, with that
/// credential.
fn first_with_own_credential(
    profiles: Vec<Settings>,
    tenant: &str,
    mut own_credential: impl FnMut(Option<&str>) -> Result<Option<AuthenticationInfo>>,
) -> Result<Option<(Settings, AuthenticationInfo)>> {
    let mut matched = false;
    for settings in profiles {
        if !pushes_to_tenant(&settings, tenant) {
            continue;
        }
        if let Some(credential) = own_credential(settings.profile())? {
            return Ok(Some((settings, credential)));
        }
        matched = true;
    }
    if matched {
        return Err(user_error(format!(
            "no credential found; {}",
            crate::auth::no_credentials_suggestion(crate::auth::in_github_actions())
        )));
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

/// The registry secret that `snouty launch` presents: [`secret`], or `None`
/// for a username and password, which the registry does not accept. With
/// `None`, the container runtime pushes with its own credentials.
pub async fn launch_secret(api: &AntithesisApi) -> Result<Option<String>> {
    match api.authentication() {
        AuthenticationInfo::Password { .. } => Ok(None),
        AuthenticationInfo::ApiKey { .. }
        | AuthenticationInfo::GithubActionsOidc(_)
        | AuthenticationInfo::OAuth(_) => secret(api).await.map(Some),
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
    async fn launch_presents_no_secret_for_a_password() {
        let auth = AuthenticationInfo::Password {
            username: FAKE_USERNAME.to_owned(),
            password: FAKE_PASSWORD.to_owned(),
        };
        assert_eq!(launch_secret(&api(NO_SERVER, auth)).await.unwrap(), None);
        let auth = AuthenticationInfo::ApiKey {
            api_key: FAKE_API_KEY_A.to_owned(),
        };
        assert_eq!(
            launch_secret(&api(NO_SERVER, auth))
                .await
                .unwrap()
                .as_deref(),
            Some(FAKE_API_KEY_A)
        );
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
        let builder = Settings::builder().tenant(tenant).registry(repository);
        match profile {
            Some(profile) => builder.profile(profile),
            None => builder,
        }
        .build()
    }

    fn registry(value: &str) -> Registry {
        value.parse().unwrap()
    }

    /// A tenant as a host name spells it: one or more DNS labels.
    fn tenants() -> impl hegel::PrintableGenerator<String> {
        generators::from_regex(
            r"[A-Za-z0-9]{1,5}(-[A-Za-z0-9]{1,4})?(\.[A-Za-z0-9]{1,5}(-[A-Za-z0-9]{1,4})?){0,2}",
        )
        .fullmatch(true)
    }

    /// A scheme, a path, the port 443 and the case of the host do not
    /// change the tenant.
    #[hegel::test]
    fn a_tenant_registry_names_its_tenant_in_any_spelling(tc: hegel::TestCase) {
        let tenant = tc.draw(tenants());
        let scheme = tc.draw(generators::sampled_from(vec!["", "https://", "http://"]));
        let port = tc.draw(generators::sampled_from(vec!["", ":443"]));
        let path = tc.draw(generators::from_regex(r"(/[a-z0-9/._-]*)?").fullmatch(true));
        let url = format!("{scheme}registry.{tenant}.antithesis.com{port}{path}");
        let lowercase = tenant.to_ascii_lowercase();
        assert_eq!(
            registry(&url).antithesis_tenant(),
            Some(lowercase.as_str()),
            "{url}"
        );
    }

    #[test]
    fn a_tenant_registry_names_its_tenant() {
        for (url, tenant) in [
            ("registry.acme.antithesis.com", "acme"),
            ("registry.acme.antithesis.com/team/app", "acme"),
            ("https://REGISTRY.Acme.antithesis.com/v2/", "acme"),
            ("https://registry.acme.antithesis.com:443/", "acme"),
            ("registry.acme.antithesis.com:443", "acme"),
            ("http://registry.acme.antithesis.com:443", "acme"),
            ("registry.eu.acme.antithesis.com", "eu.acme"),
            (" registry.acme.antithesis.com\n", "acme"),
        ] {
            assert_eq!(registry(url).antithesis_tenant(), Some(tenant), "{url:?}");
        }
    }

    #[test]
    fn another_registry_names_no_tenant() {
        for url in [
            "registry.acme.antithesis.com:5000",
            "http://registry.acme.antithesis.com:80/v2/",
            "acme.antithesis.com",
            "registry.antithesis.com",
            "registry.-acme.antithesis.com",
            "docker.acme.antithesis.com",
            "registry.acme.antithesis.com.example.com",
            "registry.acme.example.com",
            "https://index.docker.io/v1/",
            "localhost:5000",
            "127.0.0.1:5000/app",
            "[::1]:5000/app",
        ] {
            assert_eq!(registry(url).antithesis_tenant(), None, "{url:?}");
        }
    }

    #[test]
    fn the_tenant_registry_is_the_registry_host_of_that_tenant() {
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
            ("acme", " https://registry.acme.antithesis.com/app/ ", true),
            // Not a registry: a push to it fails.
            ("acme", "registry.acme.antithesis.com/MyTeam", false),
        ];
        for (tenant, repository, matches) in cases {
            let settings = settings(None, tenant, repository);
            assert_eq!(
                tenant_registry(&settings),
                matches.then(|| registry(repository)).as_ref(),
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
        let selected = first_with_own_credential(profiles, "acme", own_credential)?;
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
        // CI runs this test in GitHub Actions, which changes the suggestion.
        let suggestion = crate::auth::no_credentials_suggestion(crate::auth::in_github_actions());
        assert_eq!(
            err.to_string(),
            format!("no credential found; {suggestion}")
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

    #[test]
    fn a_registry_is_its_host_and_its_path() {
        for (given, display, host) in [
            (
                "us-central1-docker.pkg.dev/molten-verve-216720/orbitinghail-repository",
                "us-central1-docker.pkg.dev/molten-verve-216720/orbitinghail-repository",
                "us-central1-docker.pkg.dev",
            ),
            (
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            ),
            (
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/team/app/",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/team/app",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            ),
            ("localhost:5000", "localhost:5000", "localhost:5000"),
            ("foo", "foo", "foo"),
            ("[::1]:5000/a", "[::1]:5000/a", "[::1]:5000"),
            (
                "https://Registry.Example.com/team",
                "registry.example.com/team",
                "registry.example.com",
            ),
            (
                "oci://reg.example.com:443/x",
                "reg.example.com/x",
                "reg.example.com",
            ),
            (
                " HTTP://registry.example.com:5000/team//\n",
                "registry.example.com:5000/team",
                "registry.example.com:5000",
            ),
        ] {
            let parsed = registry(given);
            assert_eq!(parsed.to_string(), display, "{given:?}");
            assert_eq!(parsed.host(), host, "{given:?}");
        }
    }

    #[test]
    fn registry_refuses_what_is_not_a_registry() {
        for (given, problem) in [
            ("", "give a host"),
            (" ", "give a host"),
            ("https://", "give a host"),
            ("/team", "give a host"),
            ("registry example.com", "give a host"),
            ("ghcr.io/MyOrg", "the path must be lowercase"),
            ("ghcr.io/team/App", "the path must be lowercase"),
            ("u:p@registry.example.com", "only a host, a port and a path"),
            (
                "registry.example.com/team?q",
                "only a host, a port and a path",
            ),
            (
                "registry.example.com/team#f",
                "only a host, a port and a path",
            ),
            ("registry.example.com:99999", "invalid port"),
            ("registry.example.com:http", "invalid port"),
        ] {
            let err = given.parse::<Registry>().unwrap_err();
            assert!(err.contains(problem), "{given:?}: {err}");
        }
    }

    /// Any text that parses prints as a registry that parses to itself.
    #[hegel::test]
    fn a_registry_parses_from_its_display(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        let Ok(once) = text.parse::<Registry>() else {
            return;
        };
        assert_eq!(once.to_string().parse::<Registry>(), Ok(once), "{text:?}");
    }
}
