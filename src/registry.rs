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
use color_eyre::eyre::{OptionExt, Report, Result, eyre};
use url::Url;

use crate::api::AntithesisApi;
use crate::auth::{AuthenticationInfo, PasswordPolicy, RefreshableOAuthToken, RenewalError};
use crate::error::user_error;
use crate::render::sanitize;
use crate::settings::{Settings, SettingsRequest, Tenant, UnresolvedProfile};
use crate::tag::Tagged;

pub const USERNAME: &str = "snouty-token";

/// A registry client uses one secret for a whole push, so a token that
/// [`secret`] returns must outlive the push.
const TOKEN_MIN_LIFETIME: TimeDelta = TimeDelta::hours(2);

/// A container registry to push images to: a host, with an optional port
/// and path, such as `registry.example.com:5000/team`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registry(Url);

impl Registry {
    /// The host and the port, which `docker login` takes.
    pub fn authority(&self) -> &str {
        self.0.authority()
    }

    /// The tenant whose registry this is, when the host is
    /// `registry.<tenant>.antithesis.com`.
    pub fn antithesis_tenant(&self) -> Option<Tenant> {
        self.0
            .domain()?
            .strip_prefix("registry.")?
            .strip_suffix(".antithesis.com")?
            .parse()
            .ok()
    }

    /// Whether `self` is `other` or a path under it.
    pub fn is_within(&self, other: &Registry) -> bool {
        let path = self.0.path().trim_end_matches('/');
        let other_path = other.0.path().trim_end_matches('/');
        self.authority() == other.authority()
            && path
                .strip_prefix(other_path)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    }
}

/// The registry without a scheme, so that `{registry}/` opens an image
/// reference.
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

/// Parses a registry as `docker login` does: the whitespace around it, one
/// scheme and any trailing `/` are removed. url normalizes the rest, for
/// example to drop the default port 443, and the result is lowercase.
impl FromStr for Registry {
    type Err = Report;

    fn from_str(given: &str) -> Result<Self> {
        let given = given.trim();
        let invalid = |problem: &str| {
            eyre!(
                "`{}` is not a container registry: {problem}",
                sanitize(given)
            )
        };
        let rest = given.split_once("://").map_or(given, |(_, rest)| rest);
        if rest.contains("://") {
            return Err(invalid("give at most one scheme"));
        }
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
        // url lowercases the host, and leaves the path percent-encoded ASCII.
        url.set_path(&url.path().trim_end_matches('/').to_ascii_lowercase());
        Ok(Self(url))
    }
}

/// The tenant registry that `settings` push to: the `registry` setting, when
/// it is the registry of the tenant of the same settings. `None` when the
/// setting names another registry, or when the tenant or the registry is not
/// set.
pub fn tenant_registry(settings: &Settings) -> Option<&Registry> {
    tenant_registry_of(settings.tenant(), settings.registry().map(Tagged::value))
}

/// [`tenant_registry`] of the `tenant` and `registry` settings.
fn tenant_registry_of<'a>(
    tenant: Option<&Tenant>,
    registry: Option<&'a Registry>,
) -> Option<&'a Registry> {
    let registry = registry?;
    registry
        .antithesis_tenant()?
        .eq_ignore_ascii_case(tenant?)
        .then_some(registry)
}

/// Whether the tenant registry of `settings` is the registry of `tenant`.
fn pushes_to_tenant(settings: &Settings, tenant: &str) -> bool {
    pushes_to(
        settings.tenant(),
        settings.registry().map(Tagged::value),
        tenant,
    )
}

/// [`pushes_to_tenant`] of the `tenant_setting` and `registry_setting`.
fn pushes_to(
    tenant_setting: Option<&Tenant>,
    registry_setting: Option<&Registry>,
    tenant: &str,
) -> bool {
    tenant_registry_of(tenant_setting, registry_setting)
        .and_then(Registry::antithesis_tenant)
        .is_some_and(|own| *own == *tenant)
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
///    the top-level defaults first, then the named profiles. A profile whose
///    tenant or registry fails to resolve is skipped. Use the first of them
///    that has its own credential ([`AuthenticationInfo::find_own`]); when
///    that profile fails to resolve, fail with its error. A named profile does not use the
///    stored credential of the default profile, because that credential can
///    be for another tenant. When no profile matches, return `None`. When
///    profiles match but none has its own credential, fail: with the error
///    of the first matching profile that fails to resolve, if any.
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
        )?
        .untag();
        return Ok(Some((settings, credential)));
    }
    let profiles = request.resolve_every_profile(|tenant_setting, registry_setting| {
        pushes_to(tenant_setting, registry_setting, tenant)
    })?;
    first_with_own_credential(profiles, tenant, |profile| {
        AuthenticationInfo::find_own(profile, PasswordPolicy::Reject)
    })
}

/// The first of `profiles` whose tenant registry is the registry of `tenant`,
/// and for whose profile `own_credential` finds a credential, with that
/// credential. A profile that failed to resolve but has a credential gives
/// its error. When no profile has a credential, the error of the first
/// profile that failed to resolve.
fn first_with_own_credential(
    profiles: Vec<Result<Settings, UnresolvedProfile>>,
    tenant: &str,
    mut own_credential: impl FnMut(Option<&str>) -> Result<Option<AuthenticationInfo>>,
) -> Result<Option<(Settings, AuthenticationInfo)>> {
    let mut matched = false;
    let mut first_error = None;
    for settings in profiles {
        let settings = match settings {
            Ok(settings) => settings,
            Err(UnresolvedProfile { profile, error }) => {
                if own_credential(profile.as_deref())?.is_some() {
                    return Err(error);
                }
                first_error.get_or_insert(error);
                continue;
            }
        };
        if !pushes_to_tenant(&settings, tenant) {
            continue;
        }
        if let Some(credential) = own_credential(settings.profile())? {
            return Ok(Some((settings, credential)));
        }
        matched = true;
    }
    if let Some(err) = first_error {
        return Err(err);
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
        // The message of a renewal error says that the sign-in expired,
        // which is not true yet, so only its next step shows.
        let next_step = err
            .downcast_ref::<RenewalError>()
            .map_or_else(|| format!("{err:#}"), RenewalError::next_step);
        // A bare eprintln, not log::warn!: the notice must reach stderr
        // independent of RUST_LOG.
        eprintln!(
            "warning: the sign-in could not be renewed, so the stored token is used until it expires; {next_step}"
        );
    }
    Ok(token.access_token())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ResponseCache;
    use crate::auth::GithubActionsOidcToken;
    use crate::settings::{ANTITHESIS_REGISTRY_VAR_NAME, SettingSource};
    use crate::tag::Tag;
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
            "the sign-in expired and could not be renewed; run `snouty login` to sign in again"
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

    fn settings(profile: Option<&str>, tenant: &str, registry_setting: &str) -> Settings {
        let builder = Settings::builder()
            .tenant(tenant.parse().unwrap())
            .registry(
                registry(registry_setting)
                    .with_tag(SettingSource::Env(ANTITHESIS_REGISTRY_VAR_NAME)),
            );
        match profile {
            Some(profile) => builder.profile(profile),
            None => builder,
        }
        .build()
    }

    fn registry(value: &str) -> Registry {
        value.parse().unwrap()
    }

    /// A tenant as a lowercase host name spells it: one or more DNS labels.
    fn tenants() -> impl hegel::PrintableGenerator<String> {
        generators::from_regex(
            r"[a-z0-9]{1,5}(-[a-z0-9]{1,4})?(\.[a-z0-9]{1,5}(-[a-z0-9]{1,4})?){0,2}",
        )
        .fullmatch(true)
    }

    /// A scheme and a path do not change the tenant.
    #[hegel::test]
    fn a_tenant_registry_names_its_tenant_in_any_spelling(tc: hegel::TestCase) {
        let tenant = tc.draw(tenants());
        let scheme = tc.draw(generators::sampled_from(vec!["", "https://", "http://"]));
        let path = tc.draw(generators::from_regex(r"(/[a-z0-9_-]+)*/?").fullmatch(true));
        let url = format!("{scheme}registry.{tenant}.antithesis.com{path}");
        assert_eq!(
            registry(&url).antithesis_tenant().as_deref(),
            Some(tenant.as_str()),
            "{url}"
        );
    }

    #[test]
    fn a_tenant_registry_names_its_tenant() {
        for (url, tenant) in [
            ("registry.acme.antithesis.com", "acme"),
            ("registry.acme.antithesis.com/team/app", "acme"),
            ("https://registry.acme.antithesis.com/v2/", "acme"),
            ("REGISTRY.Acme.Antithesis.COM/App", "acme"),
            ("registry.acme.antithesis.com:5000", "acme"),
            ("registry.eu.acme.antithesis.com", "eu.acme"),
            (" registry.acme.antithesis.com\n", "acme"),
        ] {
            assert_eq!(
                registry(url).antithesis_tenant().as_deref(),
                Some(tenant),
                "{url:?}"
            );
        }
    }

    #[test]
    fn another_registry_names_no_tenant() {
        for url in [
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
            assert_eq!(
                registry(url).antithesis_tenant().as_deref(),
                None,
                "{url:?}"
            );
        }
    }

    #[test]
    fn the_tenant_registry_is_the_registry_host_of_that_tenant() {
        let cases = [
            ("acme", "registry.acme.antithesis.com/app", true),
            ("acme", "registry.acme.antithesis.com", true),
            ("acme", "REGISTRY.acme.antithesis.com/MyTeam", true),
            ("acme", "registry.acme.antithesis.com:5000/app", true),
            ("acme", "registry.other.antithesis.com/app", false),
            ("acme", "us-docker.pkg.dev/acme/repo", false),
            ("ACME", "registry.acme.antithesis.com/app", true),
            ("eu.acme", "registry.eu.acme.antithesis.com/app", true),
            ("acme", " https://registry.acme.antithesis.com/app/ ", true),
        ];
        for (tenant, registry_setting, matches) in cases {
            let settings = settings(None, tenant, registry_setting);
            assert_eq!(
                tenant_registry(&settings),
                matches.then(|| registry(registry_setting)).as_ref(),
                "{tenant} {registry_setting}"
            );
        }
        assert_eq!(
            tenant_registry(&Settings::builder().tenant("acme".parse().unwrap()).build()),
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
        let profiles = profiles.into_iter().map(Ok).collect();
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

    /// A matching profile that fails to resolve gives its error when it has
    /// its own credential. Without one, it does not hide a later profile with
    /// a credential, and its error shows when no profile has one.
    #[test]
    fn a_broken_profile_gives_its_error_when_it_has_a_credential_or_no_profile_does() {
        let profiles = || {
            vec![
                Err(UnresolvedProfile {
                    profile: Some("acme-dev".to_owned()),
                    error: eyre!("broken acme-dev"),
                }),
                Ok(settings(
                    Some("acme-prod"),
                    "acme",
                    "registry.acme.antithesis.com/prod",
                )),
            ]
        };
        let select = |stored: &[(Option<&str>, &str)]| {
            first_with_own_credential(profiles(), "acme", self::stored(stored))
                .map(|selected| {
                    selected.and_then(|(settings, _)| settings.profile().map(str::to_owned))
                })
                .map_err(|err| err.to_string())
        };
        assert_eq!(
            select(&[(Some("acme-prod"), FAKE_API_KEY_B)]),
            Ok(Some("acme-prod".to_owned()))
        );
        assert_eq!(
            select(&[
                (Some("acme-dev"), FAKE_API_KEY_A),
                (Some("acme-prod"), FAKE_API_KEY_B)
            ]),
            Err("broken acme-dev".to_owned())
        );
        assert_eq!(select(&[]), Err("broken acme-dev".to_owned()));
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

    /// The registry forms in use: an ECR host, a GAR path, and an
    /// Antithesis tenant registry.
    #[test]
    fn a_registry_is_its_authority_and_its_path() {
        const ECR: &str = "123456789012.dkr.ecr.us-east-1.amazonaws.com";
        const GAR: &str = "us-central1-docker.pkg.dev";
        const GAR_REPOSITORY: &str = "us-central1-docker.pkg.dev/my-project/my-repo";
        const TENANT: &str = "registry.acme.antithesis.com";
        for (given, display, authority) in [
            (ECR, ECR, ECR),
            (
                "https://123456789012.dkr.ecr.us-east-1.amazonaws.com/",
                ECR,
                ECR,
            ),
            ("123456789012.DKR.ECR.US-EAST-1.amazonaws.com", ECR, ECR),
            (
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/team/app/",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/team/app",
                ECR,
            ),
            (GAR, GAR, GAR),
            (GAR_REPOSITORY, GAR_REPOSITORY, GAR),
            (
                "https://us-central1-docker.pkg.dev/my-project/my-repo/",
                GAR_REPOSITORY,
                GAR,
            ),
            (
                "US-Central1-Docker.pkg.dev/My-Project/My-Repo",
                GAR_REPOSITORY,
                GAR,
            ),
            (TENANT, TENANT, TENANT),
            ("HTTPS://Registry.Acme.Antithesis.com/", TENANT, TENANT),
            (
                "registry.acme.antithesis.com/team",
                "registry.acme.antithesis.com/team",
                TENANT,
            ),
            ("localhost:5000", "localhost:5000", "localhost:5000"),
            ("foo", "foo", "foo"),
            ("[::1]:5000/a", "[::1]:5000/a", "[::1]:5000"),
            (
                "oci://reg.example.com/x",
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
            assert_eq!(parsed.authority(), authority, "{given:?}");
        }
    }

    /// url normalizes a value in other spellings, which can then look
    /// different from what the user wrote.
    #[test]
    fn a_registry_takes_the_spelling_of_url() {
        for (given, display) in [
            ("reg.example.com:443/x", "reg.example.com/x"),
            ("reg.example.com:/x", "reg.example.com/x"),
            ("0x7f.1:5000", "127.0.0.1:5000"),
            ("bücher.example/app", "xn--bcher-kva.example/app"),
            ("reg.example.com/a/../b", "reg.example.com/b"),
        ] {
            assert_eq!(registry(given).to_string(), display, "{given:?}");
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
            ("https://https://x.example.com", "give at most one scheme"),
        ] {
            let err = given.parse::<Registry>().unwrap_err().to_string();
            assert!(err.contains(problem), "{given:?}: {err}");
        }
    }

    #[test]
    fn a_registry_is_within_itself_or_a_registry_above_its_path() {
        let within = |registry: &str, other: &str| {
            registry
                .parse::<Registry>()
                .unwrap()
                .is_within(&other.parse().unwrap())
        };
        let tenant = "registry.acme.antithesis.com";
        assert!(within(tenant, tenant));
        assert!(within("registry.acme.antithesis.com/", tenant));
        assert!(within("https://registry.acme.antithesis.com", tenant));
        assert!(within(tenant, "https://registry.acme.antithesis.com/"));
        assert!(!within("registry.acme.antithesis.com:5000/team", tenant));
        assert!(within("registry.acme.antithesis.com/team/app", tenant));
        assert!(!within("registry.other.antithesis.com", tenant));
        assert!(!within(
            "proxy.example.com/registry.acme.antithesis.com",
            tenant
        ));
        assert!(!within("registry.acme.antithesis.com.evil", tenant));

        let tenant = "registry.example.com/acme";
        assert!(within("registry.example.com/acme/app", tenant));
        assert!(!within("registry.example.com/acmes", tenant));
        assert!(!within("registry.example.com", tenant));
        assert!(!within("registry.example.com/other", tenant));
    }

    /// Any text that parses prints as a registry that parses to itself.
    #[hegel::test]
    fn a_registry_parses_from_its_display(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        let Ok(once) = text.parse::<Registry>() else {
            return;
        };
        assert_eq!(
            once.to_string().parse::<Registry>().unwrap(),
            once,
            "{text:?}"
        );
    }
}
