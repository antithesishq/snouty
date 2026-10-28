//! The tenant's container registry, `registry.<tenant>.antithesis.com`, and
//! the credential that snouty gives it.
//!
//! The registry takes HTTP basic auth: the username [`USERNAME`], and an
//! Antithesis token as the password. The Antithesis API team gave this
//! contract before the registry went live, so no response from it has been
//! observed yet.

use std::{fmt, path::PathBuf, str::FromStr};

use color_eyre::eyre::Result;

use crate::settings::{Settings, SettingsRequest};

pub const USERNAME: &str = "snouty-token";

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

/// The settings that give the credential for `host`. Exactly two rules
/// select them:
///
/// 1. Explicit. When `--settings`, `SNOUTY_SETTINGS_PATH`, `--profile` or
///    `ANTITHESIS_PROFILE` names a settings file or a profile, use only the
///    settings that they resolve to, as for every snouty command. When the
///    resolution fails, fail with its error. When the tenant registry of
///    those settings is not `host`, return `None`: do not search the other
///    profiles.
/// 2. Implicit. Otherwise, use the first profile whose tenant registry is
///    `host`, in the order of [`Settings::resolve_every_profile`]: the
///    top-level defaults first, then the named profiles. When more than one
///    profile matches, the first one wins. A profile that fails to resolve
///    is skipped. When no profile matches, return `None`.
pub fn select_settings(
    project_settings_path: Option<PathBuf>,
    profile: Option<String>,
    host: &RegistryHost,
) -> Result<Option<Settings>> {
    let uses_host = |settings: &Settings| tenant_registry(settings).as_ref() == Some(host);
    let request = SettingsRequest::from_flags(project_settings_path, profile)?;
    if request.is_explicit() {
        return Ok(Some(request.resolve()?).filter(uses_host));
    }
    Ok(Settings::resolve_every_profile()?
        .into_iter()
        .find(uses_host))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;

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
