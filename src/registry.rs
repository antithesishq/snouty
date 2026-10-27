//! The tenant's container registry, `registry.<tenant>.antithesis.com`, and
//! the credential that snouty gives it.
//!
//! The registry takes HTTP basic auth: the username [`USERNAME`], and an
//! Antithesis token as the password. The Antithesis API team gave this
//! contract before the registry went live, so no response from it has been
//! observed yet.

use std::{fmt, str::FromStr};

use color_eyre::{Section, eyre::Result};

use crate::{error::user_error, settings::Settings};

/// The basic-auth username for every registry credential.
pub const USERNAME: &str = "snouty-token";

/// A registry host, such as `registry.acme.antithesis.com`, or
/// `localhost:5000` with its port. It holds no scheme and no path, and its
/// letters are lowercase, because a host name ignores case.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RegistryHost(String);

impl RegistryHost {
    /// The registry of `tenant`.
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
        let server_url = server_url.trim();
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

/// The settings whose tenant registry is `host`, from the `active` settings
/// (of the selected profile) or else the settings of `every` profile (see
/// [`Settings::resolve_every_profile`]), which load only when `active` does
/// not match.
///
/// The active profile wins when it matches. Then the top-level defaults win,
/// because a named profile that sets no tenant and no repository gets them
/// from the defaults. Then exactly one named profile can match. When more
/// than one does, the choice is the user's, and this is an error.
pub fn select_profile(
    active: Settings,
    every: impl FnOnce() -> Result<Vec<Settings>>,
    host: &RegistryHost,
) -> Result<Option<Settings>> {
    if tenant_registry(&active).as_ref() == Some(host) {
        return Ok(Some(active));
    }
    let mut matching: Vec<Settings> = every()?
        .into_iter()
        .filter(|settings| tenant_registry(settings).as_ref() == Some(host))
        .collect();
    if let Some(defaults) = matching
        .iter()
        .position(|settings| settings.profile().is_none())
    {
        return Ok(Some(matching.swap_remove(defaults)));
    }
    if matching.len() > 1 {
        let names: Vec<&str> = matching.iter().filter_map(Settings::profile).collect();
        return Err(user_error(format!(
            "more than one profile uses the registry {host}: {}",
            names.join(", ")
        ))
        .suggestion("set ANTITHESIS_PROFILE to the profile to use for this registry"));
    }
    Ok(matching.pop())
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

    /// The name of the selected profile, `defaults` for the top-level
    /// defaults.
    fn selected(active: Settings, every: Vec<Settings>, requested: &str) -> Result<Option<String>> {
        Ok(select_profile(active, || Ok(every), &host(requested))?
            .map(|settings| settings.profile().unwrap_or("defaults").to_owned()))
    }

    #[test]
    fn the_active_profile_wins_when_it_matches() {
        let active = settings(Some("b"), "acme", "registry.acme.antithesis.com/b");
        let every = vec![
            settings(None, "acme", "registry.acme.antithesis.com/x"),
            settings(Some("a"), "acme", "registry.acme.antithesis.com/a"),
        ];
        assert_eq!(
            selected(active, every, "registry.acme.antithesis.com").unwrap(),
            Some("b".to_owned())
        );
    }

    #[test]
    fn the_defaults_win_over_named_profiles() {
        let active = settings(Some("other"), "other", "registry.other.antithesis.com/x");
        let every = vec![
            settings(None, "acme", "registry.acme.antithesis.com/x"),
            settings(Some("a"), "acme", "registry.acme.antithesis.com/x"),
        ];
        assert_eq!(
            selected(active, every, "registry.acme.antithesis.com").unwrap(),
            Some("defaults".to_owned())
        );
    }

    #[test]
    fn one_matching_named_profile_is_selected() {
        let active = settings(None, "other", "registry.other.antithesis.com/x");
        let every = vec![
            settings(None, "other", "registry.other.antithesis.com/x"),
            settings(Some("a"), "acme", "registry.acme.antithesis.com/x"),
            settings(Some("b"), "beta", "registry.beta.antithesis.com/x"),
        ];
        assert_eq!(
            selected(active, every, "https://registry.acme.antithesis.com/v2/").unwrap(),
            Some("a".to_owned())
        );
    }

    #[test]
    fn two_matching_named_profiles_are_an_error_that_names_them() {
        let active = settings(None, "other", "registry.other.antithesis.com/x");
        let every = vec![
            settings(Some("a"), "acme", "registry.acme.antithesis.com/a"),
            settings(Some("b"), "acme", "registry.acme.antithesis.com/b"),
        ];
        let err = selected(active, every, "registry.acme.antithesis.com").unwrap_err();
        assert!(err.to_string().ends_with(": a, b"), "{err}");
    }

    #[test]
    fn no_matching_profile_is_none() {
        let active = settings(None, "acme", "us-docker.pkg.dev/acme/repo");
        assert_eq!(
            selected(active, vec![], "registry.acme.antithesis.com").unwrap(),
            None
        );
    }
}
