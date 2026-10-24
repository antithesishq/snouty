//! Reads the checked-out commit and the `origin` remote of a local git checkout.

use std::path::Path;

use color_eyre::eyre::{Report, Result, bail};
use gix::refs::Category;
use log::debug;

/// The state of the git checkout that holds a directory.
#[derive(Debug)]
pub struct Checkout {
    pub commit: String,
    /// `None` when HEAD is not on a local branch, for example when it is detached.
    pub branch: Option<String>,
    /// `None` when there is no `origin` remote.
    pub origin: Option<gix::Url>,
}

/// Reads the git checkout that holds `dir`. `None` outside a checkout, or on a
/// branch that has no commit yet. The reason goes to the debug log, because no
/// caller can act on it.
pub fn checkout(dir: &Path) -> Option<Checkout> {
    let read = || -> Result<Checkout> {
        let repo = gix::discover(dir)?;
        let commit = repo.head_id()?.to_string();
        let branch = repo
            .head_name()?
            .and_then(|name| match name.category_and_short_name()? {
                (Category::LocalBranch, short) => Some(short.to_string()),
                _ => None,
            });
        let origin = repo
            .find_remote("origin")
            .ok()
            .and_then(|remote| remote.url(gix::remote::Direction::Fetch).cloned());
        Ok(Checkout {
            commit,
            branch,
            origin,
        })
    };
    read()
        .inspect_err(|e| debug!("no git checkout at {}: {e:#}", dir.display()))
        .ok()
}

/// A repository on github.com.
#[derive(Debug, PartialEq, Eq)]
pub struct GitHubRepo {
    pub owner: String,
    pub name: String,
}

impl GitHubRepo {
    pub fn commit_url(&self, commit: &str) -> String {
        format!(
            "https://github.com/{}/{}/commit/{commit}",
            self.owner, self.name
        )
    }
}

/// Fails for a remote on any host other than GitHub.
impl TryFrom<&gix::Url> for GitHubRepo {
    type Error = Report;

    fn try_from(url: &gix::Url) -> Result<Self> {
        // GitHub serves SSH on port 443 at ssh.github.com.
        if !url.host().is_some_and(|host| {
            host.eq_ignore_ascii_case("github.com") || host.eq_ignore_ascii_case("ssh.github.com")
        }) {
            bail!("{url} is not a github.com remote");
        }
        let path = std::str::from_utf8(&url.path)?.trim_matches('/');
        let path = path.strip_suffix(".git").unwrap_or(path);
        match path.split_once('/') {
            Some((owner, name)) if !owner.is_empty() && !name.is_empty() && !name.contains('/') => {
                Ok(Self {
                    owner: owner.to_owned(),
                    name: name.to_owned(),
                })
            }
            _ => bail!("{url} does not name a github.com repository"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::Generator;
    use hegel::generators;

    fn github_repo(url: &str) -> Result<GitHubRepo> {
        GitHubRepo::try_from(&gix::url::parse(url)?)
    }

    // `.git` ends a URL path, and the parser strips it case-sensitively.
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    #[hegel::test]
    fn github_remote_urls_parse_in_every_form(tc: hegel::TestCase) {
        let owner = tc.draw(generators::from_regex("[A-Za-z0-9-]{1,39}").fullmatch(true));
        let name = tc.draw(
            generators::from_regex("[A-Za-z0-9._-]{1,100}")
                .fullmatch(true)
                .filter(|n: &String| !n.ends_with(".git")),
        );
        let suffix = tc.draw(generators::sampled_from(vec!["", ".git", "/", ".git/"]));
        let url = tc.draw(generators::sampled_from(vec![
            format!("https://github.com/{owner}/{name}{suffix}"),
            format!("https://user@github.com/{owner}/{name}{suffix}"),
            format!("ssh://git@github.com/{owner}/{name}{suffix}"),
            format!("ssh://git@github.com:22/{owner}/{name}{suffix}"),
            format!("ssh://git@ssh.github.com:443/{owner}/{name}{suffix}"),
            format!("git://github.com/{owner}/{name}{suffix}"),
            format!("git@github.com:{owner}/{name}{suffix}"),
            format!("github.com:{owner}/{name}{suffix}"),
        ]));
        assert_eq!(github_repo(&url).ok(), Some(GitHubRepo { owner, name }));
    }

    #[test]
    fn non_github_remotes_do_not_parse() {
        for url in [
            "https://gitlab.com/acme/widget.git",
            "git@github.example.com:acme/widget.git",
            "https://github.com.example.com/acme/widget",
            "/srv/git/github.com:acme/widget",
            "https://github.com/acme",
            "https://github.com/acme/widget/tree/main",
            "git@github.com:acme",
        ] {
            assert!(github_repo(url).is_err(), "{url} parsed");
        }
    }
}
