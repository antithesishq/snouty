//! Read the checked-out commit and the `origin` remote of a local git
//! checkout, so that `snouty launch` can record them as `vcs.*` parameters.

use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

use color_eyre::eyre::{Report, Result, bail};
use log::debug;

use crate::process::output_with_timeout;

/// git reads only a few small files for each query here.
const GIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The commit that a work tree has checked out.
#[derive(Debug)]
pub struct Head {
    pub commit: String,
    /// `None` when HEAD is detached.
    pub branch: Option<String>,
}

/// Run `git -C <dir> <args>` and return its trimmed stdout. Returns `None` when
/// git fails: `dir` is not in a git work tree, git is not installed, or the
/// query has no answer. No caller can act on the reason, so it goes to the
/// debug log.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(dir).args(args);
    let output = output_with_timeout(cmd, GIT_TIMEOUT)
        .inspect_err(|e| debug!("git {args:?} in {}: {e:#}", dir.display()))
        .ok()?;
    if !output.status.success() {
        debug!(
            "git {args:?} in {} failed: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|s| s.trim().to_owned())
}

/// The checked-out commit, or `None` when the branch has no commit yet.
pub fn head(dir: &Path) -> Option<Head> {
    let out = git(dir, &["rev-parse", "HEAD", "--symbolic-full-name", "HEAD"])?;
    let (commit, full_name) = out.split_once('\n')?;
    Some(Head {
        commit: commit.to_owned(),
        branch: full_name.strip_prefix("refs/heads/").map(str::to_owned),
    })
}

/// The URL of the `origin` remote, or `None` when there is none.
pub fn origin_url(dir: &Path) -> Option<String> {
    git(dir, &["remote", "get-url", "origin"])
}

/// A repository on github.com, parsed from a git remote URL.
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

/// Parses the remote URL forms git accepts: `scheme://[user@]host[:port]/path`
/// and the scp-like `[user@]host:path`.
impl FromStr for GitHubRepo {
    type Err = Report;

    fn from_str(url: &str) -> Result<Self> {
        let (authority, path) = match url.split_once("://") {
            Some((_, rest)) => rest.split_once('/').unwrap_or((rest, "")),
            None => url.split_once(':').unwrap_or((url, "")),
        };
        let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
        let host = host.split_once(':').map_or(host, |(h, _)| h);
        if !host.eq_ignore_ascii_case("github.com") {
            bail!("{url} is not a github.com remote");
        }
        let path = path.trim_matches('/');
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

    /// Every remote URL form git accepts for a github.com repository parses
    /// to the same owner and name.
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
            format!("git://github.com/{owner}/{name}{suffix}"),
            format!("git@github.com:{owner}/{name}{suffix}"),
            format!("github.com:{owner}/{name}{suffix}"),
        ]));
        assert_eq!(
            url.parse::<GitHubRepo>().ok(),
            Some(GitHubRepo { owner, name })
        );
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
            assert!(url.parse::<GitHubRepo>().is_err(), "{url} parsed");
        }
    }
}
