//! Read the checked-out commit and the `origin` remote of a local git
//! checkout, so that `snouty launch` can record them as `vcs.*` parameters.

use std::fmt;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

use color_eyre::eyre::{Report, Result, bail, eyre};
use log::debug;

use crate::process::output_with_timeout;

/// git reads only a few small files for each query here.
const GIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The commit that a work tree has checked out.
#[derive(Debug, PartialEq, Eq)]
pub struct Head {
    pub commit: String,
    /// `None` when HEAD is detached.
    pub branch: Option<String>,
}

/// Run `git -C <dir> <args>` and return its trimmed stdout.
fn git(dir: &Path, args: &[&str]) -> Result<String> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(dir).args(args);
    let output = output_with_timeout(cmd, GIT_TIMEOUT)?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout)
        .map(|s| s.trim().to_owned())
        .map_err(|_| eyre!("git {} printed non-UTF-8 output", args.join(" ")))
}

/// `git rev-parse HEAD --symbolic-full-name HEAD`. Returns `None` when `dir`
/// is not in a git work tree, git is not installed, or the branch has no
/// commit yet. The caller only skips the `vcs.*` parameters then, so the error
/// goes to the debug log.
pub fn head(dir: &Path) -> Option<Head> {
    let out = git(dir, &["rev-parse", "HEAD", "--symbolic-full-name", "HEAD"])
        .inspect_err(|e| debug!("no git commit in {}: {e:#}", dir.display()))
        .ok()?;
    let (commit, full_name) = out.split_once('\n')?;
    Some(Head {
        commit: commit.to_owned(),
        branch: full_name.strip_prefix("refs/heads/").map(str::to_owned),
    })
}

/// `git remote get-url origin`. Returns `None` when there is no `origin`
/// remote; the error goes to the debug log for the same reason as in [`head`].
pub fn origin_url(dir: &Path) -> Option<String> {
    git(dir, &["remote", "get-url", "origin"])
        .inspect_err(|e| debug!("no git origin in {}: {e:#}", dir.display()))
        .ok()
}

/// A repository on github.com, parsed from a git remote URL.
#[derive(Debug, PartialEq, Eq)]
pub struct GitHubRepo {
    pub owner: String,
    pub name: String,
}

impl GitHubRepo {
    pub fn commit_url(&self, commit: &str) -> String {
        format!("{self}/commit/{commit}")
    }
}

impl fmt::Display for GitHubRepo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "https://github.com/{}/{}", self.owner, self.name)
    }
}

/// Parses the remote URL forms git accepts: `scheme://[user@]host[:port]/path`
/// and the scp-like `[user@]host:path`.
impl FromStr for GitHubRepo {
    type Err = Report;

    fn from_str(url: &str) -> Result<Self> {
        let (host, path) = match url.split_once("://") {
            Some((_, rest)) => {
                let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
                let host = authority.rsplit('@').next().unwrap_or_default();
                (host.split(':').next().unwrap_or_default(), path)
            }
            None => {
                let (authority, path) = url.split_once(':').unwrap_or((url, ""));
                (authority.rsplit('@').next().unwrap_or_default(), path)
            }
        };
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

    #[test]
    fn commit_url_points_at_the_commit_on_github() {
        let repo: GitHubRepo = "git@github.com:acme/widget.git".parse().unwrap();
        assert_eq!(
            repo.commit_url("abc123"),
            "https://github.com/acme/widget/commit/abc123"
        );
    }
}
