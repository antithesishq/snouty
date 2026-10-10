//! Reads the checked-out commit and the `origin` remote of a local git checkout.

use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

use color_eyre::eyre::{Report, Result, bail};
use log::debug;

use crate::process::output_with_timeout;

const GIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The commit that a work tree has checked out.
#[derive(Debug)]
pub struct Head {
    pub commit: String,
    /// `None` when HEAD is detached.
    pub branch: Option<String>,
}

/// Returns the trimmed stdout of git, or `None` when git fails. The failure
/// reason goes to the debug log, because no caller can act on it.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let mut cmd = Command::new("git");
    // A git hook exports these, and they take precedence over `-C`.
    cmd.env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .arg("-C")
        .arg(dir)
        .args(args);
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

/// The commit checked out in the work tree that holds `dir`. `None` outside a
/// work tree, or on a branch that has no commit yet.
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

/// Parses a GitHub remote URL in either form git accepts:
/// `scheme://[user@]host[:port]/owner/name` or scp-like `[user@]host:owner/name`.
/// Fails for any other host.
impl FromStr for GitHubRepo {
    type Err = Report;

    fn from_str(url: &str) -> Result<Self> {
        let (authority, path) = match url.split_once("://") {
            Some((_, rest)) => rest.split_once('/').unwrap_or((rest, "")),
            None => url.split_once(':').unwrap_or((url, "")),
        };
        let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
        let host = host.split_once(':').map_or(host, |(h, _)| h);
        // GitHub serves SSH on port 443 at ssh.github.com.
        if !["github.com", "ssh.github.com"]
            .iter()
            .any(|h| host.eq_ignore_ascii_case(h))
        {
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
