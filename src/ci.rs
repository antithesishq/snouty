//! Reads the GitHub Actions build that runs snouty from the environment
//! variables and the event payload that GitHub Actions sets.

use color_eyre::eyre::Result;
use log::debug;
use serde::Deserialize;

use crate::git::{Checkout, GitHubRepo};
use crate::params::{
    ANT_GITHUB_CALLBACK_URL, ANT_GITHUB_TOKEN, Params, RUN_CREATOR_NAME, RUN_CRON_SCHEDULE,
    VCS_PR_ID, VCS_PR_LINK, VCS_PR_OWNER, VCS_PR_TITLE, VCS_REPO_BRANCH, VCS_REPO_NAME,
    VCS_REPO_OWNER, VCS_REPO_TYPE, VCS_VERSION_ID, VCS_VERSION_LINK,
};

const GITHUB_TOKEN_VAR_NAME: &str = "ANTITHESIS_GITHUB_TOKEN";

/// A slow GitHub must not keep the launch from printing the run ID.
const STATUS_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The commit status context without a test name. The Antithesis GitHub
/// integration posts its result to the same context.
const STATUS_CONTEXT: &str = "continuous-testing/antithesis";

/// The build of one commit, by GitHub Actions or from a local git checkout.
#[derive(Debug, PartialEq, Eq)]
pub struct Build {
    /// A full commit hash.
    commit: String,
    /// The merge commit that GitHub Actions checks out for a pull request in
    /// place of `commit`.
    merge_commit: Option<String>,
    branch: Option<String>,
    repo: Option<Repo>,
    pull_request: Option<PullRequest>,
    actor: Option<String>,
    cron_schedule: Option<String>,
    /// Only when [`GITHUB_TOKEN_VAR_NAME`] holds a token.
    commit_status: Option<CommitStatus>,
}

#[derive(Debug, PartialEq, Eq)]
/// A repository on GitHub.
struct Repo {
    owner: String,
    name: String,
    commit_link: String,
}

#[derive(Debug, PartialEq, Eq)]
struct PullRequest {
    id: u64,
    link: Option<String>,
    owner: Option<String>,
    title: Option<String>,
}

/// The GitHub commit status that tells a pull request an Antithesis run is in progress.
#[derive(Clone, PartialEq, Eq)]
pub struct CommitStatus {
    url: String,
    token: String,
    context: String,
}

impl Build {
    /// The GitHub Actions build that runs snouty, or `None` outside GitHub Actions.
    pub fn detect() -> Option<Self> {
        let build = detect_with(|key| {
            crate::env::var(key)
                .inspect_err(|e| debug!("{e}"))
                .ok()
                .flatten()
        });
        debug!("GitHub Actions build: {build:?}");
        build
    }

    /// Whether a checkout at `commit` holds the code of this build.
    pub fn checks_out(&self, commit: &str) -> bool {
        self.commit == commit || self.merge_commit.as_deref() == Some(commit)
    }

    /// The `vcs.*` params, in the shape the Antithesis GitHub trigger action
    /// sends them.
    pub fn vcs_params(&self) -> Params {
        let mut vcs = Params::new();
        if let Some(repo) = &self.repo {
            vcs.insert(VCS_REPO_TYPE, "github");
            vcs.insert(VCS_REPO_OWNER, &repo.owner);
            vcs.insert(VCS_REPO_NAME, &repo.name);
            vcs.insert(VCS_VERSION_LINK, &repo.commit_link);
        }
        if let Some(branch) = &self.branch {
            vcs.insert(VCS_REPO_BRANCH, branch);
        }
        vcs.insert(VCS_VERSION_ID, &self.commit);
        if let Some(pr) = &self.pull_request {
            vcs.insert(VCS_PR_ID, pr.id.to_string());
            for (key, value) in [
                (VCS_PR_LINK, &pr.link),
                (VCS_PR_OWNER, &pr.owner),
                (VCS_PR_TITLE, &pr.title),
            ] {
                if let Some(value) = value {
                    vcs.insert(key, value);
                }
            }
        }
        vcs
    }

    /// The `run.*` params that name who started the build, and its schedule.
    pub fn run_params(&self) -> Params {
        let mut run = Params::new();
        if let Some(actor) = &self.actor {
            run.insert(RUN_CREATOR_NAME, actor);
        }
        if let Some(schedule) = &self.cron_schedule {
            run.insert(RUN_CRON_SCHEDULE, schedule);
        }
        run
    }

    /// The commit status for this build. Each test name gets its own
    /// context, so two tests on one commit do not replace each other's status.
    pub fn commit_status(&self, test_name: Option<&str>) -> Option<CommitStatus> {
        let mut status = self.commit_status.clone()?;
        if let Some(name) = test_name {
            status.context = format!("{STATUS_CONTEXT} ({name})");
        }
        Some(status)
    }
}

/// Only a github.com `origin` remote gives a repository and a commit link.
impl From<Checkout> for Build {
    fn from(checkout: Checkout) -> Self {
        let repo = checkout
            .origin
            .as_ref()
            .and_then(|url| GitHubRepo::try_from(url).ok())
            .map(|repo| Repo {
                commit_link: repo.commit_url(&checkout.commit),
                owner: repo.owner,
                name: repo.name,
            });
        Build {
            commit: checkout.commit,
            merge_commit: None,
            branch: checkout.branch,
            repo,
            pull_request: None,
            actor: None,
            cron_schedule: None,
            commit_status: None,
        }
    }
}

/// Keeps the token out of the debug log.
impl std::fmt::Debug for CommitStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitStatus")
            .field("url", &self.url)
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

impl CommitStatus {
    /// The `antithesis.integrations.github.*` params that make Antithesis
    /// post the result of the run to this commit status.
    pub fn callback_params(&self) -> Params {
        let mut params = Params::new();
        params.insert(ANT_GITHUB_CALLBACK_URL, &self.url);
        params.insert(ANT_GITHUB_TOKEN, &self.token);
        params
    }

    pub fn context(&self) -> &str {
        &self.context
    }

    /// Sets the commit status to pending. Antithesis sets the final state when the run ends.
    pub async fn post_pending(&self) -> Result<()> {
        crate::api::client_builder()
            .connect_timeout(crate::api::CONNECT_TIMEOUT)
            .timeout(STATUS_REQUEST_TIMEOUT)
            .user_agent(crate::user_agent::user_agent())
            .build()?
            .post(&self.url)
            .bearer_auth(&self.token)
            .header("Accept", "application/vnd.github+json")
            .json(&serde_json::json!({
                "state": "pending",
                "description": "Antithesis is running your tests.",
                "context": self.context,
            }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

#[derive(Deserialize, Default)]
struct GitHubEvent {
    pull_request: Option<GitHubPullRequest>,
    schedule: Option<String>,
}

#[derive(Deserialize)]
struct GitHubPullRequest {
    number: u64,
    html_url: Option<String>,
    title: Option<String>,
    user: Option<GitHubUser>,
    head: GitHubHead,
}

#[derive(Deserialize)]
struct GitHubUser {
    login: String,
}

#[derive(Deserialize)]
struct GitHubHead {
    sha: String,
}

fn detect_with(env: impl Fn(&str) -> Option<String>) -> Option<Build> {
    env("GITHUB_ACTIONS")?;
    let server = env("GITHUB_SERVER_URL").unwrap_or_else(|| "https://github.com".to_owned());
    let api = env("GITHUB_API_URL").unwrap_or_else(|| "https://api.github.com".to_owned());
    let repository = env("GITHUB_REPOSITORY")?;
    // The build is usable without the event, so a read error goes only to
    // the debug log.
    let GitHubEvent {
        pull_request,
        schedule,
    } = env("GITHUB_EVENT_PATH")
        .and_then(|path| {
            let read =
                || -> Result<GitHubEvent> { Ok(serde_json::from_slice(&std::fs::read(&path)?)?) };
            read()
                .inspect_err(|e| debug!("cannot read the event at {path}: {e:#}"))
                .ok()
        })
        .unwrap_or_default();
    let event_name = env("GITHUB_EVENT_NAME");
    // Without its event, a pull request build cannot name the pull request
    // or its head commit.
    if pull_request.is_none()
        && event_name
            .as_deref()
            .is_some_and(|name| name.starts_with("pull_request"))
    {
        return None;
    }
    let sha = env("GITHUB_SHA")?;
    // A pull request build checks out a merge commit. The pull request's own
    // head commit is the one that its checks show. For `pull_request_target`,
    // GITHUB_SHA is the tip of the base branch, which is not a merge commit.
    let (commit, merge_commit) = match &pull_request {
        Some(pr) if event_name.as_deref() == Some("pull_request_target") => {
            (pr.head.sha.clone(), None)
        }
        Some(pr) => (pr.head.sha.clone(), Some(sha)),
        None => (sha, None),
    };
    let branch = env("GITHUB_HEAD_REF").or_else(|| {
        env("GITHUB_REF")?
            .strip_prefix("refs/heads/")
            .map(str::to_owned)
    });
    let repo = repository.split_once('/').map(|(owner, name)| Repo {
        owner: owner.to_owned(),
        name: name.to_owned(),
        commit_link: format!("{server}/{repository}/commit/{commit}"),
    });
    let commit_status = env(GITHUB_TOKEN_VAR_NAME).map(|token| CommitStatus {
        url: format!("{api}/repos/{repository}/statuses/{commit}"),
        token,
        context: STATUS_CONTEXT.to_owned(),
    });
    Some(Build {
        commit_status,
        commit,
        merge_commit,
        branch,
        repo,
        pull_request: pull_request.map(|pr| PullRequest {
            id: pr.number,
            link: pr.html_url,
            owner: pr.user.map(|user| user.login),
            title: pr.title,
        }),
        actor: env("GITHUB_ACTOR"),
        cron_schedule: schedule,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::fake_secrets::FAKE_GITHUB_TOKEN;
    use std::collections::HashMap;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
    const HEAD_SHA: &str = "fedcba9876543210fedcba9876543210fedcba98";

    fn detect(vars: &[(&str, &str)]) -> Option<Build> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|&(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        detect_with(|key| vars.get(key).cloned())
    }

    fn params(build: &Build) -> Vec<(String, String)> {
        let mut all = build.vcs_params();
        all.merge(build.run_params());
        all.as_map()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
            .collect()
    }

    fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
        expected
            .iter()
            .map(|&(k, v)| (k.to_owned(), v.to_owned()))
            .collect()
    }

    #[test]
    fn outside_ci_there_is_no_build() {
        assert_eq!(detect(&[("CI", "true"), ("GITHUB_SHA", SHA)]), None);
    }

    #[test]
    fn github_actions_push() {
        let build = detect(&[
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_REPOSITORY", "acme/widget"),
            ("GITHUB_SHA", SHA),
            ("GITHUB_REF", "refs/heads/main"),
            ("GITHUB_ACTOR", "octocat"),
            ("ANTITHESIS_GITHUB_TOKEN", FAKE_GITHUB_TOKEN),
        ])
        .unwrap();
        assert_eq!(
            params(&build),
            pairs(&[
                ("vcs.repo_type", "github"),
                ("vcs.repo_owner", "acme"),
                ("vcs.repo_name", "widget"),
                (
                    "vcs.version_link",
                    &format!("https://github.com/acme/widget/commit/{SHA}")
                ),
                ("vcs.repo_branch", "main"),
                ("vcs.version_id", SHA),
                ("run.creator_name", "octocat"),
            ])
        );
        assert_eq!(
            build.commit_status(None),
            Some(CommitStatus {
                url: format!("https://api.github.com/repos/acme/widget/statuses/{SHA}"),
                token: FAKE_GITHUB_TOKEN.to_owned(),
                context: "continuous-testing/antithesis".to_owned(),
            })
        );
    }

    #[test]
    fn github_actions_pull_request_without_its_event_is_no_build() {
        assert_eq!(
            detect(&[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_EVENT_NAME", "pull_request"),
                ("GITHUB_REPOSITORY", "acme/widget"),
                ("GITHUB_SHA", SHA),
                ("GITHUB_HEAD_REF", "main"),
            ]),
            None
        );
    }

    #[test]
    fn github_actions_pull_request_checks_out_its_head_or_merge_commit() {
        let dir = tempfile::tempdir().unwrap();
        let event = dir.path().join("event.json");
        std::fs::write(
            &event,
            serde_json::json!({"pull_request": {"number": 1, "head": {"sha": HEAD_SHA}}})
                .to_string(),
        )
        .unwrap();
        let build = |event_name: &str| {
            detect(&[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_EVENT_NAME", event_name),
                ("GITHUB_REPOSITORY", "acme/widget"),
                ("GITHUB_SHA", SHA),
                ("GITHUB_EVENT_PATH", event.to_str().unwrap()),
            ])
            .unwrap()
        };
        let pull_request = build("pull_request");
        assert!(pull_request.checks_out(HEAD_SHA));
        assert!(pull_request.checks_out(SHA));
        // For pull_request_target, GITHUB_SHA is the tip of the base branch.
        let target = build("pull_request_target");
        assert!(target.checks_out(HEAD_SHA));
        assert!(!target.checks_out(SHA));
    }

    #[test]
    fn github_actions_tag_has_no_branch() {
        let build = detect(&[
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_REPOSITORY", "acme/widget"),
            ("GITHUB_SHA", SHA),
            ("GITHUB_REF", "refs/tags/v1.0.0"),
        ])
        .unwrap();
        assert_eq!(build.branch, None);
    }

    #[test]
    fn github_actions_pull_request_uses_the_head_commit() {
        let dir = tempfile::tempdir().unwrap();
        let event = dir.path().join("event.json");
        std::fs::write(
            &event,
            serde_json::json!({
                "pull_request": {
                    "number": 42,
                    "html_url": "https://github.example.com/acme/widget/pull/42",
                    "title": "Add a widget",
                    "user": {"login": "octocat"},
                    "head": {"sha": HEAD_SHA},
                },
            })
            .to_string(),
        )
        .unwrap();
        let build = detect(&[
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_SERVER_URL", "https://github.example.com"),
            ("GITHUB_API_URL", "https://github.example.com/api/v3"),
            ("GITHUB_REPOSITORY", "acme/widget"),
            ("GITHUB_SHA", SHA),
            ("GITHUB_REF", "refs/pull/42/merge"),
            ("GITHUB_HEAD_REF", "feature"),
            ("GITHUB_EVENT_PATH", event.to_str().unwrap()),
            ("ANTITHESIS_GITHUB_TOKEN", FAKE_GITHUB_TOKEN),
        ])
        .unwrap();
        assert_eq!(
            params(&build),
            pairs(&[
                ("vcs.repo_type", "github"),
                ("vcs.repo_owner", "acme"),
                ("vcs.repo_name", "widget"),
                (
                    "vcs.version_link",
                    &format!("https://github.example.com/acme/widget/commit/{HEAD_SHA}")
                ),
                ("vcs.repo_branch", "feature"),
                ("vcs.version_id", HEAD_SHA),
                ("vcs.pr_id", "42"),
                (
                    "vcs.pr_link",
                    "https://github.example.com/acme/widget/pull/42"
                ),
                ("vcs.pr_owner", "octocat"),
                ("vcs.pr_title", "Add a widget"),
            ])
        );
        assert_eq!(
            build.commit_status(Some("smoke")),
            Some(CommitStatus {
                url: format!(
                    "https://github.example.com/api/v3/repos/acme/widget/statuses/{HEAD_SHA}"
                ),
                token: FAKE_GITHUB_TOKEN.to_owned(),
                context: "continuous-testing/antithesis (smoke)".to_owned(),
            })
        );
    }

    #[test]
    fn github_actions_schedule_records_the_cron() {
        let dir = tempfile::tempdir().unwrap();
        let event = dir.path().join("event.json");
        std::fs::write(&event, r#"{"schedule": "0 3 * * *"}"#).unwrap();
        let build = detect(&[
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_REPOSITORY", "acme/widget"),
            ("GITHUB_SHA", SHA),
            ("GITHUB_EVENT_PATH", event.to_str().unwrap()),
        ])
        .unwrap();
        assert_eq!(build.cron_schedule.as_deref(), Some("0 3 * * *"));
    }
}
