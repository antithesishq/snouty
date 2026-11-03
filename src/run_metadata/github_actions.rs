//! The run metadata of the GitHub Actions workflow run that runs snouty, from
//! the environment variables and the event payload that GitHub Actions sets.

use color_eyre::eyre::Result;
use log::debug;
use serde::Deserialize;

use crate::params::{
    ANT_GITHUB_CALLBACK_URL, ANT_GITHUB_TOKEN, Params, RUN_CREATOR_NAME, RUN_CRON_SCHEDULE,
    VCS_PR_ID, VCS_PR_LINK, VCS_PR_OWNER, VCS_PR_TITLE, VCS_REPO_BRANCH, VCS_VERSION_ID,
};

const GITHUB_TOKEN_VAR_NAME: &str = "ANTITHESIS_GITHUB_TOKEN";

/// The workflow run of one commit.
pub(super) struct WorkflowRun {
    /// A full commit hash.
    commit: String,
    /// The merge commit that GitHub Actions checks out for a pull request in
    /// place of `commit`.
    merge_commit: Option<String>,
    /// The `vcs.*` params, in the shape the Antithesis GitHub trigger action
    /// sends them.
    pub(super) vcs: Params,
    /// `run.*` and GitHub callback params.
    pub(super) other: Params,
}

impl WorkflowRun {
    /// The workflow run that runs snouty, or `None` outside GitHub Actions.
    pub(super) fn detect() -> Option<Self> {
        let run = detect_with(|key| {
            crate::env::var(key)
                .inspect_err(|e| debug!("{e}"))
                .ok()
                .flatten()
        });
        if let Some(run) = &run {
            debug!("GitHub Actions workflow run of {}", run.commit);
        }
        run
    }

    /// Whether a checkout at `commit` holds the code of this workflow run.
    pub(super) fn checks_out(&self, commit: &str) -> bool {
        self.commit == commit || self.merge_commit.as_deref() == Some(commit)
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

fn detect_with(env: impl Fn(&str) -> Option<String>) -> Option<WorkflowRun> {
    env("GITHUB_ACTIONS")?;
    let server = env("GITHUB_SERVER_URL").unwrap_or_else(|| "https://github.com".to_owned());
    let api = env("GITHUB_API_URL").unwrap_or_else(|| "https://api.github.com".to_owned());
    let repository = env("GITHUB_REPOSITORY")?;
    // The run is usable without the event, so a read error goes only to the
    // debug log.
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

    let mut vcs = Params::new();
    if let Some((owner, name)) = repository.split_once('/') {
        super::insert_github_repo(
            &mut vcs,
            owner,
            name,
            format!("{server}/{repository}/commit/{commit}"),
        );
    }
    let branch = env("GITHUB_HEAD_REF").or_else(|| {
        env("GITHUB_REF")?
            .strip_prefix("refs/heads/")
            .map(str::to_owned)
    });
    if let Some(branch) = branch {
        vcs.insert(VCS_REPO_BRANCH, branch);
    }
    vcs.insert(VCS_VERSION_ID, &commit);
    if let Some(pr) = pull_request {
        vcs.insert(VCS_PR_ID, pr.number.to_string());
        for (key, value) in [
            (VCS_PR_LINK, pr.html_url),
            (VCS_PR_OWNER, pr.user.map(|user| user.login)),
            (VCS_PR_TITLE, pr.title),
        ] {
            if let Some(value) = value {
                vcs.insert(key, value);
            }
        }
    }

    let mut other = Params::new();
    if let Some(actor) = env("GITHUB_ACTOR") {
        other.insert(RUN_CREATOR_NAME, actor);
    }
    if let Some(schedule) = schedule {
        other.insert(RUN_CRON_SCHEDULE, schedule);
    }
    if let Some(token) = env(GITHUB_TOKEN_VAR_NAME) {
        other.insert(
            ANT_GITHUB_CALLBACK_URL,
            format!("{api}/repos/{repository}/statuses/{commit}"),
        );
        other.insert(ANT_GITHUB_TOKEN, token);
    }

    Some(WorkflowRun {
        commit,
        merge_commit,
        vcs,
        other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::fake_secrets::FAKE_GITHUB_TOKEN;
    use std::collections::HashMap;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
    const HEAD_SHA: &str = "fedcba9876543210fedcba9876543210fedcba98";

    fn detect(vars: &[(&str, &str)]) -> Option<WorkflowRun> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|&(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        detect_with(|key| vars.get(key).cloned())
    }

    fn params(run: &WorkflowRun) -> Vec<(String, String)> {
        run.vcs
            .as_map()
            .iter()
            .chain(run.other.as_map())
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
            .collect()
    }

    fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
        expected
            .iter()
            .map(|&(k, v)| (k.to_owned(), v.to_owned()))
            .collect()
    }

    fn event_file(json: &serde_json::Value) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), json.to_string()).unwrap();
        file
    }

    #[test]
    fn outside_github_actions_there_is_no_run() {
        assert!(detect(&[("CI", "true"), ("GITHUB_SHA", SHA)]).is_none());
    }

    #[test]
    fn push() {
        let run = detect(&[
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_REPOSITORY", "acme/widget"),
            ("GITHUB_SHA", SHA),
            ("GITHUB_REF", "refs/heads/main"),
            ("GITHUB_ACTOR", "octocat"),
            ("ANTITHESIS_GITHUB_TOKEN", FAKE_GITHUB_TOKEN),
        ])
        .unwrap();
        assert_eq!(
            params(&run),
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
                (
                    "antithesis.integrations.github.callback_url",
                    &format!("https://api.github.com/repos/acme/widget/statuses/{SHA}")
                ),
                ("antithesis.integrations.github.token", FAKE_GITHUB_TOKEN),
            ])
        );
    }

    #[test]
    fn pull_request_without_its_event_is_no_run() {
        assert!(
            detect(&[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_EVENT_NAME", "pull_request"),
                ("GITHUB_REPOSITORY", "acme/widget"),
                ("GITHUB_SHA", SHA),
                ("GITHUB_HEAD_REF", "main"),
            ])
            .is_none()
        );
    }

    #[test]
    fn pull_request_checks_out_its_head_or_merge_commit() {
        let event = event_file(
            &serde_json::json!({"pull_request": {"number": 1, "head": {"sha": HEAD_SHA}}}),
        );
        let run = |event_name: &str| {
            detect(&[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_EVENT_NAME", event_name),
                ("GITHUB_REPOSITORY", "acme/widget"),
                ("GITHUB_SHA", SHA),
                ("GITHUB_EVENT_PATH", event.path().to_str().unwrap()),
            ])
            .unwrap()
        };
        let pull_request = run("pull_request");
        assert!(pull_request.checks_out(HEAD_SHA));
        assert!(pull_request.checks_out(SHA));
        // For pull_request_target, GITHUB_SHA is the tip of the base branch.
        let target = run("pull_request_target");
        assert!(target.checks_out(HEAD_SHA));
        assert!(!target.checks_out(SHA));
    }

    #[test]
    fn tag_has_no_branch() {
        let run = detect(&[
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_REPOSITORY", "acme/widget"),
            ("GITHUB_SHA", SHA),
            ("GITHUB_REF", "refs/tags/v1.0.0"),
        ])
        .unwrap();
        assert!(!run.vcs.contains_key(VCS_REPO_BRANCH));
    }

    #[test]
    fn pull_request_uses_the_head_commit() {
        let event = event_file(&serde_json::json!({
            "pull_request": {
                "number": 42,
                "html_url": "https://github.example.com/acme/widget/pull/42",
                "title": "Add a widget",
                "user": {"login": "octocat"},
                "head": {"sha": HEAD_SHA},
            },
        }));
        let run = detect(&[
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_SERVER_URL", "https://github.example.com"),
            ("GITHUB_API_URL", "https://github.example.com/api/v3"),
            ("GITHUB_REPOSITORY", "acme/widget"),
            ("GITHUB_SHA", SHA),
            ("GITHUB_REF", "refs/pull/42/merge"),
            ("GITHUB_HEAD_REF", "feature"),
            ("GITHUB_EVENT_PATH", event.path().to_str().unwrap()),
            ("ANTITHESIS_GITHUB_TOKEN", FAKE_GITHUB_TOKEN),
        ])
        .unwrap();
        assert_eq!(
            params(&run),
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
                (
                    "antithesis.integrations.github.callback_url",
                    &format!(
                        "https://github.example.com/api/v3/repos/acme/widget/statuses/{HEAD_SHA}"
                    )
                ),
                ("antithesis.integrations.github.token", FAKE_GITHUB_TOKEN),
            ])
        );
    }

    #[test]
    fn schedule_records_the_cron() {
        let event = event_file(&serde_json::json!({"schedule": "0 3 * * *"}));
        let run = detect(&[
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_REPOSITORY", "acme/widget"),
            ("GITHUB_SHA", SHA),
            ("GITHUB_EVENT_PATH", event.path().to_str().unwrap()),
        ])
        .unwrap();
        assert_eq!(
            run.other
                .as_map()
                .get(RUN_CRON_SCHEDULE)
                .and_then(|v| v.as_str()),
            Some("0 3 * * *")
        );
    }
}
