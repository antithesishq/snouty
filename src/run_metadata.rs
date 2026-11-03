//! The run metadata that `snouty launch` reads from its environment: the git
//! checkout of the config dir, and the GitHub Actions workflow run.

use std::path::Path;
use std::time::Duration;

use color_eyre::eyre::{Result, WrapErr};
use gix::refs::Category;
use log::debug;
use serde::Deserialize;

use crate::params::{
    ANT_GITHUB_CALLBACK_URL, ANT_GITHUB_TOKEN, ANT_TEST_NAME, Params, RUN_CALLER_TYPE,
    RUN_CREATOR_NAME, RUN_CRON_SCHEDULE, VCS_PR_ID, VCS_PR_LINK, VCS_PR_OWNER, VCS_PR_TITLE,
    VCS_REPO_BRANCH, VCS_REPO_NAME, VCS_REPO_OWNER, VCS_REPO_TYPE, VCS_VERSION_ID,
    VCS_VERSION_LINK,
};

const GITHUB_TOKEN_VAR_NAME: &str = "ANTITHESIS_GITHUB_TOKEN";

/// The commit status context without a test name. The Antithesis GitHub
/// integration posts its result to the same context.
const STATUS_CONTEXT: &str = "continuous-testing/antithesis";

/// A slow GitHub must not keep the launch from printing the run ID.
const STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The run metadata for a launch from `dir`, as params.
pub fn detect(dir: &Path) -> Params {
    detect_with(dir, |key| {
        crate::env::var(key)
            .inspect_err(|e| debug!("{e}"))
            .ok()
            .flatten()
    })
}

fn detect_with(dir: &Path, env: impl Fn(&str) -> Option<String>) -> Params {
    let mut params = Params::new();
    params.insert(RUN_CALLER_TYPE, "snouty");
    let checkout = read_git(dir, &mut params);
    read_github_actions(&env, checkout.as_deref(), &mut params);
    params
}

/// Inserts the metadata of the git checkout that holds `dir`, and returns its
/// commit. The reason that there is no checkout goes to the debug log,
/// because no caller can act on it.
fn read_git(dir: &Path, params: &mut Params) -> Option<String> {
    let read = || -> Result<(String, Option<String>, Option<gix::Url>)> {
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
        Ok((commit, branch, origin))
    };
    let (commit, branch, origin) = read()
        .inspect_err(|e| debug!("no git checkout at {}: {e:#}", dir.display()))
        .ok()?;
    if let Some((owner, name)) = origin.as_ref().and_then(github_repo) {
        let link = format!("https://github.com/{owner}/{name}/commit/{commit}");
        insert_github_repo(params, &owner, &name, link);
    }
    if let Some(branch) = branch {
        params.insert(VCS_REPO_BRANCH, branch);
    }
    params.insert(VCS_VERSION_ID, &commit);
    Some(commit)
}

/// The owner and name of a github.com remote.
fn github_repo(url: &gix::Url) -> Option<(String, String)> {
    // GitHub serves SSH on port 443 at ssh.github.com.
    let host = url.host()?;
    if !host.eq_ignore_ascii_case("github.com") && !host.eq_ignore_ascii_case("ssh.github.com") {
        return None;
    }
    let path = std::str::from_utf8(&url.path).ok()?.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    match path.split_once('/')? {
        (owner, name) if !owner.is_empty() && !name.is_empty() && !name.contains('/') => {
            Some((owner.to_owned(), name.to_owned()))
        }
        _ => None,
    }
}

fn insert_github_repo(params: &mut Params, owner: &str, name: &str, commit_link: String) {
    params.insert(VCS_REPO_TYPE, "github");
    params.insert(VCS_REPO_OWNER, owner);
    params.insert(VCS_REPO_NAME, name);
    params.insert(VCS_VERSION_LINK, commit_link);
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

/// Inserts the metadata of the GitHub Actions workflow run, over the git
/// metadata. It does nothing outside GitHub Actions, or when the checkout
/// is at a commit other than the workflow run's.
fn read_github_actions(
    env: &impl Fn(&str) -> Option<String>,
    checkout: Option<&str>,
    params: &mut Params,
) -> Option<()> {
    env("GITHUB_ACTIONS")?;
    let repository = env("GITHUB_REPOSITORY")?;
    let sha = env("GITHUB_SHA")?;
    // The run is usable without the event, so a read error goes only to the
    // debug log.
    let event: GitHubEvent = env("GITHUB_EVENT_PATH")
        .and_then(|path| {
            let read =
                || -> Result<GitHubEvent> { Ok(serde_json::from_slice(&std::fs::read(&path)?)?) };
            read()
                .inspect_err(|e| debug!("cannot read the event at {path}: {e:#}"))
                .ok()
        })
        .unwrap_or_default();
    // A pull request build checks out a merge commit, but its checks show
    // the head commit. For `pull_request_target`, GITHUB_SHA is the tip of
    // the base branch, which does not hold the pull request's code.
    let commit = event
        .pull_request
        .as_ref()
        .map_or(&sha, |pr| &pr.head.sha)
        .clone();
    let target = env("GITHUB_EVENT_NAME").as_deref() == Some("pull_request_target");
    if let Some(checkout) = checkout
        && checkout != commit
        && (target || checkout != sha)
    {
        debug!("the checkout at {checkout} is not the GitHub Actions commit {commit}");
        return None;
    }

    let server = env("GITHUB_SERVER_URL").unwrap_or_else(|| "https://github.com".to_owned());
    if let Some((owner, name)) = repository.split_once('/') {
        let link = format!("{server}/{repository}/commit/{commit}");
        insert_github_repo(params, owner, name, link);
    }
    let branch = env("GITHUB_HEAD_REF").or_else(|| {
        env("GITHUB_REF")?
            .strip_prefix("refs/heads/")
            .map(str::to_owned)
    });
    if let Some(branch) = branch {
        params.insert(VCS_REPO_BRANCH, branch);
    }
    params.insert(VCS_VERSION_ID, &commit);
    if let Some(pr) = event.pull_request {
        params.insert(VCS_PR_ID, pr.number.to_string());
        for (key, value) in [
            (VCS_PR_LINK, pr.html_url),
            (VCS_PR_OWNER, pr.user.map(|user| user.login)),
            (VCS_PR_TITLE, pr.title),
        ] {
            if let Some(value) = value {
                params.insert(key, value);
            }
        }
    }
    if let Some(actor) = env("GITHUB_ACTOR") {
        params.insert(RUN_CREATOR_NAME, actor);
    }
    if let Some(schedule) = event.schedule {
        params.insert(RUN_CRON_SCHEDULE, schedule);
    }
    if let Some(token) = env(GITHUB_TOKEN_VAR_NAME) {
        let api = env("GITHUB_API_URL").unwrap_or_else(|| "https://api.github.com".to_owned());
        params.insert(
            ANT_GITHUB_CALLBACK_URL,
            format!("{api}/repos/{repository}/statuses/{commit}"),
        );
        params.insert(ANT_GITHUB_TOKEN, token);
    }
    Some(())
}

/// Sets the GitHub commit status at the callback URL of `params` to pending,
/// when `params` hold both GitHub callback params. Antithesis sets the final
/// state when the run ends. Returns the context of the status. Each test
/// name gets its own context, so two tests on one commit do not replace each
/// other's status.
pub async fn post_pending_status(params: &Params) -> Option<Result<String>> {
    let url = params.get(ANT_GITHUB_CALLBACK_URL)?;
    let token = params.get(ANT_GITHUB_TOKEN)?;
    let context = match params.get(ANT_TEST_NAME) {
        Some(name) => format!("{STATUS_CONTEXT} ({name})"),
        None => STATUS_CONTEXT.to_owned(),
    };
    let post = async {
        crate::api::client_builder()
            .connect_timeout(crate::api::CONNECT_TIMEOUT)
            .timeout(STATUS_REQUEST_TIMEOUT)
            .user_agent(crate::user_agent::user_agent())
            .build()?
            .post(url)
            .bearer_auth(token)
            .header("Accept", "application/vnd.github+json")
            .json(&serde_json::json!({
                "state": "pending",
                "description": "Antithesis is running your tests.",
                "context": context,
            }))
            .send()
            .await?
            .error_for_status()?;
        Ok::<_, color_eyre::eyre::Report>(())
    };
    Some(
        post.await
            .wrap_err_with(|| {
                format!("could not set the GitHub commit status {context} to pending")
            })
            .map(|()| context),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::fake_secrets::FAKE_GITHUB_TOKEN;
    use hegel::Generator;
    use hegel::generators;
    use std::collections::HashMap;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
    const HEAD_SHA: &str = "fedcba9876543210fedcba9876543210fedcba98";

    /// The GitHub Actions metadata for `vars`, with the checkout at `checkout`.
    fn github_actions(vars: &[(&str, &str)], checkout: Option<&str>) -> Vec<(String, String)> {
        let vars: HashMap<&str, &str> = vars.iter().copied().collect();
        let mut params = Params::new();
        read_github_actions(
            &|key| vars.get(key).map(|&v| v.to_owned()),
            checkout,
            &mut params,
        );
        params
            .as_map()
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

    fn event_file(json: &serde_json::Value) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), json.to_string()).unwrap();
        file
    }

    #[test]
    fn outside_github_actions_there_is_no_metadata() {
        assert!(github_actions(&[("CI", "true"), ("GITHUB_SHA", SHA)], None).is_empty());
    }

    #[test]
    fn push() {
        let params = github_actions(
            &[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_REPOSITORY", "acme/widget"),
                ("GITHUB_SHA", SHA),
                ("GITHUB_REF", "refs/heads/main"),
                ("GITHUB_ACTOR", "octocat"),
                ("ANTITHESIS_GITHUB_TOKEN", FAKE_GITHUB_TOKEN),
            ],
            None,
        );
        assert_eq!(
            params,
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
    fn tag_has_no_branch() {
        let params = github_actions(
            &[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_REPOSITORY", "acme/widget"),
                ("GITHUB_SHA", SHA),
                ("GITHUB_REF", "refs/tags/v1.0.0"),
            ],
            None,
        );
        assert!(!params.iter().any(|(k, _)| k == VCS_REPO_BRANCH));
    }

    #[test]
    fn pull_request_records_the_head_commit() {
        let event = event_file(&serde_json::json!({
            "pull_request": {
                "number": 42,
                "html_url": "https://github.example.com/acme/widget/pull/42",
                "title": "Add a widget",
                "user": {"login": "octocat"},
                "head": {"sha": HEAD_SHA},
            },
        }));
        let params = github_actions(
            &[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_SERVER_URL", "https://github.example.com"),
                ("GITHUB_API_URL", "https://github.example.com/api/v3"),
                ("GITHUB_REPOSITORY", "acme/widget"),
                ("GITHUB_SHA", SHA),
                ("GITHUB_REF", "refs/pull/42/merge"),
                ("GITHUB_HEAD_REF", "feature"),
                ("GITHUB_EVENT_PATH", event.path().to_str().unwrap()),
                ("ANTITHESIS_GITHUB_TOKEN", FAKE_GITHUB_TOKEN),
            ],
            None,
        );
        assert_eq!(
            params,
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
    fn checkout_must_be_at_the_workflow_commit() {
        let event = event_file(
            &serde_json::json!({"pull_request": {"number": 1, "head": {"sha": HEAD_SHA}}}),
        );
        let applies = |event_name: &str, checkout: &str| {
            !github_actions(
                &[
                    ("GITHUB_ACTIONS", "true"),
                    ("GITHUB_EVENT_NAME", event_name),
                    ("GITHUB_REPOSITORY", "acme/widget"),
                    ("GITHUB_SHA", SHA),
                    ("GITHUB_EVENT_PATH", event.path().to_str().unwrap()),
                ],
                Some(checkout),
            )
            .is_empty()
        };
        assert!(applies("pull_request", HEAD_SHA));
        assert!(applies("pull_request", SHA));
        assert!(!applies(
            "pull_request",
            "1111111111111111111111111111111111111111"
        ));
        // For pull_request_target, GITHUB_SHA is the tip of the base branch.
        assert!(applies("pull_request_target", HEAD_SHA));
        assert!(!applies("pull_request_target", SHA));
    }

    #[test]
    fn schedule_records_the_cron() {
        let event = event_file(&serde_json::json!({"schedule": "0 3 * * *"}));
        let params = github_actions(
            &[
                ("GITHUB_ACTIONS", "true"),
                ("GITHUB_REPOSITORY", "acme/widget"),
                ("GITHUB_SHA", SHA),
                ("GITHUB_EVENT_PATH", event.path().to_str().unwrap()),
            ],
            None,
        );
        assert!(params.contains(&(RUN_CRON_SCHEDULE.to_owned(), "0 3 * * *".to_owned())));
    }

    fn github_repo_of(url: &str) -> Option<(String, String)> {
        github_repo(&gix::url::parse(url).ok()?)
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
        assert_eq!(github_repo_of(&url), Some((owner, name)));
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
            assert_eq!(github_repo_of(url), None, "{url} parsed");
        }
    }
}
