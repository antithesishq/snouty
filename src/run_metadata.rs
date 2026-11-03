//! The params that `snouty launch` derives from its environment and attaches
//! to the run: the commit that it launches, who started it, and the GitHub
//! commit status that Antithesis reports the result to.

mod git;
mod github_actions;

use std::path::Path;
use std::time::Duration;

use color_eyre::eyre::{Result, WrapErr};

use crate::params::{
    ANT_GITHUB_CALLBACK_URL, ANT_GITHUB_TOKEN, ANT_TEST_NAME, Params, RUN_CALLER_TYPE,
    VCS_REPO_NAME, VCS_REPO_OWNER, VCS_REPO_TYPE, VCS_VERSION_LINK,
};

/// Which `vcs.*` params [`detect`] derives.
pub enum VcsDetection {
    /// None, for `--no-vcs`.
    Off,
    /// Only from a GitHub Actions workflow run. Outside one, the directory
    /// need not hold the source of a pre-built config image.
    GitHubActionsOnly,
    /// From a GitHub Actions workflow run, or else from the git checkout.
    All,
}

/// The params derived for a launch from the git checkout that holds `dir`
/// and from the GitHub Actions workflow run, if any. A param that the user
/// sets replaces the derived one.
pub fn detect(dir: &Path, vcs: VcsDetection) -> Params {
    let checkout = git::checkout(dir);
    // A GitHub Actions job can check out a commit other than the one it was
    // started for. Then the workflow run does not describe the launch.
    let run = github_actions::WorkflowRun::detect().filter(|run| {
        checkout
            .as_ref()
            .is_none_or(|checkout| run.checks_out(&checkout.commit))
    });
    let mut params = Params::new();
    params.insert(RUN_CALLER_TYPE, "snouty");
    let (run_vcs, run_other) = match run {
        Some(run) => (Some(run.vcs), Some(run.other)),
        None => (None, None),
    };
    match (vcs, run_vcs) {
        (VcsDetection::Off, _) | (VcsDetection::GitHubActionsOnly, None) => {}
        (_, Some(run_vcs)) => params.merge(run_vcs),
        (VcsDetection::All, None) => {
            if let Some(checkout) = checkout {
                params.merge(checkout.vcs_params());
            }
        }
    }
    if let Some(run_other) = run_other {
        params.merge(run_other);
    }
    params
}

/// Inserts the `vcs.*` params that name a repository on GitHub.
fn insert_github_repo(params: &mut Params, owner: &str, name: &str, commit_link: String) {
    params.insert(VCS_REPO_TYPE, "github");
    params.insert(VCS_REPO_OWNER, owner);
    params.insert(VCS_REPO_NAME, name);
    params.insert(VCS_VERSION_LINK, commit_link);
}

/// The commit status context without a test name. The Antithesis GitHub
/// integration posts its result to the same context.
const STATUS_CONTEXT: &str = "continuous-testing/antithesis";

/// A slow GitHub must not keep the launch from printing the run ID.
const STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
