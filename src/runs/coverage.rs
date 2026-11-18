//! `runs coverage`: the run's coverage locations, one JSON line each.

use std::collections::HashSet;
use std::io::Write;
use std::pin::pin;

use color_eyre::Section;
use color_eyre::eyre::{Report, Result};
use futures_util::{StreamExt, TryStreamExt, stream};

use crate::api::{AntithesisApi, MIN_COVERAGE_RELEASE, RunDetail};
use crate::cli::RunsCoverageArgs;
use crate::error::{api_error_status, user_error};
use crate::settings::Settings;

use super::ready::{self, Needs, RetryDecision, RetryPolicy};
use super::{READY_POLL, mid_stream_error, normalize_vtime_field, stream_error_message};

/// The locations endpoint answers 404 for a while after the run is ready. A
/// run created before [`MIN_COVERAGE_RELEASE`] answers 404 for good. The
/// command waits for the run once, before its first request.
pub(super) const COVERAGE_RETRY: RetryPolicy = RetryPolicy {
    needs: Needs::Live,
    waiting: "waiting for the run's coverage",
    decide: |run, err| {
        RetryDecision::retry_if(
            api_error_status(err) == Some(404) && run.created_on_or_after(MIN_COVERAGE_RELEASE),
        )
    },
};

/// Annotate a locations error for `run`: a 404 from a run that will never
/// have coverage.
pub(super) fn explain_coverage_error(run: &RunDetail, err: Report) -> Report {
    if api_error_status(&err) == Some(404) && !run.created_on_or_after(MIN_COVERAGE_RELEASE) {
        let (major, minor) = MIN_COVERAGE_RELEASE;
        user_error(format!("run {} has no coverage locations", run.run_id)).note(format!(
            "coverage needs a run created on release {major}.{minor} or later"
        ))
    } else {
        err
    }
}

pub(super) async fn cmd_runs_coverage(
    args: RunsCoverageArgs,
    settings: &Settings,
    verbose: bool,
) -> Result<()> {
    let RunsCoverageArgs { run_id, module_ids } = args;
    let run_id = run_id.as_str();
    let api = &AntithesisApi::new(settings, verbose)?;
    ready::wait_until_ready(api, run_id, READY_POLL).await?;

    let mut seen = HashSet::new();
    let mut requests: Vec<Option<&str>> = module_ids
        .iter()
        .map(String::as_str)
        .filter(|id| seen.insert(*id))
        .map(Some)
        .collect();
    if requests.is_empty() {
        requests.push(None);
    }
    let locations =
        stream::iter(requests)
            .then(|module_id| {
                ready::send_when_ready(
                    api,
                    run_id,
                    READY_POLL,
                    COVERAGE_RETRY,
                    move || async move {
                        Ok(api.get_coverage_locations(run_id, module_id).await?.untag())
                    },
                    explain_coverage_error,
                )
            })
            .try_flatten();
    let mut locations = pin!(locations);
    while let Some(mut location) = locations.try_next().await? {
        if let Some(message) = stream_error_message(&location) {
            return Err(mid_stream_error(message));
        }
        if let Some(first_reached) = location.get_mut("first_reached") {
            normalize_vtime_field(first_reached, "moment");
        }
        outln!("{location}")?;
    }
    Ok(())
}
