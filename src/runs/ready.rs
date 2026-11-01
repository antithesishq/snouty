//! Send a run-scoped request once the run can serve it.
//!
//! Some endpoints refuse a run for a while after it starts or reports that it
//! is ready. Each one has a [`RetryPolicy`], and [`send_when_ready`] is the
//! only loop that waits and retries. Add a policy, not a loop.

use std::future::Future;
use std::time::Duration;

use color_eyre::eyre::{Report, Result};

use crate::api::{AntithesisApi, RunDetail};
use crate::cli::WaitUntil;

use super::{ReadyDisplay, fetch_run_after, unclassified_status_error, wait_for_run};

/// What an endpoint needs from the run before it sends.
pub(super) enum Needs {
    /// Wait until the run is ready or has ended, then send.
    Ready,
    /// Send at once.
    Live,
}

/// What to do after an endpoint answers with an error.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RetryDecision {
    /// The error means "try again shortly".
    Retry,
    Fail,
}

impl RetryDecision {
    pub(super) fn retry_if(retry: bool) -> Self {
        if retry { Self::Retry } else { Self::Fail }
    }
}

/// How to retry a run-scoped request that the run may not serve yet.
pub(super) struct RetryPolicy {
    pub needs: Needs,
    /// Shown on stderr while the endpoint answers [`RetryDecision::Retry`].
    pub waiting: &'static str,
    /// Whether an error from the endpoint is worth a retry, given the run as
    /// read after it. A run that has ended never retries.
    pub decide: fn(&RunDetail, &Report) -> RetryDecision,
}

/// Send `send` once the run meets `policy.needs`, and send again every
/// `poll` while the run has not ended and `policy` decides
/// [`RetryDecision::Retry`]. The run is read after each error, for `decide`
/// and for `explain`, which annotates the final error. The wait has no time
/// limit.
pub(super) async fn send_when_ready<T, Fut>(
    api: &AntithesisApi,
    run_id: &str,
    poll: Duration,
    policy: RetryPolicy,
    mut send: impl FnMut() -> Fut,
    explain: impl FnOnce(&RunDetail, Report) -> Report,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    // A run never leaves Ready, so one wait before the first send is enough.
    if let Needs::Ready = policy.needs {
        let mut waiting: Option<ReadyDisplay> = None;
        let run = wait_for_run(api, run_id, poll, WaitUntil::Ready, |_| {
            waiting
                .get_or_insert_with(|| ReadyDisplay::new("waiting for the run to be ready"))
                .show();
        })
        .await?;
        if let Some(waiting) = waiting {
            waiting.finish(&run);
        }
    }
    let mut retrying: Option<ReadyDisplay> = None;
    loop {
        let err = match send().await {
            Ok(value) => {
                if let Some(retrying) = retrying {
                    retrying.clear();
                }
                return Ok(value);
            }
            Err(err) => err,
        };
        let (run, err) = fetch_run_after(api, run_id, err).await?;
        if run.status.is_terminal() || (policy.decide)(&run, &err) == RetryDecision::Fail {
            return Err(explain(&run, err));
        }
        // A status this build does not know may never end.
        if run.status.is_unclassified() {
            return Err(unclassified_status_error(run_id, &run.status));
        }
        retrying
            .get_or_insert_with(|| ReadyDisplay::new(policy.waiting))
            .show();
        tokio::time::sleep(poll).await;
    }
}
