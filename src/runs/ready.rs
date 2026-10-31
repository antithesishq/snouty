//! Send a run-scoped request once the run can serve it.
//!
//! Some endpoints refuse a run for a while after it starts or reports that it
//! is ready. Each one is an [`Endpoint`], and [`send_when_ready`] is the only
//! loop that waits and retries. Add an endpoint, not a loop.

use std::future::Future;
use std::time::Duration;

use color_eyre::eyre::{Report, Result};

use crate::api::{AntithesisApi, RunDetail};
use crate::cli::WaitUntil;
use crate::error::api_error_status;

use super::{ReadyDisplay, look_at_run, wait_for_run};

/// What an endpoint needs from the run before it sends.
#[derive(Clone, Copy)]
pub(super) enum Needs {
    /// Wait until the run is ready or has ended, then send.
    Ready,
    /// Send at once, and read the run only after an error.
    Live,
}

/// What to do after an endpoint answers with an error.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RetryPolicy {
    /// The error means "try again shortly".
    Retry,
    Fail,
}

impl RetryPolicy {
    pub(super) fn retry_if(retry: bool) -> Self {
        if retry { Self::Retry } else { Self::Fail }
    }
}

/// A run-scoped request that the run may not serve yet.
#[derive(Clone, Copy)]
pub(super) struct Endpoint {
    pub needs: Needs,
    /// Shown on stderr while the endpoint answers [`RetryPolicy::Retry`].
    pub waiting: &'static str,
    /// Whether an error from the endpoint is worth a retry, given the run as
    /// last read. A run that has ended never retries.
    pub retry_policy: fn(&RunDetail, &Report) -> RetryPolicy,
}

/// Send `send` once the run meets `endpoint.needs`, and send again every
/// `poll` while the run has not ended and the endpoint's retry policy says
/// [`RetryPolicy::Retry`]. The wait has no time limit. `explain` annotates
/// the final error, with the run as last read.
pub(super) async fn send_when_ready<T, Fut>(
    api: &AntithesisApi,
    run_id: &str,
    poll: Duration,
    endpoint: Endpoint,
    mut send: impl FnMut() -> Fut,
    explain: impl FnOnce(&RunDetail, Report) -> Report,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    let mut retrying: Option<ReadyDisplay> = None;
    loop {
        let ready_run = match endpoint.needs {
            Needs::Ready => {
                let mut waiting: Option<ReadyDisplay> = None;
                let run = wait_for_run(api, run_id, poll, WaitUntil::Ready, |_| {
                    if let Some(retrying) = retrying.take() {
                        retrying.clear();
                    }
                    waiting
                        .get_or_insert_with(|| ReadyDisplay::new("waiting for the run to be ready"))
                        .show();
                })
                .await?;
                if let Some(waiting) = waiting {
                    waiting.finish(&run);
                }
                Some(run)
            }
            Needs::Live => None,
        };
        let err = match send().await {
            Ok(value) => {
                if let Some(retrying) = retrying {
                    retrying.clear();
                }
                return Ok(value);
            }
            Err(err) => err,
        };
        let run = match ready_run {
            Some(run) => run,
            None => match look_at_run(api, run_id).await {
                Ok(run) => run,
                // A 404 can mean a bad run ID, which the read names. Any
                // other error is the endpoint's own.
                Err(read_err) if api_error_status(&err) == Some(404) => return Err(read_err),
                Err(_) => return Err(err),
            },
        };
        if run.status.is_terminal() || (endpoint.retry_policy)(&run, &err) == RetryPolicy::Fail {
            return Err(explain(&run, err));
        }
        retrying
            .get_or_insert_with(|| ReadyDisplay::new(endpoint.waiting))
            .show();
        tokio::time::sleep(poll).await;
    }
}
