//! Send a run-scoped request once the run can serve it.
//!
//! Some endpoints refuse a run for a while after it starts, or after it
//! reports that it is ready. Each such endpoint is an [`Endpoint`]: what it
//! needs from the run, what to show while it waits, and which of its errors
//! mean "try again shortly". [`send_when_ready`] is the one loop that waits,
//! sends, and retries. Add an endpoint, not a loop.

use std::future::Future;
use std::time::Duration;

use color_eyre::eyre::{Report, Result};

use crate::api::{AntithesisApi, RunDetail};
use crate::cli::WaitUntil;

use super::{ReadyDisplay, look_at_run, wait_for_run};

/// What an endpoint needs from the run, before it sends and while it retries.
#[derive(Clone, Copy)]
pub(super) enum Needs {
    /// Wait until the run is ready or has ended, then send. Retry only while
    /// the run stays ready.
    Ready,
    /// Send at once. Retry only while the run has not ended.
    Live,
}

impl Needs {
    /// Whether a retry can still succeed. Checked here, so no retry policy
    /// can keep retrying a run that has ended.
    fn can_retry(self, run: &RunDetail) -> bool {
        match self {
            Needs::Ready => run.is_ready(),
            Needs::Live => !run.status.is_terminal(),
        }
    }
}

/// What to do after an endpoint answers with an error.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RetryPolicy {
    /// The error means "try again shortly".
    Retry,
    Fail,
}

/// A run-scoped request that the run may not serve yet.
#[derive(Clone, Copy)]
pub(super) struct Endpoint {
    pub needs: Needs,
    /// Shown on stderr while the endpoint answers [`RetryPolicy::Retry`].
    pub waiting: &'static str,
    /// Whether an error from the endpoint is worth a retry, for the run as
    /// last read. [`Needs`] still decides whether the run can get there.
    pub retry_policy: fn(&RunDetail, &Report) -> RetryPolicy,
}

const READY_WAITING: &str = "waiting for the run to be ready";

/// Send `send` once the run meets `endpoint.needs`. While the endpoint
/// answers an error that its retry policy retries, and the run can still get
/// there, wait `poll`, read the run again, and send again. The wait has no
/// time limit. `explain` annotates the final error, with the run as last
/// read before that send.
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
    let mut waiting = Waiting::default();
    loop {
        let run = match endpoint.needs {
            Needs::Ready => {
                let run = wait_for_run(api, run_id, poll, WaitUntil::Ready, |_| {
                    waiting.show(READY_WAITING);
                })
                .await?;
                waiting.reached(&run);
                run
            }
            Needs::Live => look_at_run(api, run_id).await?,
        };
        let err = match send().await {
            Ok(value) => {
                waiting.clear();
                return Ok(value);
            }
            Err(err) => err,
        };
        if !(endpoint.needs.can_retry(&run)
            && (endpoint.retry_policy)(&run, &err) == RetryPolicy::Retry)
        {
            return Err(explain(&run, err));
        }
        waiting.show(endpoint.waiting);
        tokio::time::sleep(poll).await;
    }
}

/// The one [`ReadyDisplay`] on stderr, for whichever wait is current.
#[derive(Default)]
struct Waiting(Option<(&'static str, ReadyDisplay)>);

impl Waiting {
    /// Show `message`. A different message ends the current display first.
    fn show(&mut self, message: &'static str) {
        if self
            .0
            .as_ref()
            .is_none_or(|(current, _)| *current != message)
        {
            self.clear();
        }
        self.0
            .get_or_insert_with(|| (message, ReadyDisplay::new(message)))
            .1
            .show();
    }

    /// End a wait for the run to be ready, which says that the run is
    /// ready when it is.
    fn reached(&mut self, run: &RunDetail) {
        if self
            .0
            .as_ref()
            .is_some_and(|(current, _)| *current == READY_WAITING)
            && let Some((_, display)) = self.0.take()
        {
            display.finish(run);
        }
    }

    fn clear(&mut self) {
        if let Some((_, display)) = self.0.take() {
            display.clear();
        }
    }
}
