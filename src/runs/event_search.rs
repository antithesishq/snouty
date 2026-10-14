//! The shared rendering pipeline behind the event-stream commands (`runs logs`,
//! `runs events`, `runs search`, `runs build-logs`), plus the events-search
//! helpers.
//!
//! Each command resolves its own backend up front — `runs events` and
//! `runs search` the events-search endpoint, `runs logs`/`runs build-logs`
//! their GET endpoints — and hands the resulting [`JsonStream`] here. From the stream on, the commands are
//! identical: every event renders to one output line.
//!
//! Nothing here filters client-side. The output of a server-side filter IS
//! the result: the server returns a capped subset of the matches, so
//! re-filtering that subset locally silently drops events that did match —
//! absence from the subset is not absence from the run.

use std::io::Write;
use std::time::Duration;

use color_eyre::Section;
use color_eyre::eyre::Result;
use serde_json::json;

use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};

use crate::api::{AntithesisApi, MIN_SEARCH_RELEASE, RunStatus, SearchMode};
use crate::error::{api_error_status, user_error};
use crate::event_render::EventStreamRenderer;
use crate::jsonl::JsonStream;

use super::{ErrorRows, FaultAnnotator, RunProbe, WaitDisplay, event_lines, probe_run, raw_lines};

/// How an event-stream command prints its rows. The illegal flag
/// combinations (`--raw` without `--json`, `--detail` with `--json`) are
/// rejected once in `cmd_runs`, so the type never carries them.
#[derive(Clone, Copy)]
pub(super) enum EventOutput {
    /// `--json`: each event as its JSON line. `raw` passes the server's
    /// stream through without normalization (each record round-trips as
    /// parsed); otherwise vtime is normalized and, on `runs logs`, each
    /// event is annotated with the faults active at its moment
    /// (`annotate_faults` — meaningless with `raw`, which touches nothing).
    Json { raw: bool, annotate_faults: bool },
    /// Human rendering through the shared [`EventStreamRenderer`]; `detail`
    /// adds a full-width vtime, source locations, and details JSON.
    Human { detail: bool },
}

impl EventOutput {
    pub(super) fn json(self) -> bool {
        matches!(self, Self::Json { .. })
    }
}

/// Render an (already server-filtered) event stream into output lines, one
/// line out per line in — the one rendering pipeline behind every
/// event-stream command (`runs logs`, `runs events`, `runs search`,
/// `runs build-logs`). The caller prints what comes back; see
/// [`super::print_event_lines`].
pub(super) fn render_event_stream(
    stream: JsonStream,
    error_rows: ErrorRows,
    output: EventOutput,
) -> BoxStream<'static, Result<String>> {
    match output {
        EventOutput::Json { raw: true, .. } => raw_lines(stream, error_rows).boxed(),
        EventOutput::Json {
            raw: false,
            annotate_faults,
        } => {
            let mut annotator = annotate_faults.then(FaultAnnotator::default);
            event_lines(stream, error_rows)
                .map_ok(move |mut entry| {
                    if let Some(annotator) = &mut annotator {
                        annotator.annotate(&mut entry);
                    }
                    entry.to_string()
                })
                .boxed()
        }
        EventOutput::Human { detail } => {
            let mut renderer = EventStreamRenderer::new(detail);
            event_lines(stream, error_rows)
                .map_ok(move |entry| renderer.render_entry(&entry))
                .boxed()
        }
    }
}

/// Ask the search backend whether `query` parses, without running it
/// (`runs search --check`). A valid query gets a 200 with an empty body; an
/// invalid one fails with the server's rejection. Nothing to read either way.
pub(super) async fn check_query(
    api: &AntithesisApi,
    run_id: &str,
    query: &str,
    json: bool,
) -> Result<()> {
    let _ = search_when_searchable(api, run_id, || {
        api.search_run_events_query(run_id, query, SearchMode::Validate)
    })
    .await?;
    let mut stdout = std::io::stdout().lock();
    if json {
        writeln!(stdout, "{}", json!({"valid": true}))?;
    } else {
        writeln!(stdout, "query is valid")?;
    }
    Ok(())
}

/// Print how many events in the run match `query` (`runs search --count`).
pub(super) async fn count_query(
    api: &AntithesisApi,
    run_id: &str,
    query: &str,
    json: bool,
) -> Result<()> {
    let count =
        search_when_searchable(api, run_id, || api.count_run_events_query(run_id, query)).await?;
    let mut stdout = std::io::stdout().lock();
    if json {
        writeln!(stdout, "{}", json!({"count": count}))?;
    } else {
        let noun = if count == 1 { "event" } else { "events" };
        writeln!(stdout, "{count} {noun} matched the query.")?;
    }
    Ok(())
}

/// The wait before the first retry of a search that answered 404. Each retry
/// doubles the wait, up to [`SEARCH_RETRY_MAX`].
#[cfg(not(test))]
const SEARCH_RETRY_FIRST: Duration = Duration::from_secs(1);
#[cfg(test)]
const SEARCH_RETRY_FIRST: Duration = Duration::from_millis(10);
const SEARCH_RETRY_MAX: Duration = Duration::from_secs(15);

/// Send an events-search request with `send`, and send it again while the
/// run's events are not searchable yet.
///
/// The endpoint's 404 body is an unhelpful "Resource not found", so a 404 is
/// explained by a probe of the run:
///
/// - The run does not exist: "run not found".
/// - The run ID names a release before [`MIN_SEARCH_RELEASE`]: that release
///   does not serve the endpoint, so there is nothing to wait for.
/// - The run is starting or in progress: the server indexes a new run's events
///   some minutes after it starts (observed on orbitinghail, release 64.0: a
///   404 for 2 to 3 minutes after the run was ready). Wait, and try again.
/// - The run has stopped, or its status is unknown: its events will not
///   become searchable, so stop.
///
/// A 400 is the server rejecting the request — usually the query, but an
/// out-of-range `limit` answers 400 too, so the note must not blame the query
/// alone (the server's own message, shown above the note, says which it was).
pub(super) async fn search_when_searchable<T, Fut>(
    api: &AntithesisApi,
    run_id: &str,
    mut send: impl FnMut() -> Fut,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    let mut waiting: Option<WaitDisplay> = None;
    let mut retry = SEARCH_RETRY_FIRST;
    loop {
        let err = match send().await {
            Ok(answer) => {
                if let Some(waiting) = waiting {
                    waiting.finish();
                }
                return Ok(answer);
            }
            Err(err) => err,
        };
        match api_error_status(&err) {
            Some(404) => {}
            Some(400) => {
                return Err(err.note(
                    "the server rejected the request; its message above says which part \
                     — for a query, check the event-set DSL syntax",
                ));
            }
            _ => return Err(err),
        }
        let run = match probe_run(api, run_id).await {
            RunProbe::NotFound => return Err(user_error(format!("run not found: {run_id}"))),
            RunProbe::ProbeFailed(probe_err) => return Err(probe_err),
            RunProbe::Exists(run) => run,
        };
        if !run.serves_events_search() {
            let (major, minor) = MIN_SEARCH_RELEASE;
            return Err(err.suggestion(format!(
                "tenant releases before {major}.{minor} do not serve the events-search \
                 API as documented; `snouty doctor` reports the tenant release"
            )));
        }
        if !matches!(run.status, RunStatus::Starting | RunStatus::InProgress) {
            return Err(err.note(format!(
                "run {run_id} is {}, and its events are not searchable",
                run.status
            )));
        }
        waiting
            .get_or_insert_with(|| {
                WaitDisplay::new("waiting for the run's events to become searchable")
            })
            .show();
        tokio::time::sleep(retry).await;
        retry = (retry * 2).min(SEARCH_RETRY_MAX);
    }
}
