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

use color_eyre::Section;
use color_eyre::eyre::Result;
use serde_json::json;

use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};

use crate::api::{AntithesisApi, MIN_SEARCH_RELEASE, RunStatus, SearchMode};
use crate::error::{api_error_status, user_error};
use crate::event_render::EventStreamRenderer;
use crate::jsonl::JsonStream;

use super::{ErrorRows, FaultAnnotator, RunProbe, event_lines, probe_run, raw_lines};

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
            let mut renderer = EventStreamRenderer::new(detail, super::terminal_width());
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
    if let Err(err) = api
        .search_run_events_query(run_id, query, SearchMode::Validate)
        .await
    {
        return Err(explain_search_error(api, run_id, err).await);
    }
    let mut stdout = std::io::stdout().lock();
    if json {
        writeln!(stdout, "{}", json!({"valid": true}))?;
    } else {
        writeln!(stdout, "query is valid")?;
    }
    Ok(())
}

/// Classify an events-search failure. On a 404, the run is probed to say
/// why: the run does not exist, the run is still starting and has no
/// searchable events, or the tenant release does not serve the endpoint
/// (`snouty doctor` reports it). The endpoint's own 404 body is an unhelpful
/// "Resource not found". A 400 is the server rejecting the
/// request — usually the query, but an out-of-range `limit` answers 400 too,
/// so the note must not blame the query alone (the server's own message,
/// shown above the note, says which it was).
pub(super) async fn explain_search_error(
    api: &AntithesisApi,
    run_id: &str,
    err: color_eyre::eyre::Report,
) -> color_eyre::eyre::Report {
    match api_error_status(&err) {
        Some(404) => match probe_run(api, run_id).await {
            RunProbe::NotFound => user_error(format!("run not found: {run_id}")),
            RunProbe::Exists(run) if run.status == RunStatus::Starting => user_error(format!(
                "run {run_id} is still starting and has no searchable events yet"
            ))
            .suggestion(format!(
                "wait for it with `snouty runs wait {run_id} --until running`, then try again"
            )),
            RunProbe::Exists(_) => {
                let (major, minor) = MIN_SEARCH_RELEASE;
                err.suggestion(format!(
                    "tenant releases before {major}.{minor} do not serve the events-search \
                     API as documented; `snouty doctor` reports the tenant release"
                ))
            }
            RunProbe::ProbeFailed(probe_err) => probe_err,
        },
        Some(400) => err.note(
            "the server rejected the request; its message above says which part \
             — for a query, check the event-set DSL syntax",
        ),
        _ => err,
    }
}
