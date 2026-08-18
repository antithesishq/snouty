//! The shared output pipeline behind the event-stream commands (`runs logs`,
//! `runs events`, `runs search`, `runs build-logs`), plus the events-search
//! helpers.
//!
//! Each command resolves its own backend up front — `runs events` from the
//! `runs-search` feature flag, `runs search` always on the events-search
//! endpoint, `runs logs`/`runs build-logs` their GET endpoints — and hands
//! the resulting [`JsonStream`] here. From the stream on, the commands are
//! identical: every line prints as it arrives, the caller's cap is enforced
//! client-side where the server does not, and an empty result gets a
//! friendly note.
//!
//! Nothing here filters client-side. The output of a server-side filter IS
//! the result: the server returns a capped subset of the matches, so
//! re-filtering that subset locally silently drops events that did match —
//! absence from the subset is not absence from the run.

use std::io::{BufWriter, IsTerminal, Write};
use std::num::NonZeroU64;

use color_eyre::Section;
use color_eyre::eyre::Result;
use serde_json::json;

use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};

use crate::api::{AntithesisApi, MIN_SEARCH_RELEASE, SearchMode};
use crate::error::{api_error_status, user_error};
use crate::event_render::EventStreamRenderer;
use crate::features::{self, Feature};
use crate::jsonl::JsonStream;

use super::{ErrorRows, FaultAnnotator, event_lines, raw_lines};

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

/// Print every line of the (already server-filtered) stream, one line out
/// per line in — the one output pipeline behind every event-stream command
/// (`runs logs`, `runs events`, `runs search`, `runs build-logs`).
///
/// The cap is the ONLY early exit: when the server
/// has returned fewer events and holds the connection open, keep waiting
/// rather than guess that the result is complete. `None` means the cap is
/// not this side's job — the limit is server-enforced, or an explicit
/// `--follow` without a limit is unbounded by design.
/// `live` marks a stream whose server may hold the connection open (the
/// events-search backend does, on an in-progress run) — its rows must flush
/// as they arrive rather than sit in the buffer waiting for an EOF that may
/// never come. A bounded dump (`runs logs`, `runs build-logs`, the GET
/// events backend) buffers normally on a pipe; a terminal always gets each
/// row as it lands.
pub(super) async fn print_event_stream(
    stream: JsonStream,
    cap: Option<NonZeroU64>,
    error_rows: ErrorRows,
    output: EventOutput,
    live: bool,
    empty_message: &str,
) -> Result<()> {
    let cap = cap.map_or(usize::MAX, |cap| {
        usize::try_from(cap.get()).unwrap_or(usize::MAX)
    });
    // The per-row rendering resolves once, up front; the one loop below owns
    // the cap, the row count, and the flush-per-row rule. The cap is ours to
    // enforce even on the raw passthrough — the search backend ignores the
    // limit.
    let lines: BoxStream<'_, Result<String>> = match output {
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
                .map_ok(move |entry| {
                    let mut line = String::new();
                    renderer
                        .render_entry(&entry, &mut line)
                        .expect("writing to a String cannot fail");
                    line
                })
                .boxed()
        }
    };
    let flush_per_row = live || std::io::stdout().is_terminal();
    let mut lines = lines.take(cap);
    let mut stdout = BufWriter::new(std::io::stdout().lock());
    let mut seen: u64 = 0;
    while let Some(line) = lines.try_next().await? {
        seen += 1;
        writeln!(stdout, "{line}")?;
        if flush_per_row {
            stdout.flush()?;
        }
    }
    stdout.flush()?;

    // Only a successfully-empty stream earns the friendly empty note; a
    // mid-stream error propagated above instead. The note goes to stderr —
    // it is commentary, not output — and only in human mode: in `--json` an
    // empty stream is the correct machine answer.
    if seen == 0 && !output.json() {
        eprintln!("{empty_message}");
    }
    Ok(())
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
        return Err(explain_search_error(run_id, err));
    }
    let mut stdout = std::io::stdout().lock();
    if json {
        writeln!(stdout, "{}", json!({"valid": true}))?;
    } else {
        writeln!(stdout, "query is valid")?;
    }
    Ok(())
}

/// The refusal for several needles while the `runs-search` feature is off.
/// The GET events endpoint matches ONE substring; AND-filtering the rest
/// client-side silently dropped true matches (the server returns a capped
/// subset), so several needles require the events-search API.
pub(super) fn multi_needle_error() -> color_eyre::eyre::Report {
    user_error("multiple search terms require the events-search API")
        .note(format!(
            "that API is behind the `{}` unstable feature",
            Feature::RUNS_SEARCH
        ))
        .suggestion(format!(
            "search a single term, or set {}={}",
            features::UNSTABLE_FEATURES_VAR_NAME,
            Feature::RUNS_SEARCH
        ))
}

/// Classify an events-search failure. With the feature on, the tenant is
/// assumed to serve the endpoint (`snouty doctor` reports a tenant release
/// that is too old), so a 404 means the run: the endpoint's own 404 body is
/// an unhelpful "Resource not found". A 400 is the server rejecting the
/// request — usually the query, but an out-of-range `limit` answers 400 too,
/// so the note must not blame the query alone (the server's own message,
/// shown above the note, says which it was).
pub(super) fn explain_search_error(
    run_id: &str,
    err: color_eyre::eyre::Report,
) -> color_eyre::eyre::Report {
    match api_error_status(&err) {
        Some(404) => {
            let (major, minor) = MIN_SEARCH_RELEASE;
            user_error(format!("run not found: {run_id}")).suggestion(format!(
                "tenant releases before {major}.{minor} do not serve the events-search \
                 API; on an older tenant, remove `runs-search` from {} to fall back",
                features::UNSTABLE_FEATURES_VAR_NAME
            ))
        }
        Some(400) => err.note(
            "the server rejected the request; its message above says which part \
             — for a query, check the event-set DSL syntax",
        ),
        // The documented contract answers an invalid query with a 400, but
        // current tenants leak it as a generic 500 whose body says "try again
        // later" — wrong advice when the query is the problem. Point at the
        // query so the user does not retry a request that can never succeed.
        Some(500) => err.note(
            "current tenant releases answer an invalid or unsupported query with \
             a generic 500 (observed through release 60.1) — check the query \
             before retrying",
        ),
        _ => err,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ApiError;

    #[test]
    fn multi_needle_error_names_the_gate_and_the_fix() {
        let err = format!("{:?}", multi_needle_error());
        assert!(
            err.contains("multiple search terms require the events-search API"),
            "{err}"
        );
        assert!(
            err.contains("SNOUTY_UNSTABLE_FEATURES=runs-search"),
            "{err}"
        );
    }

    #[test]
    fn explain_search_error_maps_404_to_run_not_found() {
        // With the feature on, the tenant is assumed to serve the endpoint,
        // so its unhelpful 404 body means the run id.
        let err = color_eyre::eyre::Report::new(ApiError {
            status: 404,
            message: "API error: 404 Not Found — Resource not found".to_string(),
        });
        let explained = format!("{:?}", explain_search_error("run-9", err));
        assert!(explained.contains("run not found: run-9"), "{explained}");
    }
}
