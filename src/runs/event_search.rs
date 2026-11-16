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

use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use color_eyre::Section;
use color_eyre::eyre::Result;
use serde_json::{Value, json};

use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt, stream};

use crate::api::{AntithesisApi, MIN_SEARCH_RELEASE, RunDetail, SearchMode};
use crate::error::api_error_status;
use crate::event_render::EventStreamRenderer;
use crate::ndjson::JsonStream;
use crate::stdio;
use crate::vtime::VTime;

use super::ready::{self, Needs, RetryDecision, RetryPolicy};
use super::{FaultAnnotator, READY_POLL, event_lines, raw_lines, stream_error_message};

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
    /// Text rendering through the shared [`EventStreamRenderer`]; `detail`
    /// adds a full-width vtime, source locations, and details JSON.
    Text { detail: bool },
}

impl EventOutput {
    pub(super) fn json(self) -> bool {
        matches!(self, Self::Json { .. })
    }

    pub(super) fn raw(self) -> bool {
        matches!(self, Self::Json { raw: true, .. })
    }
}

/// Read a closed search result to its end, and give its events back in
/// moment order: one group for each input_hash, the groups by their earliest
/// vtime, and each group's events by vtime. The server returns its matches in
/// no fixed order. Events without a moment, such as the output of a `narrow`
/// that drops it, go last in their server order.
///
/// An error, or a `Stream_Error` record, ends the read: the events before it
/// come out sorted, then the error, so the command still fails after the
/// rows it received.
pub(super) async fn in_moment_order(mut events: JsonStream) -> JsonStream {
    let mut sorted = Vec::new();
    let mut error = None;
    while let Some(item) = events.next().await {
        match item {
            Ok(event) if stream_error_message(&event).is_none() => sorted.push(event),
            stop => {
                error = Some(stop);
                break;
            }
        }
    }
    sort_by_moment(&mut sorted);
    stream::iter(sorted.into_iter().map(Ok).chain(error)).boxed()
}

fn sort_by_moment(events: &mut [Value]) {
    fn moment(event: &Value) -> Option<(&str, VTime)> {
        let moment = &event["moment"];
        Some((
            moment["input_hash"].as_str()?,
            VTime::from_json(&moment["vtime"])?,
        ))
    }
    let mut earliest: HashMap<String, VTime> = HashMap::new();
    for (hash, vtime) in events.iter().filter_map(moment) {
        earliest
            .entry(hash.to_string())
            .and_modify(|first| *first = (*first).min(vtime))
            .or_insert(vtime);
    }
    // The sort is stable, so events at one moment keep their server order.
    // The leading flag puts the events without a moment last.
    events.sort_by_cached_key(|event| match moment(event) {
        Some((hash, vtime)) => (false, Some((earliest[hash], hash.to_string(), vtime))),
        None => (true, None),
    });
}

/// Render an (already server-filtered) event stream into output lines, one
/// line out per line in — the one rendering pipeline behind every
/// event-stream command (`runs logs`, `runs events`, `runs search`,
/// `runs build-logs`). The caller prints what comes back; see
/// [`super::print_event_lines`].
pub(super) fn render_event_stream(
    stream: JsonStream,
    output: EventOutput,
) -> BoxStream<'static, Result<String>> {
    match output {
        EventOutput::Json { raw: true, .. } => raw_lines(stream).boxed(),
        EventOutput::Json {
            raw: false,
            annotate_faults,
        } => {
            let mut annotator = annotate_faults.then(FaultAnnotator::default);
            event_lines(stream)
                .map_ok(move |mut entry| {
                    if let Some(annotator) = &mut annotator {
                        annotator.annotate(&mut entry);
                    }
                    entry.to_string()
                })
                .boxed()
        }
        EventOutput::Text { detail } => {
            let mut renderer = EventStreamRenderer::new(detail);
            event_lines(stream)
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
    let _ = search_when_ready(api, run_id, READY_POLL, || {
        api.search_run_events_query(run_id, query, SearchMode::Validate)
    })
    .await?;
    let mut stdout = stdio::stdout().lock();
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
    let count = search_when_ready(api, run_id, READY_POLL, || {
        api.count_run_events_query(run_id, query)
    })
    .await?;
    let mut stdout = stdio::stdout().lock();
    if json {
        writeln!(stdout, "{}", json!({"count": count}))?;
    } else {
        let noun = if count == 1 { "event" } else { "events" };
        writeln!(stdout, "{count} {noun} matched the query.")?;
    }
    Ok(())
}

/// The events search, its count, and its query check. On orbitinghail
/// (releases 66.0 and 67.0), a ready run answers 404 `Resource not found` for
/// a minute or two. A release before [`MIN_SEARCH_RELEASE`] answers 404 for
/// every run.
const SEARCH_RETRY: RetryPolicy = RetryPolicy {
    needs: Needs::Ready,
    waiting: "waiting for the run's events to be searchable",
    decide: |run, err| {
        RetryDecision::retry_if(
            api_error_status(err) == Some(404) && run.created_on_or_after(MIN_SEARCH_RELEASE),
        )
    },
};

/// Send an events-search request with `send` once the run is ready or
/// terminal, and retry while [`SEARCH_RETRY`] allows it.
pub(super) async fn search_when_ready<T, Fut>(
    api: &AntithesisApi,
    run_id: &str,
    poll: Duration,
    send: impl FnMut() -> Fut,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    ready::send_when_ready(api, run_id, poll, SEARCH_RETRY, send, explain_search_error).await
}

/// Annotate an events-search error for `run`. `run` must exist and be ready
/// or terminal. Releases before [`MIN_SEARCH_RELEASE`] do not serve the
/// endpoint and answer 404 with an unhelpful "Resource not found", so the
/// error gets a suggestion that names the release.
fn explain_search_error(
    run: &RunDetail,
    err: color_eyre::eyre::Report,
) -> color_eyre::eyre::Report {
    match api_error_status(&err) {
        Some(404) if !run.created_on_or_after(MIN_SEARCH_RELEASE) => {
            let (major, minor) = MIN_SEARCH_RELEASE;
            err.suggestion(format!(
                "tenant releases before {major}.{minor} do not serve the events-search \
                 API as documented; `snouty doctor` reports the tenant release"
            ))
        }
        _ => err,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(hash: &str, vtime: &str, text: &str) -> Value {
        json!({"moment": {"input_hash": hash, "vtime": vtime}, "output_text": text})
    }

    async fn ordered(items: Vec<Result<Value>>) -> Vec<Result<Value>> {
        in_moment_order(stream::iter(items).boxed())
            .await
            .collect()
            .await
    }

    #[tokio::test]
    async fn events_group_by_input_hash_in_vtime_order() {
        let narrowed = json!({"output_text": "no moment"});
        let events = vec![
            event("-1", "5.0", "a late"),
            event("2", "3.0", "b"),
            narrowed.clone(),
            event("-1", "1.0", "a first"),
            event("-1", "2.0", "a second"),
            event("-1", "2.0", "a second, same vtime"),
        ];
        let sorted: Vec<Value> = ordered(events.into_iter().map(Ok).collect())
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();
        let texts: Vec<&str> = sorted
            .iter()
            .map(|e| e["output_text"].as_str().unwrap())
            .collect();
        assert_eq!(
            texts,
            [
                "a first",
                "a second",
                "a second, same vtime",
                "a late",
                "b",
                "no moment"
            ]
        );
    }

    #[tokio::test]
    async fn a_stream_error_comes_after_the_events_before_it() {
        let stream_error = json!({"error": "subscriber fell behind"});
        let items = vec![
            Ok(event("2", "3.0", "b")),
            Ok(event("1", "1.0", "a")),
            Ok(stream_error.clone()),
            Ok(event("0", "0.5", "after the error")),
        ];
        let out = ordered(items).await;
        let values: Vec<Value> = out.into_iter().map(Result::unwrap).collect();
        assert_eq!(
            values,
            [event("1", "1.0", "a"), event("2", "3.0", "b"), stream_error]
        );
    }
}
