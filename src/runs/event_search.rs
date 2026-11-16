//! The shared rendering pipeline behind the event-stream commands (`runs logs`,
//! `runs events`, `runs search`, `runs build-logs`), plus the events-search
//! helpers.
//!
//! Each command resolves its own backend up front — `runs events` and
//! `runs search` the events-search endpoint, `runs logs`/`runs build-logs`
//! their GET endpoints — and hands the resulting [`JsonStream`] here.
//! `runs events` and `runs search` first put a closed result in moment order
//! ([`in_moment_order`]). From then on, the commands are identical: every
//! event renders to one output line.
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
}

/// Read a closed search result to its end, and give its events back in
/// moment order: one group for each `input_hash`, the groups by their earliest
/// vtime, and each group's events by vtime.
///
/// An error, or a `Stream_Error` record, ends the read: the events before it
/// come out sorted, then the error, so the command still fails after the
/// rows it received.
pub(super) async fn in_moment_order(mut events: JsonStream) -> JsonStream {
    // The spec requires `moment.input_hash` and `moment.vtime` on every
    // event, and a query cannot drop them: on orbitinghail, release 67,
    // `contains(...).narrow(["output_text"])` still returns each event's
    // moment.
    let mut keyed: Vec<(String, Option<VTime>, Value)> = Vec::new();
    let mut earliest: HashMap<String, VTime> = HashMap::new();
    let mut error = None;
    while let Some(item) = events.next().await {
        match item {
            Ok(event) if stream_error_message(&event).is_none() => {
                let moment = &event["moment"];
                let hash = moment["input_hash"].as_str().unwrap_or_default();
                let vtime = VTime::from_json(&moment["vtime"]);
                if let Some(vtime) = vtime {
                    match earliest.get_mut(hash) {
                        Some(first) => *first = (*first).min(vtime),
                        None => {
                            earliest.insert(hash.to_string(), vtime);
                        }
                    }
                }
                keyed.push((hash.to_string(), vtime, event));
            }
            stop => {
                error = Some(stop);
                break;
            }
        }
    }
    // The sort is stable, so events at one moment keep their server order.
    keyed.sort_by(|(a_hash, a_vtime, _), (b_hash, b_vtime, _)| {
        (earliest.get(a_hash), a_hash, a_vtime).cmp(&(earliest.get(b_hash), b_hash, b_vtime))
    });
    let events = keyed
        .into_iter()
        .map(|(_, _, event)| Ok(event))
        .chain(error);
    stream::iter(events).boxed()
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

    fn ordered(events: Vec<Value>) -> Vec<Value> {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                in_moment_order(stream::iter(events.into_iter().map(Ok)).boxed())
                    .await
                    .map(Result::unwrap)
                    .collect()
                    .await
            })
    }

    #[test]
    fn events_group_by_input_hash_in_vtime_order() {
        let events = vec![
            event("-1", "5.0", "a late"),
            event("2", "3.0", "b"),
            event("-1", "1.0", "a first"),
            event("-1", "2.0", "a second"),
            event("-1", "2.0", "a second, same vtime"),
        ];
        let sorted = ordered(events);
        let texts: Vec<&str> = sorted
            .iter()
            .map(|e| e["output_text"].as_str().unwrap())
            .collect();
        assert_eq!(
            texts,
            ["a first", "a second", "a second, same vtime", "a late", "b"]
        );
    }

    /// Each `input_hash` is one contiguous group, the groups rise by their
    /// earliest vtime, the vtimes rise in a group, and events at one moment
    /// keep their input order.
    #[hegel::test]
    fn the_order_groups_each_input_hash_by_vtime(tc: hegel::TestCase) {
        let n = tc.draw(hegel::generators::integers::<usize>().max_value(30));
        let mut events = Vec::new();
        for index in 0..n {
            let hash = tc.draw(
                hegel::generators::integers::<i8>()
                    .min_value(-2)
                    .max_value(2),
            );
            let ticks = tc.draw(hegel::generators::integers::<u64>().max_value(8));
            let vtime = VTime::from_seconds(ticks as f64 / 4.0).unwrap();
            events.push(json!({
                "moment": {"input_hash": hash.to_string(), "vtime": vtime},
                "index": index,
            }));
        }
        let sorted = ordered(events);
        assert_eq!(sorted.len(), n);
        let key = |e: &Value| {
            (
                e["moment"]["input_hash"].as_str().unwrap().to_string(),
                VTime::from_json(&e["moment"]["vtime"]).unwrap(),
                e["index"].as_u64().unwrap(),
            )
        };
        let mut finished: Vec<String> = Vec::new();
        let mut group_starts: Vec<VTime> = Vec::new();
        for pair in sorted.windows(2) {
            let (a_hash, a_vtime, a_index) = key(&pair[0]);
            let (b_hash, b_vtime, b_index) = key(&pair[1]);
            if a_hash == b_hash {
                assert!(a_vtime < b_vtime || (a_vtime == b_vtime && a_index < b_index));
            } else {
                assert!(
                    !finished.contains(&b_hash),
                    "{b_hash} split into two groups"
                );
                finished.push(a_hash);
            }
        }
        // The first event of each group is its earliest, and the groups rise.
        let mut last_hash = None;
        for e in &sorted {
            let (hash, vtime, _) = key(e);
            if last_hash.as_ref() != Some(&hash) {
                group_starts.push(vtime);
                last_hash = Some(hash);
            }
        }
        assert!(group_starts.is_sorted());
    }

    #[test]
    fn a_stream_error_comes_after_the_events_before_it() {
        let stream_error = json!({"error": "subscriber fell behind"});
        let events = vec![
            event("2", "3.0", "b"),
            event("1", "1.0", "a"),
            stream_error.clone(),
            event("0", "0.5", "after the error"),
        ];
        assert_eq!(
            ordered(events),
            [event("1", "1.0", "a"), event("2", "3.0", "b"), stream_error]
        );
    }
}
