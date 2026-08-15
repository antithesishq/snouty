//! The engine behind `runs events` and `runs search`.
//!
//! Both subcommands build an [`EventSearch`] — from substring needles or a
//! raw event-set DSL query — and hand it to [`execute`], which dispatches to
//! one of two backends and prints whatever the server returns through one
//! pipeline:
//!
//! - [`search_backend`]: `POST /runs/{run_id}/events/search`, which runs an
//!   event-set DSL pipeline server-side.
//! - [`events_backend`]: `GET /runs/{run_id}/events`, the legacy endpoint —
//!   conceptually the subset of the search endpoint that every tenant
//!   serves: one substring, `limit` honored server-side. Retiring it means
//!   deleting that function and the dispatch arms that reach it.
//!
//! Neither path filters client-side. The output of a server-side filter IS
//! the result: the server returns a capped subset of the matches, so
//! re-filtering that subset locally silently drops events that did match —
//! absence from the subset is not absence from the run.

use std::io::{BufWriter, Write};
use std::num::NonZeroU64;

use color_eyre::Section;
use color_eyre::eyre::Result;
use serde_json::json;

use crate::api::{AntithesisApi, ByteStream, EventSearchOptions};
use crate::error::{api_error_status, user_error};
use crate::event_set_dsl;
use crate::features::{self, Feature};
use crate::render::sanitize;

use super::{
    ErrorRows, NdjsonLine, RenderedEventEntry, explain_run_scoped_error, render_event_entry,
    stream_ndjson_lines_capped,
};

/// What to match, independent of which backend runs it.
pub(super) enum EventQuery {
    /// A raw event-set DSL pipeline (`runs search`). Only the search backend
    /// can run one. `follow` keeps the connection open for new matches
    /// (`is_streaming`); it lives on this variant because streaming is a
    /// search-backend concept — the events endpoint always closes.
    Dsl { query: String, follow: bool },
    /// Substrings joined with AND, case-insensitive (`runs events`). The
    /// search backend compiles them into one DSL filter
    /// ([`event_set_dsl::substring_filter`]); the events backend takes
    /// exactly one, as its `q` parameter.
    Needles(Vec<String>),
}

/// One run-scoped event search, ready to dispatch to a backend.
pub(super) struct EventSearch {
    pub query: EventQuery,
    /// Cap on returned events. `None` leaves the backend's own default in
    /// charge: the search backend's request body always names a limit
    /// (defaulting to [`crate::api::SEARCH_DEFAULT_LIMIT`]); the events
    /// backend is sent no `limit` parameter at all and applies the server's
    /// default.
    pub limit: Option<NonZeroU64>,
}

/// Run `search` against `run_id` and print every event the server returns —
/// raw NDJSON with `json`, one `HASH VTIME SOURCE OUTPUT` line per event
/// otherwise. Unbuffered either way: on a live run the search backend holds
/// the connection open, so each row is flushed as it arrives.
pub(super) async fn execute(
    api: &AntithesisApi,
    run_id: &str,
    search: EventSearch,
    json: bool,
) -> Result<()> {
    // A user-written DSL pipeline can reshape rows into any object,
    // `{"error": ...}` included, so its stream must not guess that such a
    // row is the server's Stream_Error signal. The needles query cannot
    // reshape (snouty generates a bare filter), so it keeps the abort.
    let error_rows = match &search.query {
        EventQuery::Dsl { .. } => ErrorRows::Data,
        EventQuery::Needles(_) => ErrorRows::Abort,
    };
    let (stream, cap) = dispatch(api, run_id, &search).await?;
    print_events(
        stream.into_inner(),
        cap,
        error_rows,
        json,
        &empty_message(&search.query),
    )
    .await
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
    let opts = EventSearchOptions {
        validate_only: true,
        ..Default::default()
    };
    if let Err(err) = api.search_run_events_query(run_id, query, opts).await {
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

/// Fetch the server-filtered stream for `search`, plus the cap the pipeline
/// must enforce on it (`None` when the server enforces the limit itself).
///
/// A DSL query can only run on the search backend. Needles prefer it too —
/// it ANDs them all server-side against the whole raw event JSON, so
/// structured events that carry no `output_text` stay findable, however many
/// needles there are — but it is gated behind [`Feature::RunsSearch`] (the
/// default until the server honors the search contract), and a tenant that
/// predates it (pre-58.11) 404s it. The events backend serves those cases,
/// when its one-substring shape can express the query at all.
async fn dispatch(
    api: &AntithesisApi,
    run_id: &str,
    search: &EventSearch,
) -> Result<(ByteStream, Option<NonZeroU64>)> {
    match &search.query {
        EventQuery::Dsl { query, follow } => {
            match search_backend(api, run_id, query, search.limit, *follow).await {
                Ok(fetched) => Ok(fetched),
                Err(err) => Err(explain_search_error(api, run_id, err).await),
            }
        }
        EventQuery::Needles(needles) => {
            let gated = !features::is_enabled(Feature::RunsSearch);
            if !gated {
                let query = event_set_dsl::substring_filter(needles);
                match search_backend(api, run_id, &query, search.limit, false).await {
                    Ok(fetched) => return Ok(fetched),
                    // A 404 means the tenant predates the endpoint, or the
                    // run id is bad. With one needle the events backend below
                    // tells the two apart itself: it reports "run not found"
                    // for a bad id and otherwise serves the events. With
                    // several needles it cannot run the query at all, so
                    // settle the ambiguity here with the run probe
                    // `explain_run_scoped_error` makes.
                    Err(err) if api_error_status(&err) == Some(404) => {
                        if needles.len() > 1 {
                            let err = explain_run_scoped_error(api, run_id, err).await;
                            return Err(match api_error_status(&err) {
                                // The probe found the run, so the 404 was
                                // the endpoint's absence.
                                Some(404) => multi_needle_error(gated),
                                _ => err,
                            });
                        }
                    }
                    // Any other failure is the search endpoint's own answer;
                    // there is nothing to fall back from.
                    Err(err) => return Err(explain_run_scoped_error(api, run_id, err).await),
                }
            }
            match needles.as_slice() {
                [needle] => match events_backend(api, run_id, needle, search.limit).await {
                    Ok(fetched) => Ok(fetched),
                    Err(err) => Err(explain_run_scoped_error(api, run_id, err).await),
                },
                // The command already rejected an empty needle list, so this
                // is the several-needles case.
                _ => Err(multi_needle_error(gated)),
            }
        }
    }
}

/// `POST /runs/{run_id}/events/search`. The server is supposed to end the
/// stream at the limit the request names, but it ignores the limit and keeps
/// streaming matches past it (observed on tenant releases 58.11 through
/// 60.1; part of why [`Feature::RunsSearch`] gates this backend) — so the
/// returned cap tells the pipeline to enforce that limit, the caller's or
/// the request default of 50, itself. The cap applies with and without
/// `follow`: the documented contract terminates the stream at `limit`
/// whether or not `is_streaming` is set.
async fn search_backend(
    api: &AntithesisApi,
    run_id: &str,
    query: &str,
    limit: Option<NonZeroU64>,
    follow: bool,
) -> Result<(ByteStream, Option<NonZeroU64>)> {
    let opts = EventSearchOptions {
        is_streaming: follow,
        validate_only: false,
        limit,
    };
    let stream = api.search_run_events_query(run_id, query, opts).await?;
    Ok((stream, Some(opts.effective_limit())))
}

/// `GET /runs/{run_id}/events`: one substring, `limit` enforced server-side
/// — no cap for the pipeline to enforce.
async fn events_backend(
    api: &AntithesisApi,
    run_id: &str,
    needle: &str,
    limit: Option<NonZeroU64>,
) -> Result<(ByteStream, Option<NonZeroU64>)> {
    let stream = api.search_run_events(run_id, needle, limit).await?;
    Ok((stream, None))
}

/// The refusal for several needles when only the events backend can serve
/// the search. That endpoint matches ONE substring. The removed alternative
/// — matching the longest needle server-side and AND-filtering the rest
/// client-side — silently dropped true matches: the server returns a capped
/// subset, and a match the cap evicted vanished with no signal. Refusing
/// keeps multi-needle results trustworthy: they always come from the search
/// backend, which ANDs every needle server-side.
///
/// `gated` says why the search backend was unavailable: the feature is off,
/// or (false) the tenant does not serve the endpoint.
fn multi_needle_error(gated: bool) -> color_eyre::eyre::Report {
    let err = user_error("multiple search terms require the events-search API");
    if gated {
        err.note(format!(
            "that API is behind the `{}` unstable feature",
            Feature::RUNS_SEARCH
        ))
        .suggestion(format!(
            "search a single term, or set {}={}",
            features::UNSTABLE_FEATURES_VAR_NAME,
            Feature::RUNS_SEARCH
        ))
    } else {
        err.note(
            "the run exists but this tenant does not serve the events-search API, \
             which ships with tenant release 58.11",
        )
        .suggestion("search a single term")
    }
}

/// Like [`explain_run_scoped_error`], for the search backend. A 404 that
/// survives the probe means the run exists but the tenant does not serve the
/// endpoint, which ships with tenant release 58.11. A 400 is the server
/// rejecting the request — usually the query, but an out-of-range `limit`
/// answers 400 too, so the note must not blame the query alone (the server's
/// own message, shown above the note, says which it was).
async fn explain_search_error(
    api: &AntithesisApi,
    run_id: &str,
    err: color_eyre::eyre::Report,
) -> color_eyre::eyre::Report {
    let err = explain_run_scoped_error(api, run_id, err).await;
    match api_error_status(&err) {
        Some(404) => err.note(
            "the run exists but this tenant does not serve the events search API, \
             which ships with tenant release 58.11",
        ),
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

/// Print every line of the (already server-filtered) stream, one line out
/// per line in.
///
/// Each row is flushed as it arrives: on a live run the search backend holds
/// the connection open (with and without `is_streaming`), so rows must not
/// sit in the buffer waiting for an EOF that may never come. The cap is the
/// ONLY early exit: when the server has returned fewer events and holds the
/// connection open, keep waiting rather than guess that the result is
/// complete. Per the contract a non-streaming request is done when the
/// SERVER closes the stream — today it wrongly holds non-streaming
/// connections to live runs open (part of why [`Feature::RunsSearch`] exists),
/// and that bug is the server's to fix, not one to paper over with a
/// heuristic here.
async fn print_events<S, C>(
    stream: S,
    cap: Option<NonZeroU64>,
    error_rows: ErrorRows,
    json: bool,
    empty_message: &str,
) -> Result<()>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    let mut stdout = BufWriter::new(std::io::stdout().lock());
    let seen = stream_ndjson_lines_capped(stream, cap.map(NonZeroU64::get), error_rows, |line| {
        writeln!(stdout, "{}", render_line(&line, json))?;
        stdout.flush()?;
        Ok(())
    })
    .await?;

    // Only a successfully-empty stream earns the friendly empty state; a
    // mid-stream error propagated above instead.
    if seen == 0 && !json {
        writeln!(stdout, "{empty_message}")?;
        stdout.flush()?;
    }
    Ok(())
}

/// The text one stream line prints as. `--json` passes the line through
/// (vtime already normalized to an exact JSON number by the stream). Human
/// mode renders an event as one `HASH VTIME SOURCE OUTPUT` line; a row
/// reshaped by map/narrow/fold into an arbitrary object shows its JSON
/// itself rather than empty columns, and an undecodable line surfaces
/// sanitized rather than being dropped.
fn render_line(line: &NdjsonLine<'_>, json: bool) -> String {
    match line {
        NdjsonLine::Entry(entry) => {
            if json {
                entry.to_string()
            } else if entry.get("moment").is_some() && entry.get("source").is_some() {
                event_line(&render_event_entry(entry))
            } else {
                // A row reshaped by map/narrow/fold: without the source
                // envelope the event columns would render half-empty, so the
                // row's own JSON is the rendering — even when a `moment`
                // survived the reshape.
                sanitize(&entry.to_string())
            }
        }
        NdjsonLine::Raw(raw) => {
            if json {
                (*raw).to_string()
            } else {
                sanitize(raw)
            }
        }
    }
}

/// One event as one line: the decoded HASH, VTIME, SOURCE and OUTPUT fields,
/// space-separated. Decoded (see [`render_event_entry`]) so the line shows
/// what the raw JSON escapes — a quote in the output prints as `"`, not `\"`.
fn event_line(rendered: &RenderedEventEntry) -> String {
    format!(
        "{} {} {} {}",
        rendered.input_hash, rendered.vtime, rendered.source, rendered.output
    )
}

/// The human-mode empty state. Needles are echoed back — the message doubles
/// as a record of what was searched — but a DSL query is not: it is long,
/// and already on the user's command line.
fn empty_message(query: &EventQuery) -> String {
    match query {
        EventQuery::Dsl { .. } => "No events matched the query.".to_string(),
        EventQuery::Needles(needles) => {
            format!("No events matched \"{}\".", needles.join(" "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runs::classify_line;

    fn entry(line: &str) -> serde_json::Value {
        let NdjsonLine::Entry(entry) = classify_line(line) else {
            panic!("line should classify as an entry: {line}");
        };
        entry
    }

    #[test]
    fn render_line_renders_an_event_as_one_decoded_line() {
        // The decoded rendering unescapes the raw JSON: a quote copied from
        // the output prints literally, and the escaped form never appears.
        let line = r#"{"moment":{"input_hash":"42","vtime":"1.5"},"source":{"container":"app","name":"app","stream":"out"},"output_text":"msg \"starting\""}"#;
        let entry = entry(line);
        let human = render_line(&NdjsonLine::Entry(entry.clone()), false);
        assert_eq!(human, r#"42 1.5 [app:out] msg "starting""#);
        // `--json` passes the entry through, vtime normalized to a number.
        let json = render_line(&NdjsonLine::Entry(entry), true);
        assert!(json.contains(r#""vtime":1.5"#), "got: {json}");
        assert!(json.contains(r#"msg \"starting\""#), "got: {json}");
    }

    #[test]
    fn render_line_requires_the_source_envelope_for_the_event_form() {
        // narrow/map can keep `moment` while dropping the rest; without the
        // source envelope the event columns would render half-empty, so the
        // row prints as its own JSON instead.
        let entry = entry(r#"{"moment":{"input_hash":"42","vtime":"1.5"},"KvstoreBytesUsed":"7"}"#);
        let human = render_line(&NdjsonLine::Entry(entry), false);
        assert_eq!(
            human,
            r#"{"moment":{"input_hash":"42","vtime":1.5},"KvstoreBytesUsed":"7"}"#
        );
    }

    #[test]
    fn render_line_shows_reshaped_rows_as_their_own_json() {
        // map/narrow/fold reshape rows into arbitrary objects with no
        // `moment`; the JSON itself prints rather than empty columns.
        let entry = entry(r#"{"count":3,"container":"etcd0"}"#);
        let human = render_line(&NdjsonLine::Entry(entry), false);
        // serde_json's preserve_order keeps the server's key order.
        assert_eq!(human, r#"{"count":3,"container":"etcd0"}"#);
    }

    #[test]
    fn render_line_sanitizes_raw_lines_in_human_mode() {
        let raw = NdjsonLine::Raw("not json \x1b[31mred\x1b[0m");
        assert_eq!(render_line(&raw, false), r"not json \x1B[31mred\x1B[0m");
        // `--json` is a passthrough: the line is the server's to garble.
        assert_eq!(render_line(&raw, true), "not json \x1b[31mred\x1b[0m");
    }

    #[test]
    fn empty_message_echoes_needles_but_not_the_query() {
        assert_eq!(
            empty_message(&EventQuery::Needles(vec![
                "starting".to_string(),
                "nonexistent".to_string()
            ])),
            r#"No events matched "starting nonexistent"."#
        );
        assert_eq!(
            empty_message(&EventQuery::Dsl {
                query: "contains({output_text: \"x\"})".to_string(),
                follow: false,
            }),
            "No events matched the query."
        );
    }

    #[test]
    fn multi_needle_error_names_the_fix_for_each_cause() {
        let gated = format!("{:?}", multi_needle_error(true));
        assert!(
            gated.contains("multiple search terms require the events-search API"),
            "{gated}"
        );
        assert!(
            gated.contains("SNOUTY_UNSTABLE_FEATURES=runs-search"),
            "{gated}"
        );

        let tenant = format!("{:?}", multi_needle_error(false));
        assert!(
            tenant.contains("does not serve the events-search API"),
            "{tenant}"
        );
        assert!(tenant.contains("search a single term"), "{tenant}");
        // The feature cannot fix a tenant that lacks the endpoint, so the
        // gate must not be suggested here.
        assert!(!tenant.contains("SNOUTY_UNSTABLE_FEATURES"), "{tenant}");
    }
}
