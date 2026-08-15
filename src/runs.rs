use std::fmt::{self, Display, Formatter};
use std::io::{BufWriter, IsTerminal, Read, Write};
use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use color_eyre::Section;
use color_eyre::eyre::{Result, WrapErr, eyre};
use futures_util::{StreamExt, TryStreamExt};
use indexmap::IndexMap;
use indexmap::map::Entry;
use log::debug;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use chrono::{DateTime, Local, Utc};

use crate::api::{
    AntithesisApi, Event, EventProperty, LogsBegin, Moment, NonEventProperty, Property,
    PropertyStatus, RunDetail, RunStatus, RunSummary, RunsFilterOptions,
};
use crate::cli::{RunsCommands, RunsListArgs, RunsSearchArgs};
use crate::error::{api_error_status, user_error};
use crate::render::{OutputOptions, render_kv, sanitize, sanitize_multiline, wrap_text};
use crate::settings::Settings;
use crate::time::HumanDuration;
use crate::vtime::VTime;

mod event_search;

use event_search::{EventQuery, EventSearch};

/// `print!`/`println!`, but routed through `write!`/`writeln!` to stdout so a
/// closed pipe (e.g. `snouty runs list | head`) surfaces as an `io::Error` the
/// caller propagates with `?` — `println!` would panic instead. Each call
/// evaluates to an `io::Result<()>`, so every use must be `?`-ed.
macro_rules! out {
    ($($arg:tt)*) => {{ write!(std::io::stdout(), $($arg)*) }};
}
macro_rules! outln {
    () => {{ writeln!(std::io::stdout()) }};
    ($($arg:tt)*) => {{ writeln!(std::io::stdout(), $($arg)*) }};
}

/// Event stream classification. Variants match the canonical values that
/// appear in an event's `source.stream` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
    Info,
    Error,
}

impl Stream {
    /// Three-character display abbreviation used in the logs viewer.
    pub fn abbreviated(self) -> &'static str {
        match self {
            Self::Stdout => "out",
            Self::Stderr => "err",
            Self::Info => "inf",
            Self::Error => "err",
        }
    }
}

impl std::str::FromStr for Stream {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        // Accept the short forms too: the events/logs API reports app
        // stdout/stderr as `out`/`err` (see `abbreviated`), so the logs viewer
        // can normalize either form when rendering a stream label.
        match s {
            "stdout" | "out" => Ok(Self::Stdout),
            "stderr" | "err" => Ok(Self::Stderr),
            "info" | "inf" => Ok(Self::Info),
            "error" => Ok(Self::Error),
            other => Err(format!(
                "invalid stream '{other}' (expected one of: stdout, stderr, info, error)"
            )),
        }
    }
}

pub async fn cmd_runs(
    command: Option<RunsCommands>,
    settings: &Settings,
    output: OutputOptions,
) -> Result<()> {
    // `--detail` produces human formatting, so it can't combine with `--json`.
    // It rides on more than one subcommand (`runs list`, `runs properties`), so
    // the conflict is checked once here — a global `--json` vs a per-subcommand
    // flag can't be expressed with clap's `conflicts_with`.
    let detail = match &command {
        Some(RunsCommands::List(args)) => args.detail,
        Some(RunsCommands::Properties { detail, .. }) => *detail,
        _ => false,
    };
    reject_detail_with_json(output.json, detail)?;

    match command {
        None => cmd_runs_list(RunsListArgs::default(), settings, output).await,
        Some(RunsCommands::List(args)) => cmd_runs_list(args, settings, output).await,
        Some(RunsCommands::Show { run_id, web }) => {
            cmd_runs_show(&run_id, web, settings, output).await
        }
        Some(RunsCommands::Wait {
            run_id,
            poll_interval,
            timeout,
        }) => {
            cmd_runs_wait(
                &run_id,
                poll_interval.as_duration(),
                timeout,
                settings,
                output,
            )
            .await
        }
        Some(RunsCommands::Properties {
            run_id,
            passing,
            failing,
            name,
            group,
            detail,
        }) => {
            let status = if passing {
                Some(PropertyStatus::Passing)
            } else if failing {
                Some(PropertyStatus::Failing)
            } else {
                None
            };
            let filter = PropertyFilter {
                status,
                name: name.as_deref(),
                group: group.as_deref(),
            };
            cmd_runs_properties(&run_id, filter, detail, settings, output).await
        }
        Some(RunsCommands::BuildLogs { run_id }) => {
            cmd_runs_build_logs(&run_id, settings, output).await
        }
        Some(RunsCommands::Logs {
            run_id,
            input_hash,
            vtime,
            begin_vtime,
            begin_input_hash,
            raw,
        }) => {
            let moment = Moment { input_hash, vtime };
            // clap enforces `begin_input_hash requires begin_vtime`, so mapping
            // over the vtime cannot drop a supplied hash.
            let begin = begin_vtime.map(|vtime| LogsBegin {
                vtime,
                input_hash: begin_input_hash,
            });
            cmd_runs_logs(
                &run_id,
                moment,
                begin,
                settings,
                LogOutputOptions { output, raw },
            )
            .await
        }
        Some(RunsCommands::Exec {
            run_id,
            input_hash,
            vtime,
            script,
            timeout,
        }) => {
            let moment = Moment { input_hash, vtime };
            // The flag is a whole number of seconds; carry it as a Duration
            // from here on.
            let timeout = Duration::from_secs(timeout);
            cmd_runs_exec(&run_id, moment, script, timeout, settings, output).await
        }
        Some(RunsCommands::Events {
            run_id,
            mut matches,
            limit,
            query,
        }) => {
            // `-m/--match` is the documented form; the trailing positional
            // `query` is a backward-compatible alias whose terms are additional
            // needles. Merge both into a single needle list.
            matches.extend(query);
            cmd_runs_events(&run_id, &matches, limit, settings, output).await
        }
        Some(RunsCommands::Search(args)) => cmd_runs_search(args, settings, output).await,
    }
}

async fn cmd_runs_list(
    args: RunsListArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("listing runs");

    let api = AntithesisApi::new(settings, verbose)?;

    // clap parsed and validated the filter flags into their real types, so the
    // options struct is built directly with no further string parsing here.
    let opts = RunsFilterOptions {
        status: args.status,
        launcher: args.launcher,
        created_after: args.created_after,
        created_before: args.created_before,
    };

    // The API caps `limit` at 100, so request only as many as we'll display and
    // let the server do the trimming. For limits above 100 we still paginate,
    // capping the total client-side with `.take(limit)`.
    let page_limit = args.limit.clamp(1, 100) as u64;

    // Server returns runs newest-first; .take(limit) short-circuits pagination
    // so we don't materialise the entire run history just to drop most of it.
    let mut runs: Vec<RunSummary> = api
        .stream_runs_filtered(&opts, page_limit)
        .take(args.limit)
        .try_collect::<Vec<_>>()
        .await?;

    runs.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then(a.status.cmp(&b.status))
    });

    if json {
        for run in &runs {
            outln!("{}", serde_json::to_string(run)?)?;
        }
        return Ok(());
    }

    if runs.is_empty() {
        outln!("No runs found.")?;
        return Ok(());
    }

    if args.detail {
        out!("{}", render_runs_detail(&runs))?;
    } else {
        let width = terminal_width();
        outln!("{}", render_runs_table(&runs, width))?;
    }
    Ok(())
}

/// Width budget for terminal-aware rendering. When stdout is a tty we use its
/// real column count; otherwise (a pipe or file) we return `usize::MAX` so the
/// truncating/wrapping call sites become no-ops and full cell content reaches
/// the consumer — `snouty runs properties RUN | grep '<long name>'` must not
/// silently miss a row whose NAME was wrapped or ellipsized to fit a screen.
fn terminal_width() -> usize {
    let term = console::Term::stdout();
    if !term.is_term() {
        return usize::MAX;
    }
    term.size().1 as usize
}

/// Compact relative age for the runs table ("21h ago", "2d ago"), trading
/// prose ("21 hours ago") for column width. Rough by design: largest whole
/// unit only. Future timestamps (clock skew) clamp to "0s ago".
fn relative_time(then: DateTime<Utc>) -> String {
    let secs = (Utc::now() - then).num_seconds().max(0);
    let (value, unit) = match secs {
        s if s < 60 => (s, "s"),
        s if s < 3600 => (s / 60, "m"),
        s if s < 86_400 => (s / 3600, "h"),
        s if s < 7 * 86_400 => (s / 86_400, "d"),
        s if s < 30 * 86_400 => (s / (7 * 86_400), "w"),
        s if s < 365 * 86_400 => (s / (30 * 86_400), "mo"),
        s => (s / (365 * 86_400), "y"),
    };
    format!("{value}{unit} ago")
}

/// Format an absolute timestamp in the user's local timezone, without a
/// timezone suffix (the times in snouty's output are always local, so showing
/// the offset would just be noise). Example: `2026-05-27 08:25:13`.
fn format_local(dt: DateTime<Utc>) -> String {
    dt.with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// Reformat an RFC 3339 timestamp string into the local, suffix-less format.
/// Falls back to the original string if it can't be parsed.
fn format_local_str(raw: &str) -> String {
    match DateTime::parse_from_rfc3339(raw) {
        Ok(dt) => format_local(dt.with_timezone(&Utc)),
        Err(_) => raw.to_string(),
    }
}

/// The requested run duration (`antithesis.duration`, a count of minutes),
/// rendered in the same `1h30m` vocabulary the launcher accepts via
/// [`HumanDuration`]. A value the backend somehow stored in a form we can't
/// parse falls back to the raw string, so it still shows something truthful
/// rather than vanishing.
fn format_requested_duration(raw: &str) -> String {
    raw.parse::<HumanDuration>()
        .map_or_else(|_| raw.to_string(), |d| d.to_string())
}

/// Wall-clock time the run was (or has been) active, rendered through
/// [`HumanDuration`] in the same `h`/`m`/`s` units as the requested duration
/// beside it. A still-running run counts up to `end` (`Utc::now()` at the call
/// site). Returns `None` on clock skew (a negative span).
fn elapsed_duration(started: DateTime<Utc>, end: DateTime<Utc>) -> Option<HumanDuration> {
    let secs = (end - started).num_seconds();
    (secs >= 0).then(|| HumanDuration::from_seconds(secs as u64))
}

async fn cmd_runs_show(
    run_id: &str,
    web: bool,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("showing run: {}", run_id);

    let api = AntithesisApi::new(settings, verbose)?;
    let run = match api.get_run(run_id).await {
        Ok(run) => run,
        // A 404 here is unambiguous: the run id is bad. Say so instead of leaking
        // a bare "API error: 404 Not Found". Other errors pass through untouched.
        Err(err) => return Err(explain_run_not_found(run_id, err)),
    };

    if web {
        return open_run_report(&run, json);
    }

    if json {
        outln!("{}", serde_json::to_string_pretty(&run)?)?;
    } else {
        print_run_detail(&run)?;
    }

    Ok(())
}

/// `runs show --web`: open the run's triage report in a browser. With `--json`,
/// emit the URL instead of launching anything so scripts can capture it.
fn open_run_report(run: &RunDetail, json: bool) -> Result<()> {
    let url = run
        .links
        .as_ref()
        .and_then(|l| l.triage_report.as_deref())
        .ok_or_else(|| {
            user_error(format!("no report available for run {}", run.run_id)).note(format!(
                "reports are generated when a run completes; this run is {}",
                run.status
            ))
        })?;

    if json {
        outln!("{}", serde_json::json!({ "url": url }))?;
        return Ok(());
    }

    let launched = crate::browser::open_in_browser(url).is_ok();
    if launched {
        outln!("Opening report for run {}…", run.run_id)?;
        outln!("If your browser didn't open, manually visit:")?;
        outln!("  {url}")?;
    } else {
        outln!("Open this URL to view the report:")?;
        outln!("  {url}")?;
    }
    Ok(())
}

/// `runs wait`: poll the run until it reaches a terminal state, then report
/// the final status. The client is uncached (see
/// [`crate::api::ResponseCache::Disabled`]) so every poll observes fresh
/// state, and `timeout` bounds the whole wait — including any in-flight
/// request, since the HTTP client has no read timeout (see `CONNECT_TIMEOUT`
/// in api.rs) — so a hung request cannot outlive it.
async fn cmd_runs_wait(
    run_id: &str,
    poll_interval: Duration,
    timeout: Option<HumanDuration>,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("waiting for run: {}", run_id);

    let api = AntithesisApi::new_uncached(settings, verbose)?;
    let wait = wait_for_run(&api, run_id, poll_interval);
    let run = match timeout {
        Some(limit) => match tokio::time::timeout(limit.as_duration(), wait).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(user_error(format!(
                    "timed out waiting for run {run_id} after {limit}"
                )));
            }
        },
        None => wait.await?,
    };

    if json {
        outln!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "run_id": run.run_id,
                "status": run.status,
            }))?
        )?;
    } else {
        outln!("run {} is {}", run.run_id, run.status)?;
    }

    Ok(())
}

/// Poll `get_run` every `poll_interval` until the run reaches a terminal
/// state. Status changes and warnings go to stderr; stdout stays reserved for
/// the final result. A run that reports `unknown` fails the wait (see
/// [`RunStatus::is_terminal`]).
///
/// Never returns on its own — the caller bounds it with `tokio::time::timeout`.
async fn wait_for_run(
    api: &AntithesisApi,
    run_id: &str,
    poll_interval: Duration,
) -> Result<RunDetail> {
    let mut poll = tokio::time::interval(poll_interval);
    // A poll delayed by a slow response schedules the next one a full
    // interval later, instead of firing early to catch up.
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_status = None;
    let mut warned_over_schedule = false;

    loop {
        poll.tick().await;

        let run = match api.get_run(run_id).await {
            Ok(run) => run,
            // A 404 here is unambiguous, exactly as in `runs show`: the run id
            // is bad, so fail now rather than poll a typo until the timeout.
            Err(err) => return Err(explain_run_not_found(run_id, err)),
        };

        if run.status == RunStatus::Unknown {
            return Err(
                user_error(format!("run {run_id} reported status \"unknown\""))
                    .note("snouty cannot tell whether this run will still make progress")
                    .suggestion(format!("inspect the run with `snouty runs show {run_id}`")),
            );
        }
        if run.status.is_terminal() {
            return Ok(run);
        }

        if last_status != Some(run.status) {
            eprintln!("run {run_id} is {}", run.status);
            last_status = Some(run.status);
        }

        if !warned_over_schedule && let Some(warning) = over_schedule_warning(&run, Utc::now()) {
            eprintln!("warning: {warning}");
            warned_over_schedule = true;
        }
    }
}

/// The warning line for a run active for more than twice its scheduled
/// duration (`antithesis.duration`); the wall clock also spans provisioning,
/// setup, and teardown, so some overshoot is normal. Runs that haven't started
/// or record no usable scheduled duration never warn.
fn over_schedule_warning(run: &RunDetail, now: DateTime<Utc>) -> Option<String> {
    let scheduled = run.requested_duration()?.parse::<HumanDuration>().ok()?;
    let elapsed = elapsed_duration(run.started_at?, run.completed_at.unwrap_or(now))?;
    // A zero schedule would flag every run that has run at all.
    if scheduled.seconds() > 0 && elapsed.seconds() > 2 * scheduled.seconds() {
        Some(format!(
            "run {} has been active for {elapsed}, more than twice its scheduled duration of \
             {scheduled}",
            run.run_id
        ))
    } else {
        None
    }
}

/// Filters applied to `runs properties`. `status` is served by the API; `name`
/// and `group` are case-insensitive substring matches applied client-side.
struct PropertyFilter<'a> {
    status: Option<PropertyStatus>,
    name: Option<&'a str>,
    group: Option<&'a str>,
}

/// `--detail` formats human output, so it conflicts with `--json` (which always
/// emits the full structured data). Reject the combination with a clear error
/// rather than silently ignoring one. Called once from [`cmd_runs`] for whichever
/// subcommand carries `--detail` (`runs list`, `runs properties`).
fn reject_detail_with_json(json: bool, detail: bool) -> Result<()> {
    if json && detail {
        return Err(user_error("--detail and --json cannot be combined")
            .note("--json already emits the full data"));
    }
    Ok(())
}

async fn cmd_runs_properties(
    run_id: &str,
    filter: PropertyFilter<'_>,
    detail: bool,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("listing properties for run: {}", run_id);

    let api = AntithesisApi::new(settings, verbose)?;
    let mut properties = match api
        .stream_run_properties(run_id, filter.status)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(properties) => properties,
        // The properties endpoint 404s both for a bogus run id and for a real
        // run that simply has no triage report yet (only `completed` runs do).
        // Fetch the run to say which, instead of leaking a bare "404 Not Found".
        Err(err) => return Err(explain_properties_error(&api, run_id, err).await),
    };

    // Apply the client-side filters: both group and name are (case-insensitive)
    // substring matches.
    if let Some(group) = filter.group {
        let needle = group.to_lowercase();
        properties
            .retain(|p| property_group(p).is_some_and(|g| g.to_lowercase().contains(&needle)));
    }
    if let Some(name) = filter.name {
        let needle = name.to_lowercase();
        properties.retain(|p| p.name().to_lowercase().contains(&needle));
    }

    properties.sort_by(|a, b| {
        property_group(a)
            .unwrap_or("")
            .cmp(property_group(b).unwrap_or(""))
            .then(a.name().cmp(b.name()))
    });

    if json {
        for property in &properties {
            outln!("{}", serde_json::to_string(property)?)?;
        }
    } else if properties.is_empty() {
        outln!("{}", explain_empty_properties(&api, run_id, &filter).await)?;
    } else if detail {
        outln!("{}", render_properties_detail(&properties))?;
    } else {
        outln!("{}", render_properties_table(&properties))?;
    }

    Ok(())
}

/// The empty-result message, naming whichever filters were active.
fn no_properties_message(filter: &PropertyFilter) -> String {
    let mut parts = Vec::new();
    match filter.status {
        Some(PropertyStatus::Passing) => parts.push("passing".to_string()),
        Some(PropertyStatus::Failing) => parts.push("failing".to_string()),
        None => {}
    }
    if let Some(name) = filter.name {
        parts.push(format!("name containing '{name}'"));
    }
    if let Some(group) = filter.group {
        parts.push(format!("group '{group}'"));
    }
    if parts.is_empty() {
        "No properties found.".to_string()
    } else {
        format!("No properties match ({}).", parts.join(", "))
    }
}

/// The message for an empty (non-JSON) properties result. A *filtered* empty is
/// genuinely "nothing matched"; an *unfiltered* empty often just means the run
/// is incomplete (no triage report yet), so probe the run to say so rather than
/// implying no properties exist.
async fn explain_empty_properties(
    api: &AntithesisApi,
    run_id: &str,
    filter: &PropertyFilter<'_>,
) -> String {
    let unfiltered = filter.status.is_none() && filter.name.is_none() && filter.group.is_none();
    if unfiltered
        && let RunProbe::Exists(run) = probe_run(api, run_id).await
        && run.status != RunStatus::Completed
    {
        return format!(
            "No properties found — this run is {}; properties are generated when a run completes.",
            run.status
        );
    }
    no_properties_message(filter)
}

/// Outcome of probing a run-scoped 404 with `get_run`: does the run itself not
/// exist, or does it exist (with a known status) but the nested endpoint has no
/// data yet? Any non-404 `get_run` failure is reported as a propagating error
/// rather than misattributed to a missing run.
enum RunProbe {
    /// The run id is bad: `get_run` also returned a structured 404.
    NotFound,
    /// The run exists; carries its full detail so callers can tailor the message
    /// (status, and the failure moment for incomplete runs). Boxed to keep the
    /// enum small.
    Exists(Box<RunDetail>),
    /// `get_run` failed for some other reason (timeout, 502, auth). Propagate
    /// this rather than claiming the run doesn't exist.
    ProbeFailed(color_eyre::eyre::Report),
}

/// Probe a run-scoped 404 by fetching the run itself. Only a structured 404 from
/// `get_run` means the run is missing; any other `get_run` error is returned as
/// `ProbeFailed` so callers never misreport a timeout/5xx/auth failure as "run
/// not found".
async fn probe_run(api: &AntithesisApi, run_id: &str) -> RunProbe {
    match api.get_run(run_id).await {
        Ok(run) => RunProbe::Exists(Box::new(run)),
        Err(err) if api_error_status(&err) == Some(404) => RunProbe::NotFound,
        Err(err) => RunProbe::ProbeFailed(err),
    }
}

/// Translate an error from a run-scoped endpoint (show/build-logs/events/logs)
/// into the shared friendly "run not found: X" message when the failure is a
/// 404 for a bad run id. Any non-404 error passes through untouched (full
/// report), so genuine server faults are never masked.
///
/// `show` calls `get_run` directly, so its 404 is already unambiguous; the
/// streaming endpoints get the same treatment so every run-scoped subcommand
/// reports a bad run id identically.
fn explain_run_not_found(run_id: &str, err: color_eyre::eyre::Report) -> color_eyre::eyre::Report {
    if api_error_status(&err) == Some(404) {
        user_error(format!("run not found: {run_id}"))
    } else {
        err
    }
}

/// Like [`explain_run_not_found`] but for endpoints whose 404 is ambiguous: it
/// can mean a bad run id *or* a real run whose nested resource isn't available
/// yet. Probes the run with `get_run` to disambiguate, falling back to "run not
/// found: X" when the run is genuinely missing and otherwise to the original
/// error. Non-404 errors pass through untouched.
async fn explain_run_scoped_error(
    api: &AntithesisApi,
    run_id: &str,
    err: color_eyre::eyre::Report,
) -> color_eyre::eyre::Report {
    if api_error_status(&err) != Some(404) {
        return err;
    }
    match probe_run(api, run_id).await {
        RunProbe::NotFound => user_error(format!("run not found: {run_id}")),
        RunProbe::ProbeFailed(probe_err) => probe_err,
        // The run exists but the endpoint still 404'd — surface the original
        // error rather than guessing why.
        RunProbe::Exists(_) => err,
    }
}

/// Like [`explain_run_scoped_error`], for the logs endpoint. A 404 that
/// survives the probe means the run exists and the hash/vtime pair does not
/// name a moment in it, so say that and point at the command that lists valid
/// moments. The probe already ran inside [`explain_run_scoped_error`]; nothing
/// extra is fetched here.
async fn explain_logs_error(
    api: &AntithesisApi,
    run_id: &str,
    err: color_eyre::eyre::Report,
) -> color_eyre::eyre::Report {
    let err = explain_run_scoped_error(api, run_id, err).await;
    if api_error_status(&err) == Some(404) {
        err.suggestion(format!(
            "the run exists but no moment matches this hash and vtime — list valid moments with \
             `snouty runs events {run_id}`"
        ))
    } else {
        err
    }
}

/// Turn a properties-endpoint failure into a message that explains *why* there
/// are no properties. Only a 404 is rewritten; every other error (auth, network,
/// 5xx) passes through untouched.
async fn explain_properties_error(
    api: &AntithesisApi,
    run_id: &str,
    err: color_eyre::eyre::Report,
) -> color_eyre::eyre::Report {
    if api_error_status(&err) != Some(404) {
        return err;
    }
    // A 404 here means either the run doesn't exist or it has no triage report.
    // Probe the run to tell them apart: a missing run 404s on `get_run` too,
    // while a real run reports its status (its report just isn't available).
    match probe_run(api, run_id).await {
        RunProbe::NotFound => user_error(format!("run not found: {run_id}")),
        RunProbe::ProbeFailed(probe_err) => probe_err,
        // Completed runs are expected to have properties; if one 404s anyway,
        // that's a genuine surprise — keep the original error.
        RunProbe::Exists(run) if run.status == RunStatus::Completed => err,
        RunProbe::Exists(run) => {
            // The message states the error; the *why* and the concrete next steps
            // hang off it as notes. For an incomplete run we already fetched the
            // failure moment while probing, so prefill it into the `runs logs` hint.
            let report = user_error(format!("no properties for run {run_id}"))
                .note(format!(
                    "properties are generated when a run completes; this run is {}",
                    run.status
                ))
                .note(format!("inspect the run with `snouty runs show {run_id}`"));
            // A placeholder 0/0 moment streams no logs, so skip the "view logs"
            // hint there rather than point at an empty stream.
            match run.real_failure_moment() {
                Some(moment) => report.note(format!(
                    "view logs at the failure with `snouty runs logs {run_id} {} {}`",
                    moment.input_hash, moment.vtime
                )),
                None => report,
            }
        }
    }
}

fn property_group(p: &Property) -> Option<&str> {
    match p {
        Property::EventProperty(p) => p.group.as_deref(),
        Property::NonEventProperty(p) => p.group.as_deref(),
    }
}

/// `(examples, counterexamples)` for a property, defaulting a missing count to 0.
fn property_example_counts(p: &Property) -> (u64, u64) {
    let (ex, cex) = match p {
        Property::EventProperty(p) => (p.example_count, p.counterexample_count),
        Property::NonEventProperty(p) => (p.example_count, p.counterexample_count),
    };
    (u64::from(ex.unwrap_or(0)), u64::from(cex.unwrap_or(0)))
}

/// Format a count in short SI-style notation so the EXAMPLES column stays narrow
/// and scannable: values under 1000 print exactly (`0`, `20`, `885`); larger
/// ones use ~3 significant figures with a k/M/G/T suffix (`2.3k`, `13k`, `18M`).
fn format_count_si(n: u64) -> String {
    if n < 1000 {
        return n.to_string();
    }
    const UNITS: [&str; 4] = ["k", "M", "G", "T"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() {
        value /= 1000.0;
        unit += 1;
    }
    // `value` is now in [1, 1000). One decimal below 10 (`2.3k`), none at/above
    // (`13k`). If rounding tips it over a boundary, step up a unit / drop the
    // decimal so we never emit `10.0k` or `1000k`.
    if value < 10.0 {
        let rounded = (value * 10.0).round() / 10.0;
        if rounded >= 10.0 {
            format!("{:.0}{}", rounded, UNITS[unit - 1])
        } else {
            format!("{rounded:.1}{}", UNITS[unit - 1])
        }
    } else {
        let rounded = value.round();
        if rounded >= 1000.0 && unit < UNITS.len() {
            format!("{:.1}{}", rounded / 1000.0, UNITS[unit])
        } else {
            format!("{rounded:.0}{}", UNITS[unit - 1])
        }
    }
}

fn property_status_label(status: PropertyStatus) -> &'static str {
    match status {
        PropertyStatus::Passing => "passing",
        PropertyStatus::Failing => "failing",
    }
}

/// How a wrapped free-text block lays its label out (see [`render_prose_block`]).
enum ProseLayout {
    /// Label sits in a fixed-width column; the first body line follows it and
    /// continuation lines indent to the same column (hanging indent). The body
    /// wraps to `terminal_width - label_col`, floored at `min_body_width`.
    HangingIndent {
        label_col: usize,
        min_body_width: usize,
    },
    /// Label sits on its own line; the body follows at column 0 and wraps to the
    /// full terminal width.
    OwnLine,
}

/// Render a labelled block of free-form prose (e.g. a property/run description):
/// sanitize while keeping real line breaks, drop stray leading/trailing blank
/// lines, and wrap to the terminal so a long paragraph doesn't blow past the
/// screen. Blank interior lines are emitted bare (no padding) in every layout.
/// Returns the empty string when the text has no non-blank lines.
fn render_prose_block(label: &str, text: &str, layout: ProseLayout) -> String {
    let body_width = match layout {
        ProseLayout::HangingIndent {
            label_col,
            min_body_width,
        } => terminal_width()
            .saturating_sub(label_col)
            .max(min_body_width),
        ProseLayout::OwnLine => terminal_width(),
    };
    let wrapped = wrap_text(&sanitize_multiline(text), body_width);
    let lines = trim_blank_edges(&wrapped);
    if lines.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    match layout {
        ProseLayout::HangingIndent { label_col, .. } => {
            for (index, line) in lines.iter().enumerate() {
                if line.is_empty() {
                    out.push('\n');
                } else if index == 0 {
                    out.push_str(&format!("{label:<label_col$}{line}\n"));
                } else {
                    out.push_str(&format!("{:<label_col$}{line}\n", ""));
                }
            }
        }
        ProseLayout::OwnLine => {
            out.push_str(label);
            out.push('\n');
            for line in lines {
                out.push_str(line);
                out.push('\n');
            }
        }
    }
    out
}

/// `runs properties --detail`: the same per-group sections as the summary table,
/// but each property is expanded into its examples (indented beneath it) rather
/// than a one-line row. The group heading is shown once per section, separate
/// from the property details.
fn render_properties_detail(properties: &[Property]) -> String {
    group_property_sections(properties)
        .iter()
        .map(|(title, props)| {
            let blocks = props
                .iter()
                .map(|p| render_property_detail(p))
                .collect::<Vec<_>>()
                .join("\n\n");
            match title {
                Some(title) => format!("{}\n\n{}", sanitize(title), blocks),
                None => blocks,
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// One property's detail within a group section: a `Name`/`Status`/`Details`
/// header (no `Group` line — the section heading carries it) followed by its
/// examples. Every field goes through [`render_field`], so a short value sits
/// inline against the value column while a long or multi-line one drops to an
/// indented block beneath its label.
fn render_property_detail(property: &Property) -> String {
    let mut out = String::new();
    out.push_str(&render_field("Name", &sanitize(property.name())));
    out.push('\n');
    out.push_str(&render_field(
        "Status",
        property_status_label(property.status()),
    ));
    out.push('\n');
    let description = match property {
        Property::EventProperty(p) => p.description.as_deref(),
        Property::NonEventProperty(p) => p.description.as_deref(),
    };
    if let Some(desc) = description {
        // Details is free-form prose, wrapped under the value column so
        // continuation lines hang-indent to match the values above.
        out.push_str(&render_prose_block(
            "Details",
            desc,
            ProseLayout::HangingIndent {
                label_col: PROPERTY_LABEL_WIDTH,
                min_body_width: 20,
            },
        ));
    }
    match property {
        // Event properties have moments — the user feeds a HASH/VTIME into
        // `runs logs` — so the `Examples` field holds a STATUS/HASH/VTIME table
        // (or, when there are none, the inline "unreachable" note).
        Property::EventProperty(p) => {
            out.push_str(&render_field("Examples", &render_moments_table(p)));
        }
        // Non-event "system" properties have no moments; their values show under
        // a `Result` (or, when failing, labelled Counter-examples/Examples).
        Property::NonEventProperty(p) => out.push_str(&render_result(p)),
    }
    out
}

/// The STATUS/HASH/VTIME table for an event property's moments: counterexamples
/// (failing) first, then examples (passing), each ascending by vtime.
fn render_moments_table(p: &EventProperty) -> String {
    let mut rows: Vec<Vec<String>> = Vec::new();
    for event in sorted_by_vtime(&p.counterexamples) {
        rows.push(vec![
            "failing".to_string(),
            sanitize(&event.moment.input_hash),
            event.moment.vtime.to_string(),
        ]);
    }
    for event in sorted_by_vtime(&p.examples) {
        rows.push(vec![
            "passing".to_string(),
            sanitize(&event.moment.input_hash),
            event.moment.vtime.to_string(),
        ]);
    }
    if rows.is_empty() {
        return "(none — property was unreachable)".to_string();
    }
    let headers = vec![
        "STATUS".to_string(),
        "HASH".to_string(),
        "VTIME".to_string(),
    ];
    render_table(&headers, &rows)
}

/// The values of a non-event ("system") property under a `Result` label. A
/// passing property has only example values, shown directly. A failing property
/// also has counterexamples — the values that violated it — so those are
/// labelled and shown first (mirroring the event table's failing-first order)
/// instead of being merged into one unlabelled list where the offending value
/// can't be told from the rest.
fn render_result(p: &NonEventProperty) -> String {
    if p.counterexamples.is_empty() {
        return render_result_values(&p.examples);
    }
    let mut out = render_value_group("Counter-examples", &p.counterexamples);
    if !p.examples.is_empty() {
        out.push('\n');
        out.push_str(&render_value_group("Examples", &p.examples));
    }
    out
}

/// The example values of a passing non-event property under a single `Result`
/// field: a lone scalar (or small object) inline, several values (or a large one)
/// as a block, none as a placeholder.
fn render_result_values(values: &[Value]) -> String {
    match values {
        [] => render_field("Result", "(none)"),
        [one] => render_result_value("Result", one),
        // Several values render as one JSON array; serialize the slice of
        // references directly rather than cloning into an owned `Value::Array`.
        many => render_json_field("Result", &many),
    }
}

/// A labelled group of non-event values for the failing case — the `label` on its
/// own line with each value indented beneath. Always a block (never inline) so the
/// violating counterexamples and the satisfying examples read consistently.
/// Scalars print verbatim; objects/arrays as compact one-line JSON.
fn render_value_group(label: &str, values: &[Value]) -> String {
    let body = values
        .iter()
        .map(value_token)
        .collect::<Vec<_>>()
        .join("\n");
    format!("{label}\n{}", indent_lines(&body, "  "))
}

/// A single non-event value under `label`: a scalar (or small object/array)
/// inline, a large object/array as a pretty-printed block.
fn render_result_value(label: &str, value: &Value) -> String {
    match value {
        Value::Array(_) | Value::Object(_) => render_json_field(label, value),
        scalar => render_field(label, &value_token(scalar)),
    }
}

/// A non-event value as a single inline token: a string sanitised, other scalars
/// via their `Display`, an object/array as compact one-line JSON.
fn value_token(value: &Value) -> String {
    match value {
        Value::String(s) => sanitize(s),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

/// Render a JSON object/array (or a slice of values) under `label`: inline when
/// its compact one-line form fits the value column, otherwise pretty-printed as an
/// indented block. The inline-vs-block layout is delegated to [`render_field`].
fn render_json_field<T: serde::Serialize>(label: &str, value: &T) -> String {
    let compact = serde_json::to_string(value).unwrap_or_default();
    if fits_inline(label, &compact) {
        render_field(label, &compact)
    } else {
        render_field(
            label,
            &serde_json::to_string_pretty(value).unwrap_or_default(),
        )
    }
}

/// Render a `label` + `value` field in a property detail block. A single-line
/// value that fits sits inline, aligned to the [`PROPERTY_LABEL_WIDTH`] value
/// column (like `Name`/`Status`); a taller or wider value goes on the lines below
/// the label, indented. No trailing `:` — the column does the separating, matching
/// every other field.
fn render_field(label: &str, value: &str) -> String {
    if fits_inline(label, value) {
        format!("{label:<width$}{value}", width = PROPERTY_LABEL_WIDTH)
    } else {
        format!("{label}\n{}", indent_lines(value, "  "))
    }
}

/// Whether `value` can sit inline after `label` in the value column: it must be a
/// single line, the label must leave at least one space before the column, and the
/// whole line must fit (capped at 100 cols so piped output — where the width is
/// unbounded — still inlines only genuinely short values).
fn fits_inline(label: &str, value: &str) -> bool {
    !value.contains('\n')
        && label.chars().count() < PROPERTY_LABEL_WIDTH
        && PROPERTY_LABEL_WIDTH + value.chars().count() <= terminal_width().min(100)
}

/// Width of the label column in `render_property_detail` (`"Details   "`).
const PROPERTY_LABEL_WIDTH: usize = 10;

/// Drop leading and trailing blank lines, keeping interior ones.
fn trim_blank_edges(lines: &[String]) -> &[String] {
    let start = lines.iter().position(|l| !l.is_empty()).unwrap_or(0);
    let end = lines
        .iter()
        .rposition(|l| !l.is_empty())
        .map_or(0, |i| i + 1);
    lines.get(start..end).unwrap_or(&[])
}

/// Return references to `events` ordered ascending by `moment.vtime`. The
/// sort is stable, so events with equal vtimes keep their incoming order.
fn sorted_by_vtime(events: &[Event]) -> Vec<&Event> {
    let mut sorted: Vec<&Event> = events.iter().collect();
    sorted.sort_by_key(|e| e.moment.vtime);
    sorted
}

fn indent_lines(text: &str, prefix: &str) -> String {
    text.lines()
        .map(|line| {
            // Don't indent blank lines — that would leave trailing whitespace.
            if line.is_empty() {
                String::new()
            } else {
                format!("{prefix}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_failing(p: &Property) -> bool {
    matches!(p.status(), PropertyStatus::Failing)
}

/// The EXAMPLES cell for a property: the example count, SI-shortened so big
/// counts stay narrow (`18496678` -> `18M`), with counterexamples appended
/// inline (`examples/counterexamples`) only when a property actually has some —
/// so the common all-zero-counterexample rows aren't cluttered with `/0`.
fn property_examples_cell(p: &Property) -> String {
    let (examples, counters) = property_example_counts(p);
    if counters == 0 {
        format_count_si(examples)
    } else {
        format!(
            "{}/{}",
            format_count_si(examples),
            format_count_si(counters)
        )
    }
}

/// Order properties into display sections shared by the table and `--detail`
/// views: named groups containing a failing property first (then the rest,
/// alphabetical), with ungrouped properties in a final "(ungrouped)" section.
/// Within each section, failing entries sort first, then by name. An
/// empty-string group counts as no group.
///
/// Each section's heading is `Some(name)`, except the ungrouped section's, which
/// is `None` when there are no named groups to contrast it against — a lone
/// "(ungrouped)" heading over every property is just noise.
fn group_property_sections(properties: &[Property]) -> Vec<(Option<String>, Vec<&Property>)> {
    fn sort_section(props: &mut [&Property]) {
        props.sort_by(|a, b| {
            is_failing(b)
                .cmp(&is_failing(a))
                .then_with(|| a.name().cmp(b.name()))
        });
    }

    // BTreeMap keeps named groups alphabetical before the failing-first re-sort.
    let mut grouped: std::collections::BTreeMap<&str, Vec<&Property>> = Default::default();
    let mut ungrouped: Vec<&Property> = Vec::new();
    for p in properties {
        match property_group(p).filter(|g| !g.is_empty()) {
            Some(g) => grouped.entry(g).or_default().push(p),
            None => ungrouped.push(p),
        }
    }

    let mut named: Vec<(&str, Vec<&Property>)> = grouped.into_iter().collect();
    named.sort_by(|(a, aps), (b, bps)| {
        bps.iter()
            .any(|p| is_failing(p))
            .cmp(&aps.iter().any(|p| is_failing(p)))
            .then_with(|| a.cmp(b))
    });

    let mut sections: Vec<(Option<String>, Vec<&Property>)> = Vec::new();
    for (name, mut props) in named {
        sort_section(&mut props);
        sections.push((Some(name.to_string()), props));
    }
    if !ungrouped.is_empty() {
        sort_section(&mut ungrouped);
        // Only label the ungrouped section when named groups precede it; with no
        // named groups a "(ungrouped)" heading over everything carries no signal.
        let heading = (!sections.is_empty()).then(|| "(ungrouped)".to_string());
        sections.push((heading, ungrouped));
    }
    sections
}

fn render_properties_table(properties: &[Property]) -> String {
    // One table per group — far more scannable than a single flat table, and it
    // mirrors the report UI. The group is the section heading, so the NAME column
    // shows just the property's own name (the string a `--name`/`--detail` filter
    // matches); folding the group into NAME would have made the displayed name
    // un-copyable. This is human-facing output; use `--json` for automation.
    let headers = vec![
        "STATUS".to_string(),
        "EXAMPLES".to_string(),
        "NAME".to_string(),
    ];
    // Right-align the numeric EXAMPLES column so magnitudes line up; STATUS and
    // the wrapped NAME column stay left-aligned.
    let aligns = [Align::Left, Align::Right, Align::Left];
    let width = terminal_width();

    group_property_sections(properties)
        .iter()
        .map(|(title, props)| {
            let rows: Vec<Vec<String>> = props
                .iter()
                .map(|p| {
                    vec![
                        property_status_label(p.status()).to_string(),
                        property_examples_cell(p),
                        sanitize(p.name()),
                    ]
                })
                .collect();
            let table = render_table_wrap_last(&headers, &rows, width, &aligns);
            match title {
                Some(title) => format!("{}\n{}", sanitize(title), table),
                None => table,
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn print_run_detail(run: &RunDetail) -> Result<()> {
    // Bound once and reused for both the Failure Hash/VTime rows and the deferred
    // "view logs" hint below, so the two can't drift apart (a placeholder 0/0
    // moment is treated as no moment — see `RunDetail::real_failure_moment`).
    let failure = run.real_failure_moment();
    let mut rows: Vec<(&str, String)> = Vec::new();

    // Lead with the identifier; the human labels follow.
    rows.push(("Run ID", run.run_id.clone()));
    if let Some(name) = run.test_name() {
        rows.push(("Test Name", name.to_string()));
    }

    rows.push(("Status", run.status.to_string()));
    rows.push(("Created", format_local(run.created_at)));

    if let Some(t) = run.started_at {
        rows.push(("Started", format_local(t)));
    }
    if let Some(t) = run.completed_at {
        rows.push(("Completed", format_local(t)));
    }

    // Requested vs. actual run time. "Duration" is the configured workload
    // length; "Elapsed" is wall-clock (which also spans provisioning, setup and
    // teardown), so the two legitimately differ — they aren't a mismatch.
    if let Some(raw) = run.requested_duration() {
        rows.push(("Duration", format_requested_duration(raw)));
    }
    if let Some(started) = run.started_at
        && let Some(elapsed) = elapsed_duration(started, run.completed_at.unwrap_or_else(Utc::now))
    {
        rows.push(("Elapsed", elapsed.to_string()));
    }

    rows.push(("Launcher", run.launcher.clone()));
    if let Some(source) = run.source() {
        rows.push(("Source", source.to_string()));
    }

    if let Some(moment) = failure {
        // Hash before VTime to match the `runs logs <hash> <vtime>` argument
        // order, so the values read top-to-bottom in the order you paste them.
        rows.push(("Failure Hash", moment.input_hash.clone()));
        rows.push(("Failure VTime", moment.vtime.to_string()));
    }

    if let Some(ref creator) = run.creator
        && let Some(ref name) = creator.name
    {
        rows.push(("Creator", name.clone()));
    }

    out!("{}", render_kv(&rows, 0))?;

    // The description can be an enormous multi-paragraph blob, so it goes as its
    // own block — wrapped to the terminal, with the label on its own line —
    // rather than as a metadata row that would otherwise bury Status/timestamps
    // below a wall of text. The leading blank line separates it from the block.
    if let Some(desc) = run.test_description() {
        let block = render_prose_block("Description", desc, ProseLayout::OwnLine);
        if !block.is_empty() {
            out!("\n{block}")?;
        }
    }

    // Hand the user the exact command to read logs at the failure moment, the
    // same way the `--web` hint below hands over the report link. Both can fire
    // when an incomplete run still has a report — they're complementary.
    if let Some(moment) = failure {
        outln!(
            "\nview logs at the failure moment:\n  snouty runs logs {} {} {}",
            run.run_id,
            moment.input_hash,
            moment.vtime
        )?;
    }

    // Point at the obvious next step instead of dumping the huge signed report
    // URL into the metadata block — but only when a triage report actually
    // exists, since `--web` errors out otherwise (e.g. for incomplete runs).
    let has_report = run
        .links
        .as_ref()
        .and_then(|l| l.triage_report.as_deref())
        .is_some();
    if has_report {
        outln!(
            "\nview the report in your browser:\n  snouty runs show {} --web",
            run.run_id
        )?;
    }
    Ok(())
}

/// `runs list --detail`: one aligned key-value block per run (no table),
/// separated by blank lines. Empty optional fields are omitted.
fn render_runs_detail(runs: &[RunSummary]) -> String {
    let blocks: Vec<String> = runs
        .iter()
        .map(|run| {
            let mut rows: Vec<(&str, String)> = vec![
                ("Run ID", run.run_id.clone()),
                ("Status", run.status.to_string()),
                ("Created", format_local(run.created_at)),
                ("Launcher", run.launcher.clone()),
            ];
            if let Some(name) = run.test_name() {
                rows.push(("Test Name", name.to_string()));
            }

            // The description can be a multi-paragraph blob, so it wraps to the
            // terminal with a hanging indent under the value column (matching
            // `runs show`) instead of running off as one giant line. Include its
            // label in the width so every key-value row stays aligned with it.
            let description = run.test_description();
            let min_label_width = description.map_or(0, |_| "Description".len());
            let label_width = rows
                .iter()
                .map(|(label, _)| label.len())
                .max()
                .unwrap_or(0)
                .max(min_label_width);

            let mut out = render_kv(&rows, min_label_width);
            if let Some(description) = description {
                out.push_str(&render_prose_block(
                    "Description",
                    description,
                    // The value column starts two spaces past the label column;
                    // floored at one so the body never vanishes on a tiny term.
                    ProseLayout::HangingIndent {
                        label_col: label_width + 2,
                        min_body_width: 1,
                    },
                ));
            }
            out
        })
        .collect();

    // Each block already ends in a newline; joining with "\n" inserts one blank
    // line between consecutive runs.
    blocks.join("\n")
}

struct LogOutputOptions {
    /// The global `--json`/`--verbose` flags.
    output: OutputOptions,
    /// Skip all log post-processing: no fault annotation in JSON mode, and the
    /// human payload is rendered verbatim (no ANSI stripping or control-byte
    /// escaping).
    raw: bool,
}

/// After streaming a log/event stream in human mode, print `empty_note` when the
/// stream completed successfully (`stream_ok`) but produced no lines, so the user
/// isn't left wondering whether anything happened. In `--json` mode an empty
/// stream is the correct machine answer, so stay quiet. Shared by `cmd_runs_logs`
/// and `cmd_runs_build_logs`, which track `wrote_any` the same way.
fn note_if_empty(stream_ok: bool, json: bool, wrote_any: bool, empty_note: &str) {
    if stream_ok && !json && !wrote_any {
        eprintln!("{empty_note}");
    }
}

async fn cmd_runs_build_logs(
    run_id: &str,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("streaming build logs for run: {}", run_id);

    let api = AntithesisApi::new(settings, verbose)?;
    let stream = match api.get_run_build_logs(run_id).await {
        Ok(stream) => stream.into_inner(),
        Err(err) => return Err(explain_run_scoped_error(&api, run_id, err).await),
    };
    let mut stdout = BufWriter::new(std::io::stdout().lock());

    let mut wrote_any = false;
    let result = if json {
        // The --json contract is raw NDJSON passthrough.
        stream_raw_lines(stream, |line| {
            wrote_any = true;
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        stream_ndjson_lines(stream, |line| {
            wrote_any = true;
            match line {
                NdjsonLine::Entry(entry) => {
                    let ts = format_local_str(entry["timestamp"].as_str().unwrap_or(""));
                    let stream = entry["stream"].as_str().unwrap_or("out");
                    let text = sanitize(entry["text"].as_str().unwrap_or(""));
                    writeln!(stdout, "{ts} [{stream}] {text}")?;
                }
                NdjsonLine::Raw(line) => writeln!(stdout, "{line}")?,
            }
            Ok(())
        })
        .await
    };

    stdout.flush()?;
    note_if_empty(
        result.is_ok(),
        json,
        wrote_any,
        "No build logs for this run.",
    );
    result
}

async fn cmd_runs_search(
    args: RunsSearchArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("querying events for run: {}", args.run_id);

    let api = AntithesisApi::new(settings, verbose)?;
    if args.check {
        return event_search::check_query(&api, &args.run_id, &args.query, json).await;
    }
    let search = EventSearch {
        query: EventQuery::Dsl {
            query: args.query,
            follow: args.follow,
        },
        limit: args.limit,
    };
    event_search::execute(&api, &args.run_id, search, json).await
}

async fn cmd_runs_events(
    run_id: &str,
    matches: &[String],
    limit: Option<NonZeroU64>,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("searching events for run: {}", run_id);

    if matches.is_empty() {
        return Err(user_error("no search term given")
            .suggestion("pass at least one needle via `-m/--match` or as a positional argument"));
    }
    // An empty needle matches every event (`contains("")` is always true on
    // either backend), which would silently disable filtering, so reject it
    // rather than dump the whole stream as if no filter were given.
    if matches.iter().any(|m| m.is_empty()) {
        return Err(user_error("empty search term")
            .suggestion("each `-m/--match` needle must be a non-empty substring"));
    }

    let api = AntithesisApi::new(settings, verbose)?;
    let search = EventSearch {
        query: EventQuery::Needles(matches.to_vec()),
        limit,
    };
    event_search::execute(&api, run_id, search, json).await
}

async fn cmd_runs_logs(
    run_id: &str,
    moment: Moment,
    begin: Option<LogsBegin>,
    settings: &Settings,
    LogOutputOptions {
        output: OutputOptions { json, verbose },
        raw,
    }: LogOutputOptions,
) -> Result<()> {
    debug!("streaming logs for run: {}", run_id);

    let api = AntithesisApi::new(settings, verbose)?;
    let stream = match api.get_run_logs(run_id, moment, begin).await {
        Ok(stream) => stream.into_inner(),
        Err(err) => return Err(explain_logs_error(&api, run_id, err).await),
    };

    let mut stdout = BufWriter::new(std::io::stdout().lock());
    let mut wrote_any = false;
    let result = if json {
        // Fault annotation is the default; `--raw` opts out into a verbatim
        // NDJSON passthrough.
        if raw {
            stream_raw_lines(stream, |line| {
                wrote_any = true;
                writeln!(stdout, "{line}")?;
                Ok(())
            })
            .await
        } else {
            let mut annotator = FaultAnnotator::default();
            stream_ndjson_lines(stream, |line| {
                wrote_any = true;
                match line {
                    NdjsonLine::Entry(mut entry) => {
                        annotator.annotate(&mut entry);
                        writeln!(stdout, "{entry}")?;
                    }
                    NdjsonLine::Raw(line) => writeln!(stdout, "{line}")?,
                }
                Ok(())
            })
            .await
        }
    } else {
        stream_ndjson_lines(stream, |line| {
            wrote_any = true;
            match line {
                NdjsonLine::Entry(entry) => writeln!(stdout, "{}", format_log_entry(&entry, raw))?,
                NdjsonLine::Raw(line) => writeln!(stdout, "{line}")?,
            }
            Ok(())
        })
        .await
    };
    stdout.flush()?;
    // A moment with no logs (e.g. a manually-supplied 0/0 placeholder) yields an
    // empty stream; say so in human mode rather than printing nothing.
    note_if_empty(
        result.is_ok(),
        json,
        wrote_any,
        "No log lines at this moment.",
    );
    result
}

/// One frame of an execute-command NDJSON stream.
///
/// Deliberately permissive, because `runs exec` is a work in progress behind
/// an unstable feature and the stream may still grow frames and fields: a
/// frame this build does not recognize is kept whole as [`ExecFrame::Unknown`]
/// rather than failing the stream. Tighten this to a closed type once the
/// command stabilizes.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ExecFrame {
    Known(ExecEvent),
    /// A frame with an unknown `type`, or a known one whose shape did not fit.
    Unknown(Value),
}

/// A frame this build understands.
///
/// Each variant keeps whatever else the server sent in `extra`, so a new field
/// survives to the `--json` output and to any rendering added later instead of
/// being dropped here.
///
/// Hand-written rather than the generated `ExecuteCommandStreamEvent`: that
/// type is an untagged enum whose variants progenitor names `Variant0/1/2`,
/// which reads far worse at the match sites than the named variants here.
///
/// `moment` and `extra` are not read yet: `--json` passes the raw line
/// through, so nothing consumes the typed copies. They are kept so the data
/// survives into the type as the command grows a rendered JSON form, and so a
/// field the server adds is not dropped here in the meantime.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecEvent {
    Output {
        stream: String,
        text: String,
        /// Where the branch stood when the line was written. Kept for the
        /// `--json` output.
        #[serde(default)]
        moment: Option<Moment>,
        #[serde(flatten)]
        extra: Map<String, Value>,
    },
    Exited {
        exit_code: Option<i64>,
        /// Required by the spec and present on every observed response;
        /// `Option` only so a server that omits it costs the trailer rather
        /// than the whole event.
        #[serde(default)]
        end_moment: Option<Moment>,
        #[serde(flatten)]
        extra: Map<String, Value>,
    },
    TimedOut {
        #[serde(flatten)]
        extra: Map<String, Value>,
    },
}

/// Render one frame for a human. Only what the script itself wrote goes to
/// stdout — its stdout on stdout, its stderr on stderr — so `runs exec ... |
/// jq` sees the script's output and nothing else. A frame this build does not
/// know is not the script's output, so it goes to stderr whole.
///
/// The terminal events produce no output of their own: they decide the exit
/// status, which the caller settles after the stream ends.
fn render_exec_frame(frame: &ExecFrame) -> Result<()> {
    match frame {
        ExecFrame::Known(ExecEvent::Output { stream, text, .. }) => {
            let text = normalize_terminal_text(text);
            // Parse via `Stream` so the short form `err` routes to stderr
            // too. Anything else — stdout, info, or an unrecognized label —
            // goes to stdout, as before.
            if stream.parse::<Stream>() == Ok(Stream::Stderr) {
                eprintln!("{text}");
            } else {
                outln!("{text}")?;
            }
        }
        ExecFrame::Known(ExecEvent::Exited { .. } | ExecEvent::TimedOut { .. }) => {}
        ExecFrame::Unknown(frame) => eprintln!("{frame}"),
    }
    Ok(())
}

/// Cap on a script read from stdin. Far more than any hand-written bash
/// script needs, and small enough that `snouty runs exec ... < /dev/urandom`
/// fails with a message instead of consuming memory until the OOM killer
/// steps in.
const MAX_SCRIPT_BYTES: u64 = 2 * 1024 * 1024;

/// The script to execute: the SCRIPT argument, or stdin when it is omitted.
/// An empty script is rejected before any API call.
fn resolve_exec_script(arg: Option<String>) -> Result<String> {
    let stdin = std::io::stdin();
    let script = match arg {
        Some(script) => script,
        None => read_script_from_stdin(&stdin, stdin.is_terminal())?,
    };
    // One rule for both sources: a blank script is no script.
    if script.trim().is_empty() {
        return Err(no_script_error());
    }
    Ok(script)
}

fn no_script_error() -> color_eyre::eyre::Report {
    user_error("no script provided")
        .suggestion("pass the script as an argument, or pipe it on stdin")
}

/// Read a script from `input`, refusing to read at all from an interactive
/// terminal: with SCRIPT omitted, a bare `runs exec RUN HASH VTIME` at a
/// prompt would otherwise hang with no indication it wants input. A pipe, a
/// redirect, and a heredoc are all non-interactive, so every scripted way of
/// supplying a script still works.
///
/// Returns the text as read. Whether a blank script counts as no script is
/// [`resolve_exec_script`]'s rule, since it holds for the argument too.
fn read_script_from_stdin(input: impl Read, interactive: bool) -> Result<String> {
    if interactive {
        return Err(no_script_error());
    }
    // Read one byte past the cap so an over-long script is detected rather
    // than silently truncated into a script that means something else.
    //
    // Bytes, not text, and the size is checked before the UTF-8 conversion:
    // reading straight into a `String` would validate the whole capped buffer
    // first, so an over-long script whose cap boundary splits a multi-byte
    // character would fail as invalid UTF-8 and never mention its size.
    let mut buf = Vec::new();
    input
        .take(MAX_SCRIPT_BYTES + 1)
        .read_to_end(&mut buf)
        .wrap_err("failed to read the script from stdin")?;
    if buf.len() as u64 > MAX_SCRIPT_BYTES {
        return Err(user_error(format!(
            "the script on stdin is larger than the {} MiB limit",
            MAX_SCRIPT_BYTES / (1024 * 1024)
        ))
        .note("`runs exec` sends the whole script in one request body"));
    }
    String::from_utf8(buf).map_err(|_| user_error("the script on stdin is not valid UTF-8"))
}

async fn cmd_runs_exec(
    run_id: &str,
    moment: Moment,
    script: Option<String>,
    timeout: Duration,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    let script = resolve_exec_script(script)?;

    debug!("executing command in run: {}", run_id);
    let api = AntithesisApi::new(settings, verbose)?;
    let stream = match api.execute_command(run_id, moment, script, timeout).await {
        Ok(stream) => stream.into_inner(),
        // Translate a bad run id's 404 into the shared "run not found"
        // message every sibling run-scoped command reports (the endpoint's
        // own 404 body is an unhelpful "Resource not found").
        Err(err) => return Err(explain_run_scoped_error(&api, run_id, err).await),
    };

    // The terminal event the stream ended with, kept for the exit decision
    // below: an `output` event is rendered as it arrives, but `exited` and
    // `timed_out` decide the command's outcome, so they are acted on only
    // once the stream is known to have completed.
    let mut terminal: Option<ExecEvent> = None;
    let result = stream_ndjson_lines(stream, |line| {
        match line {
            NdjsonLine::Entry(mut entry) => {
                // classify_line normalized `moment.vtime`; the exited event
                // carries its moment under `end_moment` instead.
                normalize_vtime_field(&mut entry, "end_moment");
                let frame = ExecFrame::deserialize(&entry).map_err(|err| {
                    eyre!("could not decode a line of the command's output: {err}")
                        .note("every JSON object decodes, so this means the decoder itself failed")
                })?;

                if json {
                    outln!("{entry}")?;
                } else {
                    render_exec_frame(&frame)?;
                }

                // Either mode: the terminal event decides the exit status, and
                // is acted on once the stream is known to have completed.
                if let ExecFrame::Known(
                    event @ (ExecEvent::Exited { .. } | ExecEvent::TimedOut { .. }),
                ) = frame
                {
                    terminal = Some(event);
                }
            }
            NdjsonLine::Raw(line) => {
                if json {
                    outln!("{line}")?;
                } else {
                    eprintln!("{line}");
                }
            }
        }
        Ok(())
    })
    .await;
    result?;

    match terminal {
        Some(ExecEvent::Exited {
            exit_code,
            end_moment,
            ..
        }) => {
            // The trailer documents where the branch's timeline ended — the
            // moment to chain a follow-up command from, in the positional
            // order `runs exec` takes it (VTime's Display is exact, so it
            // pastes back unchanged). --json carries it in the exited event.
            if !json && let Some(m) = end_moment {
                eprintln!("end moment: {} {}", m.input_hash, m.vtime);
            }
            match exit_code {
                Some(0) => Ok(()),
                Some(code) => Err(user_error(format!("command exited with code {code}"))),
                None => Err(user_error("command exited without reporting an exit code")),
            }
        }
        Some(ExecEvent::TimedOut { .. }) => Err(user_error(format!(
            "command timed out after {}",
            HumanDuration::from_seconds(timeout.as_secs())
        ))),
        // Exit 0 must mean "the script ran and exited 0", so a stream that
        // ends without a terminal event — truncation — is a failure.
        _ => Err(eyre!("stream ended before the command reported completion")
            .note("the command may still have run; output above may be incomplete")),
    }
}

/// The source column is sized to fit `antithesis_test_composer` — the built-in
/// test-composer source present in nearly every run's logs — so those lines align
/// instead of overflowing. Longer sources still overflow on their own lines
/// rather than widening the column for everyone.
const LOG_SOURCE_MIN_WIDTH: usize = "antithesis_test_composer".len();
/// vtime is shown truncated to 3 decimals. Sized for runs up to ~9999 vsec
/// (`"9999.999"`, 8 chars), which covers the vast majority; longer runs overflow
/// this width on their lines rather than padding every shorter line to match.
const LOG_VTIME_WIDTH: usize = 8;
const LOG_STREAM_WIDTH: usize = 3;

fn format_log_entry(entry: &Value, raw: bool) -> String {
    let vtime = format_log_vtime(entry);
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let name = entry["source"]["name"].as_str().unwrap_or("");
    let source = if !container.is_empty() {
        container
    } else {
        name
    };
    let stream_raw = entry["source"]["stream"].as_str().unwrap_or("");
    let stream = abbreviate_stream(stream_raw);

    // Web UI format: text records sit one space after the stream bracket.
    // JSON records get an extra " - " separator before the body.
    // Run the text payload through the shared terminal normalizer: strip ANSI
    // color codes, then escape any remaining control bytes so a stray `\r`/BEL
    // in container output can't corrupt the rendered stream. `--raw` skips the
    // normalizer so colors and control bytes reach the terminal verbatim.
    let payload = if let Some(text) = entry.get("output_text").and_then(Value::as_str) {
        if raw {
            text.to_string()
        } else {
            normalize_terminal_text(text)
        }
    } else {
        format!(" - {}", strip_log_envelope(entry))
    };

    format!(
        "[{vtime:>vw$}] [{source:>sw$}] [{stream:<stw$}] {payload}",
        vw = LOG_VTIME_WIDTH,
        sw = LOG_SOURCE_MIN_WIDTH,
        stw = LOG_STREAM_WIDTH,
    )
}

/// Convert the vtime under an NDJSON entry's `key` (`moment` for a log/event
/// record, `end_moment` for an execute-command `exited` event) from the
/// server's seconds string to an exact JSON number in place — the only
/// processing snouty does to a vtime (see [`VTime`]). Returns the vtime, or
/// `None` (entry untouched) when there's no parseable one.
fn normalize_vtime_field(entry: &mut Value, key: &str) -> Option<VTime> {
    let vtime = VTime::from_json(&entry[key]["vtime"])?;
    entry[key]["vtime"] = json!(vtime);
    Some(vtime)
}

/// Render a log line's vtime in seconds with exactly 3 decimal places,
/// truncated — never rounded. Fixed precision keeps the decimal point and right
/// edge aligned down the fixed `LOG_VTIME_WIDTH` column (full-precision vtimes
/// would overflow it and desync the source column). Truncating rather than
/// rounding means a vtime copied off the screen and pasted back as
/// `--begin-vtime` lands on — never just past — the line you saw.
fn format_log_vtime(entry: &Value) -> String {
    let raw = &entry["moment"]["vtime"];
    match raw.as_str() {
        // The API sends vtime as a seconds string; truncate it directly so f64
        // round-trips can't nudge the displayed value.
        Some(s) => truncate_decimals(s, 3),
        // A JSON-number vtime: VTime's Display is the exact text the number
        // was printed from, so truncating it cuts — never rounds.
        None => match VTime::from_json(raw) {
            Some(v) => truncate_decimals(&v.to_string(), 3),
            None => String::new(),
        },
    }
}

/// Truncate (never round) the decimal string `s` to exactly `decimals`
/// fractional digits, zero-padding a short or missing fraction (`"19"` ->
/// `"19.000"`, `"14.78"` -> `"14.780"`, `"1814.71357"` -> `"1814.713"`). Fixed
/// width keeps a column of these aligned on the decimal point. Only a plain
/// decimal string is sliced; anything else (scientific notation) falls back to
/// rounded fixed-point, and a non-number is passed through verbatim.
fn truncate_decimals(s: &str, decimals: usize) -> String {
    let t = s.trim();
    let (int_part, frac_part) = match t.split_once('.') {
        Some((i, f)) => (i, f),
        None => (t, ""),
    };
    let is_plain = !int_part.is_empty()
        && int_part
            .char_indices()
            .all(|(i, c)| c.is_ascii_digit() || (i == 0 && (c == '-' || c == '+')))
        && frac_part.chars().all(|c| c.is_ascii_digit());
    if !is_plain {
        return match t.parse::<f64>() {
            Ok(v) => format!("{v:.decimals$}"),
            Err(_) => t.to_string(),
        };
    }
    if decimals == 0 {
        return int_part.to_string();
    }
    let mut frac: String = frac_part.chars().take(decimals).collect();
    while frac.len() < decimals {
        frac.push('0');
    }
    format!("{int_part}.{frac}")
}

fn abbreviate_stream(stream: &str) -> std::borrow::Cow<'static, str> {
    if let Ok(s) = stream.parse::<Stream>() {
        return std::borrow::Cow::Borrowed(s.abbreviated());
    }
    if stream.is_empty() {
        return std::borrow::Cow::Borrowed("   ");
    }
    std::borrow::Cow::Owned(stream.chars().take(LOG_STREAM_WIDTH).collect())
}

/// Keys that wrap a log record's payload; dropped before rendering the body.
const LOG_ENVELOPE_KEYS: [&str; 3] = ["moment", "source", "IPT_bytes_out"];

/// Serialize-only view over a JSON object that emits every key except the
/// envelope keys, borrowing the retained values rather than cloning them.
struct StrippedEnvelope<'a>(&'a Map<String, Value>);

impl serde::Serialize for StrippedEnvelope<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        for (key, value) in self.0 {
            if !LOG_ENVELOPE_KEYS.contains(&key.as_str()) {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

fn strip_log_envelope(entry: &Value) -> String {
    // Serialize a borrowed, filtered view of the object rather than deep-cloning
    // the whole Value just to drop three envelope keys — this runs per JSON log
    // line. The view preserves the original key order (matching serde_json's
    // preserve_order) without copying any of the retained subtrees.
    match entry.as_object() {
        Some(obj) => serde_json::to_string(&StrippedEnvelope(obj)).unwrap_or_default(),
        None => serde_json::to_string(entry).unwrap_or_default(),
    }
}

/// One line of an NDJSON stream, classified once at the stream boundary.
enum NdjsonLine<'a> {
    /// The happy case: the line parsed as a JSON *object* — the shape every
    /// log/event record has — with `moment.vtime` already normalized to an
    /// exact JSON number.
    Entry(Value),
    /// Everything else: a line that isn't valid JSON (a truncated final
    /// chunk, a proxy-injected error blob, …) or valid JSON that isn't an
    /// object. Carries the original text so callers surface it verbatim
    /// rather than dropping it silently.
    Raw(&'a str),
}

/// Classify one NDJSON line: see [`NdjsonLine`].
fn classify_line(line: &str) -> NdjsonLine<'_> {
    if let Ok(mut entry) = serde_json::from_str::<Value>(line)
        && entry.is_object()
    {
        normalize_vtime_field(&mut entry, "moment");
        return NdjsonLine::Entry(entry);
    }
    NdjsonLine::Raw(line)
}

/// Extract the message from a `Stream_Error` record, the shape every
/// streaming endpoint (logs, events, build logs) uses to report a
/// server-side failure that happens after the `200 OK` is already committed:
/// `{"error": "..."}` as its own NDJSON line, usually ending the stream.
///
/// Such a line is an out-of-band failure signal, not data — rendered as a
/// record it would print as a blank log line or be dropped by the events
/// needle filter, and the command would exit 0 over a truncated stream. The
/// streaming helpers below turn it into the command's failure instead.
///
/// A *data* record can legitimately carry a top-level `error` field of its
/// own (an event's payload is arbitrary workload JSON), but data records
/// always carry their schema's required envelope — `moment` for log/event
/// records, `timestamp` for build log lines — which `Stream_Error` never has,
/// so the envelope's absence disambiguates.
fn stream_error_message(entry: &Value) -> Option<&str> {
    let obj = entry.as_object()?;
    if obj.contains_key("moment") || obj.contains_key("timestamp") {
        return None;
    }
    obj.get("error")?.as_str()
}

/// [`stream_error_message`] for an undecoded line, used where no parsed
/// record exists (the raw-passthrough path and tests).
fn parse_stream_error(line: &str) -> Option<String> {
    // Cheap pre-filter: skip JSON parsing for the vast majority of lines,
    // which don't even contain the key's text.
    if !line.contains(r#""error""#) {
        return None;
    }
    let value: Value = serde_json::from_str(line).ok()?;
    Some(stream_error_message(&value)?.to_string())
}

/// What a `{"error": "..."}` line without a data envelope means for a
/// stream. Every fixed-schema stream (logs, build logs, events) aborts on
/// one: it is the server's out-of-band failure signal
/// ([`stream_error_message`]). A user-written event-set pipeline is the
/// exception — map/narrow/fold can legitimately reshape a result row into
/// exactly that shape (`map(ev => ({error: ev.output_text}))`), so a DSL
/// query's stream passes such rows through as data rather than guessing.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ErrorRows {
    Abort,
    Data,
}

/// The failure a `Stream_Error` line becomes.
fn mid_stream_error(message: &str) -> color_eyre::eyre::Report {
    eyre!("the server reported an error mid-stream: {message}")
        .note("output produced before the error may be incomplete")
}

/// Stream an NDJSON response, handing each line to the callback parsed and
/// normalized (see [`NdjsonLine`]) — except a `Stream_Error` record
/// ([`stream_error_message`]), which becomes the command's failure instead of
/// reaching the callback. Commands whose contract is *raw* passthrough
/// (`runs logs --json --raw`, `runs build-logs --json`) use
/// [`stream_raw_lines`] instead — parsing is not their happy case.
async fn stream_ndjson_lines<S, C>(
    stream: S,
    mut process_line: impl FnMut(NdjsonLine<'_>) -> Result<()>,
) -> Result<()>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    stream_ndjson_lines_until(stream, ErrorRows::Abort, |line| {
        process_line(line).map(|()| ControlFlow::Continue(()))
    })
    .await
}

/// [`stream_ndjson_lines`], but stop reading — dropping the connection —
/// once `cap` lines have reached the callback, and return how many did.
/// Every line the server sends counts (entries and raw lines alike),
/// mirroring what a server-enforced limit would count; `None` means no cap.
/// `runs search` and the search route of `runs events` use this to enforce
/// the limit their request named against a server that ignores it.
async fn stream_ndjson_lines_capped<S, C>(
    stream: S,
    cap: Option<u64>,
    error_rows: ErrorRows,
    mut process_line: impl FnMut(NdjsonLine<'_>) -> Result<()>,
) -> Result<u64>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    let mut seen: u64 = 0;
    stream_ndjson_lines_until(stream, error_rows, |line| {
        process_line(line)?;
        seen += 1;
        Ok(if cap.is_some_and(|cap| seen >= cap) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        })
    })
    .await?;
    Ok(seen)
}

/// [`stream_ndjson_lines`] (same decoding, same `Stream_Error` handling),
/// but the callback can end the stream early by returning
/// `ControlFlow::Break`: the connection drops on return.
async fn stream_ndjson_lines_until<S, C>(
    stream: S,
    error_rows: ErrorRows,
    mut process_line: impl FnMut(NdjsonLine<'_>) -> Result<ControlFlow<()>>,
) -> Result<()>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    split_stream_lines_until(stream, |line| {
        let classified = classify_line(line);
        if error_rows == ErrorRows::Abort
            && let NdjsonLine::Entry(entry) = &classified
            && let Some(message) = stream_error_message(entry)
        {
            return Err(mid_stream_error(message));
        }
        process_line(classified)
    })
    .await
}

/// Stream an NDJSON response, handing each line to the callback verbatim —
/// except a `Stream_Error` line ([`stream_error_message`]), which becomes the
/// command's failure instead of reaching the callback: it is the server
/// aborting the stream, so even the raw-passthrough modes must not present it
/// (and whatever follows it never arrives) as data. Detecting it is the one
/// reason each line takes a decode pass here.
async fn stream_raw_lines<S, C>(
    stream: S,
    mut process_line: impl FnMut(&str) -> Result<()>,
) -> Result<()>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    split_stream_lines(stream, |line| {
        if let Some(message) = parse_stream_error(line) {
            return Err(mid_stream_error(&message));
        }
        process_line(line)
    })
    .await
}

/// Split a byte stream into newline-delimited lines and hand each to the
/// callback. Pure line splitting, no JSON awareness — the NDJSON wrappers
/// above layer decoding and `Stream_Error` detection on top.
async fn split_stream_lines<S, C>(
    stream: S,
    mut handle_line: impl FnMut(&str) -> Result<()>,
) -> Result<()>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    split_stream_lines_until(stream, |line| {
        handle_line(line).map(|()| ControlFlow::Continue(()))
    })
    .await
}

/// [`split_stream_lines`], but the callback can end the stream early by
/// returning `ControlFlow::Break`.
async fn split_stream_lines_until<S, C>(
    mut stream: S,
    mut handle_line: impl FnMut(&str) -> Result<ControlFlow<()>>,
) -> Result<()>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    use futures_util::StreamExt;

    let mut buf: Vec<u8> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(chunk.as_ref());

        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes = &buf[..pos];
            let mut flow = ControlFlow::Continue(());
            if !line_bytes.is_empty() {
                let line = std::str::from_utf8(line_bytes)
                    .map_err(|e| eyre!("invalid UTF-8 in response stream: {e}"))?;
                flow = handle_line(line)?;
            }
            buf.drain(..=pos);
            if flow.is_break() {
                return Ok(());
            }
        }
    }

    if !buf.is_empty() {
        let line = std::str::from_utf8(&buf)
            .map_err(|e| eyre!("invalid UTF-8 in response stream: {e}"))?;
        // The stream is over; Continue and Break both mean returning Ok.
        let _ = handle_line(line)?;
    }

    Ok(())
}

struct FaultAnnotator {
    active_fault_windows: ActiveFaultWindows,
    active_faults: Value,
}

/// Not derived: an entry gets `active_faults` attached before any fault event
/// arrives, and that must render as `{}` — `Value::default()` is `null`.
impl Default for FaultAnnotator {
    fn default() -> Self {
        FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::default(),
            active_faults: json!({}),
        }
    }
}

impl FaultAnnotator {
    /// Annotate one parsed log entry in place: advance the fault windows
    /// using the entry's vtime, strip ANSI from `output_text`, and attach the
    /// current `active_faults`.
    fn annotate(&mut self, entry: &mut Value) {
        let mut update_faults = false;

        // The fault-window math runs directly in seconds. Entries without a
        // vtime fall back to zero, which never expires a window (expiry is
        // strict less-than).
        let latest_vtime = VTime::from_json(&entry["moment"]["vtime"]).unwrap_or(VTime::ZERO);
        let fault_name = entry["fault"]["name"].as_str();
        let is_fault_injector = entry["source"]["name"]
            .as_str()
            .map(|source| source.eq("fault_injector"))
            .unwrap_or(false);

        // Clear network and node faults if the fault injector was paused
        if is_fault_injector
            && entry["info"]["message"]
                .as_str()
                .map(|message| message.eq("status"))
                .unwrap_or(false)
            && entry["info"]["details"]["paused"]
                .as_bool()
                .unwrap_or(false)
        {
            update_faults = self.active_fault_windows.clear_network_faults() || update_faults;
            update_faults = self.active_fault_windows.clear_node_faults() || update_faults;
        }

        // Clear network faults if the network was restored
        if is_fault_injector && fault_name.map(|n| n.eq("restore")).unwrap_or(false) {
            update_faults = self.active_fault_windows.clear_network_faults() || update_faults;
        }

        // Clear any expired faults
        update_faults =
            self.active_fault_windows.clear_expired_faults(latest_vtime) || update_faults;

        if is_fault_injector && let Some(fault_name) = fault_name {
            let max_duration = entry["fault"]["max_duration"].as_f64().filter(|d| *d > 0.0);
            let end_vtime = max_duration.map(|duration| latest_vtime.plus_seconds(duration));
            let fault_type = entry["fault"]["type"].as_str().unwrap_or("");

            if let Some(target) = entry["fault"]["affected_nodes"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|first| first.as_str())
            {
                if fault_name.eq("partition") || fault_name.eq("clog") {
                    update_faults = self.active_fault_windows.add_network_fault(
                        fault_name.to_string(),
                        FaultWindowBounds {
                            start_vtime: latest_vtime,
                            end_vtime,
                        },
                    ) || update_faults;
                }

                if fault_type.eq("node") && (fault_name.eq("pause") || fault_name.eq("throttle")) {
                    update_faults = self.active_fault_windows.add_node_fault(
                        fault_name.to_string(),
                        target.to_string(),
                        FaultWindowBounds {
                            start_vtime: latest_vtime,
                            end_vtime,
                        },
                    ) || update_faults;
                }
            }

            if fault_name.eq("skip")
                && fault_type.eq("clock")
                && let Some(offset) = entry["fault"]["details"]["offset"].as_f64()
            {
                update_faults = self.active_fault_windows.add_clock_fault(
                    offset,
                    FaultWindowBounds {
                        start_vtime: latest_vtime,
                        end_vtime,
                    },
                ) || update_faults;
            }
        }

        if update_faults {
            self.active_faults = self.active_fault_windows.to_json();
        }

        if let Some(output_text) = entry["output_text"].as_str() {
            entry["output_text"] = Value::String(strip_ansi(output_text));
        }

        entry["active_faults"] = self.active_faults.clone();
    }
}

#[derive(Debug, PartialEq, Eq)]
struct RenderedEventEntry {
    input_hash: String,
    vtime: String,
    source: String,
    output: String,
}

fn ansi_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(concat!(
            r"\x1b\[[\x20-\x3f]*[\x40-\x7e]",      // CSI: ESC [ ... final
            r"|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", // OSC: ESC ] ... (BEL | ESC \)
            r"|\x1b[\x20-\x7e]",                   // two-byte: ESC + single printable
        ))
        .unwrap()
    })
}

fn strip_ansi(text: &str) -> String {
    ansi_re().replace_all(text, "").to_string()
}

struct FaultWindowBounds {
    start_vtime: VTime,
    end_vtime: Option<VTime>,
}

impl FaultWindowBounds {
    fn is_expired(&self, latest_vtime: VTime) -> bool {
        self.end_vtime
            .map(|expiry| expiry < latest_vtime)
            .unwrap_or(false)
    }
}

#[derive(Default)]
struct ActiveFaultWindows {
    network: IndexMap<String, FaultWindowBounds>,
    node: IndexMap<String, IndexMap<String, FaultWindowBounds>>,
    // The offset is a duration (seconds added to the clock), not a vtime.
    clock: Vec<(f64, FaultWindowBounds)>,
}

impl ActiveFaultWindows {
    fn clear_network_faults(&mut self) -> bool {
        let did_something = !self.network.is_empty();
        self.network.clear();
        did_something
    }

    fn clear_node_faults(&mut self) -> bool {
        let did_something = !self.node.is_empty();
        self.node.clear();
        did_something
    }

    fn clear_expired_faults(&mut self, latest_vtime: VTime) -> bool {
        let mut did_something;

        let clock_faults_length = self.clock.len();
        self.clock.retain(|fault| !fault.1.is_expired(latest_vtime));
        did_something = self.clock.len() != clock_faults_length;

        for _ in self
            .network
            .extract_if(.., |_k, window| window.is_expired(latest_vtime))
        {
            did_something = true;
        }

        let mut dropped_categories_of_node_faults = false;
        for _ in self.node.extract_if(.., |_k, windows_by_container| {
            for _ in
                windows_by_container.extract_if(.., |_c, window| window.is_expired(latest_vtime))
            {
                did_something = true;
            }

            windows_by_container.is_empty()
        }) {
            dropped_categories_of_node_faults = true;
        }
        did_something = did_something || dropped_categories_of_node_faults;

        did_something
    }

    fn add_network_fault(&mut self, name: String, window: FaultWindowBounds) -> bool {
        match self.network.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(window);
                true
            }
            Entry::Occupied(mut entry) => {
                if let Some(updated) = merge_fault_windows(entry.get(), window) {
                    entry.insert(updated);
                    return true;
                }

                false
            }
        }
    }

    fn add_node_fault(
        &mut self,
        name: String,
        container_name: String,
        window: FaultWindowBounds,
    ) -> bool {
        match self.node.entry(name) {
            Entry::Vacant(entry) => {
                let mut by_container_name_map = IndexMap::new();
                by_container_name_map.insert(container_name, window);
                entry.insert(by_container_name_map);
                true
            }
            Entry::Occupied(mut container_name_to_window_map) => {
                match container_name_to_window_map.get_mut().entry(container_name) {
                    Entry::Vacant(entry) => {
                        entry.insert(window);
                        true
                    }
                    Entry::Occupied(mut entry) => {
                        if let Some(updated) = merge_fault_windows(entry.get(), window) {
                            entry.insert(updated);
                            return true;
                        }

                        false
                    }
                }
            }
        }
    }

    fn add_clock_fault(&mut self, offset: f64, window: FaultWindowBounds) -> bool {
        self.clock.push((offset, window));
        true
    }

    fn to_json(&self) -> Value {
        let mut result = Map::new();

        for entry in &self.network {
            result.insert(
                format!("network_{}", entry.0),
                json!({"vtime": entry.1.start_vtime}),
            );
        }

        for entry in &self.node {
            let mut node_fault_starts_by_container = Map::new();
            for entry in entry.1 {
                node_fault_starts_by_container
                    .insert(entry.0.to_string(), json!(entry.1.start_vtime));
            }

            result.insert(
                format!("node_{}", entry.0),
                Value::Object(node_fault_starts_by_container),
            );
        }

        if !&self.clock.is_empty() {
            let mut offset_sum = 0f64;
            let mut max_clock_fault_start = VTime::ZERO;

            for entry in &self.clock {
                offset_sum += entry.0;
                max_clock_fault_start = max_clock_fault_start.max(entry.1.start_vtime);
            }

            result.insert(
                "clock_skip".to_string(),
                json!({"cumulative_offset": offset_sum, "vtime": max_clock_fault_start}),
            );
        }

        Value::Object(result)
    }
}

fn merge_fault_windows(
    incumbent: &FaultWindowBounds,
    new: FaultWindowBounds,
) -> Option<FaultWindowBounds> {
    if new.start_vtime < incumbent.start_vtime {
        return Some(FaultWindowBounds {
            start_vtime: new.start_vtime,
            end_vtime: incumbent.end_vtime.and_then(|prev_expiry| {
                new.end_vtime.map(|new_expiry| new_expiry.max(prev_expiry))
            }),
        });
    }

    match incumbent.end_vtime {
        None => None,
        Some(prev_expiry) => match new.end_vtime {
            None => Some(FaultWindowBounds {
                start_vtime: incumbent.start_vtime,
                end_vtime: None,
            }),
            Some(new_expiry) => {
                if new_expiry > prev_expiry {
                    return Some(FaultWindowBounds {
                        start_vtime: incumbent.start_vtime,
                        end_vtime: Some(new_expiry),
                    });
                }

                None
            }
        },
    }
}

#[derive(Debug, Deserialize)]
struct AssertionPayload {
    hit: Option<bool>,
    condition: Option<bool>,
    #[serde(default)]
    must_hit: bool,
    message: Option<String>,
    assert_type: Option<String>,
    display_type: Option<String>,
    #[serde(default)]
    location: Option<AssertionLocation>,
}

#[derive(Debug, Deserialize)]
struct AssertionLocation {
    file: Option<String>,
    function: Option<String>,
    begin_line: Option<serde_json::Number>,
}

#[derive(Debug, PartialEq, Eq)]
struct AssertionSummary {
    label: String,
    status: AssertionStatus,
    message: String,
    must_hit: bool,
    location: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssertionStatus {
    Pass,
    Fail,
    Unhit,
}

impl Display for AssertionStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Unhit => "UNHIT",
        })
    }
}

impl TryFrom<AssertionPayload> for AssertionSummary {
    type Error = ();

    fn try_from(payload: AssertionPayload) -> std::result::Result<Self, Self::Error> {
        let hit = payload.hit.ok_or(())?;
        let condition = payload.condition.ok_or(())?;
        let message = payload
            .message
            .map(|message| message.trim().to_string())
            .filter(|message| !message.is_empty())
            .ok_or(())?;
        let label = payload
            .display_type
            .map(|label| label.trim().to_string())
            .filter(|label| !label.is_empty())
            .or_else(|| {
                payload
                    .assert_type
                    .map(|label| label.trim().to_string())
                    .filter(|label| !label.is_empty())
            })
            .ok_or(())?;

        let status = if !hit {
            AssertionStatus::Unhit
        } else if condition {
            AssertionStatus::Pass
        } else {
            AssertionStatus::Fail
        };

        Ok(Self {
            label,
            status,
            message,
            must_hit: payload.must_hit,
            location: payload.location.and_then(render_assertion_location),
        })
    }
}

fn render_event_entry(entry: &Value) -> RenderedEventEntry {
    let input_hash = entry["moment"]["input_hash"]
        .as_str()
        .unwrap_or("")
        .to_string();
    // The stream normalizes vtime to a JSON number; VTime's Display prints
    // the same text the server sent, so the column stays copy-paste exact.
    let vtime = VTime::from_json(&entry["moment"]["vtime"])
        .map(|v| v.to_string())
        .unwrap_or_default();
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let name = entry["source"]["name"].as_str().unwrap_or("");
    let stream = entry["source"]["stream"].as_str().unwrap_or("");

    // Trim the OUTPUT cell: container log lines often carry leading indentation
    // (e.g. `    GORACE: …`) that would ragged-align the column against neighbours.
    if let Some(summary) = parse_assertion_summary(entry) {
        return RenderedEventEntry {
            input_hash,
            vtime,
            source: render_source(container, name, Some("assert")),
            output: render_assertion_summary(&summary).trim().to_string(),
        };
    }

    RenderedEventEntry {
        input_hash,
        vtime,
        source: render_source(container, name, (!stream.is_empty()).then_some(stream)),
        output: render_event_output(entry).trim().to_string(),
    }
}

fn render_event_output(entry: &Value) -> String {
    if let Some(rendered) = render_known_event(entry) {
        return rendered;
    }
    if let Some(output_text) = entry.get("output_text").and_then(Value::as_str) {
        // Strip ANSI color codes before escaping controls so colorized container
        // output shows the plain text, not visible `\x1B[…` escape noise.
        return normalize_terminal_text(output_text);
    }
    sanitize(&serde_json::to_string(entry).unwrap_or_default())
}

struct EventKind {
    source_name: &'static str,
    fields: &'static [&'static str],
}

const EVENT_KINDS: &[EventKind] = &[
    EventKind {
        source_name: "antithesis_test_composer",
        fields: &[
            "task_status",
            "command",
            "container_id",
            "command_return_code",
            "command_runtime",
            "additional_stderr",
            "added_task",
            "got_pid_back",
            "tasks_len",
            "weight",
            "weight_type",
        ],
    },
    EventKind {
        source_name: "fault_injector",
        fields: &[
            "fault.name",
            "fault.type",
            "fault.details.disruption_type",
            "fault.affected_nodes",
            "fault.max_duration",
        ],
    },
];

fn render_known_event(entry: &Value) -> Option<String> {
    let source_name = entry["source"]["name"].as_str()?;
    let kind = EVENT_KINDS
        .iter()
        .find(|kind| kind.source_name == source_name)?;

    let parts: Vec<String> = kind
        .fields
        .iter()
        .filter_map(|path| {
            let value = lookup_path(entry, path)?;
            let rendered = format_event_value(value)?;
            Some(format!("{path}={rendered}"))
        })
        .collect();

    (!parts.is_empty()).then(|| parts.join(" "))
}

fn lookup_path<'a>(entry: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(entry, |current, segment| current.get(segment))
}

fn format_event_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) if s.is_empty() => None,
        Value::String(s) => Some(sanitize(s)),
        Value::Array(items) => {
            let scalars: Option<Vec<String>> = items
                .iter()
                .map(|item| match item {
                    Value::Null => Some(String::new()),
                    Value::Bool(b) => Some(b.to_string()),
                    Value::Number(n) => Some(n.to_string()),
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            let scalars = scalars?;
            if scalars.is_empty() {
                return None;
            }
            Some(sanitize(&scalars.join(",")))
        }
        Value::Object(_) => None,
    }
}

fn parse_assertion_summary(entry: &Value) -> Option<AssertionSummary> {
    let assertion = entry.get("antithesis_assert")?;
    let payload = AssertionPayload::deserialize(assertion).ok()?;
    AssertionSummary::try_from(payload).ok()
}

fn render_source(container: &str, name: &str, stream: Option<&str>) -> String {
    let label = if !container.trim().is_empty() {
        sanitize(container)
    } else {
        sanitize(name.trim().strip_prefix("antithesis_").unwrap_or(name))
    };
    let stream = stream.map(sanitize).filter(|stream| !stream.is_empty());

    match (label.is_empty(), stream) {
        (false, Some(stream)) => format!("[{label}:{stream}]"),
        (false, None) => format!("[{label}]"),
        (true, Some(stream)) => format!("[{stream}]"),
        (true, None) => "[]".to_string(),
    }
}

fn render_assertion_summary(summary: &AssertionSummary) -> String {
    let mut output = format!(
        "{} {} \"{}\"",
        summary.status,
        sanitize(&summary.label),
        sanitize(&summary.message),
    );

    if summary.must_hit {
        output.push_str(" must-hit");
    }

    if let Some(location) = &summary.location {
        output.push_str(" @ ");
        output.push_str(location);
    }

    output
}

fn render_assertion_location(location: AssertionLocation) -> Option<String> {
    let file = location.file.as_deref().and_then(file_basename);
    let function = location
        .function
        .as_deref()
        .map(str::trim)
        .filter(|function| !function.is_empty())
        .map(sanitize);
    let line = location.begin_line.map(|line| line.to_string());

    let mut rendered = String::new();

    if let Some(file) = file {
        rendered.push_str(&sanitize(file));
    }
    if let Some(function) = function {
        if !rendered.is_empty() {
            rendered.push(':');
        }
        rendered.push_str(&function);
    }
    if let Some(line) = line {
        if !rendered.is_empty() {
            rendered.push(':');
        }
        rendered.push_str(&line);
    }

    (!rendered.is_empty()).then_some(rendered)
}

fn file_basename(file: &str) -> Option<&str> {
    let trimmed = file.trim();
    if trimmed.is_empty() {
        return None;
    }

    Path::new(trimmed)
        .file_name()
        .and_then(|name| name.to_str())
        .or(Some(trimmed))
}

/// How the final column of a table is laid out once the leading columns have
/// been sized to their widest cell. The leading columns are always padded to
/// their widest cell; only the final column's policy varies.
enum LastColumn {
    /// Emit the final column verbatim with no padding or width bound. The table
    /// can grow as wide as its widest final cell.
    Raw,
    /// Bound the final column to whatever width remains after the leading columns
    /// fit within `total_width`, truncating a too-long cell with an ellipsis.
    Truncate { total_width: usize },
    /// Bound the final column like [`Truncate`], but wrap a too-long cell across
    /// multiple lines with a hanging indent under the column start (floored at a
    /// readable minimum width).
    Wrap { total_width: usize },
}

/// Per-column horizontal alignment for the leading (fixed-width) columns. The
/// final column is never padded, so its alignment is ignored.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Align {
    Left,
    Right,
}

/// Pad `cell` to `width` columns on the side dictated by `align`.
fn pad_cell(cell: &str, width: usize, align: Align) -> String {
    match align {
        Align::Left => format!("{cell:<width$}"),
        Align::Right => format!("{cell:>width$}"),
    }
}

/// Shared column-sizing core for every table snouty renders. Leading columns are
/// sized to their widest cell (header included, counted in chars) and aligned per
/// `aligns`; the final column follows `last_column`. Lines are right-trimmed, so
/// a `Raw` final column never leaves trailing padding. `aligns` must have one
/// entry per header (the last entry is ignored — the final column isn't padded).
fn render_columns(
    headers: &[String],
    rows: &[Vec<String>],
    last_column: LastColumn,
    aligns: &[Align],
) -> String {
    let last = headers.len() - 1;

    // Size every leading column to the widest of its header and cells. The final
    // column is sized below according to the policy.
    let mut widths = headers
        .iter()
        .map(|header| header.chars().count())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate().take(last) {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }

    // Two-space separators between columns; the leading columns plus separators
    // form the prefix a wrapped final column hangs under.
    let prefix_width: usize = widths.iter().take(last).sum::<usize>() + 2 * last;

    match &last_column {
        LastColumn::Raw => {
            // Final column unbounded: `push_table_row` emits it unpadded, so its
            // width never matters — leave `widths[last]` at the header width.
            let mut output = String::new();
            push_table_row(&mut output, headers, &widths, aligns);
            for row in rows {
                push_table_row(&mut output, row, &widths, aligns);
            }
            output.trim_end().to_string()
        }
        LastColumn::Truncate { total_width } => {
            let last_width = total_width
                .saturating_sub(prefix_width)
                .max(headers[last].chars().count());
            widths[last] = last_width;

            let mut output = String::new();
            push_table_row(&mut output, headers, &widths, aligns);
            for row in rows {
                let mut row = row.clone();
                row[last] = console::truncate_str(&row[last], last_width, "…").into_owned();
                push_table_row(&mut output, &row, &widths, aligns);
            }
            output.trim_end().to_string()
        }
        LastColumn::Wrap { total_width } => {
            let last_width = total_width
                .saturating_sub(prefix_width)
                .max(headers[last].chars().count())
                .max(20);

            let mut output = String::new();
            push_table_row(&mut output, headers, &widths, aligns);
            for row in rows {
                let wrapped = wrap_text(&row[last], last_width);
                let wrapped = if wrapped.is_empty() {
                    vec![String::new()]
                } else {
                    wrapped
                };
                for (line_index, line) in wrapped.iter().enumerate() {
                    if line_index == 0 {
                        for index in 0..last {
                            output.push_str(&pad_cell(&row[index], widths[index], aligns[index]));
                            output.push_str("  ");
                        }
                        output.push_str(line);
                    } else {
                        output.push_str(&format!("{:prefix_width$}{line}", ""));
                    }
                    output.push('\n');
                }
            }
            output.trim_end().to_string()
        }
    }
}

fn render_runs_table(runs: &[RunSummary], width: usize) -> String {
    // The default view omits the description entirely — it never fit usefully
    // beside the (necessarily full) run id, and `runs list --detail` shows it in
    // full. Test name is the final, width-bounded column truncated with an
    // ellipsis (a `runs show RUN` follow-up still works off the full id). A
    // launcher filter doesn't add a column — every row would carry the same
    // value; `--detail`/`--json` surface the launcher when it's actually wanted.
    let headers = vec![
        "RUN ID".to_string(),
        "STATUS".to_string(),
        "CREATED".to_string(),
        "TEST NAME".to_string(),
    ];

    let rows: Vec<Vec<String>> = runs
        .iter()
        .map(|run| {
            let test_name = run.test_name().map(sanitize).unwrap_or_else(|| "-".into());
            vec![
                sanitize(&run.run_id),
                run.status.to_string(),
                relative_time(run.created_at),
                test_name,
            ]
        })
        .collect();

    render_columns(
        &headers,
        &rows,
        LastColumn::Truncate { total_width: width },
        &left_aligned(headers.len()),
    )
}

/// All-left-aligned alignment vector for a table with `n` columns — the default
/// for tables that don't right-align any column.
fn left_aligned(n: usize) -> Vec<Align> {
    vec![Align::Left; n]
}

/// Auto-width table whose final column is emitted verbatim (no padding, no
/// width bound). All leading columns are left-aligned.
fn render_table(headers: &[String], rows: &[Vec<String>]) -> String {
    render_columns(headers, rows, LastColumn::Raw, &left_aligned(headers.len()))
}

/// Like [`render_table`], but the final column wraps to whatever width is left
/// over after the (fixed-width) leading columns, so a single long cell can't
/// push the table past `total_width`. Continuation lines indent to the start of
/// the final column. Leading columns are sized to their widest cell and aligned
/// per `aligns` (one entry per header; the final, wrapped column isn't padded).
fn render_table_wrap_last(
    headers: &[String],
    rows: &[Vec<String>],
    total_width: usize,
    aligns: &[Align],
) -> String {
    render_columns(headers, rows, LastColumn::Wrap { total_width }, aligns)
}

fn push_table_row(output: &mut String, row: &[String], widths: &[usize], aligns: &[Align]) {
    let last = row.len().saturating_sub(1);
    for (index, cell) in row.iter().enumerate() {
        if index > 0 {
            output.push_str("  ");
        }
        if index == last {
            output.push_str(cell);
        } else {
            output.push_str(&pad_cell(cell, widths[index], aligns[index]));
        }
    }
    output.push('\n');
}

/// Single choke point for terminal-bound free text: strip ANSI escape sequences
/// first, then escape any remaining control bytes so stray `\r`/`\x08`/BEL can't
/// corrupt the terminal. Used by both the `runs logs` `output_text` path and the
/// `runs events` OUTPUT column so the two render container output identically.
fn normalize_terminal_text(text: &str) -> String {
    sanitize(&strip_ansi(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;
    use serde_json::json;

    /// Test surface for the annotated-logs pipeline: classify the line
    /// exactly as the stream does, annotate entries, and return the emitted
    /// text. `None` means the stream would pass the line through raw.
    trait TryTransform {
        fn try_transform(&mut self, line: &str) -> Option<String>;
    }

    impl TryTransform for FaultAnnotator {
        fn try_transform(&mut self, line: &str) -> Option<String> {
            match classify_line(line) {
                NdjsonLine::Entry(mut entry) => {
                    self.annotate(&mut entry);
                    Some(entry.to_string())
                }
                NdjsonLine::Raw(_) => None,
            }
        }
    }

    #[test]
    fn renders_assertion_records_in_compact_form() {
        let entry = json!({
            "antithesis_assert": {
                "assert_type": "always",
                "condition": false,
                "details": null,
                "display_type": "AlwaysOrUnreachable",
                "hit": false,
                "id": "Counter's value retrieved",
                "location": {
                    "begin_column": 0,
                    "begin_line": 87,
                    "class": "",
                    "file": "/go/src/antithesis/control/control.go",
                    "function": "get"
                },
                "message": "Counter's value retrieved",
                "must_hit": false
            },
            "source": {
                "container": "control",
                "name": "control",
                "pid": 1
            },
            "moment": {
                "input_hash": "-4735081784258020614",
                "vtime": "311.8487535319291"
            }
        });

        assert_eq!(
            render_event_entry(&entry),
            RenderedEventEntry {
                input_hash: "-4735081784258020614".to_string(),
                vtime: "311.8487535319291".to_string(),
                source: "[control:assert]".to_string(),
                output:
                    "UNHIT AlwaysOrUnreachable \"Counter's value retrieved\" @ control.go:get:87"
                        .to_string(),
            }
        );
    }

    #[test]
    fn falls_back_to_plain_output_for_non_assertion_records() {
        let entry = json!({
            "output_text": "starting",
            "source": {
                "container": "app",
                "stream": "out"
            },
            "moment": {
                "vtime": "1.0"
            }
        });

        assert_eq!(
            render_event_entry(&entry),
            RenderedEventEntry {
                input_hash: "".to_string(),
                vtime: "1.0".to_string(),
                source: "[app:out]".to_string(),
                output: "starting".to_string(),
            }
        );
    }

    #[test]
    fn ignores_schema_valid_but_incomplete_assertions() {
        let entry = json!({
            "antithesis_assert": {},
            "output_text": "raw log line",
            "source": {
                "container": "control"
            },
            "moment": {
                "vtime": "5.0"
            }
        });

        assert_eq!(
            render_event_entry(&entry),
            RenderedEventEntry {
                input_hash: "".to_string(),
                vtime: "5.0".to_string(),
                source: "[control]".to_string(),
                output: "raw log line".to_string(),
            }
        );
    }

    #[test]
    fn renders_must_hit_and_partial_location_details() {
        let summary = AssertionSummary::try_from(AssertionPayload {
            hit: Some(false),
            condition: Some(false),
            must_hit: true,
            message: Some("setup reached".to_string()),
            assert_type: Some("reachability".to_string()),
            display_type: Some("SetupReached".to_string()),
            location: Some(AssertionLocation {
                file: None,
                function: Some("run_setup".to_string()),
                begin_line: Some(serde_json::Number::from(42)),
            }),
        })
        .unwrap();

        assert_eq!(
            render_assertion_summary(&summary),
            "UNHIT SetupReached \"setup reached\" must-hit @ run_setup:42"
        );
    }

    #[test]
    fn prefers_display_type_but_falls_back_to_assert_type() {
        let summary = AssertionSummary::try_from(AssertionPayload {
            hit: Some(true),
            condition: Some(true),
            must_hit: false,
            message: Some("first_setup ran".to_string()),
            assert_type: Some("sometimes".to_string()),
            display_type: Some("".to_string()),
            location: None,
        })
        .unwrap();

        assert_eq!(
            render_assertion_summary(&summary),
            "PASS sometimes \"first_setup ran\""
        );
    }

    #[test]
    fn source_without_stream_omits_trailing_colon() {
        assert_eq!(render_source("control", "", None), "[control]");
    }

    #[test]
    fn source_falls_back_to_name_when_container_empty() {
        assert_eq!(
            render_source("", "fault_injector", None),
            "[fault_injector]"
        );
    }

    #[test]
    fn source_strips_antithesis_prefix_from_name() {
        assert_eq!(
            render_source("", "antithesis_test_composer", None),
            "[test_composer]"
        );
    }

    #[test]
    fn source_prefers_container_over_name() {
        assert_eq!(render_source("client1", "python3.11", None), "[client1]");
    }

    #[test]
    fn source_combines_name_fallback_with_stream() {
        assert_eq!(
            render_source("", "antithesis_test_composer", Some("info")),
            "[test_composer:info]"
        );
    }

    #[test]
    fn renders_test_composer_event_with_name_fallback() {
        let entry = json!({
            "added_task": "parallel_driver_fetch",
            "tasks_len": "1",
            "source": {
                "name": "antithesis_test_composer",
                "pid": 974
            },
            "moment": {
                "input_hash": "5181922178177328213",
                "vtime": "315.41654103668407"
            }
        });

        assert_eq!(
            render_event_entry(&entry).source,
            "[test_composer]".to_string()
        );
    }

    #[test]
    fn renders_started_task_as_key_value_pairs() {
        let entry = json!({
            "command": "core/parallel_driver_fetch",
            "container_id": "d700ef3d05a263877d0d0c175f2954bdc8bc098faf501211b34bb20ba09f4435",
            "started_task": "d700ef3d_parallel_driver_fetch",
            "task_status": "started",
            "tasks_len": "1",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"vtime": "1.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "task_status=started command=core/parallel_driver_fetch container_id=d700ef3d05a263877d0d0c175f2954bdc8bc098faf501211b34bb20ba09f4435 tasks_len=1"
        );
    }

    #[test]
    fn renders_finished_task_omitting_empty_stderr() {
        let entry = json!({
            "additional_stderr": "",
            "additional_stdout": "",
            "command": "core/parallel_driver_fetch",
            "command_return_code": "0",
            "command_runtime": "2.1254637241363525",
            "finished_task": "abc",
            "task_status": "finished",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"vtime": "2.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "task_status=finished command=core/parallel_driver_fetch command_return_code=0 command_runtime=2.1254637241363525"
        );
    }

    #[test]
    fn renders_weight_event_as_key_value_pairs() {
        let entry = json!({
            "command": "abc_/opt/antithesis/test/v1/core/parallel_driver_fetch",
            "weight": "0.157917609630634",
            "weight_type": "masked_for_step",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"vtime": "7.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "command=abc_/opt/antithesis/test/v1/core/parallel_driver_fetch weight=0.157917609630634 weight_type=masked_for_step"
        );
    }

    #[test]
    fn renders_fault_event_with_nested_paths_and_array() {
        let entry = json!({
            "fault": {
                "name": "clog",
                "type": "network",
                "details": {"disruption_type": "Stopped"},
                "affected_nodes": ["client2", "setup"],
                "max_duration": 0.267319258
            },
            "source": {"name": "fault_injector"},
            "moment": {"vtime": "3.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "fault.name=clog fault.type=network fault.details.disruption_type=Stopped fault.affected_nodes=client2,setup fault.max_duration=0.267319258"
        );
    }

    #[test]
    fn renders_fault_event_with_empty_affected_nodes() {
        let entry = json!({
            "fault": {
                "name": "clog",
                "type": "network",
                "details": {"disruption_type": "Stopped"},
                "affected_nodes": [],
                "max_duration": 0.259267334
            },
            "source": {"name": "fault_injector"},
            "moment": {"vtime": "4.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "fault.name=clog fault.type=network fault.details.disruption_type=Stopped fault.max_duration=0.259267334"
        );
    }

    #[test]
    fn falls_back_to_json_dump_for_unknown_source() {
        let entry = json!({
            "antithesis_sdk": {"sdk_version": "0.2.0"},
            "source": {"container": "client1", "name": "python3.11"},
            "moment": {"vtime": "6.0"}
        });

        // source.name is not in EVENT_KINDS; no output_text; fall back to JSON.
        let output = render_event_entry(&entry).output;
        assert!(output.starts_with('{'), "expected JSON dump, got: {output}");
        assert!(output.contains("antithesis_sdk"));
    }

    #[test]
    fn event_output_strips_ansi_and_escapes_remaining_controls() {
        // The events OUTPUT column now runs through the shared terminal
        // normalizer (item 7): ANSI color codes are stripped (no visible
        // `\x1B[…` noise) and stray control bytes are escaped, not passed raw.
        let entry = json!({
            "output_text": "\x1B[4mhello\x1B[0m\u{0008}world\r\n",
            "source": {"container": "app", "stream": "out"},
            "moment": {"vtime": "1.0"}
        });
        let output = render_event_entry(&entry).output;
        // ANSI sequences are gone, the backspace/CR are escaped, and the
        // trailing newline is escaped as a single-line cell.
        assert_eq!(output, r"hello\x08world\r\n");
        assert!(!output.contains('\x1B'));
    }

    fn event(input_hash: &str, vtime: &str) -> Event {
        Event {
            moment: Moment {
                input_hash: input_hash.to_string(),
                vtime: vtime.parse().unwrap(),
            },
        }
    }

    fn event_property(
        name: &str,
        status: PropertyStatus,
        group: Option<&str>,
        examples: Vec<Event>,
        counterexamples: Vec<Event>,
    ) -> Property {
        let ex_count = examples.len() as u32;
        let cex_count = counterexamples.len() as u32;
        Property::EventProperty(EventProperty {
            counterexample_count: Some(cex_count),
            counterexamples,
            description: None,
            example_count: Some(ex_count),
            examples,
            group: group.map(str::to_string),
            is_event: true,
            is_group: None,
            name: name.to_string(),
            status,
        })
    }

    fn non_event_property(
        name: &str,
        status: PropertyStatus,
        examples: Vec<Value>,
        counterexamples: Vec<Value>,
    ) -> Property {
        let ex_count = examples.len() as u32;
        let cex_count = counterexamples.len() as u32;
        Property::NonEventProperty(NonEventProperty {
            counterexample_count: Some(cex_count),
            counterexamples,
            description: None,
            example_count: Some(ex_count),
            examples,
            group: None,
            is_event: false,
            is_group: None,
            name: name.to_string(),
            status,
        })
    }

    #[test]
    fn properties_table_renders_one_table_per_group() {
        let properties = vec![
            event_property(
                "Counter value stays below limit",
                PropertyStatus::Failing,
                Some("Safety"),
                vec![event("-300", "15.0")],
                vec![event("-100", "5.0"), event("-200", "10.0")],
            ),
            event_property(
                "Setup completes",
                PropertyStatus::Passing,
                None,
                vec![event("-400", "1.0")],
                vec![],
            ),
        ];

        let table = render_properties_table(&properties);
        let lines: Vec<&str> = table.lines().collect();

        // One table per group: the failing "Safety" group leads with its name as
        // a heading, followed by a STATUS/EXAMPLES/NAME table.
        assert_eq!(lines[0], "Safety");
        assert!(
            table.contains("\nSTATUS"),
            "expected a column header\n{table}"
        );
        assert!(table.contains("EXAMPLES"));
        assert!(table.contains("NAME"));
        assert!(!table.contains("GROUP"));
        // The group is the heading, not folded into NAME — so the displayed name
        // is exactly what a `--name` filter matches.
        assert!(
            !table.contains('▸'),
            "group should not be folded into NAME\n{table}"
        );

        // Counter property: 1 example + 2 counterexamples -> `1/2`, name only.
        let counter_row = lines.iter().find(|l| l.contains("Counter value")).unwrap();
        assert!(counter_row.contains("failing"));
        assert!(counter_row.contains("1/2"));
        assert!(
            counter_row
                .trim_end()
                .ends_with("Counter value stays below limit")
        );

        // Ungrouped properties land in a trailing "(ungrouped)" section.
        assert!(
            table.contains("(ungrouped)"),
            "expected an ungrouped section\n{table}"
        );
        let setup_row = lines
            .iter()
            .find(|l| l.contains("Setup completes"))
            .unwrap();
        assert!(setup_row.contains("passing"));
        assert!(setup_row.contains("1"));
    }

    #[test]
    fn all_ungrouped_properties_omit_the_ungrouped_heading() {
        // With no named groups, the lone "(ungrouped)" heading is just noise, so
        // it's omitted — the table is shown bare.
        let properties = vec![
            event_property(
                "Setup completes",
                PropertyStatus::Passing,
                None,
                vec![event("-400", "1.0")],
                vec![],
            ),
            event_property(
                "Teardown completes",
                PropertyStatus::Passing,
                None,
                vec![event("-401", "2.0")],
                vec![],
            ),
        ];

        let table = render_properties_table(&properties);
        assert!(
            !table.contains("(ungrouped)"),
            "all-ungrouped output should omit the heading\n{table}"
        );
        // The properties themselves still render.
        assert!(table.contains("Setup completes"), "{table}");
        assert!(table.contains("Teardown completes"), "{table}");

        // ...but a single named group brings the "(ungrouped)" heading back, to
        // distinguish the two sections.
        let mixed = vec![
            event_property(
                "Counter stays low",
                PropertyStatus::Failing,
                Some("Safety"),
                vec![],
                vec![event("-1", "1.0")],
            ),
            event_property(
                "Setup completes",
                PropertyStatus::Passing,
                None,
                vec![event("-400", "1.0")],
                vec![],
            ),
        ];
        assert!(render_properties_table(&mixed).contains("(ungrouped)"));
    }

    #[test]
    fn format_count_si_shortens_large_counts() {
        // Under 1000: exact.
        assert_eq!(format_count_si(0), "0");
        assert_eq!(format_count_si(20), "20");
        assert_eq!(format_count_si(885), "885");
        // One decimal below 10k, none above.
        assert_eq!(format_count_si(1000), "1.0k");
        assert_eq!(format_count_si(2323), "2.3k");
        assert_eq!(format_count_si(13396), "13k");
        assert_eq!(format_count_si(74843), "75k");
        assert_eq!(format_count_si(278493), "278k");
        // Millions.
        assert_eq!(format_count_si(18496678), "18M");
        // Rounding that tips a boundary steps up cleanly (never "10.0k"/"1000k").
        assert_eq!(format_count_si(9950), "10k");
        assert_eq!(format_count_si(999999), "1.0M");
    }

    fn event_prop(
        status: PropertyStatus,
        examples: Vec<Event>,
        counterexamples: Vec<Event>,
    ) -> EventProperty {
        match event_property("Counter", status, None, examples, counterexamples) {
            Property::EventProperty(p) => p,
            _ => unreachable!(),
        }
    }

    #[test]
    fn render_moments_table_uses_status_column() {
        let p = event_prop(
            PropertyStatus::Failing,
            vec![event("ex", "2.0")],
            vec![event("cex", "1.0")],
        );
        let out = render_moments_table(&p);
        assert!(out.contains("STATUS"));
        assert!(out.contains("HASH"));
        assert!(out.contains("VTIME"));
        assert!(out.contains("passing"));
        assert!(out.contains("failing"));
    }

    #[test]
    fn render_moments_table_sorts_each_group_by_vtime() {
        // API order is arbitrary; rows must come out failing-first, then passing,
        // each ascending by vtime numerically (5.0 < 10.0, not lexically).
        let p = event_prop(
            PropertyStatus::Failing,
            vec![event("ex-b", "20.0"), event("ex-a", "2.0")],
            vec![event("cex-b", "10.0"), event("cex-a", "5.0")],
        );
        let out = render_moments_table(&p);
        let rows: Vec<&str> = out
            .lines()
            .filter(|l| l.contains("failing") || l.contains("passing"))
            .collect();
        // Failing rows come first, sorted by vtime numerically (5.0 < 10.0).
        assert!(rows[0].contains("failing") && rows[0].contains("cex-a"));
        assert!(rows[1].contains("failing") && rows[1].contains("cex-b"));
        // Passing rows follow, also ascending by vtime (2.0 < 20.0).
        assert!(rows[2].contains("passing") && rows[2].contains("ex-a"));
        assert!(rows[3].contains("passing") && rows[3].contains("ex-b"));
    }

    #[test]
    fn render_non_event_detail_shows_result_not_examples() {
        let property = non_event_property(
            "Determinator Max Memory",
            PropertyStatus::Passing,
            vec![json!({
                "maximum_used_bytes": 17012928512u64,
                "percent_used": "0.04"
            })],
            vec![],
        );
        let out = render_property_detail(&property);
        // A non-event property's value shows under a `Result` label (no `:`) —
        // never the moment-oriented `Examples` table.
        assert!(out.contains("Result"), "got: {out}");
        assert!(out.contains("maximum_used_bytes"));
        assert!(!out.contains("Examples"));
        // The label carries no colon, matching Name/Status/Details.
        assert!(!out.contains("Result:"), "got: {out}");
    }

    #[test]
    fn render_properties_detail_groups_and_indents_examples() {
        let properties = vec![
            event_property(
                "First",
                PropertyStatus::Failing,
                Some("Safety"),
                vec![],
                vec![event("h", "1.0")],
            ),
            event_property(
                "Second",
                PropertyStatus::Passing,
                None,
                vec![event("h2", "2.0")],
                vec![],
            ),
        ];
        let out = render_properties_detail(&properties);
        // Grouped: the "Safety" group heads its section, ungrouped trails.
        assert!(out.contains("Safety"), "got: {out}");
        assert!(out.contains("(ungrouped)"));
        // No Group key/value line (the heading carries it) and no rule separator.
        assert!(!out.contains("Group     Safety"));
        assert!(!out.contains('─'));
        // Each property keeps its header and an Examples section whose table is
        // indented beneath the (column-0) "Examples" label (no `:`).
        assert!(out.contains("Name      First"));
        assert_eq!(out.matches("Examples\n").count(), 2);
        assert!(
            out.contains("Examples\n  STATUS"),
            "examples table should be indented\n{out}"
        );
    }

    #[test]
    fn render_property_detail_result_forms() {
        // A lone scalar sits inline next to an aligned `Result` label.
        for (value, expected) in [
            (json!(1234), "Result    1234"),
            (json!("n2-standard-4"), "Result    n2-standard-4"),
            (json!(true), "Result    true"),
        ] {
            let p = non_event_property("Metric", PropertyStatus::Passing, vec![value], vec![]);
            let out = render_property_detail(&p);
            assert!(out.contains(expected), "got: {out}");
            assert!(!out.contains("Examples"));
        }

        // Tiny objects/arrays inline against the `Result` column (compact JSON,
        // no `:` — same value column as a scalar).
        let empty = non_event_property("E", PropertyStatus::Passing, vec![json!([])], vec![]);
        assert!(
            render_property_detail(&empty).contains("Result    []"),
            "empty array"
        );
        let tiny = non_event_property("T", PropertyStatus::Passing, vec![json!({"k": 1})], vec![]);
        assert!(
            render_property_detail(&tiny).contains(r#"Result    {"k":1}"#),
            "tiny object"
        );

        // A large object spills to an indented pretty-printed block under `Result`.
        let big = non_event_property(
            "Big",
            PropertyStatus::Passing,
            vec![json!({
                "session_id": "dfa97857ebbbc219f543e423fea597fd-54-8",
                "total_output_mb": 11773,
                "total_output_rate": "35.05274518966351"
            })],
            vec![],
        );
        let out = render_property_detail(&big);
        assert!(
            out.contains("Result\n  {"),
            "big object should be a block\n{out}"
        );
        assert!(out.contains("total_output_mb"));
        assert!(!out.contains("Examples"));

        // Several small values collapse into one inline JSON array.
        let multi = non_event_property(
            "Two values",
            PropertyStatus::Passing,
            vec![json!(1), json!(2)],
            vec![],
        );
        assert!(
            render_property_detail(&multi).contains("Result    [1,2]"),
            "multi inline"
        );
    }

    #[test]
    fn format_log_line_renders_json_record_with_stripped_envelope() {
        let entry = json!({
            "moment": {"input_hash": "6409410329507290816", "vtime": "9.093"},
            "IPT_bytes_out": 126952,
            "source": {"name": "fault_injector", "pid": 924},
            "info": {"details": {"started": true}, "message": "status"}
        });
        assert_eq!(
            format_log_entry(&entry, false),
            "[   9.093] [          fault_injector] [   ]  - {\"info\":{\"details\":{\"started\":true},\"message\":\"status\"}}"
        );
    }

    #[test]
    fn format_log_line_renders_text_record_with_inf_stream() {
        let entry = json!({
            "moment": {"input_hash": "1", "vtime": "15.174"},
            "source": {"container": "bank/first_setup.sh", "name": "bank/first_setup.sh", "stream": "info"},
            "output_text": "NbmXgEki  INFO main lsm_tree::tree::ingest: Finished ingestion writer"
        });
        assert_eq!(
            format_log_entry(&entry, false),
            "[  15.174] [     bank/first_setup.sh] [inf] NbmXgEki  INFO main lsm_tree::tree::ingest: Finished ingestion writer"
        );
    }

    #[test]
    fn format_log_line_strips_ansi_from_output_text() {
        let entry = json!({
            "moment": {"input_hash": "1", "vtime": "14.118"},
            "source": {"name": "setup", "stream": "error"},
            "output_text": "\x1B[4m>>>> hello\x1B[0m"
        });
        let rendered = format_log_entry(&entry, false);
        assert!(rendered.contains(">>>> hello"));
        assert!(!rendered.contains('\x1B'));
        assert!(rendered.contains("[err]"));
    }

    #[test]
    fn format_log_entry_raw_preserves_ansi_in_output_text() {
        // `--raw` opts out of the terminal normalizer: ANSI colors (and any
        // other control bytes) in the payload reach the terminal verbatim.
        let entry = json!({
            "moment": {"input_hash": "1", "vtime": "14.118"},
            "source": {"name": "setup", "stream": "error"},
            "output_text": "\x1B[4m>>>> hello\x1B[0m"
        });
        let rendered = format_log_entry(&entry, true);
        assert!(rendered.contains("\x1B[4m>>>> hello\x1B[0m"));
        assert!(rendered.contains("[err]"));
    }

    #[test]
    fn format_log_line_truncates_vtime_to_three_decimals() {
        let entry = json!({
            "moment": {"input_hash": "1", "vtime": "18.9148034489"},
            "source": {"name": "client", "stream": "info"},
            "output_text": "hello"
        });
        let rendered = format_log_entry(&entry, false);
        // Fixed 3 decimals (so the column stays aligned), truncated not rounded:
        // 18.9148… -> 18.914 (rounding would give 18.915).
        assert!(rendered.starts_with("[  18.914] "), "got: {rendered}");
    }

    #[test]
    fn format_log_line_accepts_numeric_vtime() {
        // A JSON-number vtime (not a string) must still render, not blank out,
        // and gets the same fixed-3-decimal treatment (12.5 -> 12.500).
        let entry = json!({
            "moment": {"input_hash": "1", "vtime": 12.5},
            "source": {"name": "client", "stream": "info"},
            "output_text": "hello"
        });
        assert!(
            format_log_entry(&entry, false).starts_with("[  12.500] "),
            "got: {}",
            format_log_entry(&entry, false)
        );
    }

    #[test]
    fn truncate_decimals_keeps_fixed_precision_without_rounding() {
        // Always exactly 3 decimals, zero-padded, so a column aligns.
        assert_eq!(truncate_decimals("19", 3), "19.000");
        assert_eq!(truncate_decimals("19.0", 3), "19.000");
        assert_eq!(truncate_decimals("14.78", 3), "14.780");
        // Truncates, never rounds: 1814.7135… -> .713 (rounding would give .714).
        assert_eq!(truncate_decimals("1814.7135719023645", 3), "1814.713");
        assert_eq!(truncate_decimals("18.9148034489", 3), "18.914");
        // Non-plain input: scientific notation falls back to fixed-point, and a
        // non-number is passed through untouched.
        assert_eq!(truncate_decimals("1e3", 3), "1000.000");
        assert_eq!(truncate_decimals("n/a", 3), "n/a");
    }

    #[test]
    fn exec_event_parses_each_stream_shape() {
        // Real event shapes captured from the live API (orbitinghail, r60).
        let output: ExecEvent = serde_json::from_str(
            r#"{"type":"output","stream":"stderr","text":"hello-err","moment":{"input_hash":"-3160476794197372487","vtime":"16.304728139657527"}}"#,
        )
        .unwrap();
        let ExecEvent::Output {
            stream,
            text,
            moment,
            ..
        } = output
        else {
            panic!("expected output event");
        };
        assert_eq!((stream.as_str(), text.as_str()), ("stderr", "hello-err"));
        // The output moment is kept, not discarded — the rendered JSON form
        // will show it.
        let moment = moment.expect("an output event carries its moment");
        assert_eq!(moment.input_hash, "-3160476794197372487");
        assert_eq!(moment.vtime.to_string(), "16.304728139657527");

        let exited: ExecEvent = serde_json::from_str(
            r#"{"type":"exited","exit_code":5,"end_moment":{"input_hash":"-316","vtime":"16.3"}}"#,
        )
        .unwrap();
        let ExecEvent::Exited {
            exit_code,
            end_moment,
            ..
        } = exited
        else {
            panic!("expected exited event");
        };
        assert_eq!(exit_code, Some(5));
        // VTime's Display is exact, so a vtime copied off the trailer names
        // the same moment.
        let m = end_moment.expect("exited carries a moment");
        assert_eq!(m.input_hash, "-316");
        assert_eq!(m.vtime.to_string(), "16.3");

        // `exit_code` is nullable ("no exit code was available").
        let ExecEvent::Exited { exit_code, .. } = serde_json::from_str(
            r#"{"type":"exited","exit_code":null,"end_moment":{"input_hash":"-1","vtime":"1.0"}}"#,
        )
        .unwrap() else {
            panic!("expected exited event");
        };
        assert_eq!(exit_code, None);

        assert!(matches!(
            serde_json::from_str(r#"{"type":"timed_out"}"#).unwrap(),
            ExecEvent::TimedOut { .. }
        ));
    }

    #[test]
    fn exec_frame_decodes_any_object() {
        // The stream handler treats a decode failure as an error. This says
        // why that arm should never run: `Unknown(Value)` accepts any JSON, so
        // no shape the server can send fails to decode. If this ever fails,
        // the error path in `cmd_runs_exec` is the right place to find out.
        for line in [
            r#"{}"#,
            r#"{"type":null}"#,
            r#"{"type":"output"}"#,
            r#"{"type":"exited","exit_code":"not-a-number"}"#,
            r#"{"type":42,"nested":{"deep":[1,2,{"x":null}]}}"#,
            r#"{"type":"output","stream":[],"text":{}}"#,
        ] {
            let entry: Value = serde_json::from_str(line).unwrap();
            assert!(
                ExecFrame::deserialize(&entry).is_ok(),
                "should decode: {line}"
            );
        }
    }

    #[test]
    fn exec_frame_keeps_what_it_does_not_understand() {
        // The stream may grow frames and fields while the command is a work
        // in progress, so neither kind may fail the stream.
        let frame: ExecFrame = serde_json::from_str(r#"{"type":"heartbeat","at":"12.5"}"#).unwrap();
        let ExecFrame::Unknown(value) = frame else {
            panic!("an unknown frame type is kept whole");
        };
        assert_eq!(value["type"], json!("heartbeat"));
        assert_eq!(value["at"], json!("12.5"));

        // A known frame with a field this build doesn't know keeps that field
        // rather than dropping it.
        let frame: ExecFrame = serde_json::from_str(
            r#"{"type":"output","stream":"stdout","text":"hi","moment":{"input_hash":"-1","vtime":"1.0"},"truncated":true}"#,
        )
        .unwrap();
        let ExecFrame::Known(ExecEvent::Output { extra, .. }) = frame else {
            panic!("expected a known output frame");
        };
        assert_eq!(extra.get("truncated"), Some(&json!(true)));
    }

    #[test]
    fn normalize_vtime_field_normalizes_either_moment_key() {
        // The exited event's moment sits under `end_moment`; the rule that
        // rewrites the seconds string to an exact JSON number is the same one
        // `moment` gets, so --json output agrees across commands.
        let mut entry = json!({"end_moment": {"input_hash": "-1", "vtime": "398.4898056755774"}});
        normalize_vtime_field(&mut entry, "end_moment");
        assert_eq!(entry["end_moment"]["vtime"], json!(398.4898056755774));
        assert!(entry["end_moment"]["vtime"].is_number());

        // An entry without that key is left untouched.
        let mut entry = json!({"moment": {"vtime": "1.0"}});
        assert_eq!(normalize_vtime_field(&mut entry, "end_moment"), None);
        assert_eq!(entry["moment"]["vtime"], json!("1.0"));
    }

    #[test]
    fn resolve_exec_script_takes_the_argument_verbatim() {
        assert_eq!(
            resolve_exec_script(Some("uname -a".to_string())).unwrap(),
            "uname -a".to_string()
        );
        // A blank argument is "no script". The same rule covers a blank
        // stdin, which specs/runs_exec.txt drives end to end.
        let err = resolve_exec_script(Some("  ".to_string())).unwrap_err();
        assert!(err.to_string().contains("no script provided"), "{err}");
    }

    #[test]
    fn read_script_from_stdin_accepts_piped_input() {
        // A pipe, a redirect, and a heredoc all arrive here non-interactive.
        let script = read_script_from_stdin(&b"uname -a\n"[..], false).unwrap();
        assert_eq!(script, "uname -a\n");
    }

    #[test]
    fn read_script_from_stdin_refuses_an_interactive_terminal() {
        // Nothing is read: with SCRIPT omitted at a prompt, waiting on stdin
        // would look like a hang.
        let err = read_script_from_stdin(&b"uname -a"[..], true).unwrap_err();
        assert!(err.to_string().contains("no script provided"), "{err}");
    }

    #[test]
    fn read_script_from_stdin_reports_size_before_utf8() {
        // An over-cap script is refused for its size even when the cap
        // boundary splits a multi-byte character. Reading straight into a
        // `String` validated the whole buffer first, so this failed as
        // "invalid UTF-8" and never mentioned the limit.
        let two_byte_chars = "é".repeat(MAX_SCRIPT_BYTES as usize); // 2 bytes each
        let err = read_script_from_stdin(two_byte_chars.as_bytes(), false).unwrap_err();
        assert!(
            err.to_string().contains("larger than the 2 MiB limit"),
            "{err}"
        );

        // Binary input over the cap is the same case, and the one the doc
        // comment names.
        let binary = vec![0xffu8; MAX_SCRIPT_BYTES as usize + 1];
        let err = read_script_from_stdin(binary.as_slice(), false).unwrap_err();
        assert!(
            err.to_string().contains("larger than the 2 MiB limit"),
            "{err}"
        );
    }

    #[test]
    fn read_script_from_stdin_rejects_invalid_utf8_under_the_cap() {
        // Under the cap, a non-text script says so plainly rather than
        // surfacing the raw io error.
        let err = read_script_from_stdin(&[0xff, 0xfe][..], false).unwrap_err();
        assert!(err.to_string().contains("not valid UTF-8"), "{err}");
    }

    #[test]
    fn read_script_from_stdin_caps_the_script_size() {
        // Exactly at the cap is fine; one byte over is refused rather than
        // silently truncated into a script that means something else.
        let at_cap = vec![b'x'; MAX_SCRIPT_BYTES as usize];
        assert_eq!(
            read_script_from_stdin(at_cap.as_slice(), false)
                .unwrap()
                .len(),
            MAX_SCRIPT_BYTES as usize
        );

        let over_cap = vec![b'x'; MAX_SCRIPT_BYTES as usize + 1];
        let err = read_script_from_stdin(over_cap.as_slice(), false).unwrap_err();
        assert!(
            err.to_string().contains("larger than the 2 MiB limit"),
            "{err}"
        );
    }

    #[test]
    fn parse_stream_error_extracts_the_server_message() {
        assert_eq!(
            parse_stream_error(r#"{"error":"mock stream failure"}"#).as_deref(),
            Some("mock stream failure")
        );
        // additionalProperties is open on Stream_Error: extra keys don't
        // stop the line from being an error.
        assert_eq!(
            parse_stream_error(r#"{"error":"boom","request_id":"r-1"}"#).as_deref(),
            Some("boom")
        );
    }

    #[test]
    fn parse_stream_error_leaves_data_records_alone() {
        // A log/event record carrying its own top-level `error` field is data:
        // the `moment` envelope identifies it.
        assert_eq!(
            parse_stream_error(r#"{"error":"disk full","moment":{"vtime":"1.0"}}"#),
            None
        );
        // A build log line is identified by its `timestamp`.
        assert_eq!(
            parse_stream_error(r#"{"error":"x","timestamp":"2025-03-20T02:01:12Z"}"#),
            None
        );
        // `error` under output_text (or any nested position) is not the signal.
        assert_eq!(
            parse_stream_error(r#"{"output_text":"an \"error\" happened","moment":{}}"#),
            None
        );
        // A non-string `error`, a non-object line, and a non-JSON line are not
        // stream errors.
        assert_eq!(parse_stream_error(r#"{"error":42}"#), None);
        assert_eq!(parse_stream_error(r#"["error"]"#), None);
        assert_eq!(parse_stream_error("plain text with \"error\" in it"), None);
    }

    #[test]
    fn format_log_vtime_agrees_between_string_and_number_forms() {
        // A vtime rendered from the JSON-number form (snouty's own --json
        // output) must land on the same truncated — never rounded — text as
        // the server's string form, so a value copied off the screen and fed
        // back as --begin-vtime lands on the line you saw, never past it.
        for s in ["398.4898056755774", "311.8487535319291", "402.0", "1.9995"] {
            let vtime: VTime = s.parse().unwrap();
            let from_string = format_log_vtime(&json!({"moment": {"vtime": s}}));
            let from_number = format_log_vtime(&json!({"moment": {"vtime": vtime}}));
            assert_eq!(from_string, from_number, "for {s}");
            assert_eq!(from_string, truncate_decimals(s, 3), "for {s}");
        }
        // Spot-check the truncation: .4898… cuts to .489 (rounding gives .490).
        assert_eq!(
            format_log_vtime(&json!({"moment": {"vtime": "398.4898056755774"}})),
            "398.489"
        );
    }

    #[test]
    fn format_log_line_overflows_source_column_when_too_long() {
        let entry = json!({
            "moment": {"vtime": "14.284"},
            "source": {
                "container": "antithesis/pods/client/sdk.jsonl",
                "name": "antithesis/pods/client/sdk.jsonl"
            },
            "antithesis_setup": {"details": null, "status": "complete"}
        });
        assert_eq!(
            format_log_entry(&entry, false),
            "[  14.284] [antithesis/pods/client/sdk.jsonl] [   ]  - {\"antithesis_setup\":{\"details\":null,\"status\":\"complete\"}}"
        );
    }

    #[test]
    fn format_log_line_fits_test_composer_source_exactly() {
        // The source column is sized to `antithesis_test_composer`, a built-in
        // source in nearly every run, so it fills the column exactly — no leading
        // pad before it and no overflow past it.
        let entry = json!({
            "moment": {"vtime": "401.500"},
            "source": {"name": "antithesis_test_composer"},
            "output_text": "started"
        });
        assert_eq!(
            format_log_entry(&entry, false),
            "[ 401.500] [antithesis_test_composer] [   ] started"
        );
    }

    #[test]
    fn render_moments_table_marks_unreachable_when_empty() {
        let p = event_prop(PropertyStatus::Passing, vec![], vec![]);
        assert!(render_moments_table(&p).contains("unreachable"));
    }

    #[test]
    fn trim_blank_edges_drops_only_outer_blanks() {
        let lines = vec![
            String::new(),
            "a".to_string(),
            String::new(),
            "b".to_string(),
            String::new(),
        ];
        assert_eq!(
            trim_blank_edges(&lines),
            &["a".to_string(), String::new(), "b".to_string()]
        );
    }

    #[test]
    fn render_result_handles_no_values() {
        // A non-event property with no values at all -> "(none)".
        let empty = non_event_property("Empty", PropertyStatus::Passing, vec![], vec![]);
        let out = render_property_detail(&empty);
        assert!(out.contains("Result    (none)"), "got: {out}");
        assert!(!out.contains("Examples"));
    }

    #[test]
    fn render_result_labels_failing_non_event_counterexamples() {
        // A failing non-event property carries the violating value(s) in
        // `counterexamples`. They must be labelled and shown first, so the
        // offending value isn't lost among the satisfying `examples`.
        let p = non_event_property(
            "Peak memory below limit",
            PropertyStatus::Failing,
            vec![json!(720), json!(880)], // satisfying
            vec![json!(1340)],            // violating
        );
        let out = render_property_detail(&p);
        // Both groups are labelled (no `:`), with counterexamples first.
        let cex = out
            .find("Counter-examples\n")
            .expect("counter-examples label");
        let ex = out.find("\nExamples\n").expect("examples label");
        assert!(cex < ex, "counterexamples should come first\n{out}");
        // The violating value sits under the Counter-examples heading, the
        // satisfying ones under Examples — not merged into one unlabelled list.
        assert!(out.contains("Counter-examples\n  1340"), "got: {out}");
        assert!(out.contains("Examples\n  720\n  880"), "got: {out}");
        // No anonymous `Result` blob in the failing case.
        assert!(!out.contains("Result"), "got: {out}");
    }

    #[test]
    fn render_result_failing_non_event_without_examples_only_shows_counterexamples() {
        let p = non_event_property(
            "Invariant held",
            PropertyStatus::Failing,
            vec![],
            vec![json!("n2-standard-4")],
        );
        let out = render_property_detail(&p);
        assert!(
            out.contains("Counter-examples\n  n2-standard-4"),
            "got: {out}"
        );
        // No standalone Examples group (there are no satisfying examples). Guard
        // against the "Examples" inside "Counter-examples" by anchoring on `\n`.
        assert!(!out.contains("\nExamples"), "got: {out}");
    }

    #[test]
    fn ansi_sgr() {
        assert_eq!(strip_ansi("\x1b[1mbold\x1b[0m"), "bold");
        assert_eq!(strip_ansi("\x1b[38;5;196mred\x1b[0m"), "red");
        assert_eq!(strip_ansi("\x1b[38;2;255;0;0mred\x1b[0m"), "red");
        assert_eq!(strip_ansi("\x1b[1;31;42mtext\x1b[0m"), "text");
        assert_eq!(
            strip_ansi(
                "\x1b[2m2026-04-03T08:19:54Z\x1b[0m \x1b[32m INFO\x1b[0m \x1b[2mfoobar\x1b[0m\x1b[2m:\x1b[0m ready"
            ),
            "2026-04-03T08:19:54Z  INFO foobar: ready"
        );
    }

    #[test]
    fn ansi_csi_non_sgr() {
        assert_eq!(strip_ansi("left\x1b[2Aright"), "leftright");
        assert_eq!(strip_ansi("text\x1b[2K"), "text");
        assert_eq!(strip_ansi("\x1b[?25hvisible"), "visible");
        assert_eq!(strip_ansi("\x1b[?25l hidden"), " hidden");
    }

    #[test]
    fn ansi_osc() {
        assert_eq!(
            strip_ansi("\x1b]0;my window title\x07text after"),
            "text after"
        );
        assert_eq!(strip_ansi("\x1b]0;my title\x1b\\text after"), "text after");
    }

    #[test]
    fn ansi_two_byte() {
        assert_eq!(strip_ansi("\x1bcafter reset"), "after reset");
        assert_eq!(strip_ansi("before\x1b7after"), "beforeafter");
    }

    #[test]
    fn ansi_passthrough() {
        let cases = [
            "no escapes here",
            r#"{"key": "value", "nested": {"a": [1,2,3]}}"#,
            r#"{"url": "http://example.com/path?q=1&r=2", "count": 42}"#,
            r#"Options { address: Some(0.0.0.0:3307), deployment: "mydb", mode: Standalone }"#,
            r#"Settings { inner: Inner { values: [1, 2, 3] }, name: "test" }"#,
            "[2026-04-03] [INFO] [main] started",
            r#"path: "/nix/store/abc-pkg/bin/cmd""#,
            r#"{"msg": "he said \"hello\""}"#,
        ];
        for c in cases {
            assert_eq!(strip_ansi(c), c, "passthrough failed: {c:?}");
        }
    }

    #[test]
    fn ansi_mixed() {
        assert_eq!(
            strip_ansi("\x1b[2m{\"key\": \"value\"}\x1b[0m"),
            r#"{"key": "value"}"#
        );
        assert_eq!(
            strip_ansi("\x1b[3mOptions { mode: Standalone }\x1b[0m"),
            "Options { mode: Standalone }"
        );
        assert_eq!(
            strip_ansi(
                "\x1b[2m2026-04-03T00:00:00Z\x1b[0m \x1b[32m INFO\x1b[0m request completed {\"status\": 200, \"latency_ms\": 42}"
            ),
            r#"2026-04-03T00:00:00Z  INFO request completed {"status": 200, "latency_ms": 42}"#
        );
    }

    #[test]
    fn tracks_which_faults_are_active_based_on_vtime_and_max_duration() {
        let mut transformer = FaultAnnotator::default();

        // No faults yet
        assert_eq!(
            transformer.try_transform("{\"foo\":\"bar\"}"),
            Some("{\"foo\":\"bar\",\"active_faults\":{}}".to_string())
        );

        // Open a network partition fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "1"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["a", "b"],
                    "max_duration": 10
                }
            }))),
            Some("{\"moment\":{\"vtime\":1.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"partition\",\"type\":\"network\",\"affected_nodes\":[\"a\",\"b\"],\"max_duration\":10},\"active_faults\":{\"network_partition\":{\"vtime\":1.0}}}".to_string())
        );

        // Another log message; should retain active window state since the log message had no timestamp
        assert_eq!(
            transformer.try_transform("{\"foo\":\"bar\"}"),
            Some(
                "{\"foo\":\"bar\",\"active_faults\":{\"network_partition\":{\"vtime\":1.0}}}"
                    .to_string()
            )
        );

        // Open a node throttled fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "2"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "throttle",
                    "type": "node",
                    "affected_nodes": ["c"],
                    "max_duration": 9
                }
            }))),
            Some("{\"moment\":{\"vtime\":2.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"throttle\",\"type\":\"node\",\"affected_nodes\":[\"c\"],\"max_duration\":9},\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Another non-fault injector message; should retain state
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "11"
                },
                "foo": "bar"
            }))),
            Some("{\"moment\":{\"vtime\":11.0},\"foo\":\"bar\",\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Expire both windows
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": {
                        "vtime": "11.5"
                    },
                    "foo": "bar"
                })
            )),
            Some("{\"moment\":{\"vtime\":11.5},\"foo\":\"bar\",\"active_faults\":{}}".to_string())
        );
    }

    #[test]
    fn restore_closes_network_faults() {
        let mut transformer = FaultAnnotator::default();

        // Open a network partition fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "1"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["a", "b"]
                }
            }))),
            Some("{\"moment\":{\"vtime\":1.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"partition\",\"type\":\"network\",\"affected_nodes\":[\"a\",\"b\"]},\"active_faults\":{\"network_partition\":{\"vtime\":1.0}}}".to_string())
        );

        // Open a node throttled fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "2"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "throttle",
                    "type": "node",
                    "affected_nodes": ["c"]
                }
            }))),
            Some("{\"moment\":{\"vtime\":2.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"throttle\",\"type\":\"node\",\"affected_nodes\":[\"c\"]},\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Open a network clog fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "3"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "clog",
                    "type": "network",
                    "affected_nodes": ["b", "c"]
                }
            }))),
            Some("{\"moment\":{\"vtime\":3.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"clog\",\"type\":\"network\",\"affected_nodes\":[\"b\",\"c\"]},\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Verify that state is retained for a non-control log message
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({"foo": "bar"}))),
            Some("{\"foo\":\"bar\",\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Send a network restore message
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "restore",
                    "type": "network"
                }
            }))),
            Some("{\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"restore\",\"type\":\"network\"},\"active_faults\":{\"node_throttle\":{\"c\":2.0}}}".to_string())
        );
    }

    #[test]
    fn fault_injector_pause_clears_network_and_node_faults() {
        let mut transformer = FaultAnnotator::default();

        // Open a network partition fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "1"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["a", "b"]
                }
            }))),
            Some("{\"moment\":{\"vtime\":1.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"partition\",\"type\":\"network\",\"affected_nodes\":[\"a\",\"b\"]},\"active_faults\":{\"network_partition\":{\"vtime\":1.0}}}".to_string())
        );

        // Open a node throttled fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "2"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "throttle",
                    "type": "node",
                    "affected_nodes": ["c"]
                }
            }))),
            Some("{\"moment\":{\"vtime\":2.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"throttle\",\"type\":\"node\",\"affected_nodes\":[\"c\"]},\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Open a network clog fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "3"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "clog",
                    "type": "network",
                    "affected_nodes": ["b", "c"]
                }
            }))),
            Some("{\"moment\":{\"vtime\":3.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"clog\",\"type\":\"network\",\"affected_nodes\":[\"b\",\"c\"]},\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Open a clock fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "4"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": {
                        "offset": 10.5
                    }
                }
            }))),
            Some("{\"moment\":{\"vtime\":4.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"skip\",\"type\":\"clock\",\"details\":{\"offset\":10.5}},\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0},\"clock_skip\":{\"cumulative_offset\":10.5,\"vtime\":4.0}}}".to_string())
        );

        // Verify that state is retained for a non-control log message
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({"foo": "bar"}))),
            Some("{\"foo\":\"bar\",\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0},\"clock_skip\":{\"cumulative_offset\":10.5,\"vtime\":4.0}}}".to_string())
        );

        // Send a fault injector pause message
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "source": {
                    "name": "fault_injector"
                },
                "info": {
                    "message": "status",
                    "details": {
                        "paused": true
                    }
                }
            }))),
            Some("{\"source\":{\"name\":\"fault_injector\"},\"info\":{\"message\":\"status\",\"details\":{\"paused\":true}},\"active_faults\":{\"clock_skip\":{\"cumulative_offset\":10.5,\"vtime\":4.0}}}".to_string())
        );
    }

    #[test]
    fn clock_offsets_are_combined() {
        let mut transformer = FaultAnnotator::default();

        // Open a network partition fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "1"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": {
                        "offset": 10.5
                    }
                }
            }))),
            Some("{\"moment\":{\"vtime\":1.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"skip\",\"type\":\"clock\",\"details\":{\"offset\":10.5}},\"active_faults\":{\"clock_skip\":{\"cumulative_offset\":10.5,\"vtime\":1.0}}}".to_string())
        );

        // Open a node throttled fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "2"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": {
                        "offset": 0.1
                    }
                }
            }))),
            Some("{\"moment\":{\"vtime\":2.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"skip\",\"type\":\"clock\",\"details\":{\"offset\":0.1}},\"active_faults\":{\"clock_skip\":{\"cumulative_offset\":10.6,\"vtime\":2.0}}}".to_string())
        );

        // Open a network clog fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "vtime": "3"
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": {
                        "offset": -2.3
                    }
                }
            }))),
            Some("{\"moment\":{\"vtime\":3.0},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"skip\",\"type\":\"clock\",\"details\":{\"offset\":-2.3}},\"active_faults\":{\"clock_skip\":{\"cumulative_offset\":8.3,\"vtime\":3.0}}}".to_string())
        );
    }

    #[test]
    fn empty_affected_nodes_does_not_open_network_window() {
        let mut transformer = FaultAnnotator::default();

        // Open a real window first
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "clog",
                    "type": "network",
                    "affected_nodes": ["node-1"],
                    "max_duration": 100
                }
            })
        ));

        // Empty affected_nodes: network and node faults are only considered active if at least one node is affected,
        // so no new window is pushed and the existing one is unchanged.
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "2" },
                    "source": { "name": "fault_injector" },
                    "fault": {
                        "name": "clog",
                        "type": "network",
                        "affected_nodes": []
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":2.0},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"clog","type":"network","affected_nodes":[]},"#,
                    r#""active_faults":{"network_clog":{"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    #[test]
    fn missing_affected_nodes_does_not_open_network_window() {
        let mut transformer = FaultAnnotator::default();

        // Open a real window first
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 100
                }
            })
        ));

        // No affected_nodes field at all: same result — no new window
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "2" },
                    "source": { "name": "fault_injector" },
                    "fault": {
                        "name": "partition",
                        "type": "network"
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":2.0},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"partition","type":"network"},"#,
                    r#""active_faults":{"network_partition":{"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: untracked fault names produce no window
    // -----------------------------------------------------------------------

    #[test]
    fn untracked_fault_names_produce_empty_active_faults() {
        let mut transformer = FaultAnnotator::default();

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "1" },
                    "source": { "name": "fault_injector" },
                    "fault": { "name": "kill" }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":1.0},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"kill"},"#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "2" },
                    "source": { "name": "fault_injector" },
                    "fault": { "name": "stop" }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":2.0},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"stop"},"#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    #[test]
    fn restore_after_only_untracked_faults_is_noop() {
        let mut transformer = FaultAnnotator::default();

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": { "name": "kill" }
            })
        ));

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "2" },
                    "source": { "name": "fault_injector" },
                    "fault": {
                        "name": "restore",
                        "type": "network",
                        "affected_nodes": ["ALL"]
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":2.0},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"restore","type":"network","affected_nodes":["ALL"]},"#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: non-fault_injector sources do not open windows
    // -----------------------------------------------------------------------

    #[test]
    fn fault_fields_from_non_fault_injector_source_are_ignored() {
        let mut transformer = FaultAnnotator::default();

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "1" },
                    "source": { "name": "some_other_source" },
                    "fault": {
                        "name": "partition",
                        "type": "network",
                        "affected_nodes": ["ALL"]
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":1.0},"source":{"name":"some_other_source"},"#,
                    r#""fault":{"name":"partition","type":"network","affected_nodes":["ALL"]},"#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: event without a vtime still gets active_faults
    // (and gets no vtime field added)
    // -----------------------------------------------------------------------

    #[test]
    fn event_without_vtime_still_gets_active_faults() {
        let mut transformer = FaultAnnotator::default();

        // Open a partition window at a known vtime
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"]
                }
            })
        ));

        // Event with no moment at all: no expiry check, no vtime added, but active_faults injected
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({"output_text": "no moment here"}))),
            Some(
                concat!(
                    r#"{"output_text":"no moment here","#,
                    r#""active_faults":{"network_partition":{"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: natural expiration — boundary semantics
    //
    // is_expired uses strict less-than: end_vtime < latest_vtime.
    // So at exactly end_vtime the window is still active; it expires only when
    // the next message arrives with a strictly greater vtime.
    // -----------------------------------------------------------------------

    #[test]
    fn fault_window_active_at_exact_end_vtime_expires_just_past_end() {
        let mut transformer = FaultAnnotator::default();

        // partition at vtime 5, max_duration=5s → end_vtime = 5 + 5 = 10
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "5" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 5
                }
            })
        ));

        // At exactly end_vtime (10): window is still active (end < latest is false when equal)
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "10" },
                    "output_text": "at exact end"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":10.0},"output_text":"at exact end","#,
                    r#""active_faults":{"network_partition":{"vtime":5.0}}}"#
                )
                .to_string()
            )
        );

        // Just past end_vtime: now expired
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "10.5" },
                    "output_text": "just past end"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":10.5},"output_text":"just past end","#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // vtime precision through the NDJSON paths
    // -----------------------------------------------------------------------

    #[test]
    fn fault_annotator_emits_full_precision_vtime_byte_identically() {
        let mut transformer = FaultAnnotator::default();

        // The full-precision seconds string becomes a JSON number carrying
        // the exact value, byte-for-byte (see src/vtime.rs).
        assert_eq!(
            transformer
                .try_transform(r#"{"moment":{"vtime":"398.4898056755774"},"output_text":"hi"}"#),
            Some(
                concat!(
                    r#"{"moment":{"vtime":398.4898056755774},"output_text":"hi","#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    /// The copy-paste contract of the log vtime column, over the whole tick
    /// domain: the truncated text parses to a value at or before the real
    /// vtime — never past it — and less than one millisecond behind. The
    /// string and number wire forms render identically.
    #[hegel::test]
    fn truncated_log_vtime_never_lands_past_the_line(tc: hegel::TestCase) {
        let ticks = tc.draw(generators::integers::<u64>().max_value((1 << 53) - 1));
        let vtime = VTime::from_seconds(ticks as f64 / 4294967296.0).unwrap();

        let truncated: f64 = truncate_decimals(&vtime.to_string(), 3).parse().unwrap();
        assert!(truncated <= vtime.as_seconds());
        assert!(vtime.as_seconds() - truncated < 0.001);

        let from_string = format_log_vtime(&json!({"moment": {"vtime": vtime.to_string()}}));
        let from_number = format_log_vtime(&json!({"moment": {"vtime": vtime}}));
        assert_eq!(from_string, from_number);
    }

    /// The issue #191 guarantee over the whole tick domain: a vtime string
    /// the server sends survives the stream's classify -> normalize ->
    /// serialize pipeline byte-identically, as a JSON number.
    #[hegel::test]
    fn classify_line_preserves_any_real_vtime_byte_for_byte(tc: hegel::TestCase) {
        let ticks = tc.draw(generators::integers::<u64>().max_value((1 << 53) - 1));
        let vtime = VTime::from_seconds(ticks as f64 / 4294967296.0).unwrap();

        let line = format!(r#"{{"moment":{{"vtime":"{vtime}"}},"output_text":"x"}}"#);
        let NdjsonLine::Entry(entry) = classify_line(&line) else {
            panic!("an object line should classify as Entry");
        };
        assert_eq!(
            entry.to_string(),
            format!(r#"{{"moment":{{"vtime":{vtime}}},"output_text":"x"}}"#)
        );
    }

    /// classify_line never panics, whatever the server sends; anything that
    /// isn't a JSON object comes back Raw and untouched.
    #[hegel::test]
    fn classify_line_never_panics_and_returns_raw_for_non_objects(tc: hegel::TestCase) {
        let line = tc.draw(generators::text());
        match classify_line(&line) {
            NdjsonLine::Entry(entry) => assert!(entry.is_object()),
            NdjsonLine::Raw(raw) => assert_eq!(raw, line),
        }
    }

    #[test]
    fn normalize_vtime_field_converts_string_vtime_to_exact_number() {
        let mut entry = json!({"a": 1, "moment": {"vtime": "313.15126806590706"}, "z": 2});
        assert!(normalize_vtime_field(&mut entry, "moment").is_some());
        assert_eq!(
            entry.to_string(),
            r#"{"a":1,"moment":{"vtime":313.15126806590706},"z":2}"#
        );

        // No parseable vtime: the entry is untouched.
        let mut entry = json!({"moment": {"vtime": "n/a"}});
        assert!(normalize_vtime_field(&mut entry, "moment").is_none());
        assert_eq!(entry, json!({"moment": {"vtime": "n/a"}}));
    }

    // -----------------------------------------------------------------------
    // active_faults: partition without max_duration never expires naturally
    // -----------------------------------------------------------------------

    #[test]
    fn partition_without_max_duration_never_expires() {
        let mut transformer = FaultAnnotator::default();

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"]
                    // no max_duration → end_vtime = None → is_expired always false
                }
            })
        ));

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "1000" },
                    "output_text": "much later"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":1000.0},"output_text":"much later","#,
                    r#""active_faults":{"network_partition":{"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: restore before natural expiration clears network faults
    // -----------------------------------------------------------------------

    #[test]
    fn restore_before_natural_expiration_clears_network_faults() {
        let mut transformer = FaultAnnotator::default();

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 100
                }
            })
        ));

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "5" },
                    "source": { "name": "fault_injector" },
                    "fault": {
                        "name": "restore",
                        "type": "network",
                        "affected_nodes": ["ALL"]
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":5.0},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"restore","type":"network","affected_nodes":["ALL"]},"#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: partition and clog expire independently
    // -----------------------------------------------------------------------

    #[test]
    fn partition_and_clog_expire_independently() {
        let mut transformer = FaultAnnotator::default();

        // Partition at vtime 5, max_duration=20 → expires after 25
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "5" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 20
                }
            })
        ));

        // Clog at vtime 10, max_duration=3 → expires after 13
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "10" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "clog",
                    "type": "network",
                    "affected_nodes": ["A"],
                    "max_duration": 3
                }
            })
        ));

        // At vtime 14: clog's end_vtime (13) < 14, so it expires; partition still active
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "14" },
                    "output_text": "clog expired, partition still active"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":14.0},"#,
                    r#""output_text":"clog expired, partition still active","#,
                    r#""active_faults":{"network_partition":{"vtime":5.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: non-overlapping windows — first expires, new one
    // starts fresh with the new start_vtime
    // -----------------------------------------------------------------------

    #[test]
    fn non_overlapping_windows_start_fresh_after_expiry() {
        let mut transformer = FaultAnnotator::default();

        // First window: vtime 1, max_duration=3 → expires after 4
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 3
                }
            }))),
            Some(concat!(
                r#"{"moment":{"vtime":1.0},"source":{"name":"fault_injector"},"#,
                r#""fault":{"name":"partition","type":"network","affected_nodes":["ALL"],"max_duration":3},"#,
                r#""active_faults":{"network_partition":{"vtime":1.0}}}"#
            ).to_string())
        );

        // Second window at vtime 5, after the first has expired (5 > 4):
        // the old window is pruned before the new one is pushed, so the snapshot
        // reflects only the new window's start_vtime.
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "vtime": "5" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 3
                }
            }))),
            Some(concat!(
                r#"{"moment":{"vtime":5.0},"source":{"name":"fault_injector"},"#,
                r#""fault":{"name":"partition","type":"network","affected_nodes":["ALL"],"max_duration":3},"#,
                r#""active_faults":{"network_partition":{"vtime":5.0}}}"#
            ).to_string())
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: overlapping same-name windows — active_fault_dictionary
    // reports the earliest start_vtime among all live windows
    // -----------------------------------------------------------------------

    #[test]
    fn overlapping_windows_report_earliest_start_vtime() {
        let mut transformer = FaultAnnotator::default();

        // First partition at vtime 10, max_duration=5 → expires after 15
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "10" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 5
                }
            })
        ));

        // Second partition at vtime 14 (overlapping), max_duration=5 → expires after 19
        // Both windows are alive; active_fault_dictionary picks the min start_vtime (10)
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "vtime": "14" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 5
                }
            }))),
            Some(concat!(
                r#"{"moment":{"vtime":14.0},"source":{"name":"fault_injector"},"#,
                r#""fault":{"name":"partition","type":"network","affected_nodes":["ALL"],"max_duration":5},"#,
                r#""active_faults":{"network_partition":{"vtime":10.0}}}"#
            ).to_string())
        );

        // At vtime 16: first window expired (15 < 16), second still alive (19 not < 16)
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "16" },
                    "output_text": "after first window expired"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":16.0},"output_text":"after first window expired","#,
                    r#""active_faults":{"network_partition":{"vtime":10.0}}}"#
                )
                .to_string()
            )
        );

        // At vtime 20: second window also expired (19 < 20)
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "20" },
                    "output_text": "after both expired"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":20.0},"output_text":"after both expired","#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: pause preserves clock windows
    // -----------------------------------------------------------------------

    #[test]
    fn fault_injector_pause_preserves_clock_windows() {
        let mut transformer = FaultAnnotator::default();

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": { "offset": 10.0 }
                }
            })
        ));

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "2" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 100
                }
            })
        ));

        // Pause clears network and node windows; clock survives
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "source": { "name": "fault_injector" },
                    "info": {
                        "message": "status",
                        "details": { "paused": true }
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"source":{"name":"fault_injector"},"#,
                    r#""info":{"message":"status","details":{"paused":true}},"#,
                    r#""active_faults":{"clock_skip":{"cumulative_offset":10.0,"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: multiple node containers tracked simultaneously
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_containers_paused_simultaneously() {
        let mut transformer = FaultAnnotator::default();

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "pause",
                    "type": "node",
                    "affected_nodes": ["A"],
                    "max_duration": 100
                }
            })
        ));

        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "vtime": "2" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "pause",
                    "type": "node",
                    "affected_nodes": ["B"],
                    "max_duration": 100
                }
            }))),
            Some(concat!(
                r#"{"moment":{"vtime":2.0},"source":{"name":"fault_injector"},"#,
                r#""fault":{"name":"pause","type":"node","affected_nodes":["B"],"max_duration":100},"#,
                r#""active_faults":{"node_pause":{"A":1.0,"B":2.0}}}"#
            ).to_string())
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: node fault expires via max_duration
    // -----------------------------------------------------------------------

    #[test]
    fn node_fault_expires_via_max_duration() {
        let mut transformer = FaultAnnotator::default();

        // Throttle C at vtime 1, max_duration=5 → expires after 6
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "throttle",
                    "type": "node",
                    "affected_nodes": ["C"],
                    "max_duration": 5
                }
            })
        ));

        // Mid-window at vtime 3: still active
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "3.0" },
                    "output_text": "mid-window"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":3.0},"output_text":"mid-window","#,
                    r#""active_faults":{"node_throttle":{"C":1.0}}}"#
                )
                .to_string()
            )
        );

        // After expiry at vtime 7 (6 < 7): empty
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "7" },
                    "output_text": "after expiry"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":7.0},"output_text":"after expiry","#,
                    r#""active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    fn summary(
        run_id: &str,
        status: RunStatus,
        created: &str,
        launcher: &str,
        test_name: Option<&str>,
        description: Option<&str>,
    ) -> RunSummary {
        let parameters = if test_name.is_some() || description.is_some() {
            let mut extra = std::collections::HashMap::new();
            if let Some(name) = test_name {
                extra.insert("antithesis.test_name".to_string(), name.to_string());
            }
            Some(crate::api::RunParams {
                antithesis_config_image: None,
                antithesis_description: description.map(str::to_string),
                antithesis_duration: None,
                antithesis_images: None,
                antithesis_is_ephemeral: None,
                antithesis_report_recipients: None,
                antithesis_source: None,
                antithesis_filter_logs_matching: None,
                extra,
            })
        } else {
            None
        };
        RunSummary {
            run_id: run_id.to_string(),
            status,
            created_at: created.parse().unwrap(),
            started_at: None,
            completed_at: None,
            launcher: launcher.to_string(),
            creator: None,
            description: None,
            parameters,
            links: None,
        }
    }

    #[test]
    fn relative_time_is_compact() {
        let now = Utc::now();
        assert_eq!(relative_time(now - chrono::Duration::seconds(5)), "5s ago");
        assert_eq!(relative_time(now - chrono::Duration::minutes(3)), "3m ago");
        assert_eq!(relative_time(now - chrono::Duration::hours(21)), "21h ago");
        assert_eq!(relative_time(now - chrono::Duration::days(2)), "2d ago");
        assert_eq!(relative_time(now - chrono::Duration::days(8)), "1w ago");
        assert_eq!(relative_time(now - chrono::Duration::days(45)), "1mo ago");
        assert_eq!(relative_time(now - chrono::Duration::days(400)), "1y ago");
        // Clock skew: a slightly-future timestamp clamps instead of rendering
        // a negative age.
        assert_eq!(relative_time(now + chrono::Duration::hours(1)), "0s ago");
    }

    #[test]
    fn requested_duration_renders_via_human_duration() {
        // Whole minutes, the launcher's h/m vocabulary, and a fractional minute
        // count from older runs all render through HumanDuration.
        assert_eq!(format_requested_duration("30"), "30m");
        assert_eq!(format_requested_duration("90"), "1h30m");
        assert_eq!(format_requested_duration("15.5"), "15m30s");
        // A value we can't parse falls back to the raw string rather than
        // dropping the row entirely.
        assert_eq!(format_requested_duration("soon"), "soon");
    }

    #[test]
    fn elapsed_is_wall_clock_with_second_precision() {
        let started: DateTime<Utc> = "2025-03-20T02:01:12Z".parse().unwrap();
        let end: DateTime<Utc> = "2025-03-20T02:31:45Z".parse().unwrap();
        assert_eq!(
            elapsed_duration(started, end).unwrap().to_string(),
            "30m33s"
        );
        // Clock skew (end before start) yields no elapsed rather than a negative.
        assert!(elapsed_duration(end, started).is_none());
    }

    #[test]
    fn runs_table_default_view_omits_description_and_bounds_test_name() {
        // Use a fixed past time so relative_time produces a stable-ish value.
        let runs = vec![summary(
            "abc-54-1",
            RunStatus::Completed,
            "2024-01-01T00:00:00Z",
            "basic_test",
            Some("a-very-long-test-name-that-should-be-truncated-on-a-narrow-terminal"),
            Some("issue #91: probe Antithesis behavior with empty test template dir"),
        )];

        let width = 80;
        let table = render_runs_table(&runs, width);
        let lines: Vec<&str> = table.lines().collect();

        assert!(lines[0].contains("RUN ID"));
        assert!(lines[0].contains("STATUS"));
        assert!(lines[0].contains("CREATED"));
        assert!(lines[0].contains("TEST NAME"));
        // The default view no longer shows DESCRIPTION (use `--detail` for that).
        assert!(!lines[0].contains("DESCRIPTION"));
        assert!(!lines[0].contains("LAUNCHER"));

        assert!(lines[1].contains("abc-54-1"));
        assert!(lines[1].contains("completed"));
        // Test name is the final column, truncated with an ellipsis to fit, and
        // every line stays within the terminal width.
        assert!(lines[1].contains('…'));
        for line in &lines {
            assert!(
                line.chars().count() <= width,
                "line exceeds width {width}: {line}"
            );
        }
    }

    #[test]
    fn runs_long_view_renders_aligned_key_value_block() {
        let runs = vec![
            summary(
                "abc-54-1",
                RunStatus::Completed,
                "2024-01-01T00:00:00Z",
                "basic_test",
                Some("snouty-empty-tt"),
                Some("full description goes here"),
            ),
            summary(
                "def-54-2",
                RunStatus::Incomplete,
                "2024-01-02T00:00:00Z",
                "spanner",
                None,
                None,
            ),
        ];

        let out = render_runs_detail(&runs);
        // No table header — each field sits on its own aligned line. Labels are
        // padded to the widest label *within each block*, so the first block
        // (which has a "Description" label) is wider than the second.
        assert!(!out.contains("RUN ID  "));
        assert!(out.contains("Run ID       abc-54-1"));
        assert!(out.contains("Status       completed"));
        assert!(out.contains("Test Name    snouty-empty-tt"));
        assert!(out.contains("Description  full description goes here"));
        // Second run omits the empty Test Name / Description fields.
        assert!(out.contains("def-54-2"));
        assert!(out.contains("incomplete"));
        // A blank line separates the two run blocks.
        assert!(out.contains("\n\n"));
    }

    #[test]
    fn runs_table_does_not_truncate_test_name_when_piped() {
        // `terminal_width()` returns usize::MAX for a non-tty so piped output
        // keeps the full TEST NAME — `snouty runs | grep` must not miss a row.
        let long = "a-very-long-test-name-that-would-be-truncated-on-a-narrow-terminal";
        let runs = vec![summary(
            "abc-54-1",
            RunStatus::Completed,
            "2024-01-01T00:00:00Z",
            "basic_test",
            Some(long),
            None,
        )];
        let table = render_runs_table(&runs, usize::MAX);
        assert!(table.contains(long), "name was truncated: {table}");
        assert!(!table.contains('…'));
    }

    #[test]
    fn wrap_last_does_not_wrap_when_piped() {
        // With usize::MAX width the final column is emitted on a single line
        // (no wrap), so a long NAME survives intact for piping/grep.
        let long = "Safety ▸ a property name long enough to wrap on any real terminal width";
        let headers = vec!["STATUS".to_string(), "NAME".to_string()];
        let rows = vec![vec!["failing".to_string(), long.to_string()]];
        let out = render_table_wrap_last(&headers, &rows, usize::MAX, &[Align::Left, Align::Left]);
        let row = out.lines().find(|l| l.contains("failing")).unwrap();
        assert!(row.contains(long), "name was wrapped: {out}");
    }

    #[test]
    fn runs_table_renders_dashes_when_test_name_and_description_missing() {
        let runs = vec![summary(
            "abc-54-1",
            RunStatus::Incomplete,
            "2024-01-01T00:00:00Z",
            "basic_test",
            None,
            None,
        )];
        let table = render_runs_table(&runs, 100);
        let lines: Vec<&str> = table.lines().collect();
        assert!(lines[1].contains("incomplete"));
        // A placeholder dash stands in for the missing test name (final column).
        assert!(
            lines[1].trim_end().ends_with('-'),
            "expected dash placeholder, got: {}",
            lines[1]
        );
    }

    mod run_scoped_errors {
        use super::*;
        use crate::auth::AuthenticationInfo;
        use crate::error::ApiError;
        use crate::settings::Settings;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn api_error(status: u16, message: &str) -> color_eyre::eyre::Report {
            color_eyre::eyre::Report::new(ApiError {
                status,
                message: message.to_string(),
            })
        }

        fn test_api(base_url: &str) -> AntithesisApi {
            AntithesisApi::build(
                &Settings::for_test_base_url(base_url.to_owned()),
                AuthenticationInfo::Password {
                    username: "user".to_string(),
                    password: "pass".to_string(),
                },
                false,
                crate::api::ResponseCache::Disabled,
            )
            .unwrap()
        }

        async fn mock_get_run(run_id: &str, status: u16, body: serde_json::Value) -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(format!("/api/v0/runs/{run_id}")))
                .respond_with(ResponseTemplate::new(status).set_body_json(body))
                .mount(&server)
                .await;
            server
        }

        #[test]
        fn run_not_found_rewrites_only_404() {
            let rewritten = explain_run_not_found("BAD-ID", api_error(404, "API error: 404"));
            let msg = format!("{rewritten:#}");
            assert_eq!(msg, "run not found: BAD-ID");
        }

        #[test]
        fn run_not_found_passes_through_non_404() {
            // A 500 whose body mentions 404 must NOT be rewritten — it's a real
            // server fault, not a missing run.
            let original = api_error(500, "API error: 500 — upstream 404 page");
            let result = explain_run_not_found("run-1", original);
            assert_eq!(api_error_status(&result), Some(500));
            assert!(format!("{result:#}").contains("upstream 404 page"));
        }

        #[tokio::test]
        async fn scoped_error_reports_missing_run_on_404_probe() {
            let server = mock_get_run("BAD-ID", 404, json!({"message": "nope"})).await;
            let api = test_api(&server.uri());
            let result =
                explain_run_scoped_error(&api, "BAD-ID", api_error(404, "API error: 404")).await;
            assert_eq!(format!("{result:#}"), "run not found: BAD-ID");
        }

        #[tokio::test]
        async fn scoped_error_keeps_original_when_run_exists() {
            // Endpoint 404'd but the run is real — surface the original error
            // rather than claiming the run is missing.
            let server = mock_get_run(
                "run-1",
                200,
                json!({
                    "run_id": "run-1",
                    "status": "in_progress",
                    "created_at": "2025-03-20T02:00:00Z",
                    "launcher": "nightly"
                }),
            )
            .await;
            let api = test_api(&server.uri());
            let result =
                explain_run_scoped_error(&api, "run-1", api_error(404, "endpoint 404")).await;
            assert!(format!("{result:#}").contains("endpoint 404"));
            assert!(!format!("{result:#}").contains("run not found"));
        }

        // The logs endpoint's 404-with-existing-run means "bad moment", the
        // one case where snouty can name the next command.
        #[tokio::test]
        async fn logs_error_suggests_listing_moments_when_the_run_exists() {
            let server = mock_get_run(
                "run-1",
                200,
                json!({
                    "run_id": "run-1",
                    "status": "completed",
                    "created_at": "2025-03-20T02:00:00Z",
                    "launcher": "nightly"
                }),
            )
            .await;
            let api = test_api(&server.uri());
            let result = explain_logs_error(&api, "run-1", api_error(404, "API error: 404")).await;
            let debug = format!("{result:?}");
            assert!(
                debug.contains("snouty runs events run-1"),
                "expected the moment-listing suggestion, got: {debug}"
            );
        }

        // A missing run keeps the plain "run not found" with no moment talk.
        #[tokio::test]
        async fn logs_error_reports_a_missing_run_without_the_suggestion() {
            let server = mock_get_run("BAD-ID", 404, json!({"message": "nope"})).await;
            let api = test_api(&server.uri());
            let result = explain_logs_error(&api, "BAD-ID", api_error(404, "API error: 404")).await;
            assert_eq!(format!("{result:#}"), "run not found: BAD-ID");
            assert!(!format!("{result:?}").contains("runs events"));
        }

        #[tokio::test]
        async fn scoped_error_propagates_non_404_probe_failure() {
            // get_run fails with a 502 (not a 404): the run-scoped error must NOT
            // be misreported as "run not found"; the probe's own error wins.
            let server = mock_get_run("run-1", 502, json!({"message": "bad gateway"})).await;
            let api = test_api(&server.uri());
            let result =
                explain_run_scoped_error(&api, "run-1", api_error(404, "endpoint 404")).await;
            assert!(!format!("{result:#}").contains("run not found"));
            assert_eq!(api_error_status(&result), Some(502));
        }

        #[tokio::test]
        async fn scoped_error_passes_through_non_404_without_probing() {
            // A 500 from the endpoint never probes get_run and is surfaced as-is.
            let server = MockServer::start().await;
            // No mock mounted: a probe would 404 and wrongly say "run not found".
            let api = test_api(&server.uri());
            let result = explain_run_scoped_error(&api, "run-1", api_error(500, "boom 404")).await;
            assert_eq!(api_error_status(&result), Some(500));
            assert!(!format!("{result:#}").contains("run not found"));
        }

        #[tokio::test]
        async fn properties_error_reports_missing_run_on_404_probe() {
            let server = mock_get_run("BAD-ID", 404, json!({"message": "nope"})).await;
            let api = test_api(&server.uri());
            let result =
                explain_properties_error(&api, "BAD-ID", api_error(404, "API error: 404")).await;
            assert_eq!(format!("{result:#}"), "run not found: BAD-ID");
        }

        #[tokio::test]
        async fn properties_error_explains_incomplete_run() {
            let server = mock_get_run(
                "run-3",
                200,
                json!({
                    "run_id": "run-3",
                    "status": "incomplete",
                    "created_at": "2025-03-18T08:00:00Z",
                    "launcher": "nightly"
                }),
            )
            .await;
            let api = test_api(&server.uri());
            let result =
                explain_properties_error(&api, "run-3", api_error(404, "API error: 404")).await;
            // The message is a clean statement of the error …
            let msg = format!("{result:#}");
            assert_eq!(msg, "no properties for run run-3", "got: {msg}");
            // … while the *why* and the next step ride along as notes (rendered by
            // the full report, not the message chain).
            let report = format!("{result:?}");
            assert!(report.contains("this run is incomplete"), "got: {report}");
            assert!(report.contains("snouty runs show run-3"), "got: {report}");
        }

        #[tokio::test]
        async fn properties_error_propagates_non_404_probe_failure() {
            let server = mock_get_run("run-1", 502, json!({"message": "bad gateway"})).await;
            let api = test_api(&server.uri());
            let result =
                explain_properties_error(&api, "run-1", api_error(404, "API error: 404")).await;
            assert!(!format!("{result:#}").contains("run not found"));
            assert_eq!(api_error_status(&result), Some(502));
        }

        #[tokio::test]
        async fn properties_error_passes_through_500_with_404_in_body() {
            let server = MockServer::start().await;
            let api = test_api(&server.uri());
            let result = explain_properties_error(
                &api,
                "run-1",
                api_error(500, "API error: 500 — proxy 404 page"),
            )
            .await;
            assert_eq!(api_error_status(&result), Some(500));
            assert!(!format!("{result:#}").contains("run not found"));
            assert!(!format!("{result:#}").contains("no properties for run"));
        }
    }

    /// `runs wait` status transitions: each test mounts a sequence of
    /// `get_run` responses so the run's status changes between polls. Sub-30s
    /// intervals are fine here: the CLI enforces the floor, `wait_for_run`
    /// takes any `Duration`.
    mod wait {
        use super::*;
        use crate::auth::AuthenticationInfo;
        use crate::error::api_error_status;
        use crate::settings::Settings;
        use std::time::Duration;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn test_api(base_url: &str) -> AntithesisApi {
            AntithesisApi::build(
                &Settings::for_test_base_url(base_url.to_owned()),
                AuthenticationInfo::ApiKey {
                    api_key: "test-key".to_owned(),
                },
                false,
                crate::api::ResponseCache::Disabled,
            )
            .unwrap()
        }

        fn run_body(status: &str) -> serde_json::Value {
            json!({
                "run_id": "run-w",
                "status": status,
                "created_at": "2025-03-20T02:00:00Z",
                "launcher": "nightly"
            })
        }

        /// Serve each status in order, the last one indefinitely: wiremock
        /// matches in mount order and skips a mock whose `up_to_n_times`
        /// budget is spent.
        async fn mount_get_run_statuses(server: &MockServer, statuses: &[&str]) {
            for (i, status) in statuses.iter().enumerate() {
                let mock = Mock::given(method("GET"))
                    .and(path("/api/v0/runs/run-w"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(run_body(status)));
                let mock = if i + 1 < statuses.len() {
                    mock.up_to_n_times(1)
                } else {
                    mock
                };
                mock.mount(server).await;
            }
        }

        /// Sub-minute polling is fine below the CLI: the 1-minute floor is
        /// clap's, `wait_for_run` takes any interval.
        const FAST_POLL: Duration = Duration::from_millis(10);

        #[tokio::test]
        async fn wait_polls_until_the_run_completes() {
            let server = MockServer::start().await;
            mount_get_run_statuses(&server, &["starting", "in_progress", "completed"]).await;
            let api = test_api(&server.uri());

            let run = wait_for_run(&api, "run-w", FAST_POLL).await.unwrap();

            assert_eq!(run.status, RunStatus::Completed);
            // One poll per status: the loop stopped at the first terminal one.
            let requests = server.received_requests().await.unwrap();
            assert_eq!(requests.len(), 3);
        }

        #[tokio::test]
        async fn wait_treats_every_terminal_state_as_success() {
            for status in ["completed", "cancelled", "incomplete"] {
                let server = MockServer::start().await;
                mount_get_run_statuses(&server, &["in_progress", status]).await;
                let api = test_api(&server.uri());

                let run = wait_for_run(&api, "run-w", FAST_POLL).await.unwrap();
                assert_eq!(run.status.to_string(), status);
            }
        }

        #[tokio::test]
        async fn wait_fails_when_the_run_turns_unknown() {
            let server = MockServer::start().await;
            mount_get_run_statuses(&server, &["in_progress", "unknown"]).await;
            let api = test_api(&server.uri());

            let err = wait_for_run(&api, "run-w", FAST_POLL).await.unwrap_err();

            let msg = format!("{err:#}");
            assert_eq!(msg, "run run-w reported status \"unknown\"", "got: {msg}");
            // The next steps ride along as notes on the full report.
            let report = format!("{err:?}");
            assert!(report.contains("snouty runs show run-w"), "got: {report}");
            assert!(!report.contains("snouty runs wait"), "got: {report}");
        }

        #[tokio::test]
        async fn wait_never_returns_while_the_run_is_in_progress() {
            let server = MockServer::start().await;
            mount_get_run_statuses(&server, &["in_progress"]).await;
            let api = test_api(&server.uri());

            // cmd_runs_wait bounds the wait exactly like this.
            let wait = wait_for_run(&api, "run-w", Duration::from_secs(3600));
            tokio::time::timeout(Duration::from_secs(1), wait)
                .await
                .expect_err("a non-terminal run must keep the wait pending");

            // One immediate poll, then a one-hour tick the timeout cuts short.
            let requests = server.received_requests().await.unwrap();
            assert_eq!(requests.len(), 1);
        }

        #[tokio::test]
        async fn wait_timeout_cuts_off_a_hung_request() {
            // The HTTP client has no read timeout, so only the wrapping
            // tokio::time::timeout keeps a never-answering server from
            // hanging the command forever.
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/api/v0/runs/run-w"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(run_body("in_progress"))
                        .set_delay(Duration::from_secs(3600)),
                )
                .mount(&server)
                .await;
            let api = test_api(&server.uri());

            let wait = wait_for_run(&api, "run-w", FAST_POLL);
            tokio::time::timeout(Duration::from_secs(1), wait)
                .await
                .expect_err("a hung request must not outlive the timeout");
        }

        #[tokio::test]
        async fn wait_reports_a_missing_run_immediately() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/api/v0/runs/BAD-ID"))
                .respond_with(ResponseTemplate::new(404).set_body_json(json!({"message": "nope"})))
                .mount(&server)
                .await;
            let api = test_api(&server.uri());

            let err = wait_for_run(&api, "BAD-ID", FAST_POLL).await.unwrap_err();
            assert_eq!(format!("{err:#}"), "run not found: BAD-ID");
        }

        #[tokio::test]
        async fn wait_passes_through_a_server_fault() {
            // Unlike a 404, a 500 must not be reshaped into "run not found".
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/api/v0/runs/run-w"))
                .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
                .mount(&server)
                .await;
            let api = test_api(&server.uri());

            let err = wait_for_run(&api, "run-w", FAST_POLL).await.unwrap_err();
            assert_eq!(api_error_status(&err), Some(500));
        }

        const STARTED_AT: &str = "2025-03-20T02:00:00Z";

        /// A still-running `RunDetail`, started at [`STARTED_AT`], with the
        /// given `antithesis.duration` (minutes, or absent).
        fn active_run(duration_minutes: Option<&str>) -> RunDetail {
            let mut parameters = serde_json::Map::new();
            if let Some(d) = duration_minutes {
                parameters.insert("antithesis.duration".into(), json!(d));
            }
            serde_json::from_value(json!({
                "run_id": "run-w",
                "status": "in_progress",
                "created_at": STARTED_AT,
                "started_at": STARTED_AT,
                "launcher": "nightly",
                "parameters": parameters
            }))
            .unwrap()
        }

        fn minutes_after_start(minutes: i64) -> DateTime<Utc> {
            STARTED_AT.parse::<DateTime<Utc>>().unwrap() + chrono::Duration::minutes(minutes)
        }

        #[test]
        fn over_schedule_warns_past_twice_the_scheduled_duration() {
            let run = active_run(Some("30"));
            // Strictly past twice the schedule: exactly 2x does not warn.
            assert_eq!(over_schedule_warning(&run, minutes_after_start(60)), None);
            let warning = over_schedule_warning(&run, minutes_after_start(61)).unwrap();
            assert!(warning.contains("run run-w"), "got: {warning}");
            assert!(warning.contains("1h1m"), "got: {warning}");
            assert!(warning.contains("30m"), "got: {warning}");
        }

        #[test]
        fn over_schedule_needs_a_usable_schedule_and_a_start() {
            let far_out = minutes_after_start(1000);
            assert_eq!(over_schedule_warning(&active_run(None), far_out), None);
            assert_eq!(
                over_schedule_warning(&active_run(Some("bogus")), far_out),
                None
            );
            assert_eq!(over_schedule_warning(&active_run(Some("0")), far_out), None);

            // A run that hasn't started has no elapsed time to compare.
            let unstarted: RunDetail = serde_json::from_value(json!({
                "run_id": "run-w",
                "status": "starting",
                "created_at": STARTED_AT,
                "launcher": "nightly",
                "parameters": {"antithesis.duration": "30"}
            }))
            .unwrap();
            assert_eq!(over_schedule_warning(&unstarted, far_out), None);
        }
    }
}
