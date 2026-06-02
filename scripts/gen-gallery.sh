#!/usr/bin/env bash
#
# gen-gallery.sh — regenerate the snouty "gallery" against fresh, live run IDs.
#
# The committed ./gallery captures example output for many `snouty runs ...`
# commands, but it is pinned to specific (now-aging) run IDs. This script
# rebuilds an equivalent gallery from scratch every time it runs:
#
#   1. Uses whatever ANTITHESIS_* env vars you already have exported to query
#      the live API and discover fresh runs — at least one completed run and
#      one incomplete (failing) run, plus a cancelled run if one exists.
#   2. Derives the per-run details the commands need (a real event moment,
#      failing/passing/non-event property names, the incomplete run's failure
#      moment, etc.) so every story actually returns data.
#   3. Runs each "story" and writes its output to its own .md file in a fresh
#      temp directory, in the same format as ./gallery.
#   4. Prints the temp directory at the end.
#
# Nothing is written to ./gallery — output goes to a tempdir so you never
# accidentally commit it. Diff successive runs (or diff against ./gallery) to
# see how your snouty changes affect command output.
#
# Adding a new story: add one `story` (or `story_opt`) line in the STORIES
# section near the bottom. That's the only place you should need to touch.
#
# Usage:
#   scripts/gen-gallery.sh
#
# Env knobs:
#   SNOUTY=/path/to/snouty   use this binary instead of building target/debug
#   GALLERY_OUT=/some/dir    write here instead of a fresh mktemp dir

set -euo pipefail

# ----------------------------------------------------------------------------
# Setup: locate repo, resolve the snouty binary, neutralize `runs open`.
# ----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

for tool in jq awk; do
    command -v "$tool" >/dev/null || { echo "error: '$tool' is required" >&2; exit 1; }
done

if [[ -n "${SNOUTY:-}" ]]; then
    SNOUTY="$(command -v "$SNOUTY" || echo "$SNOUTY")"
else
    echo "building snouty (target/debug)…" >&2
    if ! cargo build -q; then
        echo "error: cargo build failed — fix the build, or pass SNOUTY=path/to/snouty" >&2
        exit 1
    fi
    SNOUTY="$REPO_ROOT/target/debug/snouty"
fi
echo "using binary: $SNOUTY" >&2

# `snouty runs open` shells out to xdg-open / open. Drop a no-op shim on PATH
# (and point $BROWSER at it) so regenerating the gallery never spawns a browser.
SHIM_DIR="$(mktemp -d)"
trap 'rm -rf "$SHIM_DIR"' EXIT
for prog in xdg-open open; do
    printf '#!/bin/sh\nexit 0\n' >"$SHIM_DIR/$prog"
    chmod +x "$SHIM_DIR/$prog"
done
export PATH="$SHIM_DIR:$PATH"
export BROWSER="$SHIM_DIR/xdg-open"

OUTDIR="${GALLERY_OUT:-$(mktemp -d -t snouty-gallery.XXXXXX)}"
mkdir -p "$OUTDIR"

# Run snouty, capturing stdout+stderr (verbose logging and error messages are
# part of what we want to showcase). Never abort on a non-zero exit — several
# stories deliberately exercise error paths.
sn() {
    set +e
    "$SNOUTY" "$@" 2>&1
    local rc=$?
    set -e
    return 0
}

# First NDJSON line of a `--json` command, or empty.
json_first() { "$SNOUTY" --json "$@" 2>/dev/null | head -1; }

# Probe a run's events endpoint for a usable event.
#
#   probe_events <run_id>
#
# Tries a series of common substrings against `runs events` and, on the first
# hit, sets the globals EVENT_JSON (the raw NDJSON line) and EVENT_KEYWORD (the
# substring that matched). Crucially, it distinguishes the three outcomes the
# old discovery code silently collapsed into "no events":
#
#   return 0  a real event was sampled         (EVENT_JSON/EVENT_KEYWORD set)
#   return 1  endpoint reachable but no match   (run genuinely has no such event)
#   return 2  endpoint errored / timed out      (transport failure — try another run)
#
# A timeout affects every keyword equally, so we bail on the first transport
# error instead of burning the 30s client timeout once per keyword.
probe_events() {
    local run="$1" kw out err
    EVENT_JSON=""; EVENT_KEYWORD=""
    err="$(mktemp)"
    for kw in error test client info setup start Container the; do
        out="$("$SNOUTY" --json runs events "$run" --match "$kw" 2>"$err" | head -1)"
        if grep -qiE 'failed to contact API|operation timed out|error sending request' "$err"; then
            rm -f "$err"
            return 2
        fi
        if [[ -n "$out" ]]; then
            EVENT_JSON="$out"; EVENT_KEYWORD="$kw"
            rm -f "$err"
            return 0
        fi
    done
    rm -f "$err"
    return 1
}

# ----------------------------------------------------------------------------
# Story emitter.
#
#   story <slug> <title> <snouty args...>
#
# Writes <slug>.md containing the title as a leading comment and a ```shell
# block with the command line and its output — matching ./gallery's format.
# ----------------------------------------------------------------------------

story() {
    local slug="$1" title="$2"
    shift 2
    local file="$OUTDIR/$slug.md"
    {
        printf '# %s\n\n' "$title"
        printf '```shell\n'
        printf '$ snouty %s\n' "$*"
        sn "$@"
        printf '```\n'
    } >"$file"
    printf '  %-32s snouty %s\n' "$slug.md" "$*" >&2
}

# Like `story`, but skips (with a warning) if any argument is empty — used for
# stories that depend on a derived value that might not exist for every run.
story_opt() {
    local slug="$1"
    local arg
    for arg in "${@:3}"; do
        if [[ -z "$arg" ]]; then
            printf '  %-32s SKIPPED (missing derived value)\n' "$slug.md" >&2
            return 0
        fi
    done
    story "$@"
}

# ----------------------------------------------------------------------------
# Discovery: find fresh runs and derive the values the stories reference.
# ----------------------------------------------------------------------------

echo "discovering runs via the live API…" >&2

FAIL="$(json_first runs list --status incomplete -n 1 | jq -r '.run_id // empty')"
CANCELLED="$(json_first runs list --status cancelled -n 1 | jq -r '.run_id // empty')"
# A syntactically-valid but nonexistent run id, for clean-error stories.
UNKNOWN="ffffffffffffffffffffffffffffffff-54-5"

# Derivations below are best-effort: a missing value just means the dependent
# story is skipped (via story_opt), so don't let errexit abort here. In
# particular, `… | head -1` truncation makes snouty exit via SIGPIPE, which
# would otherwise trip `set -e`.
set +e

# --- Pick the completed run that drives most stories ------------------------
# It is not enough to grab the *first* completed run: a given run's events
# endpoint can time out (the 30s client timeout) or simply have nothing to
# sample, which would silently skip every event/logs story. Instead, walk the
# recent completed runs and select the first whose events endpoint actually
# returns data — that run then drives the event, logs, and property stories.
mapfile -t COMPLETED < <(
    "$SNOUTY" --json runs list --status completed -n 15 2>/dev/null \
        | jq -r '.run_id // empty'
)
[[ ${#COMPLETED[@]} -gt 0 ]] || { echo "error: no completed run found" >&2; exit 1; }

SUCCESS=""
for cand in "${COMPLETED[@]}"; do
    probe_events "$cand"
    case $? in
        0) SUCCESS="$cand"
           echo "  completed run : $cand (events matched '$EVENT_KEYWORD')" >&2
           break ;;
        2) echo "  skipping $cand — events endpoint errored/timed out" >&2 ;;
        1) echo "  skipping $cand — no events matched probe keywords" >&2 ;;
    esac
done

# Refuse to emit a partial gallery: if no completed run yields events, the
# event/logs stories cannot be generated and silently dropping them is exactly
# the failure mode this script is meant to avoid.
if [[ -z "$SUCCESS" ]]; then
    echo "error: none of the ${#COMPLETED[@]} most recent completed runs returned events" >&2
    echo "       (all timed out or had no sampleable events) — refusing to write a" >&2
    echo "       gallery with the event/logs stories skipped. Investigate the events" >&2
    echo "       endpoint, or widen the completed-run search above." >&2
    exit 1
fi

echo "  incomplete run: ${FAIL:-<none>}" >&2
echo "  cancelled run : ${CANCELLED:-<none>}" >&2
[[ -n "$FAIL" ]] || echo "warning: no incomplete run found — incomplete stories will be skipped" >&2

# --- Derive event details from the sampled event ----------------------------
# probe_events guaranteed EVENT_JSON/EVENT_KEYWORD are set for $SUCCESS.
EVENT_HASH="$(jq -r '.moment.input_hash // empty' <<<"$EVENT_JSON")"
EVENT_VTIME="$(jq -r '.moment.vtime // empty' <<<"$EVENT_JSON")"
# Events carry a `source.name` and may also carry a `source.container`; the
# --source filter matches either, so fall back to name when container is absent.
EVENT_SOURCE="$(jq -r '.source.container // .source.name // empty' <<<"$EVENT_JSON")"
EVENT_STREAM="$(jq -r '.source.stream // empty' <<<"$EVENT_JSON")"
# A vtime window straddling the sampled event, so the window story matches it.
EVENT_VMIN="$(awk "BEGIN{printf \"%.3f\", $EVENT_VTIME - 0.5}")"
EVENT_VMAX="$(awk "BEGIN{printf \"%.3f\", $EVENT_VTIME + 0.5}")"
# A second needle that co-occurs in the same event (for the multi-match story).
# Filtering is done against the whole NDJSON line, so prefer a content token
# from output_text but fall back to any other token on the line — that keeps
# the multi-match story populated even for terse events.
EVENT_KW2="$(jq -r '.output_text // ""' <<<"$EVENT_JSON" \
    | grep -oE '[A-Za-z_]{4,}' | grep -vixF "$EVENT_KEYWORD" | head -1)"
[[ -n "$EVENT_KW2" ]] || EVENT_KW2="$(grep -oE '[A-Za-z_]{4,}' <<<"$EVENT_JSON" \
    | grep -vixF "$EVENT_KEYWORD" | head -1)"

# --- Pull property metadata from the completed run --------------------------
PROPS_JSON="$(mktemp)"
"$SNOUTY" --json runs properties "$SUCCESS" >"$PROPS_JSON" 2>/dev/null || true

FAIL_PROP="$(jq -rs 'map(select(.status=="Failing")) | .[0].name // empty' "$PROPS_JSON")"
PASS_PROP="$(jq -rs 'map(select(.status=="Passing")) | .[0].name // empty' "$PROPS_JSON")"
NONEVENT_PROP="$(jq -rs 'map(select(.is_event==false)) | .[0].name // empty' "$PROPS_JSON")"
PASS_EVENT_PROP="$(jq -rs 'map(select(.is_event==true and .status=="Passing")) | .[0].name // empty' "$PROPS_JSON")"

# Surface, loudly, any property category the chosen run lacks. These stories
# are genuinely run-dependent (a run with no failing property can't show one),
# so we warn rather than abort — but never let a skip pass silently.
[[ -n "$FAIL_PROP" ]]       || echo "warning: $SUCCESS has no failing property — runs-property-failing will be skipped" >&2
[[ -n "$PASS_EVENT_PROP" ]] || echo "warning: $SUCCESS has no passing event property — runs-property-passing will be skipped" >&2
[[ -n "$NONEVENT_PROP" ]]   || echo "warning: $SUCCESS has no non-event property — runs-property-non-event will be skipped" >&2

# A fuzzy substring: the first word of a known property name.
FUZZY="$(awk '{print $1; exit}' <<<"${FAIL_PROP:-$PASS_PROP}")"
# An ambiguous substring: a >4-char token shared across multiple property names.
AMBIG="$(jq -rs '
    [ .[] | (.name | ascii_downcase) as $n
      | [ $n | splits("[^a-z0-9]+") | select(length > 4) ] | unique | .[] ]
    | group_by(.) | map(select(length > 1)) | max_by(length) | .[0] // empty
' "$PROPS_JSON")"
rm -f "$PROPS_JSON"

# --- Incomplete run's failure moment ----------------------------------------
FAIL_HASH=""; FAIL_VTIME=""
if [[ -n "$FAIL" ]]; then
    FAIL_SHOW="$("$SNOUTY" --json runs show "$FAIL" 2>/dev/null || true)"
    FAIL_HASH="$(jq -r '.failure_moment.input_hash // empty' <<<"$FAIL_SHOW")"
    FAIL_VTIME="$(jq -r '.failure_moment.vtime // empty' <<<"$FAIL_SHOW")"
fi

set -e  # end of best-effort derivations

# ============================================================================
# STORIES — add new commands here. Each line produces one <slug>.md file.
#   story      <slug> <title> <snouty args...>            (always runs)
#   story_opt  <slug> <title> <snouty args...>            (skips if an arg is empty)
# ============================================================================

# --- listing ---------------------------------------------------------------
story runs                       "Quickly check what test runs are around (bare 'snouty runs' is the same as 'list')" runs
story runs-list                  "List recent runs to find one to inspect" runs list -n 10
story runs-list--limit           "Show me just the last three runs" runs list -n 3
story runs-list--long            "Get the full descriptions instead of truncated titles" runs list -n 6 --long
story runs-list--status-completed   "Only show runs that finished cleanly" runs list -n 8 --status completed
story runs-list--status-incomplete  "Find recent failures to triage" runs list -n 8 --status incomplete
story runs-list--launcher        "Show only spanner-launched runs" runs list -n 8 --launcher spanner
story runs-list--created-after   "What runs have we kicked off recently?" runs list --created-after 2026-05-25T00:00:00Z
story runs-list--created-window  "Look at runs from a specific two-day window" runs list --created-after 2026-05-20T00:00:00Z --created-before 2026-05-22T00:00:00Z
story runs-verbose               "Get the API calls printed to stderr while you list runs" runs list -n 3 --verbose

# --- single-run metadata ----------------------------------------------------
story     runs-show             "Peek at the metadata for a completed run" runs show "$SUCCESS"
story     runs-open             "Jump straight to the triage report in the browser" runs open "$SUCCESS"
story_opt runs-show-incomplete  "Inspect a run that aborted early — note the failure vtime/hash" runs show "$FAIL"
story_opt runs-show-cancelled   "What does a cancelled run look like?" runs show "$CANCELLED"

# --- properties -------------------------------------------------------------
story     runs-properties             "See all properties — pass and fail — for a completed run" runs properties "$SUCCESS"
story     runs-properties--passing    "List only the green properties" runs properties "$SUCCESS" --passing
story     runs-properties--failing    "Focus on the properties that broke" runs properties "$SUCCESS" --failing
story_opt runs-properties-incomplete  "Trying to view properties for a run that never finished — no report available" runs properties "$FAIL"

story_opt runs-property-failing           "Drill into a failing property's counter-examples" runs property "$SUCCESS" "$FAIL_PROP"
story_opt runs-property-passing           "Look at the examples behind a passing property" runs property "$SUCCESS" "$PASS_EVENT_PROP"
story_opt runs-property-non-event         "View a non-event property — just a single value with no moments" runs property "$SUCCESS" "$NONEVENT_PROP"
story_opt runs-property-fuzzy             "Substring match — let snouty figure out which property you meant" runs property "$SUCCESS" "$FUZZY"
story_opt runs-property-ambiguous         "Substring matches multiple properties — see which one snouty picks (or refuses)" runs property "$SUCCESS" "$AMBIG"
story     runs-property-not-found         "Typo'd a property name — get a clean error" runs property "$SUCCESS" "this property does not exist"

# --- events -----------------------------------------------------------------
story     runs-events-single        "Find events that mention a particular keyword" runs events "$SUCCESS" --match "$EVENT_KEYWORD"
story     runs-events-source        "Restrict events to those from a specific container" runs events "$SUCCESS" --match "$EVENT_KEYWORD" --source "$EVENT_SOURCE"
story     runs-events-stream        "Filter events to a specific stream (info/error/stdout/stderr)" runs events "$SUCCESS" --match "$EVENT_KEYWORD" --stream "$EVENT_STREAM"
story     runs-events-vtime-window  "Restrict events to a virtual-time window" runs events "$SUCCESS" --match "$EVENT_KEYWORD" --vtime-min "$EVENT_VMIN" --vtime-max "$EVENT_VMAX"
story     runs-events-multi-match   "AND-narrow with two --match needles (both must appear in the event)" runs events "$SUCCESS" --match "$EVENT_KEYWORD" --match "$EVENT_KW2"
story     runs-events-combined      "Combine multiple filters — match, source, stream, and vtime range" runs events "$SUCCESS" --match "$EVENT_KEYWORD" --source "$EVENT_SOURCE" --stream "$EVENT_STREAM" --vtime-min "$EVENT_VMIN" --vtime-max "$EVENT_VMAX"
story     runs-events-no-results    "Search events that don't match anything" runs events "$SUCCESS" --match "this string will not appear anywhere"
story_opt runs-events-incomplete    "Search events on an incomplete run to find the failure context" runs events "$FAIL" --match error

# --- logs -------------------------------------------------------------------
story     runs-logs              "Stream logs at a specific moment" runs logs "$SUCCESS" "$EVENT_HASH" "$EVENT_VTIME"
story     runs-logs-begin-vtime  "Skip ahead — start streaming from a later moment instead of the root" runs logs "$SUCCESS" "$EVENT_HASH" "$EVENT_VTIME" --begin-vtime "$EVENT_VMIN" --begin-input-hash "$EVENT_HASH"
story     runs-logs-bad-moment   "Try logs with a moment that doesn't exist in this run" runs logs "$SUCCESS" 0 999999.0
story_opt runs-logs-incomplete   "Stream logs at the failure moment of an incomplete run" runs logs "$FAIL" "$FAIL_HASH" "$FAIL_VTIME"

# --- build logs -------------------------------------------------------------
story     runs-build-logs          "Stream the build logs to see how a run was set up" runs build-logs "$SUCCESS"
story     runs-build-logs-unknown  "Wrong run ID — build-logs reports a clean error" runs build-logs "$UNKNOWN"

# ============================================================================

echo >&2
echo "gallery written to:" >&2
echo "$OUTDIR"
