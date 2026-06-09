#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Regenerate the snouty "gallery" against fresh, live run IDs.

The gallery is a set of Markdown files, one per "story". Each story models a
hypothetical user with a concrete *goal*, runs the snouty command that user
would run, captures the output, and records a plain-language *rubric* for how a
human or LLM reviewer should judge whether the output satisfied that goal. Every
story is also gated by a programmatic check; a degenerate example (an empty
table, an unintended error, a filter that didn't narrow) fails the run rather
than being silently emitted.

Credentials come from the usual ANTITHESIS_* environment variables (snouty reads
them). Behaviour is controlled with flags, not env vars:

    uv run scripts/gen-gallery.py --out ./out
    uv run scripts/gen-gallery.py --only runs-events-single
    uv run scripts/gen-gallery.py --list

Nothing is written to ./gallery; output goes to a tempdir (or --out) so you
never accidentally commit it. Diff successive runs (or against ./gallery) to see
how snouty changes affect command output.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

# A syntactically-valid but nonexistent run id, for clean-error stories.
UNKNOWN_RUN = "ffffffffffffffffffffffffffffffff-54-5"

# Keywords probed (in order) to sample a real event from a run.
EVENT_KEYWORDS = ["error", "test", "client", "info", "setup", "start", "the"]


class GalleryError(Exception):
    """A precondition could not be met; refuse to emit a partial gallery."""


# ---------------------------------------------------------------------------
# snouty runner
# ---------------------------------------------------------------------------


@dataclass
class Result:
    args: list[str]
    stdout: str
    stderr: str
    returncode: int

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def combined(self) -> str:
        # Stories showcase verbose/error output, which snouty writes to stderr.
        return self.stdout if not self.stderr else f"{self.stdout}{self.stderr}"


class Snouty:
    """Shells out to the snouty binary. The gallery is a black-box test of the
    CLI, so we never reach into snouty's internals — we run it and read what a
    user would see."""

    def __init__(self, binary: Path):
        self.binary = binary
        # `runs show --web` shells out to xdg-open/open. Drop no-op shims on PATH
        # (and point $BROWSER at one) so regenerating never spawns a browser.
        self._shim = Path(tempfile.mkdtemp(prefix="snouty-gallery-shim."))
        for prog in ("xdg-open", "open"):
            shim = self._shim / prog
            shim.write_text("#!/bin/sh\nexit 0\n")
            shim.chmod(0o755)
        self._env = dict(os.environ)
        self._env["PATH"] = f"{self._shim}{os.pathsep}{self._env.get('PATH', '')}"
        self._env["BROWSER"] = str(self._shim / "xdg-open")

    def cleanup(self) -> None:
        shutil.rmtree(self._shim, ignore_errors=True)

    def run(self, args: list[str]) -> Result:
        proc = subprocess.run(
            [str(self.binary), *args],
            capture_output=True,
            text=True,
            env=self._env,
        )
        return Result(args, proc.stdout, proc.stderr, proc.returncode)

    def json_lines(self, args: list[str]) -> list[dict]:
        """Run `snouty --json <args>` and parse NDJSON rows.

        A non-zero exit is a hard error (timeout, 5xx, auth, DNS, ...) and raises
        — discovery distinguishes this from an empty-but-healthy result (exit 0,
        no rows). This is exact, unlike grepping stderr for known phrases."""
        res = self.run(["--json", *args])
        if not res.ok:
            raise GalleryError(
                f"`snouty {' '.join(args)}` failed (exit {res.returncode}): "
                f"{res.stderr.strip() or '<no stderr>'}"
            )
        rows = []
        for line in res.stdout.splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        return rows

    def json_obj(self, args: list[str]) -> dict:
        res = self.run(["--json", *args])
        if not res.ok:
            raise GalleryError(
                f"`snouty {' '.join(args)}` failed (exit {res.returncode}): "
                f"{res.stderr.strip() or '<no stderr>'}"
            )
        return json.loads(res.stdout)


# ---------------------------------------------------------------------------
# Discovery: pick runs and derive values that make each story meaningful.
# ---------------------------------------------------------------------------


@dataclass
class Discovery:
    success: str  # completed run that drives the event/logs/property stories
    fail: str  # an incomplete run
    cancelled: str  # a cancelled run
    launcher: str  # a real launcher value (for the --launcher story)
    created_after: str  # a timestamp with runs after it
    window_after: str
    window_before: str
    event_keyword: str
    event_kw2: str
    event_hash: str
    event_vtime: float
    fail_prop: str  # failing event property whose detail shows counter-examples
    pass_event_prop: str  # passing event property whose detail shows examples
    nonevent_prop: str  # non-event property whose detail shows a real value
    fuzzy: str  # substring that resolves to exactly one property
    ambiguous: str  # substring shared by >= 2 property names
    fail_hash: str
    fail_vtime: str


def _first_run(sn: Snouty, *filters: str) -> str | None:
    rows = sn.json_lines(["runs", "list", *filters, "-n", "1"])
    return rows[0]["run_id"] if rows else None


def _sample_event(sn: Snouty, run: str) -> dict | None:
    """Return the first event matching any probe keyword, or None if the run has
    no sampleable events. Raises GalleryError if the endpoint is unreachable."""
    for kw in EVENT_KEYWORDS:
        rows = sn.json_lines(["runs", "events", run, "--match", kw])
        for row in rows:
            # Need a moment with both coordinates for the logs stories.
            moment = row.get("moment") or {}
            if moment.get("input_hash") and moment.get("vtime"):
                row["_keyword"] = kw
                return row
    return None


def _pick_event_completed_run(sn: Snouty, scan: int) -> tuple[str, dict]:
    runs = sn.json_lines(["runs", "list", "--status", "completed", "-n", str(scan)])
    if not runs:
        raise GalleryError("no completed runs found on this tenant")
    for r in runs:
        run = r["run_id"]
        try:
            event = _sample_event(sn, run)
        except GalleryError:
            print(f"  skip {run}: events endpoint unreachable", file=sys.stderr)
            continue
        if event is not None:
            print(
                f"  completed run : {run} (events matched '{event['_keyword']}')",
                file=sys.stderr,
            )
            return run, event
        print(f"  skip {run}: no sampleable events", file=sys.stderr)
    raise GalleryError(
        f"none of the {len(runs)} most recent completed runs returned events "
        "(all unreachable or empty) — refusing to write a gallery with the "
        "event/logs stories skipped"
    )


def _pick_second_needle(event: dict, keyword: str) -> str:
    """A token that co-occurs with the keyword in this event and is distinct
    from it, preferring real content from output_text. snouty matches against
    the whole NDJSON line, so any token on the line is valid; we prefer content
    so the multi-match story reads naturally."""
    kw = keyword.lower()

    def tokens(text: str) -> list[str]:
        return [t for t in re.findall(r"[A-Za-z_]{4,}", text) if t.lower() != kw]

    for t in tokens(event.get("output_text") or ""):
        return t
    for t in tokens(json.dumps(event)):
        return t
    return ""


def _render_property(sn: Snouty, run: str, name: str) -> str:
    return sn.run(["runs", "property", run, name]).combined


_MOMENT_ROW = re.compile(r"-?\d{6,}\s+\d+\.\d+")  # long hash + float vtime


def _has_moment_rows(rendered: str) -> bool:
    return _MOMENT_ROW.search(rendered) is not None


def _pick_property_with_moments(sn: Snouty, run: str, props: list[dict], status: str) -> str:
    """Pick a property of the given status whose *rendered detail* actually shows
    example/counter-example moment rows (HASH/VTIME). Counts in the JSON are not
    reliable predictors of the rendered detail, so we render-probe, trying the
    highest-count candidates first."""
    count_key = "counterexample_count" if status == "Failing" else "example_count"
    candidates = [p for p in props if p.get("status") == status]
    candidates.sort(key=lambda p: p.get(count_key) or 0, reverse=True)
    for p in candidates:
        if _has_moment_rows(_render_property(sn, run, p["name"])):
            return p["name"]
    raise GalleryError(
        f"no {status} property on {run} renders example moments — cannot build "
        f"the {'failing' if status == 'Failing' else 'passing'} property story"
    )


_VALUE_ROW = re.compile(r"^(?:passing|failing|unreachable)\s+(.*)$", re.MULTILINE)


def _pick_nonevent_property(sn: Snouty, run: str, props: list[dict]) -> str:
    """Pick a non-event property whose detail shows a real single value (not an
    empty `[0 items]` placeholder)."""
    candidates = [p for p in props if p.get("is_event") is False]
    candidates.sort(key=lambda p: p.get("example_count") or 0, reverse=True)
    for p in candidates:
        rendered = _render_property(sn, run, p["name"])
        m = _VALUE_ROW.search(rendered)
        if m:
            value = m.group(1).strip()
            if value and value not in ("-", "[0 items]"):
                return p["name"]
    raise GalleryError(f"no non-event property on {run} renders a usable value")


def _pick_fuzzy(prop_names: list[str]) -> str:
    """A case-insensitive substring contained in exactly one property name — so
    snouty resolves it to a single property instead of erroring."""
    lower = [n.lower() for n in prop_names]
    # Try whole words first (read naturally), then fall back to any substring.
    for name in prop_names:
        for word in re.findall(r"[A-Za-z]{4,}", name):
            w = word.lower()
            if sum(w in n for n in lower) == 1:
                return word
    raise GalleryError("no substring resolves to exactly one property")


def _pick_ambiguous(prop_names: list[str]) -> str:
    counts: dict[str, int] = {}
    for name in prop_names:
        for word in {w.lower() for w in re.findall(r"[A-Za-z]{5,}", name)}:
            counts[word] = counts.get(word, 0) + 1
    shared = sorted((w for w, c in counts.items() if c > 1), key=len, reverse=True)
    if shared:
        return shared[0]
    raise GalleryError("no substring is shared by >= 2 properties")


def _rfc3339(ts: str) -> str:
    return ts


def discover(sn: Snouty, scan: int) -> Discovery:
    print("discovering runs via the live API…", file=sys.stderr)

    success, event = _pick_event_completed_run(sn, scan)

    fail = _first_run(sn, "--status", "incomplete")
    cancelled = _first_run(sn, "--status", "cancelled")
    if not fail:
        raise GalleryError("no incomplete run found — incomplete stories cannot run")
    if not cancelled:
        raise GalleryError("no cancelled run found — the cancelled story cannot run")
    print(f"  incomplete run: {fail}", file=sys.stderr)
    print(f"  cancelled run : {cancelled}", file=sys.stderr)

    # Dynamic listing params from real runs, so listing stories aren't empty.
    recent = sn.json_lines(["runs", "list", "-n", "30"])
    if not recent:
        raise GalleryError("no runs found at all")
    launcher = next((r["launcher"] for r in recent if r.get("launcher")), "")
    if not launcher:
        raise GalleryError("no run has a launcher — the --launcher story cannot run")
    by_time = sorted(recent, key=lambda r: r["created_at"])
    # created-after: a timestamp with several runs after it.
    created_after = by_time[max(0, len(by_time) - 6)]["created_at"]
    # created-window: brackets the middle of the recent runs.
    window_after = by_time[0]["created_at"]
    window_before = by_time[-1]["created_at"]

    keyword = event["_keyword"]
    moment = event["moment"]

    fail_show = sn.json_obj(["runs", "show", fail])
    fail_moment = fail_show.get("failure_moment") or {}

    props = sn.json_lines(["runs", "properties", success])
    prop_names = [p["name"] for p in props]

    disc = Discovery(
        success=success,
        fail=fail,
        cancelled=cancelled,
        launcher=launcher,
        created_after=_rfc3339(created_after),
        window_after=_rfc3339(window_after),
        window_before=_rfc3339(window_before),
        event_keyword=keyword,
        event_kw2=_pick_second_needle(event, keyword),
        event_hash=moment["input_hash"],
        event_vtime=float(moment["vtime"]),
        fail_prop=_pick_property_with_moments(sn, success, props, "Failing"),
        pass_event_prop=_pick_property_with_moments(sn, success, props, "Passing"),
        nonevent_prop=_pick_nonevent_property(sn, success, props),
        fuzzy=_pick_fuzzy(prop_names),
        ambiguous=_pick_ambiguous(prop_names),
        fail_hash=fail_moment.get("input_hash", ""),
        fail_vtime=fail_moment.get("vtime", ""),
    )
    if not disc.event_kw2:
        raise GalleryError("could not derive a second needle for multi-match story")
    if not disc.fail_hash or not disc.fail_vtime:
        raise GalleryError(f"incomplete run {fail} has no failure moment")
    return disc


# ---------------------------------------------------------------------------
# Checks: each returns (passed, detail). They validate the captured output so a
# degenerate story can never pass silently.
# ---------------------------------------------------------------------------


@dataclass
class Story:
    slug: str
    title: str
    goal: str
    judge: str
    args: list[str]
    check: Callable[["StoryRun", "Registry"], tuple[bool, str]]
    json_capable: bool = True  # can we re-run with --json for structured rows?


@dataclass
class StoryRun:
    story: Story
    result: Result
    rows: list[dict] | None  # structured rows from the --json variant, if any


class Registry:
    """Holds per-slug row counts so dependent checks (e.g. "narrowed vs the bare
    keyword search") can compare against an earlier story."""

    def __init__(self) -> None:
        self.row_counts: dict[str, int] = {}


# -- check factories --------------------------------------------------------


def non_empty_table(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    n = len(sr.rows or [])
    return (n > 0, f"{n} rows")


def rows_at_most(limit: int):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        n = len(sr.rows or [])
        return (1 <= n <= limit, f"{n} rows (limit {limit})")

    return chk


def all_status(status: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        bad = [r.get("status") for r in rows if r.get("status") != status]
        return (bool(rows) and not bad, f"{len(rows)} rows, all status={status}")

    return chk


def properties_pass_and_fail(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    rows = sr.rows or []
    has_p = any(r.get("status") == "Passing" for r in rows)
    has_f = any(r.get("status") == "Failing" for r in rows)
    return (has_p and has_f, f"{len(rows)} props, passing={has_p} failing={has_f}")


def all_launcher(value: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        bad = [r for r in rows if r.get("launcher") != value]
        return (bool(rows) and not bad, f"{len(rows)} rows, all launcher={value!r}")

    return chk


def all_created_after(ts: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        lo = datetime.fromisoformat(ts)
        bad = [r for r in rows if datetime.fromisoformat(r["created_at"]) < lo]
        return (bool(rows) and not bad, f"{len(rows)} rows, all >= {ts}")

    return chk


def all_created_within(after: str, before: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        lo, hi = datetime.fromisoformat(after), datetime.fromisoformat(before)
        bad = [r for r in rows if not (lo <= datetime.fromisoformat(r["created_at"]) <= hi)]
        return (bool(rows) and not bad, f"{len(rows)} rows, all in [{after}, {before}]")

    return chk


def expect_message(*needles: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = sr.result.combined.lower()
        hit = [n for n in needles if n.lower() in text]
        return (bool(hit), f"matched {hit!r}" if hit else f"expected one of {needles!r}")

    return chk


def contains_all(*needles: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = sr.result.combined
        missing = [n for n in needles if n not in text]
        return (not missing, "all present" if not missing else f"missing {missing!r}")

    return chk


def verbose_api_calls(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    has_get = "> GET" in sr.result.stderr or "> GET" in sr.result.stdout
    has_table = "STATUS" in sr.result.combined or "RUN" in sr.result.combined.upper()
    return (has_get and has_table, f"api_calls={has_get} table={has_table}")


def event_multi_match(needle: str, second: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        n1, n2 = needle.lower(), second.lower()
        bad = [r for r in rows if not (n1 in json.dumps(r).lower() and n2 in json.dumps(r).lower())]
        single = reg.row_counts.get("runs-events-single", 1 << 30)
        narrowed = len(rows) < single
        ok = bool(rows) and not bad and narrowed
        return (ok, f"{len(rows)} rows w/ both needles, narrowed {single}->{len(rows)}={narrowed}")

    return chk


def event_keyword_present(keyword: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        present = keyword.lower() in sr.result.combined.lower()
        return (bool(rows) and present, f"{len(rows)} rows, keyword shown={present}")

    return chk


def property_has_examples(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    ok = _has_moment_rows(sr.result.combined)
    return (ok, "shows example moments" if ok else "no example moments (degenerate)")


def property_single_value(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    text = sr.result.combined
    m = _VALUE_ROW.search(text)
    value = m.group(1).strip() if m else ""
    ok = bool(value) and value not in ("-", "[0 items]") and not _has_moment_rows(text)
    return (ok, f"value={value!r}, no moments")


def resolves_single_property(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    text = sr.result.combined
    ok = sr.result.ok and "multiple properties match" not in text.lower() and "Name" in text
    return (ok, "resolved to one property" if ok else "did not resolve to a single property")


def shows_ambiguity(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    ok = "multiple properties match" in sr.result.combined.lower()
    return (ok, "listed candidates" if ok else "did not show the ambiguity prompt")


def succeeds_with(*needles: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = sr.result.combined
        missing = [n for n in needles if n not in text]
        return (sr.result.ok and not missing, f"exit ok, missing={missing!r}")

    return chk


def logs_non_empty(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    n = len(sr.rows or [])
    return (n > 0, f"{n} log lines")


def logs_begin_at(begin: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        if not rows:
            return (False, "no log lines")
        first = float(rows[0].get("moment", {}).get("vtime", rows[0].get("vtime", 0.0)))
        ok = first >= float(begin) - 1e-9
        return (ok, f"{len(rows)} lines, first vtime {first} >= {begin}")

    return chk


# ---------------------------------------------------------------------------
# Story definitions
# ---------------------------------------------------------------------------


def build_stories(d: Discovery) -> list[Story]:
    kw, kw2 = d.event_keyword, d.event_kw2
    # `--begin-vtime` for the logs skip-ahead story: just before the sampled moment.
    vmin = f"{max(0.0, d.event_vtime - 0.5):.3f}"
    return [
        # -- listing --------------------------------------------------------
        Story(
            "runs",
            "Quickly check what test runs are around",
            "I just want to glance at what test runs exist without recalling any subcommands.",
            "A readable table of recent runs (id, status, title, time) appears — `runs` behaves like `runs list`.",
            ["runs"],
            non_empty_table,
        ),
        Story(
            "runs-list",
            "List recent runs to find one to inspect",
            "I want to scan recent runs and pick one to dig into.",
            "Up to 10 recent runs, newest first, with legible id/status/title/time columns.",
            ["runs", "list", "-n", "10"],
            rows_at_most(10),
        ),
        Story(
            "runs-list--limit",
            "Show me just the last three runs",
            "I only care about the very latest handful of runs.",
            "At most 3 rows, the most recent ones.",
            ["runs", "list", "-n", "3"],
            rows_at_most(3),
        ),
        Story(
            "runs-list--detail",
            "Get full descriptions instead of truncated titles",
            "Default titles are truncated; I want to read the full descriptions.",
            "Descriptions are shown in full (longer than the default view), one row per run.",
            ["runs", "list", "-n", "6", "--detail"],
            non_empty_table,
        ),
        Story(
            "runs-list--status-completed",
            "Only show runs that finished cleanly",
            "I want to ignore in-flight/failed runs and see only completed ones.",
            "Every row has status=completed.",
            ["runs", "list", "-n", "8", "--status", "completed"],
            all_status("completed"),
        ),
        Story(
            "runs-list--status-incomplete",
            "Find recent failures to triage",
            "I'm triaging and want only runs that ended incomplete.",
            "Every row has status=incomplete.",
            ["runs", "list", "-n", "8", "--status", "incomplete"],
            all_status("incomplete"),
        ),
        Story(
            "runs-list--launcher",
            f"Show only {d.launcher}-launched runs",
            "I want to see only the runs kicked off by one particular launcher.",
            f"Non-empty, and every row's launcher is {d.launcher!r}.",
            ["runs", "list", "-n", "8", "--launcher", d.launcher],
            all_launcher(d.launcher),
        ),
        Story(
            "runs-list--created-after",
            "What runs have we kicked off recently?",
            "I want runs created on or after a given date.",
            f"Non-empty, and every row was created at/after {d.created_after}.",
            ["runs", "list", "--created-after", d.created_after],
            all_created_after(d.created_after),
        ),
        Story(
            "runs-list--created-window",
            "Look at runs from a specific window",
            "I want runs created within a specific time window.",
            f"Non-empty, and every row was created within [{d.window_after}, {d.window_before}].",
            [
                "runs",
                "list",
                "--created-after",
                d.window_after,
                "--created-before",
                d.window_before,
            ],
            all_created_within(d.window_after, d.window_before),
        ),
        Story(
            "runs-verbose",
            "See the API calls printed while you list runs",
            "I'm debugging and want to see the HTTP requests snouty makes.",
            "The run table prints, and stderr shows the `> GET` request lines (tokens redacted). "
            "This is a debugging flag: the FULL request AND response is intended output, "
            "including bulky response headers (e.g. content-security-policy) and long lines — "
            "do not treat header verbosity or line width here as a defect.",
            ["runs", "list", "-n", "3", "--verbose"],
            verbose_api_calls,
        ),
        # -- single-run metadata -------------------------------------------
        Story(
            "runs-show",
            "Peek at the metadata for a completed run",
            "I want the metadata for one specific run.",
            "Shows the run id, status, timestamps, launcher, and links.",
            ["runs", "show", d.success],
            contains_all(d.success, "completed"),
            json_capable=False,
        ),
        Story(
            "runs-show--web",
            "Jump straight to the triage report in the browser",
            "I want to open this run's triage report in my browser.",
            "Prints the report URL and exits cleanly (the browser is shimmed to a no-op here).",
            ["runs", "show", d.success, "--web"],
            succeeds_with("http"),
            json_capable=False,
        ),
        Story(
            "runs-show-incomplete",
            "Inspect a run that aborted early",
            "A run ended incomplete; I want to see where it died (failure vtime/hash).",
            "Status is incomplete and the failure moment (vtime/hash) is shown.",
            ["runs", "show", d.fail],
            contains_all("incomplete"),
            json_capable=False,
        ),
        Story(
            "runs-show-cancelled",
            "What does a cancelled run look like?",
            "I want to see the metadata of a cancelled run.",
            "Status is shown as cancelled.",
            ["runs", "show", d.cancelled],
            contains_all("cancelled"),
            json_capable=False,
        ),
        # -- properties -----------------------------------------------------
        Story(
            "runs-properties",
            "See all properties — pass and fail",
            "I want the full property list for a completed run.",
            "A table with both passing and failing properties present.",
            ["runs", "properties", d.success],
            properties_pass_and_fail,
        ),
        Story(
            "runs-properties--passing",
            "List only the green properties",
            "I want to see only the properties that passed.",
            "Every row is a passing property.",
            ["runs", "properties", d.success, "--passing"],
            all_status("Passing"),
        ),
        Story(
            "runs-properties--failing",
            "Focus on the properties that broke",
            "I want to see only the properties that failed.",
            "Every row is a failing property.",
            ["runs", "properties", d.success, "--failing"],
            all_status("Failing"),
        ),
        Story(
            "runs-properties-incomplete",
            "Properties for a run that never finished",
            "I try to view properties on an incomplete run.",
            "A clean error explaining the properties aren't available (because the run is incomplete) — not a crash or stack trace.",
            ["runs", "properties", d.fail],
            expect_message("no properties", "incomplete", "not found", "404"),
            json_capable=False,
        ),
        # -- property detail ------------------------------------------------
        Story(
            "runs-property-failing",
            "Drill into a failing property's counter-examples",
            "A property failed; I want to see concrete counter-examples I can debug.",
            "Shows the property plus at least one counter-example with a moment (hash/vtime) — not an empty `unreachable`.",
            ["runs", "property", d.success, d.fail_prop],
            property_has_examples,
            json_capable=False,
        ),
        Story(
            "runs-property-passing",
            "Look at the examples behind a passing property",
            "A property passed; I want to see example moments that satisfied it.",
            "Shows at least one example with a moment (hash/vtime).",
            ["runs", "property", d.success, d.pass_event_prop],
            property_has_examples,
            json_capable=False,
        ),
        Story(
            "runs-property-non-event",
            "View a non-event property — a single value",
            "I want to inspect a non-event property, which is a single value rather than moments.",
            "Shows the property's value and has no per-moment rows.",
            ["runs", "property", d.success, d.nonevent_prop],
            property_single_value,
            json_capable=False,
        ),
        Story(
            "runs-property-fuzzy",
            "Substring match — let snouty figure out which property",
            "I type part of a property name and expect snouty to find the one I meant.",
            "Resolves to exactly one property and shows it; does NOT print 'multiple properties match'.",
            ["runs", "property", d.success, d.fuzzy],
            resolves_single_property,
            json_capable=False,
        ),
        Story(
            "runs-property-ambiguous",
            "Substring matches multiple properties",
            "I type an ambiguous substring; I want to understand how snouty responds.",
            "Lists the candidate properties and asks me to disambiguate.",
            ["runs", "property", d.success, d.ambiguous],
            shows_ambiguity,
            json_capable=False,
        ),
        Story(
            "runs-property-not-found",
            "Typo'd a property name — get a clean error",
            "I mistyped a property name and want a helpful error.",
            "A clear 'no property matches' message, not a stack trace.",
            ["runs", "property", d.success, "this property does not exist"],
            expect_message("no property matches"),
            json_capable=False,
        ),
        # -- events ---------------------------------------------------------
        Story(
            "runs-events-single",
            f"Find events that mention '{kw}'",
            "I want to find events that mention a particular keyword.",
            f"At least one matching event row, and the keyword '{kw}' appears in the output.",
            ["runs", "events", d.success, "--match", kw],
            event_keyword_present(kw),
        ),
        Story(
            "runs-events-multi-match",
            "AND-narrow with two --match needles",
            "I want to narrow results to events that mention BOTH of two terms.",
            f"At least one row, every row contains both '{kw}' and '{kw2}', and strictly fewer rows than single-match.",
            ["runs", "events", d.success, "--match", kw, "--match", kw2],
            event_multi_match(kw, kw2),
        ),
        Story(
            "runs-events-no-results",
            "Search events that don't match anything",
            "I search for a string that doesn't occur; I want a friendly empty result.",
            "A clear 'No events matched' message — not an error or a crash.",
            ["runs", "events", d.success, "--match", "this string will not appear anywhere"],
            expect_message("No events matched"),
            json_capable=False,
        ),
        Story(
            "runs-events-incomplete",
            "Search events on an incomplete run for failure context",
            "An incomplete run failed; I want events around the failure.",
            "At least one matching event row from the incomplete run.",
            ["runs", "events", d.fail, "--match", "error"],
            non_empty_table,
        ),
        # -- logs -----------------------------------------------------------
        Story(
            "runs-logs",
            "Stream logs at a specific moment",
            "I want the log lines at a particular moment of the run.",
            "At least one log line is streamed at/around the moment.",
            ["runs", "logs", d.success, d.event_hash, f"{d.event_vtime}"],
            logs_non_empty,
        ),
        Story(
            "runs-logs-begin-vtime",
            "Skip ahead — start from a later moment",
            "I want to start streaming from a later moment instead of the root.",
            f"At least one line, and the stream starts at/after vtime {vmin} (not the root).",
            [
                "runs",
                "logs",
                d.success,
                d.event_hash,
                f"{d.event_vtime}",
                "--begin-vtime",
                vmin,
                "--begin-input-hash",
                d.event_hash,
            ],
            logs_begin_at(vmin),
        ),
        Story(
            "runs-logs-bad-moment",
            "Try logs with a moment that doesn't exist",
            "I ask for a moment that isn't in this run; I want a clean error.",
            "A clean error, not a crash or stack trace.",
            ["runs", "logs", d.success, "0", "999999.0"],
            expect_message("error", "not found", "no ", "invalid", "bad"),
            json_capable=False,
        ),
        Story(
            "runs-logs-incomplete",
            "Stream logs at the failure moment of an incomplete run",
            "I want the logs right at the moment an incomplete run failed.",
            "At least one log line at the failure moment.",
            ["runs", "logs", d.fail, d.fail_hash, d.fail_vtime],
            logs_non_empty,
        ),
        # -- build logs -----------------------------------------------------
        Story(
            "runs-build-logs",
            "Stream the build logs to see how a run was set up",
            "I want to see the build/setup logs for a run.",
            "At least one build-log line is streamed.",
            ["runs", "build-logs", d.success],
            logs_non_empty,
        ),
        Story(
            "runs-build-logs-unknown",
            "Wrong run ID — build-logs reports a clean error",
            "I pass a run id that doesn't exist; I want a clean error.",
            "A clean error, not a crash or stack trace.",
            ["runs", "build-logs", UNKNOWN_RUN],
            expect_message("error", "not found", "no ", "invalid"),
            json_capable=False,
        ),
    ]


# ---------------------------------------------------------------------------
# Rendering + main
# ---------------------------------------------------------------------------


def write_story(out_dir: Path, story: Story, sr: StoryRun, passed: bool, detail: str) -> None:
    verdict = "PASS" if passed else "FAIL"
    body = sr.result.combined.rstrip("\n")
    md = (
        f"# {story.title}\n\n"
        f"**User goal:** {story.goal}\n\n"
        f"**Judge satisfaction by:** {story.judge}\n\n"
        f"```shell\n$ snouty {' '.join(story.args)}\n{body}\n```\n\n"
        f"_Automated check: {verdict} — {detail}_\n"
    )
    (out_dir / f"{story.slug}.md").write_text(md)


def run_story(sn: Snouty, story: Story) -> StoryRun:
    result = sn.run(story.args)
    rows = None
    if story.json_capable:
        try:
            rows = sn.json_lines(story.args)
        except (GalleryError, json.JSONDecodeError):
            rows = None  # error stories are validated on rendered text instead
    return StoryRun(story, result, rows)


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Regenerate the snouty gallery.")
    parser.add_argument("--snouty", type=Path, help="snouty binary (default: target/debug/snouty)")
    parser.add_argument(
        "--build",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="cargo build before running (default: yes)",
    )
    parser.add_argument("--out", type=Path, help="output dir (default: a fresh tempdir)")
    parser.add_argument(
        "--runs-to-scan",
        type=int,
        default=15,
        help="recent completed runs to probe for one with events",
    )
    parser.add_argument("--only", nargs="+", metavar="SLUG", help="only generate these stories")
    parser.add_argument("--list", action="store_true", help="list story slugs and exit")
    parser.add_argument("--fail-fast", action="store_true", help="stop at the first failing story")
    args = parser.parse_args()

    if args.list:
        # Build with a dummy Discovery just to enumerate slugs. event_vtime
        # (index 10) must be a float since build_stories derives the window.
        dummy = Discovery(*(["x"] * 10 + [0.0] + ["x"] * 9))  # type: ignore[arg-type]
        for s in build_stories(dummy):
            print(s.slug)
        return 0

    snouty_bin = args.snouty
    if snouty_bin is None:
        if args.build:
            print("building snouty (target/debug)…", file=sys.stderr)
            if subprocess.run(["cargo", "build", "-q"], cwd=repo_root).returncode != 0:
                print("error: cargo build failed", file=sys.stderr)
                return 1
        snouty_bin = repo_root / "target" / "debug" / "snouty"
    snouty_bin = snouty_bin.resolve()
    if not snouty_bin.exists():
        print(f"error: snouty binary not found: {snouty_bin}", file=sys.stderr)
        return 1
    print(f"using binary: {snouty_bin}", file=sys.stderr)

    out_dir = args.out or Path(tempfile.mkdtemp(prefix="snouty-gallery."))
    out_dir.mkdir(parents=True, exist_ok=True)

    sn = Snouty(snouty_bin)
    failures: list[tuple[str, str]] = []
    try:
        disc = discover(sn, args.runs_to_scan)
        stories = build_stories(disc)
        if args.only:
            wanted = set(args.only)
            stories = [s for s in stories if s.slug in wanted]
            unknown = wanted - {s.slug for s in stories}
            if unknown:
                print(f"error: unknown story slug(s): {sorted(unknown)}", file=sys.stderr)
                return 1

        reg = Registry()
        for story in stories:
            sr = run_story(sn, story)
            if sr.rows is not None:
                reg.row_counts[story.slug] = len(sr.rows)
            passed, detail = story.check(sr, reg)
            write_story(out_dir, story, sr, passed, detail)
            mark = "ok  " if passed else "FAIL"
            print(f"  {mark} {story.slug:<32} {detail}", file=sys.stderr)
            if not passed:
                failures.append((story.slug, detail))
                if args.fail_fast:
                    break
    except GalleryError as e:
        # A precondition could not be met. Fail loudly and clearly rather than
        # emitting a partial gallery or dumping a traceback.
        print(f"\nerror: {e}", file=sys.stderr)
        return 1
    finally:
        sn.cleanup()

    print(file=sys.stderr)
    if failures:
        print(f"{len(failures)} story/stories failed their check:", file=sys.stderr)
        for slug, detail in failures:
            print(f"  - {slug}: {detail}", file=sys.stderr)
        print(f"\ngallery written to:\n{out_dir}", file=sys.stderr)
        return 1
    print("all stories passed their checks", file=sys.stderr)
    print("gallery written to:", file=sys.stderr)
    print(out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
