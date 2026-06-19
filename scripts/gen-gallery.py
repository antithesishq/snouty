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

There are two kinds of story:

  * goal stories — capture one command's output and judge it against a user goal
    (the bulk of the gallery; slugs like `runs-events-single`).
  * help stories — capture `snouty <cmd> --help` next to that command's default
    output (slugs like `help-runs-properties`) and judge whether the help is
    informative, clear, concise, consistent, and *aligned* with what the command
    prints. Commands that mutate state or need an interactive arg (launch,
    debug, validate, update, completions) are help-only. An automated check
    verifies any column/field the help names actually appears in the output.

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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

# A syntactically-valid but nonexistent run id, for clean-error stories.
UNKNOWN_RUN = "ffffffffffffffffffffffffffffffff-54-5"

# Stories are captured concurrently (each is one or two snouty subprocesses that
# can each block on snouty's 60s timeout); checks are then evaluated serially.
CAPTURE_WORKERS = 6

# Keywords probed (in order) to sample a real event from a run. --match is
# required, so a truly unfiltered call isn't possible; ordering broadest-first
# (a near-universal needle, then common log words) makes the common case a
# single probe per run before falling back to narrower terms.
EVENT_KEYWORDS = ["the", "info", "test", "start", "error", "client", "setup"]


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

    def run(self, args: list[str], env: dict[str, str | None] | None = None) -> Result:
        # `env` overrides individual vars for this call only: a string sets the
        # var, None unsets it (so a story can model an environment missing some
        # credential). Everything else inherits the live ANTITHESIS_* env.
        run_env = self._env
        if env is not None:
            run_env = dict(self._env)
            for key, value in env.items():
                if value is None:
                    run_env.pop(key, None)
                else:
                    run_env[key] = value
        proc = subprocess.run(
            [str(self.binary), *args],
            capture_output=True,
            text=True,
            env=run_env,
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
    # All fields default so `--list` can build an empty Discovery() just to
    # enumerate slugs, immune to future field additions.
    success: str = ""  # completed run that drives the event/logs/property stories
    fail: str = ""  # an incomplete run
    cancelled: str = ""  # a cancelled run
    launcher: str = ""  # a real launcher value (for the --launcher story)
    created_after: str = ""  # a timestamp with runs after it
    window_after: str = ""
    window_before: str = ""
    event_keyword: str = ""
    event_kw2: str = ""
    event_hash: str = ""
    event_vtime: float = 0.0
    fail_prop: str = ""  # failing event property whose detail shows counter-examples
    pass_event_prop: str = ""  # passing event property whose detail shows examples
    nonevent_prop: str = ""  # non-event property whose detail shows a real value
    name_filter: str = ""  # substring matching exactly one property name
    fail_hash: str = ""
    fail_vtime: str = ""


def _first_run(sn: Snouty, *filters: str) -> str | None:
    rows = sn.json_lines(["runs", "list", *filters, "-n", "1"])
    return rows[0]["run_id"] if rows else None


def _sample_event(sn: Snouty, run: str) -> dict | None:
    """Return the first event matching any probe keyword that is usable for the
    event/logs stories, or None if the run has no such event. Raises GalleryError
    if the endpoint is unreachable.

    "Usable" means it has a moment with both coordinates (for the logs stories)
    *and* yields a distinctive second needle (for the multi-match story); we stash
    both on the returned row so discovery doesn't have to re-derive them."""
    for kw in EVENT_KEYWORDS:
        rows = sn.json_lines(["runs", "events", run, "--match", kw])
        for row in rows:
            moment = row.get("moment") or {}
            if not (moment.get("input_hash") and moment.get("vtime")):
                continue
            second = _pick_second_needle(row, kw)
            if second is None:
                continue
            row["_keyword"] = kw
            row["_second_needle"] = second
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


# Envelope keys and ubiquitous JSON literals that occur on (nearly) every raw
# NDJSON event line. A second --match needle drawn from these cannot narrow the
# result set, so the multi-match story's `len(rows) < single` check is
# unsatisfiable — exclude them entirely.
_UBIQUITOUS_TOKENS = frozenset(
    {
        "output_text",
        "moment",
        "input_hash",
        "vtime",
        "source",
        "container",
        "stream",
        "name",
        "level",
        "true",
        "false",
        "null",
    }
)


def _pick_second_needle(event: dict, keyword: str) -> str | None:
    """A token that co-occurs with the keyword in this event and is *distinctive*
    enough to narrow a search — drawn from the event's output_text content, never
    from envelope keys. Returns None when no distinctive token exists so the
    caller can try a different event/run rather than committing to a needle that
    cannot narrow the multi-match story."""
    kw = keyword.lower()
    for t in re.findall(r"[A-Za-z_]{4,}", event.get("output_text") or ""):
        low = t.lower()
        if low != kw and low not in _UBIQUITOUS_TOKENS:
            return t
    return None


def _render_property(sn: Snouty, run: str, name: str) -> str:
    # `--name` is a substring filter; passing the exact name selects it (plus any
    # other name it's a substring of, which is fine for these render probes).
    return sn.run(["runs", "properties", run, "--name", name, "--detail"]).combined


_MOMENT_ROW = re.compile(r"-?\d{6,}\s+\d+\.\d+")  # long hash + float vtime


def _has_moment_rows(rendered: str) -> bool:
    return _MOMENT_ROW.search(rendered) is not None


def _moments(values: list) -> list:
    """The subset of an examples/counterexamples array that has a moment with
    both coordinates — exactly the elements snouty renders as HASH/VTIME rows."""
    out = []
    for v in values or []:
        moment = (v.get("moment") if isinstance(v, dict) else None) or {}
        if moment.get("input_hash") and moment.get("vtime"):
            out.append(v)
    return out


def _pick_property_with_moments(sn: Snouty, run: str, props: list[dict], status: str) -> str:
    """Pick a property of the given status that has example/counter-example moment
    rows. The properties JSON already carries the full `examples`/`counterexamples`
    arrays snouty renders, so we select straight from it (most moments first) and
    only render-probe the one chosen property as a cheap safety net."""
    arr_key = "counterexamples" if status == "Failing" else "examples"
    candidates = [
        (p, m)
        for p in props
        if p.get("status") == status and (m := _moments(p.get(arr_key) or []))
    ]
    candidates.sort(key=lambda c: len(c[1]), reverse=True)
    for p, _ in candidates:
        if _has_moment_rows(_render_property(sn, run, p["name"])):
            return p["name"]
        break  # the array said it has moments but the render disagreed — bail
    raise GalleryError(
        f"no {status} property on {run} renders example moments — cannot build "
        f"the {'failing' if status == 'Failing' else 'passing'} property story"
    )


# A non-event ("system") property renders its value under a `Result` label
# (see render_result in src/runs.rs) — `Result   <scalar>` inline, or a bare
# `Result` label line above indented JSON (no colon) for an object/array —
# never as a moment HASH/VTIME row.
_NONEVENT_RESULT = re.compile(r"^\s*Result\b", re.MULTILINE)


def _has_real_value(value) -> bool:
    """Whether a non-event example renders as a usable value — i.e. a scalar or a
    non-empty collection (an empty one renders as the `(no value)` placeholder)."""
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True  # scalars (incl. False/0) all render to a visible block


def _pick_nonevent_property(sn: Snouty, run: str, props: list[dict]) -> str:
    """Pick a non-event property that renders a real example value. We read the
    example arrays straight from the JSON and only render-probe the chosen one."""
    # `--name` is a substring filter, so the chosen name must not be a substring
    # of any *other* property's name — otherwise the --detail probe would also
    # expand that other property, and an event sibling's moment rows would make us
    # think we'd misclassified. Require the name to match only itself.
    lower_names = [p["name"].lower() for p in props]
    candidates = []
    for p in props:
        if p.get("is_event") is not False:
            continue
        if sum(p["name"].lower() in n for n in lower_names) != 1:
            continue
        values = (p.get("examples") or []) + (p.get("counterexamples") or [])
        if any(_has_real_value(v) for v in values):
            candidates.append(p)
    candidates.sort(key=lambda p: p.get("example_count") or 0, reverse=True)
    # Render-probe candidates (most examples first) until one renders the
    # non-event shape: a `Result` label block and no moment rows (which would mean
    # we misclassified an event property). Try several rather than bailing on the
    # first miss, so one oddly-rendering top candidate can't sink the whole story.
    for p in candidates[:8]:
        rendered = _render_property(sn, run, p["name"])
        if _NONEVENT_RESULT.search(rendered) and not _has_moment_rows(rendered):
            return p["name"]
    raise GalleryError(f"no non-event property on {run} renders a usable value")


def _pick_name_filter(prop_names: list[str]) -> str:
    """A case-insensitive word contained in exactly one property name — so the
    `--name` filter story narrows the list to a single, predictable property."""
    lower = [n.lower() for n in prop_names]
    for name in prop_names:
        for word in re.findall(r"[A-Za-z]{4,}", name):
            w = word.lower()
            if sum(w in n for n in lower) == 1:
                return word
    raise GalleryError("no substring matches exactly one property")


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
        created_after=created_after,
        window_after=window_after,
        window_before=window_before,
        event_keyword=keyword,
        event_kw2=event["_second_needle"],
        event_hash=moment["input_hash"],
        event_vtime=float(moment["vtime"]),
        fail_prop=_pick_property_with_moments(sn, success, props, "Failing"),
        pass_event_prop=_pick_property_with_moments(sn, success, props, "Passing"),
        nonevent_prop=_pick_nonevent_property(sn, success, props),
        name_filter=_pick_name_filter(prop_names),
        fail_hash=fail_moment.get("input_hash", ""),
        fail_vtime=fail_moment.get("vtime", ""),
    )
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
    # -- help stories ------------------------------------------------------
    # When `help_cmd` is set the story is a "help story": it captures
    # `snouty <help_cmd> --help` and renders it next to the command's default
    # output (`args`, plus any `samples`), so a reviewer can judge whether the
    # help is informative/clear/concise/consistent *and* matches what the
    # command actually prints. `args` may be empty (help-only, for commands we
    # must not run because they mutate state, e.g. launch/debug).
    help_cmd: list[str] | None = None
    # Extra labelled default-output captures shown after the primary one
    # (e.g. `runs list --detail`): list of (label, args).
    samples: list[tuple[str, list[str]]] | None = None
    # Tokens that must appear in BOTH the help text and the default output —
    # an automated "help aligns with output" gate (e.g. column headers the help
    # promises). Only enforced when there is default output to compare against.
    align_tokens: tuple[str, ...] = ()
    # Whether the default-output command is expected to exit 0. True for read
    # commands; set False for a command that legitimately exits non-zero while
    # still printing representative output (e.g. `doctor` when a check fails).
    expect_ok: bool = True
    # Per-story ANTITHESIS_* env overrides for the default-output command: a
    # string sets a var, None unsets it. Used by the doctor stories to model a
    # specific credential setup regardless of the operator's real environment.
    env: dict[str, str | None] | None = None


@dataclass
class StoryRun:
    story: Story
    result: Result
    rows: list[dict] | None  # structured rows from the --json variant, if any
    help_result: Result | None = None  # `<help_cmd> --help` capture, for help stories
    sample_results: list[tuple[str, Result]] | None = None  # extra labelled captures


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


def doctor_check(
    *,
    contains: tuple[str, ...],
    absent: tuple[str, ...] = (),
    ok: bool | None = None,
):
    """Gate a doctor story: every `contains` needle must appear, no `absent`
    needle may, and (when `ok` is given) the exit status must match. `ok` is left
    None for the api-key/legacy stories because their overall pass/fail also
    depends on the machine's container runtime — only the auth lines, which this
    asserts on, are deterministic."""

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = sr.result.combined
        missing = [n for n in contains if n not in text]
        unexpected = [n for n in absent if n in text]
        exit_matches = ok is None or sr.result.ok == ok
        passed = not missing and not unexpected and exit_matches
        bits = []
        if missing:
            bits.append(f"missing={missing!r}")
        if unexpected:
            bits.append(f"unexpected={unexpected!r}")
        if not exit_matches:
            bits.append(f"exit_ok={sr.result.ok} (want {ok})")
        return (passed, "; ".join(bits) or "auth output as expected")

    return chk


def doctor_json_check(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """Gate the `doctor --json` story: stdout must be a parseable report with a
    boolean `ok` and a non-empty `checks` array of well-formed records (name,
    status, message), it must include the api_key check, and a missing required
    credential must drive `ok` false."""
    try:
        data = json.loads(sr.result.stdout)
    except json.JSONDecodeError as e:
        return (False, f"stdout is not valid JSON: {e}")
    checks = data.get("checks")
    if not isinstance(data.get("ok"), bool) or not isinstance(checks, list) or not checks:
        return (False, f"missing ok/checks ({data!r:.80})")
    well_formed = all(
        isinstance(c.get("name"), str)
        and c.get("status") in ("ok", "warn", "error")
        and isinstance(c.get("message"), str)
        for c in checks
    )
    names = {c.get("name") for c in checks}
    passed = well_formed and "api_key" in names and data["ok"] is False
    return (passed, f"ok={data['ok']}, {len(checks)} checks, well_formed={well_formed}")


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


def property_non_event_result(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """A non-event property shows its value(s) under a `Result` label, with no
    per-moment HASH/VTIME rows (those belong to event properties' Examples)."""
    text = sr.result.combined
    has_result = _NONEVENT_RESULT.search(text) is not None
    no_moments = not _has_moment_rows(text)
    ok = has_result and no_moments
    return (ok, f"result={has_result}, no moments={no_moments}")


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


def help_story_check(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """Gate a help story: the `--help` must be well-formed (exit 0, has a Usage
    line), any documented `align_tokens` must appear in BOTH the help and the
    default output (the "help matches output" guarantee), and a command that is
    supposed to print default output must actually print something. The
    subjective judgment — is the help clear/concise/consistent? — is left to the
    reviewer; this only stops a degenerate help/output pair from passing."""
    h = sr.help_result
    if h is None or not h.ok or "Usage:" not in h.combined:
        rc = "none" if h is None else h.returncode
        return (False, f"help missing or malformed (exit {rc})")
    help_text = h.combined
    story = sr.story

    # Help-only story (mutating command we don't run): just the help is enough.
    if not story.args:
        return (True, "help only")

    out = sr.result.combined
    if not out.strip():
        return (False, "default output is empty")

    # A non-zero exit means `out` is probably an error (auth/API failure) captured
    # as if it were the command's normal output — don't pass it off as a sample.
    # `doctor` opts out (expect_ok=False): it exits non-zero on a failed check yet
    # still prints representative output.
    if story.expect_ok and not sr.result.ok:
        return (False, f"default command failed (exit {sr.result.returncode})")

    if story.align_tokens:
        miss_help = [t for t in story.align_tokens if t not in help_text]
        miss_out = [t for t in story.align_tokens if t not in out]
        if miss_help or miss_out:
            return (False, f"misaligned — absent from help={miss_help} output={miss_out}")
        return (True, f"help + output aligned on {list(story.align_tokens)}")
    return (True, "help + default output present")


# ---------------------------------------------------------------------------
# Story definitions
# ---------------------------------------------------------------------------


# `doctor` never calls the API, so each doctor story drives one specific output
# with a controlled ANTITHESIS_* env, independent of the operator's shell. Each
# flag says whether that var should be *set* for the story: a set var inherits
# the operator's real value when present (so the stories reflect the real
# environment) and falls back to a placeholder otherwise, so the state still
# holds on a machine missing that credential. doctor only checks presence, never
# the value, so an inherited secret is never printed and a placeholder is purely
# a stand-in.
def _doctor_env(
    *, api_key: bool, username: bool, password: bool, tenant: bool, repo: bool
) -> dict[str, str | None]:
    def want(name: str, set_it: bool, placeholder: str) -> str | None:
        return (os.environ.get(name) or placeholder) if set_it else None

    return {
        "ANTITHESIS_API_KEY": want("ANTITHESIS_API_KEY", api_key, "demo-api-key"),
        "ANTITHESIS_USERNAME": want("ANTITHESIS_USERNAME", username, "demo-user"),
        "ANTITHESIS_PASSWORD": want("ANTITHESIS_PASSWORD", password, "demo-pass"),
        "ANTITHESIS_TENANT": want("ANTITHESIS_TENANT", tenant, "demo-tenant"),
        "ANTITHESIS_REPOSITORY": want(
            "ANTITHESIS_REPOSITORY", repo, "registry.example.com/acme/demo"
        ),
    }


def build_stories(d: Discovery) -> list[Story]:
    kw, kw2 = d.event_keyword, d.event_kw2
    # `--begin-vtime` for the logs skip-ahead story: just before the sampled moment.
    vmin = f"{max(0.0, d.event_vtime - 0.5):.3f}"
    stories = [
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
            # --detail can't be combined with --json, so validate the rendered
            # key-value blocks directly rather than re-running for JSON rows. Check
            # the always-present title-case labels (the default table uses
            # UPPERCASE headers and no "Launcher"); "Description" is omitted when a
            # run has none, so requiring it would falsely fail on description-less
            # runs even though the detailed view rendered correctly.
            contains_all("Run ID", "Created", "Launcher"),
            json_capable=False,
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
            # The friendly error (explain_properties_error in src/runs.rs) says the
            # run "is incomplete"; require that distinctive word. A bare "404"/"not
            # found" would mean the friendly message regressed, and "no properties"
            # also matches the success-path "No properties found." — so neither is
            # a safe needle here.
            expect_message("incomplete"),
            json_capable=False,
        ),
        # -- property detail (`properties --name <x> --detail`) -------------
        Story(
            "runs-properties-detail-failing",
            "Drill into a failing property's counter-examples",
            "A property failed; I want to see concrete counter-examples I can debug.",
            "Shows the property plus at least one counter-example with a moment (hash/vtime) — not an empty `unreachable`.",
            ["runs", "properties", d.success, "--name", d.fail_prop, "--detail"],
            property_has_examples,
            json_capable=False,
        ),
        Story(
            "runs-properties-detail-passing",
            "Look at the examples behind a passing property",
            "A property passed; I want to see example moments that satisfied it.",
            "Shows at least one example with a moment (hash/vtime).",
            ["runs", "properties", d.success, "--name", d.pass_event_prop, "--detail"],
            property_has_examples,
            json_capable=False,
        ),
        Story(
            "runs-properties-detail-non-event",
            "Detail a non-event property — its result value",
            "I want to inspect a non-event ('system') property, whose value is data rather than moments.",
            "Shows the value under a 'Result' label (scalar inline, or JSON for an object/array), with no per-moment hash/vtime rows.",
            ["runs", "properties", d.success, "--name", d.nonevent_prop, "--detail"],
            property_non_event_result,
            json_capable=False,
        ),
        Story(
            "runs-properties--name",
            "Filter the property list by name substring",
            "I want to narrow the property list to ones whose name matches a substring.",
            "Non-empty: every shown property's name contains the substring (case-insensitive).",
            ["runs", "properties", d.success, "--name", d.name_filter],
            non_empty_table,
        ),
        Story(
            "runs-properties--name-no-match",
            "A name filter that matches nothing",
            "I filter on a substring no property has; I want a friendly empty result.",
            "A clear 'No properties match' message — not an error or a crash.",
            ["runs", "properties", d.success, "--name", "this property does not exist"],
            expect_message("No properties match"),
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
        # -- doctor (one story per distinct auth output) --------------------
        Story(
            "doctor-api-key",
            "Confirm my environment is ready with an API key",
            "I've configured snouty with an API key and want doctor to confirm I'm set up.",
            "doctor reports the API key as set without mentioning username/password, and also "
            "contacts the API to confirm it's reachable and report the API and tenant versions.",
            ["doctor"],
            doctor_check(
                contains=("ANTITHESIS_API_KEY is set", "Antithesis API reachable"),
                absent=("ANTITHESIS_USERNAME", "ANTITHESIS_PASSWORD"),
            ),
            json_capable=False,
            env=_doctor_env(api_key=True, username=False, password=False, tenant=True, repo=True),
        ),
        Story(
            "doctor-offline",
            "Check my setup without touching the network",
            "I don't want snouty making any network calls; I just want to validate my local "
            "tooling and environment variables.",
            "doctor runs every local check but skips the API connectivity/version check entirely "
            "— there is no 'Antithesis API' line — and still reports the rest.",
            ["doctor", "--offline"],
            doctor_check(
                contains=("ANTITHESIS_API_KEY is set",),
                absent=("Antithesis API",),
            ),
            json_capable=False,
            env=_doctor_env(api_key=True, username=False, password=False, tenant=True, repo=True),
        ),
        Story(
            "doctor-api-unreachable",
            "The Antithesis API can't be reached",
            "I run doctor but the API host is unreachable (wrong tenant, blocked network, or the "
            "service is down); I want a clear failure fast, not a hang.",
            "doctor reports the API as unreachable and fails (non-zero exit), and it returns "
            "promptly — the connect timeout bounds a black-holed or unresolvable host rather than "
            "letting doctor hang.",
            ["doctor"],
            doctor_check(contains=("Antithesis API unreachable",), ok=False),
            json_capable=False,
            # Point the client at a reserved, unroutable address (RFC 5737
            # TEST-NET-1) so the connect attempt is black-holed.
            env={
                **_doctor_env(api_key=True, username=False, password=False, tenant=True, repo=True),
                "ANTITHESIS_BASE_URL": "http://192.0.2.1",
            },
        ),
        Story(
            "doctor-verbose",
            "See the API request doctor makes",
            "I'm debugging connectivity and want to see the exact request doctor sends to the "
            "Antithesis API.",
            "doctor's report prints, and stderr shows the `> GET .../api/version` request (auth "
            "token redacted) for the version check. This is a debugging flag: the full request "
            "AND response is intended output, including bulky response headers and long lines — "
            "do not treat header verbosity or line width here as a defect.",
            ["doctor", "--verbose"],
            doctor_check(contains=("> GET", "Antithesis API reachable")),
            json_capable=False,
            env=_doctor_env(api_key=True, username=False, password=False, tenant=True, repo=True),
        ),
        Story(
            "doctor-api-key-and-legacy",
            "Both an API key and leftover username/password are set",
            "I have an API key but also still have ANTITHESIS_USERNAME/PASSWORD exported; "
            "I want to know which one snouty uses.",
            "doctor reports only the API key (it takes precedence) and does not mention the "
            "legacy username/password at all.",
            ["doctor"],
            doctor_check(
                contains=("ANTITHESIS_API_KEY is set",),
                absent=("ANTITHESIS_USERNAME", "ANTITHESIS_PASSWORD"),
            ),
            json_capable=False,
            env=_doctor_env(api_key=True, username=True, password=True, tenant=True, repo=True),
        ),
        Story(
            "doctor-no-auth",
            "Fresh install — doctor tells me what to configure",
            "I just installed snouty and haven't set any credentials; I want doctor to tell me what I need.",
            "doctor states an API key is required and points ONLY at ANTITHESIS_API_KEY — it must not "
            "steer me toward username/password, which is legacy auth (issue #145).",
            ["doctor"],
            doctor_check(
                contains=(
                    "ANTITHESIS_API_KEY is not set",
                    "requires an API key",
                    "ask Antithesis support",
                ),
                absent=("ANTITHESIS_USERNAME", "ANTITHESIS_PASSWORD"),
                ok=False,
            ),
            json_capable=False,
            env=_doctor_env(
                api_key=False, username=False, password=False, tenant=False, repo=False
            ),
        ),
        Story(
            "doctor-legacy-auth",
            "I only have a legacy username and password",
            "I authenticate with a username/password and no API key; I want doctor to tell me whether that's enough.",
            "doctor warns the API key is missing (so `snouty runs` and other API commands won't work), "
            "flags username/password as legacy auth limited to `snouty launch`/`snouty debug`, and steers "
            "me toward setting an API key.",
            ["doctor"],
            doctor_check(
                contains=(
                    "ANTITHESIS_API_KEY is not set",
                    "ANTITHESIS_USERNAME",
                    "legacy",
                    "snouty launch",
                    "ask Antithesis support",
                ),
            ),
            json_capable=False,
            env=_doctor_env(api_key=False, username=True, password=True, tenant=True, repo=True),
        ),
        Story(
            "doctor-json",
            "Gate CI on a ready environment with --json",
            "I want to check my environment in a script/CI step and parse the result, "
            "not scrape human text.",
            "`doctor --json` prints a structured report — a top-level `ok` boolean and a `checks` "
            "array, each with name/status/message and any notes — and exits non-zero when a required "
            "check fails, so CI can gate on it.",
            ["doctor", "--json"],
            doctor_json_check,
            json_capable=False,
            env=_doctor_env(
                api_key=False, username=False, password=False, tenant=False, repo=False
            ),
        ),
    ]
    return stories + build_help_stories(d)


# ---------------------------------------------------------------------------
# Help stories: render each command's `--help` next to its default output, with
# rubrics that ask whether the help is informative, clear, concise, consistent,
# and aligned with what the command actually prints. Commands that mutate state
# (launch, debug, validate, update) or need an interactive arg (completions) are
# help-only — `args=[]` so nothing is executed.
# ---------------------------------------------------------------------------


# How a reviewer should judge every help story (shared rubric, kept in one place
# so the bar is consistent across commands).
HELP_RUBRIC = (
    "The `--help` should be **informative** (says what the command does and, for "
    "read commands, how to read the output and the obvious next command), "
    "**clear**, **concise** (no wall of text), and **consistent** with the other "
    "commands' help in tone, layout, and flag ordering. Where default output is "
    "shown, the help must **align** with it: any columns/fields the help names "
    "must actually appear, and nothing in the output should be unexplained."
)

_OUTPUT_RUBRIC = (
    " Compare the help against the default output shown below it."
)
_HELP_ONLY_RUBRIC = (
    " This command mutates state or needs an interactive argument, so only its "
    "help is shown — judge the help text on its own merits and for consistency "
    "with its siblings."
)


def _help_story(
    slug: str,
    title: str,
    goal: str,
    help_cmd: list[str],
    args: list[str] | None = None,
    *,
    samples: list[tuple[str, list[str]]] | None = None,
    align: tuple[str, ...] = (),
    expect_ok: bool = True,
) -> Story:
    args = args or []
    judge = HELP_RUBRIC + (_OUTPUT_RUBRIC if args else _HELP_ONLY_RUBRIC)
    return Story(
        slug=slug,
        title=title,
        goal=goal,
        judge=judge,
        args=args,
        check=help_story_check,
        json_capable=False,
        help_cmd=help_cmd,
        samples=samples,
        align_tokens=align,
        expect_ok=expect_ok,
    )


def build_help_stories(d: Discovery) -> list[Story]:
    s = d.success
    return [
        # -- top level + read commands with default output ------------------
        _help_story(
            "help-root",
            "Discover what snouty can do",
            "I just installed snouty and run `snouty --help` to see what's available.",
            [],
        ),
        _help_story(
            # The parent help is an overview/index of subcommands, not a column
            # legend (it points at `runs list` for the table), so no align tokens.
            "help-runs",
            "Understand the runs command group",
            "I run `snouty runs --help` to learn how to work with test runs.",
            ["runs"],
            ["runs"],
        ),
        _help_story(
            "help-runs-list",
            "Learn to list runs, including the detailed view",
            "I want to know what `runs list` shows and how the columns map to the output, "
            "including the fuller `--detail` view.",
            ["runs", "list"],
            ["runs", "list", "-n", "6"],
            samples=[("with --detail", ["runs", "list", "-n", "3", "--detail"])],
            align=("RUN ID", "STATUS", "CREATED", "TEST NAME"),
        ),
        _help_story(
            "help-runs-show",
            "Learn what `runs show` reports",
            "I want to confirm the help explains the metadata fields and the failure "
            "moment shown for incomplete runs.",
            ["runs", "show"],
            ["runs", "show", s],
            samples=[("an incomplete run (shows the failure moment)", ["runs", "show", d.fail])],
            # show prints a key/value card (prose help vs Title-Case labels), not a
            # columnar table — no strict token alignment; the reviewer compares the
            # prose field list and the failure-moment claim against the two samples.
        ),
        _help_story(
            "help-runs-properties",
            "Learn the properties table, filters, and --detail",
            "I want the help to explain the STATUS/EXAMPLES/NAME columns and the "
            "examples/counterexamples count, and the --name/--group/--detail flags "
            "(including how --detail feeds a moment into `runs logs`).",
            ["runs", "properties"],
            ["runs", "properties", s],
            samples=[
                ("--failing only", ["runs", "properties", s, "--failing"]),
                (
                    "--name <x> --detail (one property's moments)",
                    ["runs", "properties", s, "--name", d.pass_event_prop, "--detail"],
                ),
            ],
            align=("STATUS", "EXAMPLES", "NAME"),
        ),
        _help_story(
            "help-runs-events",
            "Learn to search events and chain into logs",
            "I want the help to explain the HASH/VTIME/SOURCE/OUTPUT columns and that "
            "a moment feeds `runs logs`.",
            ["runs", "events"],
            ["runs", "events", s, "--match", d.event_keyword],
            align=("HASH", "VTIME", "SOURCE", "OUTPUT"),
        ),
        _help_story(
            "help-runs-logs",
            "Learn what the positional moment vs --begin-vtime do",
            "I want the help to make clear that the positional moment streams logs up to "
            "it and --begin-vtime sets the start, and to describe the line format.",
            ["runs", "logs"],
            ["runs", "logs", s, d.event_hash, f"{d.event_vtime}"],
        ),
        _help_story(
            "help-runs-build-logs",
            "Learn what build-logs streams",
            "I want the help to tell me this is the build/setup log and the line format.",
            ["runs", "build-logs"],
            ["runs", "build-logs", s],
        ),
        _help_story(
            "help-doctor",
            "Learn what doctor checks",
            "I run `snouty doctor --help` to see what it verifies, then run it.",
            ["doctor"],
            ["doctor"],
            # `doctor` exits non-zero when a check fails but still prints its
            # findings — that output is exactly what we want to show.
            expect_ok=False,
        ),
        _help_story(
            "help-version",
            "Check the version command's help",
            "I want `version --help` to be a clear, minimal description.",
            ["version"],
            ["version"],
        ),
        # -- help-only (mutating / interactive) -----------------------------
        _help_story(
            "help-launch",
            "Understand how to launch a run",
            "I run `snouty launch --help` to learn how to start a test run.",
            ["launch"],
        ),
        _help_story(
            "help-debug",
            "Understand how to open a debugging session",
            "I run `snouty debug --help` to learn how to debug a moment.",
            ["debug"],
        ),
        _help_story(
            "help-validate",
            "Understand local validation",
            "I run `snouty validate --help` to learn how to validate my config.",
            ["validate"],
        ),
        _help_story(
            "help-completions",
            "Generate shell completions",
            "I run `snouty completions --help` to learn how to install completions.",
            ["completions"],
        ),
        _help_story(
            "help-update",
            "Check for updates",
            "I run `snouty update --help` to understand what updating does.",
            ["update"],
        ),
        # -- docs (help-only: output depends on a downloaded docs DB) --------
        _help_story(
            "help-docs",
            "Understand the docs command group",
            "I run `snouty docs --help` to see how to search the documentation.",
            ["docs"],
        ),
        _help_story(
            "help-docs-search",
            "Learn to search the docs",
            "I run `snouty docs search --help` to learn the search syntax and output.",
            ["docs", "search"],
        ),
        _help_story(
            "help-docs-tree",
            "Learn to browse the docs tree",
            "I run `snouty docs tree --help` to learn how to browse documentation paths.",
            ["docs", "tree"],
        ),
        _help_story(
            "help-docs-show",
            "Learn to show a docs page",
            "I run `snouty docs show --help` to learn how to read a page.",
            ["docs", "show"],
        ),
        _help_story(
            "help-docs-sqlite",
            "Locate the docs database",
            "I run `snouty docs sqlite --help` to find the cached documentation DB.",
            ["docs", "sqlite"],
        ),
    ]


# ---------------------------------------------------------------------------
# Rendering + main
# ---------------------------------------------------------------------------


# Default-output samples in a help story are capped — the point there is to see
# the *shape* of the output next to the help, not the full stream. The
# goal-based stories (above) still capture full, untruncated output.
HELP_SAMPLE_MAX_LINES = 18


def _shell_block(args: list[str], text: str, returncode: int, cap: int | None = None) -> str:
    """A ```shell block showing `$ snouty <args>`, its output, and the exit code
    on the line below — so a reviewer can judge whether the return code makes
    sense given the output (e.g. a clean error should be non-zero; a healthy
    listing should be zero). When `cap` is set the output is truncated to that
    many lines with a marker (help-story samples cap; full goal output does not)."""
    lines = text.rstrip("\n").split("\n")
    if cap is not None and len(lines) > cap:
        hidden = len(lines) - cap
        lines = lines[:cap] + [f"… ({hidden} more lines)"]
    body = "\n".join(lines)
    return f"```shell\n$ snouty {' '.join(args)}\n{body}\n```\nExit code: `{returncode}`"


def _write_help_story(out_dir: Path, story: Story, sr: StoryRun, verdict: str, detail: str) -> None:
    help_text = (sr.help_result.combined if sr.help_result else "").rstrip("\n")
    help_rc = sr.help_result.returncode if sr.help_result else 0
    parts = [
        f"# {story.title}",
        f"**User goal:** {story.goal}",
        f"**Judge satisfaction by:** {story.judge}",
        "## Help text",
        _shell_block([*(story.help_cmd or []), "--help"], help_text, help_rc),
    ]
    if story.args:
        parts.append("## Default output")
        parts.append(
            _shell_block(
                story.args, sr.result.combined, sr.result.returncode, cap=HELP_SAMPLE_MAX_LINES
            )
        )
        for label, res in sr.sample_results or []:
            parts.append(f"### Variant: {label}")
            parts.append(_shell_block(res.args, res.combined, res.returncode, cap=HELP_SAMPLE_MAX_LINES))
    parts.append(f"_Automated check: {verdict} — {detail}_")
    (out_dir / f"{story.slug}.md").write_text("\n\n".join(parts) + "\n")


def write_story(out_dir: Path, story: Story, sr: StoryRun, passed: bool, detail: str) -> None:
    verdict = "PASS" if passed else "FAIL"
    if story.help_cmd is not None:
        _write_help_story(out_dir, story, sr, verdict, detail)
        return
    md = (
        f"# {story.title}\n\n"
        f"**User goal:** {story.goal}\n\n"
        f"**Judge satisfaction by:** {story.judge}\n\n"
        f"{_shell_block(story.args, sr.result.combined, sr.result.returncode)}\n\n"
        f"_Automated check: {verdict} — {detail}_\n"
    )
    (out_dir / f"{story.slug}.md").write_text(md)


def run_story(sn: Snouty, story: Story) -> StoryRun:
    # Help-only stories pass no `args`; don't invoke a bare `snouty`.
    result = sn.run(story.args, story.env) if story.args else Result([], "", "", 0)
    rows = None
    if story.json_capable and story.args:
        try:
            rows = sn.json_lines(story.args)
        except (GalleryError, json.JSONDecodeError):
            rows = None  # error stories are validated on rendered text instead

    help_result = None
    sample_results = None
    if story.help_cmd is not None:
        help_result = sn.run([*story.help_cmd, "--help"])
        if story.samples:
            sample_results = [(label, sn.run(a)) for label, a in story.samples]
    return StoryRun(story, result, rows, help_result, sample_results)


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
        # An all-default Discovery is enough to enumerate slugs (build_stories
        # only reads a few fields, and event_vtime defaults to a real float).
        for s in build_stories(Discovery()):
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

        # Capture concurrently (subprocess + API roundtrips dominate), preserving
        # story order in the results list. Checks are then evaluated serially in
        # that order — the only cross-story dependency, event_multi_match reading
        # reg.row_counts["runs-events-single"], is satisfied by ordered evaluation.
        print(f"capturing {len(stories)} stories…", file=sys.stderr)
        with ThreadPoolExecutor(max_workers=CAPTURE_WORKERS) as pool:
            captured = list(pool.map(lambda s: run_story(sn, s), stories))

        reg = Registry()
        for sr in captured:
            story = sr.story
            if sr.rows is not None:
                reg.row_counts[story.slug] = len(sr.rows)
            passed, detail = story.check(sr, reg)
            write_story(out_dir, story, sr, passed, detail)
            mark = "ok  " if passed else "FAIL"
            print(f"  {mark} {story.slug:<32} {detail}", file=sys.stderr)
            if not passed:
                failures.append((story.slug, detail))
                if args.fail_fast:
                    # Everything was already captured; just stop reporting here.
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
