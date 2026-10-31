#!/usr/bin/env -S uv run
"""Regenerate the snouty "gallery" against fresh, live run IDs.

The gallery is a set of Markdown files, one per "story". Each story models a
hypothetical user with a concrete *goal*, runs the snouty command that user
would run, captures the output, and records a plain-language *rubric* for how a
human or LLM reviewer should judge whether the output satisfied that goal. Every
story is also gated by a programmatic check; a degenerate example (an empty
table, an unintended error, a filter that didn't narrow) fails the run rather
than being silently emitted.

Text output, including help and isolated configuration stories, runs in a
120-column, 40-row terminal. Markdown contains the terminal's text and full
scrollback; adjacent asciinema recordings preserve colors and redraws. JSON
commands use pipes so their structured output stays separate from stderr. A
story that models a log file uses pipes too, with stderr in the stdout pipe.

There are three kinds of story:

  * goal stories — capture one command's output and judge it against a user goal
    (the bulk of the gallery; slugs like `runs-events-single`). The run stories
    test snouty end to end over the life of one real run: `validate-example`
    validates the example project in examples/antithesis, `launch` starts a
    15-minute run of it, and `runs-stream-run` (and `--json`) follow the run's
    property stream from the launch to the end. Once the run is ready, the
    `runs exec` stories (`runs-exec*`) execute in it, and the rewarm stories
    load shallow moments of another completed run into it. Once it is final,
    the result stories (`runs-wait-run`, `runs-properties-run`, …) read its
    results. The gallery reuses a live run that an earlier gallery launched,
    or launches one. The run stories take about 25 minutes. `--no-run` leaves
    them out. When the gallery fails after it launched a run, it cancels the
    run.
  * help stories — capture `snouty <cmd> --help` next to that command's default
    output (slugs like `help-runs-properties`) and judge whether the help is
    informative, clear, concise, consistent, and *aligned* with what the command
    prints. Commands that mutate state or need an interactive arg (launch,
    debug, validate, update, completions) are help-only. An automated check
    verifies any column/field the help names actually appears in the output.
  * MCP tool help stories — start `snouty mcp`, read one tool's entry in the
    tool list, and call `tool_help` for it (slugs like `mcp-help-runs-logs`),
    to judge whether an AI agent could call the tool from that text alone.
    The tools come from the server's own tool list. They need no API.
  * TTY stories — drive an interactive command (today only `snouty login`) on a
    real pseudo-terminal and record the conversation: one *frame* per prompt,
    an asciinema recording to replay, and the files the command persisted. See
    "TTY stories" below.

Credentials come from the usual ANTITHESIS_* environment variables (snouty reads
them). Behaviour is controlled with flags, not env vars:

    uv run scripts/gen-gallery.py --out ./out
    uv run scripts/gen-gallery.py --only runs-events-single
    uv run scripts/gen-gallery.py --no-run
    uv run scripts/gen-gallery.py --list

Nothing is written to ./gallery; output goes to a tempdir (or --out) so you
never accidentally commit it. Diff successive runs (or against ./gallery) to see
how snouty changes affect command output.
"""

from __future__ import annotations

import argparse
import codecs
import fnmatch
import functools
import json
import os
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import islice
from pathlib import Path
from typing import TypeVar

import pexpect
import pyte

# A syntactically-valid but nonexistent run id, for clean-error stories.
UNKNOWN_RUN = "ffffffffffffffffffffffffffffffff-54-5"
# A run ID that only the `debug-bad-duration` story names, so that the guard
# does not count another story's refused launch against it.
DURATION_RUN = "debug-bad-duration-run"
# A config image that only the `launch-bare-setup-timeout` story names, so
# that the guard counts only that story's refused launch against it.
SETUP_TIMEOUT_IMAGE = "launch-bare-setup-timeout:latest"
# An input hash that no run reaches, for the `snouty debug` error stories.
UNKNOWN_HASH = "1234567890123456789"
# A property ID that no run has, for the property lookup error story.
UNKNOWN_PROPERTY = "ffffffffffffffffffffffffffffffff"

# Stories are captured concurrently (each is one or two snouty subprocesses that
# can each block on snouty's 60s timeout); checks are then evaluated serially.
CAPTURE_WORKERS = 6
# The MCP protocol version that the gallery's client asks for, and how long one
# `snouty mcp` session may take.
MCP_PROTOCOL_VERSION = "2025-06-18"
MCP_TIMEOUT = 60

# Committed `snouty validate` sample projects (their goal is documented in the
# comment atop each docker-compose.yaml). The validate stories run `snouty
# validate` against these.
SAMPLES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "validate"

# The live samples bake their test commands into images; `snouty validate`
# never builds or pulls, so the images must exist first. This script builds
# them (and pulls the glibc base) and is run before the live validate stories.
BUILD_SAMPLES_SCRIPT = Path(__file__).resolve().parent / "build-validate-samples.sh"

# The events/search needle comes from the logs at a property's moment, so the
# stories search for text the run really printed. A needle must match at least
# NEEDLE_MIN_MATCHES events, so the `-n 3` story reaches its limit and the
# renderer aligns several rows. NEEDLE_PROBES bounds the searches discovery runs
# per run, and MOMENT_PROBES bounds the moments it tries per property.
NEEDLE_MIN_MATCHES = 4
NEEDLE_PROBES = 12
MOMENT_PROBES = 4

# Discovery reads `runs list` in pages of DISCOVERY_PAGE_SIZE runs, the maximum
# `limit` that the API accepts for one page. It reads at most
# DISCOVERY_MAX_PAGES pages before it stops and reports that no run is usable.
DISCOVERY_PAGE_SIZE = 100
DISCOVERY_MAX_PAGES = 10

# `runs list` reports this launcher for a debugging session.
DEBUGGING_LAUNCHER = "debugging"

# A run that is `starting` or `in_progress` is live.
RUN_IN_PROGRESS = "in_progress"


class GalleryError(Exception):
    """A precondition could not be met; refuse to emit a partial gallery."""


# ---------------------------------------------------------------------------
# snouty runner
# ---------------------------------------------------------------------------


def _ndjson(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _moment_strs(moment: dict) -> tuple[str, str]:
    """A moment as snouty's positional `<input_hash> <vtime>` arguments."""
    return str(moment["input_hash"]), str(moment["vtime"])


@dataclass
class Result:
    args: list[str]
    stdout: str
    stderr: str
    returncode: int
    cast: str | None = None
    stdin: str | None = None

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def combined(self) -> str:
        # Terminal captures already merge both streams in stdout in display
        # order. JSON captures keep stderr separate from the structured data.
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
        # The snouty children that are running, so that `kill_children` can
        # end the long ones, such as a `runs stream` in the background.
        self._children: set[pexpect.spawn | subprocess.Popen[str]] = set()
        self._children_lock = threading.Lock()

    def cleanup(self) -> None:
        shutil.rmtree(self._shim, ignore_errors=True)

    def kill_children(self) -> None:
        """Kill every snouty child that is still running. Each `run` that
        waits on a killed child returns the output that arrived before."""
        with self._children_lock:
            children = list(self._children)
        for child in children:
            if isinstance(child, subprocess.Popen):
                child.kill()
            else:
                child.terminate(force=True)

    def _track(self, child: pexpect.spawn | subprocess.Popen[str], live: bool) -> None:
        with self._children_lock:
            if live:
                self._children.add(child)
            else:
                self._children.discard(child)

    def env_with(self, overrides: dict[str, str | None]) -> dict[str, str]:
        """The environment a snouty child inherits, with `overrides` applied: a
        string sets a var, None removes it (so a story can model an environment
        missing some credential). Everything else inherits the live
        ANTITHESIS_* env."""
        env = dict(self._env)
        for key, value in overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        return env

    def run(
        self,
        args: list[str],
        env: dict[str, str | None] | None = None,
        stdin: str | None = None,
        pipe: bool = False,
        timeout: float | None = None,
        cwd: Path | None = None,
    ) -> Result:
        """Run snouty on a terminal, or on pipes for `--json` or with `pipe`.
        A `pipe` run without `--json` sends stderr into the stdout pipe, so the
        lines keep the order that a log file would show. When `timeout`
        seconds elapse, the method kills snouty and returns the output that
        arrived before."""
        if "--json" not in args and not pipe:
            screen = pyte.HistoryScreen(TTY_COLS, TTY_ROWS, history=sys.maxsize)
            # A shell pipes `stdin` to snouty, as a heredoc does, so stdin is a
            # pipe while stdout and stderr stay on the terminal.
            program, argv = self.binary, args
            if stdin is not None:
                feed = 'stdin=$1; shift; printf %s "$stdin" | "$@"'
                program, argv = (
                    Path("/bin/sh"),
                    ["-c", feed, "sh", stdin, str(self.binary), *args],
                )
            session = TtySession(
                program,
                argv,
                self.env_with({**(env or {}), "TERM": "xterm-256color"}),
                screen=screen,
                cwd=cwd,
            )
            self._track(session.child, live=True)
            try:
                returncode = session.finish(timeout=timeout)
            finally:
                self._track(session.child, live=False)
            return Result(
                args,
                _terminal_transcript(screen),
                "",
                returncode,
                session.cast(_command_line(args)),
                stdin=stdin,
            )
        proc = subprocess.Popen(
            [str(self.binary), *args],
            stdin=subprocess.PIPE if stdin is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE if "--json" in args else subprocess.STDOUT,
            text=True,
            env=self._env if env is None else self.env_with(env),
            cwd=cwd,
        )
        self._track(proc, live=True)
        try:
            try:
                stdout, stderr = proc.communicate(stdin, timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
        finally:
            self._track(proc, live=False)
        return Result(args, stdout, stderr or "", proc.returncode, stdin=stdin)

    def mcp(self, requests: list[tuple[str, dict]]) -> list[dict]:
        """Start `snouty mcp`, initialize a session, send each `(method,
        params)` request, and return each response's `result`, in order. The
        server stops when its stdin closes."""
        env = self.env_with({"SNOUTY_UNSTABLE_FEATURES": "mcp"})
        messages = [
            {
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "gen-gallery", "version": "1"},
                },
            },
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
        ]
        for id_, (method, params) in enumerate(requests, 1):
            messages.append(
                {"jsonrpc": "2.0", "id": id_, "method": method, "params": params}
            )
        # The server answers each request before stdin closes, as rmcp handles
        # each one as it reads it, and the requests here take no time.
        proc = subprocess.run(
            [str(self.binary), "mcp"],
            input="".join(json.dumps(m) + "\n" for m in messages),
            capture_output=True,
            text=True,
            env=env,
            timeout=MCP_TIMEOUT,
        )
        responses = {}
        for line in proc.stdout.splitlines():
            message = json.loads(line)
            if "id" in message:
                responses[message["id"]] = message
        results = []
        for id_, (method, _) in enumerate(requests, 1):
            response = responses.get(id_)
            if response is None or "result" not in response:
                raise GalleryError(
                    f"`snouty mcp` gave no result for {method}: {response!r}\n"
                    f"stderr: {proc.stderr.strip()}"
                )
            results.append(response["result"])
        return results

    def json_lines(
        self, args: list[str], env: dict[str, str | None] | None = None
    ) -> list[dict]:
        """Run `snouty --json <args>` and parse NDJSON rows.

        A non-zero exit is a hard error (timeout, 5xx, auth, DNS, ...) and raises
        — discovery distinguishes this from an empty-but-healthy result (exit 0,
        no rows). This is exact, unlike grepping stderr for known phrases."""
        res = self.run(["--json", *args], env)
        if not res.ok:
            raise GalleryError(
                f"`snouty {' '.join(args)}` failed (exit {res.returncode}): "
                f"{res.stderr.strip() or '<no stderr>'}"
            )
        return _ndjson(res.stdout)

    def json_obj(self, args: list[str]) -> dict:
        res = self.run(["--json", *args])
        if not res.ok:
            raise GalleryError(
                f"`snouty {' '.join(args)}` failed (exit {res.returncode}): "
                f"{res.stderr.strip() or '<no stderr>'}"
            )
        return json.loads(res.stdout)


# The only paths that the launch guard forwards: the runs endpoints, which
# the checks before a launch read, and the version probe. Every launch
# endpoint is under /api/v1/launch/, so the guard refuses it.
_GUARD_RUNS_PREFIX = "/api/v0/runs/"
_GUARD_VERSION_PATH = "/api/version"

# Headers that apply to one connection, which the guard does not forward.
_HOP_HEADERS = frozenset(
    {"connection", "content-length", "host", "keep-alive", "transfer-encoding"}
)

# A base URL where nothing listens, for a guarded story that is only listed.
_NO_SERVER_URL = "http://127.0.0.1:9"


class LaunchGuard:
    """A local HTTP proxy in front of the tenant that refuses every launch.

    The `snouty debug` error stories must fail before the launch, but if that
    check regresses, the launch starts a billable session. These stories set
    ANTITHESIS_BASE_URL to the guard. The guard forwards only the runs
    endpoints and the version probe to the tenant. It answers every other
    request, a launch included, with 403 and records the request line and body
    in `refused`, so a regression fails the story and starts no session."""

    def __init__(self, upstream: str, https_proxy: str | None) -> None:
        self.upstream = upstream.rstrip("/")
        self.refused: list[tuple[str, str]] = []
        proxies = {"https": https_proxy, "http": https_proxy} if https_proxy else {}
        opener = urllib.request.build_opener(urllib.request.ProxyHandler(proxies))
        guard = self

        class Handler(BaseHTTPRequestHandler):
            def forward(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                path = urllib.parse.urlsplit(self.path).path
                runs = path.startswith(_GUARD_RUNS_PREFIX) and ".." not in path
                if not (runs or path == _GUARD_VERSION_PATH):
                    text = body.decode(errors="replace")
                    guard.refused.append((f"{self.command} {self.path}", text))
                    self.reply(
                        403,
                        {},
                        b'{"error": "the gallery launch guard refuses this path"}',
                    )
                    return
                request = urllib.request.Request(
                    guard.upstream + self.path,
                    data=body or None,
                    headers={
                        k: v
                        for k, v in self.headers.items()
                        if k.lower() not in _HOP_HEADERS
                    },
                    method=self.command,
                )
                try:
                    with opener.open(request, timeout=60) as resp:
                        self.reply(resp.status, dict(resp.headers.items()), resp.read())
                except urllib.error.HTTPError as e:
                    self.reply(e.code, dict(e.headers.items()), e.read())

            def reply(self, status: int, headers: dict[str, str], body: bytes) -> None:
                self.send_response(status)
                for k, v in headers.items():
                    if k.lower() not in _HOP_HEADERS:
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = forward

            def log_message(self, format: str, *args) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    @classmethod
    def for_tenant(cls, sn: Snouty) -> LaunchGuard:
        """A guard in front of the base URL that snouty's settings name."""
        shown = sn.run(["--json", "settings", "show"])
        settings = json.loads(shown.stdout).get("settings") or {}
        upstream = settings.get("base_url")
        if not upstream:
            raise GalleryError("no tenant or ANTITHESIS_BASE_URL is set")
        return cls(upstream, settings.get("https_proxy"))

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


# ---------------------------------------------------------------------------
# TTY driver: run an interactive command on a real pseudo-terminal.
#
# `snouty login` prompts through `inquire`, which engages only when stdin is a
# terminal — piping answers at it takes the non-interactive path instead. A TTY
# story therefore spawns snouty on a pseudo-terminal and types at it.
#
# What the terminal receives is not readable as it stands: `inquire` redraws the
# whole prompt on every keystroke, in ANSI escapes. `pyte` replays that stream
# the way a terminal would, so a *frame* is the screen exactly as it stood at
# one moment. The same stream is written out as an asciinema v2 recording, which
# `asciinema play` replays at its original speed.
# ---------------------------------------------------------------------------


# One step of a dialogue: text to wait for on screen, then the keys to type once
# it is there. Every send is gated on its prompt being rendered, so no keystroke
# can race ahead of the prompt meant to read it.
Step = tuple[str, str]

# One frame: the prompt that was waiting, and the screen at that moment.
Frame = tuple[str, str]

# Keys a step can send. `inquire` holds the terminal in raw mode for a prompt's
# whole lifetime, so these arrive as key events rather than line-edited text.
ENTER = "\r"
ESC = "\x1b"

# The pseudo-terminal's size. 120 columns keeps snouty's own lines — which carry
# absolute paths under the throwaway `$HOME` — clear of the wrap boundary, so a
# reviewer judges snouty's wording rather than the sandbox's path length. 40
# rows fits a whole login on one screen.
TTY_COLS = 120
TTY_ROWS = 40

# How long to wait for one prompt. Generous: a healthy exchange completes in
# milliseconds, but the first credential prompt waits on snouty probing the
# tenant for its OAuth configuration.
PROMPT_TIMEOUT = 30

# How long the output must stay quiet before a frame is taken. A prompt matches
# mid-stream, so without this the frame would catch a half-drawn screen.
SETTLE = 0.2


class _Recorder:
    """Sink for everything the child writes.

    `pexpect` hands its `logfile_read` every byte it reads, once and in order —
    unlike `before`/`after`, which repeat whatever is still in its buffer. Each
    chunk is timestamped for the recording and fed to the terminal emulator for
    the frames.

    Both outputs decode incrementally, because a read can split a multi-byte
    character: `pyte.ByteStream` carries the leftover bytes into the next chunk,
    and the recording's own decoder does the same. Decoding each chunk on its own
    would turn a split `↑` into replacement characters in the recording while the
    frames still rendered it correctly."""

    def __init__(self, screen: pyte.Screen):
        self.started = time.monotonic()
        self.events: list[tuple[float, str]] = []
        self.stream = pyte.ByteStream(screen)
        self._decoder = codecs.getincrementaldecoder("utf-8")("replace")

    def write(self, data: bytes) -> None:
        if not data:
            return
        self.stream.feed(data)
        text = self._decoder.decode(data)
        # A chunk that held only the first bytes of a split character decodes to
        # nothing; its bytes arrive with the next one.
        if text:
            self.events.append((time.monotonic() - self.started, text))

    def flush(self) -> None:
        pass


class TtySession:
    """One snouty run on a pseudo-terminal.

    Holds the child, the bytes it has written (kept whole, for the recording),
    and a terminal emulator fed the same bytes (for the frames)."""

    def __init__(
        self,
        binary: Path,
        args: list[str],
        env: dict[str, str],
        *,
        screen: pyte.Screen | None = None,
        cwd: Path | None = None,
    ):
        self.screen = screen if screen is not None else pyte.Screen(TTY_COLS, TTY_ROWS)
        self.recorder = _Recorder(self.screen)
        # `echo=False` stops the terminal driver echoing what we type, so a
        # secret can only reach the screen if snouty itself renders it — which
        # is exactly what the `secrets_absent` check asserts.
        self.child = pexpect.spawn(
            str(binary),
            args,
            env=env,
            cwd=cwd,
            dimensions=(TTY_ROWS, TTY_COLS),
            timeout=PROMPT_TIMEOUT,
            echo=False,
            encoding=None,
        )
        # `logfile_read` is an attribute rather than a constructor argument.
        # Setting it here still catches every byte: pexpect reads the child only
        # when asked to, and nothing has asked yet.
        self.child.logfile_read = self.recorder

    def wait_for(self, needle: str) -> None:
        """Consume output through `needle`, then let the prompt finish drawing."""
        self.child.expect_exact(needle.encode())
        self.settle()

    def settle(self) -> None:
        """Wait until the output has stayed quiet for `SETTLE`, so a frame
        catches a settled screen rather than a half-drawn one. A prompt matches
        mid-stream, and `inquire` draws a menu's rows after its question.
        Matching on TIMEOUT is how pexpect waits for silence."""
        self.child.expect([pexpect.TIMEOUT, pexpect.EOF], timeout=SETTLE)

    def send(self, keys: str) -> None:
        self.child.send(keys.encode())

    def frame(self) -> str:
        """The screen as it stands, trailing blank rows dropped."""
        rows = [row.rstrip() for row in self.screen.display]
        while rows and not rows[-1]:
            rows.pop()
        return "\n".join(rows)

    def finish(self, timeout: float | None) -> int:
        """Wait for the child to exit and return its status. When `timeout`
        seconds elapse, kill the child. An interactive dialogue uses a short
        timeout, because an unanswered prompt does not end by itself."""
        try:
            self.child.expect(pexpect.EOF, timeout=timeout)
        except pexpect.TIMEOUT:
            # The child is still up: a dialogue stalled or a long command ran
            # out of time. The forced close below ends it.
            pass
        finally:
            self.child.close(force=True)
        if self.child.exitstatus is not None:
            return self.child.exitstatus
        return -self.child.signalstatus if self.child.signalstatus is not None else 1

    def cast(self, command: str) -> str:
        """The session as an asciinema v2 recording: a header line, then one
        `[time, "o", data]` line per chunk the terminal received."""
        header = {
            "version": 2,
            "width": TTY_COLS,
            "height": TTY_ROWS,
            "timestamp": int(time.time()),
            "command": command,
            "env": {"TERM": "xterm-256color"},
        }
        lines = [json.dumps(header)]
        for at, text in self.recorder.events:
            lines.append(json.dumps([round(at, 6), "o", text]))
        return "\n".join(lines) + "\n"


def _terminal_transcript(screen: pyte.HistoryScreen) -> str:
    """Keep scrollback as well as the final screen. Screen.display handles wide
    and combining characters; joining character cells directly would not."""
    lines = [*screen.history.top, *(screen.buffer[y] for y in range(screen.lines))]
    transcript = pyte.Screen(screen.columns, len(lines))
    transcript.buffer.update(enumerate(lines))
    return "\n".join(row.rstrip() for row in transcript.display).rstrip("\n")


def drive_tty(
    binary: Path,
    args: list[str],
    env: dict[str, str],
    dialogue: tuple[Step, ...],
) -> tuple[Result, list[Frame], str]:
    """Run `snouty <args>` on a pseudo-terminal, typing `dialogue` at it.

    Returns the run's result (its output is the final screen), one frame per
    step — the screen the user faced when that prompt asked for an answer — plus
    a closing frame, and the asciinema recording.

    A step that cannot be taken — its prompt never arrives, or the screen does
    not offer what the step meant to choose — is reported in the frames and as a
    failing exit, so it shows up in that one story rather than aborting the whole
    gallery."""
    session = TtySession(binary, args, env)
    frames: list[Frame] = []
    completed = True
    for prompt, keys in dialogue:
        try:
            session.wait_for(prompt)
        except (pexpect.TIMEOUT, pexpect.EOF) as e:
            stalled = "no output" if isinstance(e, pexpect.TIMEOUT) else "snouty exited"
            frames.append(
                (f"[gallery] waiting for {prompt!r}: {stalled}", session.frame())
            )
            completed = False
            break
        frames.append((prompt, session.frame()))
        try:
            session.send(keys)
        except GalleryError as e:
            frames.append((f"[gallery] answering {prompt!r}: {e}", session.frame()))
            completed = False
            break
    # A story that broke off leaves snouty waiting at a prompt, so give it only
    # long enough to be sure rather than the whole prompt budget.
    returncode = session.finish(PROMPT_TIMEOUT if completed else SETTLE)
    # The child has exited, so nothing can feed the emulator from here: one
    # closing screen serves as both the last frame and the story's output.
    closing = session.frame()
    frames.append(("[after it exits]", closing))
    return (
        Result(args, closing, "", returncode),
        frames,
        session.cast(_command_line(args)),
    )


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
    # A property example's moment that has logs, for the logs stories.
    logs_hash: str = ""
    # The exact text of the JSON number, so positional moment arguments reach
    # the API byte-identically.
    logs_vtime: str = "0"
    fail_prop: str = ""  # failing event property whose detail shows counter-examples
    fail_prop_id: str = ""  # fail_prop's ID, for the lookup-by-ID story
    pass_event_prop: str = ""  # passing event property whose detail shows examples
    nonevent_prop: str = ""  # non-event property whose detail shows a real value
    name_filter: str = ""  # substring matching exactly one property name
    fail_hash: str = ""
    fail_vtime: str = ""
    fail_event_kw: str = ""  # needle the incomplete run's events story matches on


class RunScan:
    """The runs that `runs list <filters>` returns, newest first, without
    debugging sessions, which have no properties or build logs of their own.

    `runs list` has no cursor option, so each page asks for runs created before
    the oldest run of the previous page (`--created-before`). That bound
    includes the oldest run itself, so the scan removes runs that it already
    returned. The scan reads a page only when the caller needs more runs, and
    it reads at most DISCOVERY_MAX_PAGES pages. `seen` counts the test runs
    that the scan returned, and `pages` counts the pages that it read."""

    def __init__(self, sn: Snouty, *filters: str) -> None:
        self.sn = sn
        self.filters = filters
        self.seen = 0
        self.pages = 0

    def __iter__(self) -> Iterator[dict]:
        returned: set[str] = set()
        before: str | None = None
        while self.pages < DISCOVERY_MAX_PAGES:
            args = ["runs", "list", *self.filters, "-n", str(DISCOVERY_PAGE_SIZE)]
            if before is not None:
                args += ["--created-before", before]
            rows = self.sn.json_lines(args)
            self.pages += 1
            new = [r for r in rows if r["run_id"] not in returned]
            if not new:
                return
            for r in new:
                returned.add(r["run_id"])
                if r.get("launcher") == DEBUGGING_LAUNCHER:
                    continue
                self.seen += 1
                yield r
            if len(rows) < DISCOVERY_PAGE_SIZE:
                return
            before = rows[-1]["created_at"]


def _no_usable_run(what: str, scan: RunScan, stories: str, need: str) -> GalleryError:
    """A loud error for a scan that found no usable run. `what` is the run
    status, `stories` names the stories that need the run, and `need` is the
    predicate that a usable run satisfies ("has ..."). The error also says how
    to fix the tenant."""
    rule = "!" * 78
    pages = f"{scan.pages} of at most {DISCOVERY_MAX_PAGES} pages"
    return GalleryError(
        f"\n{rule}\n"
        f"NO USABLE {what.upper()} RUN.\n"
        f"None of the {scan.seen} most recent {what} runs ({pages}) {need}.\n"
        f"The {stories} stories cannot run, so the gallery is not written.\n"
        f"Start a new {what} run in the tenant (for example, a nightly trigger) and try again.\n"
        f"{rule}"
    )


def _logs(sn: Snouty, args: list[str]) -> list[dict]:
    """The `runs logs` rows for `args`, or none when the endpoint fails."""
    try:
        return sn.json_lines(["runs", "logs", *args])
    except GalleryError:
        return []


def _begin_vtime(vtime: str) -> str:
    """`--begin-vtime` for the logs skip-ahead story: just before the moment. A
    lower bound, so the float round-trip is harmless here."""
    return f"{max(0.0, float(vtime) - 0.5):.3f}"


def _pick_needles(
    sn: Snouty,
    run: str,
    logs: list[dict],
    min_matches: int,
    probed: set[str] | None = None,
) -> tuple[str, str] | None:
    """A needle and a second needle for the events/search stories, taken from
    the text of `logs`. The last lines come first, because they are nearest to
    the moment. The needle must match at least `min_matches` events. The second
    needle comes from the same line, so both needles match at least that event.
    `probed` holds the tokens already searched. Pass the same set for each
    moment of a run, so that a token is searched only once per run. Returns
    None when no needle fits within NEEDLE_PROBES searches."""
    probed = set() if probed is None else probed
    for row in reversed(logs):
        for token in re.findall(r"[A-Za-z_]{5,}", row.get("output_text") or ""):
            low = token.lower()
            if low in probed or low in _UBIQUITOUS_TOKENS:
                continue
            second = _pick_second_needle(row, token)
            if second is None:
                continue
            if len(probed) >= NEEDLE_PROBES:
                return None
            probed.add(low)
            # `runs events` matches more fields, case-insensitively, than the
            # search query, so a needle that the query finds `min_matches`
            # times also passes in `runs events`.
            query = f'contains({{output_text: "{token}"}})'
            try:
                found = sn.json_lines(
                    ["runs", "search", run, query, "-n", str(min_matches)]
                )
            except GalleryError:
                return None
            if len(found) >= min_matches:
                return token, second
    return None


@dataclass
class CompletedPick:
    """A completed run that can drive *every* completed-run story, plus the
    per-story selections derived from it (so discovery doesn't re-derive them)."""

    run: str
    logs_moment: dict  # a property example's moment that has logs
    keyword: str
    keyword2: str
    fail_prop: str
    fail_prop_id: str
    pass_prop: str
    nonevent_prop: str
    name_filter: str


def _pick_logs_moment(
    sn: Snouty, run: str, props: list[dict], fail_prop: str, pass_prop: str
) -> tuple[dict, str, str] | None:
    """A moment for the logs stories, and the needles for the events/search
    stories. The moment comes from the failing property's counter-examples, then
    the passing property's examples (MOMENT_PROBES from each), so the logs
    stories stream a moment that the property detail stories show. Both logs
    stories must return at least one line at the moment, and its logs must give
    a needle (see _pick_needles). All the moments share one NEEDLE_PROBES budget.
    Returns (moment, needle, second needle), or None when no moment fits."""
    by_name = {p["name"]: p for p in props}
    moments = [
        v["moment"]
        for values in (
            by_name[fail_prop].get("counterexamples"),
            by_name[pass_prop].get("examples"),
        )
        for v in _moments(values or [])[:MOMENT_PROBES]
    ]
    probed: set[str] = set()
    for moment in moments:
        if len(probed) >= NEEDLE_PROBES:
            break
        h, v = str(moment["input_hash"]), str(moment["vtime"])
        logs = _logs(sn, [run, h, v])
        begin = ["--begin-vtime", _begin_vtime(v)]
        if not logs or not _logs(sn, [run, h, v, *begin]):
            print(f"  skip moment {h} {v}: no logs", file=sys.stderr)
            continue
        needles = _pick_needles(sn, run, logs, NEEDLE_MIN_MATCHES, probed)
        if needles is None:
            print(f"  skip moment {h} {v}: no needle in its logs", file=sys.stderr)
            continue
        return moment, *needles
    return None


def _pick_completed_run(sn: Snouty) -> CompletedPick:
    """Pick a completed run that can drive *all* the completed-run stories: the
    property stories, the logs stories (see _pick_logs_moment), and the
    events/search stories. Scan recent completed runs and take the first that
    satisfies every requirement at once."""
    runs = RunScan(sn, "--status", "completed")
    last_reason = "no completed run found"
    for r in runs:
        run = r["run_id"]
        # `--name` matches the shown name, which drops the `<group>: ` prefix.
        props = [
            {**p, "name": p["name"].removeprefix(f"{g}: ")}
            if (g := p.get("group"))
            else p
            for p in sn.json_lines(["runs", "properties", run])
        ]
        # Each picker raises GalleryError if this run can't satisfy its story —
        # catch it and move on rather than committing to a run that fails
        # downstream.
        try:
            name_filter = _pick_name_filter(props)
            fail = _pick_property_with_moments(sn, run, props, "Failing")
            fail_prop, fail_prop_id = fail["name"], _pick_property_id(sn, run, fail)
            pass_prop = _pick_property_with_moments(sn, run, props, "Passing")["name"]
            nonevent_prop = _pick_nonevent_property(sn, run, props)
        except GalleryError as e:
            print(f"  skip {run}: {e}", file=sys.stderr)
            last_reason = str(e)
            continue
        picked = _pick_logs_moment(sn, run, props, fail_prop, pass_prop)
        if picked is None:
            last_reason = "no property moment with logs and a needle"
            print(f"  skip {run}: {last_reason}", file=sys.stderr)
            continue
        moment, kw, kw2 = picked
        print(
            f"  completed run : {run} (logs at {moment['input_hash']} "
            f"{moment['vtime']}; needles '{kw}', '{kw2}')",
            file=sys.stderr,
        )
        return CompletedPick(
            run,
            moment,
            kw,
            kw2,
            fail_prop,
            fail_prop_id,
            pass_prop,
            nonevent_prop,
            name_filter,
        )
    raise _no_usable_run(
        "completed",
        runs,
        "completed-run (properties, events, search, and logs)",
        "has failing, passing, and non-event property moments, a "
        "unique name filter, and a property moment with logs and a needle "
        f"(last reason: {last_reason})",
    )


# TODO: drop the missing-field check once the API reports a failure moment for
# every run.
def _real_failure_moment(moment: dict) -> bool:
    """Whether a run's failure moment is a real, streamable coordinate. The API
    reports the sentinel `input_hash "0"`, `vtime 0` for an incomplete run with no
    specific failure point (a timeout or kill, not a moment-pinned failure) and
    omits the fields for some runs; neither yields any logs. snouty emits vtime
    as a JSON number, so the zero check is numeric."""
    h, v = moment.get("input_hash"), moment.get("vtime")
    if not h or v is None:
        return False
    return not (str(h) == "0" and float(v) == 0.0)


def _pick_incomplete_run(sn: Snouty) -> tuple[str, dict, str]:
    """Pick an incomplete run that makes the incomplete-run stories meaningful,
    scanning the recent ones rather than blindly taking the first — which is
    routinely a timeout with a 0/0 failure moment and no error events, leaving the
    logs/events stories empty. The chosen run must have a real failure moment whose
    logs are non-empty (so runs-logs-incomplete streams lines and runs-show-incomplete
    renders a moment) AND a needle from those logs that matches events (for
    runs-events-incomplete). Returns (run_id, failure_moment, event_keyword)."""
    runs = RunScan(sn, "--status", "incomplete")
    for r in runs:
        run = r["run_id"]
        moment = sn.json_obj(["runs", "show", run]).get("failure_moment") or {}
        if not _real_failure_moment(moment):
            print(
                f"  skip {run}: no real failure moment (0/0 sentinel)", file=sys.stderr
            )
            continue
        h, v = str(moment["input_hash"]), str(moment["vtime"])
        logs = _logs(sn, [run, h, v])
        if not logs:
            print(f"  skip {run}: no logs at the failure moment", file=sys.stderr)
            continue
        needles = _pick_needles(sn, run, logs, 1)
        if needles is None:
            print(f"  skip {run}: no needle in the failure logs", file=sys.stderr)
            continue
        kw = needles[0]
        print(
            f"  incomplete run: {run} (logs at failure moment; events match '{kw}')",
            file=sys.stderr,
        )
        return run, moment, kw
    raise _no_usable_run(
        "incomplete",
        runs,
        "incomplete-run (show, events, and logs)",
        "has a real failure moment with logs and a needle",
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
    return sn.run(["runs", "properties", run, "--name", name, "--detail"]).combined


def _matches_only_itself(prop: dict, props: list[dict]) -> bool:
    """Whether `--name <prop's name>` selects only this property. `--name` matches
    a substring, and two groups can hold the same shown name."""
    name = prop["name"].lower()
    return sum(name in p["name"].lower() for p in props) == 1


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


def _pick_property_with_moments(
    sn: Snouty, run: str, props: list[dict], status: str
) -> dict:
    """Pick a property of the given status that has example/counter-example moment
    rows. The properties JSON already carries the full `examples`/`counterexamples`
    arrays snouty renders, so we select straight from it (most moments first) and
    only render-probe the one chosen property as a cheap safety net."""
    arr_key = "counterexamples" if status == "Failing" else "examples"
    candidates = [
        (p, m)
        for p in props
        if p.get("status") == status
        and (m := _moments(p.get(arr_key) or []))
        and _matches_only_itself(p, props)
    ]
    candidates.sort(key=lambda c: len(c[1]), reverse=True)
    for p, _ in candidates:
        if _has_moment_rows(_render_property(sn, run, p["name"])):
            return p
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

# snouty does not show a property's description, so `--detail` has no
# `Details` row.
_DETAILS_ROW = re.compile(r"^\s*Details\b", re.MULTILINE)


def _has_real_value(value) -> bool:
    """Whether a non-event example renders as a usable value — i.e. a scalar or a
    non-empty collection (an empty one renders as the `(no value)` placeholder)."""
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True  # scalars (incl. False/0) all render to a visible block


def _pick_nonevent_property(sn: Snouty, run: str, props: list[dict]) -> str:
    """Pick a non-event property that renders a real example value. We read the
    example arrays straight from the JSON and only render-probe the chosen one."""
    # If `--name` also selects an event property, its moment rows make the probe
    # reject this one.
    candidates = []
    for p in props:
        if p.get("is_event") is not False or not _matches_only_itself(p, props):
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


def _pick_property_id(sn: Snouty, run: str, prop: dict) -> str:
    """`prop`'s ID, once a lookup by that ID succeeds. Runs created before
    release 65.3 reject the lookup."""
    prop_id = prop.get("id")
    if not prop_id:
        raise GalleryError(f"property {prop['name']!r} has no ID")
    if not sn.run(["runs", "property", run, prop_id]).ok:
        raise GalleryError(f"looking up property {prop_id} fails")
    return prop_id


def _pick_name_filter(props: list[dict]) -> str:
    """A case-insensitive word contained in exactly one property name — so the
    `--name` filter story narrows the list to a single, predictable property."""
    lower = [p["name"].lower() for p in props]
    for p in props:
        for word in re.findall(r"[A-Za-z]{4,}", p["name"]):
            w = word.lower()
            if sum(w in n for n in lower) == 1:
                return word
    raise GalleryError("no substring matches exactly one property")


def discover(sn: Snouty) -> Discovery:
    print("discovering runs via the live API…", file=sys.stderr)

    pick = _pick_completed_run(sn)
    success, moment = pick.run, pick.logs_moment

    fail, fail_moment, fail_event_kw = _pick_incomplete_run(sn)
    cancelled_scan = RunScan(sn, "--status", "cancelled")
    cancelled = next((r["run_id"] for r in cancelled_scan), None)
    if not cancelled:
        raise _no_usable_run("cancelled", cancelled_scan, "cancelled-run", "exists")
    print(f"  cancelled run : {cancelled}", file=sys.stderr)

    # Dynamic listing params from the newest 30 runs, so listing stories aren't
    # empty.
    recent = list(islice(RunScan(sn), 30))
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

    disc = Discovery(
        success=success,
        fail=fail,
        cancelled=cancelled,
        launcher=launcher,
        created_after=created_after,
        window_after=window_after,
        window_before=window_before,
        event_keyword=pick.keyword,
        event_kw2=pick.keyword2,
        logs_hash=str(moment["input_hash"]),
        logs_vtime=str(moment["vtime"]),
        # Property-story selections were derived against `success` during the
        # holistic run pick, so reuse them rather than re-probing the API.
        fail_prop=pick.fail_prop,
        fail_prop_id=pick.fail_prop_id,
        pass_event_prop=pick.pass_prop,
        nonevent_prop=pick.nonevent_prop,
        name_filter=pick.name_filter,
        # _pick_incomplete_run guarantees a real (non-0/0) failure moment.
        fail_hash=str(fail_moment["input_hash"]),
        fail_vtime=str(fail_moment["vtime"]),
        fail_event_kw=fail_event_kw,
    )
    return disc


@dataclass
class GalleryRun:
    """The gallery's own run of the example project, which the run stories
    follow from its launch to its final status, and the moments that the
    `runs exec` stories execute at."""

    run: str = ""
    # The launch, or None when the gallery reused a live run that an earlier
    # gallery launched.
    launch: Result | None = None
    hash: str = ""  # a moment just after the run's setup, which is warm
    vtime: str = "0"
    container: str = ""  # a container name from the run's logs
    # The end moment of the readiness exec, which wrote EXEC_MARKER_TEXT to
    # EXEC_MARKER_PATH.
    follow_hash: str = ""
    follow_vtime: str = "0"
    # Two different shallow moments of `rewarm_source`, a completed run on
    # RUN_TIER: one for the terminal story and one for the pipe story, so that
    # each story loads a cold moment.
    rewarm_source: str = ""
    rewarm_hash: str = ""
    rewarm_vtime: str = "0"
    rewarm_pipe_hash: str = ""
    rewarm_pipe_vtime: str = "0"


@dataclass
class FinalCaptures:
    """What the gallery captures once its run is final."""

    wait: Result
    # The streams that started right after the launch, or None when their
    # story is not selected.
    stream: Result | None = None
    stream_json: Result | None = None
    # The ID of EXAMPLE_BUG_PROPERTY in the run, or "" when it has none.
    bug_prop_id: str = ""


EXAMPLE_DIR = Path(__file__).resolve().parent.parent / "examples" / "antithesis"

# How the gallery launches its run. The launch passes no --source and no tier,
# so the run is ephemeral and gets the tenant's default tier, which must equal
# RUN_TIER. A rewarm source must be on the same tier as the run that loads its
# moment: the API spec says so for the exec endpoint (src/openapi.json).
GALLERY_RUN_LAUNCHER = "basic_test"
# The smallest run the launcher accepts on RUN_TIER. It fuzzes for
# GALLERY_RUN_MINUTES on RUN_TIER's 48 cores, after setup.
GALLERY_RUN_COMPUTE_HOURS = 12
GALLERY_RUN_MINUTES = 15
# The oldest gallery run that a new gallery reuses. A run reports ready a few
# minutes after it starts, and its test then lasts GALLERY_RUN_MINUTES; an
# older run can end before the ready stories run.
REUSE_MAX_AGE = timedelta(minutes=8)
GALLERY_RUN_TEST_NAME = "snouty-gallery"
# The description of each run that the gallery launches starts with this.
GALLERY_RUN_DESCRIPTION = "snouty gen-gallery end-to-end run"
RUN_TIER = "standard"
_TIER = "antithesis.performance_tier"

# The example's property that fails, because of its deliberate bug.
EXAMPLE_BUG_PROPERTY = "Account balances never go negative"

# Bounds on the waits for the gallery run, in minutes, so a run that never
# becomes ready or never ends fails the gallery instead of hanging it.
RUN_READY_MINUTES = 60
RUN_FINAL_MINUTES = 90
RUN_READY_TIMEOUT = f"{RUN_READY_MINUTES}m"
RUN_FINAL_TIMEOUT = f"{RUN_FINAL_MINUTES}m"
# The streams start at the launch and must outlast both waits, and the stories
# that the gallery captures between them, so that a stream is never killed
# while `runs wait` still succeeds.
STREAM_SLACK_MINUTES = 60
STREAM_TIMEOUT = (RUN_READY_MINUTES + RUN_FINAL_MINUTES + STREAM_SLACK_MINUTES) * 60

# Bounds on the polls in `_first_post_setup_moment` and `_container`.
MOMENT_SEARCH_DEADLINE = 600
MOMENT_SEARCH_POLL = 15

# Rewarm sources: the gallery picks one of the newest REWARM_SOURCE_PROBES
# completed runs on RUN_TIER, and one of the SHALLOW_MOMENTS earliest moments
# after its setup. The choice is random, because a reused run can already
# hold a moment that an earlier gallery run loaded.
REWARM_SOURCE_PROBES = 10
SHALLOW_MOMENTS = 6

EXEC_MARKER_PATH = "/tmp/snouty-gallery-marker"
EXEC_MARKER_TEXT = "written by an earlier runs exec"

# Match system events; workload log text is not a reliable marker. A run whose
# workload never calls the SDK's setup-complete API has no setup event, so
# also match two other events that occur just after setup. The fault injector
# unpauses many times in a run, so callers take the earliest matches (see
# `earliest_moments`).
POST_SETUP_QUERY = (
    'filter(ev => ev.antithesis_setup?.status == "complete"'
    ' || ev.composer_lifecycle == "first_randomizer_new_input"'
    ' || (ev.source?.name == "fault_injector" && ev.info?.message == "status"'
    " && ev.info?.details?.paused == false))"
)


def _exec_json(sn: Snouty, x: GalleryRun, script: str) -> Result:
    return sn.run(["--json", "runs", "exec", x.run, x.hash, x.vtime, script])


def _run_description(row: dict) -> str:
    params = row.get("parameters") or {}
    return row.get("description") or params.get("antithesis.description") or ""


def _live_gallery_run(sn: Snouty) -> dict | None:
    """The newest run that the gallery launched and that is in progress on
    RUN_TIER, or None. A run that the gallery did not launch is never reused:
    the stories would execute in it. Only an in-progress run is reused: a
    starting run lists no description and no parameters (orbitinghail,
    release 66.0, about a minute after the launch), so the gallery cannot
    tell that it launched the run, and a rerun soon after a launch launches a
    second run."""
    rows = sn.json_lines(
        [
            "runs",
            "list",
            "--launcher",
            GALLERY_RUN_LAUNCHER,
            "--status",
            RUN_IN_PROGRESS,
            "-n",
            "50",
        ]
    )
    now = datetime.now(UTC)
    for r in rows:
        tier = (r.get("parameters") or {}).get(_TIER)
        if (
            r.get("status") == RUN_IN_PROGRESS
            and tier == RUN_TIER
            and _run_description(r).startswith(GALLERY_RUN_DESCRIPTION)
            and young_enough(r, now)
        ):
            return r
    return None


def young_enough(row: dict, now: datetime) -> bool:
    """Whether the run in `row` started less than REUSE_MAX_AGE before `now`."""
    started = datetime.fromisoformat(row.get("started_at") or row["created_at"])
    return now - started < REUSE_MAX_AGE


def gallery_launch_args(description: str) -> list[str]:
    """The `snouty --json launch` command for the gallery's run."""
    return [
        "--json",
        "launch",
        "--launcher",
        GALLERY_RUN_LAUNCHER,
        "--config",
        str(EXAMPLE_DIR),
        "--max-compute-hours",
        str(GALLERY_RUN_COMPUTE_HOURS),
        "--test-name",
        GALLERY_RUN_TEST_NAME,
        "--description",
        description,
        "--param",
        "attrs.source=snouty-gallery",
    ]


def launched_run_id(stdout: str) -> str | None:
    """The run ID in `snouty --json launch` output, or None."""
    try:
        out = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    run = out.get("runId") if isinstance(out, dict) else None
    return run if isinstance(run, str) and run else None


def gallery_run(sn: Snouty, reuse: bool) -> GalleryRun:
    """The gallery's run. With `reuse`, prefer a live run that the gallery
    launched; otherwise launch one."""
    row = _live_gallery_run(sn) if reuse else None
    if row is not None:
        print(f"  reusing gallery run {row['run_id']}", file=sys.stderr)
        return GalleryRun(run=row["run_id"])
    ensure_example_images()
    # The timestamp helps a person tell the gallery's runs apart in the web UI.
    description = f"{GALLERY_RUN_DESCRIPTION} {datetime.now().isoformat()}"
    launch = sn.run(gallery_launch_args(description))
    run = launched_run_id(launch.stdout)
    if not launch.ok or run is None:
        raise GalleryError(
            f"`snouty launch` failed (exit {launch.returncode}): {launch.combined}"
        )
    print(f"  launched gallery run {run}", file=sys.stderr)
    return GalleryRun(run=run, launch=launch)


def abandon_run(sn: Snouty, run: str) -> None:
    """Cancel `run`, which the gallery launched and can no longer use, and kill
    the snouty children that follow it."""
    cancel = sn.run(["--json", "runs", "cancel", run])
    if cancel.ok:
        print(f"  cancelled gallery run {run}", file=sys.stderr)
    else:
        print(
            f"  cannot cancel gallery run {run} (exit {cancel.returncode}): "
            f"{cancel.stderr.strip() or '<no stderr>'}",
            file=sys.stderr,
        )
    sn.kill_children()


def earliest_moments(rows: list[dict], n: int) -> list[dict]:
    """The `n` earliest distinct moments of the event `rows`, earliest first.
    Search returns a sample in no fixed order."""
    picked: dict[tuple[str, str], dict] = {}
    for m in sorted((r["moment"] for r in rows), key=lambda m: float(m["vtime"])):
        picked.setdefault(_moment_strs(m), m)
    return list(picked.values())[:n]


def _post_setup_moments(sn: Snouty, run: str) -> list[dict]:
    rows = sn.json_lines(["runs", "search", run, POST_SETUP_QUERY, "-n", "100"])
    return earliest_moments(rows, SHALLOW_MOMENTS)


def _first_post_setup_moment(sn: Snouty, run: str) -> dict:
    """The earliest moment after `run` completes its setup. Poll until the
    moment appears."""
    deadline = time.monotonic() + MOMENT_SEARCH_DEADLINE
    args = ["--json", "runs", "search", run, POST_SETUP_QUERY, "-n", "100"]
    while True:
        # Search answers 404 for a minute or two after the run is ready
        # (orbitinghail, release 66.0), and then finds nothing for a while.
        res = sn.run(args)
        moments: list[dict] = []
        if res.ok:
            moments = earliest_moments(_ndjson(res.stdout), SHALLOW_MOMENTS)
        else:
            reason = res.stderr.strip() or "<no stderr>"
            print(
                f"  search in {run} failed (exit {res.returncode}): {reason}",
                file=sys.stderr,
            )
        if moments:
            return moments[0]
        if time.monotonic() > deadline:
            raise GalleryError(
                f"{run} has no post-setup event after {MOMENT_SEARCH_DEADLINE}s (tried "
                "the SDK setup-complete event, the test composer's "
                "first_randomizer_new_input event, and the fault injector's unpause event)"
            )
        print(f"  waiting for a post-setup event in {run}", file=sys.stderr)
        time.sleep(MOMENT_SEARCH_POLL)


def _container(sn: Snouty, run: str, h: str, v: str) -> str:
    """The container that logs most often up to a moment. A one-shot setup
    container can log last and then exit, and `--container` fails on it. The
    logs of an in-progress run can lag its search, so poll until a log line
    names a container."""
    deadline = time.monotonic() + MOMENT_SEARCH_DEADLINE
    while True:
        names = Counter(
            name
            for row in _logs(sn, [run, h, v])
            if (name := (row.get("source") or {}).get("container"))
        )
        if names:
            return names.most_common(1)[0][0]
        if time.monotonic() > deadline:
            raise GalleryError(
                f"no log line of {run} up to {h} {v} names a container after "
                f"{MOMENT_SEARCH_DEADLINE}s"
            )
        print(f"  waiting for the logs of {run} up to {h} {v}", file=sys.stderr)
        time.sleep(MOMENT_SEARCH_POLL)


def _rewarm_moments(sn: Snouty, target: str) -> tuple[str, dict, dict]:
    """A completed run on RUN_TIER other than `target`, and two different
    shallow moments of it: moments just after its setup, which load quickly."""
    candidates = list(
        islice(
            (
                r["run_id"]
                for r in RunScan(sn, "--status", "completed")
                if (r.get("parameters") or {}).get(_TIER) == RUN_TIER
                and r["run_id"] != target
            ),
            REWARM_SOURCE_PROBES,
        )
    )
    random.shuffle(candidates)
    for run in candidates:
        try:
            moments = _post_setup_moments(sn, run)
        except GalleryError as e:
            print(f"  skip rewarm source {run}: {e}", file=sys.stderr)
            continue
        if len(moments) >= 2:
            first, second = random.sample(moments, 2)
            return run, first, second
        print(f"  skip rewarm source {run}: fewer than two moments", file=sys.stderr)
    raise GalleryError(
        f"none of the {len(candidates)} newest completed {RUN_TIER} runs has two "
        "post-setup moments to rewarm"
    )


def prepare_exec(sn: Snouty, x: GalleryRun, rewarm: tuple[str, dict, dict]) -> None:
    """Fill in the moments that the exec stories execute at, with `rewarm`
    from `_rewarm_moments`. The run must be ready."""
    x.hash, x.vtime = _moment_strs(_first_post_setup_moment(sn, x.run))
    # The follow-up story starts at this exec's end moment, so the gallery
    # cannot continue when the exec fails.
    first = _exec_json(sn, x, f'echo "{EXEC_MARKER_TEXT}" > {EXEC_MARKER_PATH}')
    if not first.ok:
        raise GalleryError(f"`runs exec` failed in run {x.run}: {first.stderr.strip()}")
    x.follow_hash, x.follow_vtime = _moment_strs(
        _ndjson(first.stdout)[-1]["end_moment"]
    )
    x.container = _container(sn, x.run, x.hash, x.vtime)
    x.rewarm_source, tty, pipe = rewarm
    x.rewarm_hash, x.rewarm_vtime = _moment_strs(tty)
    x.rewarm_pipe_hash, x.rewarm_pipe_vtime = _moment_strs(pipe)
    print(
        f"  exec moment   : {x.hash} {x.vtime} (container {x.container}; rewarm "
        f"from {x.rewarm_source})",
        file=sys.stderr,
    )


def property_id_named(rows: list[dict], name: str) -> str:
    """The ID of the property whose name contains `name`, or "". A grouped
    property's name can carry a `<group>: ` prefix."""
    return next((r.get("id") or "" for r in rows if name in r.get("name", "")), "")


_T = TypeVar("_T")


def in_background(work: Callable[[], _T]) -> Future[_T]:
    """Run `work` in a daemon thread and return a future of its result. Use
    it for work that can last as long as the run, such as a stream: a daemon
    thread does not keep the gallery alive after it fails."""
    future: Future[_T] = Future()

    def run() -> None:
        try:
            future.set_result(work())
        except BaseException as e:
            future.set_exception(e)

    threading.Thread(target=run, daemon=True).start()
    return future


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
    check: Callable[[StoryRun, Registry], tuple[bool, str]]
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
    # Run with the global config dir isolated to an empty throwaway
    # `$XDG_CONFIG_HOME`. `env` only controls ANTITHESIS_* *env* credentials, but
    # `snouty login` persists credentials to `credentials.toml` / `settings.toml`
    # under `$XDG_CONFIG_HOME/snouty` — those would otherwise leak in and mask a
    # story that models an unconfigured machine (e.g. the no-auth doctor stories).
    isolate_config: bool = False
    stdin: str | None = None
    # -- MCP tool help stories ---------------------------------------------
    # The MCP tool whose tool list entry and `tool_help` the story shows.
    mcp_tool: str | None = None
    # Capture on pipes rather than a terminal, with stderr in the stdout pipe,
    # as a CI log or `2>&1 | tee` shows the output.
    pipe: bool = False
    # A result that discovery captured, because later stories depend on its side
    # effects. The story shows it and runs nothing.
    precaptured: Result | None = None
    # The story starts real containers (a live `snouty validate` sample), as
    # opposed to the static checks that fail before any container starts. All
    # validate stories require a container runtime (see `ensure_validate_runtime`);
    # this flag just documents which ones spin up live containers.
    needs_docker: bool = False
    # -- TTY stories -------------------------------------------------------
    # A story with a `dialogue` is a TTY story: snouty is spawned on a real
    # pseudo-terminal and the dialogue is typed at it, one `(wait for, send)`
    # step per prompt. It runs in a throwaway `$HOME` so the credentials and
    # settings it persists never touch the operator's real config.
    dialogue: tuple[Step, ...] | None = None
    # Files to pre-write into the throwaway HOME before running, keyed by
    # HOME-relative path — models pre-existing state (a prior login, a broken
    # settings file). Mirrors the spec fixtures' txtar `-- path --` sections.
    seed_files: dict[str, str] | None = None
    # HOME-relative files to read back after the run and render under a
    # "Persisted state" section (secrets are redacted before embedding).
    post_capture: tuple[str, ...] = ()


@dataclass
class StoryRun:
    story: Story
    result: Result
    rows: list[dict] | None  # structured rows from the --json variant, if any
    help_result: Result | None = None  # `<help_cmd> --help` capture, for help stories
    sample_results: list[tuple[str, Result]] | None = None  # extra labelled captures
    mcp_entry: dict | None = None  # the tool's entry in the MCP tool list
    # (rel_path, contents|None) for each `post_capture` file, in order; None means
    # the file was not written (which some TTY stories assert on).
    captured_files: list[tuple[str, str | None]] | None = None
    # TTY stories: the screen at each prompt, and the asciinema recording of the
    # whole session.
    frames: list[Frame] | None = None
    cast: str | None = None


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


def _first_line(text: object) -> str:
    if not isinstance(text, str):
        return ""
    # Split as Rust's `str::lines()` does: only on `\n`. `strip()` drops a `\r`.
    return next((line.strip() for line in text.split("\n") if line.strip()), "")


def expected_run_cell(row: dict) -> str:
    """The RUN cell that `runs list` shows for a `--json` row.

    The order is: test name, the first line of the description, the first line
    of the event description that a debugging session sets, `-`.
    """
    params = row.get("parameters") or {}
    candidates = [
        params.get("antithesis.test_name"),
        _first_line(_run_description(row)),
        _first_line(params.get("antithesis.event_description")),
    ]
    return next((c for c in candidates if isinstance(c, str) and c), "-")


def _cell_matches(cell: str, want: str) -> bool:
    """A table cell shows `want`, or a prefix of it cut with `…`."""
    if cell.endswith("…"):
        return want.startswith(cell[:-1])
    return cell == want


def rows_at_most_with_run_cells(limit: int):
    """`rows_at_most`, plus each table row's LAUNCHER and RUN cells are right.

    LAUNCHER is the launcher, and RUN is `expected_run_cell`; either may be
    cut to fit with `…`.
    """

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        lines = sr.result.stdout.splitlines()
        bad: list[str] = []
        for row in rows:
            run_id = row.get("run_id", "")
            line = next((ln for ln in lines if ln.startswith(f"{run_id} ")), None)
            cells = re.split(r" {2,}", line.strip(), maxsplit=4) if line else []
            launcher_cell, cell = cells[3:] if len(cells) == 5 else ("", "")
            launcher = row.get("launcher") or "-"
            if not _cell_matches(launcher_cell, launcher):
                bad.append(f"{run_id}: LAUNCHER {launcher_cell!r} != {launcher!r}")
            want = expected_run_cell(row)
            if not _cell_matches(cell, want):
                bad.append(f"{run_id}: {cell!r} != {want!r}")
        n = len(rows)
        detail = "; ".join(bad) or "LAUNCHER and RUN cells match"
        return (1 <= n <= limit and not bad, f"{n} rows (limit {limit}), {detail}")

    return chk


def rows_at_most_with_limit_note(limit: int):
    """`rows_at_most`, plus the note that names the limit.

    The note claims more results may exist, so it belongs only when the server
    filled the limit. Fewer rows than asked for means the result set is
    exhausted, and the note must be absent.
    """

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        n = len(sr.rows or [])
        noted = f"Showing up to {limit} results." in sr.result.combined
        ok = 1 <= n <= limit and noted == (n == limit)
        return (ok, f"{n} rows (limit {limit}), note={noted}")

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
    no_desc = not any("description" in r for r in rows)
    return (
        has_p and has_f and no_desc,
        f"{len(rows)} props, passing={has_p} failing={has_f} no description={no_desc}",
    )


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
        bad = [
            r for r in rows if not (lo <= datetime.fromisoformat(r["created_at"]) <= hi)
        ]
        return (bool(rows) and not bad, f"{len(rows)} rows, all in [{after}, {before}]")

    return chk


def contains_text(text: str, needle: str) -> bool:
    """Match wording across terminal line breaks without changing the capture."""
    return " ".join(needle.split()) in " ".join(text.split())


def expect_message(*needles: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = sr.result.combined.lower()
        hit = [n for n in needles if contains_text(text, n.lower())]
        return (
            bool(hit),
            f"matched {hit!r}" if hit else f"expected one of {needles!r}",
        )

    return chk


def caret_under(query: str, segment: str):
    """The rejection prints `query` on a line of its own, and the caret line
    right under it points into `segment`, the part of the query that fails."""
    start = query.index(segment)
    end = start + len(segment)

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        lines = sr.result.combined.splitlines()
        for i, line in enumerate(lines[:-1]):
            if line.strip() != query:
                continue
            indent = len(line) - len(line.lstrip())
            caret = lines[i + 1]
            if caret.strip() != "^":
                return (False, f"no caret line under the query: {caret!r}")
            col = caret.index("^") - indent
            ok = start <= col <= end
            return (
                ok,
                f"caret at query column {col}, failing segment spans {start}..{end}",
            )
        return (False, "the query is not on a line of its own")

    return chk


def contains_all(*needles: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = sr.result.combined
        missing = [n for n in needles if not contains_text(text, n)]
        return (not missing, "all present" if not missing else f"missing {missing!r}")

    return chk


# -- login (interactive/stateful) check factories ---------------------------
#
# These read `sr.captured_files` (the `post_capture` files read back from the
# throwaway HOME) rather than stdout, because the interesting outcome of a
# stateful command is the file it wrote, not what it printed.


def _captured(sr: StoryRun, rel_path: str) -> str | None:
    for path, contents in sr.captured_files or []:
        if path == rel_path:
            return contents
    return None


def _shown(sr: StoryRun) -> str:
    """Everything a TTY story put on screen: every frame, not just the last one.
    A menu is erased once chosen, so its options survive only in the frame taken
    while it was up."""
    return "\n".join(screen for _, screen in sr.frames or [])


def tty_persisted(
    *,
    prompts: tuple[str, ...] = (),
    absent_prompts: tuple[str, ...] = (),
    files: tuple[tuple[str, tuple[str, ...]], ...] = (),
    absent_files: tuple[str, ...] = (),
    secrets_absent: tuple[str, ...] = (),
    expect_ok: bool = True,
):
    """Gate a TTY story. Combines the concerns one such story cares about: the
    command exits with the expected success/failure; the expected `prompts` all
    appear on screen and no `absent_prompts` do; each `(rel_path, needles)` in
    `files` was written and contains every needle; each path in `absent_files`
    was NOT written; and no raw `secrets_absent` value ever reached the screen.
    Any failure lists what went wrong."""

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = _shown(sr)
        problems: list[str] = []

        if sr.result.ok != expect_ok:
            problems.append(f"exit={sr.result.returncode} (want ok={expect_ok})")

        # snouty wraps long lines at word boundaries, so match a prompt across
        # a line break.
        flat = " ".join(text.split())
        missing_prompts = [p for p in prompts if p not in flat]
        if missing_prompts:
            problems.append(f"missing prompts={missing_prompts!r}")
        shown_absent = [p for p in absent_prompts if p in text]
        if shown_absent:
            problems.append(f"unexpected prompts={shown_absent!r}")

        for rel_path, needles in files:
            contents = _captured(sr, rel_path)
            if contents is None:
                problems.append(f"{rel_path} not written")
                continue
            missing = [n for n in needles if n not in contents]
            if missing:
                problems.append(f"{rel_path} missing {missing!r}")

        for rel_path in absent_files:
            if _captured(sr, rel_path) is not None:
                problems.append(f"{rel_path} was written (expected absent)")

        leaked = [s for s in secrets_absent if s in text]
        if leaked:
            problems.append(f"secret reached the screen={leaked!r}")

        return (
            not problems,
            "; ".join(problems) or "prompts + persisted state as expected",
        )

    return chk


class OneLine(str):
    """A `doctor_check` needle that must appear unwrapped on one terminal line."""


def doctor_check(
    *,
    contains: tuple[str, ...],
    absent: tuple[str, ...] = (),
    ok: bool | None = None,
):
    """Gate a doctor story: every `contains` needle must appear (a `OneLine`
    needle on one terminal line), no `absent` needle may, and (when `ok` is
    given) the exit status must match. `ok` is left
    None for the api-key/legacy stories because their overall pass/fail also
    depends on the machine's container runtime — only the auth lines, which this
    asserts on, are deterministic."""

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = sr.result.combined
        lines = text.splitlines()
        missing = [
            n
            for n in contains
            if not (
                any(n in line for line in lines)
                if isinstance(n, OneLine)
                else contains_text(text, n)
            )
        ]
        unexpected = [n for n in absent if contains_text(text, n)]
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
    """The report, required checks, and exit status must agree with the setup
    described by the story."""
    try:
        data = json.loads(sr.result.stdout)
    except json.JSONDecodeError as e:
        return (False, f"stdout is not valid JSON: {e}")
    if not isinstance(data, dict):
        return (False, "report is not a JSON object")
    checks = data.get("checks")
    if (
        not isinstance(data.get("ok"), bool)
        or not isinstance(checks, list)
        or not checks
    ):
        return (False, "report needs a boolean ok and a non-empty checks array")
    well_formed = all(
        isinstance(c, dict)
        and isinstance(c.get("name"), str)
        and c.get("status") in ("ok", "warn", "error")
        and isinstance(c.get("message"), str)
        for c in checks
    )
    if not well_formed:
        return (False, "checks need string names/messages and ok/warn/error statuses")
    expected = (
        {"tenant": "ok", "api_key": "ok"}
        if sr.story.expect_ok
        else {"tenant": "error", "credentials": "error"}
    )
    statuses = {c["name"]: c["status"] for c in checks}
    required_match = all(
        statuses.get(name) == status for name, status in expected.items()
    )
    consistent = data["ok"] == all(c["status"] != "error" for c in checks)
    passed = (
        data["ok"] == sr.result.ok == sr.story.expect_ok
        and required_match
        and consistent
    )
    return (
        passed,
        f"ok={data['ok']} (want {sr.story.expect_ok}), exit={sr.result.returncode}, "
        f"{len(checks)} checks, required_match={required_match}, consistent={consistent}",
    )


def verbose_api_calls(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    has_get = "> GET" in sr.result.stderr or "> GET" in sr.result.stdout
    has_table = "STATUS" in sr.result.combined or "RUN" in sr.result.combined.upper()
    return (has_get and has_table, f"api_calls={has_get} table={has_table}")


def event_multi_match(needle: str, second: str):
    """Both needles must appear in every returned row's raw JSON: the server
    requires every needle."""

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        n1, n2 = needle.lower(), second.lower()
        bad = [
            r
            for r in rows
            if not (n1 in json.dumps(r).lower() and n2 in json.dumps(r).lower())
        ]
        ok = bool(rows) and not bad
        return (ok, f"{len(rows)} rows, all contain both needles={not bad}")

    return chk


def event_keyword_present(keyword: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        present = keyword.lower() in sr.result.combined.lower()
        return (bool(rows) and present, f"{len(rows)} rows, keyword shown={present}")

    return chk


_EXAMPLE_TOTALS = re.compile(
    r"^Examples\s+(showing )?(?:(\d+) out of )?(\d+) passing examples?"
    r" and (?:(\d+) out of )?(\d+) failing examples?$",
    re.M,
)


def _sample_note_ok(text: str) -> bool:
    """When the table shows only part of a group, the `Examples` line starts
    with `showing` and gives `N out of M` for every group. When it shows
    all, the line gives only the totals."""
    m = _EXAMPLE_TOTALS.search(text)
    if m is None:
        return False
    rows = {
        s: len(re.findall(rf"^  {s}\s+-?\d+\s", text, re.M))
        for s in ("passing", "failing")
    }
    groups = {"passing": (m[2], int(m[3])), "failing": (m[4], int(m[5]))}
    sampled = any(rows[s] < total for s, (_, total) in groups.items())
    for status, (shown, total) in groups.items():
        want = rows[status] if sampled else None
        if (None if shown is None else int(shown)) != want or rows[status] > total:
            return False
    return (m[1] is not None) == sampled


def property_has_examples(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """Example moments with no `Details` row, totals that say when the table
    is a sample, plus the one trailing next step they feed."""
    text = sr.result.combined
    rows = _has_moment_rows(text)
    no_details = _DETAILS_ROW.search(text) is None
    sample = _sample_note_ok(text)
    hint = contains_text(text, "view logs leading up to an example:")
    return (
        rows and no_details and sample and hint,
        f"moment rows={rows}, no Details row={no_details}, sample note={sample}, "
        f"logs hint={hint}",
    )


def property_non_event_result(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """A non-event property shows its value(s) under a `Result` label, with no
    per-moment HASH/VTIME rows (those belong to event properties' Examples)."""
    text = sr.result.combined
    has_result = _NONEVENT_RESULT.search(text) is not None
    no_moments = not _has_moment_rows(text)
    no_details = _DETAILS_ROW.search(text) is None
    ok = has_result and no_moments and no_details
    return (
        ok,
        f"result={has_result}, no moments={no_moments}, no Details row={no_details}",
    )


def _exit_with(*needles: str, want_ok: bool):
    """Story check: the command must exit with the expected success/failure AND
    every needle must appear in its output. Requiring the exit polarity stops a
    command that merely prints a needle (while exiting the other way) from
    passing as the wrong kind of story."""

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        text = sr.result.combined
        missing = [n for n in needles if not contains_text(text, n)]
        ok = (sr.result.ok == want_ok) and not missing
        return (ok, f"exit={sr.result.returncode}, missing={missing!r}")

    return chk


def properties_or_none(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """A run that stopped early lists the properties it reported (a run from
    release 65.3 on reports them while it runs), or says it has none."""
    text = sr.result.combined
    table = contains_text(text, "STATUS") and contains_text(text, "NAME")
    empty = contains_text(text, "No properties found.")
    return (
        sr.result.ok and table != empty,
        f"exit={sr.result.returncode}, table={table}, empty message={empty}",
    )


def succeeds_with(*needles: str):
    """A clean-success story: exit zero AND every needle appears in the output."""
    return _exit_with(*needles, want_ok=True)


def fails_with(*needles: str):
    """A clean-error story: exit non-zero AND every needle appears in the output."""
    return _exit_with(*needles, want_ok=False)


def exits_1_with(*needles: str):
    """`fails_with`, and the exit code is 1: snouty reports a script's exit code
    and does not forward it."""
    failed = fails_with(*needles)

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        ok, detail = failed(sr, reg)
        return (ok and sr.result.returncode == 1, detail)

    return chk


def fails_before_launch(guard: LaunchGuard | None, run: str, *needles: str):
    """`fails_with`, and `guard` refused no request for `run`. The debug
    stories share one guard and are captured at the same time, so the check
    counts only the refused requests that name the story's own run."""
    failed = fails_with(*needles)

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        ok, detail = failed(sr, reg)
        refused = [
            line
            for line, body in (guard.refused if guard is not None else [])
            if run in line + body
        ]
        return (ok and not refused, f"{detail}, launches refused={refused!r}")

    return chk


REWARM_LINE = re.compile(r"^rewarming moment: (\d+)%$", re.MULTILINE)


def rewarm_lines_once(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """A piped `runs exec` that loads a moment: at least one rewarm line, no
    percentage twice, and the end-moment trailer."""
    text = sr.result.combined
    percents = REWARM_LINE.findall(text)
    repeated = sorted({p for p in percents if percents.count(p) > 1})
    trailer = contains_text(text, "end moment:")
    ok = sr.result.ok and bool(percents) and not repeated and trailer
    return (
        ok,
        f"exit={sr.result.returncode}, rewarm lines={percents!r}, repeated={repeated!r}, "
        f"trailer={trailer}",
    )


def launched_run(run: str):
    """The `--json launch` story: exit 0, and the output names the run that
    the gallery follows."""

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        got = launched_run_id(sr.result.stdout)
        ok = sr.result.ok and got == run
        return (ok, f"exit={sr.result.returncode}, runId={got!r}")

    return chk


_STREAM_EVENTS = frozenset({"property", "sample", "verdict", "completed", "unknown"})
# The `--json` key of the field after the property ID in a stream line.
_STREAM_SECOND_KEY = {"sample": "kind", "verdict": "verdict"}

# What `runs stream` shows while the run has no stream yet.
STREAM_WAITING = "waiting for the run's stream to open"
# What `runs stream` shows once the stream is open: the spinner, or with
# `--json`, a line on stderr.
STREAM_SPINNER = "streaming run {run}"
STREAM_OPEN = "the stream of run {run} is open"


def stream_events(text: str) -> list[dict]:
    """The events of `runs stream`'s terminal output, with the keys of its
    `--json` output. A line that is no event, such as the tail of a wrapped
    line, is skipped."""
    rows = []
    for line in text.splitlines():
        event, *fields = line.split() or [""]
        if event not in _STREAM_EVENTS or not fields:
            continue
        first, rest = fields[0], fields[1:]
        if event == "completed":
            rows.append({"event": event, "run_id": first})
        elif event == "property":
            rows.append({"event": event, "property_id": first, "name": " ".join(rest)})
        elif event in _STREAM_SECOND_KEY:
            key = _STREAM_SECOND_KEY[event]
            rows.append(
                {"event": event, "property_id": first, key: next(iter(rest), "")}
            )
        else:
            rows.append({"event": event})
    return rows


def stream_verdict(
    rows: list[dict], run: str, bug: str, opened: bool
) -> tuple[bool, str]:
    """Whether the stream events `rows` cover a whole run: the stream opened,
    at least one property, sample, and verdict, a `failed` verdict for the
    property named `bug`, and a last event that completes `run`."""
    counts = Counter(r.get("event") for r in rows)
    bug_ids = {
        r.get("property_id")
        for r in rows
        if r.get("event") == "property" and bug in (r.get("name") or "")
    }
    failed = {
        r.get("property_id")
        for r in rows
        if r.get("event") == "verdict" and r.get("verdict") == "failed"
    }
    bug_failed = bool(bug_ids & failed)
    last = rows[-1] if rows else {}
    completed = last.get("event") == "completed" and last.get("run_id") == run
    ok = (
        opened
        and all(counts[e] > 0 for e in ("property", "sample", "verdict"))
        and bug_failed
        and completed
    )
    return (
        ok,
        f"opened={opened}, {counts['property']} property, {counts['sample']} sample, "
        f"{counts['verdict']} verdict lines, {bug!r} failed={bug_failed}, "
        f"ends with completed={completed}",
    )


def _cast_text(cast: str | None) -> str:
    """Everything a recording's terminal received, redraws included. Line 1
    of an asciicast is its header, and each other line is `[time, type,
    data]`."""
    lines = (cast or "").splitlines()[1:]
    return "".join(json.loads(line)[2] for line in lines if line.strip())


def stream_run(run: str, *, json_output: bool):
    """A `runs stream` story that lasts the whole run. The output must show
    that the stream opened: the wait, or the open notice. The server can open
    the stream at once, even while the run is starting."""

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        if json_output:
            try:
                rows = _ndjson(sr.result.stdout)
            except json.JSONDecodeError as e:
                return (False, f"stdout is not NDJSON: {e}")
            text, notice = sr.result.stderr, STREAM_OPEN
        else:
            rows = stream_events(sr.result.combined)
            text, notice = _cast_text(sr.result.cast), STREAM_SPINNER
        waited = STREAM_WAITING in text
        open_line = notice.format(run=run) in text
        ok, detail = stream_verdict(
            rows, run, EXAMPLE_BUG_PROPERTY, waited or open_line
        )
        return (
            ok and sr.result.ok,
            f"exit={sr.result.returncode}, waited={waited}, "
            f"open notice={open_line}, {detail}",
        )

    return chk


def run_properties(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """`runs properties` on the finished gallery run: passing and failing
    properties, and EXAMPLE_BUG_PROPERTY among the failing."""
    ok, detail = properties_pass_and_fail(sr, reg)
    bug_failing = any(
        EXAMPLE_BUG_PROPERTY in r.get("name", "") and r.get("status") == "Failing"
        for r in sr.rows or []
    )
    return (
        ok and bug_failing,
        f"{detail}, {EXAMPLE_BUG_PROPERTY!r} failing={bug_failing}",
    )


def bug_property(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """`runs property` for the example's failing property: its name, and
    counterexample moments as `property_has_examples` checks them."""
    ok, detail = property_has_examples(sr, reg)
    named = contains_text(sr.result.combined, EXAMPLE_BUG_PROPERTY)
    return (ok and named and sr.result.ok, f"{detail}, named={named}")


def logs_non_empty(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    n = len(sr.rows or [])
    return (n > 0, f"{n} log lines")


def logs_with_footer(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
    """Log lines, then the footer that marks the end of the stream."""
    n = len(sr.rows or [])
    footer = contains_text(sr.result.combined, "— end of logs")
    return (n > 0 and footer, f"{n} log lines, footer={footer}")


def logs_begin_at(begin: str):
    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        rows = sr.rows or []
        if not rows:
            return (False, "no log lines")
        first = float(rows[0].get("moment", {}).get("vtime", rows[0].get("vtime", 0.0)))
        ok = first >= float(begin) - 1e-9
        return (ok, f"{len(rows)} lines, first vtime {first} >= {begin}")

    return chk


def exec_json(needle: str, *, system_events: bool):
    """Check a `--json runs exec` story. With `--events`, timeline events
    without `output_text` appear."""

    def chk(sr: StoryRun, reg: Registry) -> tuple[bool, str]:
        try:
            rows = _ndjson(sr.result.stdout)
        except json.JSONDecodeError as e:
            return (False, f"stdout is not NDJSON: {e}")
        if not rows:
            return (False, "no rows")
        *events, result = rows
        exited = result.get("status") == "exited" and result.get("exit_code") == 0
        found = any(needle in (e.get("output_text") or "") for e in events)
        system = sum("output_text" not in e for e in events)
        ok = sr.result.ok and exited and found and (system > 0) == system_events
        return (
            ok,
            f"exit={sr.result.returncode}, {len(events)} events ({system} without "
            f"output text), exited 0={exited}, {needle!r} found={found}",
        )

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
        miss_help = [t for t in story.align_tokens if not contains_text(help_text, t)]
        miss_out = [t for t in story.align_tokens if not contains_text(out, t)]
        if miss_help or miss_out:
            return (
                False,
                f"misaligned — absent from help={miss_help} output={miss_out}",
            )
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
        "ANTITHESIS_API_KEY": want("ANTITHESIS_API_KEY", api_key, _FAKE_API_KEY_A),
        "ANTITHESIS_USERNAME": want("ANTITHESIS_USERNAME", username, "demo-user"),
        "ANTITHESIS_PASSWORD": want("ANTITHESIS_PASSWORD", password, _FAKE_PASSWORD),
        "ANTITHESIS_TENANT": want("ANTITHESIS_TENANT", tenant, "demo-tenant"),
        "ANTITHESIS_REPOSITORY": want(
            "ANTITHESIS_REPOSITORY", repo, "registry.example.com/acme/demo"
        ),
    }


# The doctor stories that assert a *reachable* API can't use `_doctor_env`'s
# synthesized creds — a placeholder tenant resolves to an unreachable host. They
# run against the operator's real credentials instead (gen-gallery already
# requires them; discovery hits the same API), dropping only any ambient legacy
# username/password so the API key is reported on its own.
def _reachable_doctor_env() -> dict[str, str | None]:
    return {"ANTITHESIS_USERNAME": None, "ANTITHESIS_PASSWORD": None}


# A long query whose third verb names an invalid field, so the server's caret lands
# in the middle of the query rather than at its start.
INVALID_QUERY_SEGMENT = 'not_matches({host: "setup"})'
# Short enough that the query, indented under the error, fits on one line of
# the gallery's TTY_COLS terminal: a wrapped query moves the caret's column.
INVALID_QUERY = (
    'contains({output_text: "error"}).filter(ev => ev.moment.vtime > 10)'
    f'.{INVALID_QUERY_SEGMENT}.narrow(["source"])'
)


def build_stories(d: Discovery, guard: LaunchGuard | None = None) -> list[Story]:
    """The goal stories that need discovery. The `debug-bad-*` stories send
    their requests through `guard`; without one, a listing, they point at a
    base URL where nothing listens."""
    kw, kw2 = d.event_keyword, d.event_kw2
    guard_url = guard.url if guard is not None else _NO_SERVER_URL
    guarded: dict[str, str | None] = {"ANTITHESIS_BASE_URL": guard_url}
    vmin = _begin_vtime(d.logs_vtime)
    stories = [
        # -- listing --------------------------------------------------------
        Story(
            "runs",
            "Quickly check what test runs are around",
            "I just want to glance at what test runs exist without recalling any subcommands.",
            "A readable table of recent runs (RUN ID, STATUS, CREATED, LAUNCHER, RUN) appears — `runs` behaves like `runs list`.",
            ["runs"],
            non_empty_table,
        ),
        Story(
            "runs-list",
            "List recent runs to find one to inspect",
            "I want to scan recent runs and pick one to dig into.",
            "Up to 10 recent runs, newest first, with legible RUN ID/STATUS/CREATED/"
            "LAUNCHER/RUN columns; a LAUNCHER or RUN cell cut to fit ends in `…`; RUN is the "
            "test name, and a run with no test name (a debugging session) shows the "
            "first line of its description or event description there, else `-`; "
            "when more runs exist, a stderr note says the output stopped at the limit.",
            ["runs", "list", "-n", "10"],
            rows_at_most_with_run_cells(10),
        ),
        Story(
            "runs-list--limit",
            "Show me just the last three runs",
            "I only care about the very latest handful of runs.",
            "At most 3 rows, the most recent ones; when more runs exist, a stderr note "
            "says the output stopped at the limit.",
            ["runs", "list", "-n", "3"],
            rows_at_most(3),
        ),
        Story(
            "runs-list--detail",
            "Get full descriptions instead of the truncated RUN column",
            "The default RUN column is truncated; I want to read the full descriptions.",
            "Descriptions are shown in full (longer than the default view), one row per run. "
            "A run with no description, such as a debugging session, shows its event "
            "description in the Description block. When more runs exist, a blank line sets the limit note apart from the last run.",
            ["runs", "list", "-n", "6", "--detail"],
            # --detail can't be combined with --json, so validate the rendered
            # key-value blocks directly rather than re-running for JSON rows. Check
            # the always-present title-case labels (the default table uses
            # UPPERCASE headers and no "Launcher"); "Description" is omitted when a
            # run has no description and no event description, so requiring it would falsely fail on description-less
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
        # -- usage ----------------------------------------------------------
        Story(
            "runs-usage",
            "See how many hours recent runs used",
            "I want to know which recent runs used the most hours.",
            "Up to 10 runs, newest first, with RUN ID/STARTED/ENDED/CORE HOURS columns; "
            "STARTED/ENDED are local times, and CORE HOURS is last, right-aligned, with one decimal; "
            "when more runs exist, a stderr note says the output stopped at the limit.",
            ["runs", "usage"],
            rows_at_most(10),
        ),
        Story(
            "runs-usage--rollup-week",
            "Track core-hour usage week by week",
            "I want the total runs and hours of each of the last few weeks.",
            "Exactly 4 rows: the current week and the 3 before it, newest first, with "
            "START/END/RUNS/CORE HOURS columns; START is a Monday and END the Sunday after it, a week with "
            "no runs shows 0, and CORE HOURS has one decimal; a stderr note says it shows up to 4 periods.",
            ["runs", "usage", "--rollup", "week", "-n", "4"],
            rows_at_most(4),
        ),
        Story(
            "runs-usage--rollup-month",
            "Compare core-hour usage across months",
            "I want to see how our usage changed from month to month.",
            "At most 6 rows, newest month first; START and END are the first and last days of each month.",
            ["runs", "usage", "--rollup", "month", "-n", "6"],
            rows_at_most(6),
        ),
        Story(
            "runs-usage--utc",
            "Report usage in UTC",
            "I share usage numbers with a team in other timezones, so I want UTC times.",
            "Up to 5 runs; STARTED/ENDED are UTC times, and the --json times end in `Z`.",
            ["runs", "usage", "--utc", "-n", "5"],
            rows_at_most(5),
        ),
        Story(
            "runs-usage--window",
            "Total the usage of one month",
            "I want the daily usage of the runs that ended in a given month.",
            "One row per day of September, newest first, with no limit note; days with no "
            "runs show 0.",
            [
                "runs",
                "usage",
                "--rollup",
                "day",
                "--after",
                "2026-09-01",
                "--before",
                "2026-10-01",
            ],
            rows_at_most(30),
        ),
        Story(
            "runs-usage--after",
            "See the first runs after a date",
            "I want the first few runs that finished after the start of October.",
            "At most 5 rows: the 5 runs that ended first on or after 2026-10-01 (local time), "
            "shown newest first; when more runs exist, a stderr note says the output stopped "
            "at the limit.",
            ["runs", "usage", "--after", "2026-10-01", "-n", "5"],
            rows_at_most(5),
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
            "Shows the run id, status, timestamps, launcher, and links, then hints at "
            "`runs properties` for the property results and `--web` for the report.",
            ["runs", "show", d.success],
            contains_all(d.success, "completed", f"snouty runs properties {d.success}"),
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
            "Status is incomplete, the failure moment (vtime/hash) is shown, and it "
            "points at `snouty runs properties` for the properties the run recorded.",
            ["runs", "show", d.fail],
            # The rubric promises a failure moment; assert the real hash renders so
            # a run without one (the 0/0 sentinel) can't pass silently.
            contains_all("incomplete", d.fail_hash, f"snouty runs properties {d.fail}"),
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
        Story(
            "runs-cancel-finished",
            "Try to cancel a run that has already finished",
            "I try to cancel a run that already completed; I want a clean error that says why.",
            "A clean error that says the run has already finished, a note that only a "
            "starting or in-progress run can be cancelled, and a suggestion to check its "
            "status with `snouty runs show`. Non-zero exit.",
            ["runs", "cancel", d.success],
            fails_with(
                f"Error: run {d.success} has already finished",
                f"snouty runs show {d.success}",
            ),
            json_capable=False,
            expect_ok=False,
        ),
        Story(
            "runs-wait",
            "Block until a run reaches a terminal state",
            "I scripted a launch and want one command that blocks until the run finishes, "
            "instead of writing my own polling loop.",
            "A single final-status line once the run is terminal — immediately here, since "
            "this run is already completed.",
            ["runs", "wait", d.success],
            contains_all(d.success, "completed"),
            json_capable=False,
        ),
        Story(
            "runs-wait-bare-timeout",
            "Wait with a timeout written as a bare number",
            "My old script passes --timeout 90, with no unit.",
            "The wait works as before, counting 90 minutes, and a warning on stderr says a "
            "bare number is deprecated and shows the value with a unit: 1h30m. Exit 0.",
            ["runs", "wait", d.success, "--timeout", "90"],
            succeeds_with(
                "warning: a bare number for --timeout is deprecated; write 1h30m",
                "completed",
            ),
            json_capable=False,
        ),
        # -- properties -----------------------------------------------------
        Story(
            "runs-properties",
            "See all properties — pass and fail",
            "I want the full property list for a completed run.",
            "A STATUS/✓/✗/ID/NAME table with green pass and red fail labels on a "
            "terminal, and each property's full ID. "
            "The --json rows carry no `description` field.",
            ["runs", "properties", d.success],
            properties_pass_and_fail,
        ),
        Story(
            "runs-properties--passing",
            "List only the green properties",
            "I want to see only the properties that passed.",
            "Every row is a passing property (STATUS `pass`).",
            ["runs", "properties", d.success, "--passing"],
            all_status("Passing"),
        ),
        Story(
            "runs-properties--failing",
            "Focus on the properties that broke",
            "I want to see only the properties that failed.",
            "Every row is a failing property (STATUS `fail`).",
            ["runs", "properties", d.success, "--failing"],
            all_status("Failing"),
        ),
        Story(
            "runs-properties-incomplete",
            "Properties for a run that never finished",
            "I try to view properties on an incomplete run.",
            "Exits zero with the properties the run reported before it stopped, "
            "or says `No properties found.` when it reported none.",
            ["runs", "properties", d.fail],
            properties_or_none,
            json_capable=False,
        ),
        # -- property detail (`properties --name <x> --detail`) -------------
        Story(
            "runs-properties-detail-failing",
            "Drill into a failing property's counter-examples",
            "A property failed; I want to see concrete counter-examples I can debug.",
            "Shows the property, its passing/failing example totals, and at least one counter-example with a moment (hash/vtime) — not `failed with no examples`. Ends with one `snouty runs logs` next step. "
            "There is no `Details` row: snouty does not show property descriptions.",
            ["runs", "properties", d.success, "--name", d.fail_prop, "--detail"],
            property_has_examples,
            json_capable=False,
        ),
        Story(
            "runs-properties-detail-passing",
            "Look at the examples behind a passing property",
            "A property passed; I want to see example moments that satisfied it.",
            "Shows its example totals and at least one example with a moment (hash/vtime), then one `snouty runs logs` next step. "
            "There is no `Details` row: snouty does not show property descriptions.",
            ["runs", "properties", d.success, "--name", d.pass_event_prop, "--detail"],
            property_has_examples,
            json_capable=False,
        ),
        Story(
            "runs-properties-detail-non-event",
            "Detail a non-event property — its result value",
            "I want to inspect a non-event ('system') property, whose value is data rather than moments.",
            "Shows the value under a 'Result' label (scalar inline, or JSON for an object/array), with no per-moment hash/vtime rows "
            "and no `Details` row.",
            ["runs", "properties", d.success, "--name", d.nonevent_prop, "--detail"],
            property_non_event_result,
            json_capable=False,
        ),
        Story(
            "runs-property",
            "Look up one property by its ID",
            "I have a property ID from the JSON output, and I want that property alone.",
            "Shows only that property, in full as --detail does: its ID, totals, and at least one "
            "moment (hash/vtime), then one `snouty runs logs` next step.",
            ["runs", "property", d.success, d.fail_prop_id],
            property_has_examples,
            json_capable=False,
        ),
        Story(
            "runs-property-unknown",
            "Look up a property ID that the run does not have",
            "I mistype a property ID; I want to know which ID is wrong and how to find the right one.",
            "A clean `property not found: <id>` error that suggests how to list the IDs. Exit 1.",
            ["runs", "property", d.success, UNKNOWN_PROPERTY],
            expect_message("property not found"),
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
        # Both searches pass the same explicit --limit, so the AND-narrowing
        # check can tell a genuinely smaller result from one the server capped.
        Story(
            "runs-events-single",
            f"Find events that mention '{kw}'",
            "I want to find events that mention a particular keyword.",
            f"At least one matching event row, and the keyword '{kw}' appears in the output. "
            "A `moment HASH` divider groups `VTIME [source] payload` lines. Its hash "
            "is sufficient for `runs logs` to stream to the timeline's current end. "
            "When more events match than the default limit of "
            "50, a stderr note says the output stopped at the limit.",
            ["runs", "events", d.success, "--match", kw],
            event_keyword_present(kw),
        ),
        Story(
            "runs-events-multi-match",
            "AND-narrow with two --match needles",
            "I want to narrow results to events that mention BOTH of two terms. "
            "Several terms route through the events-search API, which ANDs them "
            "server-side.",
            f"At least one row, and every row's raw JSON contains both '{kw}' and '{kw2}'. "
            "When more events match than the default limit of 50, a stderr note says "
            "the output stopped at the limit.",
            ["runs", "events", d.success, "--match", kw, "--match", kw2],
            event_multi_match(kw, kw2),
        ),
        Story(
            "runs-events-no-results",
            "Search events that don't match anything",
            "I search for a string that doesn't occur; I want a friendly empty result.",
            "A clear 'No events matched' message — not an error or a crash.",
            [
                "runs",
                "events",
                d.success,
                "--match",
                "this string will not appear anywhere",
            ],
            expect_message("No events matched"),
            json_capable=False,
        ),
        Story(
            "runs-events-incomplete",
            "Search events on an incomplete run for failure context",
            "An incomplete run failed; I want events around the failure.",
            f"At least one event row matching '{d.fail_event_kw}' from the incomplete run.",
            ["runs", "events", d.fail, "--match", d.fail_event_kw],
            non_empty_table,
        ),
        # -- search (event-set DSL) -------------------------------------------
        Story(
            "runs-search-contains",
            f"Query events with the DSL: contains '{kw}'",
            "I want to run an event-set DSL query and read the matching events.",
            f"At least one matching event line, keyword '{kw}' visible. A `moment HASH` "
            "divider groups `VTIME [source] payload` lines. Its hash is sufficient for "
            "`runs logs` to stream to the timeline's current end. When more "
            "events match than the default limit of 50, a stderr note says the output "
            "stopped at the limit.",
            ["runs", "search", d.success, f'contains({{output_text: "{kw}"}})'],
            event_keyword_present(kw),
        ),
        Story(
            "runs-search-limit",
            "Cap a DSL query at -n 3",
            "I want only the first few matches, not the whole stream — and I want to "
            "know whether the first few are all of them.",
            "Exactly at most 3 event lines, then the command exits promptly; a stderr "
            "note names the limit and says more results may be available.",
            [
                "runs",
                "search",
                d.success,
                f'contains({{output_text: "{kw}"}})',
                "-n",
                "3",
            ],
            rows_at_most_with_limit_note(3),
        ),
        Story(
            "runs-search-no-results",
            "A DSL query that matches nothing",
            "My query matches no events; I want a friendly empty result.",
            "A clear 'No events matched the query' message — not an error, not silence.",
            [
                "runs",
                "search",
                d.success,
                'contains({output_text: "this string will not appear anywhere"})',
            ],
            expect_message("No events matched the query"),
            json_capable=False,
        ),
        Story(
            "runs-search-check-valid",
            "Validate a query without running it",
            "I want to check my query's syntax before running an expensive search.",
            "A clear 'query is valid' confirmation and exit 0.",
            ["runs", "search", d.success, 'contains({output_text: "x"})', "--check"],
            expect_message("query is valid"),
            json_capable=False,
        ),
        Story(
            "runs-search-check-invalid",
            "Validate an invalid query",
            "My long query names a field the string verbs do not have, in its third "
            "verb; I want the validator to show me exactly where.",
            "A non-zero exit with the server's rejection: the query on its own line, "
            "a caret under the bad field in the middle of the query, and the "
            "reason naming the valid fields. It must be clear the query (not snouty) "
            "is the problem.",
            ["runs", "search", d.success, INVALID_QUERY, "--check"],
            caret_under(INVALID_QUERY, INVALID_QUERY_SEGMENT),
            json_capable=False,
            expect_ok=False,
        ),
        Story(
            "runs-search-count",
            f"Count the events that match '{kw}'",
            "A search shows only a sample of the matches; I want to know how many "
            "events in the whole run match my query.",
            "One line that states the number of matching events, and exit 0. No "
            "event lines, and no note about a limit.",
            [
                "runs",
                "search",
                d.success,
                f'contains({{output_text: "{kw}"}})',
                "--count",
            ],
            expect_message("matched the query"),
            json_capable=False,
        ),
        # -- logs -----------------------------------------------------------
        Story(
            "runs-logs",
            "Stream logs using the hash from a property example",
            "I copy an example's input hash from `runs properties --detail` to read "
            "logs through the end of that moment.",
            "Logs stream to the branch's current end without a vtime argument. "
            "Each divider shows only `moment HASH`; event lines keep their vtimes.",
            ["runs", "logs", d.success, d.logs_hash],
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
                d.logs_hash,
                d.logs_vtime,
                "--begin-vtime",
                vmin,
            ],
            logs_begin_at(vmin),
        ),
        Story(
            "runs-logs-bad-moment",
            "Try logs with a moment that doesn't exist",
            "I ask for a moment that isn't in this run; I want a clean error.",
            "A clean error with two suggestion lines: the run exists but the moment does not; "
            "search with `snouty runs events <run ID> <search query>`. Non-zero exit.",
            ["runs", "logs", d.success, "0", "999999.0"],
            fails_with(
                "Error: API error: 404 Not Found — Resource not found",
                "Suggestion: the run exists but no moment matches this hash and vtime\n"
                f"Suggestion: search for a moment using `snouty runs events {d.success} <search query>`",
            ),
            json_capable=False,
            expect_ok=False,
        ),
        Story(
            "runs-exec-completed-run",
            "Try to run a command in a completed run",
            "I copy a moment from a completed run and try to run a shell command at it.",
            "A clean error: `runs exec` needs a run that is in progress, and this run is "
            "completed. A suggestion gives the `snouty debug` command that starts a "
            "debugging session at the same moment, one flag per line with shell line "
            "continuations, so it fits the terminal and pastes as one command. "
            "Non-zero exit.",
            ["runs", "exec", d.success, d.logs_hash, d.logs_vtime, "uname -n"],
            fails_with(
                "Note: `runs exec` needs a run that is in progress. This run is completed",
                "snouty debug \\\n"
                f"    --run-id {d.success} \\\n"
                f"    --input-hash {d.logs_hash} \\\n"
                f"    --vtime {d.logs_vtime}",
            ),
            json_capable=False,
            expect_ok=False,
        ),
        Story(
            "runs-logs-incomplete",
            "Stream logs at the failure moment of an incomplete run",
            "I want the logs right at the moment an incomplete run failed.",
            "At least one log line, then a footer that marks the end of the stream, so a quiet "
            "timeline doesn't read as cut short.",
            ["runs", "logs", d.fail, d.fail_hash, d.fail_vtime],
            logs_with_footer,
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
            "runs-stream-final",
            "Stream a run that is already final",
            "I start the property stream on a completed run; I want it to end at once, not hang.",
            "stdout is one `completed <run id>` line. Exit 0.",
            ["runs", "stream", d.success],
            succeeds_with(f"completed  {d.success}"),
            json_capable=False,
        ),
        Story(
            "runs-stream-unknown",
            "Wrong run ID — stream reports a clean error",
            "I pass a run id that doesn't exist; I want a clean error.",
            "A clean `run not found` error, not a crash or a stream that hangs. Exit 1.",
            ["runs", "stream", UNKNOWN_RUN],
            fails_with("run not found"),
            json_capable=False,
            expect_ok=False,
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
        # -- debug errors (fail before the launch) ---------------------------
        Story(
            "debug-bad-run",
            "Debug a run that does not exist",
            "I mistype the run ID when I open a debugging session.",
            "snouty fails with `run not found: <id>` and starts no session, so nothing "
            "is billed. Exit 1.",
            [
                "debug",
                "--run-id",
                UNKNOWN_RUN,
                "--input-hash",
                UNKNOWN_HASH,
                "--vtime",
                "100",
            ],
            fails_before_launch(guard, UNKNOWN_RUN, f"run not found: {UNKNOWN_RUN}"),
            json_capable=False,
            env=guarded,
        ),
        Story(
            "debug-bad-hash",
            "Debug at an input hash that the run does not have",
            f"I paste an input hash from a different run when I debug run {d.success}.",
            "snouty fails, says that no moment of the run matches the input hash, and "
            "tells me how to find a moment with `snouty runs events`. It starts no "
            "session. Exit 1.",
            [
                "debug",
                "--run-id",
                d.success,
                "--input-hash",
                UNKNOWN_HASH,
                "--vtime",
                "100",
            ],
            fails_before_launch(
                guard, d.success, f"no moment matches input hash {UNKNOWN_HASH}"
            ),
            json_capable=False,
            env=guarded,
        ),
        Story(
            "debug-bad-duration",
            "Debug with a session length that is out of range",
            "I ask for an 8-hour debugging session.",
            "snouty fails and says that --duration must be from 30 minutes to 6 "
            "hours. It starts no session. Non-zero exit.",
            [
                "debug",
                "--run-id",
                DURATION_RUN,
                "--input-hash",
                UNKNOWN_HASH,
                "--vtime",
                "100",
                "--duration",
                "8h",
            ],
            fails_before_launch(
                guard, DURATION_RUN, "must be from 30 minutes to 6 hours"
            ),
            json_capable=False,
            env=guarded,
        ),
        Story(
            "launch-bare-setup-timeout",
            "Launch with a setup timeout that has no unit",
            "I want the run to fail fast if setup takes more than 10 minutes, and I "
            "pass a bare 10.",
            "snouty fails and says that --setup-timeout needs a unit, and that a bare "
            "number is not permitted. It starts no run. Non-zero exit.",
            [
                "launch",
                "--launcher",
                GALLERY_RUN_LAUNCHER,
                "--config-image",
                SETUP_TIMEOUT_IMAGE,
                "--setup-timeout",
                "10",
            ],
            fails_before_launch(
                guard, SETUP_TIMEOUT_IMAGE, "a bare number is not permitted"
            ),
            json_capable=False,
            env=guarded,
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
                contains=("API key provided", "Antithesis API reachable"),
                absent=("ANTITHESIS_USERNAME", "ANTITHESIS_PASSWORD"),
            ),
            json_capable=False,
            env=_reachable_doctor_env(),
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
                contains=("API key provided",),
                absent=("Antithesis API",),
            ),
            json_capable=False,
            env=_doctor_env(
                api_key=True, username=False, password=False, tenant=True, repo=True
            ),
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
                **_doctor_env(
                    api_key=True, username=False, password=False, tenant=True, repo=True
                ),
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
            env=_reachable_doctor_env(),
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
                contains=("API key provided",),
                absent=("ANTITHESIS_USERNAME", "ANTITHESIS_PASSWORD"),
            ),
            json_capable=False,
            env=_doctor_env(
                api_key=True, username=True, password=True, tenant=True, repo=True
            ),
        ),
        Story(
            "doctor-no-auth",
            "Fresh install — doctor tells me what to configure",
            "I just installed snouty and haven't set any credentials; I want doctor to tell me what I need.",
            "doctor identifies the missing credentials and offers `snouty login` or "
            "ANTITHESIS_API_KEY, with a support contact for obtaining a key. It must not "
            "recommend legacy username/password authentication (issue #145).",
            ["doctor"],
            doctor_check(
                contains=(
                    "No Antithesis credentials found",
                    "snouty login",
                    "ANTITHESIS_API_KEY",
                    "ask Antithesis support",
                ),
                absent=("ANTITHESIS_USERNAME", "ANTITHESIS_PASSWORD"),
                ok=False,
            ),
            json_capable=False,
            env=_doctor_env(
                api_key=False, username=False, password=False, tenant=False, repo=False
            ),
            isolate_config=True,
        ),
        Story(
            "doctor-legacy-auth",
            "I only have a legacy username and password",
            "I authenticate with a username/password and no API key; I want doctor to tell me whether that's enough.",
            "doctor reports one warning: username/password works only for `snouty launch` and "
            "`snouty debug`, is deprecated, and `snouty runs` refuses it. It names where the "
            "credentials come from and gives one next step: run `snouty login` to store an API "
            "key, then a copyable unset command. It suggests `snouty login` only once.",
            ["doctor"],
            doctor_check(
                contains=(
                    "username/password for user [",
                    "only works for `snouty launch` and `snouty debug`",
                    "WARNING: username/password is deprecated, and `snouty runs` and other API "
                    "commands refuse it",
                    "NOTE: read from the `ANTITHESIS_USERNAME` and `ANTITHESIS_PASSWORD` "
                    "environment variables",
                    "NOTE: run `snouty login` to store an API key, then "
                    "`unset ANTITHESIS_USERNAME ANTITHESIS_PASSWORD`",
                    OneLine("`unset ANTITHESIS_USERNAME ANTITHESIS_PASSWORD`"),
                ),
                absent=(
                    "No credentials the API commands accept",
                    "Using password credentials",
                    "run `snouty login` to sign in",
                ),
            ),
            json_capable=False,
            env=_doctor_env(
                api_key=False, username=True, password=True, tenant=True, repo=True
            ),
        ),
        Story(
            "doctor-shadowed-credentials",
            "I ran `snouty login` but doctor still reports my old credentials",
            "I stored an API key with `snouty login`, but a username and password are still "
            "exported in my shell; I want to know why doctor keeps reporting the old ones.",
            "doctor reports one warning: the username and password are hiding the API key. It "
            "names where they come from and why `snouty runs` refuses them, and gives one next "
            "step, a copyable unset command, naming the API key's file. It doesn't tell me to run "
            "`snouty login` again.",
            ["doctor"],
            doctor_check(
                contains=(
                    "the username and password are hiding your API key",
                    "WARNING: snouty uses the username and password from the `ANTITHESIS_USERNAME`",
                    "which `snouty runs` refuses",
                    "NOTE: `unset ANTITHESIS_USERNAME ANTITHESIS_PASSWORD` to use the API key in",
                    "credentials.toml",
                ),
                absent=(
                    "run `snouty login` to sign in",
                    "more than one credential source",
                    "Using password credentials",
                ),
            ),
            json_capable=False,
            env=_doctor_env(
                api_key=False, username=True, password=True, tenant=True, repo=True
            ),
            isolate_config=True,
            seed_files={_CREDS: _SEED_CREDS_TOML},
        ),
        # -- config ---------------------------------------------------------
        Story(
            "settings-show",
            "See the settings snouty will use",
            "I want to see each setting that snouty resolved from my environment and "
            "settings files, before I run a command against my tenant.",
            "settings show prints one aligned row per setting, with `not set` (or the default) "
            "for a setting that is not configured, then a TYPE/PATH table "
            "of the settings files it read, project before global.",
            ["settings", "show"],
            # A machine with no settings file prints "settings files: none"
            # instead of the table.
            expect_message("TYPE", "settings files: none"),
            json_capable=False,
        ),
        Story(
            "settings-untrusted",
            "A cloned repository ships a settings file",
            "I cloned a repository that contains a .snouty.toml, and I ran snouty in it for "
            "the first time.",
            "snouty stops before it reads the file, names the file, and tells me how to trust it, "
            "deny it, or skip the check in CI.",
            ["settings", "show"],
            fails_with(
                "is not trusted",
                "snouty settings trust",
                "snouty settings deny",
                "SNOUTY_SETTINGS_TRUST=all",
            ),
            json_capable=False,
            expect_ok=False,
            isolate_config=True,
            seed_files={
                ".snouty.toml": 'tenant = "acme"\nbase_url = "https://attacker.example.com"\n'
            },
        ),
        Story(
            "doctor-json",
            "Stop CI when required configuration is missing",
            "I want CI to stop when my tenant and credentials are missing, and I want "
            "a structured report that explains the failures.",
            "`doctor --json` prints `ok: false` and exits non-zero. Its checks array "
            "contains tenant and credentials errors, each with name/status/message. "
            "The report's ok field agrees with both the check statuses and the exit status.",
            ["doctor", "--json"],
            doctor_json_check,
            json_capable=False,
            expect_ok=False,
            env=_doctor_env(
                api_key=False, username=False, password=False, tenant=False, repo=False
            ),
            isolate_config=True,
        ),
        Story(
            "doctor-json-ready",
            "Allow CI when local configuration is ready",
            "I have configured my tenant, repository, and API key. I want a script "
            "to confirm my local setup without a network request.",
            "`doctor --json --offline` prints `ok: true` and exits zero. Its checks "
            "array contains successful tenant and api_key checks and no errors. "
            "The report's ok field agrees with both the check statuses and the exit status.",
            ["doctor", "--json", "--offline"],
            doctor_json_check,
            json_capable=False,
            env=_doctor_env(
                api_key=True, username=False, password=False, tenant=True, repo=True
            ),
            isolate_config=True,
        ),
    ]
    return stories + build_help_stories(d)


def build_launch_stories(x: GalleryRun) -> list[Story]:
    """The launch of the gallery's run, when the gallery launched it rather
    than reusing a live run."""
    if x.launch is None:
        return []
    return [
        Story(
            "launch",
            "Launch a run of my project",
            f"My project validates, and I want a {GALLERY_RUN_COMPUTE_HOURS}-compute-hour run "
            "of it on Antithesis, with a test name, a description, and a custom attrs.source "
            "parameter. My script reads the run ID from the JSON output.",
            "snouty pushes the project's images and the config image, with the progress on "
            "stderr, and stdout is one JSON object with the new run's `runId`. Exit 0.",
            x.launch.args,
            launched_run(x.run),
            json_capable=False,
            precaptured=x.launch,
        )
    ]


def build_ready_stories(d: Discovery, x: GalleryRun, wait_ready: Result) -> list[Story]:
    """Stories that need the gallery's run to be ready, most of which execute
    in it, after `wait_ready` waited for it. No story is JSON-capable, so each
    script executes once."""
    exec_at = ["runs", "exec", x.run, x.hash, x.vtime]
    script = 'echo "hello from $(uname -n)"; echo "a warning on stderr" >&2'

    def exec_story(
        slug: str, title: str, goal: str, judge: str, args: list[str], check, **kw
    ):
        return Story(slug, title, goal, judge, args, check, json_capable=False, **kw)

    stories = [
        Story(
            "runs-wait-ready",
            "Wait until my new run accepts commands",
            "I just launched a run, and I want my script to block until I can run "
            "commands in it.",
            "snouty prints each status the run passes through on stderr, then one line on "
            "stdout once the run is in_progress:ready. Exit 0.",
            wait_ready.args,
            succeeds_with(f"run {x.run} is in_progress:ready"),
            json_capable=False,
            precaptured=wait_ready,
        ),
    ]
    stories += [
        exec_story(
            "runs-show-ready",
            "Check that my run is ready",
            "My run accepts commands now, and I want its status.",
            "Status is in_progress:ready: the run is in progress, and its sub-status says "
            f"that it is ready. The launcher is {GALLERY_RUN_LAUNCHER}.",
            ["runs", "show", x.run],
            contains_all("in_progress:ready", GALLERY_RUN_LAUNCHER),
        ),
        exec_story(
            "runs-exec",
            "Run a command in a live run",
            "My run is ready, and I want to run a shell command at a moment just after its "
            "setup and see what it prints.",
            "The script's stdout and stderr appear as the script printed them, then a "
            "trailer names the end moment of the branch, to chain a follow-up from. Exit 0.",
            [*exec_at, script],
            succeeds_with("hello from", "a warning on stderr", "end moment:"),
        ),
        exec_story(
            "runs-exec-exit-code",
            "See a failing command fail",
            "I run a command that exits non-zero, and I want snouty to tell me so.",
            "The script's output appears, snouty reports the script's exit code, and the "
            "trailer still names the end moment. snouty exits 1, not with the script's code.",
            [*exec_at, 'echo "checking the data directory"; exit 3'],
            exits_1_with("checking the data directory", "code 3", "end moment:"),
        ),
        exec_story(
            "runs-exec--container",
            f"Run a command in the {x.container} container",
            f"I want to look around inside the `{x.container}` container rather than on the host.",
            "The script executes in the named container: its hostname and process list "
            "belong to the container, not the host. Exit 0, with the end-moment trailer.",
            [
                *exec_at,
                "--container",
                x.container,
                "uname -n; ps -eo pid,comm | head -5",
            ],
            succeeds_with("PID", "end moment:"),
        ),
        # TODO: update the expected code if the server stops reporting 125 for a
        # container it cannot find.
        exec_story(
            "runs-exec--container-unknown",
            "Name a container that does not exist",
            "I mistype a container name, and I want to learn that the container does not exist.",
            "snouty fails, and the output makes clear that the container could not be "
            "used. The server reports exit code 125 for a container it cannot find.",
            [*exec_at, "--container", "no-such-container", "uname -n"],
            fails_with("125"),
        ),
        exec_story(
            "runs-exec--events",
            "See what else happens while my command executes",
            "I want to see the whole timeline while my script executes: the workload's logs, "
            "the assertions, and Antithesis events, next to my script's output.",
            "Events print as `runs logs` prints them, the script's output among them, then "
            "the end-moment trailer. Exit 0.",
            [*exec_at, "--events", 'echo "first line"; echo "second line"'],
            succeeds_with("first line", "second line", "end moment:"),
        ),
        exec_story(
            "runs-exec--events--detail",
            "See the full detail of the events while my command executes",
            "I want the timeline while my script executes with every event in full: the "
            "exact vtime, where each assertion is in the source, and its details.",
            "Events print as `runs logs --detail` prints them: a full-width vtime on every "
            "line, with source locations and details on indented lines, the script's output "
            "among them, then the end-moment trailer. Exit 0.",
            [*exec_at, "--events", "--detail", 'echo "first line"'],
            succeeds_with("first line", "end moment:"),
        ),
        exec_story(
            "runs-exec--json",
            "Read a command's output from a script",
            "I want machine-readable output, to read the script's output and exit code with jq.",
            "One JSON object per line: the output events, then a result with `status` "
            "`exited`, the exit code, and the end moment. No trailer.",
            ["--json", *exec_at, script],
            exec_json("hello from", system_events=False),
        ),
        exec_story(
            "runs-exec--json--events",
            "Read the whole timeline from a script",
            "I want every event of the timeline while my script executes, as JSON.",
            "One JSON object per line, including timeline events that have no output text, "
            "then the result line.",
            # The moment is just after setup, when the run is quiet: the
            # script sleeps, in virtual time, so that the run's own events
            # happen while it executes.
            ["--json", *exec_at, "--events", f"{script}; sleep 10"],
            exec_json("hello from", system_events=True),
        ),
        exec_story(
            "runs-exec-stdin",
            "Run a multi-line script from stdin",
            "My script is several lines long, so I pipe it to snouty instead of quoting it.",
            "snouty reads the script from stdin and executes it; its output appears with the "
            "end-moment trailer. Exit 0.",
            exec_at,
            succeeds_with("line 1", "line 3", "end moment:"),
            stdin='for i in 1 2 3; do\n  echo "line $i"\ndone\n',
        ),
        exec_story(
            "runs-exec--timeout",
            "Stop a command that hangs",
            "My script can hang, and I want it stopped after a few seconds.",
            "The output printed before the timeout appears, then snouty says the command "
            "timed out and after how long. No end-moment trailer. snouty exits 1.",
            # `sleep` counts virtual time, which can run far ahead of the wall
            # clock that --timeout counts: a `sleep 60` once exited within 5s.
            [*exec_at, "--timeout", "5s", 'echo "waiting for a lock"; sleep 3600'],
            fails_with("waiting for a lock", "timed out"),
        ),
        exec_story(
            "runs-exec-follow-up",
            "Continue from where my last command ended",
            f'An earlier `runs exec` wrote a file (`echo "{EXEC_MARKER_TEXT}" > '
            f"{EXEC_MARKER_PATH}`), and its trailer named an end moment. I run my next "
            "command at that moment, to build on what the first one did.",
            "The command executes at the earlier command's end moment, so the file it wrote "
            "is there. Exit 0, with a new end-moment trailer.",
            [
                "runs",
                "exec",
                x.run,
                x.follow_hash,
                x.follow_vtime,
                f"cat {EXEC_MARKER_PATH}",
            ],
            succeeds_with(EXEC_MARKER_TEXT, "end moment:"),
        ),
        exec_story(
            "runs-exec-cold-moment",
            "Execute at a moment from another run",
            f"I copied a moment from run {d.fail} and try to execute at it in my run.",
            "snouty fails, says that the moment is not loaded into the run, and tells me "
            "how to load it: use --source-run-id to load a moment from a different run.",
            # No story loads d.fail's moment, so it stays cold in a reused run.
            ["runs", "exec", x.run, d.fail_hash, d.fail_vtime, "uname -n"],
            fails_with("use --source-run-id to load"),
        ),
        exec_story(
            "runs-exec--source-run-id",
            "Load a moment from another run",
            f"My run is in progress, and I want to execute at a moment of the completed run "
            f"{x.rewarm_source}, from just after its setup.",
            "With --source-run-id, the server loads the moment by replaying the run's inputs, "
            "which can take a while, and snouty shows the progress on stderr; then the script "
            "executes. Exit 0, with the end-moment trailer.",
            [
                "runs",
                "exec",
                x.run,
                x.rewarm_hash,
                x.rewarm_vtime,
                "--source-run-id",
                x.rewarm_source,
                "uname -n",
            ],
            succeeds_with("end moment:"),
        ),
        exec_story(
            "runs-exec--source-run-id-piped",
            "Load a moment from a script, with the output in a log",
            "My CI job runs `snouty runs exec` at a moment of the completed run "
            f"{x.rewarm_source}, and its stdout and stderr go to a log file, not a terminal.",
            "Off a terminal, snouty shows the rewarm progress as `rewarming moment: N%` "
            "lines, one line for each new percentage, with no repeated line and no "
            "progress-bar redraws. Then the script's output and the end-moment trailer "
            "appear. Exit 0.",
            [
                "runs",
                "exec",
                x.run,
                x.rewarm_pipe_hash,
                x.rewarm_pipe_vtime,
                "--source-run-id",
                x.rewarm_source,
                "uname -n",
            ],
            rewarm_lines_once,
            pipe=True,
        ),
    ]
    return stories


def stream_args(run: str) -> list[str]:
    return ["runs", "stream", run]


def build_final_stories(x: GalleryRun, f: FinalCaptures) -> list[Story]:
    """Stories about the gallery's run once it is final: the streams that
    followed it from its launch, and its results."""
    stories = [
        Story(
            "runs-wait-run",
            "Wait for my run to finish",
            "My script launched a run, and I want one command that blocks until it ends.",
            "snouty prints each status the run passes through on stderr, then one line on "
            "stdout once the run is completed. Exit 0.",
            f.wait.args,
            succeeds_with(f"run {x.run} is completed"),
            json_capable=False,
            precaptured=f.wait,
        ),
    ]
    stories += [
        Story(
            "runs-stream-run",
            "Follow my run's properties as they happen",
            "I just launched a run, and I want to watch its properties pass and fail "
            "live, from the start of the run to its end.",
            "If the run's stream is not open yet, snouty waits for it. Then it prints "
            "one line per event "
            "as it happens: a `property` line when a property appears, `sample` lines "
            "with moments, and `verdict` lines, including the failed verdict of "
            f"{EXAMPLE_BUG_PROPERTY!r}, the example's deliberate bug. It ends with "
            "`completed <run id>` once the run is final. Exit 0.",
            stream_args(x.run),
            stream_run(x.run, json_output=False),
            json_capable=False,
            precaptured=f.stream,
        ),
        Story(
            "runs-stream-run--json",
            "Follow my run's properties from a script",
            "My script follows a run's property events as JSON, from its launch to its "
            "end, to react to a failed verdict.",
            "One JSON object per event, with an `event` key: property, sample, and "
            "verdict events, including a failed verdict, then a `completed` event with "
            "the run ID. stderr says when the stream is open, after a wait if it was "
            "not open yet. Exit 0.",
            ["--json", *stream_args(x.run)],
            stream_run(x.run, json_output=True),
            json_capable=False,
            precaptured=f.stream_json,
        ),
        Story(
            "runs-properties-run",
            "See which properties my run passed and failed",
            "My run is finished, and I want its property results.",
            "A table of the example's properties: the passing ones, and "
            f"{EXAMPLE_BUG_PROPERTY!r} failing because of the deliberate bug.",
            ["runs", "properties", x.run],
            run_properties,
        ),
        Story(
            "runs-property-run",
            "Look at the property my run failed",
            f"My run failed {EXAMPLE_BUG_PROPERTY!r}, and I want its counterexamples.",
            "Shows that property alone, in full: its ID, totals, and counterexample moments "
            "(hash/vtime), then one `snouty runs logs` next step.",
            ["runs", "property", x.run, f.bug_prop_id],
            bug_property,
            json_capable=False,
        ),
        Story(
            "runs-stream-run-final",
            "Stream my run after it finished",
            "My run is finished, and I start its property stream; I want it to end at once.",
            "stdout is one `completed <run id>` line. Exit 0.",
            ["runs", "stream", x.run],
            succeeds_with(f"completed  {x.run}"),
            json_capable=False,
        ),
    ]
    return stories


# Placeholders that enumerate every run story's slug, for `--list` and
# `--only`.
_LISTED_RUN = GalleryRun(launch=Result(["launch"], "", "", 0))
_LISTED_RESULT = Result([], "", "", 0)
_LISTED_FINAL = FinalCaptures(wait=_LISTED_RESULT)


def listed_run_slugs() -> tuple[list[str], list[str], list[str]]:
    """The slugs of the launch, ready, and final run stories."""
    return (
        [s.slug for s in build_launch_stories(_LISTED_RUN)],
        [s.slug for s in build_ready_stories(Discovery(), _LISTED_RUN, _LISTED_RESULT)],
        [s.slug for s in build_final_stories(_LISTED_RUN, _LISTED_FINAL)],
    )


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

_OUTPUT_RUBRIC = " Compare the help against the default output shown below it."
_HELP_ONLY_RUBRIC = (
    " This command mutates state or needs an interactive argument, so only its "
    "help is shown — judge the help text on its own merits and for consistency "
    "with its siblings."
)


MCP_HELP_RUBRIC = (
    "The reader is an AI agent, not a person at a terminal. It sees the tool's "
    "entry in the tool list (description, annotations, input schema) and the "
    "`tool_help` text, and nothing else. Judge whether it could **call the tool "
    "correctly** from these alone: the help names each arg by its param in the "
    "schema, never by a CLI flag; it says what the tool returns, in what shape; "
    "each example is a valid call; and nothing tells the agent to do what a "
    "tool call cannot (open a browser, pipe to `jq`, read stdin, run a command "
    "that has no tool). It should also be **concise** and **consistent** with "
    "the schema's own param descriptions, and say which tool to call next with "
    "the fields that it returns."
)


def mcp_tool_names(sn: Snouty) -> list[str]:
    """The tools that `snouty mcp` lists, in its order."""
    (listing,) = sn.mcp([("tools/list", {})])
    return [tool["name"] for tool in listing["tools"]]


def _mcp_slug(tool: str) -> str:
    return "mcp-help-" + tool.replace("_", "-")


def build_mcp_help_stories(tools: list[str]) -> list[Story]:
    # tool_help is the tool that returns the help; its own help is its entry.
    return [
        Story(
            slug=_mcp_slug(tool),
            title=f"Learn to call {tool} over MCP",
            goal=(
                f"My AI agent wants to use `{tool}`. It reads the tool's entry in the "
                f'tool list, then calls `tool_help {{"tool": "{tool}"}}` before it '
                "calls the tool."
            ),
            judge=MCP_HELP_RUBRIC,
            args=["mcp"],
            check=mcp_help_check,
            json_capable=False,
            mcp_tool=tool,
        )
        for tool in tools
        if tool != "tool_help"
    ]


def mcp_help_check(sr: StoryRun, _reg: Registry) -> tuple[bool, str]:
    # The Rust tests check what the help and the schema say; this checks
    # only that the server serves them.
    tool = sr.story.mcp_tool
    if sr.mcp_entry is None:
        return False, f"{tool} is not in the tool list"
    if not sr.result.ok:
        return False, f"tool_help failed: {sr.result.stdout.strip()[:200]}"
    text = sr.result.stdout
    if not text.startswith(f"The help of {tool}"):
        return False, "tool_help text does not start with the tool's name"
    params = len(sr.mcp_entry.get("inputSchema", {}).get("properties", {}))
    return (
        True,
        f"{len(text.splitlines())} lines of help, {params} params in the schema",
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
            align=("RUN ID", "STATUS", "CREATED", "LAUNCHER", "RUN"),
        ),
        _help_story(
            "help-runs-usage",
            "Learn what `runs usage` reports",
            "I want to know what the usage columns mean and how --rollup groups runs.",
            ["runs", "usage"],
            ["runs", "usage", "-n", "5"],
            samples=[
                ("with --rollup week", ["runs", "usage", "--rollup", "week", "-n", "4"])
            ],
            align=("RUN ID", "CORE HOURS", "STARTED", "ENDED"),
        ),
        _help_story(
            "help-runs-show",
            "Learn what `runs show` reports",
            "I want to confirm the help explains the metadata fields and the failure "
            "moment shown for incomplete runs.",
            ["runs", "show"],
            ["runs", "show", s],
            samples=[
                (
                    "an incomplete run (shows the failure moment)",
                    ["runs", "show", d.fail],
                )
            ],
            # show prints a key/value card (prose help vs Title-Case labels), not a
            # columnar table — no strict token alignment; the reviewer compares the
            # prose field list and the failure-moment claim against the two samples.
        ),
        _help_story(
            "help-runs-wait",
            "Learn how to wait for a run to start or finish",
            "I want the help to explain the two --until statuses (ready and complete), "
            "that each is a minimum, that an `unknown` status fails the command, and what "
            "--poll-interval and --timeout do.",
            ["runs", "wait"],
            ["runs", "wait", s],
            samples=[
                (
                    "--until ready on a completed run (it is already complete)",
                    ["runs", "wait", s, "--until", "ready"],
                )
            ],
            # wait on a terminal run prints one status line; nothing columnar
            # to align tokens against.
        ),
        _help_story(
            "help-runs-cancel",
            "Learn what cancelling a run does",
            "I want the help to explain that cancellation is asynchronous and that a "
            "completed run cannot be cancelled.",
            ["runs", "cancel"],
            # Only a completed run is safe to cancel here: the server refuses it
            # and leaves the run unchanged.
            ["runs", "cancel", s],
            expect_ok=False,
        ),
        _help_story(
            "help-runs-properties",
            "Learn the properties table, filters, and --detail",
            "I want the help to explain the STATUS, ✓, ✗, ID, and NAME columns "
            "and the example and counterexample counts, and the --name/--group/--detail flags "
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
            align=("STATUS", "✓", "✗", "ID", "NAME"),
        ),
        _help_story(
            "help-runs-events",
            "Learn to search events and chain into logs",
            "I want the help to explain `moment HASH` dividers and `VTIME [source] payload` "
            "lines, that the hash alone feeds `runs logs`, and that several terms must all "
            "match, through the events-search API.",
            ["runs", "events"],
            ["runs", "events", s, "--match", d.event_keyword],
        ),
        _help_story(
            "help-runs-search",
            "Learn the event-set DSL query command",
            "I want the help to explain the QUERY syntax (verbs), the tenant release the "
            "events-search API needs, the mode switches, --count, and the output line format.",
            ["runs", "search"],
            ["runs", "search", s, f'contains({{output_text: "{d.event_keyword}"}})'],
        ),
        _help_story(
            "help-runs-logs",
            "Learn how the hash and optional vtime select logs",
            "I want the help to explain that a hash streams to the branch's current end, "
            "an optional vtime sets an earlier end, and --begin-vtime sets the start.",
            ["runs", "logs"],
            ["runs", "logs", s, d.logs_hash],
            samples=[
                (
                    "with an explicit end vtime",
                    ["runs", "logs", s, d.logs_hash, d.logs_vtime],
                )
            ],
        ),
        _help_story(
            "help-runs-property",
            "Learn how to look up one property",
            "I want the help to tell me where a property ID comes from and what the lookup shows.",
            ["runs", "property"],
            ["runs", "property", s, d.fail_prop_id],
        ),
        _help_story(
            "help-runs-build-logs",
            "Learn what build-logs streams",
            "I want the help to tell me this is the build/setup log and the line format.",
            ["runs", "build-logs"],
            ["runs", "build-logs", s],
        ),
        _help_story(
            "help-runs-stream",
            "Learn what the property stream shows",
            "I want the help to tell me what each line means, when the stream ends, and how to "
            "get the current state as well as the changes.",
            ["runs", "stream"],
            ["runs", "stream", s],
        ),
        _help_story(
            "help-runs-exec",
            "Learn how to execute a script in a live run",
            "I want the help to tell me that a script needs a run that is ready, "
            "a test run or a debugging session, that the command waits until it is ready, "
            "which tenant "
            "release the command needs, how "
            "to pick the moment and the container, when a moment needs "
            "--source-run-id, where the script's output goes, "
            "what --events prints "
            "instead, and how to chain a follow-up command. Help-only: the "
            "`runs-exec*` stories show its output in the gallery's live run.",
            ["runs", "exec"],
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
            "I run `snouty launch --help` to learn how to start a test run. The environment "
            "variable list should describe each setting without implying that its variable "
            "is required or preferred. Username/password should remain marked deprecated. "
            "The help should say that credentials come from `snouty login` and name the next "
            "step, `snouty runs wait <run_id>`, and how to watch the run live with "
            "`snouty runs stream <run_id>`. It should say that a pre-built config "
            "image turns off the `vcs.*` detection. `--performance-tier` should list the "
            "three tiers: standard, fast, and turbo. `--fault-tolerance` should list the four "
            "levels: none, resilient, high_availability, and legacy, and say what each one "
            "injects. `--fault-exclusions` should say that the pattern is a Rust regex, "
            "matched against the container name. No code span in a flag's help should "
            "split across two lines. Each example for a launcher that needs a config image "
            "should pass one, so that I can copy it as written.",
            ["launch"],
        ),
        _help_story(
            "help-debug",
            "Understand how to open a debugging session",
            "I run `snouty debug --help` to learn how to debug a moment. The usage line "
            "should show that --input-hash, --vtime, and --run-id are required, and that "
            "--session-id is only for a moment that names a session ID.",
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
            "I run `snouty completions --help` to learn how to install completions. "
            "For zsh, I want a complete .zshrc setup that initializes completion before "
            "sourcing the script. Advice for shells that already initialize completion "
            "should come before the setup example. Every shell's setup should generate "
            "completions at startup, so they stay current after `snouty update`.",
            ["completions"],
        ),
        _help_story(
            "help-update",
            "Check for updates",
            "I run `snouty update --help` to understand what updating does.",
            ["update"],
        ),
        _help_story(
            # Help-only: get reads the user's settings and credentials, and
            # prints a secret.
            "help-docker-credential",
            "Let Docker or Podman push to my tenant registry",
            "I run `snouty docker-credential --help` to learn how to set up Docker or "
            "Podman for registry.<tenant>.antithesis.com, and where the helper reads my "
            "settings.",
            ["docker-credential"],
        ),
        _help_story(
            "help-mcp",
            "Connect an AI agent over MCP",
            "I run `snouty mcp --help` to learn how to give my AI agent snouty's tools, "
            "and what those tools can do.",
            ["mcp"],
        ),
        _help_story(
            "help-settings",
            "Understand the settings command group",
            "I run `snouty settings --help` to learn why snouty asks me to trust a "
            "settings file, and how to see what snouty resolved.",
            ["settings"],
        ),
        _help_story(
            "help-settings-show",
            "Learn where my settings come from",
            "I run `snouty settings show --help` to learn which value wins when a setting "
            "is set in more than one place.",
            ["settings", "show"],
            ["settings", "show"],
        ),
        # Help-only: `settings trust` and `settings deny` write the trust record.
        _help_story(
            "help-settings-trust",
            "Learn what trusting a settings file does",
            "I run `snouty settings trust --help` before I trust a file that came with a "
            "repository.",
            ["settings", "trust"],
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


def resolve_compose_command() -> list[str] | None:
    """Resolve a Docker Compose v2 command: the standalone `docker-compose`
    binary or the `docker compose` plugin (snouty supports either). Returns the
    command as a list of argv tokens, or None if neither is a working Compose v2.

    A v1 `docker-compose` is rejected — its `version` banner reads
    `docker-compose version 1.x` (hyphenated), while v2 (binary or plugin) reads
    `Docker Compose version v2.x`, so requiring the un-hyphenated 'docker compose'
    substring accepts exactly Compose v2."""
    for cmd in (["docker-compose"], ["docker", "compose"]):
        try:
            ver = subprocess.run([*cmd, "version"], capture_output=True, text=True)
        except FileNotFoundError:
            continue
        if ver.returncode == 0 and "docker compose" in ver.stdout.lower():
            return cmd
    return None


def ensure_validate_runtime() -> None:
    """Verify a container runtime is ready for the validate stories and build the
    sample images, raising GalleryError if anything is missing.

    gen-gallery is a developer tool, so a container runtime is required — not
    optional. `snouty validate` resolves docker/podman before it even inspects a
    config, so *every* validate story (including the static misconfiguration
    checks) needs the runtime, and the live samples additionally need a running
    daemon to start containers. The live samples bake their test commands into
    images and `snouty validate` never builds or pulls, so the images must exist
    first — built here via scripts/build-validate-samples.sh."""
    try:
        if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
            raise GalleryError(
                "Docker daemon not reachable (`docker info` failed). gen-gallery "
                "is a developer tool and requires a running container runtime — "
                "start Docker and retry."
            )
    except FileNotFoundError as e:
        raise GalleryError(
            f"container runtime not found ({e.filename}): gen-gallery requires "
            "docker on PATH."
        ) from e
    if resolve_compose_command() is None:
        raise GalleryError(
            "Docker Compose v2 not available: gen-gallery needs either the "
            "`docker-compose` binary or the `docker compose` plugin (whichever is "
            "present must report Compose v2). Install one and retry."
        )
    print("building validate sample images…", file=sys.stderr)
    build = subprocess.run(
        ["bash", str(BUILD_SAMPLES_SCRIPT)], capture_output=True, text=True
    )
    if build.returncode != 0:
        tail = (build.stderr or build.stdout).strip()
        raise GalleryError(f"validate sample image build failed:\n{tail}")


@functools.cache
def ensure_example_images() -> None:
    """Build the example project's images, which `snouty validate` and
    `snouty launch` need in the local image store. Both call this, and the
    build runs once."""
    compose = resolve_compose_command()
    if compose is None:
        raise GalleryError("Docker Compose v2 is needed to build the example's images")
    print("building the example project's images…", file=sys.stderr)
    build = subprocess.run(
        [*compose, "-f", str(EXAMPLE_DIR / "docker-compose.yaml"), "build", "-q"],
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        tail = (build.stderr or build.stdout).strip()
        raise GalleryError(f"the example's image build failed:\n{tail}")


def build_validate_stories(ephemeral: Path | None) -> list[Story]:
    """`snouty validate` stories, run against the committed sample projects under
    tests/fixtures/validate (each sample has its own README). The static
    misconfiguration samples fail fast (but still need the runtime resolved); the
    live ones (`needs_docker`) start real containers. All require a container
    runtime — `ensure_validate_runtime` enforces that before these run.

    Two degenerate inputs can't be committed — a non-existent directory and an
    empty `manifests/` dir (git can't track an empty directory) — so they're
    synthesized under `ephemeral` at run time. In `--list` mode `ephemeral` is
    None and they fall back to placeholder paths (their args are never run)."""
    s = SAMPLES_DIR
    missing_dir = s / "does-not-exist"  # deliberately never created
    if ephemeral is not None:
        empty_manifests = ephemeral / "empty-manifests"
        (empty_manifests / "manifests").mkdir(parents=True, exist_ok=True)
    else:
        empty_manifests = s / "empty-manifests"  # placeholder; not run for --list

    def v(slug, title, goal, judge, sample_args, check, *, needs_docker=False):
        return Story(
            slug=slug,
            title=title,
            goal=goal,
            judge=judge,
            args=["validate", *sample_args],
            check=check,
            json_capable=False,  # validate isn't a list/stream command
            needs_docker=needs_docker,
        )

    return [
        # -- static misconfigurations (detected before any container starts) --
        v(
            "validate-not-a-config",
            "Validate a directory that isn't a config",
            "I point validate at a directory with no compose file or manifests/.",
            "The error says no docker-compose.yaml or manifests/ was found, so I know what's missing.",
            [str(s / "neither")],
            fails_with(
                "does not contain a docker-compose.yaml file or a manifests/ subdirectory"
            ),
        ),
        v(
            "validate-missing-dir",
            "Validate a path that doesn't exist",
            "I mistype the path to my config directory.",
            "The error says the path is not a directory, rather than something cryptic.",
            [str(missing_dir)],
            fails_with("is not a directory"),
        ),
        v(
            "validate-wrong-extension",
            "Validate a .yml compose file",
            "My compose file is named docker-compose.yml instead of .yaml.",
            "The error names the wrong filename and suggests the exact rename.",
            [str(s / "wrong-extension")],
            fails_with(
                "not the required docker-compose.yaml",
                "rename it to docker-compose.yaml",
            ),
        ),
        v(
            "validate-ambiguous",
            "Validate a dir with both compose and manifests",
            "My directory has both a docker-compose.yaml and a manifests/ subdirectory.",
            "The error explains the ambiguity and says to provide one or the other.",
            [str(s / "ambiguous")],
            fails_with(
                "contains both docker-compose.yaml and a manifests/ subdirectory"
            ),
        ),
        v(
            "validate-empty-manifests",
            "Validate an empty manifests/ directory",
            "My manifests/ directory is empty.",
            "The error says the manifests/ dir is empty, not something obscure further along.",
            [str(empty_manifests)],
            fails_with("empty manifests/ subdirectory"),
        ),
        v(
            "validate-malformed-compose",
            "Validate a broken compose file",
            "My docker-compose.yaml has a YAML syntax error.",
            "The error shows compose config failed and includes the parser's message.",
            [str(s / "malformed-compose")],
            # snouty echoes the compose command it used, which differs between the
            # standalone binary (`docker-compose config`) and the plugin (`docker
            # compose config`); assert only the spelling-agnostic tail.
            fails_with("compose config' failed"),
        ),
        v(
            "validate-no-services",
            "Validate a compose file with no services",
            "My compose file declares no services.",
            "The error states plainly that no services were found.",
            [str(s / "no-services")],
            fails_with("no services found in docker-compose.yaml"),
        ),
        v(
            "validate-external-network",
            "Validate a compose file with an external network",
            "My compose file references an external network.",
            "The error explains an external network can't work on Antithesis.",
            [str(s / "external-network")],
            fails_with("declared as external"),
        ),
        v(
            "validate-build-only",
            "Validate a service that has only a build: section",
            "My compose service has a `build:` section but no `image:` name.",
            "The error says every service needs an explicit `image:` reference and names "
            "the service that has none, before any container starts.",
            [str(s / "build-only")],
            fails_with(
                "every compose service needs an explicit `image:` reference",
                "service 'app' has no `image:` field",
            ),
        ),
        v(
            "validate-missing-image",
            "Validate when a service image is missing locally",
            "A service references an image I haven't built or pulled.",
            "The error lists the missing image and says snouty does not pull or build images.",
            [str(s / "missing-image")],
            fails_with("some images are not available locally"),
            needs_docker=True,
        ),
        # -- live container runs (require a Docker daemon) --------------------
        v(
            "validate-valid",
            "Validate a correct harness",
            "I validate a well-formed harness before launching it.",
            "Setup-complete is detected and the discovered test commands are summarized; exit is clean.",
            [str(s / "valid")],
            succeeds_with(
                "Setup-complete event detected", "Setup validation successful"
            ),
            needs_docker=True,
        ),
        v(
            "validate-timeout",
            "Validate a harness that never signals setup-complete",
            "My harness never emits the setup-complete event.",
            "snouty waits up to --timeout, then fails with the timeout it used and the likely causes, most likely first.",
            [str(s / "timeout"), "--timeout", "5s"],
            fails_with(
                "timed out waiting for setup-complete event (5s)",
                "make sure your workload emits setup_complete",
                "raise --timeout",
                "may not see this machine's temp directory",
            ),
            needs_docker=True,
        ),
        v(
            "validate-unrecognized-command",
            "Validate a harness with an unknown test command",
            "One of my test commands has a name with no recognized prefix.",
            "Discovery fails and the offending command is named.",
            [str(s / "unrecognized-command")],
            fails_with("test command discovery failed", "Unrecognized command names"),
            needs_docker=True,
        ),
        v(
            "validate-non-executable-command",
            "Validate a harness with a non-executable test command",
            "One of my test commands is missing its executable bit.",
            "Discovery fails and the non-executable command is named.",
            [str(s / "non-executable-command")],
            fails_with("test command discovery failed", "are not executable"),
            needs_docker=True,
        ),
        v(
            "validate-example",
            "Validate the example project before I launch it",
            "I built the images of the example project in examples/antithesis, and I "
            "validate it before I launch a run of it.",
            "Setup-complete is detected, and the summary counts the example's test "
            "commands: one driver, one anytime, and one eventually command. Exit 0.",
            [str(EXAMPLE_DIR)],
            succeeds_with(
                "Setup-complete event detected",
                "1 driver, 1 anytime, 1 eventually",
                "Setup validation successful",
            ),
            needs_docker=True,
        ),
        v(
            "validate-stranded",
            "Validate a harness whose init container exits",
            "A one-shot container holds test commands but exits during startup.",
            "snouty warns those commands are stranded but still validates successfully.",
            [str(s / "stranded")],
            succeeds_with("their containers exited", "Setup validation successful"),
            needs_docker=True,
        ),
    ]


# ---------------------------------------------------------------------------
# TTY stories: `snouty login` holds a conversation on a terminal and writes
# settings.toml/credentials.toml as it goes. Each story runs in a throwaway
# `$HOME`, types a scripted dialogue at a real pseudo-terminal, and captures both
# the screens and the files written, so a reviewer judges the conversation AND
# its result. No API, discovery, or container runtime is needed. On Linux the
# keychain is a no-op, so credentials land in the file backend — the realistic
# default here.
#
# The tenant is contacted for real: `snouty login` asks it whether single
# sign-on is available before it asks for credentials, so a story shows what a
# human would see on that tenant.
# ---------------------------------------------------------------------------

# Fake secrets, never a real credential, and redacted again before embedding
# (see `_redact_secrets`). Each one has the name and value of a constant in
# src/testutils/fake_secrets.rs.
_FAKE_API_KEY_A = "antithesis_api_key_v2_fake-a_9Pgw"
_FAKE_API_KEY_B = "antithesis_api_key_v2_fake-b-longer_7Qxz"
_FAKE_PASSWORD = "fake-password"
_TENANT = "acme"
_REPO = "registry.example.com/acme/app"
_SETTINGS = ".config/snouty/settings.toml"
_CREDS = ".config/snouty/credentials.toml"
_SEED_SETTINGS_TOML = f'tenant = "{_TENANT}"\nrepository = "{_REPO}"\n'
# A `credentials.toml` exactly as `snouty login` writes it.
_SEED_CREDS_TOML = f'[default]\ntype = "ApiKey"\napi_key = "{_FAKE_API_KEY_A}"\n'

# Prompts the dialogues wait for. The gallery's tenant has no CLI OAuth, so
# login asks for the API key with no credential menu.
_ASK_TENANT = "What Antithesis tenant"
_ASK_REPO = "What container repository"
_ASK_CREDENTIALS = "What kind of credentials"
_ASK_KEY = "Please enter your API Key"
_EMPTY_KEY_REFUSED = "An empty value is not valid."
_ASK_REPLACE_PASSWORD = "Replace your stored username and password?"
_RETAINED = "Retained your previously stored credentials"
# A `credentials.toml` with a username and password, as an older snouty wrote it.
_SEED_PASSWORD_CREDS_TOML = (
    f'[default]\ntype = "Password"\nusername = "puser"\npassword = "{_FAKE_PASSWORD}"\n'
)

# Shared satisfaction rubric for the TTY stories: judge the conversation AND the
# persisted result, not just the exit code.
_TTY_RUBRIC = (
    "Judge the prompt conversation and the persisted state together. Are the "
    "prompts clear, in a sensible order, and only for values not already known? "
    "After it finishes, does the user know WHAT was saved, WHERE, and the next "
    "step (e.g. `snouty doctor`)? Are secrets never echoed? For the error/repair "
    "paths, is the message clear about what went wrong and how to recover, and is "
    "a mutating overwrite done safely (warn + back up)?"
)


def _tty_story(
    slug: str,
    title: str,
    goal: str,
    args: list[str],
    dialogue: tuple[Step, ...],
    check,
    *,
    seed_files: dict[str, str] | None = None,
    post_capture: tuple[str, ...] = (_SETTINGS, _CREDS),
) -> Story:
    return Story(
        slug=slug,
        title=title,
        goal=goal,
        judge=_TTY_RUBRIC,
        args=args,
        check=check,
        json_capable=False,
        dialogue=dialogue,
        seed_files=seed_files,
        post_capture=post_capture,
    )


def build_tty_stories() -> list[Story]:
    return [
        _tty_story(
            "login-fresh-apikey",
            "First-time setup with an API key",
            "I just installed snouty and want to configure my tenant, repository, and API key.",
            ["login"],
            (
                (_ASK_TENANT, _TENANT + ENTER),
                (_ASK_REPO, _REPO + ENTER),
                (_ASK_KEY, _FAKE_API_KEY_B + ENTER),
            ),
            tty_persisted(
                prompts=(_ASK_TENANT, _ASK_REPO, _ASK_KEY),
                absent_prompts=(_ASK_CREDENTIALS,),
                files=(
                    (_SETTINGS, (f'tenant = "{_TENANT}"', f'repository = "{_REPO}"')),
                    (_CREDS, ('type = "ApiKey"', f'api_key = "{_FAKE_API_KEY_B}"')),
                ),
                secrets_absent=(_FAKE_API_KEY_B,),
            ),
        ),
        _tty_story(
            "login-reuse-default",
            "Re-run login and keep my stored values",
            "I already logged in; re-running should offer my previous tenant, repository, and key "
            "back so I can just hit enter.",
            ["login"],
            (
                (_ASK_TENANT, ENTER),
                (_ASK_REPO, ENTER),
                (_ASK_KEY, ENTER),
            ),
            tty_persisted(
                prompts=(_TENANT, _REPO),
                # Keeping the stored key replaces nothing.
                absent_prompts=("replacing your stored",),
                files=(
                    (_SETTINGS, (f'tenant = "{_TENANT}"', f'repository = "{_REPO}"')),
                    # Every answer was a bare Enter, so the stored key must survive.
                    (_CREDS, ('type = "ApiKey"', f'api_key = "{_FAKE_API_KEY_A}"')),
                ),
                secrets_absent=(_FAKE_API_KEY_A,),
            ),
            seed_files={
                _SETTINGS: _SEED_SETTINGS_TOML,
                _CREDS: _SEED_CREDS_TOML,
            },
        ),
        _tty_story(
            "login-replaces-stored-password",
            "Move from a stored username/password to an API key",
            "An older snouty stored my username and password. Re-running login should switch me to "
            "an API key, without offering username/password again.",
            ["login"],
            (
                (_ASK_TENANT, ENTER),
                (_ASK_REPO, ENTER),
                (_ASK_KEY, _FAKE_API_KEY_B + ENTER),
                (_ASK_REPLACE_PASSWORD, "y" + ENTER),
            ),
            tty_persisted(
                prompts=(
                    _ASK_KEY,
                    _ASK_REPLACE_PASSWORD,
                    ", replacing your stored username and password.",
                ),
                absent_prompts=(_ASK_CREDENTIALS,),
                files=(
                    (_CREDS, ('type = "ApiKey"', f'api_key = "{_FAKE_API_KEY_B}"')),
                ),
                secrets_absent=(_FAKE_API_KEY_B, _FAKE_PASSWORD),
            ),
            seed_files={
                _SETTINGS: _SEED_SETTINGS_TOML,
                _CREDS: _SEED_PASSWORD_CREDS_TOML,
            },
        ),
        _tty_story(
            "login-enter-keeps-stored-password",
            "Hit Enter through login with a stored username/password",
            "An older snouty stored my username and password. I re-ran login to check my settings "
            "and hit Enter at every prompt. My working credentials must not be lost.",
            ["login"],
            (
                (_ASK_TENANT, ENTER),
                (_ASK_REPO, ENTER),
                (_ASK_KEY, ENTER),
                (_EMPTY_KEY_REFUSED, ESC),
            ),
            tty_persisted(
                prompts=(_ASK_KEY, _EMPTY_KEY_REFUSED, _RETAINED),
                files=(
                    (_CREDS, ('type = "Password"', f'password = "{_FAKE_PASSWORD}"')),
                ),
                secrets_absent=(_FAKE_PASSWORD,),
            ),
            seed_files={
                _SETTINGS: _SEED_SETTINGS_TOML,
                _CREDS: _SEED_PASSWORD_CREDS_TOML,
            },
        ),
        _tty_story(
            "login-declines-replacing-password",
            "Decline to replace a stored username/password",
            "An older snouty stored my username and password. I typed an API key at login, but "
            "then decided to keep the password for now.",
            ["login"],
            (
                (_ASK_TENANT, ENTER),
                (_ASK_REPO, ENTER),
                (_ASK_KEY, _FAKE_API_KEY_B + ENTER),
                (_ASK_REPLACE_PASSWORD, "n" + ENTER),
            ),
            tty_persisted(
                prompts=(_ASK_REPLACE_PASSWORD, _RETAINED),
                files=(
                    (_CREDS, ('type = "Password"', f'password = "{_FAKE_PASSWORD}"')),
                ),
                secrets_absent=(_FAKE_API_KEY_B, _FAKE_PASSWORD),
            ),
            seed_files={
                _SETTINGS: _SEED_SETTINGS_TOML,
                _CREDS: _SEED_PASSWORD_CREDS_TOML,
            },
        ),
        _tty_story(
            "login-verbose",
            "See the requests login sends",
            "I want to see which requests login sends to the tenant and what comes back, to "
            "debug a sign-in problem.",
            ["--verbose", "login"],
            (
                (_ASK_TENANT, ENTER),
                (_ASK_REPO, ENTER),
                (_ASK_KEY, _FAKE_API_KEY_B + ENTER),
            ),
            tty_persisted(
                # The live tenant decides the probe's outcome, so only the
                # request is checked.
                prompts=(f"> GET https://{_TENANT}.antithesis.com/auth/cli/config",),
                files=(
                    (_CREDS, ('type = "ApiKey"', f'api_key = "{_FAKE_API_KEY_B}"')),
                ),
                secrets_absent=(_FAKE_API_KEY_B,),
            ),
            seed_files={_SETTINGS: _SEED_SETTINGS_TOML},
        ),
    ]


# ---------------------------------------------------------------------------
# Rendering + main
# ---------------------------------------------------------------------------


# Default-output samples in a help story are capped — the point there is to see
# the *shape* of the output next to the help, not the full stream. The
# goal-based stories (above) still capture full, untruncated output.
HELP_SAMPLE_MAX_LINES = 18


# `snouty --version` prints `snouty VERSION (SHA)`, with `-dirty` on the sha when
# the build had uncommitted changes; a build made outside git has no sha.
_VERSION_LINE = re.compile(r"^snouty (\S+)(?: \(([0-9a-f]+)(-dirty)?\))?$")


def snouty_build(binary: Path, repo_root: Path) -> str:
    """The build under test, for every story's header: its version, its commit,
    and whether it is a release. A release is a clean build of the commit that
    the `vVERSION` tag points to; every other build is a dev build, which can
    show changes that no release has."""
    try:
        line = subprocess.run(
            [str(binary), "--version"], capture_output=True, text=True
        ).stdout.strip()
    except OSError as e:
        raise GalleryError(f"cannot run `{binary} --version`: {e}") from e
    m = _VERSION_LINE.match(line)
    if m is None:
        raise GalleryError(f"cannot parse `snouty --version` output: {line!r}")
    version, sha, dirty = m.groups()
    if sha is None:
        return (
            f"{version}, commit unknown, release unknown (the binary records no commit)"
        )
    tag = f"v{version}"
    try:
        tagged = subprocess.run(
            ["git", "rev-parse", f"--short={len(sha)}", f"{tag}^{{commit}}"],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
    except OSError:
        tagged = None
    if dirty:
        kind = "dev build (uncommitted changes)"
    elif tagged is None:
        kind = f"release unknown (cannot run git to find tag {tag})"
    elif tagged.returncode != 0:
        kind = f"dev build (no tag {tag} in this checkout)"
    elif tagged.stdout.strip() != sha:
        kind = f"dev build (not the {tag} commit)"
    else:
        kind = f"release {tag}"
    return f"{version}, commit {sha}{dirty or ''}, {kind}"


def _build_line(build: str) -> str:
    return f"**snouty build:** {build}"


def _command_line(args: list[str]) -> str:
    """`snouty <args>` as a line a reader can paste into a shell.

    Arguments are quoted where a shell would need it: several stories pass a
    value that contains spaces (`--launcher 'Basic Test git'`, a multi-word
    `--match` needle), and joining on a bare space would show a command that
    means something different from the one that ran."""
    return f"snouty {shlex.join(args)}"


def _shell_block(
    args: list[str],
    text: str,
    returncode: int,
    cap: int | None = None,
    stdin: str | None = None,
) -> str:
    """A ```shell block showing `$ snouty <args>`, its output, and the exit code
    on the line below — so a reviewer can judge whether the return code makes
    sense given the output (e.g. a clean error should be non-zero; a healthy
    listing should be zero). When `cap` is set the output is truncated to that
    many lines with a marker (help-story samples cap; full goal output does not).
    A script fed on stdin shows as a heredoc after the command."""
    lines = text.rstrip("\n").split("\n")
    if cap is not None and len(lines) > cap:
        hidden = len(lines) - cap
        lines = lines[:cap] + [f"… ({hidden} more lines)"]
    body = "\n".join(lines)
    command = _command_line(args)
    if stdin is not None:
        script = stdin.rstrip("\n")
        command += f" <<'EOF'\n{script}\nEOF"
    return f"```shell\n$ {command}\n{body}\n```\nExit code: `{returncode}`"


def _captured_block(
    out_dir: Path, name: str, result: Result, *, cap: int | None = None
) -> str:
    block = _shell_block(
        result.args, result.combined, result.returncode, cap, result.stdin
    )
    if result.cast is None:
        if "--json" not in result.args:
            return block + (
                "\n\n_Capture: pipes, stderr into the stdout pipe; no terminal emulation._"
            )
        return block + "\n\n_Capture: pipes (JSON); no terminal emulation._"
    cast_name = f"{name}.cast"
    (out_dir / cast_name).write_text(result.cast)
    return (
        block + f"\n\n_Capture: terminal, {TTY_COLS} columns × {TTY_ROWS} rows; "
        "plain-text screen and scrollback._"
        + f"\n\n_Replay colors and redraws: `asciinema play {cast_name}`_"
        + f"\n\n[Download recording]({cast_name})"
    )


def _write_help_story(
    out_dir: Path, story: Story, sr: StoryRun, verdict: str, detail: str, build: str
) -> None:
    assert sr.help_result is not None
    parts = [
        f"# {story.title}",
        _build_line(build),
        f"**User goal:** {story.goal}",
        f"**Judge satisfaction by:** {story.judge}",
        "## Help text",
        _captured_block(out_dir, f"{story.slug}-help", sr.help_result),
    ]
    if story.args:
        parts.append("## Default output")
        parts.append(
            _captured_block(
                out_dir, f"{story.slug}-default", sr.result, cap=HELP_SAMPLE_MAX_LINES
            )
        )
        for index, (label, res) in enumerate(sr.sample_results or [], 1):
            parts.append(f"### Variant: {label}")
            parts.append(
                _captured_block(
                    out_dir,
                    f"{story.slug}-variant-{index}",
                    res,
                    cap=HELP_SAMPLE_MAX_LINES,
                )
            )
    parts.append(f"_Automated check: {verdict} — {detail}_")
    (out_dir / f"{story.slug}.md").write_text("\n\n".join(parts) + "\n")


# Redact the secret values from persisted TOML before embedding it in a story —
# belt-and-suspenders on top of the fake secrets the TTY stories type.
_SECRET_LINE = re.compile(r'^(\s*(?:api_key|password)\s*=\s*)"[^"]*"', re.MULTILINE)


def _redact_secrets(text: str) -> str:
    return _SECRET_LINE.sub(r'\1"[REDACTED]"', text)


def _write_tty_story(
    out_dir: Path, story: Story, sr: StoryRun, verdict: str, detail: str, build: str
) -> None:
    # One frame per prompt, so a reviewer sees the screen the user faced at each
    # decision — including the menus, which are erased once chosen and so appear
    # nowhere in the closing screen. The keys typed are not listed separately:
    # `inquire` draws each answer next to its prompt, and the frames carry that.
    parts = [
        f"# {story.title}",
        _build_line(build),
        f"**User goal:** {story.goal}",
        f"**Judge satisfaction by:** {story.judge}",
        f"_Capture: terminal, {TTY_COLS} columns × {TTY_ROWS} rows; prompt frames._",
        "## Conversation",
        f"```shell\n$ {_command_line(story.args)}\n```",
    ]
    for prompt, screen in sr.frames or []:
        parts.append(f"### At `{prompt}`\n```\n{screen}\n```")
    parts.append(f"Exit code: `{sr.result.returncode}`")
    if sr.cast is not None:
        cast_name = f"{story.slug}.cast"
        (out_dir / cast_name).write_text(sr.cast)
        parts.append(f"_Replay the session: `asciinema play {cast_name}`_")
        parts.append(f"[Download recording]({cast_name})")
    if story.post_capture:
        parts.append("## Persisted state")
        for rel_path, contents in sr.captured_files or []:
            if contents is None:
                parts.append(f"`~/{rel_path}` — _(not written)_")
            else:
                body = _redact_secrets(contents).rstrip("\n")
                parts.append(f"`~/{rel_path}`\n```toml\n{body}\n```")
    parts.append(f"_Automated check: {verdict} — {detail}_")
    (out_dir / f"{story.slug}.md").write_text("\n\n".join(parts) + "\n")


def _write_mcp_story(
    out_dir: Path, story: Story, sr: StoryRun, verdict: str, detail: str, build: str
) -> None:
    entry = json.dumps(sr.mcp_entry, indent=2, ensure_ascii=False)
    call = json.dumps({"tool": story.mcp_tool})
    parts = [
        f"# {story.title}",
        _build_line(build),
        f"**User goal:** {story.goal}",
        f"**Judge satisfaction by:** {story.judge}",
        "_Capture: a JSON-RPC session with `snouty mcp` over stdio; "
        + "no terminal emulation._",
        "## Tool list entry",
        f"```json\n{entry}\n```",
        f"## `tool_help {call}`",
        f"```text\n{sr.result.stdout.rstrip()}\n```",
        f"isError: `{str(not sr.result.ok).lower()}`",
        f"_Automated check: {verdict} — {detail}_",
    ]
    (out_dir / f"{story.slug}.md").write_text("\n\n".join(parts) + "\n")


def write_story(
    out_dir: Path, story: Story, sr: StoryRun, passed: bool, detail: str, build: str
) -> None:
    verdict = "PASS" if passed else "FAIL"
    if story.mcp_tool is not None:
        _write_mcp_story(out_dir, story, sr, verdict, detail, build)
        return
    if story.help_cmd is not None:
        _write_help_story(out_dir, story, sr, verdict, detail, build)
        return
    if story.dialogue is not None:
        _write_tty_story(out_dir, story, sr, verdict, detail, build)
        return
    md = (
        f"# {story.title}\n\n"
        f"{_build_line(build)}\n\n"
        f"**User goal:** {story.goal}\n\n"
        f"**Judge satisfaction by:** {story.judge}\n\n"
        f"{_captured_block(out_dir, story.slug, sr.result)}\n\n"
        f"_Automated check: {verdict} — {detail}_\n"
    )
    (out_dir / f"{story.slug}.md").write_text(md)


def _write_seed_files(root: Path, seed_files: dict[str, str] | None) -> None:
    """Pre-write a story's `seed_files` under `root`, creating parent dirs."""
    for rel_path, contents in (seed_files or {}).items():
        dest = root / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(contents)


def run_tty_story(sn: Snouty, story: Story) -> StoryRun:
    """Run a TTY story in a throwaway `$HOME` so the credentials and settings it
    persists never touch the operator's real config. The home is seeded with
    `seed_files`, the dialogue is typed at a pseudo-terminal, and the
    `post_capture` files are read back before the home is removed."""
    # A short prefix on purpose: this path shows up inside snouty's own output,
    # and a long one would push those lines over the terminal's width for a
    # reason that belongs to the harness rather than to snouty.
    home = Path(tempfile.mkdtemp(prefix="snouty-tty."))

    try:
        _write_seed_files(home, story.seed_files)

        # Pin HOME, clear XDG_CONFIG_HOME (snouty treats empty as unset), and
        # drop any ambient ANTITHESIS_* credentials, so the story shows the state
        # it seeded rather than the operator's own and no real secret can reach a
        # captured file. TERM names a terminal `inquire` can draw on.
        env = sn.env_with(
            {
                "HOME": str(home),
                "TERM": "xterm-256color",
                "XDG_CONFIG_HOME": None,
                **{k: None for k in os.environ if k.startswith("ANTITHESIS_")},
                **(story.env or {}),
            }
        )
        result, frames, cast = drive_tty(
            sn.binary, story.args, env, story.dialogue or ()
        )

        captured: list[tuple[str, str | None]] = []
        for rel_path in story.post_capture:
            path = home / rel_path
            captured.append((rel_path, path.read_text() if path.is_file() else None))
        return StoryRun(
            story, result, None, captured_files=captured, frames=frames, cast=cast
        )
    finally:
        shutil.rmtree(home, ignore_errors=True)


def _run_isolated_story(sn: Snouty, story: Story) -> StoryRun:
    """Run a story with the global config dir pointed at a throwaway home, so no
    persisted `snouty login` credentials (`credentials.toml` / `settings.toml`)
    leak in. The home starts empty, which models an unconfigured machine;
    `seed_files` writes the persisted state a story needs. `$XDG_CONFIG_HOME`
    points at `<home>/.config`, so a seed path means the same here as in
    `run_tty_story`. The story runs in the home, so a seeded `.snouty.toml` is
    its project settings file, and the operator's own one stays out. These
    stories validate their primary result directly; they do not need a second
    JSON command."""
    home = Path(tempfile.mkdtemp(prefix="snouty-gallery-config."))
    try:
        _write_seed_files(home, story.seed_files)

        env = {
            **(story.env or {}),
            "XDG_CONFIG_HOME": str(home / ".config"),
            # The operator's own settings file and trust override stay out too.
            "SNOUTY_SETTINGS_PATH": None,
            "SNOUTY_SETTINGS_TRUST": None,
        }
        result = sn.run(story.args, env, cwd=home)
        return StoryRun(story, result, None)
    finally:
        shutil.rmtree(home, ignore_errors=True)


def run_mcp_story(sn: Snouty, story: Story) -> StoryRun:
    listing, help_ = sn.mcp(
        [
            ("tools/list", {}),
            (
                "tools/call",
                {"name": "tool_help", "arguments": {"tool": story.mcp_tool}},
            ),
        ]
    )
    entry = next((t for t in listing["tools"] if t["name"] == story.mcp_tool), None)
    text = "\n".join(block.get("text", "") for block in help_["content"])
    returncode = 1 if help_.get("isError") else 0
    return StoryRun(story, Result(["mcp"], text, "", returncode), None, mcp_entry=entry)


def run_story(sn: Snouty, story: Story) -> StoryRun:
    if story.precaptured is not None:
        return StoryRun(story, story.precaptured, None)
    if story.mcp_tool is not None:
        return run_mcp_story(sn, story)
    if story.dialogue is not None:
        return run_tty_story(sn, story)
    if story.isolate_config:
        return _run_isolated_story(sn, story)
    # Help-only stories pass no `args`; don't invoke a bare `snouty`.
    result = (
        sn.run(story.args, story.env, story.stdin, story.pipe)
        if story.args
        else Result([], "", "", 0)
    )
    rows = None
    if story.json_capable and story.args:
        try:
            rows = sn.json_lines(story.args, story.env)
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
    parser.add_argument(
        "--snouty", type=Path, help="snouty binary (default: target/debug/snouty)"
    )
    parser.add_argument(
        "--build",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="cargo build before running (default: yes)",
    )
    parser.add_argument(
        "--out", type=Path, help="output dir (default: a fresh tempdir)"
    )
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="SLUG",
        help="only generate stories matching these slugs (globs allowed, e.g. 'login-*')",
    )
    parser.add_argument(
        "--run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="record the run stories, which follow one real "
        f"{GALLERY_RUN_MINUTES}-minute run of examples/antithesis from its launch to its "
        "end and execute the runs exec stories in it; they reuse a live run that an "
        "earlier gallery launched, or launch one, and take about 25 minutes "
        "(default: yes)",
    )
    parser.add_argument("--list", action="store_true", help="list story slugs and exit")
    parser.add_argument(
        "--fail-fast", action="store_true", help="stop at the first failing story"
    )
    args = parser.parse_args()

    if args.list:
        # An all-default Discovery is enough to enumerate slugs (build_stories
        # only reads a few fields, and logs_vtime defaults to a real vtime).
        for s in build_stories(Discovery()):
            print(s.slug)
        for s in build_validate_stories(None):
            print(s.slug)
        for s in build_tty_stories():
            print(s.slug)
        # The MCP tool help stories come from the tools that the binary lists.
        binary = args.snouty or repo_root / "target" / "debug" / "snouty"
        if binary.exists():
            listing = Snouty(binary.resolve())
            try:
                for s in build_mcp_help_stories(mcp_tool_names(listing)):
                    print(s.slug)
            finally:
                listing.cleanup()
        for slugs in listed_run_slugs():
            for slug in slugs:
                print(slug)
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
    try:
        build = snouty_build(snouty_bin, repo_root)
    except GalleryError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"using binary: {snouty_bin} ({build})", file=sys.stderr)

    out_dir = args.out or Path(tempfile.mkdtemp(prefix="snouty-gallery."))
    out_dir.mkdir(parents=True, exist_ok=True)

    # The uncommittable validate fixtures (a non-existent dir, an empty
    # manifests/ dir) are synthesized here for this run only.
    fixtures_dir = Path(tempfile.mkdtemp(prefix="snouty-gallery-fixtures."))

    # Which story groups does this run actually need? The TTY stories need no
    # API, discovery, or container runtime, so `--only login-*` must not drag in
    # (and fail on) live-API discovery or a Docker daemon. Decide up front from
    # cheaply-enumerable slugs which groups are in scope.
    def selected(slug: str) -> bool:
        return not args.only or any(fnmatch.fnmatch(slug, pat) for pat in args.only)

    sn = Snouty(snouty_bin)
    try:
        mcp_stories = build_mcp_help_stories(mcp_tool_names(sn))
    except (GalleryError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
        sn.cleanup()
        print(f"error: cannot list the MCP tools: {e}", file=sys.stderr)
        return 1

    api_slugs = {s.slug for s in build_stories(Discovery())}
    launch_slugs, ready_slugs, final_slugs = (
        listed_run_slugs() if args.run else ([], [], [])
    )
    validate_slugs = {s.slug for s in build_validate_stories(None)}
    tty_slugs = {s.slug for s in build_tty_stories()}
    need_ready = any(selected(s) for s in ready_slugs)
    need_final = any(selected(s) for s in final_slugs)
    need_run = need_ready or need_final or any(selected(s) for s in launch_slugs)
    # The exec stories need discovery: the cold-moment story uses a moment of
    # the incomplete run.
    need_api = need_ready or any(selected(s) for s in api_slugs)
    need_validate = any(selected(s) for s in validate_slugs)

    if args.only:
        known = [
            *api_slugs,
            *launch_slugs,
            *ready_slugs,
            *final_slugs,
            *validate_slugs,
            *tty_slugs,
            *(s.slug for s in mcp_stories),
        ]
        unmatched = [
            p for p in args.only if not any(fnmatch.fnmatch(s, p) for s in known)
        ]
        if unmatched:
            print(f"error: --only matched no stories: {unmatched}", file=sys.stderr)
            sn.cleanup()
            return 1

    failures: list[tuple[str, str]] = []
    guard: LaunchGuard | None = None
    run: GalleryRun | None = None
    finished = False
    try:
        # The phases, in order:
        #   1. validate the example, then launch the gallery's run (or reuse a
        #      live one) and start following its stream in the background;
        #   2. discovery, then capture the regular stories while the run starts;
        #   3. once the run is ready, capture the stories that execute in it;
        #   4. once the run is final, capture the stories about its results.
        # Checks are evaluated at the end, in that order.
        validate_stories: list[Story] = []
        if need_validate:
            # Validate stories run against the committed sample projects, all of
            # which require a container runtime (`snouty validate` resolves
            # docker/podman before inspecting any config). gen-gallery is a
            # developer tool, so the runtime is mandatory: build the sample images
            # and hard-fail if it isn't available, rather than silently dropping
            # stories.
            ensure_validate_runtime()
            if selected("validate-example"):
                ensure_example_images()
            validate_stories = build_validate_stories(fixtures_dir)
            n_live = sum(s.needs_docker for s in validate_stories)
            print(
                f"including all {len(validate_stories)} validate stories "
                f"({n_live} start live containers)",
                file=sys.stderr,
            )
        # Validate the example before it is launched, as a user would.
        early = [
            s
            for s in validate_stories
            if s.slug == "validate-example" and selected(s.slug)
        ]
        captured = [run_story(sn, s) for s in early]

        stream: Future[Result] | None = None
        stream_json: Future[Result] | None = None
        rewarm: Future[tuple[str, dict, dict]] | None = None
        if need_run:
            # When `--only` selects `launch`, launch a new run so the story has
            # a launch to show.
            reuse = not (args.only and selected("launch"))
            run = gallery_run(sn, reuse)
            if run.launch is None and selected("launch"):
                print(
                    "  no launch story: the gallery reused a live run", file=sys.stderr
                )
            # The stream shows only the events after it starts, so it starts
            # right after the launch.
            args_stream = stream_args(run.run)
            if selected("runs-stream-run"):
                stream = in_background(
                    lambda: sn.run(args_stream, timeout=STREAM_TIMEOUT)
                )
            if selected("runs-stream-run--json"):
                stream_json = in_background(
                    lambda: sn.run(["--json", *args_stream], timeout=STREAM_TIMEOUT)
                )
            if need_ready:
                # The rewarm source is another run, so it is found while the
                # gallery's run starts.
                target = run.run
                rewarm = in_background(lambda: _rewarm_moments(sn, target))

        regular: list[Story] = []
        disc = Discovery()
        if need_api:
            disc = discover(sn)
            guard = LaunchGuard.for_tenant(sn)
            regular += build_stories(disc, guard)
        regular += [s for s in validate_stories if s not in early]
        # TTY and MCP tool help stories need no external dependencies, so they
        # always run.
        regular += build_tty_stories()
        regular += mcp_stories
        regular = [s for s in regular if selected(s.slug)]

        # Capture concurrently (subprocess + API roundtrips dominate), preserving
        # story order in the results list. Checks are then evaluated serially in
        # that order; no check currently depends on another story's registry
        # entry, but the ordered evaluation keeps that option open.
        # The stories in the run get a pool of their own, so they need not wait
        # for the regular stories while the run's minutes run out.
        with (
            ThreadPoolExecutor(max_workers=CAPTURE_WORKERS) as pool,
            ThreadPoolExecutor(max_workers=CAPTURE_WORKERS) as run_pool,
        ):
            print(f"capturing {len(regular)} stories…", file=sys.stderr)
            pending = [pool.submit(run_story, sn, s) for s in regular]

            if run is not None:
                captured += [
                    run_story(sn, s)
                    for s in build_launch_stories(run)
                    if selected(s.slug)
                ]
            if run is not None and rewarm is not None:
                # The run must accept commands before the exec stories.
                print(f"waiting until {run.run} is ready…", file=sys.stderr)
                wait_ready = sn.run(
                    [
                        "runs",
                        "wait",
                        run.run,
                        "--until",
                        "ready",
                        "--timeout",
                        RUN_READY_TIMEOUT,
                    ]
                )
                if not wait_ready.ok:
                    raise GalleryError(
                        f"run {run.run} did not become ready: {wait_ready.combined}"
                    )
                prepare_exec(sn, run, rewarm.result())
                ready = [
                    s
                    for s in build_ready_stories(disc, run, wait_ready)
                    if selected(s.slug)
                ]
                print(f"capturing {len(ready)} stories in {run.run}…", file=sys.stderr)
                pending += [run_pool.submit(run_story, sn, s) for s in ready]
            captured += [f.result() for f in pending]

        if run is not None and need_final:
            print(f"waiting until {run.run} is final…", file=sys.stderr)
            final = FinalCaptures(
                wait=sn.run(["runs", "wait", run.run, "--timeout", RUN_FINAL_TIMEOUT]),
                stream=stream.result() if stream is not None else None,
                stream_json=stream_json.result() if stream_json is not None else None,
            )
            props = sn.json_lines(["runs", "properties", run.run])
            final.bug_prop_id = property_id_named(props, EXAMPLE_BUG_PROPERTY)
            if not final.bug_prop_id:
                raise GalleryError(
                    f"run {run.run} has no property named {EXAMPLE_BUG_PROPERTY!r}"
                )
            captured += [
                run_story(sn, s)
                for s in build_final_stories(run, final)
                if selected(s.slug)
            ]

        reg = Registry()
        for sr in captured:
            story = sr.story
            if sr.rows is not None:
                reg.row_counts[story.slug] = len(sr.rows)
            passed, detail = story.check(sr, reg)
            write_story(out_dir, story, sr, passed, detail, build)
            mark = "ok  " if passed else "FAIL"
            print(f"  {mark} {story.slug:<32} {detail}", file=sys.stderr)
            if not passed:
                failures.append((story.slug, detail))
                if args.fail_fast:
                    # Everything was already captured; just stop reporting here.
                    break
        finished = True
    except GalleryError as e:
        # A precondition could not be met. Fail loudly and clearly rather than
        # emitting a partial gallery or dumping a traceback.
        print(f"\nerror: {e}", file=sys.stderr)
        return 1
    finally:
        if not finished and run is not None and run.launch is not None:
            # A run that the gallery launched would go on for its full
            # duration, and the streams with it. A reused run is not the
            # gallery's to cancel.
            abandon_run(sn, run.run)
        if guard is not None:
            guard.close()
        sn.cleanup()
        shutil.rmtree(fixtures_dir, ignore_errors=True)

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
