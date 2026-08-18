#!/usr/bin/env -S uv run
"""Poll one or more pull requests and print one line per new event.

Events: issue comments, review comments, reviews, completed CI checks, and
close/merge. Output is one line per event, so a background monitor can react
line by line. An event line carries only metadata — the reader fetches the
full content through the API. The script exits when every watched PR is
closed.

All requests go through the gh CLI, so the script works wherever gh works.

A duplicate line is preferred over a missed one: `since` advances to each
poll's start time, and GitHub's `?since` filters on `updated_at`, so an event
that lands mid-poll emits twice and an edit of an old comment re-emits it.

Usage: uv run scripts/watch-prs.py [-i seconds] [-R owner/repo] [-u login] PR [PR ...]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from itertools import count
from typing import Any


def emit(line: str) -> None:
    print(line, flush=True)


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def gh_json(args: list[str], *, any_exit: bool = False) -> Any | None:
    """Run gh and parse its JSON output. None means a transient failure:
    the caller skips this poll and retries on the next tick.

    any_exit keeps the output despite a non-zero exit — `gh pr checks`
    exits non-zero on failed or pending checks.
    """
    proc = subprocess.run(["gh", *args], capture_output=True, text=True)
    if proc.returncode != 0 and not any_exit:
        return None
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None


def login_of(item: dict[str, Any]) -> str:
    return (item.get("user") or {}).get("login", "")


def api_host() -> str:
    """The host gh api calls must name. gh api does not resolve the host
    from the git remote the way gh pr does, so derive it the same way:
    GH_HOST when set, else the origin remote, else github.com.
    """
    if host := os.environ.get("GH_HOST"):
        return host
    proc = subprocess.run(
        ["git", "remote", "get-url", "origin"], capture_output=True, text=True
    )
    m = re.match(
        r"(?:https://|ssh://git@|git@)([^/:]+)[/:]", proc.stdout.strip()
    )
    return m.group(1) if m else "github.com"


def paginate(host: str, path: str) -> Iterator[Any]:
    # Page with explicit page params; --paginate follows Link headers, which
    # a transparent auth proxy returns pointing at the upstream host.
    sep = "&" if "?" in path else "?"
    for page in count(1):
        items = gh_json(["api", "--hostname", host, f"{path}{sep}per_page=100&page={page}"])
        if items is None:
            return
        yield from items
        if len(items) < 100:
            return


def poll_pr(host: str, repo: str, pr: int, since: str, ignore: str, seen_checks: set[str]) -> bool:
    """Emit the new events on one PR. Return False once the PR is closed."""
    info = gh_json(["api", "--hostname", host, f"repos/{repo}/pulls/{pr}"])
    if info is not None and info["state"] == "closed":
        emit(f"PR #{pr} merged" if info["merged"] else f"PR #{pr} closed without merge")
        return False

    for c in paginate(host, f"repos/{repo}/issues/{pr}/comments?since={since}"):
        if login_of(c) != ignore:
            emit(f"PR #{pr} comment by {login_of(c)} (id {c['id']})")

    for rc in paginate(host, f"repos/{repo}/pulls/{pr}/comments?since={since}"):
        if login_of(rc) != ignore:
            emit(f"PR #{pr} review comment by {login_of(rc)} on {rc['path']} (id {rc['id']})")

    # The reviews endpoint has no ?since filter; compare timestamps instead.
    for r in paginate(host, f"repos/{repo}/pulls/{pr}/reviews"):
        if (r.get("submitted_at") or "") > since and login_of(r) != ignore:
            emit(f"PR #{pr} review by {login_of(r)}: {r['state']} (id {r['id']})")

    rollup = gh_json(
        ["pr", "checks", str(pr), "-R", f"{host}/{repo}", "--json", "name,bucket"],
        any_exit=True,
    )
    if rollup is not None:
        done = {f"{c['name']}: {c['bucket']}" for c in rollup if c["bucket"] != "pending"}
        for line in sorted(done - seen_checks):
            emit(f"PR #{pr} check {line}")
        seen_checks.clear()
        seen_checks.update(done)

    return True


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Poll pull requests and print one line per new event."
    )
    ap.add_argument("-i", "--interval", type=int, default=45, help="poll interval in seconds")
    ap.add_argument("-R", "--repo", help="owner/repo (default: the current directory's repo)")
    ap.add_argument("-u", "--ignore", default="", help="suppress events from this login")
    ap.add_argument("prs", nargs="+", type=int, metavar="PR")
    args = ap.parse_args()

    host = api_host()
    repo = args.repo
    if not repo:
        view = gh_json(["repo", "view", "--json", "nameWithOwner"])
        if view is None:
            ap.error("cannot resolve the repository; pass -R owner/repo")
        repo = view["nameWithOwner"]

    open_prs = set(args.prs)
    seen_checks: dict[int, set[str]] = {pr: set() for pr in args.prs}
    since = utcnow()

    while open_prs:
        now = utcnow()
        for pr in sorted(open_prs):
            if not poll_pr(host, repo, pr, since, args.ignore, seen_checks[pr]):
                open_prs.discard(pr)
        if not open_prs:
            break
        since = now
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
