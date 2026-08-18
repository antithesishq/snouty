#!/usr/bin/env -S uv run
"""Poll one or more pull requests and print one line per new event.

Events: comments, reviews, completed CI checks, and close/merge. Output is
one line per event, so a background monitor can react line by line. An event
line carries only metadata — the reader fetches the full content through gh.
Inline review comments always belong to a review, so the review event is
their trigger: a COMMENTED review with an empty body usually carries them.
The script exits when every watched PR is closed.

All requests go through the gh CLI, so the script works wherever gh works.
The first poll seeds the baseline silently; the script reports what changes
after it starts.

Usage: uv run scripts/watch-prs.py [-i seconds] [-R owner/repo] [-u login] PR [PR ...]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from typing import Any


def emit(line: str) -> None:
    print(line, flush=True)


def gh_json(args: list[str]) -> Any | None:
    """Run gh and parse its JSON output. None means a transient failure:
    the caller skips this poll and retries on the next tick.
    """
    proc = subprocess.run(["gh", *args], capture_output=True, text=True)
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None


def login_of(item: dict[str, Any]) -> str:
    return norm_login((item.get("author") or {}).get("login", ""))


def norm_login(login: str) -> str:
    # One app account renders three ways across the API: "app/name" as a PR
    # author, "name" as a review author, "name[bot]" over REST.
    return login.removeprefix("app/").removesuffix("[bot]")


def poll_pr(
    repo_flag: list[str],
    pr: int,
    ignore: str,
    seeded: set[int],
    seen: set[str],
    checks: set[str],
) -> bool:
    """Emit the events on one PR that are new since the last poll. Return
    False once the PR is closed. The first successful poll only seeds the
    baseline.
    """
    view = gh_json(
        ["pr", "view", str(pr), *repo_flag, "--json", "state,comments,reviews,statusCheckRollup"]
    )
    if view is None:
        return True
    if view["state"] != "OPEN":
        emit(f"PR #{pr} merged" if view["state"] == "MERGED" else f"PR #{pr} closed without merge")
        return False
    first = pr not in seeded
    seeded.add(pr)

    events = [
        (f"comment:{c['id']}", login_of(c), f"PR #{pr} comment by {login_of(c)} ({c['url']})")
        for c in view["comments"]
    ] + [
        (f"review:{r['id']}", login_of(r), f"PR #{pr} review by {login_of(r)}: {r['state']} (id {r['id']})")
        for r in view["reviews"]
    ]
    for key, who, line in events:
        if key not in seen:
            seen.add(key)
            if not first and who != ignore:
                emit(line)

    cur = set()
    for c in view["statusCheckRollup"]:
        name = c.get("name") or c.get("context") or "?"
        result = (c.get("conclusion") or c.get("state") or "").lower()
        if result and result != "pending":
            cur.add(f"{name}: {result}")
    if not first:
        for line in sorted(cur - checks):
            emit(f"PR #{pr} check {line}")
    checks.clear()
    checks.update(cur)

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

    repo_flag = ["-R", args.repo] if args.repo else []
    ignore = norm_login(args.ignore)
    open_prs = set(args.prs)
    seeded: set[int] = set()
    seen: dict[int, set[str]] = {pr: set() for pr in args.prs}
    checks: dict[int, set[str]] = {pr: set() for pr in args.prs}

    while open_prs:
        for pr in sorted(open_prs):
            if not poll_pr(repo_flag, pr, ignore, seeded, seen[pr], checks[pr]):
                open_prs.discard(pr)
        if not open_prs:
            break
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
