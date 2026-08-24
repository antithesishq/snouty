#!/usr/bin/env -S uv run
"""Poll one or more pull requests and print one line per new event.

Events: comments, review comments, reviews, failed CI checks, check
verdicts ("all checks passed", "has no checks"), base branch changes (a
retarget, or the base branch tip moving to a new commit), close/merge, and
a failure of gh itself.
Output is one line per event, so a background monitor can react line by
line. An event line carries only metadata — the reader fetches the full
content through gh.
The script exits when every watched PR is closed.

Comments and reviews by the PR's own author are suppressed: the watcher
alerts the author's agent, so those are echoes of its own replies. Checks
report failures. The watcher also prints one "all checks passed" line per
head commit once every check on it concludes success, skipped, or neutral.
A PR with no checks gets a "has no checks" line instead. A verdict prints
only after two consecutive polls agree on it, because checks can register
a moment after the PR's creation or after a fast workflow concludes.

All requests go through the gh CLI, so the script works wherever gh works.
The first poll emits the PR's existing comments, reviews, and failing checks
as a baseline, so nothing that predates the watcher is missed. It also
emits a base-moved line when the base branch tip already sits past the
PR's recorded base, so a PR that waits for a rebase announces it at once.

Usage: uv run scripts/watch-prs.py [-i seconds] [-R owner/repo] PR [PR ...]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from collections.abc import Iterator
from itertools import count
from typing import Any
from urllib.parse import quote


def emit(line: str) -> None:
    print(line, flush=True)


# Every gh failure already reported. A gh that fails on every poll prints
# one line, not one line per tick.
reported_failures: set[str] = set()


def gh_json(args: list[str]) -> Any | None:
    """Run gh and parse its JSON output. None means the call failed: the
    caller skips this poll and retries on the next tick. Each distinct
    failure prints one line, because a gh that fails on every poll reads
    exactly like a quiet PR — an old gh that rejects a --json field kept
    the watcher silent for a whole day.
    """
    proc = subprocess.run(["gh", *args], capture_output=True, text=True)
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        message = next((ln for ln in proc.stderr.splitlines() if ln.strip()), "no output")
        if message not in reported_failures:
            reported_failures.add(message)
            emit(f"gh failed: {message}")
        return None


def paginate(path: str) -> Iterator[Any]:
    # Page with explicit page params; --paginate follows Link headers, which
    # a transparent auth proxy returns pointing at the upstream host.
    for page in count(1):
        items = gh_json(["api", f"{path}?per_page=100&page={page}"])
        if items is None:
            return
        yield from items
        if len(items) < 100:
            return


def login_of(item: dict[str, Any]) -> str:
    return norm_login((item.get("author") or item.get("user") or {}).get("login", ""))


def norm_login(login: str) -> str:
    # One app account renders three ways across the API: "app/name" as a PR
    # author, "name" as a review author, "name[bot]" over REST.
    return login.removeprefix("app/").removesuffix("[bot]")


def changed(seen: set[str], key: str, value: str, baseline: str | None = None) -> bool:
    """Track a current-value state in `seen`. The first observation is a
    silent baseline; every later change of the value returns True. An
    explicit `baseline` pre-seeds the first observation, so a first value
    that already differs from it counts as a change.
    """
    if baseline is not None and not any(k.startswith(f"{key}:") for k in seen):
        seen.add(f"{key}:{baseline}")
    tag = f"{key}:{value}"
    if tag in seen:
        return False
    prior = {k for k in seen if k.startswith(f"{key}:")}
    seen.difference_update(prior)
    seen.add(tag)
    return bool(prior)


def poll_pr(
    repo_flag: list[str],
    slug: str,
    pr: int,
    seen: set[str],
    checks: set[str],
) -> bool:
    """Emit the events on one PR that are new since the last poll. Return
    False once the PR is closed.
    """
    view = gh_json(
        [
            "pr",
            "view",
            str(pr),
            *repo_flag,
            "--json",
            "state,author,comments,reviews,statusCheckRollup,headRefOid,baseRefName,baseRefOid",
        ]
    )
    if view is None:
        return True
    if view["state"] != "OPEN":
        emit(f"PR #{pr} merged" if view["state"] == "MERGED" else f"PR #{pr} closed without merge")
        return False
    ignore = login_of(view)

    if changed(seen, "base", view["baseRefName"]):
        emit(f"PR #{pr} base changed to {view['baseRefName']}")
    # The PR's own baseRefOid freezes at PR creation, so the live tip must
    # come from the branch itself. Seeding with baseRefOid makes a PR that
    # is already behind its base emit a moved event on the first poll.
    tip = ((gh_json(["api", f"repos/{slug}/branches/{quote(view['baseRefName'], safe='')}"]) or {}).get("commit") or {}).get("sha")
    if tip and changed(seen, "baseoid", tip, baseline=view["baseRefOid"]):
        emit(f"PR #{pr} base {view['baseRefName']} moved to {tip[:7]}")

    events = [
        (
            f"comment:{c['id']}",
            login_of(c),
            # The numeric id (usable with gh api) only appears in the URL tail.
            f"PR #{pr} comment by {login_of(c)} (id {c['url'].rpartition('issuecomment-')[2]})",
        )
        for c in view["comments"]
    ] + [
        (f"review:{r['id']}", login_of(r), f"PR #{pr} review by {login_of(r)}: {r['state']}")
        for r in view["reviews"]
        # An empty COMMENTED review only groups inline comments, which emit
        # their own events below.
        if r["state"] != "COMMENTED" or r["body"]
    ] + [
        (
            f"rc:{rc['id']}",
            login_of(rc),
            f"PR #{pr} review comment by {login_of(rc)} on {rc['path']} (id {rc['id']})",
        )
        for rc in paginate(f"repos/{slug}/pulls/{pr}/comments")
    ]
    for key, who, line in events:
        if key not in seen:
            seen.add(key)
            if who != ignore:
                emit(line)

    failure_results = {"failure", "timed_out", "action_required", "cancelled", "error", "startup_failure"}
    results = [
        (c.get("name") or c.get("context") or "?", (c.get("conclusion") or c.get("state") or "").lower())
        for c in view["statusCheckRollup"]
    ]
    cur = {f"{name}: {result}" for name, result in results if result in failure_results}
    for line in sorted(cur - checks):
        emit(f"PR #{pr} check {line}")
    checks.clear()
    checks.update(cur)

    # An unconcluded check reports an empty conclusion or a "pending" state.
    # GitHub counts a "neutral" conclusion as passing.
    sha = view["headRefOid"]
    if all(r in {"success", "skipped", "neutral"} for _, r in results):
        verdict = f"all checks passed for {sha[:7]}" if results else "has no checks"
    else:
        verdict = None
    # A green rollup can be premature: checks register a moment after the
    # PR's creation, and a slow workflow can register its checks after a
    # fast one concludes. A verdict emits only when two consecutive polls
    # agree on it.
    last = f"last:{sha}:{verdict}"
    agreed = last in seen
    seen.difference_update({k for k in seen if k.startswith("last:")})
    seen.add(last)
    if verdict and agreed and f"emitted:{last}" not in seen:
        seen.add(f"emitted:{last}")
        emit(f"PR #{pr} {verdict}")

    return True


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Poll pull requests and print one line per new event."
    )
    ap.add_argument("-i", "--interval", type=int, default=45, help="poll interval in seconds")
    ap.add_argument("-R", "--repo", help="owner/repo (default: the current directory's repo)")
    ap.add_argument("prs", nargs="+", type=int, metavar="PR")
    args = ap.parse_args()

    repo_flag = ["-R", args.repo] if args.repo else []
    slug = args.repo
    if not slug:
        view = gh_json(["repo", "view", "--json", "nameWithOwner"])
        if view is None:
            ap.error("cannot resolve the repository; pass -R owner/repo")
        slug = view["nameWithOwner"]
    open_prs = set(args.prs)
    seen: dict[int, set[str]] = {pr: set() for pr in args.prs}
    checks: dict[int, set[str]] = {pr: set() for pr in args.prs}

    while open_prs:
        for pr in sorted(open_prs):
            if not poll_pr(repo_flag, slug, pr, seen[pr], checks[pr]):
                open_prs.discard(pr)
        if not open_prs:
            break
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
