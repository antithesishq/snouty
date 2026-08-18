#!/usr/bin/env -S uv run
"""Poll one or more pull requests and print one line per new event.

Events: issue comments, review comments, reviews, completed CI checks, and
close/merge. Output is one line per event, so a background monitor can react
line by line. The script exits when every watched PR is closed.

A duplicate line is preferred over a missed one: `since` advances to each
poll's start time, and GitHub's `?since` filters on `updated_at`, so an event
that lands mid-poll emits twice and an edit of an old comment re-emits it.

Auth: GITHUB_TOKEN when set, anonymous otherwise (an authenticating proxy
needs no token). The API host comes from the origin remote.

Usage: uv run scripts/watch-prs.py [-i seconds] [-R owner/repo] [-u login] PR [PR ...]
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from itertools import count
from typing import Any

import httpx
from githubkit import GitHub, TokenAuthStrategy
from githubkit.exception import GitHubException


def paginate(
    request: Callable[..., Any],
    unwrap: Callable[[Any], list[Any]] = lambda data: data,
    **kwargs: Any,
) -> Iterator[Any]:
    """Page with explicit page/per_page params. githubkit's own paginator
    follows the Link headers, which a transparent auth proxy returns pointing
    at the upstream host it does not serve.
    """
    for page in count(1):
        items = unwrap(request(per_page=100, page=page, **kwargs).parsed_data)
        yield from items
        if len(items) < 100:
            return


def emit(line: str) -> None:
    print(line, flush=True)


def one_line(body: object, limit: int = 300) -> str:
    if not isinstance(body, str):
        return ""
    return " ".join(body.split())[:limit]


def login_of(item: object) -> str:
    user = getattr(item, "user", None)
    return user.login if user else ""


def origin_remote() -> tuple[str, str]:
    """Host and owner/repo slug of the origin remote."""
    url = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    m = re.match(r"(?:https://|ssh://git@|git@)([^/:]+)[/:](.+?)(?:\.git)?/?$", url)
    if m is None:
        raise SystemExit(f"cannot parse the origin remote: {url}")
    return m.group(1), m.group(2)


def poll_pr(
    gh: GitHub, owner: str, repo: str, pr: int, since: datetime, ignore: str, seen_checks: set[str]
) -> bool:
    """Emit the new events on one PR. Return False once the PR is closed."""
    info = gh.rest.pulls.get(owner, repo, pr).parsed_data
    if info.state == "closed":
        emit(f"PR #{pr} merged" if info.merged else f"PR #{pr} closed without merge")
        return False

    for c in paginate(gh.rest.issues.list_comments, owner=owner, repo=repo, issue_number=pr, since=since):
        if login_of(c) != ignore:
            emit(f"PR #{pr} comment by {login_of(c)}: {one_line(c.body)}")

    for rc in paginate(
        gh.rest.pulls.list_review_comments, owner=owner, repo=repo, pull_number=pr, since=since
    ):
        if login_of(rc) != ignore:
            emit(f"PR #{pr} review comment by {login_of(rc)} on {rc.path}: {one_line(rc.body)}")

    # The reviews endpoint has no ?since filter; compare timestamps instead.
    for r in paginate(gh.rest.pulls.list_reviews, owner=owner, repo=repo, pull_number=pr):
        if r.submitted_at and r.submitted_at > since and login_of(r) != ignore:
            emit(f"PR #{pr} review by {login_of(r)}: {r.state} {one_line(r.body)}")

    done = {
        f"{run.name}: {run.conclusion}"
        for run in paginate(
            gh.rest.checks.list_for_ref,
            unwrap=lambda data: data.check_runs,
            owner=owner,
            repo=repo,
            ref=info.head.sha,
        )
        if run.status == "completed"
    }
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
    ap.add_argument("-R", "--repo", help="owner/repo (default: the origin remote's slug)")
    ap.add_argument("-u", "--ignore", default="", help="suppress events from this login")
    ap.add_argument("prs", nargs="+", type=int, metavar="PR")
    args = ap.parse_args()

    host, slug = origin_remote()
    base_url = "https://api.github.com" if host == "github.com" else f"https://{host}/api/v3"
    token = os.environ.get("GITHUB_TOKEN")
    gh = GitHub(TokenAuthStrategy(token), base_url=base_url) if token else GitHub(base_url=base_url)
    owner, repo = (args.repo or slug).split("/", 1)

    open_prs = set(args.prs)
    seen_checks: dict[int, set[str]] = {pr: set() for pr in args.prs}
    since = datetime.now(timezone.utc)

    while open_prs:
        now = datetime.now(timezone.utc)
        for pr in sorted(open_prs):
            try:
                if not poll_pr(gh, owner, repo, pr, since, args.ignore, seen_checks[pr]):
                    open_prs.discard(pr)
            except (GitHubException, httpx.HTTPError):
                pass  # transient; retry on the next tick
        if not open_prs:
            break
        since = now
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
