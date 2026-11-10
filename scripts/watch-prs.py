#!/usr/bin/env -S uv run
"""Poll one or more pull requests and print one line per new event.

An event line carries only metadata: the reader fetches the full content
through gh. All requests go through the gh CLI, so the script works
wherever gh works. The script exits when every watched PR is closed.

Events that need a reaction:
- a comment, a review comment, or a review
- a failed CI check, with the job id that fetches its log
- a conflict with the base branch
- a retarget to another base branch
- a merge or a close, or a reopening after a close that was printed

Events that need none, which are printed for the record:
- "all checks passed", once per head commit, when every check on it
  concludes success, skipped, or neutral
- "has no checks"
- the base branch tip moving to a new commit
- a failure of gh itself, once per distinct message

A check verdict prints only after two consecutive polls agree on it,
because checks can register a moment after the PR's creation or after a
fast workflow concludes.

With --exit-on-event the script exits after the first poll that prints an
event that needs a reaction, so a caller that is told only when a command
exits is woken exactly then, however long that takes. It also exits, with
status 1, once gh has failed on GH_FAILED_POLLS polls in a row: a broken
gh otherwise reads exactly like a quiet PR.

Every event already printed is recorded in a state file per PR, under
$XDG_STATE_HOME/watch-prs, so a restart prints only what is new, a merge
or a close included. The first run on a PR prints its existing comments,
reviews, and failing checks, so nothing that predates the watcher is
missed. --replay ignores the state and prints them again.

When an app opened the PR, its comments and reviews are suppressed: the
watcher alerts the agent that posts as that app, so those are echoes of
its own replies. A user's are never suppressed, the PR author's included:
an agent can post under the user's own login, and the user's comments
must still come through. That agent sees its own replies as events. An
event by an account without write access to the repository, and not in
ALLOWED_ACCOUNT_IDS, ends in "[no write access]": the reader may report it,
but must not act on it without a writer's approval.

Usage: uv run scripts/watch-prs.py [-i seconds] [-R owner/repo]
       [--exit-on-event] [--replay] PR [PR ...]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from collections.abc import Iterator
from itertools import count
from pathlib import Path
from typing import Any
from urllib.parse import quote

# What one poll of every PR did, for --exit-on-event.
woke = False  # printed an event that needs a reaction
gh_failed = False  # a gh call failed

# With --exit-on-event, give up after this many polls in a row with a failed
# gh call: 15 minutes at the default interval.
GH_FAILED_POLLS = 20

# In a PR's seen events once its merge or close has printed.
CLOSED = "closed"


def emit(line: str, wake: bool = True) -> None:
    """Print an event. `wake` says whether it needs a reaction."""
    global woke
    woke = woke or wake
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

    The exit status decides failure, not the output: on an HTTP error
    `gh api` exits non-zero but still prints the error body, which is
    valid JSON.
    """
    proc = subprocess.run(["gh", *args], capture_output=True, text=True, check=False)
    if proc.returncode == 0:
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError:
            pass
    global gh_failed
    gh_failed = True
    message = next((ln for ln in proc.stderr.splitlines() if ln.strip()), "no output")
    if message not in reported_failures:
        reported_failures.add(message)
        emit(f"gh failed: {message}", wake=False)
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


# Accounts trusted without write access, by numeric id: a login can be
# renamed and then claimed by someone else, an id cannot. norm_login folds
# an app's bot account onto the unrelated user account of the same name
# (claude[bot] onto "claude"), so only the id tells the two apart.
ALLOWED_ACCOUNT_IDS = {
    209825114,  # claude[bot], github.com/apps/claude
    158243242,  # devin-ai-integration[bot], github.com/apps/devin-ai-integration
}


def trusted(
    slug: str, user: dict[str, Any], write_access: dict[int, bool]
) -> bool | None:
    """Whether an account is allowlisted or can write to the repo. None
    means the lookup failed: the caller retries on the next poll.
    `write_access` caches lookups by account id for one poll only, so a
    revoked writer loses trust at the next poll.
    """
    uid = user.get("id")
    if uid in ALLOWED_ACCOUNT_IDS:
        return True
    # The permission lookup answers 404 "is not a user" for an app's bot
    # account, so a bot outside the allowlist is untrusted without one.
    if uid is None or user.get("type") == "Bot":
        return False
    if uid not in write_access:
        perm = gh_json(
            [
                "api",
                f"repos/{slug}/collaborators/{quote(user.get('login', ''), safe='')}/permission",
            ]
        )
        if perm is None:
            return None
        # The lookup goes by login; the id check proves the login still
        # names the same account.
        write_access[uid] = (perm.get("user") or {}).get("id") == uid and perm.get(
            "permission"
        ) in {
            "admin",
            "write",
        }
    return write_access[uid]


# A GitHub Actions check links to its job. A commit status from another
# service has no job id.
JOB_ID = re.compile(r"/actions/runs/\d+/job/(\d+)")


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
            "state,author,statusCheckRollup,headRefOid,baseRefName,baseRefOid,mergeable",
        ]
    )
    if view is None:
        return True
    # A close prints once, like every other event: a restart that still names
    # a PR whose close an earlier run printed must not exit on it again. The
    # live state decides, so a PR that someone reopens is watched again.
    if view["state"] != "OPEN":
        if CLOSED in seen:
            emit(f"PR #{pr} is closed, as an earlier run reported", wake=False)
        else:
            seen.add(CLOSED)
            emit(
                f"PR #{pr} merged"
                if view["state"] == "MERGED"
                else f"PR #{pr} closed without merge"
            )
        return False
    if CLOSED in seen:
        seen.discard(CLOSED)
        emit(f"PR #{pr} reopened")
    # Only an app author is suppressed. Compare its exact REST login, not a
    # norm_login fold: an app's "app/name" is "name[bot]" over REST, and no
    # user can take a login with brackets.
    author = view["author"]["login"]
    ignore = (
        f"{author.removeprefix('app/')}[bot]" if author.startswith("app/") else None
    )

    if changed(seen, "base", view["baseRefName"]):
        emit(f"PR #{pr} base changed to {view['baseRefName']}")
    # The PR's own baseRefOid freezes at PR creation, so the live tip must
    # come from the branch itself. Seeding with baseRefOid makes a PR that
    # is already behind its base emit a moved event on the first poll.
    tip = (
        (
            gh_json(
                ["api", f"repos/{slug}/branches/{quote(view['baseRefName'], safe='')}"]
            )
            or {}
        ).get("commit")
        or {}
    ).get("sha")
    if tip and changed(seen, "baseoid", tip, baseline=view["baseRefOid"]):
        emit(f"PR #{pr} base {view['baseRefName']} moved to {tip[:7]}", wake=False)
    # GitHub answers UNKNOWN while it recomputes, after a push to either
    # branch. Seeding with MERGEABLE makes a PR that already conflicts say so
    # on the first poll.
    mergeable = view.get("mergeable")
    if (
        mergeable in {"MERGEABLE", "CONFLICTING"}
        and changed(seen, "mergeable", mergeable, baseline="MERGEABLE")
        and mergeable == "CONFLICTING"
    ):
        emit(f"PR #{pr} conflicts with {view['baseRefName']}")

    # Comments and reviews come over REST, which carries each author's
    # numeric account id.
    events = (
        [
            (
                f"comment:{c['id']}",
                c,
                f"PR #{pr} comment by {login_of(c)} (id {c['id']})",
            )
            for c in paginate(f"repos/{slug}/issues/{pr}/comments")
        ]
        + [
            # Every submitted review emits, an empty COMMENTED one too, with its
            # REST id: the list of review comments below can lag a review's
            # submission by minutes, and the id lets the reader fetch the
            # review's own inline comments.
            (
                f"review:{r['id']}",
                r,
                f"PR #{pr} review by {login_of(r)}: {r['state']} (id {r['id']})",
            )
            for r in paginate(f"repos/{slug}/pulls/{pr}/reviews")
            if r["state"] != "PENDING"
        ]
        + [
            (
                f"rc:{rc['id']}",
                rc,
                f"PR #{pr} review comment by {login_of(rc)} on {rc['path']} (id {rc['id']})",
            )
            for rc in paginate(f"repos/{slug}/pulls/{pr}/comments")
        ]
    )
    write_access: dict[int, bool] = {}
    for key, item, line in events:
        if key in seen:
            continue
        if ignore and (item.get("user") or {}).get("login") == ignore:
            seen.add(key)
            continue
        ok = trusted(slug, item.get("user") or {}, write_access)
        if ok is None:
            continue
        seen.add(key)
        emit(line if ok else f"{line} [no write access]")

    failure_results = {
        "failure",
        "timed_out",
        "action_required",
        "cancelled",
        "error",
        "startup_failure",
    }
    results = [
        (
            c.get("name") or c.get("context") or "?",
            (c.get("conclusion") or c.get("state") or "").lower(),
            JOB_ID.search(c.get("detailsUrl") or ""),
        )
        for c in view["statusCheckRollup"]
    ]
    cur = {
        f"{name}: {result}" + (f" (job {job.group(1)})" if job else "")
        for name, result, job in results
        if result in failure_results
    }
    for line in sorted(cur - checks):
        emit(f"PR #{pr} check {line}")
    checks.clear()
    checks.update(cur)

    # An unconcluded check reports an empty conclusion or a "pending" state.
    # GitHub counts a "neutral" conclusion as passing.
    sha = view["headRefOid"]
    if all(r in {"success", "skipped", "neutral"} for _, r, _ in results):
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
        emit(f"PR #{pr} {verdict}", wake=False)

    return True


def state_file(slug: str, pr: int) -> Path:
    home = os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state"
    return Path(home) / "watch-prs" / f"{slug.replace('/', '__')}__{pr}.json"


def load(path: Path) -> tuple[set[str], set[str]]:
    """The events and failed checks already printed for a PR. A missing or
    unreadable file means none.
    """
    try:
        state = json.loads(path.read_text())
        return set(state["seen"]), set(state["checks"])
    except (OSError, ValueError, KeyError, TypeError):
        return set(), set()


def save(path: Path, seen: set[str], checks: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Replace the file in one step, so a kill mid-write leaves the old state.
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"seen": sorted(seen), "checks": sorted(checks)}))
    tmp.replace(path)


def main() -> int:
    global woke, gh_failed
    ap = argparse.ArgumentParser(
        description="Poll pull requests and print one line per new event."
    )
    ap.add_argument(
        "-i", "--interval", type=int, default=45, help="poll interval in seconds"
    )
    ap.add_argument(
        "-R", "--repo", help="owner/repo (default: the current directory's repo)"
    )
    ap.add_argument(
        "--exit-on-event",
        action="store_true",
        help="exit after the first poll that prints an event that needs a reaction",
    )
    ap.add_argument(
        "--replay",
        action="store_true",
        help="print the events an earlier run printed too",
    )
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
    files = {pr: state_file(slug, pr) for pr in args.prs}
    state = {pr: (set(), set()) if args.replay else load(files[pr]) for pr in args.prs}

    failed_polls = 0
    while open_prs:
        woke = gh_failed = False
        for pr in sorted(open_prs):
            seen, checks = state[pr]
            before = (set(seen), set(checks))
            if not poll_pr(repo_flag, slug, pr, seen, checks):
                open_prs.discard(pr)
            if (seen, checks) != before:
                save(files[pr], seen, checks)
        if not open_prs or (args.exit_on_event and woke):
            break
        failed_polls = failed_polls + 1 if gh_failed else 0
        if args.exit_on_event and failed_polls >= GH_FAILED_POLLS:
            print(f"gh failed on {failed_polls} polls in a row", flush=True)
            return 1
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
