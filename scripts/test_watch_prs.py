import contextlib
import importlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from typing import Any
from unittest import mock

watch_prs = importlib.import_module("watch-prs")

SLUG = "owner/repo"
WRITER = {"login": "reviewer", "id": 7, "type": "User"}


def comment(cid: int, user: dict[str, Any]) -> dict[str, Any]:
    return {"id": cid, "user": user}


class FakeGh:
    """Answers the gh calls that the watcher makes for one PR."""

    def __init__(self, author: str = "app/agent"):
        self.view: dict[str, Any] = {
            "state": "OPEN",
            "author": {"login": author},
            "statusCheckRollup": [],
            "headRefOid": "a" * 40,
            "baseRefName": "main",
            "baseRefOid": "b" * 40,
            "mergeable": "MERGEABLE",
        }
        self.comments: list[dict[str, Any]] = []
        self.down = False

    def run(self, cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        if self.down:
            return subprocess.CompletedProcess(cmd, 1, "", "gh is down\n")
        return subprocess.CompletedProcess(cmd, 0, json.dumps(self.answer(cmd[1:])))

    def answer(self, args: list[str]) -> Any:
        if args[:2] == ["pr", "view"]:
            return self.view
        path = args[1]
        if "/branches/" in path:
            return {"commit": {"sha": self.view["baseRefOid"]}}
        if "/collaborators/" in path:
            return {"user": {"id": WRITER["id"]}, "permission": "write"}
        if "/issues/" in path:
            return self.comments
        return []


class WatcherTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.gh = FakeGh()
        for patch in (
            mock.patch.dict(os.environ, {"XDG_STATE_HOME": tmp.name}),
            mock.patch.object(
                watch_prs.subprocess, "run", lambda cmd, **kw: self.gh.run(cmd, **kw)
            ),
            mock.patch.object(watch_prs.time, "sleep", self.sleep),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.on_sleep = lambda: None
        self.set_woke(False)
        watch_prs.reported_failures.clear()

    def set_woke(self, value: bool) -> None:
        setattr(watch_prs, "woke", value)  # noqa: B010 (a module, to pyright)

    def sleep(self, _seconds: float) -> None:
        self.on_sleep()

    def poll(self, seen: set[str], checks: set[str] | None = None) -> list[str]:
        """The lines that one poll of PR 1 prints."""
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            watch_prs.poll_pr([], SLUG, 1, seen, set() if checks is None else checks)
        return out.getvalue().splitlines()

    def run_main(self, *argv: str) -> tuple[int, list[str]]:
        out = io.StringIO()
        with (
            mock.patch("sys.argv", ["watch-prs.py", "-R", SLUG, *argv]),
            contextlib.redirect_stdout(out),
        ):
            status = watch_prs.main()
        return status, out.getvalue().splitlines()


class Poll(WatcherTest):
    def test_user_author_is_not_suppressed(self):
        self.gh = FakeGh(author=WRITER["login"])
        self.gh.comments = [comment(5, WRITER)]
        self.assertEqual(self.poll(set()), ["PR #1 comment by reviewer (id 5)"])

    def test_app_author_is_suppressed(self):
        self.gh.comments = [comment(5, {"login": "agent[bot]", "id": 9, "type": "Bot"})]
        self.assertEqual(self.poll(set()), [])

    def test_failed_check_names_its_job(self):
        self.gh.view["statusCheckRollup"] = [
            {
                "name": "lint",
                "conclusion": "FAILURE",
                "detailsUrl": "https://github.com/owner/repo/actions/runs/3/job/44",
            },
            {"context": "external", "state": "ERROR"},
        ]
        self.assertEqual(
            self.poll(set()),
            ["PR #1 check external: error", "PR #1 check lint: failure (job 44)"],
        )
        self.assertTrue(watch_prs.woke)

    def test_pass_and_base_move_need_no_reaction(self):
        self.gh.view["statusCheckRollup"] = [{"name": "lint", "conclusion": "SUCCESS"}]
        self.gh.view["baseRefOid"] = "c" * 40
        seen: set[str] = set()
        self.poll(seen)
        self.gh.view["baseRefOid"] = "d" * 40
        self.assertEqual(
            self.poll(seen),
            ["PR #1 base main moved to ddddddd", "PR #1 all checks passed for aaaaaaa"],
        )
        self.assertFalse(watch_prs.woke)

    def test_conflict_prints_once(self):
        # A check that still runs keeps the check verdict out of the output.
        self.gh.view["statusCheckRollup"] = [{"name": "lint", "conclusion": ""}]
        self.gh.view["mergeable"] = "CONFLICTING"
        seen: set[str] = set()
        self.assertEqual(self.poll(seen), ["PR #1 conflicts with main"])
        self.gh.view["mergeable"] = "UNKNOWN"
        self.assertEqual(self.poll(seen), [])
        self.gh.view["mergeable"] = "CONFLICTING"
        self.assertEqual(self.poll(seen), [])
        self.gh.view["mergeable"] = "MERGEABLE"
        self.assertEqual(self.poll(seen), [])
        self.gh.view["mergeable"] = "CONFLICTING"
        self.assertEqual(self.poll(seen), ["PR #1 conflicts with main"])

    def test_close_prints_once_and_a_reopening_prints(self):
        self.gh.view["state"] = "CLOSED"
        seen: set[str] = set()
        self.assertEqual(self.poll(seen), ["PR #1 closed without merge"])
        self.set_woke(False)
        self.assertEqual(
            self.poll(seen), ["PR #1 is closed, as an earlier run reported"]
        )
        self.assertFalse(watch_prs.woke)
        self.gh.view["state"] = "OPEN"
        self.assertEqual(self.poll(seen), ["PR #1 reopened"])


class Main(WatcherTest):
    def test_exit_on_event_and_restart_prints_only_new_events(self):
        self.gh.comments = [comment(5, WRITER)]
        self.assertEqual(
            self.run_main("--exit-on-event", "1"),
            (0, ["PR #1 comment by reviewer (id 5)"]),
        )

        # The restart waits through quiet polls, and exits on the merge.
        polls = 0

        def merge_on_third_sleep():
            nonlocal polls
            polls += 1
            if polls == 3:
                self.gh.view["state"] = "MERGED"

        self.on_sleep = merge_on_third_sleep
        self.assertEqual(
            self.run_main("--exit-on-event", "1"),
            (0, ["PR #1 has no checks", "PR #1 merged"]),
        )
        self.assertEqual(
            self.run_main("--exit-on-event", "1"),
            (0, ["PR #1 is closed, as an earlier run reported"]),
        )

    def test_replay_prints_earlier_events_again(self):
        self.gh.comments = [comment(5, WRITER)]
        self.run_main("--exit-on-event", "1")
        self.assertEqual(
            self.run_main("--exit-on-event", "--replay", "1"),
            (0, ["PR #1 comment by reviewer (id 5)"]),
        )

    def test_exit_on_event_gives_up_on_a_broken_gh(self):
        self.gh.down = True
        status, lines = self.run_main("--exit-on-event", "1")
        self.assertEqual(status, 1)
        self.assertEqual(
            lines,
            [
                "gh failed: gh is down",
                f"gh failed on {watch_prs.GH_FAILED_POLLS} polls in a row",
            ],
        )

    def test_one_gh_failure_resets_when_gh_recovers(self):
        self.gh.down = True
        polls = 0

        def recover_then_merge():
            nonlocal polls
            polls += 1
            self.gh.down = polls % 2 == 0
            if polls == 2 * watch_prs.GH_FAILED_POLLS + 1:
                self.gh.view["state"] = "MERGED"

        self.on_sleep = recover_then_merge
        status, lines = self.run_main("--exit-on-event", "1")
        self.assertEqual((status, lines[-1]), (0, "PR #1 merged"))


if __name__ == "__main__":
    unittest.main()
