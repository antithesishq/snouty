import contextlib
import importlib
import io
import json
import os
import subprocess
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from typing import Any

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
        self.lookup_down = False
        self.lookup_fails = 0  # the next lookups that fail, after lookup_down
        self.lists_down = False

    def run(self, cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "git":
            # The current directory is the top level of a checkout.
            return subprocess.CompletedProcess(cmd, 0, f"{os.getcwd()}\n")
        if self.down:
            return subprocess.CompletedProcess(cmd, 1, "", "gh is down\n")
        answer = self.answer(cmd[1:])
        if answer is None:
            return subprocess.CompletedProcess(cmd, 1, "", "HTTP 403\n")
        return subprocess.CompletedProcess(cmd, 0, json.dumps(answer))

    def answer(self, args: list[str]) -> Any | None:
        if args[:2] == ["pr", "view"]:
            return self.view
        path = args[1]
        if "/branches/" in path:
            return {"commit": {"sha": self.view["baseRefOid"]}}
        if "/collaborators/" in path and self.lookup_fails:
            self.lookup_fails -= 1
            return None
        if "/collaborators/" in path and self.lookup_down:
            return None
        if "/collaborators/" in path:
            return {"user": {"id": WRITER["id"]}, "permission": "write"}
        if "/issues/" in path and self.lists_down:
            return None
        if "/issues/" in path:
            return self.comments
        return []


class WatcherTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state_home = Path(tmp.name)
        self.gh = FakeGh()
        for patch in (
            unittest.mock.patch.dict(os.environ, {"XDG_STATE_HOME": tmp.name}),
            unittest.mock.patch.object(
                watch_prs.subprocess, "run", lambda cmd, **kw: self.gh.run(cmd, **kw)
            ),
            unittest.mock.patch.object(watch_prs.time, "sleep", self.sleep),
            unittest.mock.patch.object(watch_prs.time, "monotonic", lambda: self.now),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.on_sleep = lambda: None
        self.now = 0.0
        self.set_woke(False)
        watch_prs.reported_failures.clear()
        watch_prs.verdict_since.clear()
        setattr(watch_prs, "agree_seconds", 45.0)  # noqa: B010 (a module, to pyright)

    def set_woke(self, value: bool) -> None:
        setattr(watch_prs, "woke", value)  # noqa: B010 (a module, to pyright)

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        if self.now > 7 * 24 * 3600:
            raise AssertionError("the run did not end in a week")
        self.on_sleep()

    def poll(self, seen: set[str], checks: set[str] | None = None) -> list[str]:
        """The lines that one poll of PR 1 prints."""
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            watch_prs.poll_pr([], SLUG, 1, seen, set() if checks is None else checks)
        return out.getvalue().splitlines()

    def run_main(self, *argv: str, settle: int | None = 0) -> tuple[int, list[str]]:
        """Run the script. `settle` is 0 by default, so a run ends on the
        poll that prints its first event. None leaves the script's default.
        """
        # Each run is a new process: it keeps only what its state file holds.
        watch_prs.verdict_since.clear()
        out = io.StringIO()
        if settle is not None:
            argv = ("--settle", str(settle), *argv)
        with (
            unittest.mock.patch("sys.argv", ["watch-prs.py", "-R", SLUG, *argv]),
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

    def test_a_verdict_needs_agreement_for_a_whole_interval(self):
        self.gh.view["statusCheckRollup"] = [{"name": "lint", "conclusion": "SUCCESS"}]
        seen: set[str] = set()
        self.assertEqual(self.poll(seen), [])
        self.now += 15
        self.assertEqual(self.poll(seen), [])
        self.now += 30
        self.assertEqual(self.poll(seen), ["PR #1 all checks passed for aaaaaaa"])

    def test_replay_keeps_the_saved_state_until_it_polls(self):
        self.gh.comments = [comment(5, WRITER)]
        self.run_main("--exit-on-event", "1")
        self.gh.down = True
        self.run_main("--exit-on-event", "--replay", "1")
        self.gh.down = False
        self.gh.comments.append(comment(6, WRITER))
        self.assertEqual(
            self.run_main("--exit-on-event", "1"),
            (0, ["PR #1 comment by reviewer (id 6)"]),
        )

    def test_pass_and_base_move_need_no_reaction(self):
        self.gh.view["statusCheckRollup"] = [{"name": "lint", "conclusion": "SUCCESS"}]
        self.gh.view["baseRefOid"] = "c" * 40
        seen: set[str] = set()
        self.poll(seen)
        self.now += 45
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

    def test_settle_collects_the_events_that_follow_one_into_one_run(self):
        self.gh.comments = [comment(5, WRITER)]
        polls = 0

        def comment_after_the_first_sleep():
            nonlocal polls
            polls += 1
            if polls == 1:
                self.gh.comments.append(comment(6, WRITER))

        self.on_sleep = comment_after_the_first_sleep
        status, lines = self.run_main("--exit-on-event", "-i", "10", "1", settle=None)
        self.assertEqual(status, 0)
        self.assertEqual(
            [line for line in lines if "comment by" in line],
            ["PR #1 comment by reviewer (id 5)", "PR #1 comment by reviewer (id 6)"],
        )
        # The second comment printed at 10 seconds, so the run ends with a
        # poll at 25 seconds, when the default 15-second wait ends.
        self.assertEqual(self.now, 25)

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

    def test_a_failed_lookup_prints_as_unknown_then_with_its_trust(self):
        self.gh.comments = [comment(5, WRITER)]
        self.gh.lookup_down = True
        self.assertEqual(
            self.run_main("--exit-on-event", "1"),
            (
                0,
                [
                    "gh failed: HTTP 403",
                    "PR #1 comment by reviewer (id 5) [write access unknown]",
                ],
            ),
        )
        # A restart that still cannot look it up prints nothing new.
        polls = 0

        def recover_on_third_sleep():
            nonlocal polls
            polls += 1
            if polls == 3:
                self.gh.lookup_down = False

        self.on_sleep = recover_on_third_sleep
        status, lines = self.run_main("--exit-on-event", "1")
        self.assertEqual((status, lines[-1]), (0, "PR #1 comment by reviewer (id 5)"))
        self.assertEqual(polls, 3)

    def test_a_pending_lookup_does_not_hold_back_the_close(self):
        self.gh.comments = [comment(5, WRITER)]
        self.gh.lookup_down = True

        def fail_on_a_second_poll():
            raise AssertionError("the comment waited for another poll")

        # The comment prints on the first poll, before the PR can close.
        self.on_sleep = fail_on_a_second_poll
        status, lines = self.run_main("--exit-on-event", "1")
        self.assertEqual(
            (status, [line for line in lines if line.startswith("PR #1 comment")]),
            (0, ["PR #1 comment by reviewer (id 5) [write access unknown]"]),
        )
        self.gh.view["state"] = "MERGED"
        self.assertEqual(self.run_main("--exit-on-event", "1"), (0, ["PR #1 merged"]))

    def test_one_author_costs_one_lookup_per_poll(self):
        self.gh.comments = [comment(5, WRITER), comment(6, WRITER), comment(7, WRITER)]
        self.gh.lookup_down = True
        calls = []
        run = self.gh.run

        def counting_run(cmd, **kw):
            if "/collaborators/" in " ".join(cmd):
                calls.append(cmd)
            return run(cmd, **kw)

        self.gh.run = counting_run
        self.poll(set())
        self.assertEqual(len(calls), 1)

    def test_an_unwritable_state_directory_stops_the_run_before_any_event(self):
        # A file where the directory must go fails for any user. Root ignores
        # the mode bits of a directory.
        (self.state_home / "watch-prs").write_text("")
        self.gh.comments = [comment(5, WRITER)]
        status, lines = self.run_main("--exit-on-event", "1")
        self.assertEqual(status, 2)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("cannot save state: "))

        # An explicit state directory recovers, and keeps the state there.
        with tempfile.TemporaryDirectory() as state_dir:
            run = ("--exit-on-event", "--state-dir", state_dir, "1")
            self.assertEqual(
                self.run_main(*run), (0, ["PR #1 comment by reviewer (id 5)"])
            )
            self.assertEqual(os.listdir(state_dir), ["owner__repo__1.json"])

    def test_a_failed_list_of_comments_ends_the_run(self):
        self.gh.lists_down = True
        status, lines = self.run_main("--exit-on-event", "1")
        self.assertEqual(status, 1)
        self.assertEqual(
            lines[-1], f"gh failed on {watch_prs.GH_FAILED_POLLS} polls in a row"
        )

    def test_each_checkout_keeps_its_own_state(self):
        self.gh.comments = [comment(5, WRITER)]
        self.run_main("--exit-on-event", "1")
        with tempfile.TemporaryDirectory() as other:
            cwd = os.getcwd()
            os.chdir(other)
            self.addCleanup(os.chdir, cwd)
            self.assertEqual(
                self.run_main("--exit-on-event", "1"),
                (0, ["PR #1 comment by reviewer (id 5)"]),
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
