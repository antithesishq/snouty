import copy
import importlib
import json
import os
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

gallery = importlib.import_module("gen-gallery")


class TerminalCapture(unittest.TestCase):
    def setUp(self):
        self.snouty = gallery.Snouty(Path(sys.executable))
        self.addCleanup(self.snouty.cleanup)

    def test_text_capture_uses_terminal_dimensions_and_preserves_wrapping(self):
        result = self.snouty.run(
            [
                "-c",
                textwrap.dedent("""
            import os
            import textwrap
            print('tty:', *(os.isatty(fd) for fd in (0, 1, 2)))
            columns, rows = os.get_terminal_size()
            print(f'size: {columns} {rows}')
            print(textwrap.fill('A long description with many words. ' * 12,
                  width=columns, initial_indent='Details   ', subsequent_indent=' ' * 10))
        """),
            ]
        )
        lines = result.combined.splitlines()
        self.assertEqual(lines[0], "tty: True True True")
        self.assertEqual(lines[1], f"size: {gallery.TTY_COLS} {gallery.TTY_ROWS}")
        self.assertTrue(lines[2].startswith("Details   "))
        self.assertGreater(len(lines[3:]), 1)
        self.assertTrue(all(line.startswith(" " * 10) for line in lines[3:]))
        self.assertTrue(all(len(line) <= gallery.TTY_COLS for line in lines))

    def test_terminal_redraw_unicode_stream_order_and_scrollback(self):
        result = self.snouty.run(
            [
                "-c",
                textwrap.dedent("""
            import os
            import sys
            os.write(1, b'old progress\\r\\x1b[2K')
            os.write(2, '\\x1b[32m完成 e\\u0301\\x1b[0m\\n'.encode())
            arrow = '↑'.encode()
            os.write(1, arrow[:1])
            os.write(1, arrow[1:] + b'\\n')
            for i in range(1200):
                os.write(1 if i % 2 == 0 else 2, f'row {i:04}\\n'.encode())
            os.write(1, b'X' * (os.get_terminal_size().columns + 5) + b'\\n')
            sys.exit(7)
        """),
            ]
        )
        lines = result.combined.splitlines()
        self.assertEqual(lines[:2], ["完成 é", "↑"])
        self.assertEqual(lines[2:1202], [f"row {i:04}" for i in range(1200)])
        self.assertEqual(lines[1202:], ["X" * gallery.TTY_COLS, "X" * 5])
        self.assertEqual(result.returncode, 7)
        self.assertFalse(result.ok)
        self.assertNotIn("\x1b", result.combined)
        self.assertNotIn("old progress", result.combined)
        cast = [json.loads(line) for line in result.cast.splitlines()]
        self.assertEqual(
            (cast[0]["width"], cast[0]["height"]), (gallery.TTY_COLS, gallery.TTY_ROWS)
        )
        raw = "".join(event[2] for event in cast[1:])
        self.assertIn("\x1b[32m", raw)
        self.assertIn("old progress", raw)
        self.assertIn("↑", raw)
        self.assertNotIn("�", raw)

    def test_json_remains_exact_and_separate_from_stderr(self):
        result = self.snouty.run(
            [
                "-c",
                textwrap.dedent("""
            import json
            import os
            print(json.dumps({'tty': [os.isatty(1), os.isatty(2)], 'value': 'x' * 250}))
            os.write(2, b'diagnostic\\n')
        """),
                "--json",
            ]
        )
        self.assertTrue(result.ok)
        self.assertEqual(
            result.stdout,
            json.dumps({"tty": [False, False], "value": "x" * 250}) + "\n",
        )
        self.assertEqual(result.stderr, "diagnostic\n")
        self.assertIsNone(result.cast)

    def test_limit_note_is_checked_in_the_merged_terminal_output(self):
        result = self.snouty.run(
            ["-c", "import os; os.write(2, b'Showing up to 2 results.\\n')"]
        )
        story = next(
            s
            for s in gallery.build_stories(gallery.Discovery())
            if s.slug == "runs-list--limit"
        )
        run = gallery.StoryRun(story, result, [{}, {}])
        self.assertTrue(
            gallery.rows_at_most_with_limit_note(2)(run, gallery.Registry())[0]
        )

    def test_stalled_dialogue_fails_and_closes_the_child(self):
        with tempfile.TemporaryDirectory() as directory:
            pidfile = Path(directory) / "child.pid"
            code = "import os, pathlib, sys, time; pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)"
            session = gallery.TtySession(
                self.snouty.binary, ["-c", code, str(pidfile)], self.snouty.env_with({})
            )
            self.assertNotEqual(session.finish(timeout=0.5), 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)

    def test_pipe_timeout_kills_the_child_and_keeps_its_output(self):
        code = "import sys, time; print('before', flush=True); time.sleep(30)"
        result = self.snouty.run(["-c", code, "--json"], timeout=0.5)
        self.assertEqual(result.returncode, -9)
        self.assertEqual(result.stdout, "before\n")

    def test_kill_children_ends_running_commands_on_both_captures(self):
        code = "import sys, time; print('before', flush=True); time.sleep(30)"
        terminal = gallery.in_background(lambda: self.snouty.run(["-c", code]))
        pipe = gallery.in_background(lambda: self.snouty.run(["-c", code, "--json"]))
        deadline = time.monotonic() + 10
        while len(self.snouty._children) < 2 and time.monotonic() < deadline:
            time.sleep(0.05)
        time.sleep(0.5)
        self.snouty.kill_children()
        self.assertEqual(pipe.result(timeout=10).stdout, "before\n")
        self.assertIn("before", terminal.result(timeout=10).combined)
        self.assertFalse(terminal.result().ok)
        self.assertEqual(self.snouty._children, set())

    def test_signal_exit_keeps_the_output_and_failure_status(self):
        result = self.snouty.run(
            [
                "-c",
                "import os, signal; os.write(1, b'before signal\\n'); os.kill(os.getpid(), signal.SIGTERM)",
            ]
        )
        self.assertEqual(result.returncode, -15)
        self.assertEqual(result.combined, "before signal")
        self.assertFalse(result.ok)

    def test_story_writes_capture_mode_and_recordings_for_each_help_sample(self):
        result = self.snouty.run(["-c", "print('Usage: sample')"])
        story = gallery.Story(
            "sample",
            "Sample",
            "Read help",
            "Readable output",
            result.args,
            gallery.non_empty_table,
            help_cmd=["sample"],
        )
        run = gallery.StoryRun(story, result, None, result, [("variant", result)])
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            gallery.write_story(out, story, run, True, "ok", "snouty 0.0.0")
            markdown = (out / "sample.md").read_text()
            self.assertEqual(markdown.count("Capture: terminal"), 3)
            for name in ("sample-help", "sample-default", "sample-variant-1"):
                self.assertIn(f"asciinema play {name}.cast", markdown)
                self.assertIn(f"[Download recording]({name}.cast)", markdown)
                self.assertEqual((out / f"{name}.cast").read_text(), result.cast)
            story.help_cmd = None
            gallery.write_story(out, story, run, True, "ok", "snouty 0.0.0")
            self.assertIn("asciinema play sample.cast", (out / "sample.md").read_text())
            self.assertEqual((out / "sample.cast").read_text(), result.cast)
            run.result = gallery.Result(["doctor", "--json"], "{}\n", "", 0)
            gallery.write_story(out, story, run, True, "ok", "snouty 0.0.0")
            self.assertIn("Capture: pipes (JSON)", (out / "sample.md").read_text())

    def test_wording_checks_accept_wrapping_but_require_the_text_and_exit(self):
        story = next(
            s
            for s in gallery.build_stories(gallery.Discovery(fail="run-1"))
            if s.slug == "runs-properties-incomplete"
        )
        output = "No properties\nfound."
        run = gallery.StoryRun(story, gallery.Result(story.args, output, "", 0), None)
        self.assertTrue(story.check(run, gallery.Registry())[0])
        run.result.returncode = 1
        self.assertFalse(story.check(run, gallery.Registry())[0])
        run.result.returncode = 0
        run.result.stdout = "No properties match (failing)."
        self.assertFalse(story.check(run, gallery.Registry())[0])


class DoctorGalleryChecks(unittest.TestCase):
    def setUp(self):
        self.stories = {
            story.slug: story for story in gallery.build_stories(gallery.Discovery())
        }
        self.ready = {
            "ok": True,
            "checks": [
                {"id": "tenant", "status": "ok", "message": "tenant is set"},
                {
                    "id": "credentials",
                    "status": "ok",
                    "kind": "api_key",
                    "message": "API key provided",
                },
            ],
        }
        self.missing = {
            "ok": False,
            "checks": [
                {"id": "tenant", "status": "error", "message": "tenant is not set"},
                {
                    "id": "credentials",
                    "status": "error",
                    "message": "No Antithesis credentials found",
                },
            ],
        }

    def check(self, slug, stdout, returncode):
        story = self.stories[slug]
        result = gallery.Result(story.args, stdout, "", returncode)
        return story.check(gallery.StoryRun(story, result, None), gallery.Registry())

    def test_json_accepts_both_configurations(self):
        for slug, report, returncode in (
            ("doctor-json", self.missing, 1),
            ("doctor-json", self.missing, 2),
            ("doctor-json-ready", self.ready, 0),
        ):
            with self.subTest(slug=slug, returncode=returncode):
                passed, detail = self.check(slug, json.dumps(report), returncode)
                self.assertTrue(passed, detail)

        self.ready["checks"].append(
            {"id": "repository", "status": "warn", "message": "repository is not set"}
        )
        passed, detail = self.check("doctor-json-ready", json.dumps(self.ready), 0)
        self.assertTrue(passed, detail)

    def test_json_rejects_exit_and_outcome_mismatches(self):
        for slug, report, returncode in (
            ("doctor-json", self.missing, 1),
            ("doctor-json-ready", self.ready, 0),
        ):
            with self.subTest(slug=slug):
                passed, _ = self.check(slug, json.dumps(report), 1 - returncode)
                self.assertFalse(passed, "the exit status must agree with ok")

                changed = copy.deepcopy(report)
                changed["ok"] = not changed["ok"]
                passed, _ = self.check(slug, json.dumps(changed), returncode)
                self.assertFalse(passed, "ok must match the expected configuration")

        passed, _ = self.check("doctor-json", json.dumps(self.ready), 0)
        self.assertFalse(
            passed, "a healthy report does not exercise missing configuration"
        )
        passed, _ = self.check("doctor-json-ready", json.dumps(self.missing), 1)
        self.assertFalse(passed, "an error report does not exercise a ready setup")

    def test_json_requires_the_credential_kind(self):
        for kind in (None, "oauth"):
            with self.subTest(kind=kind):
                changed = copy.deepcopy(self.ready)
                changed["checks"][1].pop("kind")
                if kind:
                    changed["checks"][1]["kind"] = kind
                passed, _ = self.check("doctor-json-ready", json.dumps(changed), 0)
                self.assertFalse(passed, "the ready story sets an API key")

        changed = copy.deepcopy(self.missing)
        changed["checks"][1]["kind"] = "api_key"
        passed, _ = self.check("doctor-json", json.dumps(changed), 1)
        self.assertFalse(passed, "no credential means no kind")

    def test_json_requires_the_correct_checks_and_statuses(self):
        for slug, report, returncode in (
            ("doctor-json", self.missing, 1),
            ("doctor-json-ready", self.ready, 0),
        ):
            for index in range(len(report["checks"])):
                for mutation in ("remove", "rename", "status"):
                    with self.subTest(slug=slug, index=index, mutation=mutation):
                        changed = copy.deepcopy(report)
                        if mutation == "remove":
                            changed["checks"].pop(index)
                        elif mutation == "rename":
                            changed["checks"][index]["id"] = "unrelated"
                        else:
                            changed["checks"][index]["status"] = "warn"
                        passed, _ = self.check(slug, json.dumps(changed), returncode)
                        self.assertFalse(
                            passed, "an unrelated check must not satisfy this story"
                        )

        self.ready["checks"].append(
            {"id": "container_runtime", "status": "error", "message": "no runtime"}
        )
        passed, _ = self.check("doctor-json-ready", json.dumps(self.ready), 0)
        self.assertFalse(passed, "an error check must make the overall report fail")
        self.ready["ok"] = False
        passed, _ = self.check("doctor-json-ready", json.dumps(self.ready), 1)
        self.assertFalse(
            passed, "a failed runtime check must fail the ready-setup story"
        )

    def test_json_rejects_malformed_reports_without_raising(self):
        invalid = [
            "not JSON",
            "null",
            "[]",
            "true",
            "{}",
            '{"ok": 0, "checks": []}',
            '{"ok": false, "checks": []}',
            '{"ok": false, "checks": {}}',
            '{"ok": false, "checks": [null]}',
            '{"ok": false, "checks": [{}]}',
        ]
        for key, value in (("id", 1), ("status", "failed"), ("message", None)):
            changed = copy.deepcopy(self.missing)
            changed["checks"][0][key] = value
            invalid.append(json.dumps(changed))
        changed = copy.deepcopy(self.missing)
        changed["ok"] = 0
        invalid.append(json.dumps(changed))
        for stdout in invalid:
            with self.subTest(stdout=stdout):
                passed, _ = self.check("doctor-json", stdout, 1)
                self.assertFalse(passed)

    def test_no_auth_requires_setup_actions(self):
        output = """No Antithesis credentials found
run `snouty login` to sign in and store credentials
or set ANTITHESIS_API_KEY; ask Antithesis support for an API key if you don't have one
"""
        passed, detail = self.check("doctor-no-auth", output, 1)
        self.assertTrue(passed, detail)
        for action in ("snouty login", "ANTITHESIS_API_KEY", "ask Antithesis support"):
            with self.subTest(action=action):
                passed, _ = self.check("doctor-no-auth", output.replace(action, ""), 1)
                self.assertFalse(passed)
        for variable in ("ANTITHESIS_USERNAME", "ANTITHESIS_PASSWORD"):
            with self.subTest(variable=variable):
                passed, _ = self.check("doctor-no-auth", output + variable, 1)
                self.assertFalse(
                    passed, "fresh setup must not recommend legacy credentials"
                )
        passed, _ = self.check("doctor-no-auth", output, 0)
        self.assertFalse(passed)

    def test_legacy_auth_requires_restrictions_and_migration_actions(self):
        output = """  ⚠ username/password for user [alice] only works for `snouty launch` and `snouty debug`
      WARNING: username/password is deprecated, and `snouty runs` and other API commands refuse it
      NOTE: read from the `ANTITHESIS_USERNAME` and `ANTITHESIS_PASSWORD` environment variables
      NOTE: run `snouty login` to store an API key, then `unset ANTITHESIS_USERNAME ANTITHESIS_PASSWORD`
"""
        passed, detail = self.check("doctor-legacy-auth", output, 0)
        self.assertTrue(passed, detail)
        for action in (
            "refuse it",
            "deprecated",
            "snouty launch",
            "snouty debug",
            "snouty login",
            "unset ANTITHESIS_USERNAME",
        ):
            with self.subTest(action=action):
                passed, _ = self.check(
                    "doctor-legacy-auth", output.replace(action, ""), 0
                )
                self.assertFalse(passed)


class GalleryRunHelpers(unittest.TestCase):
    RUN = "0123456789abcdef0123456789abcdef-66-0"
    BUG = gallery.EXAMPLE_BUG_PROPERTY

    def setUp(self):
        self.snouty = gallery.Snouty(Path(sys.executable))
        self.addCleanup(self.snouty.cleanup)

    def test_launched_run_id_reads_the_json_launch_output(self):
        self.assertEqual(gallery.launched_run_id('{\n  "runId": "run-1"\n}\n'), "run-1")
        for stdout in (
            "",
            "Test run started: run_id run-1",
            "{}",
            '{"runId": ""}',
            "[]",
        ):
            with self.subTest(stdout=stdout):
                self.assertIsNone(gallery.launched_run_id(stdout))

    def test_launch_args_mark_the_run_as_the_gallery_s(self):
        args = gallery.gallery_launch_args("snouty gen-gallery end-to-end run now")
        self.assertEqual(args[:2], ["--json", "launch"])
        self.assertIn("attrs.source=snouty-gallery", args)
        self.assertEqual(args[args.index("--max-compute-hours") + 1], "12")
        self.assertNotIn("--duration", args)
        self.assertTrue(
            args[args.index("--description") + 1].startswith(
                gallery.GALLERY_RUN_DESCRIPTION
            )
        )

    def test_earliest_moments_sorts_by_vtime_and_drops_duplicates(self):
        rows = [
            {"moment": {"input_hash": "3", "vtime": 30.5}},
            {"moment": {"input_hash": "1", "vtime": 9.25}},
            {"moment": {"input_hash": "2", "vtime": 12.0}},
            {"moment": {"input_hash": "1", "vtime": 9.25}},
        ]
        picked = gallery.earliest_moments(rows, 2)
        self.assertEqual([m["input_hash"] for m in picked], ["1", "2"])
        self.assertEqual(gallery.earliest_moments([], 2), [])

    def test_property_id_named_matches_a_grouped_name(self):
        rows = [
            {"id": "a", "name": "bank: Total money in the bank is conserved"},
            {"id": "b", "name": f"bank: {self.BUG}"},
        ]
        self.assertEqual(gallery.property_id_named(rows, self.BUG), "b")
        self.assertEqual(gallery.property_id_named(rows[:1], self.BUG), "")

    def transcript(self, *, verdict="failed", last=None):
        lines = [
            f"property   p1  {self.BUG}",
            "property   p2  Total money in the bank is conserved",
            "sample     p1  counterexample  -4242424242  81.5",
            'sample     p2  example         {"total": 300}',
            "verdict    p2  passed",
            f"verdict    p1  {verdict}",
            last or f"completed  {self.RUN}",
        ]
        return "\n".join(lines)

    def test_stream_events_read_the_terminal_lines(self):
        rows = gallery.stream_events("an unrelated line\n" + self.transcript())
        self.assertEqual(
            [r["event"] for r in rows],
            [
                "property",
                "property",
                "sample",
                "sample",
                "verdict",
                "verdict",
                "completed",
            ],
        )
        self.assertEqual(rows[0]["name"], self.BUG)
        self.assertEqual(rows[2]["kind"], "counterexample")
        self.assertEqual(rows[5]["verdict"], "failed")
        self.assertEqual(rows[-1]["run_id"], self.RUN)

    def test_stream_verdict_requires_a_whole_run(self):
        rows = gallery.stream_events(self.transcript())
        self.assertTrue(gallery.stream_verdict(rows, self.RUN, self.BUG, True)[0])
        self.assertFalse(gallery.stream_verdict(rows, self.RUN, self.BUG, False)[0])
        failing = [
            ("no failed verdict", self.transcript(verdict="passed")),
            ("not completed", self.transcript(last="verdict    p2  failed")),
            ("another run", self.transcript(last="completed  other-run")),
            (
                "no samples",
                "\n".join(
                    line
                    for line in self.transcript().splitlines()
                    if not line.startswith("sample")
                ),
            ),
        ]
        for name, text in failing:
            with self.subTest(name):
                rows = gallery.stream_events(text)
                self.assertFalse(
                    gallery.stream_verdict(rows, self.RUN, self.BUG, True)[0]
                )

    def story(self, slug, launched=True):
        launch = gallery.Result([], "", "", 0) if launched else None
        x = gallery.GalleryRun(run=self.RUN, launch=launch)
        stories = gallery.build_final_stories(x, gallery._LISTED_FINAL)
        return next(s for s in stories if s.slug == slug)

    def test_terminal_stream_story_needs_the_wait_in_the_recording(self):
        cast = "\n".join(
            [
                json.dumps({"version": 2}),
                json.dumps([0.1, "o", f"\u280b {gallery.STREAM_WAITING}\r"]),
            ]
        )
        result = gallery.Result([], self.transcript(), "", 0, cast=cast)
        story = self.story("runs-stream-run")
        run = gallery.StoryRun(story, result, None)
        self.assertTrue(story.check(run, gallery.Registry())[0])
        run.result = gallery.Result([], self.transcript(), "", 0, cast=None)
        self.assertFalse(story.check(run, gallery.Registry())[0])
        # A reused run can be past the wait; a property line shows the stream
        # opened.
        reused = self.story("runs-stream-run", launched=False)
        self.assertTrue(
            reused.check(
                gallery.StoryRun(reused, run.result, None), gallery.Registry()
            )[0]
        )
        run.result = gallery.Result([], self.transcript(), "", 1, cast=cast)
        self.assertFalse(story.check(run, gallery.Registry())[0])

    def test_json_stream_story_reads_ndjson_and_stderr(self):
        rows = [
            {"event": "property", "property_id": "p1", "name": self.BUG},
            {
                "event": "sample",
                "property_id": "p1",
                "kind": "counterexample",
                "input_hash": "-1",
                "vtime": 2.5,
            },
            {"event": "verdict", "property_id": "p1", "verdict": "failed"},
            {"event": "completed", "run_id": self.RUN},
        ]
        stdout = "".join(json.dumps(r) + "\n" for r in rows)
        stderr = f"{gallery.STREAM_WAITING} (5s)\n"
        story = self.story("runs-stream-run--json")
        run = gallery.StoryRun(story, gallery.Result([], stdout, stderr, 0), None)
        self.assertTrue(story.check(run, gallery.Registry())[0])
        run.result.stderr = ""
        self.assertFalse(story.check(run, gallery.Registry())[0])
        run.result = gallery.Result([], "not json\n", stderr, 0)
        self.assertFalse(story.check(run, gallery.Registry())[0])

    def test_timeout_keeps_the_output_of_a_killed_command(self):
        code = "import time; print('before the timeout', flush=True); time.sleep(30)"
        for pipe in (True, False):
            with self.subTest(pipe=pipe):
                result = self.snouty.run(["-c", code], pipe=pipe, timeout=1)
                self.assertFalse(result.ok)
                self.assertIn("before the timeout", result.combined)


class RunReuse(unittest.TestCase):
    def test_only_a_young_run_is_reused(self):
        now = gallery.datetime.fromisoformat("2026-10-26T03:20:00Z")
        young = {"started_at": "2026-10-26T03:14:00Z"}
        old = {"started_at": "2026-10-26T03:10:00Z"}
        self.assertTrue(gallery.young_enough(young, now))
        self.assertFalse(gallery.young_enough(old, now))


if __name__ == "__main__":
    unittest.main()
