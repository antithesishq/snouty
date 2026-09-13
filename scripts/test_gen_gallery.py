import copy
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest


gallery = importlib.import_module("gen-gallery")


class TerminalCapture(unittest.TestCase):
    def setUp(self):
        self.snouty = gallery.Snouty(Path(sys.executable))
        self.addCleanup(self.snouty.cleanup)

    def test_human_capture_uses_terminal_dimensions_and_preserves_wrapping(self):
        result = self.snouty.run(["-c", textwrap.dedent('''
            import os
            import textwrap
            print('tty:', *(os.isatty(fd) for fd in (0, 1, 2)))
            columns, rows = os.get_terminal_size()
            print(f'size: {columns} {rows}')
            print(textwrap.fill('A long description with many words. ' * 12,
                  width=columns, initial_indent='Details   ', subsequent_indent=' ' * 10))
        ''')])
        lines = result.combined.splitlines()
        self.assertEqual(lines[0], "tty: True True True")
        self.assertEqual(lines[1], f"size: {gallery.TTY_COLS} {gallery.TTY_ROWS}")
        self.assertTrue(lines[2].startswith("Details   "))
        self.assertGreater(len(lines[3:]), 1)
        self.assertTrue(all(line.startswith(" " * 10) for line in lines[3:]))
        self.assertTrue(all(len(line) <= gallery.TTY_COLS for line in lines))

    def test_terminal_redraw_unicode_stream_order_and_scrollback(self):
        result = self.snouty.run(["-c", textwrap.dedent('''
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
        ''')])
        lines = result.combined.splitlines()
        self.assertEqual(lines[:2], ["完成 é", "↑"])
        self.assertEqual(lines[2:1202], [f"row {i:04}" for i in range(1200)])
        self.assertEqual(lines[1202:], ["X" * gallery.TTY_COLS, "X" * 5])
        self.assertEqual(result.returncode, 7)
        self.assertFalse(result.ok)
        self.assertNotIn("\x1b", result.combined)
        self.assertNotIn("old progress", result.combined)
        cast = [json.loads(line) for line in result.cast.splitlines()]
        self.assertEqual((cast[0]["width"], cast[0]["height"]), (gallery.TTY_COLS, gallery.TTY_ROWS))
        raw = "".join(event[2] for event in cast[1:])
        self.assertIn("\x1b[32m", raw)
        self.assertIn("old progress", raw)
        self.assertIn("↑", raw)
        self.assertNotIn("�", raw)

    def test_json_remains_exact_and_separate_from_stderr(self):
        result = self.snouty.run(["-c", textwrap.dedent('''
            import json
            import os
            print(json.dumps({'tty': [os.isatty(1), os.isatty(2)], 'value': 'x' * 250}))
            os.write(2, b'diagnostic\\n')
        '''), "--json"])
        self.assertTrue(result.ok)
        self.assertEqual(result.stdout, json.dumps({"tty": [False, False], "value": "x" * 250}) + "\n")
        self.assertEqual(result.stderr, "diagnostic\n")
        self.assertIsNone(result.cast)

    def test_limit_note_is_checked_in_the_merged_terminal_output(self):
        result = self.snouty.run(["-c", "import os; os.write(2, b'Showing up to 2 results.\\n')"])
        story = next(s for s in gallery.build_stories(gallery.Discovery()) if s.slug == "runs-list--limit")
        run = gallery.StoryRun(story, result, [{}, {}])
        self.assertTrue(gallery.rows_at_most_with_limit_note(2)(run, gallery.Registry())[0])

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

    def test_signal_exit_keeps_the_output_and_failure_status(self):
        result = self.snouty.run(["-c", "import os, signal; os.write(1, b'before signal\\n'); os.kill(os.getpid(), signal.SIGTERM)"])
        self.assertEqual(result.returncode, -15)
        self.assertEqual(result.combined, "before signal")
        self.assertFalse(result.ok)

    def test_story_writes_capture_mode_and_recordings_for_each_help_sample(self):
        result = self.snouty.run(["-c", "print('Usage: sample')"])
        story = gallery.Story(
            "sample", "Sample", "Read help", "Readable output", result.args,
            gallery.non_empty_table, help_cmd=["sample"],
        )
        run = gallery.StoryRun(story, result, None, result, [("variant", result)])
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            gallery.write_story(out, story, run, True, "ok")
            markdown = (out / "sample.md").read_text()
            self.assertEqual(markdown.count("Capture: terminal"), 3)
            for name in ("sample-help", "sample-default", "sample-variant-1"):
                self.assertIn(f"asciinema play {name}.cast", markdown)
                self.assertIn(f"[Download recording]({name}.cast)", markdown)
                self.assertEqual((out / f"{name}.cast").read_text(), result.cast)
            story.help_cmd = None
            gallery.write_story(out, story, run, True, "ok")
            self.assertIn("asciinema play sample.cast", (out / "sample.md").read_text())
            self.assertEqual((out / "sample.cast").read_text(), result.cast)
            run.result = gallery.Result(["doctor", "--json"], '{}\n', "", 0)
            gallery.write_story(out, story, run, True, "ok")
            self.assertIn("Capture: pipes (JSON)", (out / "sample.md").read_text())

    def test_wording_checks_accept_wrapping_but_require_the_text_and_exit(self):
        story = next(s for s in gallery.build_stories(gallery.Discovery(fail="run-1")) if s.slug == "runs-properties-incomplete")
        output = "No properties found.\n\nInspect the run with\n`snouty runs show run-1`."
        run = gallery.StoryRun(story, gallery.Result(story.args, output, "", 0), None)
        self.assertTrue(story.check(run, gallery.Registry())[0])
        run.result.returncode = 1
        self.assertFalse(story.check(run, gallery.Registry())[0])
        run.result.returncode = 0
        run.result.stdout = output.replace("runs show", "runs logs")
        self.assertFalse(story.check(run, gallery.Registry())[0])


class DoctorGalleryChecks(unittest.TestCase):
    def setUp(self):
        self.stories = {
            story.slug: story for story in gallery.build_stories(gallery.Discovery())
        }
        self.ready = {
            "ok": True,
            "checks": [
                {"name": "tenant", "status": "ok", "message": "tenant is set"},
                {"name": "api_key", "status": "ok", "message": "API key provided"},
            ],
        }
        self.missing = {
            "ok": False,
            "checks": [
                {"name": "tenant", "status": "error", "message": "tenant is not set"},
                {
                    "name": "credentials",
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
            {"name": "repository", "status": "warn", "message": "repository is not set"}
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
        self.assertFalse(passed, "a healthy report does not exercise missing configuration")
        passed, _ = self.check("doctor-json-ready", json.dumps(self.missing), 1)
        self.assertFalse(passed, "an error report does not exercise a ready setup")

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
                            changed["checks"][index]["name"] = "unrelated"
                        else:
                            changed["checks"][index]["status"] = "warn"
                        passed, _ = self.check(slug, json.dumps(changed), returncode)
                        self.assertFalse(passed, "an unrelated check must not satisfy this story")

        self.ready["checks"].append(
            {"name": "container_runtime", "status": "error", "message": "no runtime"}
        )
        passed, _ = self.check("doctor-json-ready", json.dumps(self.ready), 0)
        self.assertFalse(passed, "an error check must make the overall report fail")
        self.ready["ok"] = False
        passed, _ = self.check("doctor-json-ready", json.dumps(self.ready), 1)
        self.assertFalse(passed, "a failed runtime check must fail the ready-setup story")

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
        for key, value in (("name", 1), ("status", "failed"), ("message", None)):
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
                self.assertFalse(passed, "fresh setup must not recommend legacy credentials")
        passed, _ = self.check("doctor-no-auth", output, 0)
        self.assertFalse(passed)

    def test_legacy_auth_requires_restrictions_and_migration_actions(self):
        output = """`snouty runs` and other API commands refuse username/password
username/password authentication is deprecated; run `snouty login` to switch
username/password only enables `snouty launch` and `snouty debug`
read from the [ANTITHESIS_USERNAME, ANTITHESIS_PASSWORD] environment variables
or set ANTITHESIS_API_KEY; ask Antithesis support for an API key if you don't have one
"""
        passed, detail = self.check("doctor-legacy-auth", output, 0)
        self.assertTrue(passed, detail)
        for action in (
            "refuse username/password",
            "deprecated",
            "snouty launch",
            "snouty debug",
            "snouty login",
            "ANTITHESIS_API_KEY",
        ):
            with self.subTest(action=action):
                passed, _ = self.check("doctor-legacy-auth", output.replace(action, ""), 0)
                self.assertFalse(passed)


if __name__ == "__main__":
    unittest.main()
