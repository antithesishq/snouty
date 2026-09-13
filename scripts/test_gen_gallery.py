import copy
import importlib
import json
import unittest


gallery = importlib.import_module("gen-gallery")


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
