import importlib
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

collect_todos = importlib.import_module("collect-todos")


class Collect(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)

    def write(self, path: str, text: str):
        (self.root / path).parent.mkdir(parents=True, exist_ok=True)
        (self.root / path).write_text(textwrap.dedent(text))
        subprocess.run(["git", "add", path], cwd=self.root, check=True)

    def test_one_entry_per_comment_block(self):
        self.write(
            "src/api.rs",
            """\
            fn a() {}

            /// Request cancellation of a run.
            ///
            /// Observed on orbitinghail, release 64.0: a repeat cancel answers
            /// 200. TODO: expect 409 once the server
            /// follows the spec.
            fn cancel() {}
            // FIXME: a separate block
            """,
        )
        items = collect_todos.collect(self.root)
        self.assertEqual(
            [(i["line"], i["end_line"], i["kinds"]) for i in items],
            [(5, 7, ["todo", "observed-on"]), (9, 9, ["fixme"])],
        )
        self.assertEqual(
            items[0]["text"],
            "Observed on orbitinghail, release 64.0: a repeat cancel answers "
            "200. TODO: expect 409 once the server follows the spec.",
        )

    def test_matches_wrapped_phrases_and_trailing_comments_only(self):
        self.write(
            "src/lib.rs",
            """\
            /// The server rejects it (observed
            /// on tenant `orbitinghail`).
            #[derive(Debug)]
            #[allow(dead_code)] // TODO: drop once unused
            const HELP: &str = "TODO: not a comment";
            /// Seen live on orbitinghail.
            fn f() {}
            /// Shaped as (orbitinghail, release 64.0) sends it.
            """,
        )
        self.assertEqual(
            [
                (i["line"], i["end_line"], i["kinds"], i["text"])
                for i in collect_todos.collect(self.root)
            ],
            [
                (
                    1,
                    2,
                    ["observed-on"],
                    "The server rejects it (observed on tenant `orbitinghail`).",
                ),
                (4, 4, ["todo"], "TODO: drop once unused"),
                (6, 6, ["observed-on"], "Seen live on orbitinghail."),
                (
                    8,
                    8,
                    ["observed-on"],
                    "Shaped as (orbitinghail, release 64.0) sends it.",
                ),
            ],
        )

    def test_matches_release_citations_and_upstream_issues(self):
        self.write(
            "src/lib.rs",
            """\
            /// Releases 58.11 through 60.1 ignore the field.
            fn a() {}
            /// Under the tenant-58.6 schema.
            fn b() {}
            /// Seen as a string (tenant version 63).
            fn c() {}
            /// Ignores --format json (docker/compose#11627).
            fn d() {}
            /// Fixed in #136, and reads the release notes.
            """,
        )
        self.assertEqual(
            [(i["line"], i["kinds"]) for i in collect_todos.collect(self.root)],
            [
                (1, ["observed-on"]),
                (3, ["observed-on"]),
                (5, ["observed-on"]),
                (7, ["upstream"]),
            ],
        )

    def test_marker_survives_moving_the_comment(self):
        self.write("src/a.rs", "// TODO: drop the retry\nfn f() {}\n")
        before = collect_todos.collect(self.root)[0]
        self.write("src/a.rs", "fn g() {}\n\n    // TODO: drop the retry\nfn f() {}\n")
        after = collect_todos.collect(self.root)[0]
        self.assertEqual(after["line"], 3)
        self.assertEqual(after["marker"], before["marker"])
        self.assertRegex(after["marker"], r"^todo-audit: src/a\.rs#[0-9a-f]{12}$")

    def test_repeated_text_gets_its_own_marker(self):
        self.write("src/a.rs", "// TODO: drop\nfn f() {}\n// TODO: drop\n")
        first, second = (i["marker"] for i in collect_todos.collect(self.root))
        self.assertEqual(second, first + "-2")

    def test_shebang_is_not_a_comment(self):
        self.write("run.sh", "#!/usr/bin/env bash\n# TODO: quote the args\n")
        [item] = collect_todos.collect(self.root)
        self.assertEqual((item["line"], item["text"]), (2, "TODO: quote the args"))

    def test_skips_markdown_and_untracked_files(self):
        self.write("AGENTS.md", "A TODO with a concrete trigger.\n")
        (self.root / "notes.rs").write_text("// TODO: untracked\n")
        self.assertEqual(collect_todos.collect(self.root), [])


if __name__ == "__main__":
    unittest.main()
