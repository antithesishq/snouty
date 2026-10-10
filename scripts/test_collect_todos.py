import importlib
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


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
        self.write("src/api.rs", """\
            fn a() {}

            /// Request cancellation of a run.
            ///
            /// Observed on orbitinghail, release 64.0: a repeat cancel answers
            /// 200. TODO: expect 409 once the server
            /// follows the spec.
            fn cancel() {}
            // FIXME: a separate block
            """)
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

    def test_marker_survives_moving_the_comment(self):
        self.write("src/a.rs", "// TODO: drop the retry\nfn f() {}\n")
        before = collect_todos.collect(self.root)[0]
        self.write("src/a.rs", "fn g() {}\n\n    // TODO: drop the retry\nfn f() {}\n")
        after = collect_todos.collect(self.root)[0]
        self.assertEqual(after["line"], 3)
        self.assertEqual(after["marker"], before["marker"])
        self.assertRegex(after["marker"], r"^todo-audit: src/a\.rs#[0-9a-f]{12}$")

    def test_skips_markdown_and_untracked_files(self):
        self.write("AGENTS.md", "A TODO with a concrete trigger.\n")
        (self.root / "notes.rs").write_text("// TODO: untracked\n")
        self.assertEqual(collect_todos.collect(self.root), [])


if __name__ == "__main__":
    unittest.main()
