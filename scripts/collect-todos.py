#!/usr/bin/env -S uv run
"""Collect the TODO, FIXME and "observed on" comments that the nightly
todo-audit workflow (`.github/workflows/todo-audit.md`) hands to its agent.

Prints a JSON array to stdout, one object per comment block:

    {"file": "src/api.rs", "line": 606, "end_line": 613,
     "kinds": ["observed-on"], "text": "Observed on orbitinghail, ...",
     "marker": "todo-audit: src/api.rs#1a2b3c4d5e6f"}

`marker` hashes the file and the comment text, not the line number, so it
survives edits elsewhere in the file, and a later run can find the issue it
filed.

Usage: scripts/collect-todos.py [ROOT]
"""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

PATTERNS = {
    "todo": re.compile(r"\bTODO\b"),
    "fixme": re.compile(r"\bFIXME\b"),
    # "Observed on orbitinghail, release 64.0", "seen live on orbitinghail",
    # "(orbitinghail, release 64.0)". Patterns run on a block's joined text, so
    # a phrase may wrap across lines.
    "observed-on": re.compile(
        r"\b(observed|seen)( live)? on\b|\brelease \d+\.\d+", re.IGNORECASE
    ),
}

# Markdown files are guidance for contributors, and the audit's own files name
# the patterns they look for.
EXCLUDE = [
    ":!*.md",
    ":!.github/workflows/todo-audit.*",
    ":!scripts/collect-todos.py",
    ":!scripts/test_collect_todos.py",
]

# `#` opens a comment in Python and YAML, but `#[` and `#![` open Rust
# attributes.
COMMENT = re.compile(r"^\s*(//[/!]?|#(?!!?\[))\s?")


def tracked_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z", "--", *EXCLUDE], cwd=root, check=True, capture_output=True
    ).stdout
    return [p for p in out.decode().split("\0") if p]


def block_end(lines: list[str], start: int) -> int:
    """The end of the block that starts at `lines[start]`: the run of non-blank
    comment lines with the same prefix. Any other line is a block of its own."""

    def prefix(i: int) -> str | None:
        m = COMMENT.match(lines[i])
        return m.group(1) if m and lines[i][m.end() :].strip() else None

    first = prefix(start)
    end = start + 1
    while first and end < len(lines) and prefix(end) == first:
        end += 1
    return end


TRAILING = re.compile(r"\s(//|#)\s(.*)")


def block_text(lines: list[str]) -> str:
    """The prose of a block, with comment prefixes stripped. A line of code
    contributes only its trailing comment, so a pattern inside a plain string
    literal does not match."""
    if not COMMENT.match(lines[0]):
        m = TRAILING.search(lines[0])
        return " ".join(m.group(2).split()) if m else ""
    return " ".join(" ".join(COMMENT.sub("", line) for line in lines).split())


def collect(root: Path) -> list[dict]:
    items = []
    for path in tracked_files(root):
        try:
            lines = (root / path).read_text().splitlines()
        except (UnicodeDecodeError, OSError):
            continue
        start = 0
        while start < len(lines):
            end = block_end(lines, start)
            text = block_text(lines[start:end])
            kinds = [k for k, p in PATTERNS.items() if p.search(text)]
            if not kinds:
                start = end
                continue
            digest = hashlib.sha256(f"{path}\n{text}".encode()).hexdigest()[:12]
            items.append(
                {
                    "file": path,
                    "line": start + 1,
                    "end_line": end,
                    "kinds": kinds,
                    "text": text,
                    "marker": f"todo-audit: {path}#{digest}",
                }
            )
            start = end
    return items


def main() -> None:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    items = collect(root)
    json.dump(items, sys.stdout, indent=2)
    print()
    print(f"collected {len(items)} comment blocks", file=sys.stderr)


if __name__ == "__main__":
    main()
