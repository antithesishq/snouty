"""Collect the TODO, FIXME and "observed on" comments that the nightly
todo-audit workflow (`.github/workflows/todo-audit.md`) hands to its agent.

Prints a JSON array to stdout, one object per comment block:

    {"file": "src/api.rs", "line": 606, "end_line": 613,
     "kinds": ["observed-on"], "text": "Observed on orbitinghail, ...",
     "marker": "todo-audit: src/api.rs#1a2b3c4d5e6f",
     "context": "600 | ...\n601 | ..."}

`marker` hashes the file and the comment text, not the line number, so it
survives edits elsewhere in the file. Every issue the workflow files embeds it,
which is how a later run finds that issue again.

Usage: uv run scripts/collect-todos.py [ROOT]
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
    # A server quirk note, e.g. "Observed on orbitinghail, release 64.0". The
    # tenant often wraps onto the next line, so match the phrase alone.
    "observed-on": re.compile(r"\bobserved on\b", re.IGNORECASE),
}

# Markdown files are guidance for contributors, and the audit's own files name
# the patterns they look for.
EXCLUDE = re.compile(
    r"(\.md$|^\.github/workflows/todo-audit\.|^scripts/(test_)?collect[-_]todos\.py$)"
)

COMMENT = re.compile(r"^\s*(//[/!]?|#)\s?")
CONTEXT_LINES = 8


def tracked_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, check=True, capture_output=True
    ).stdout
    return [p for p in out.decode().split("\0") if p and not EXCLUDE.search(p)]


def comment_block(lines: list[str], index: int) -> tuple[int, int]:
    """The [start, end) range of the comment block around `lines[index]`: the
    run of adjacent lines with the same comment prefix. A match outside a
    comment is a block of one line."""
    m = COMMENT.match(lines[index])
    if not m:
        return index, index + 1
    prefix = m.group(1)

    def same(i: int) -> bool:
        c = COMMENT.match(lines[i])
        return c is not None and c.group(1) == prefix and lines[i][c.end() :].strip() != ""

    start, end = index, index + 1
    while start > 0 and same(start - 1):
        start -= 1
    while end < len(lines) and same(end):
        end += 1
    return start, end


def block_text(lines: list[str]) -> str:
    return " ".join(" ".join(COMMENT.sub("", line, count=1).split()) for line in lines)


def collect(root: Path) -> list[dict]:
    items = []
    for path in tracked_files(root):
        try:
            lines = (root / path).read_text().splitlines()
        except (UnicodeDecodeError, OSError):
            continue
        seen = set()
        for i, line in enumerate(lines):
            kinds = [k for k, p in PATTERNS.items() if p.search(line)]
            if not kinds:
                continue
            start, end = comment_block(lines, i)
            if start in seen:
                continue
            seen.add(start)
            block = lines[start:end]
            kinds = [k for k, p in PATTERNS.items() if any(p.search(b) for b in block)]
            text = block_text(block)
            digest = hashlib.sha256(f"{path}\n{text}".encode()).hexdigest()[:12]
            lo, hi = max(0, start - CONTEXT_LINES), min(len(lines), end + CONTEXT_LINES)
            items.append(
                {
                    "file": path,
                    "line": start + 1,
                    "end_line": end,
                    "kinds": kinds,
                    "text": text,
                    "marker": f"todo-audit: {path}#{digest}",
                    "context": "\n".join(f"{n + 1} | {lines[n]}" for n in range(lo, hi)),
                }
            )
    return items


def main() -> None:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    items = collect(root)
    json.dump(items, sys.stdout, indent=2)
    print()
    print(f"collected {len(items)} comment blocks", file=sys.stderr)


if __name__ == "__main__":
    main()
