#!/usr/bin/env python3
"""Build a docs.db fixture from a folder of sample markdown files."""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE pages (
  path TEXT PRIMARY KEY,
  title TEXT,
  content TEXT NOT NULL
);
CREATE VIRTUAL TABLE pages_fts USING fts5(
  title,
  content,
  content=pages,
  content_rowid=rowid,
  tokenize='unicode61'
);
CREATE TRIGGER pages_ai AFTER INSERT ON pages BEGIN
  INSERT INTO pages_fts(rowid, title, content)
    VALUES (new.rowid, new.title, new.content);
END;
""".strip()

H1_RE = re.compile(r"^#\s+(?P<title>.+?)\s*$")


def parse_markdown(path: Path) -> tuple[str, str]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    title = None
    body_start = 0
    for idx, line in enumerate(lines):
        match = H1_RE.match(line)
        if match:
            title = match.group("title").strip()
            body_start = idx + 1
            break

    if title is None:
        title = path.stem.replace("_", " ").replace("-", " ").title()
        body_lines = lines
    else:
        body_lines = lines[body_start:]
        while body_lines and not body_lines[0].strip():
            body_lines = body_lines[1:]

    body = "\n".join(body_lines).strip() + "\n"
    return title, body


def markdown_path_to_db_path(root: Path, path: Path) -> str:
    rel = path.relative_to(root).with_suffix("")
    return f"/docs/{rel.as_posix()}/"


def collect_pages(root: Path) -> list[tuple[str, str, str]]:
    pages = []
    for path in sorted(root.rglob("*.md")):
        title, content = parse_markdown(path)
        pages.append((markdown_path_to_db_path(root, path), title, content))
    return pages


def build_db(source_dir: Path, output_path: Path) -> None:
    pages = collect_pages(source_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    conn = sqlite3.connect(output_path)
    try:
        conn.executescript(SCHEMA)
        conn.executemany(
            "INSERT INTO pages(path, title, content) VALUES (?, ?, ?)",
            pages,
        )
        conn.commit()
        conn.execute("VACUUM")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source_dir",
        nargs="?",
        default=Path("tests/fixtures/docs-src"),
        type=Path,
        help="directory containing sample markdown files",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default=Path("tests/fixtures/docs.db"),
        type=Path,
        help="path to write the SQLite fixture",
    )
    args = parser.parse_args()
    build_db(args.source_dir, args.output)
