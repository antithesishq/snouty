#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
command -v uv >/dev/null 2>&1 || exit 0
cd "$(dirname "$0")/.." || exit 1
# Skip when uv cannot set up ruff, for example with no network.
uv run --quiet ruff --version >/dev/null 2>&1 || exit 0
uv run --quiet ruff format scripts >/dev/null
out=$(uv run --quiet ruff check --output-format concise scripts 2>&1) || { echo "$out"; exit 2; }
# The pyright package downloads Node and pyright from npm on its first run.
# Skip when that fails. Of pyright's own exit codes, only 1 means type errors.
uv run --quiet pyright --version >/dev/null 2>&1 || exit 0
out=$(uv run --quiet pyright 2>&1)
[ $? -ne 1 ] || { echo "$out"; exit 2; }
