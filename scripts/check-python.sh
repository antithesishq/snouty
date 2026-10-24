#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
cd "$(dirname "$0")/.." || exit 1
# Put the mise.toml tools on PATH when mise is installed. A nix shell brings
# its own tools.
if [[ -z ${IN_NIX_SHELL:-} ]] && command -v mise >/dev/null 2>&1; then
  eval "$(mise env -s bash 2>/dev/null)"
fi
# A mise shim exists even where mise.toml sets no version, so run the tool.
uv --version >/dev/null 2>&1 || exit 0
# Skip when uv cannot set up ruff, for example with no network.
uv run --quiet ruff --version >/dev/null 2>&1 || exit 0
uv run --quiet ruff format scripts >/dev/null
out=$(uv run --quiet ruff check --output-format concise scripts 2>&1) || { echo "$out"; exit 2; }
# The pyright package downloads Node and pyright from npm on its first run.
# Skip when that fails. Of pyright's own exit codes, only 1 means type errors.
uv run --quiet pyright --version >/dev/null 2>&1 || exit 0
out=$(uv run --quiet pyright 2>&1)
[ $? -ne 1 ] || { echo "$out"; exit 2; }
