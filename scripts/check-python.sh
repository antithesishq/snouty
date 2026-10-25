#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
cd "$(dirname "$0")/.." || exit 1
# shellcheck source=scripts/hook-lib.sh
. scripts/hook-lib.sh
require uv --version
# uv sets up ruff on first use, which needs the network.
require uv run --quiet ruff --version
# ruff format fails only on a file it cannot parse, which the agent must fix.
out=$(run uv run --quiet ruff format scripts 2>&1) || { echo "$out"; exit 2; }
# ruff check exits 1 for lint errors, and 2 when it cannot run.
out=$(run uv run --quiet ruff check --output-format concise scripts 2>&1)
case $? in
  0) ;;
  1) echo "$out"; exit 2 ;;
  *) fail "ruff check failed to run" "$out" ;;
esac
# The pyright package downloads Node and pyright from npm on its first run.
require uv run --quiet pyright --version
# pyright exits 1 for type errors, and 2 or more when it cannot run.
out=$(run uv run --quiet pyright 2>&1)
case $? in
  0) ;;
  1) echo "$out"; exit 2 ;;
  *) fail "pyright failed to run" "$out" ;;
esac
