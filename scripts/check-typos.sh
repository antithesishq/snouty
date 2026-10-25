#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
cd "$(dirname "$0")/.." || exit 1
# shellcheck source=scripts/hook-lib.sh
. scripts/hook-lib.sh
require typos --version
out=$(run typos --color never 2>&1) || { echo "$out"; exit 2; }
