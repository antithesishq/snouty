#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
command -v typos >/dev/null 2>&1 || exit 0
cd "$(dirname "$0")/.." || exit 1
out=$(typos --color never 2>&1) || { echo "$out"; exit 2; }
