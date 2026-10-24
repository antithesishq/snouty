#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
cd "$(dirname "$0")/.." || exit 1
cargo fmt
out=$(cargo check --all-targets --quiet --color never 2>&1) || { echo "$out"; exit 2; }
