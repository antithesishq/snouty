#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
cd "$(dirname "$0")/.." || exit 1
# shellcheck source=scripts/hook-lib.sh
. scripts/hook-lib.sh
require cargo --version
require cargo fmt --version
require cargo clippy --version
# cargo fmt fails only on a file it cannot parse, which the agent must fix.
out=$(cargo fmt 2>&1) || { echo "$out"; exit 2; }
# -D warnings: clippy exits 0 on warnings, and CI denies them.
out=$(cargo clippy --all-targets --quiet --color never -- -D warnings 2>&1) || { echo "$out"; exit 2; }
