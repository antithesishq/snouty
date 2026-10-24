#!/usr/bin/env bash
# Format snouty and type-check it. Agent Stop hooks run this before a session
# finishes, from whatever directory the agent's shell is in, so it always cds
# to the repo root first.
#
# Prints cargo check's errors and exits 2 when the check fails. Claude Code
# treats exit 2 as "show the output and wake the agent".
cd "$(dirname "$0")/.." || exit 1
cargo fmt
out=$(cargo check --all-targets --quiet --color never 2>&1) || { echo "$out"; exit 2; }
