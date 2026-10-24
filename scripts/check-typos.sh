#!/usr/bin/env bash
# Spell-check snouty with typos, if it is installed. Agent Stop hooks run this
# before a session finishes, from whatever directory the agent's shell is in,
# so it always cds to the repo root first.
#
# Prints the misspellings and exits 2 when typos finds any. Claude Code treats
# exit 2 as "show the output and wake the agent".
command -v typos >/dev/null 2>&1 || exit 0
cd "$(dirname "$0")/.." || exit 1
out=$(typos --color never 2>&1) || { echo "$out"; exit 2; }
