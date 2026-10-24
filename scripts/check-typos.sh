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
typos --version >/dev/null 2>&1 || exit 0
out=$(typos --color never 2>&1) || { echo "$out"; exit 2; }
