#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
cd "$(dirname "$0")/.." || exit 1
# Put the mise.toml tools on PATH when mise is installed.
command -v mise >/dev/null 2>&1 && eval "$(mise env -s bash 2>/dev/null)"
# A mise shim exists even where mise.toml sets no version, so run the tool.
shellcheck --version >/dev/null 2>&1 || exit 0
# Check *.sh files and extensionless scripts with a sh or bash shebang,
# tracked or new.
files=()
while IFS= read -r -d '' f; do
  [[ -f $f ]] || continue
  if [[ $f == *.sh ]] || head -n1 "$f" | grep -qE '^#!.*[/ ](bash|dash|sh)( |$)'; then
    files+=("$f")
  fi
done < <(git ls-files -z --cached --others --exclude-standard)
(( ${#files[@]} )) || exit 0
out=$(shellcheck "${files[@]}" 2>&1) || { echo "$out"; exit 2; }
