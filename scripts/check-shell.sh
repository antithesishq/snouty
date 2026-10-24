#!/usr/bin/env bash
# Stop hooks can run from any directory, so cd to the repo root.
# Exit 2 makes Claude Code show the output and wake the agent.
cd "$(dirname "$0")/.." || exit 1
# shellcheck source=scripts/hook-lib.sh
. scripts/hook-lib.sh
# Skip the check when the tool cannot run.
run shellcheck --version >/dev/null 2>&1 || exit 0
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
out=$(run shellcheck --external-sources "${files[@]}" 2>&1) || { echo "$out"; exit 2; }
