# shellcheck shell=bash
# Sourced by the Stop hooks.

# Run a tool through mise, which installs it at the mise.toml version on first
# use. In a nix shell, without mise, or when mise cannot run the tool (an
# untrusted mise.toml, or no network), run the tool from PATH.
run() {
  if [[ -z ${IN_NIX_SHELL:-} ]] && command -v mise >/dev/null 2>&1 &&
    mise exec -- "$1" --version >/dev/null 2>&1; then
    mise exec -- "$@"
  else
    "$@"
  fi
}

# Run a command that proves a tool works, such as `typos --version`, and fail
# the hook when it does not. Exit 1 shows the error to the user without waking
# the agent.
require() {
  local out
  out=$(run "$@" 2>&1) && return
  {
    echo "${0##*/}: cannot run '$*'"
    echo "$out"
  } >&2
  exit 1
}

# Fail the hook, as `require` does, with a tool's output.
fail() {
  {
    echo "${0##*/}: $1"
    echo "$2"
  } >&2
  exit 1
}
