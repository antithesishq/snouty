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
