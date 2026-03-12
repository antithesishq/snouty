#!/bin/sh
set -eu

base="podman"
version="podman version 5.0.0"
version_detail="Client: Podman Engine"

echo "$base $*" >> "$SNOUTY_SHIM_LOG"

if [ "${1:-}" = "--version" ]; then
  printf '%s\n' "$version"
  exit 0
fi

if [ "${1:-}" = "version" ]; then
  printf '%s\n' "$version_detail"
  exit 0
fi

if [ "${1:-}" = "compose" ] && [ "${2:-}" = "config" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    printf '%s\n' "$line"
  done < docker-compose.yaml
  exit 0
fi

if [ "${1:-}" = "build" ]; then
  while IFS= read -r _; do :; done
  exit 0
fi

if [ "${1:-}" = "push" ]; then
  joined_args="$*"
  digest='sha256:9999999999999999999999999999999999999999999999999999999999999999'

  case "$joined_args" in
    *snouty-config:*) digest='sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc' ;;
  esac
  case "$joined_args" in
    */app:*) digest='sha256:1111111111111111111111111111111111111111111111111111111111111111' ;;
  esac
  case "$joined_args" in
    */sidecar@*) digest='sha256:2222222222222222222222222222222222222222222222222222222222222222' ;;
  esac

  for arg in "$@"; do
    case "$arg" in
      --digestfile=*)
        printf '%s' "$digest" > "${arg#--digestfile=}"
        ;;
    esac
  done

  exit 0
fi

printf '%s\n' "unexpected args: $*" >&2
exit 1
