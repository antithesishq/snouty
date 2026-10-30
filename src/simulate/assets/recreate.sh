set -euo pipefail
export COMPOSE_PROJECT_NAME=antithesis
composer_has_runnable_commands() {
  local commands_file
  for commands_file in /opt/antithesis/containers/*/commands.json; do
    [[ -f "$commands_file" ]] || continue
    if jq -e '
      any(.commands[]?;
        split("/")[-1] |
        test("^(parallel_driver_|serial_driver_|singleton_driver_|anytime_|finally_|eventually_|first_)")
      )
    ' "$commands_file" >/dev/null 2>&1; then
      return 0
    fi
  done
  return 1
}

remove_stale_composer_registrations() {
  local container_dir container_id
  [[ -d /opt/antithesis/containers ]] || return 0
  for container_dir in /opt/antithesis/containers/*; do
    [[ -d "$container_dir" ]] || continue
    container_id=${container_dir##*/}
    if [[ ! -e "/run/crun/$container_id/config.json" ]]; then
      rm -rf -- "$container_dir"
    fi
  done
}

if ! composer_has_runnable_commands; then
  echo '"idle"'
  exit 0
fi
compose_file=/opt/config/docker-compose.yaml
podman compose -p antithesis -f "$compose_file" down --volumes --remove-orphans >&2
remove_stale_composer_registrations
podman compose -p antithesis -f "$compose_file" up -d --no-build --pull=never >&2
echo '"restarted"'
