set -euo pipefail
export COMPOSE_PROJECT_NAME=antithesis
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

fault_injector_update --pause
compose_file=/opt/config/docker-compose.yaml
podman compose -p antithesis -f "$compose_file" down --volumes --remove-orphans >&2
remove_stale_composer_registrations
podman compose -p antithesis -f "$compose_file" up -d --no-build --pull=never >&2
