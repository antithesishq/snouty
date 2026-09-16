set -euo pipefail
compose_file=/opt/config/docker-compose.yaml
restart_enabled=${restart_enabled:-yes}
state_dir=/run/antithesis-local-injection
printf '%s\n' "$compose_file" > "$state_dir/compose-file"
podman compose -p antithesis -f "$compose_file" up -d --no-build --pull=never

cat > "$state_dir/restart-compose" <<'SUPERVISOR'
#!/run/current-system/sw/bin/bash
set -euo pipefail
export PATH=/run/current-system/sw/bin:/run/current-system/sw/sbin

compose_file=$1
mode=$2
state_dir=/run/antithesis-local-injection

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

start_composer_rollout() {
  systemctl reset-failed antithesis-test-composer.service 2>/dev/null || true
  systemctl start antithesis-test-composer

  # Type=simple does not wait for the composer to install its rollout watch.
  composer_cgroup=$(systemctl show antithesis-test-composer --property=ControlGroup --value)
  composer_ready=
  for _ in $(seq 1 100); do
    while read -r pid; do
      for fdinfo in /proc/"$pid"/fdinfo/*; do
        if grep -q '^inotify ' "$fdinfo" 2>/dev/null; then
          composer_ready=yes
          break 2
        fi
      done
    done < "/sys/fs/cgroup$composer_cgroup/cgroup.procs"
    if [[ "$composer_ready" == yes ]]; then
      break
    fi
    sleep 0.1
  done
  if [[ "$composer_ready" != yes ]]; then
    echo "Test composer did not become ready for a rollout notification" >&2
    return 1
  fi

  # Closing this file starts the next rollout.
  touch /opt/antithesis/rollouts/new
}

while true; do
  start_composer_rollout
  touch "$state_dir/restart-ready"

  if [[ "$mode" == once ]]; then
    exit 0
  fi

  # No executable test scripts means the composer keeps running. In that
  # case this loop waits indefinitely and leaves the Compose stack alone.
  while systemctl is-active --quiet antithesis-test-composer.service; do
    sleep 0.2
  done

  if [[ "$(systemctl show antithesis-test-composer --property=Result --value)" != success ]]; then
    journalctl -u antithesis-test-composer --no-pager -n 20 >&2
    exit 1
  fi

  if ! composer_has_runnable_commands; then
    echo "Test composer exited without runnable commands; leaving the local Compose project running"
    exit 0
  fi

  echo "Test composer completed; recreating the local Compose project"
  podman compose -p antithesis -f "$compose_file" \
    down --volumes --remove-orphans
  remove_stale_composer_registrations
  podman compose -p antithesis -f "$compose_file" up -d --no-build --pull=never
done
SUPERVISOR
chmod +x "$state_dir/restart-compose"

rm -f "$state_dir/restart-ready"
if [[ "$restart_enabled" == yes ]]; then
  systemd-run \
    --unit=antithesis-local-compose-restart.service \
    --service-type=exec \
    --property=RemainAfterExit=yes \
    "$state_dir/restart-compose" "$compose_file" restart

  supervisor_ready=
  for _ in $(seq 1 100); do
    if [[ -f "$state_dir/restart-ready" ]]; then
      supervisor_ready=yes
      break
    fi
    if ! systemctl is-active --quiet antithesis-local-compose-restart.service; then
      break
    fi
    sleep 0.1
  done
  if [[ "$supervisor_ready" != yes ]]; then
    echo "Local Compose restart supervisor did not become ready" >&2
    journalctl -u antithesis-local-compose-restart.service --no-pager -n 20 >&2 || true
    exit 1
  fi
else
  "$state_dir/restart-compose" "$compose_file" once
fi
