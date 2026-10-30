set -euo pipefail
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
  exit 1
fi
