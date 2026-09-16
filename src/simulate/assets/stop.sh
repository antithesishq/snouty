set -euo pipefail
systemctl stop antithesis-local-compose-restart.service 2>/dev/null || true
systemctl stop antithesis-test-composer
if [[ -f /run/antithesis-local-injection/compose-file ]]; then
    podman compose -p antithesis -f /opt/config/docker-compose.yaml down --volumes --remove-orphans
fi
