set -euo pipefail
compose_file=/opt/config/docker-compose.yaml
export COMPOSE_PROJECT_NAME=antithesis
podman compose -p antithesis -f "$compose_file" up -d --no-build --pull=never
