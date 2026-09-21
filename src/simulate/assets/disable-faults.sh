set -euo pipefail
fault_injector_update --pause

cat > /run/antithesis-local-injection/configure-composer <<'CONFIG'
set -euo pipefail
composer_config=/opt/antithesis/test/config.json
config=$(jq '.unpause_fault_injector = false' "$composer_config")
printf '%s\n' "$config" > "$composer_config"
CONFIG

# The guest creates config.json in ExecStartPre, on each composer start.
mkdir -p /run/systemd/system/antithesis-test-composer.service.d
cat > /run/systemd/system/antithesis-test-composer.service.d/fault-control.conf <<'UNIT'
[Service]
ExecStartPre=/run/current-system/sw/bin/bash /run/antithesis-local-injection/configure-composer
UNIT
systemctl daemon-reload
