set -euo pipefail
unit=${unit:-antithesis-local-compose-restart.service}
result=$(systemctl show "$unit" --property=Result --value)
state=$(systemctl show "$unit" --property=ActiveState --value)
if [[ "$result" != success || "$state" == failed ]]; then
    journalctl -u "$unit" --no-pager -n 20 >&2
    exit 1
fi
