set -euo pipefail
unit=antithesis-test-composer.service
result=$(systemctl show "$unit" --property=Result --value)
state=$(systemctl show "$unit" --property=ActiveState --value)
if [[ "$result" != success || "$state" == failed ]]; then
    fault_injector_update --pause
    journalctl -u "$unit" --no-pager -n 20 >&2
    exit 1
fi
if [[ "$state" == active || "$state" == activating ]]; then
    echo '"running"'
else
    fault_injector_update --pause
    echo '"completed"'
fi
