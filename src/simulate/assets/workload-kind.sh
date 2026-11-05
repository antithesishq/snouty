set -euo pipefail
for commands_file in /opt/antithesis/containers/*/commands.json; do
  [[ -f "$commands_file" ]] || continue
  container_dir=${commands_file%/commands.json}
  container_id=${container_dir##*/}
  [[ -e "/run/crun/$container_id/config.json" ]] || continue
  has_test_commands=$(jq -r '
    any(.commands[]?;
      split("/")[-1] |
      test("^(parallel_driver_|serial_driver_|singleton_driver_|anytime_|finally_|eventually_)")
    )
  ' "$commands_file")
  if [[ "$has_test_commands" == true ]]; then
    echo '"composed"'
    exit 0
  fi
done
echo '"autonomous"'
