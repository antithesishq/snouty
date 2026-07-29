#!/usr/bin/env bash
# Create and start the podman machine used by the macOS CI leg. CI runs this
# twice — once normally, once if the machine has to be recreated — so both
# attempts size the VM the same way.
set -euo pipefail

# Size the VM to the runner: all logical cores and 60% of physical memory,
# instead of the tiny (~2GB) default. Not 100%: the VM's memory is effectively
# exclusive (vfkit grows into it and does not hand it back), and the test
# harness runs on the host, so the host needs headroom.
cpus=$(sysctl -n hw.ncpu)
mem_mib=$(( $(sysctl -n hw.memsize) * 6 / 10 / 1024 / 1024 ))

echo "Provisioning podman machine: ${cpus} cpus, ${mem_mib} MiB memory"

# Time each portion so we can see where setup spends its time.
t=$SECONDS
podman machine init --cpus "${cpus}" --memory "${mem_mib}"
init_s=$(( SECONDS - t ))

t=$SECONDS
podman machine start
start_s=$(( SECONDS - t ))

printf '[timing] machine init: %ss | machine start: %ss\n' "$init_s" "$start_s"
if [ -n "${GITHUB_ACTIONS:-}" ]; then
  echo "::notice title=podman machine timing::machine init ${init_s}s | machine start ${start_s}s"
fi
