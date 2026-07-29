#!/usr/bin/env bash
# Create and start the podman machine used by the macOS CI leg.
#
# Lives in a script rather than inline in the workflow because CI runs it twice:
# once normally, and once more if the first attempt hangs and the machine has to
# be recreated (see the `Provision podman machine` steps in
# .github/workflows/build.yml). Both attempts must size the VM identically, so
# the sizing lives here in one place.
#
# Bounding a hang is the workflow's job, not this script's: each attempt runs as
# its own step with `timeout-minutes`, which is the only bound that reliably
# stops these commands. macOS ships no coreutils `timeout`, and the usual
# dependency-free substitute — perl's `alarm`, which survives execve — does not
# work here, because podman is a Go program and the Go runtime absorbs SIGALRM.
#
# Run it by hand the same way CI does:
#
#   scripts/provision-podman-machine.sh
set -euo pipefail

# Size the podman machine VM to the runner: all logical cores and 60% of
# physical memory, instead of the tiny (~2GB) default, so the parallel spec run
# has real CPU and RAM to work with. Not 100%: the VM's memory is effectively
# exclusive (vfkit grows into it and does not hand it back), and the test
# harness — cargo nextest and the snouty processes — runs on the host, so the
# host needs headroom.
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
