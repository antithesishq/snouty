# shellcheck shell=bash
set -euo pipefail
# The workload images arrive as a read-only EROFS drive after the scratch
# disk. containers-storage reads them in place as an additional image store.
device=/dev/vdb
store=/var/lib/containers/snouty-images
config=/etc/containers/storage.conf
modprobe virtio_blk
modprobe erofs 2>/dev/null || true
for _ in $(seq 1 50); do
  [[ -b "$device" ]] && break
  sleep 0.1
done
[[ -b "$device" ]] || { echo "simulation image disk was not detected" >&2; exit 1; }
mkdir -p "$store"
mount -t erofs -o ro "$device" "$store"
if grep -q '^\[storage.options\]' "$config"; then
  echo "$config already has [storage.options]; cannot add the image store" >&2
  exit 1
fi
# The configuration is a link into the read-only Nix store; replace it.
original=$(cat "$config")
rm -f "$config"
printf '%s\n\n[storage.options]\nadditionalimagestores = ["%s"]\n' "$original" "$store" > "$config"
