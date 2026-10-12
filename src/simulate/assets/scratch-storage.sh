set -euo pipefail
device=/dev/vda
modprobe virtio_blk
modprobe ext4
for _ in $(seq 1 50); do
  [[ -b "$device" ]] && break
  sleep 0.1
done
[[ -b "$device" ]] || { echo "simulation scratch disk was not detected" >&2; exit 1; }
mkfs.ext4 -F "$device" >/dev/null
mkdir -p /mnt/snouty-scratch /var/tmp /var/lib/containers/storage
mount "$device" /mnt/snouty-scratch
mkdir -p /mnt/snouty-scratch/tmp /mnt/snouty-scratch/storage
mount --bind /mnt/snouty-scratch/tmp /var/tmp
mount --bind /mnt/snouty-scratch/storage /var/lib/containers/storage
mkdir -p /etc/containers/containers.conf.d
cat > /etc/containers/containers.conf.d/99-snouty-simulate.conf <<'CONFIG'
[engine]
image_copy_tmp_dir = "/var/tmp"
CONFIG
