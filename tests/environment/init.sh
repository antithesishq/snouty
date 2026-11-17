#!@busybox@/bin/sh
# Nix replaces the busybox, bash and dropbear placeholders with store paths.
# shellcheck disable=SC2239

# PID 1 of the toy simulation environment. It implements only the transport
# contract that snouty depends on (see src/simulate/vm.rs), in boot order:
#
# 1. The boot console is the first serial port (ttyS0). The kernel and this
#    script write to it; snouty saves it as boot.log.
# 2. QEMU gives the SSH public key through fw_cfg, at
#    opt/antithesis/authorized_key. It becomes root's authorized key.
# 3. The workload images are on a read-only virtio-blk drive with the serial
#    antithesis-inject, at /dev/disk/by-id/virtio-antithesis-inject. It holds
#    an EROFS image. This environment only names the drive; it does not import it.
# 4. QEMU user networking forwards port 22 of the environment to snouty's socket. sshd
#    accepts root logins with that key, and runs `bash -s` and
#    `tar --extract --file=- --directory=/opt/config`.
# 5. When sshd listens, one ssh_ready line goes to the second serial port
#    (ttyS1), which snouty saves as instrumentation.log.
# 6. `poweroff` and `poweroff -f` power off the machine, so QEMU exits.
#
# It does not implement the services that a Compose run needs, such as
# systemd, podman or the fault injector.

PATH=/bin
export PATH
@busybox@/bin/busybox mkdir -p /bin
@busybox@/bin/busybox --install -s /bin
ln -s @bash@/bin/bash /bin/bash

mkdir -p /proc /sys /dev /tmp /run /etc /root /mnt /opt/config
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mkdir -p /dev/pts
mount -t devpts devpts /dev/pts
echo "toy environment: starting"

# The stock kernel builds these drivers as modules.
modprobe -a virtio_pci virtio_net virtio_blk erofs qemu_fw_cfg af_packet

# 2. The authorized key.
mkdir -p /root/.ssh
cp /sys/firmware/qemu_fw_cfg/by_name/opt/antithesis/authorized_key/raw \
  /root/.ssh/authorized_keys
chmod 700 /root/.ssh
chmod 600 /root/.ssh/authorized_keys

# 3. The /dev/disk/by-id name that udev gives a virtio disk with a serial.
mkdir -p /dev/disk/by-id
for disk in /sys/block/vd*; do
  [ -s "$disk/serial" ] || continue
  ln -s "../../${disk##*/}" "/dev/disk/by-id/virtio-$(cat "$disk/serial")"
done

# 4. The network, by DHCP from QEMU, and sshd. Root logs in to bash, which
#    snouty runs as `bash -s`.
ip link set lo up
ip link set eth0 up
udhcpc -q -n -i eth0 -s @busybox@/default.script
echo "root:x:0:0:root:/root:/bin/bash" >/etc/passwd
echo "root:x:0:" >/etc/group
printf '/bin/bash\n/bin/sh\n' >/etc/shells
mkdir -p /etc/dropbear
@dropbear@/bin/dropbear -R -E -s -p 22
until netstat -ltn | grep -q ':22 '; do
  sleep 0.1
done

# 5. The readiness line, in the instrumentation format:
#    <time> [<source>] [JSON] '<payload>'
echo "$(cut -d ' ' -f 1 /proc/uptime) [antithesis/ssh-ready.json] [JSON]" \
  "'{\"antithesis_host\":{\"status\":\"ssh_ready\"}}'" >/dev/ttyS1
echo "toy environment: SSH ready"

# 6. `poweroff -f` powers off at once. A plain `poweroff` sends SIGUSR2 to
#    PID 1. A trap runs only between commands, so wait on a background sleep,
#    which a signal interrupts.
trap 'poweroff -f' USR2 TERM
while true; do
  sleep 3600 &
  wait $!
done
