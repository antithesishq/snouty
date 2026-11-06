#!/usr/bin/env bash
# Build erofs-builder, create a test image with the sample binary,
# and verify it with fsck.erofs / dump.erofs from nixpkgs erofs-utils.
set -euo pipefail

cd "$(dirname "$0")"

IMAGE=erofs-test.img
TREE=erofs-test-tree
TAR="$TREE.tar"
mkdir -p $TREE
trap 'rm -rf "$IMAGE" "$TREE" "$TAR"' EXIT

echo "==> building"
cargo build --features clap

ERFS="nix shell nixpkgs#erofs-utils -c "

fail() { echo "FAIL: $1"; exit 1; }

echo "==> generating fixture image: $IMAGE"
./target/debug/mkfs-sample $IMAGE fixture --build-time 1700000000 --checksum \
  || fail "mkfs-sample failed"

echo "==> fsck on fixture image"
$ERFS fsck.erofs $IMAGE || fail "fsck.erofs rejected fixture image"

echo "==> dump.erofs on fixture image"
$ERFS dump.erofs -s $IMAGE || fail "dump.erofs failed on fixture image"

echo "==> extracting fixture and comparing content"
EXTRACT=erofs-builder-extract
mkdir -p $EXTRACT

trap 'rm -rf "$IMAGE" "$TREE" "$EXTRACT" "$TAR"' EXIT
$ERFS fsck.erofs --extract=$EXTRACT $IMAGE || fail "extraction failed"

[ "$(cat "$EXTRACT/etc/motd")" = "hello from erofs-builder" ] || fail "/etc/motd content mismatch"
[ -L "$EXTRACT/link" ] && [ "$(readlink "$EXTRACT/link")" = "etc/motd" ] || fail "/link symlink mismatch"
if [ "$(id -u)" = "0" ]; then
  [ -c "$EXTRACT/zero" ] || fail "/zero is not a char device"
else
  echo "==> note: not root, skipping /zero char device check (mknod needs root)"
fi
[ -d "$EXTRACT/empty" ] || fail "/empty dir missing"
[ -f "$EXTRACT/empty-file" ] || fail "/empty-file missing"
[ ! -s "$EXTRACT/empty-file" ] || fail "/empty-file should be empty"

echo "==> verifying streamed file content byte-for-byte"
python3 - "$EXTRACT/big.bin" <<'EOF'
import sys
data = open(sys.argv[1], 'rb').read()
expected = bytes(i % 251 for i in range(100_000))
sys.exit(0 if data == expected else 1)
EOF
[ $? -eq 0 ] || fail "big.bin content mismatch"

echo "==> packing a real directory tree and verifying"
mkdir -p "$TREE/sub/deep"
printf 'root file\n' > "$TREE/root.txt"
printf '#!/bin/sh\necho hi\n' > "$TREE/sub/run.sh"
chmod 755 "$TREE/sub/run.sh"
head -c 200000 /dev/urandom > "$TREE/sub/deep/blob.bin"
ln -s root.txt "$TREE/root-link"
mkfifo "$TREE/fifo"

./target/debug/mkfs-sample "$IMAGE" dir --dir "$TREE" --build-time 1700000000 \
  || fail "dir-mode mkfs failed"

$ERFS fsck.erofs "$IMAGE" || fail "fsck.erofs rejected tree image"
$ERFS fsck.erofs --extract="$EXTRACT" "$IMAGE" || fail "tree extraction failed"

cmp "$TREE/root.txt" "$EXTRACT/root.txt" || fail "root.txt mismatch"
cmp "$TREE/sub/run.sh" "$EXTRACT/sub/run.sh" || fail "run.sh mismatch"
cmp "$TREE/sub/deep/blob.bin" "$EXTRACT/sub/deep/blob.bin" || fail "blob.bin mismatch"
[ "$(readlink "$EXTRACT/root-link")" = "root.txt" ] || fail "root-link mismatch"
[ -p "$EXTRACT/fifo" ] || fail "fifo not preserved"

echo "==> packing a tar archive single-pass and verifying"
COPYFILE_DISABLE=1 tar --format=ustar -cf "$TAR" -C "$TREE" .

./target/debug/mkfs-sample "$IMAGE" tar --tar "$TAR" --build-time 1700000000 \
  || fail "tar-mode mkfs failed"

$ERFS fsck.erofs "$IMAGE" || fail "fsck.erofs rejected tar image"
rm -rf "$EXTRACT"
mkdir -p "$EXTRACT"
$ERFS fsck.erofs --extract="$EXTRACT" "$IMAGE" || fail "tar extraction failed"

cmp "$TREE/root.txt" "$EXTRACT/root.txt" || fail "tar: root.txt mismatch"
cmp "$TREE/sub/run.sh" "$EXTRACT/sub/run.sh" || fail "tar: run.sh mismatch"
cmp "$TREE/sub/deep/blob.bin" "$EXTRACT/sub/deep/blob.bin" || fail "tar: blob.bin mismatch"
[ "$(readlink "$EXTRACT/root-link")" = "root.txt" ] || fail "tar: root-link mismatch"
[ -p "$EXTRACT/fifo" ] || fail "tar: fifo not preserved"

echo
echo "ALL TESTS PASSED"
