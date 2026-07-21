#!/usr/bin/env bash
# Build the container images for the live `snouty validate` sample projects.
#
# The live samples bake their Antithesis test commands into an image (rather
# than bind-mounting them from the host), so the images must already exist
# locally before `snouty validate` runs — validate never builds and never
# pulls. The gallery generator (scripts/gen-gallery.py) runs this before its
# live validate stories; run it by hand the same way:
#
#   scripts/build-validate-samples.sh
#
# Any sample with a Dockerfile is built (its compose `image:` tag). The
# `missing-image` sample intentionally has no Dockerfile — it references an
# image that doesn't exist, to exercise validate's "image not available"
# path — so it is skipped here.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
samples_dir="$repo_root/tests/fixtures/validate"
base_image="debian:bookworm-slim"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker not found on PATH" >&2
  exit 1
fi
# Resolve a Compose v2 command: the standalone `docker-compose` binary or the
# `docker compose` plugin. snouty supports either, so build with whichever is
# present. A v1 `docker-compose` (banner `docker-compose version 1.x`) is
# rejected — only v2 prints the un-hyphenated `Docker Compose version v2.x`.
compose_is_v2() { "$@" version 2>/dev/null | tr '[:upper:]' '[:lower:]' | grep -q 'docker compose'; }
if compose_is_v2 docker-compose; then
  COMPOSE=(docker-compose)
elif compose_is_v2 docker compose; then
  COMPOSE=(docker compose)
else
  echo "error: Compose v2 not found (need the 'docker-compose' binary or the 'docker compose' plugin)" >&2
  exit 1
fi

# Pull the glibc base once. validate's `--pull=never` means the samples that
# use the base directly (timeout, stranded's app service) need it present, and
# the built images FROM it.
echo "Pulling base image ${base_image}…"
docker pull -q "$base_image" >/dev/null

built=0
for compose in "$samples_dir"/*/docker-compose.yaml; do
  dir="$(dirname "$compose")"
  [ -f "$dir/Dockerfile" ] || continue
  echo "Building image(s) for sample '$(basename "$dir")'…"
  "${COMPOSE[@]}" -f "$compose" build
  built=$((built + 1))
done

echo "Built ${built} sample image(s); base ${base_image} present."
