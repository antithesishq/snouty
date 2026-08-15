#!/usr/bin/env bash
# Fetch the images the test suite needs and start a registry for it to push to.
#
# The suite pulls these implicitly otherwise — `registry:2` when a test starts
# its own registry, the fixture base images when a spec builds one, and
# k8s-validator when a k8s validate spec runs. Doing it here means a slow or
# rate-limited registry fails setup, with the engine's own error, instead of
# failing a test minutes later for reasons that look like a bug in snouty.
#
# Starting the registry here is also what keeps CI off the per-test container
# path: tests use $SNOUTY_TEST_REGISTRY when it is set and only start their own
# when it isn't, so local runs are unaffected.
#
# Usage:
#   scripts/setup-test-images.sh <engine> [<engine>...]
#
# Pass every engine the suite will exercise; each needs its own copy of the
# images. Under GitHub Actions the registry address is exported to later steps.
#
# Set SNOUTY_TEST_IMAGE_CACHE to a directory to cache the images as tarballs:
# an image the engine does not have is loaded from a tarball there, and one that
# had to be pulled is saved back for the next run. CI caches that directory,
# because GitHub's cache has far better tail latency than Docker Hub and quay,
# and because it takes their rate limits out of the run entirely. That matters
# most on macOS, where every pull also crosses the podman VM's network proxy —
# the pulls that wedge are the ones that cross it.
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <engine> [<engine>...]" >&2
  exit 2
fi

# Floating tags, matching what the tests and fixtures ask for.
IMAGES=(
  registry:2
  busybox
  debian:bookworm-slim
  docker.io/antithesishq/k8s-validator:1.0.0
)

REGISTRY_CONTAINER=snouty-test-registry
# Fixed because tests read it from the environment, and 127.0.0.1 because snouty
# only skips TLS verification for localhost addresses.
REGISTRY_ADDR=127.0.0.1:5000

CACHE_DIR="${SNOUTY_TEST_IMAGE_CACHE:-}"
if [ -n "$CACHE_DIR" ]; then
  mkdir -p "$CACHE_DIR"
fi

# The tarball path for an image: its reference with the characters that cannot
# appear in a filename replaced.
image_tarball() {
  printf '%s/%s.tar' "$CACHE_DIR" "$(printf '%s' "$1" | tr '/:' '__')"
}

for engine in "$@"; do
  for image in "${IMAGES[@]}"; do
    # Already present is the common case for the second engine, which finds
    # what the first one loaded or pulled.
    if "$engine" image inspect "$image" >/dev/null 2>&1; then
      echo "$engine: $image already present"
      continue
    fi

    tarball=""
    if [ -n "$CACHE_DIR" ]; then
      tarball=$(image_tarball "$image")
    fi

    if [ -n "$tarball" ] && [ -f "$tarball" ]; then
      echo "$engine: loading $image from $tarball"
      "$engine" load -i "$tarball"
      continue
    fi

    echo "$engine: pulling $image"
    "$engine" pull "$image"
    if [ -n "$tarball" ]; then
      echo "$engine: saving $image to $tarball"
      "$engine" save "$image" -o "$tarball"
    fi
  done
done

# The first engine hosts the registry; any of them can, since tests reach it
# over TCP rather than through the engine that runs it.
host_engine=$1
"$host_engine" rm -f "$REGISTRY_CONTAINER" >/dev/null 2>&1 || true
"$host_engine" run -d --name "$REGISTRY_CONTAINER" -p "$REGISTRY_ADDR:5000" registry:2

# Fail here, not in the first test that tries to push.
for _ in $(seq 1 60); do
  if curl -fsS "http://$REGISTRY_ADDR/v2/" >/dev/null 2>&1; then
    echo "registry ready at $REGISTRY_ADDR"
    if [ -n "${GITHUB_ENV:-}" ]; then
      echo "SNOUTY_TEST_REGISTRY=$REGISTRY_ADDR" >> "$GITHUB_ENV"
    fi
    exit 0
  fi
  sleep 1
done

echo "registry at $REGISTRY_ADDR never answered /v2/; container logs:" >&2
"$host_engine" logs "$REGISTRY_CONTAINER" >&2 || true
exit 1
