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

for engine in "$@"; do
  for image in "${IMAGES[@]}"; do
    # Already present is the common case on macOS, where k8s-validator is
    # restored from a cache tarball before this runs.
    if "$engine" image inspect "$image" >/dev/null 2>&1; then
      echo "$engine: $image already present"
      continue
    fi
    echo "$engine: pulling $image"
    "$engine" pull "$image"
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
