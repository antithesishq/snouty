#!/usr/bin/env bash
# Usage: scripts/bump-rust.sh VERSION
#
# Pins rust-toolchain.toml to VERSION, a stable release such as 1.101.0, and
# raises `rust-version` in Cargo.toml only when that pays for itself. Clippy
# applies its fixes as if `rust-version` were VERSION, so it uses any std API
# or language feature that a newer Rust brings. When it changes code,
# cargo-msrv finds the lowest Rust that builds the result, and that becomes the
# new `rust-version`. Otherwise `rust-version` stays where it is, so builders
# on an older Rust, such as nixpkgs, are not cut off for nothing.
#
# Needs rustup, cargo-msrv and a clean tree. bump-rust.yml runs it each week,
# and reads the final `rust-version` from the `msrv` step output.
set -euo pipefail
cd "$(dirname "$0")/.."

version=$1
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "not a stable Rust version: $version" >&2; exit 1; }
newest=${version%.*}
current=$(sed -n 's/^rust-version = "\(.*\)"$/\1/p' Cargo.toml)
[[ -n $current ]] || { echo "no rust-version in Cargo.toml" >&2; exit 1; }

set_msrv() {
  sed -i "s/^rust-version = \".*\"$/rust-version = \"$1\"/" Cargo.toml
  grep -qx "rust-version = \"$1\"" Cargo.toml
}

output_msrv() {
  [[ -z ${GITHUB_OUTPUT:-} ]] || echo "msrv=$1" >> "$GITHUB_OUTPUT"
}

sed -i "s/^channel = \".*\"$/channel = \"$version\"/" rust-toolchain.toml
grep -qx "channel = \"$version\"" rust-toolchain.toml

set_msrv "$newest"
cargo clippy --fix --allow-dirty --workspace --all-targets --locked
cargo fmt --all
# Cargo refuses a toolchain older than `rust-version`, so cargo-msrv searches
# from the current value up.
set_msrv "$current"

if git diff --quiet -- '*.rs'; then
  echo "No simplification needs a newer Rust; rust-version stays $current"
  output_msrv "$current"
  exit 0
fi

floor=$(cargo msrv find --min "$current" --max "$newest" --output-format minimal --no-log \
  -- cargo check --workspace --all-targets --locked)
[[ $floor =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "unexpected cargo-msrv result: $floor" >&2; exit 1; }
set_msrv "${floor%.*}"
echo "rust-version: $current -> ${floor%.*}"
output_msrv "${floor%.*}"
