#!/usr/bin/env bash
# Refresh src/openapi.json from the tenant's published spec.
#
# Auth and tenant come from whatever the environment already has, in the same
# order snouty itself resolves them:
#
#   API key:  $ANTITHESIS_API_KEY, else the api_key of the ApiKey profile
#             ($ANTITHESIS_PROFILE, default "default") in
#             ~/.config/snouty/credentials.toml
#   Base URL: $ANTITHESIS_BASE_URL, else https://{tenant}.antithesis.com with
#             the tenant from $ANTITHESIS_TENANT, else from the tenant key in
#             ~/.config/snouty/settings.toml
#
# The downloaded document is passed through `jq .` so the vendored file always
# carries one consistent formatting (2-space indent, key order preserved)
# regardless of how the server happens to serialize it, keeping refresh diffs
# free of formatting noise.
#
# After a refresh, build.rs may fail with a pinned-count mismatch (see
# EXPECTED_ADDITIONAL_PROPERTIES_FALSE and EXPECTED_UNTAGGED_CODE_FENCES):
# that is the point — report the spec defect to the API team, then update the
# pin.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
spec_path="$repo_root/src/openapi.json"
config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/snouty"

for tool in curl jq; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: $tool not found on PATH" >&2
    exit 1
  fi
done

# Read `key = "value"` out of one [section] of a snouty TOML file. snouty
# writes these files itself in a flat, predictable shape, so line-oriented
# parsing is sufficient; this is not a general TOML parser.
toml_get() {
  local file="$1" section="$2" key="$3"
  [ -f "$file" ] || return 0
  awk -v section="$section" -v key="$key" '
    /^\[/ { in_section = ($0 == "[" section "]") ; next }
    in_section && $1 == key && $2 == "=" {
      gsub(/^"|"$/, "", $3); print $3; exit
    }
  ' "$file"
}

# A settings.toml top-level key lives before any [section] header.
toml_get_toplevel() {
  local file="$1" key="$2"
  [ -f "$file" ] || return 0
  awk -v key="$key" '
    /^\[/ { exit }
    $1 == key && $2 == "=" { gsub(/^"|"$/, "", $3); print $3; exit }
  ' "$file"
}

api_key="${ANTITHESIS_API_KEY:-}"
if [ -z "$api_key" ]; then
  profile="${ANTITHESIS_PROFILE:-default}"
  credentials="$config_dir/credentials.toml"
  if [ "$(toml_get "$credentials" "$profile" type)" = "ApiKey" ]; then
    api_key="$(toml_get "$credentials" "$profile" api_key)"
  fi
fi
if [ -z "$api_key" ]; then
  echo "error: no API key: set ANTITHESIS_API_KEY, or log in with an ApiKey profile ($config_dir/credentials.toml)" >&2
  exit 1
fi

base_url="${ANTITHESIS_BASE_URL:-}"
if [ -z "$base_url" ]; then
  tenant="${ANTITHESIS_TENANT:-$(toml_get_toplevel "$config_dir/settings.toml" tenant)}"
  if [ -z "$tenant" ]; then
    echo "error: no tenant: set ANTITHESIS_TENANT or ANTITHESIS_BASE_URL, or set tenant in $config_dir/settings.toml" >&2
    exit 1
  fi
  base_url="https://$tenant.antithesis.com"
fi
base_url="${base_url%/}"

spec_url="$base_url/api/v1/openapi.json"
echo "fetching $spec_url" >&2

body="$(curl --fail --silent --show-error -H "Authorization: Bearer $api_key" "$spec_url")"

# Refuse to overwrite the vendored spec with something that isn't one (an
# HTML error page from a proxy, an error envelope, ...).
if ! jq -e '.openapi and .paths' >/dev/null 2>&1 <<<"$body"; then
  echo "error: response from $spec_url does not look like an OpenAPI document" >&2
  exit 1
fi

jq . <<<"$body" > "$spec_path"
echo "wrote $spec_path ($(jq -r '.paths | length' "$spec_path") paths)" >&2
