#!/usr/bin/env -S uv run
"""Refresh src/openapi.json from the tenant's published spec.

Auth and tenant come from whatever the environment already has, in the same
order snouty itself resolves them:

  API key:  $ANTITHESIS_API_KEY, else the api_key of the ApiKey profile
            ($ANTITHESIS_PROFILE, default "default") in
            ~/.config/snouty/credentials.toml
  Base URL: $ANTITHESIS_BASE_URL, else https://{tenant}.antithesis.com with
            the tenant from $ANTITHESIS_TENANT, else from the tenant key in
            ~/.config/snouty/settings.toml

`--version vN` selects the spec route (/api/vN/openapi.json); the default is
v1.

The downloaded document is passed through `jq .` so the vendored file always
carries one consistent formatting (2-space indent, key order preserved)
regardless of how the server happens to serialize it, keeping refresh diffs
free of formatting noise.

Alongside the spec, src/openapi.provenance.json records where the vendored
document came from — the tenant release version and latest API version (from
GET /api/version) and the spec route fetched — for quick reference when the
spec's provenance is in question.

After a refresh, build.rs may fail with a pinned-count mismatch (see
EXPECTED_ADDITIONAL_PROPERTIES_FALSE and EXPECTED_UNTAGGED_CODE_FENCES):
that is the point — report the spec defect to the API team, then update the
pin.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path
from typing import NoReturn

REPO_ROOT = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO_ROOT / "src" / "openapi.json"
PROVENANCE_PATH = REPO_ROOT / "src" / "openapi.provenance.json"


def fail(message: str) -> NoReturn:
    print(f"error: {message}", file=sys.stderr)
    sys.exit(1)


def snouty_config_dir() -> Path:
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "snouty"


def load_toml(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def resolve_api_key() -> str:
    if key := os.environ.get("ANTITHESIS_API_KEY"):
        return key
    credentials_path = snouty_config_dir() / "credentials.toml"
    profile = os.environ.get("ANTITHESIS_PROFILE", "default")
    entry = load_toml(credentials_path).get(profile, {})
    if entry.get("type") == "ApiKey" and (key := entry.get("api_key")):
        return key
    fail(
        "no API key: set ANTITHESIS_API_KEY, or log in with an ApiKey profile "
        f"({credentials_path})"
    )


def resolve_base_url() -> str:
    if base_url := os.environ.get("ANTITHESIS_BASE_URL"):
        return base_url.rstrip("/")
    settings_path = snouty_config_dir() / "settings.toml"
    tenant = os.environ.get("ANTITHESIS_TENANT") or load_toml(settings_path).get("tenant")
    if not tenant:
        fail(
            "no tenant: set ANTITHESIS_TENANT or ANTITHESIS_BASE_URL, or set "
            f"tenant in {settings_path}"
        )
    return f"https://{tenant}.antithesis.com"


def fetch_json(url: str, api_key: str) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
    # urlopen's built-in default context offers ALPN `http/1.1`, which some
    # HTTPS-intercepting proxies mishandle (the connection dies with an SSL
    # EOF). A plain default context sends no ALPN and works everywhere.
    context = ssl.create_default_context()
    try:
        with urllib.request.urlopen(request, context=context) as response:
            body = response.read()
    except urllib.error.HTTPError as e:
        fail(f"GET {url} failed: {e.code} {e.reason}")
    except urllib.error.URLError as e:
        fail(f"GET {url} failed: {e.reason}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as e:
        fail(f"response from {url} is not JSON: {e}")


def write_via_jq(document: dict, path: Path) -> None:
    """Format `document` with `jq .` and write it to `path`."""
    result = subprocess.run(
        ["jq", "."],
        input=json.dumps(document),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        fail(f"jq failed: {result.stderr.strip()}")
    path.write_text(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--version",
        default="v1",
        help="API version of the spec route to fetch, e.g. v1 (the default)",
    )
    args = parser.parse_args()
    if not re.fullmatch(r"v\d+", args.version):
        fail(f"--version must look like v1, got {args.version!r}")

    api_key = resolve_api_key()
    base_url = resolve_base_url()

    spec_route = f"/api/{args.version}/openapi.json"
    spec_url = f"{base_url}{spec_route}"
    print(f"fetching {spec_url}", file=sys.stderr)
    spec = fetch_json(spec_url, api_key)
    # Refuse to overwrite the vendored spec with something that isn't one (an
    # HTML error page from a proxy, an error envelope, ...).
    if "openapi" not in spec or "paths" not in spec:
        fail(f"response from {spec_url} does not look like an OpenAPI document")

    version_info = fetch_json(f"{base_url}/api/version", api_key)
    provenance = {
        "release_version": version_info.get("release_version"),
        "latest_api_version": version_info.get("latest_api_version"),
        "spec_route": spec_route,
    }

    write_via_jq(spec, SPEC_PATH)
    write_via_jq(provenance, PROVENANCE_PATH)
    print(f"wrote {SPEC_PATH} ({len(spec['paths'])} paths)", file=sys.stderr)
    print(f"wrote {PROVENANCE_PATH} {json.dumps(provenance)}", file=sys.stderr)


if __name__ == "__main__":
    main()
