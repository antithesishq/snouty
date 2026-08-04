# Unreleased

## Login and credentials

- Add OAuth login: `snouty login` offers a browser-based OAuth flow when the tenant supports it, and refreshes tokens automatically with a file-based lock so concurrent snouty processes do not race (#178)
- Migrate `snouty login` to a small TUI (#178)
- `snouty login` stores credentials in the OS keychain on macOS; Linux uses a `credentials.toml` file in the home directory (#157)
- Add GitHub Actions OIDC authentication, with the OIDC token cached within a single invocation (#157)
- `snouty login` confirms what it saved and where (#173)
- `snouty login` rejects blank usernames and empty argument values (#173)
- `snouty login` backs up unparsable settings/credentials files before it overwrites them, and merges into the targeted profile instead of overwriting the whole file (#157)
- Credential setup can be skipped during `snouty login` (#157)
- Log keychain lookup failures other than a missing entry (#181)

## Container engines

- Support the `docker compose` plugin in addition to standalone `docker-compose` (#172)
- `launch` and `validate` announce the auto-detected container engine (#171)
- Warn about the container engine only when the choice is ambiguous (#175)
- When an image is missing, point to the other container engine if it holds the image (#170)
- Detect podman that masquerades as docker via `--version` instead of the `version` subcommand (#182)
- `validate` fails when the compose file resolves differently in the Antithesis environment (#168)
- Explain in errors why images must be local and point to the `--config-image` escape hatch (#169)

## API and CLI

- Add a `VTime` type and show vtime as an exact JSON number (#192)
- Render property-failure concerns at print time for cleaner failure output (#193)
- Classify API failures on the HTTP status alone (#183)
- Events v2: update the OpenAPI spec and add `--limit` to `runs events` (#176)
- `docs search` treats the query as literal text by default; `--match` enables FTS5 syntax (#166)
- Add a Snouty-specific proxy setting (`ANTITHESIS_HTTPS_PROXY` / `https_proxy`), separate from `HTTP(S)_PROXY` which docker/podman also honor (#164)
- Allow extra images via `--param antithesis.images` for images the config parser cannot discover (#161)
- Allow arbitrary additional HTTP headers via `ANTITHESIS_EXTRA_HEADERS`, for consumption by proxies (#158)
- Add an unstable update channel (#195)

## Docs and dependencies

- Add `COOKBOOK.md` with the failure-moment logs recipe (#185)
- Remove the experimental warning from the README (#174)
- Update Cargo dependencies (#196)


# Version 0.6.1 (2026-06-21)

See the [v0.6.1 release notes](https://github.com/antithesishq/snouty/releases/tag/v0.6.1).
