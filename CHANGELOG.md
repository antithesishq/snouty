# Unreleased

- snouty now requires Docker Compose v2.24.7 or newer and checks for it up front, instead of failing partway through `snouty launch` with an error from compose itself
- `snouty validate` no longer reports an unrelated `docker-compose config` failure as "depends on your shell environment". Only a genuine interpolation failure counts as an environment dependency; anything else surfaces as itself. `PATH` is also no longer scrubbed during that check — compose shells out to `docker`, so scrubbing it made every compose file fail the check on a standalone compose install
- `snouty validate` fails fast when the compose project already has containers — leftovers from a `--keep-running` session or a crashed run — and prints the exact `compose down` command to run first
- `snouty validate` streams container logs in the order things actually happened. It previously started containers detached and then replayed their logs, which printed each container's history as its own block; the startup sequence now interleaves correctly. `--timeout` is now a single budget covering both container startup and the wait for setup-complete (previously each got its own), and its default rises from 60 to 120 seconds to match the old combined ceiling
- `snouty launch` accepts unknown `antithesis.*` params, so new platform params work without a snouty update ([#213](https://github.com/antithesishq/snouty/pull/213))
- Deprecate username/password authentication: `snouty launch` and `snouty debug` print a warning that steers the user to `snouty login` ([#212](https://github.com/antithesishq/snouty/pull/212))
- Add `snouty login`: interactive sign-in with browser-based OAuth or an API key, multiple profiles, and credential storage in the macOS keychain or a credentials file on Linux ([#157](https://github.com/antithesishq/snouty/pull/157), [#173](https://github.com/antithesishq/snouty/pull/173), [#178](https://github.com/antithesishq/snouty/pull/178), [#212](https://github.com/antithesishq/snouty/pull/212))
- Add GitHub Actions OIDC authentication ([#157](https://github.com/antithesishq/snouty/pull/157))
- Add support for the `docker compose` plugin in addition to standalone `docker-compose` ([#172](https://github.com/antithesishq/snouty/pull/172))
- Announce the auto-detected container engine during `launch` and `validate`, and point to the other engine when it holds a missing image ([#170](https://github.com/antithesishq/snouty/pull/170), [#171](https://github.com/antithesishq/snouty/pull/171), [#175](https://github.com/antithesishq/snouty/pull/175))
- `snouty validate` fails when the compose file resolves differently in the Antithesis environment ([#168](https://github.com/antithesishq/snouty/pull/168))
- Add `--limit` to `snouty runs events` ([#176](https://github.com/antithesishq/snouty/pull/176))
- `snouty docs search` treats the query as literal text; add `--match` for FTS5 query syntax ([#166](https://github.com/antithesishq/snouty/pull/166))
- Add the `ANTITHESIS_HTTPS_PROXY` setting to proxy snouty API requests without affecting docker/podman ([#164](https://github.com/antithesishq/snouty/pull/164))
- Add `--param antithesis.images` to register images the config parser cannot discover ([#161](https://github.com/antithesishq/snouty/pull/161))
- Add `ANTITHESIS_EXTRA_HEADERS` to set additional HTTP headers on API requests ([#158](https://github.com/antithesishq/snouty/pull/158))
- Add an unstable update channel ([#195](https://github.com/antithesishq/snouty/pull/195))
- Show vtime as an exact JSON number ([#192](https://github.com/antithesishq/snouty/pull/192))
- Add `COOKBOOK.md` with the failure-moment logs recipe ([#185](https://github.com/antithesishq/snouty/pull/185))


# Version 0.6.1 (2026-06-21)

See the [v0.6.1 release notes](https://github.com/antithesishq/snouty/releases/tag/v0.6.1).
