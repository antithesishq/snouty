# Unreleased

- Deprecate username/password authentication: `snouty launch` and `snouty debug` print a warning that steers the user to `snouty login` (single sign-on or an API key)
- Add `snouty login`: interactive sign-in with browser-based OAuth or an API key, multiple profiles, and credential storage in the macOS keychain or a credentials file on Linux ([#157](https://github.com/antithesishq/snouty/pull/157), [#173](https://github.com/antithesishq/snouty/pull/173), [#178](https://github.com/antithesishq/snouty/pull/178))
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
