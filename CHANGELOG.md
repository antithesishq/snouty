# Unreleased

- Converge the human output of `runs logs`, `runs events`, and `runs search` on one event-aware renderer. Each event is auto-classified (SDK assertions, guidance, SDK handshake, setup-complete, faults, container lifecycle, test-composer chatter, plain log text, unknown JSON) and rendered as a concise colored block: a `moment HASH VTIME` divider (full precision, pasteable into `runs logs`/`runs exec`/`snouty debug`) opens each timeline segment, and each event renders as one `VTIME [source] payload` line with dim indented detail lines. `--raw` (now also on `runs events` and `runs search`) is the uninterpreted view: legacy `[vtime] [source] [stream]` lines with verbatim text and raw JSON payloads. Scripts should use `--json`, which is unchanged
- Remove the `Moment.from({...})` input format; `snouty debug --stdin` now accepts JSON only ([#243](https://github.com/antithesishq/snouty/pull/243))
- Add `snouty runs wait`: poll a run until it reaches a terminal state (completed, cancelled, or incomplete) ([#221](https://github.com/antithesishq/snouty/pull/221))
- `snouty validate` reads the SDK output file from the start instead of tailing it, so a setup-complete event written over bytes snouty had already read is still detected ([#218](https://github.com/antithesishq/snouty/pull/218))
- Add `snouty runs search`: run an event-set DSL query against a run's events. The command is gated behind the `runs-search` unstable feature (`SNOUTY_UNSTABLE_FEATURES=runs-search`) ([#206](https://github.com/antithesishq/snouty/pull/206))
- `snouty runs events` now filters entirely server-side and prints each matching event as its JSON, one line per event as it arrives, replacing the buffered table. Multiple `--match` needles require the events-search API (behind the `runs-search` unstable feature) ([#206](https://github.com/antithesishq/snouty/pull/206))
- Add `snouty runs exec`: run a bash script in a run's live session at a given moment, on a fresh branch of the multiverse. The command is gated behind the `runs-exec` unstable feature (`SNOUTY_UNSTABLE_FEATURES=runs-exec`), because the API it calls is unstable and unavailable on most tenants ([#208](https://github.com/antithesishq/snouty/pull/208))
- snouty now requires Docker Compose v2.24.7 or newer, checked up front ([#214](https://github.com/antithesishq/snouty/pull/214))
- `snouty validate` streams container logs in real time and in order, treats `--timeout` as a single budget (default 120 seconds), refuses to start when the project already has leftover containers, and reports compose failures as themselves instead of as environment divergence ([#214](https://github.com/antithesishq/snouty/pull/214))
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
