# Writing specs

These rules cover `specs/` and `specs_engine/`. The root `AGENTS.md` section
"Tests" says when a spec is the right test.

## What a spec asserts

A spec checks the facts that a user or a script relies on: the exit status
(`!`), the stream (stdout or stderr), IDs, JSON keys, column headers, and the
command in a `Suggestion:` (with `-literal`). Do not match whole sentences of
text output. The help snapshots and the gallery show the wording. For example,
write `stderr 'unknown launcher: no_such_launcher'`, not the full error
paragraph.

- Pass only the flags that the case needs. Leave optional flags, such as
  `--duration`, at their default, so that a change to one flag does not
  rewrite every case.
- A comment says in one line which contract the case checks. Server history,
  release numbers and reasons go in the commit message.
- Use only the fake secrets: `set-env` with `${FAKE_<TYPE>}` for a variable,
  `substitute <path>` for a fixture file, and `${FAKE_<TYPE>@R}` in a pattern.

## Running specs

`spec_tests` finds each file in `specs/` by itself. A file in `specs_engine/`
does not run until you add it to `tests/spec.rs` by hand: one
`engine_spec_case_test!` entry for podman, and one for docker. Such a test
does nothing when `SNOUTY_TEST_RUNTIME` names the other runtime. When its
runtime is not installed, it skips, except in CI, where it fails. The engine
runner has no `staging` condition, and `skip` fails the test.

## Syntax

A spec is a [testscript](https://pkg.go.dev/github.com/rogpeppe/go-internal/testscript)
file, which `tests/spec.rs` runs with our fork of testscript-rs (the
`testscript-rs` git dependency in `Cargo.toml`). A `-- name --` line starts a
fixture file, which lands in `$WORK`. The parser removes blank lines from a
fixture.

- A `stdout` or `stderr` pattern is always a regex, with Go's flags: `^` and
  `$` match at line boundaries, and `.` stops at a newline. Write `(?s)` for a
  `.` that must cross lines.
- `-literal` matches plain text. Use it for fixed text, such as a
  `Suggestion:` line or a command with flags and quotes.
- `-count=N` requires exactly N matches, and N must be at least 1. Write
  `! stdout 'x'` for no match.
- Quotes: the parser reads escapes before the regex gets the pattern. In
  single quotes, it reads `\\`, `\'`, `\n`, `\t` and `\r`. In double quotes
  and outside quotes, it also reads `\"`. It keeps every other backslash.
- So a literal backslash is `\\` with `-literal`, and `\\\\` in a regex. In a
  regex, `'a\\b'` becomes `a\b`, which is a word boundary, not a backslash.
  Prefer `-literal` to `\x5c`.
- The match runs on the output with its trailing whitespace removed. `\n`
  matches a newline between lines, but the last line has no `\n` after it:
  `stdout 'one\ntwo$'` matches the output `one\ntwo\n`, and
  `stdout 'two\n'` does not.
- When a pattern is the name of a file in `$WORK`, the contents of the file
  become the pattern.
- `${VAR}` expands in a pattern, also with `-literal`. `${VAR@R}` expands to
  the value with its regex characters escaped. Bare `$VAR` also expands, and
  `$$` becomes `$`. Write `${VAR}`.
- `snouty` fails the line on a non-zero exit. Write `! snouty ...` for a
  command that must fail.
- `[cond] line` runs the line only when the condition holds, and
  `[!cond] line` only when it does not. `[cond] ! stdout 'x'` passes when the
  condition does not hold, because the line does not run. The
  conditions are the built-in ones, such as `linux`, `darwin`, `net`,
  `exec:<program>` and `env:<VAR>` (which reads the environment of the test
  process, not `set-env`), plus `staging` in `specs/` and `docker` in
  `specs_engine/`. For `[!staging]` and `[staging] skip` (`specs/` only), see
  [Running spec tests against a staging backend](#running-spec-tests-against-a-staging-backend).

## Commands

`tests/spec.rs` adds these commands. `specs_engine/` has only `snouty`,
`mock-server`, `env_from_json`, `set-env`, `substitute` and `build-image`.

- `snouty <args>`: run the binary under test, with a clean environment and
  an isolated config dir.
- `snouty-bg <args>`: run it in the background. It does not expand `${VAR}`,
  and it does not pass `stdin` through.
- `mock-server [-header '<name>: <value>'] [-rate-limit <n>] <status> <body>`:
  answer every request with one response, and set snouty's API URL to it.
  Against staging, it forwards the real credentials.
- `mock-runs-server [empty]`: start the mock Antithesis API, with its fixture
  runs or with none. Against the mock, it replaces the last output with the
  mock's API key, so a `stdout` line right after it checks that key. Against
  staging, it forwards the real credentials and leaves the last output alone,
  so put `[!staging]` on a line that checks the key. `empty` fails against
  staging, so put `[!staging]` on it too.
- `mock-proxy`: start the mock API behind an HTTP proxy, and set
  `ANTITHESIS_HTTPS_PROXY`. It fails against staging, so put `[!staging]` on
  it.
- `env_from_json <line> <key>`: store a key of one NDJSON stdout line as
  `$R_<key>`.
- `env_from_json_find <key> <filter>...`: store a key of the first NDJSON line
  that matches each filter, as `$R_<key>`.
- `file <path> <pattern>`: match a file's contents against a regex. The
  pattern does not get the `(?m)` flag. After `isolate-home`, `~` in the path
  is that `$HOME`.
- `set-env <KEY> <value>`: set a variable, with `${VAR}` expanded in the value.
- `substitute <path>`: expand `${VAR}` in a fixture file.
- `isolate-home <name>`: give the section a fresh `$HOME`, for `snouty login`.
- `setup-docs-db`: seed the docs cache with `tests/fixtures/docs.db`.
- `build-image [--platform <platform>] <name:tag> <dir>`: build a test image
  (`specs_engine/` only).

## Running spec tests against a staging backend

By default `spec_tests` runs against an in-process mock server. To exercise
the real HTTP wiring end-to-end, set `SNOUTY_STAGING=1` and make sure your
normal `ANTITHESIS_*` credentials are exported:

```
SNOUTY_STAGING=1 cargo nextest run spec_tests
```

Required env in staging mode: `ANTITHESIS_TENANT` plus either
`ANTITHESIS_API_KEY` or `ANTITHESIS_USERNAME`+`ANTITHESIS_PASSWORD`.
The `runs` specs require `ANTITHESIS_API_KEY` (every endpoint other than
launch only accepts API key authentication).
`ANTITHESIS_BASE_URL` is optional (defaults to `https://<tenant>.antithesis.com`).
The harness also forwards `ANTITHESIS_EXTRA_HEADERS` and `ANTITHESIS_HTTPS_PROXY`
when they are set.

When `SNOUTY_STAGING` is set, the `mock-runs-server` directive becomes a
pass-through that forwards those vars instead of starting the mock. Spec
lines prefixed with `[!staging]` are skipped (those assert on hardcoded
mock data); unprefixed lines still run and hit staging. Only read-oriented
checks run against staging — a file that would mutate tenant state stops at a
`[staging] skip` line, which skips everything below it.

The `staging` job in `build.yml` runs this suite on every pull request and
gates the merge. It skips on a pull request from a fork or from Dependabot,
because GitHub does not pass secrets there. These rules follow:

- An unprefixed assertion must hold against any tenant. Put
  `stdout 'Run ID +[^ ]'` unprefixed and `stdout 'Run ID .*run-1'` behind
  `[!staging]`.
- Assert the shape of staging data, never a property of the run that staging
  picked, such as its launcher or its report link. When you move an
  assertion behind `[!staging]`, keep an unprefixed shape assertion for the
  same endpoint: staging is the only check for server drift.
- The tenant must have a completed run from a launcher other than `debugging`
  among its 100 newest completed runs. `runs.txt` captures that run's id and
  every later command uses it.
