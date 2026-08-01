# Snouty

CLI tool for the Antithesis API. Written in Rust.

## Key Directories

- `specs/` — feature specs
- `src/` — all source code
- `tests/` — integration tests
- `scripts/` — maintenance/dev scripts (Python, run via `uv`)
- `.github/workflows/` — CI/CD

## Specs

This project uses spec driven development. The `./specs` folder contains specs
for all features in the project.

Any new subcommands or flags must have a spec accompanying them. Having to change
an existing spec is a good sign of backwards incompatible breakage, which will
be subject to extra review.

## Cookbook

`COOKBOOK.md` collects composable patterns for scripting against snouty —
recipes that combine existing commands rather than adding new ones. Reach for it
when a request would grow the CLI surface to serve a case that `--json` plus a
few lines of shell already covers: a flag that special-cases one command around
another command's concepts is harder to change later than a recipe is. PR #179
(`runs logs --failure`) is the precedent — it was declined as a flag and became
the first recipe instead.

Recipes come from declined proposals, repeated questions, and patterns worth
writing down once. When you add one:

- Give it a header, and add a matching entry to the top-level table of contents.
- Put a metadata block directly under the header, one italic line:
  `*snouty <version> · <date> · source: [#N](<link>)*` — the version the recipe
  was added in (from `Cargo.toml`), the date, and the PR or issue it came from.
  Drop the `source:` segment when there is no link.
- Keep examples minimal and free of error handling — no `set -euo pipefail`, no
  null checks, no retries. A recipe is a sketch, not a program; leave room for
  the implementation details a caller's script will need.
- Verify every command and `jq` expression by running it before writing it down.
  Do not guess at JSON field names.

## Tests

Internal functions should be accompanied by unit tests. For small, simple
functions (e.g. trivial env/string/path plumbing) this is a judgement call —
skip the test when it would add more indirection or complexity than the coverage
is worth.

## Checks and lints

Run the following commands to validate code meets required standards:

```
cargo test
cargo clippy
cargo fmt
```

If `cargo nextest` is available, always prefer to use `cargo nextest run` for testing.

### Running spec tests against a staging backend

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

When `SNOUTY_STAGING` is set, the `mock-runs-server` directive becomes a
pass-through that forwards those vars instead of starting the mock. Spec
lines prefixed with `[!staging]` are skipped (those assert on hardcoded
mock data); unprefixed lines still run and hit staging. Only read-oriented
checks run against staging — a file that would mutate tenant state stops at a
`[staging] skip` line, which skips everything below it.

**Never put `!` on a line that also carries a condition.** `[!staging] ! snouty
runs` does not skip under staging — testscript-rs checks the condition inside
the inner executor, which returns `Ok` for a skipped line, and the outer
negation wrapper reads that `Ok` as "the command was expected to fail but
succeeded". The line fails the whole file instead of being skipped, and it is
invisible in a normal run because the condition is met there and the line
executes for real. Write the positive form instead: `stdout -count=0 'x'` for a
negated assertion, or gate the whole block with `[staging] skip`. The
`no_spec_line_combines_a_condition_with_a_negated_command` test enforces this.

**A `stdout`/`stderr` pattern is only a regex if it contains one of
`^ $ [ ( * .`** — otherwise testscript compares it as literal text, so
`stdout 'Run ID\s+\S+'` matches nothing at all and quietly fails. Write
`[^ ]` instead of `\S`, or include a `.`. Enforced by
`no_spec_pattern_looks_like_a_regex_without_being_one`.

Structural assertions must hold against *any* tenant: `stdout 'Run ID +[^ ]'`
belongs unprefixed, `stdout 'Run ID .*run-1'` belongs behind `[!staging]`.

Two CI jobs keep this path honest:

- `staging-harness` in `build.yml` runs the staging code path against the
  in-process mock (`cargo run --example mock_api` supplies the credentials), on
  every PR. No secrets, so it can gate the merge — it is what catches the
  harness rotting.
- `spec-tests` in `staging.yml` runs the same specs against a real tenant,
  nightly and on demand. It needs the `STAGING_ANTITHESIS_TENANT` and
  `STAGING_ANTITHESIS_API_KEY` repository secrets (plus optional
  `STAGING_ANTITHESIS_BASE_URL`), and warns rather than fails when they are
  absent.

## Scripts

The `scripts/` directory holds Python helpers run via `uv` (e.g.
`uv run scripts/gen-gallery.py`). The Python version and dependencies are
managed centrally in the top-level `pyproject.toml` rather than per-script.
After changing `gen-gallery.py`, type-check it with pyright and confirm it
reports 0 errors before considering the change done:

```
uvx pyright scripts/gen-gallery.py
```

## AI Coding Workflow

1. ensure that all changes are reflected by a spec, update that first if needed,
   but make sure to confirm changes with the developer.
2. practice red-green TDD; write tests first, confirm that they demonstrate the
   desired feature or change, then iterate on code until tests pass
3. test, check, and format code before finishing

Golden rule: always leave the project in a better state than when you started.

## Rust coding conventions

- All code must be simple and idiomatic
- Avoid taking a ref & cloning a value when you can just take the value
- Avoid unnecessary heap allocations
- Use `eyre` for errors
- Use `log` for debug logging
