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

- Delete tests that assert a derive, a std-library behavior, an enum variant's
  own spelling, or anything the compiler already guarantees.
- Never extract a function only so a test can call it. Inline it and delete the
  test.
- A test helper with more than three parameters takes a builder or `Default`.
- Code whose only callers are tests lives in the test harness: the mock server,
  a test module, or behind `#[cfg(test)]`.

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

The `staging` job in `build.yml` runs this suite on every pull request and
gates the merge. It skips on a fork's pull request, because GitHub does not
pass secrets there. Two rules follow:

- An unprefixed assertion must hold against any tenant. Put
  `stdout 'Run ID +[^ ]'` unprefixed and `stdout 'Run ID .*run-1'` behind
  `[!staging]`.
- The tenant must have at least one completed run. `runs.txt` captures a run id
  from the list and every later command uses it.

A `stdout` or `stderr` pattern is always a regex, with Go's flags: `^` and
`$` match at line boundaries, and `.` stops at a newline. State `(?s)` for a
pattern that spans lines, and `-literal` for plain-text matching. `-count=N`
requires N >= 1; write `! stdout 'x'` for the zero-match assertion.

## Scripts

The `scripts/` directory holds Python helpers run via `uv` (e.g.
`uv run scripts/gen-gallery.py`). The Python version and dependencies are
managed centrally in the top-level `pyproject.toml` rather than per-script.
After changing `gen-gallery.py`, type-check it with pyright and confirm it
reports 0 errors before considering the change done:

```
uv sync                             # so ./.venv holds the dependencies
uvx pyright scripts/gen-gallery.py  # `[tool.pyright]` points it at ./.venv
```

## AI Coding Workflow

1. ensure that all changes are reflected by a spec, update that first if needed,
   but make sure to confirm changes with the developer.
2. practice red-green TDD; write tests first, confirm that they demonstrate the
   desired feature or change, then iterate on code until tests pass
3. test, check, and format code before finishing

Golden rule: always leave the project in a better state than when you started.

## Rust coding conventions

All code must be simple and idiomatic.

- Avoid taking a ref & cloning a value when you can just take the value
- Avoid unnecessary heap allocations
- Use `eyre` for errors
- Use `log` for debug logging

### Types

- Model a fixed set of alternatives as an enum, never as two or more bools, a
  string compared against literals, or an int. A variant carries its own data.
- Parse eagerly. When a type is more precise than `String`, parse into it as
  early as you can. Parsing a string late means the type is missing — add it and
  parse sooner.
- Every CLI argument uses its domain type as the `value_parser`. No dispatch arm
  parses a string.
- A presentation type stops at the presentation boundary. `HumanDuration` parses
  the flag and formats the error; everything past that takes a `Duration`.
- Implement the std trait rather than a bespoke method: `Display` not `as_str`,
  `FromStr` not `from_id`, `From`/`TryFrom` not `to_x`, `Default` not an
  argument-less `new()`. Keep an inherent method only when it returns a type the
  trait cannot produce.

### Duplication

- Give a literal a constant when the compiler cannot catch a typo in it —
  feature ids, env var names, param keys, tenant versions.
- Derive a list of an enum's values from the enum. Add an exhaustive match so a
  new variant fails the build.
- Merge two implementations of one behavior. When both are needed, say why in a
  comment on the survivor.
- Fix a wrong shape in generated code in `build.rs`, not at the call site. Each
  transform asserts its target exists, so a spec refresh fails the build.

### Errors

- Never pass a `Result` into a function. Resolve it where the error can be acted
  on. To annotate an error, use `map_err`; do not pass the whole `Result`. When
  no caller reads the error, map to `Option` where it is produced and note why
  it is dropped.
- An error's suggestion names an action the user can take, or there is no
  suggestion.

### Structure

- Inline a function with one call site, unless it is recursive or it keeps a
  distinct responsibility out of its caller.
- Search std, then crates.io, then write it yourself. Vet a candidate crate on
  downloads, reverse dependencies, and last release date, and record that in the
  PR. A utility you write goes in `util`, not in a domain module.
- Implement the ecosystem trait, then use its combinators. A type that yields a
  sequence implements `futures::Stream`; delete hand-written loops that
  duplicate `map`, `take`, or `filter`.
- A module that drives an external tool exposes a typed API: one method per
  subcommand, typed arguments in, typed values out. It never returns or accepts
  `Command`, argument vectors, or raw process handles. Read the tool's
  machine-readable output; never parse its human-facing text. Detect a version
  by parsing a version number.

### The server boundary

- Send only what the user asked for. Never send a field to set the server's own
  default; strip the field in `build.rs` when the generated type forces a value.
- Handle only response shapes you have observed or found in the spec. A fixture
  you wrote yourself is not evidence. Record the observation — endpoint, tenant,
  actual response — in the PR or a doc comment.

## Comments, changelog, and prose

- A comment states a precondition, an invariant, a non-obvious reason, or a TODO
  with a trigger. Delete anything else. Narration of how a change was
  investigated belongs in the PR.
- A changelog entry describes behavior the user can perceive, one entry per
  feature, with the PR link. Leave out implementation detail unless the reader
  needs it to understand the entry.
- All prose is ASD-STE100 simplified technical english. Do not name one platform
  when the problem is general. Use generic examples, not one machine's paths.
