# Snouty

CLI tool for the Antithesis API. Written in Rust.

## Key Directories

- `specs/` — feature specs
- `src/` — all source code
- `help/` — long-help templates (see Help text)
- `tests/` — integration tests
- `scripts/` — maintenance/dev scripts (Python run via `uv`, plus a few bash scripts such as the Stop hooks `check-cargo.sh`, `check-typos.sh`, `check-python.sh` and `check-shell.sh`)
- `.github/workflows/` — CI/CD

## Specs

This project uses spec-driven development. The `./specs` folder contains specs
for all features in the project.

Any new subcommands or flags must have a spec accompanying them. Having to change
an existing spec is a good sign of backward-incompatible breakage, which will
be subject to extra review.

## Help text

Each command's long help (`--help`) is a template in `help/`, one file per
command, named for its path below `snouty` with `_` between the parts:
`help/runs_build_logs.txt`. Do not write a long help as a string in Rust code.

- Write each paragraph on one line. `HelpPage::cli` wraps the paragraphs when
  the help prints: to the terminal width, up to 100 columns, and to 80
  columns off a terminal. Indent a line that must keep its layout, such as an
  example command or a table row; an indented line never wraps.
- `build.rs` renders each template with [minijinja](https://docs.rs/minijinja),
  and `src/help.rs` embeds the renders. A `HelpPage` variant names each page.
- A template error or an undefined variable fails `cargo build`, and the
  error names the file.
- Put text that two or more pages share in `help/fragments/`, and add it with
  `{% include "fragments/<name>.txt" %}`. An `{% include %}` line becomes the
  lines of the included file. The final newline of a page is removed.
- `tests/help_snapshots.rs` keeps an [insta](https://insta.rs) snapshot of
  every command's `--help` as it prints off a terminal, in
  `tests/snapshots/help/`. After you change a template, a flag or a
  subcommand, review the snapshot diff with `cargo insta review`, or accept
  every change and read the diff in git:

  ```
  INSTA_UPDATE=always cargo nextest run --test help_snapshots
  ```
- Specs do not test `--help`. The snapshots cover it.

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
functions (e.g. trivial env/string/path plumbing) this is a judgment call —
skip the test when it would add more indirection or complexity than the coverage
is worth.

- Delete tests that assert a derive, a std-library behavior, an enum variant's
  own spelling, or anything the compiler already guarantees.
- Avoid creating many small functions just to be able to unit test them. Prefer
  to cover behaviors via spec and integration tests whenever possible.
- A test helper with more than three parameters takes a builder or `Default`.
- Code whose only callers are tests lives in the test harness: the mock server,
  a test module, or behind `#[cfg(test)]`.
- Use Hegel property tests to verify code that parses, converts, transforms,
  validates, and normalizes. If the code has clear properties, use Hegel to
  verify they hold.

## Checks and lints

Run the following commands to validate that the code meets required standards:

```
cargo nextest run
cargo clippy
```

If `cargo nextest` is not available, you may use `cargo test`.

The Python scripts in `scripts/` use [ruff](https://docs.astral.sh/ruff/) for
formatting and linting, and [pyright](https://microsoft.github.io/pyright/)
for type checking. `pyproject.toml` holds the configuration, and `uv.lock`
pins the versions. After you change a script, run the script tests:

```
uv run --directory scripts python -m unittest discover -p 'test_*.py'
```

zizmor audits the GitHub Actions workflows and `dependabot.yml` for security
problems. Run it after you change a file in `.github/`. Its config is
`.github/zizmor.yml`. Do not fix a finding in a generated workflow by hand.
Change its generator, as the config describes.

```
uvx zizmor==1.31.1 --offline .
```

When you change a file in `.github/workflows/`, run
[actionlint](https://github.com/rhysd/actionlint). It also runs shellcheck on
each `run:` script when shellcheck is on your `PATH`. Its configuration is in
`.github/actionlint.yaml`. The `actionlint` job in `build.yml` sets the
version.

```
actionlint
```

shellcheck checks each tracked or new `*.sh` file and each script with a `sh`
or `bash` shebang. CI uses shellcheck v0.11.0. Fix each finding. Do not
disable a check without a comment that tells why.

[taplo](https://taplo.tamasfe.dev) 0.10.0 checks the format of the TOML files.
CI fails when a file is not formatted. `.taplo.toml` holds the settings. Run
`taplo fmt` without `--check` to format the files:

```
taplo fmt --check
```

[nixfmt](https://github.com/NixOS/nixfmt) 1.5.0 checks the format of the Nix
files. CI fails when a file is not formatted. The check skips `npins/`, because
npins generates it. Remove `--check` to format the files:

```
git ls-files -z '*.nix' ':!:npins/*' | xargs -0 nixfmt --check
```

Run [cargo-shear](https://github.com/Boshen/cargo-shear) to find unused
dependencies in the workspace. CI runs it in the `cargo-shear` job. If it
reports a dependency that the code uses (for example, only through a macro),
add the dependency to `[package.metadata.cargo-shear] ignored` and write the
reason in a comment.

```
cargo shear
```

The minimum supported Rust version (MSRV) is `rust-version` in the
`[workspace.package]` table of `Cargo.toml`. Both crates use it. The `msrv` CI
job checks the workspace on that version, for the linux-gnu and linux-musl
targets. It does not check macOS. To change the MSRV, change only that
line. To do the check yourself, put that version in place of `<rust-version>`:

```
cargo +<rust-version> check --workspace --all-targets --locked
```

`cargo deny` checks the dependencies against `deny.toml`. It finds security
advisories, licenses that are not on the allow list, and crates from unknown
sources. Fix an advisory with an upgrade. Ignore it only if no upgrade exists,
and give a reason and a link. A pull request fails on licenses, bans and
sources. Only the nightly run fails on advisories, because a new advisory can
appear with no change in the repository.

```
cargo deny check
```

### Checks that the Stop hooks run

The Stop hooks in `.claude/settings.json` run these checks each time the agent
stops, and wake the agent with the errors when a check fails. Do not run them
yourself while you work. The one exception is a push: a hook does not run
until the agent stops, so run the checks before a `git push` in the same turn.

- `scripts/check-cargo.sh`: `cargo fmt`, then `cargo check --all-targets`.
- `scripts/check-typos.sh`: `typos`.
- `scripts/check-python.sh`: `ruff format scripts`, then `ruff check scripts`,
  then `pyright`.
- `scripts/check-shell.sh`: `shellcheck` on the shell scripts.

When you add a check to a hook, add it to this list.

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
The harness also forwards `ANTITHESIS_EXTRA_HEADERS` and `ANTITHESIS_HTTPS_PROXY`
when they are set.

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
- The tenant must have a completed run from a launcher other than `debugging`
  among its 100 newest completed runs. `runs.txt` captures that run's id and
  every later command uses it.

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

Each `scripts/test_*.py` module tests the script it is named after. CI runs
them all, and so can you:

```
cd scripts && uv run python -m unittest discover -p 'test_*.py'
```

## AI Coding Workflow

1. Follow the Rust coding conventions in this file at all times
2. Ensure that all user-visible changes are covered by a CLI spec or integration test
3. Update gen-gallery.py when changing CLI behavior
4. Verify all tests catch what you want them to catch via targeted mutation testing
5. Test, check, and format code before finishing

## Rust coding conventions

All code must be simple and idiomatic.

- Avoid taking a ref & cloning a value when you can just take the value
- Avoid unnecessary heap allocations
- Use `eyre` for errors
- Use `log` for debug logging
- Do not use `unsafe` without explicit authorization

### Types

- Model a fixed set of alternatives as an enum, never as two or more bools, a
  string compared against literals, or an int. A variant carries its own data.
- Parse eagerly. When a type is more precise than `String`, parse into it as
  early as you can. Parsing a string late means the type is missing — add it and
  parse sooner.
- Every CLI argument uses its domain type as the `value_parser`. No dispatch arm
  parses a string.
- Use the right type for the job. For example, prefer to convert `HumanDuration`
  to `Duration` once you no longer need the additional behavior.
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
- Use high-quality dependencies rather than implementing everything from scratch.
  Search std, then crates.io, before writing it yourself. Vet a candidate crate on
  downloads, reverse dependencies, and last release date, and record that in the
  PR. Keep general purpose utilities separate from domain code.
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
  investigated belongs in the PR. Prefer to let the code speak for itself.
- A PR description states the behavior change that the user sees, or says
  that there is none. Before each push, make the title and description agree
  with the branch. Only the release PR edits `CHANGELOG.md`.
- All prose is ASD-STE100 simplified technical English.
- Do not name one platform when the problem is general. Use generic examples,
  not one machine's paths.
