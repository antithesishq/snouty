# Snouty

CLI tool for the Antithesis API. Written in Rust.

## Key Directories

- `specs/` — feature specs
- `specs_engine/` — specs that need a container engine
- `src/` — the CLI source code
- `snouty-macros/` — the proc-macro crate (the second workspace member)
- `help/` — long-help templates (see Help text)
- `tests/` — integration tests
- `scripts/` — maintenance/dev scripts (Python run via `uv`, plus a few bash scripts such as the Stop hooks `check-cargo.sh`, `check-typos.sh`, `check-python.sh` and `check-shell.sh`)
- `.github/workflows/` — CI/CD

## Specs

This project uses spec-driven development. The `./specs` folder contains specs
for all features in the project, and `./specs_engine` the specs that need a
container engine.

Any new subcommands or flags must have a spec accompanying them. Having to change
an existing spec is a good sign of backward-incompatible breakage, which will
be subject to extra review.

`spec_tests` finds each file in `specs/` by itself. A file in `specs_engine/`
does not run until you add it to `tests/spec.rs` by hand: one
`engine_spec_case_test!` entry for podman, and one for docker. Such a test
does nothing when `SNOUTY_TEST_RUNTIME` names the other runtime. When its
runtime is not installed, it skips, except in CI, where it fails. The engine
runner has no `staging` condition, and `skip` fails the test.

## Writing specs

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

## Help text

Each command's long help (`--help`) is a template in `help/`, one file per
command, named for its path below `snouty` with `_` between the parts:
`help/runs_build_logs.txt`. Do not write a long help as a string in Rust code.

- Write each paragraph on one line. `HelpPage::cli` wraps the paragraphs when
  the help prints: to the terminal width, up to 100 columns, and to 80
  columns off a terminal. Indent a line that must keep its layout, such as an
  example command or a table row; an indented line never wraps.
- Each template is an [askama](https://askama.rs) template, which the build
  compiles: a syntax error or a missing include fails `cargo build`. A page
  renders at run time, for two readers: `--help`, and the `tool_help` MCP
  tool. A `HelpPage` row in `src/help/pages.rs` names each page and its
  command.
  `cli::command()` sets each page as its command's long help; parse with
  `cli::parse()`, not with `Cli::parse`. clap renders a page only when it
  builds that command, so a run renders only the pages on its command path.
- Name each flag, positional arg, command and example with `t()`, never as
  literal text. `t()` looks the name up in the clap definition of the page's
  command, and writes it for the reader:

  | template | `--help` | `tool_help` |
  |---|---|---|
  | `{{ t("--begin-vtime")? }}` | `--begin-vtime` | `` `begin_vtime` `` |
  | `{{ t("input_hash")? }}` | `INPUT_HASH` | `` `input_hash` `` |
  | `` `{{ t("runs search")? }}` `` | `` `runs search` `` | `` `runs_search` `` |
  | `{{ t("snouty runs wait <run_id> --until ready")? }}` | the command as written | `runs_wait {"run_id": "<run_id>", "until": "ready"}` |

  Write an example that spans lines, or that holds quotes or backslashes, in
  a filter block, which takes its text as written. The whitespace around
  the text drops out, so the tags stand on lines of their own:

  ```
    {% filter cmd %}
    snouty debug \
      --run-id <run_id> \
      --title "stalled leader election"
    {% endfilter %}
  ```

  Over MCP, the call of a filter block that spans lines puts one param on
  each line, and a placeholder for a number param prints unquoted, as in
  `"vtime": <vtime>`.

  A name that does not exist, an example that clap refuses, or a literal
  value that its parser refuses is an error. A `<placeholder>` can stand for
  any value. A command with flags but not its required args, such as
  `runs properties --detail`, names the command, and over MCP its flags drop
  out. Over MCP, `t()` refuses a command with no tool, an arg that the tool
  leaves out, and a shell pipeline or redirect.
- Do not write that `--json` prints JSON: the flag documents itself. Name
  JSON in help only to describe the shape of the output, or a meaning that
  callers must know.
- A command has two output modes: text output, the default, and JSON, with
  `--json`. Do not call the default "human output".
- An MCP tool call returns the command's text output, with `--detail` when
  the command has that flag, and its `json` param runs it with `--json`
  instead. So the MCP reader needs the layout of the detailed output, and
  the JSON shape as what `json` returns: `With {{ t("--json")? }}, each run
  prints as one JSON object`.
- Put text that only one reader needs, such as the CLI's `--json` paragraph,
  a pipeline example, or a layout that only the default (not `--detail`)
  output has, in `{% if cli %}…{% else %}…{% endif %}`. The other
  reader's branch is not rendered, so `t()` does not check it for that
  reader. `{{ tool_table() }}` is the list of MCP tools.
- Describe what a command reports in words that suit both readers. Put a
  layout that both readers see, such as the columns of a command with no
  `--detail`, outside a block. Say that a command succeeds or fails; name exit
  codes, stdout and stderr only in a CLI block.
- A line that holds only a block tag (`if`, `else`, `endif`) prints as a
  blank line, and the render collapses each run of blank lines into one. So
  put a block that holds whole paragraphs on lines of its own, with blank
  lines around it as you like. In a run of lines that must stay together,
  such as a list of examples, write `{%- if cli %}`, `{%- else %}` and
  `{%- endif %}`: the mark drops the tag's line. A tag inside a line of text
  takes no mark.
- A `t()` error is a test failure: `tests/help_snapshots.rs` renders every
  page for both readers, and the error names the page.
- Write each page's whole text in its own file. When two pages share a
  paragraph, copy it into both: a reviewer reads a page as one file.
- `tests/help_snapshots.rs` keeps an [insta](https://insta.rs) snapshot of
  every command's `--help` as it prints off a terminal, in
  `tests/snapshots/help/`, and of each MCP tool's `tool_help`, in
  `tests/snapshots/mcp_help/`. A test fails when an MCP render names a flag
  outside an example. After you change a template, a flag or a
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
- Tests use only the fake secrets in `src/testutils/fake_secrets.rs`: API
  keys, passwords, tokens, and all other credentials, named `FAKE_<TYPE>`,
  with `_A`, `_B` and so on when a test needs more than one of a type. Its
  module doc tells how to add one, and its `scan` test fails on any other
  secret-shaped literal. A spec writes `${FAKE_<TYPE>}`: `set-env` for a
  variable, `substitute <path>` for a fixture file, and `${FAKE_<TYPE>@R}` in
  a pattern.

## Checks and lints

[mise](https://mise.jdx.dev) runs the checks. `mise.toml` pins the version of
each tool and defines a task for each check, and `mise.lock` records each
tool's checksum. CI runs the same tasks. rustup installs Rust, through
`rust-toolchain.toml`. Trust the config once in each checkout:

```
mise trust
```

`mise run` installs any missing tool before it runs a task:

```
mise run test           # the Rust tests (test:rust) and Python tests (test:scripts)
mise run lint           # every lint that CI runs
mise run fmt            # format the Rust, TOML, Nix and Python files
mise tasks              # list the tasks, such as lint:zizmor
```

The `msrv` CI job checks the workspace on the MSRV. To do that check yourself,
put the `rust-version` from `Cargo.toml` in place of `<rust-version>`:

```
cargo +<rust-version> check --workspace --all-targets --locked
```

On a system that cannot run mise's prebuilt binaries, such as NixOS, use the
nix shell (`nix-shell`, or direnv with `.envrc`) instead. It has the same
tools from nixpkgs, whose versions can differ from CI's. Run the command of a
task from `mise.toml` directly. In a nix shell, the Stop hooks use the nix
tools and do not use mise.

Dependabot does not update `mise.toml`. To bump a tool, change its version
in `mise.toml`, then regenerate the lockfile for the platforms that CI and
contributors use:

```
mise lock --platform linux-x64,macos-arm64,macos-x64
```

CI installs mise itself through `.github/actions/setup-tools`. To bump mise,
change its version there, and the SHA-256 of its binary for each platform:
the `digest` that GitHub shows for each `mise-v<version>-<platform>` asset.

- **Python scripts:** ruff formats and lints them, and pyright checks their
  types. `pyproject.toml` holds the configuration, and `uv.lock` pins the
  versions.
- **zizmor:** `.github/zizmor.yml` holds its configuration. Do not fix a
  finding in a generated workflow by hand. Change its generator, as the
  configuration describes.
- **actionlint:** `.github/actionlint.yaml` holds its configuration. It also
  runs shellcheck on each `run:` script.
- **shellcheck:** fix each finding. Do not disable a check without a comment
  that tells why.
- **cargo-shear:** if it reports a dependency that the code uses (for example,
  only through a macro), add the dependency to
  `[package.metadata.cargo-shear] ignored` and write the reason in a comment.
- **cargo-deny:** `deny.toml` holds its configuration. Fix an advisory with an
  upgrade. Ignore it only if no upgrade exists, and give a reason and a link.
  A pull request fails on licenses, bans and sources. Only the nightly run
  fails on advisories, because a new advisory can appear with no change in the
  repository.
- **MSRV:** `rust-version` in the `[workspace.package]` table of `Cargo.toml`.
  Both crates use it. To change the MSRV, change only that line. CI checks the
  linux-gnu and linux-musl targets on that version, but not macOS.
- **Clippy:** runs with the `pedantic` lint group. The
  `[workspace.lints.clippy]` table in `Cargo.toml` sets it, and lists the
  pedantic lints that are off, with a reason for each. Fix a new finding in the
  code. If a fix is wrong, add an `#[allow(clippy::<lint>)]` on the item, with
  a comment that tells why. CI denies all warnings.

### Checks that the Stop hooks run

The Stop hooks in `.claude/settings.json` run these checks each time the agent
stops, and wake the agent with the errors when a check fails. Do not run them
yourself while you work. The one exception is a push: a hook does not run
until the agent stops, so run the checks before a `git push` in the same turn.

- `scripts/check-cargo.sh`: `cargo fmt`, then
  `cargo clippy --all-targets -- -D warnings`.
- `scripts/check-typos.sh`: `typos`.
- `scripts/check-python.sh`: `ruff format scripts`, then `ruff check scripts`,
  then `pyright`.
- `scripts/check-shell.sh`: `shellcheck` on each `*.sh` file and each script
  with a `sh` or `bash` shebang, tracked or new.

When you add a check to a hook, add it to this list. A hook runs its tool
through `mise exec`, which installs the tool on first use.

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
gates the merge. It skips on a pull request from a fork or from Dependabot,
because GitHub does not pass secrets there. Two rules follow:

- An unprefixed assertion must hold against any tenant. Put
  `stdout 'Run ID +[^ ]'` unprefixed and `stdout 'Run ID .*run-1'` behind
  `[!staging]`.
- The tenant must have a completed run from a launcher other than `debugging`
  among its 100 newest completed runs. `runs.txt` captures that run's id and
  every later command uses it.

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
  machine-readable output; never parse its text output. Detect a version
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
