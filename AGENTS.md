# Snouty

CLI tool for the Antithesis API. Written in Rust.

## Key Directories

- `src/` — the CLI source code
- `snouty-macros/` — the proc-macro crate (the second workspace member)
- `help/` — long-help templates. Read `help/AGENTS.md` before you change a
  page, `src/help/`, or the help of an MCP tool.
- `specs/` — end-to-end tests of the binary. Read `specs/AGENTS.md` before
  you change a spec here or in `specs_engine/`.
- `specs_engine/` — specs that need a container engine
- `tests/` — integration, PTY, MCP and help snapshot tests
- `scripts/` — maintenance and dev scripts: Python run through `uv`, the
  gallery (`gen-gallery.py`), and the Stop hook scripts (`check-*.sh`)
- `.github/workflows/` — CI/CD

## Workflow

1. Cover each behavior change with the test that "Tests" names for its job.
2. Edit `scripts/gen-gallery.py` when you add a command (add a story), or when
   a gallery check names text that you changed.
3. Run `mise run test` before you finish. Before a `git push`, also run the
   checks that the Stop hooks run (see "Checks and lints") and `mise run lint`:
   a hook does not run until you stop.

## Rust coding conventions

All code must be simple and idiomatic.

- Avoid taking a ref & cloning a value when you can just take the value
- Avoid unnecessary heap allocations
- Use `eyre` for errors
- Use `log` for debug logging
- Do not use `unsafe` without explicit authorization
- Read each module that you change from start to end. Delete the helpers,
  branches and messages that your change makes dead.
- Validate a value once, where it enters, and trust it after that. Do not
  guard against input that cannot occur.

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

- Give a literal a constant when it crosses a module boundary and nothing else
  catches a typo in it: a feature id, an env var that two places read, a
  tenant version. Keep a clap argument id, or a string that one module uses,
  inline.
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
- Make one request where one will do. Outside a diagnostic command such as
  `doctor`, never send a request only to explain or check another response:
  print a hint that names the command to run next.
- Handle only response shapes you have observed or found in the spec. A fixture
  you wrote yourself is not evidence. Record the observation — endpoint, tenant,
  actual response — in the PR or a doc comment.

## User-facing text

These rules cover the help pages in `help/`, the `///` docs of clap flags,
errors, warnings, hints, status lines and the README.

Help tells the user what they need to call a command correctly and to read its
result. snouty does the rest, so leave the details to snouty. Write for the few
things that users do with the command, not for every state that the server can
report.

Say:

- what the command does, in one line;
- what the user supplies or chooses, and the default when it matters;
- behavior that the user can rely on: "The command waits until the run is
  ready";
- a consequence that changes the user's choice: "snouty cannot validate the
  image or pin it to a digest";
- what the command prints: the columns, the line layout, the JSON shape;
- the next command, when there is an obvious one.

Leave out:

- how snouty or the server does it: retries, polling, caching, token refresh,
  profile matching, replay, protocol details;
- why a limit exists, or that snouty enforces it. State the limit;
- what a name already says: that `--json` prints JSON, that `--timeout` sets
  a timeout;
- deprecated flags, and facts about other commands;
- edge cases that the command reports in its own error.

Examples from review:

| Before | After |
|---|---|
| Compose service images must already exist locally — snouty never pulls — unless --allow-remote-images is set. | By default all images must be available locally. Use --allow-remote-images to skip this check. |
| A run must fuzz for at least 15 minutes, a quarter of an hour, so the minimum budget is a quarter of the tier's cores. snouty refuses a launch below the minimum, and names it. | The minimum budget is a quarter of the tier's cores: 12 compute hours on standard, 24 on fast and 48 on turbo. |
| For a moment off the session's own timeline, name the run it comes from with --source-run-id. | Use --source-run-id to load a moment from a different run. |
| Then docker push signs in with your snouty credential, and docker login is not necessary. Podman also reads `${XDG_RUNTIME_DIR}/containers/auth.json`… | Then `docker push`, `docker pull`, `podman push` and `podman pull` automatically use your snouty credentials for the Antithesis docker registry. |

- An error or a warning says what is wrong and the one action to take. It does
  not name status codes, server responses or internal checks, and it does not
  list every cause.
- A status line is short and static: `— end of logs`.
- Use the usual technical word ("truncated") and the user's nouns ("names",
  not "the NAME column").

## Tests

Choose the test by its job: the cheapest test that fails when the behavior
breaks.

- Parsing, validation, conversion and arithmetic: a unit test. Use a Hegel
  property test when the code has a property, such as a round trip, an order,
  or "never panics on any input".
- Rendering a response (tables, notes, empty states): a unit test of the render
  function over a typed value, or an insta snapshot of its output.
- Help text: the help snapshots in `tests/help_snapshots.rs`. No other test
  asserts help prose.
- Terminal-only behavior (prompts, wrapping, progress): a PTY test,
  `tests/*_pty.rs`.
- Behavior that a spec cannot set up, such as a real file system layout or a
  helper protocol: an integration test in `tests/`.
- The MCP protocol: `tests/mcp.rs`.
- The contract of the binary: a spec. That is the exit status, stdout against
  stderr, JSON keys, the request that a flag sends, flags that conflict, how
  settings and credentials resolve, and that bad input fails before any
  request. A new subcommand gets a spec. A new flag gets a spec case only when
  it changes that contract.
- Drift from the live server: an unprefixed spec assertion that runs against
  staging (see `specs/AGENTS.md`).

A breaking change is a removed or renamed flag, a changed JSON key, or a
changed exit status. Say so in the PR. A change to the wording of text output
is not a breaking change.

A test for a fix or a new branch must fail without the change. Check it once:
break the logic (a branch, a condition, a request field), not a string.

- Delete tests that assert a derive, a std-library behavior, an enum variant's
  own spelling, or anything the compiler already guarantees.
- Test behavior through the public entry point. Do not split a function only
  to test it.
- A test helper with more than three parameters takes a builder or `Default`.
- Code whose only callers are tests lives in the test harness: the mock server,
  a test module, or behind `#[cfg(test)]`.
- Tests use only the fake secrets in `src/testutils/fake_secrets.rs`. Its
  module doc tells how to add one.

## Comments, changelog, and prose

- A comment states a precondition, an invariant, a non-obvious reason, or a TODO
  with a trigger. Delete anything else. Narration of how a change was
  investigated belongs in the PR. Prefer to let the code speak for itself.
- A PR makes one change. Groundwork for a later PR goes in that later PR.
- A PR description states the behavior change that the user sees, or says
  that there is none. Before each push, make the title and description agree
  with the branch. Only the release PR edits `CHANGELOG.md`.
- All prose is ASD-STE100 simplified technical English. This does not forbid a
  usual technical word.
- Do not name one platform when the problem is general. Use generic examples,
  not one machine's paths.

## Cookbook

`COOKBOOK.md` collects composable patterns for scripting against snouty —
recipes that combine existing commands rather than adding new ones. Reach for it
when a request would grow the CLI surface to serve a case that `--json` plus a
few lines of shell already covers: a flag that special-cases one command around
another command's concepts is harder to change later than a recipe is. PR #179
(`runs logs --failure`) is the precedent — it was declined as a flag and became
the first recipe instead.

When you add a recipe:

- Give it a header, and add a matching entry to the top-level table of contents.
- Put a metadata block directly under the header, one italic line:
  `*snouty <version> · <date> · source: [#N](<link>)*` — the version the recipe
  was added in (from `Cargo.toml`), the date, and the PR or issue it came from.
  Drop the `source:` segment when there is no link.
- Keep examples minimal and free of error handling — no `set -euo pipefail`, no
  null checks, no retries.
- Verify every command and `jq` expression by running it before writing it down.
  Do not guess at JSON field names.

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

Fix each lint finding in the code. When a fix is wrong, suppress the finding
on the item, with a comment that tells why: `#[allow(clippy::<lint>)]`, a
shellcheck directive, or an entry in `[package.metadata.cargo-shear] ignored`.
Clippy runs with the `pedantic` group, and `[workspace.lints.clippy]` in
`Cargo.toml` gives a reason for each pedantic lint that is off. Fix a
cargo-deny advisory with an upgrade; ignore it in `deny.toml` only when no
upgrade exists, with a reason and a link. A pull request fails on licenses,
bans and sources; only the nightly and manual runs fail on advisories. Do not fix a
zizmor finding in a generated workflow by hand; change its generator, as
`.github/zizmor.yml` describes.

The Python scripts share one set of dependencies, in the top-level
`pyproject.toml` and `uv.lock`. Do not declare dependencies in a script.

The `msrv` CI job checks the workspace on the MSRV, the `rust-version` in the
`[workspace.package]` table of `Cargo.toml`, which both crates use. CI checks
the linux-gnu and linux-musl targets on it, but not macOS. The MSRV tracks the
newest stable Rust: each week, `.github/workflows/bump-rust.yml` updates the
rust-overlay pin and sets that line to the overlay's stable release. When that
changes the line, it pushes the `bump-rust` branch and files an issue with a
link that opens its pull request. To change the MSRV by hand, change only that line. To run the check
yourself:

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

### Checks that the Stop hooks run

The Stop hooks in `.claude/settings.json` run these checks each time the agent
stops, and wake the agent with the errors when a check fails. Do not run them
yourself while you work, except before a push.

- `scripts/check-cargo.sh`: `cargo fmt`, then
  `cargo clippy --all-targets -- -D warnings`.
- `scripts/check-typos.sh`: `typos`.
- `scripts/check-python.sh`: `ruff format scripts`, then `ruff check scripts`,
  then `pyright`.
- `scripts/check-shell.sh`: `shellcheck` on each `*.sh` file and each script
  with a `sh` or `bash` shebang, tracked or new.

When you add a check to a hook, add it to this list. A hook runs its tool
through `mise exec`, which installs the tool on first use.

## Changing this file

Write a rule here only when it applies to most tasks. A rule for one kind of
file goes in the `AGENTS.md` next to those files. When a review corrects one
case, write the general rule, not the one fix. Do not restate what a build, a
test or a lint error already tells the agent.
