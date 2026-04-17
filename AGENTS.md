# Snouty

CLI tool for the Antithesis API. Written in Rust.

## Key Directories

- `specs/` — feature specs
- `src/` — all source code
- `tests/` — integration tests
- `.github/workflows/` — CI/CD

## Specs

This project uses spec driven development. The `./specs` folder contains specs
for all features in the project.

Any new subcommands or flags must have a spec accompanying them. Having to change
an existing spec is a good sign of backwards incompatible breakage, which will
be subject to extra review.

## Tests

Internal functions must be accompanied by unit tests.

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
`ANTITHESIS_BASE_URL` is optional (defaults to `https://<tenant>.antithesis.com`).

When `SNOUTY_STAGING` is set, the `mock-runs-server` directive becomes a
pass-through that forwards those vars instead of starting the mock. Spec
lines prefixed with `[!staging]` are skipped (those assert on hardcoded
mock data); unprefixed lines still run and hit staging. Only read-oriented
checks run against staging — any spec that would mutate state is gated
`[!staging]`.

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
