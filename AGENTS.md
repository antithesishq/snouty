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

## Tests

New commands and options must be accompanied by [expect style
tests](tests/cli.rs). Having to change an existing test is a good sign of
backwards incompatible breakage, which will be subject to extra review.

## Checks and lints

Run the following commands to validate code meets required standards:

```
cargo test
cargo clippy
cargo fmt
```

If `cargo nextest` is available, always prefer to use `cargo nextest run` for testing.

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
