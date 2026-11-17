# Vendored into snouty

This directory is a `git subtree` of
<https://github.com/stephenjudkins/erofs-builder-rs> (MIT, see `LICENSE`).
It was added from upstream `main` at `0ab30ab`, whose sources match the
erofs-builder 0.2.1 crate on crates.io.

snouty compiles `src/` into itself as its `erofs_builder` module, and
`tests/erofs_builder.rs` runs the tests in `tests/`. It is not a crate of its
own: `cargo publish` rejects a dependency by path, and `cargo package` leaves
out any directory with a `Cargo.toml`. So `Cargo.toml` and `Cargo.lock` are
deleted, and `src/bin/mkfs-sample.rs` is not built.

Update it with:

```sh
git subtree pull --prefix=vendor/erofs-builder \
  https://github.com/stephenjudkins/erofs-builder-rs main --squash
```

The pull brings back `Cargo.toml` and `Cargo.lock`, as a conflict with their
deletion. Delete them again.

## snouty changes

Each change is marked with a `snouty:` comment, to keep merges small.

- `Writer::link` adds a hard link: a directory entry for an existing inode,
  which then counts it in its nlink.
- `Writer::write_data` writes file contents without naming them, and
  `Writer::add_data` names them later. One `Data` can back several files,
  which share its blocks. `Writer::add_file` does both.
- `Writer::mkdir` on a directory that already exists, for example one that
  a deeper path created implicitly, takes the new metadata. Upstream
  dropped it, which lost `trusted.overlay.opaque` on such directories.

- The modules import each other through `super`, not `crate`, and the
  tests import `snouty::erofs_builder` as `erofs_builder`, because the crate is a module of
  snouty. The code compiles under edition 2024 as well.

Known limits that snouty works around instead of patching: paths must be
UTF-8, and a directory finds a child by a linear scan.
