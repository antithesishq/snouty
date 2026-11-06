# Vendored into snouty

This directory is a `git subtree` of
<https://github.com/stephenjudkins/erofs-builder-rs> (MIT, see `LICENSE`).
It was added from upstream `main` at `0ab30ab`, whose sources match the
erofs-builder 0.2.1 crate on crates.io.

Update it with:

```sh
git subtree pull --prefix=vendor/erofs-builder \
  https://github.com/stephenjudkins/erofs-builder-rs main --squash
```

## snouty changes

Each change is marked with a `snouty:` comment, to keep merges small.

- `Cargo.toml`: `rand` is optional, behind the `clap` feature that the
  sample binary needs, and the unused `crc32c` is gone.
- `Writer::link` adds a hard link: a directory entry for an existing inode,
  which then counts it in its nlink.
- `Writer::mkdir` on a directory that already exists, for example one that
  a deeper path created implicitly, takes the new metadata. Upstream
  dropped it, which lost `trusted.overlay.opaque` on such directories.

Known limits that snouty works around instead of patching: paths must be
UTF-8, and a directory finds a child by a linear scan.
