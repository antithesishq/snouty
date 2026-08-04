---
name: Release Snouty
description: This skill should be used when the user asks to "release snouty", "cut a release", "cut a pre-release", "cut an rc", "bump the version", "create a release", or provides a version like "release snouty v0.2.0" or "release snouty v0.7.0-rc.1". Handles version validation, Cargo.toml bump, build, test, commit, and tagging, for releases and pre-releases.
---

# Release Snouty

Perform a versioned release of snouty by bumping `Cargo.toml`, building, testing, committing, and tagging. _Do not_ push the resulting commit so the user has a chance to audit it first.

## Pre-releases

A version with an `-rc.N` suffix (e.g. `0.7.0-rc.1`) is a pre-release. The procedure is the same as for a release; only these points differ:

- When the user asks for a pre-release without a full version (e.g. "cut an rc for 0.7.0"), pick the next rc number: list existing tags with `git tag -l 'v0.7.0-rc.*'` and use N+1 of the highest, or `-rc.1` when there are none.
- Shipping is automatic. Pushing the tag triggers the cargo-dist workflow (`.github/workflows/release.yml`), which builds the artifacts and creates the GitHub release. A pre-release suffix in the tag makes cargo-dist mark it as a GitHub **pre-release**, so `snouty update` ignores it unless the user is on the beta channel (`update_channel = "beta"` or `snouty update --channel beta`). Never create the GitHub release by hand — a hand-made release could miss the pre-release flag and ship the rc to everyone.
- `cargo publish` works for pre-releases too, and is safe: cargo only selects a pre-release when a user asks for it explicitly (e.g. `cargo install snouty --version 0.7.0-rc.1`), so stable users never receive it.
- A later full release of the same version (e.g. `0.7.0` after `0.7.0-rc.2`) is a normal upgrade: semver sorts every `-rc.N` before the release.

## Release Procedure

### 1. Parse and Validate the Version

Extract the version from the user's input. Accept formats like `v0.2.0` or `0.2.0`, including pre-release forms like `v0.7.0-rc.1`. Strip the leading `v` to get the bare semver.

Run all of the following sanity checks before making any changes:

- Validate the version matches `MAJOR.MINOR.PATCH` where each component is a non-negative integer, optionally followed by `-rc.N` where N is a positive integer.
- Read `Cargo.toml` and extract the current version.
- Confirm the new version is strictly greater than the current version under semver ordering (compare major, then minor, then patch; a pre-release sorts before its release, so `0.7.0-rc.1` < `0.7.0`, and `0.7.0-rc.2` > `0.7.0-rc.1`).
- Confirm the git tag `vX.Y.Z` does not already exist (`git tag -l vX.Y.Z`).
- Confirm the working tree is clean (`git status --porcelain` returns empty).
- Confirm the current branch is `main`.

If any check fails, report the issue clearly and stop.

### 2. Bump the Version in Cargo.toml

Edit the `version = "..."` line in `Cargo.toml` to the new version.

### 3. Build

Run `cargo build` to update `Cargo.lock` and verify the project compiles.

### 4. Run Tests

Run `cargo nextest run` to ensure everything passes. If tests fail, stop and report.

### 5. Commit the Release

Stage only `Cargo.toml` and `Cargo.lock`. If there are any other changes abort. Then commit with message:

```
chore: Release snouty version X.Y.Z
```

### 6. Create an Annotated Tag

Create an annotated git tag:

```
git tag -a vX.Y.Z -m "chore: Release snouty version X.Y.Z"
```

### 7. Ask user to audit

Do NOT push. Show the user:

- The commit: `git log --oneline -1`
- The tag: `git show vX.Y.Z --no-patch`
- The diff: `git diff HEAD~1`

Tell the user to run the following once satisfied:

```
git push && git push --tags
cargo publish
```

For a pre-release (`-rc.N`), also tell the user that after the tag push, CI creates the GitHub pre-release automatically and only beta-channel users receive it via `snouty update`. Publishing the pre-release crate is safe: cargo never selects a pre-release unless a user requests it explicitly.
