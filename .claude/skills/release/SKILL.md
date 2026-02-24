---
name: Release Snouty
description: This skill should be used when the user asks to "release snouty", "cut a release", "bump the version", "create a release", or provides a version like "release snouty v0.2.0". Handles version validation, Cargo.toml bump, build, test, commit, and tagging.
---

# Release Snouty

Perform a versioned release of snouty by bumping `Cargo.toml`, building, testing, committing, and tagging. _Do not_ push the resulting commit so the user has a chance to audit it first.

## Release Procedure

### 1. Parse and Validate the Version

Extract the version from the user's input. Accept formats like `v0.2.0` or `0.2.0`. Strip the leading `v` to get the bare semver.

Run all of the following sanity checks before making any changes:

- Validate the version matches `MAJOR.MINOR.PATCH` where each component is a non-negative integer.
- Read `Cargo.toml` and extract the current version.
- Confirm the new version is strictly greater than the current version (compare major, then minor, then patch).
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

Tell the user to run `git push && git push --tags` once satisfied.
