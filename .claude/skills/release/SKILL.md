---
name: Release Snouty
description: This skill should be used when the user asks to "release snouty", "cut a release", "cut a pre-release", "cut an rc", "bump the version", "create a release", or provides a version like "release snouty v0.2.0" or "release snouty v0.7.0-rc.1". Handles version validation, changelog update, Cargo.toml bump, build, test, commit, and tagging, for releases and pre-releases.
---

# Release Snouty

Perform a versioned release of snouty by updating `CHANGELOG.md`, bumping `Cargo.toml`, building, testing, committing, and tagging. _Do not_ push the resulting commit so the user has a chance to audit it first.

## How the changelog reaches GitHub Releases

cargo-dist reads the top-level `CHANGELOG.md` when the release tag is pushed. It finds the section for the version and puts it in the GitHub Release body. The matching rules are:

1. A heading with the exact version (e.g. `# Version 0.7.0`) matches that version.
2. A pre-release (e.g. `0.7.0-rc.2`) with no exact heading falls back to the stable heading (`# Version 0.7.0`), then to the `# Unreleased` heading. cargo-dist rewrites the heading to include the pre-release version in the GitHub Release.
3. A stable version matches only its exact heading. A missing heading does not fail the release; cargo-dist just omits the notes.

The convention: entries accumulate under `# Unreleased` as PRs land. Pre-releases publish the `# Unreleased` section as-is. A stable release renames `# Unreleased` to its version, so all rc-era entries fold into the final release notes.

## Pre-releases

The procedure for a pre-release (`-rc.N` suffix) is the same as for a release, with one difference: when the user asks for a pre-release without a full version (e.g. "cut an rc for 0.7.0"), pick the next rc number — list existing tags with `git tag -l 'v0.7.0-rc.*'` and use N+1 of the highest, or `-rc.1` when there are none.

## Release Procedure

### 1. Parse and Validate the Version

Extract the version from the user's input. Accept formats like `v0.2.0`, `0.2.0`, or `v0.7.0-rc.1`. Strip the leading `v` to get the bare semver. If `-rc.N` is specified, this is a pre-release.

Run all of the following sanity checks before making any changes:

- Validate the version matches `MAJOR.MINOR.PATCH` where each component is a non-negative integer, optionally followed by `-rc.N` where N is a positive integer.
- Read `Cargo.toml` and extract the current version.
- Confirm the new version is strictly greater than the current version under semver ordering (compare major, then minor, then patch; a pre-release sorts before its release, so `0.7.0-rc.1` < `0.7.0`, and `0.7.0-rc.2` > `0.7.0-rc.1`).
- Confirm the git tag `vX.Y.Z[-rc.N]` does not already exist (`git tag -l vX.Y.Z[-rc.N]`).
- Confirm the working tree is clean (`git status --porcelain` returns empty).
- Confirm the current branch is `main`.

If any check fails, report the issue clearly and stop.

### 2. Update CHANGELOG.md

Read `CHANGELOG.md` and check the `# Unreleased` section.

If the `# Unreleased` section is missing, empty, or stale (it does not cover the changes since the last release tag), draft entries first: list the commits with `git log --oneline vPREV..HEAD` (where `vPREV` is the most recent tag), and write one factual bullet per notable feature. Follow these rules:

- Write each entry as the net change relative to the previous release. When a feature is new in this release, describe the feature once; do not list the iterations that built it (e.g. a rewrite of a command that did not exist in the previous release is part of the feature, not an entry).
- Compress to the set of notable features. The changelog does not need to describe every change or every detail of a feature.
- Link PR numbers with the public base URL, e.g. `([#176](https://github.com/antithesishq/snouty/pull/176))`. Do not use the exe proxy hostname.
- Skip internal-only changes (CI, refactors, dependency bumps).

Then, depending on the release type:

- **Pre-release (`-rc.N`)**: do not rename anything. cargo-dist publishes the `# Unreleased` section under the rc version automatically. Do not create a heading for the rc version.
- **Stable release**: rename `# Unreleased` to `# Version X.Y.Z (YYYY-MM-DD)` with today's date, and insert a fresh section above it:

  ```markdown
  # Unreleased

  Nothing Yet!
  ```

Verify the result parses: `parse-changelog CHANGELOG.md X.Y.Z` must print the section for a stable release; `parse-changelog CHANGELOG.md Unreleased` must print it for a pre-release. Install the CLI with `cargo install parse-changelog` if it is missing. cargo-dist uses this same library, so this check proves the GitHub Release will pick up the notes.

### 3. Bump the Version in Cargo.toml

Edit the `version = "..."` line in `Cargo.toml` to the new version. The
`snouty-macros` crate is versioned in lockstep: bump its `version` in
`snouty-macros/Cargo.toml` and the `=`-pinned version on the
`snouty-macros` dependency in the root `Cargo.toml` to the same value.

### 4. Build

Run `cargo build` to update `Cargo.lock` and verify the project compiles.

### 5. Run Tests

Run `cargo nextest run` to ensure everything passes. If tests fail, stop and report.

### 6. Commit the Release

Stage only `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md`. If there are any other changes abort. Then commit with message:

```
chore: Release snouty version X.Y.Z
```

### 7. Create an Annotated Tag

Create an annotated git tag:

```
git tag -a vX.Y.Z -m "chore: Release snouty version X.Y.Z"
```

### 8. Ask user to audit

Do NOT push. Show the user:

- The commit: `git log --oneline -1`
- The tag: `git show vX.Y.Z --no-patch`
- The diff: `git diff HEAD~1`

Tell the user to run the following once satisfied:

```
git push && git push --tags
```

There is no manual publish step. The tag push starts the release workflow,
which publishes both crates.

## What CI does after the tag push

`.github/workflows/release.yml` is generated by dist from `dist-workspace.toml`.
Do not edit it by hand. Change the config and run `dist generate`.

The workflow does three things on a tag push:

- It builds the binaries and attests them. `github-attestations = true` makes
  each build job run `actions/attest-build-provenance`, so every released
  archive carries a build provenance attestation. Verify one with
  `gh attestation verify <archive> -R antithesishq/snouty`.
- It creates the GitHub Release with the changelog notes.
- It calls `.github/workflows/publish-crates.yml`, which publishes
  `snouty-macros` and then `snouty`. snouty pins the exact `snouty-macros`
  version, so that order is required. `publish-prereleases = true` makes
  `-rc.N` tags publish to crates.io as well.

### crates.io Trusted Publishing

The publish job holds no crates.io token. It mints a GitHub OIDC token, and
crates.io exchanges that token for a registry token that lives for the job.
Each crate needs a Trusted Publishing config on crates.io, under the crate
settings, with these values:

- Repository owner: `antithesishq`
- Repository name: `snouty`
- Workflow filename: `release.yml`. Use the entry-point workflow, not
  `publish-crates.yml`. crates.io reads the filename from the OIDC
  `workflow_ref` claim, which names the workflow that the tag push starts.
- Environment: empty

crates.io attaches a config only to a crate that already exists. A crate with no
published version therefore needs one manual `cargo publish` with a personal
token first. `snouty-macros` is in that state today. Publish it by hand once,
add its Trusted Publishing config, and every later release runs unattended.
