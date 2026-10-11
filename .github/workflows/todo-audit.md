---
description: Nightly agent audit of snouty's TODOs and server-behavior notes against the live tenant
on:
  schedule: daily
  workflow_dispatch:
  push:
    branches: [issue-382-todo-audit]

permissions:
  contents: read
  issues: read
  pull-requests: read
  id-token: write

# Anthropic Workload Identity Federation: a GitHub OIDC token in place of an
# ANTHROPIC_API_KEY secret.
engine:
  id: claude
  auth:
    type: github-oidc
    provider: anthropic
    federation-rule-id: fdrl_01K4hMS47cps3hp17VYzGqtY
    organization-id: 93f004c5-9d07-4352-b0d1-1730ec1c4b28
    service-account-id: svac_015k3uT4yb8vkcS4WBXQMyc5
    workspace-id: wrkspc_01J1wcfqkKJ5ey1wBmqnYb9D
timeout-minutes: 45

tools:
  edit: false
  bash: [cat, grep, head, tail, wc, ls, jq, "git log:*", "git blame:*", "git show:*"]
  github:
    toolsets: [issues, pull_requests]
  # Claude abandons an MCP call after 60s by default, before a slow
  # `runs search` finishes.
  timeout: 150

# The agent reaches the tenant only through this tool, and the API key stays on
# the runner. `launch`, `debug` and `runs exec` start billable sessions, and
# `runs cancel` mutates, so the tool rejects them.
mcp-scripts:
  snouty:
    description: >-
      Run a read-only snouty command against the Antithesis tenant, with snouty
      built from the commit under test. Allowed: `doctor`, and `runs list`,
      `runs show`, `runs properties`, `runs logs`, `runs events`, `runs search`.
      Pass the arguments after `snouty`, e.g. `runs list --limit 5 --json`.
      Output past 60000 characters is cut, and `truncated` says so.
    inputs:
      args:
        type: string
        required: true
        description: Arguments after `snouty`, split like a POSIX shell would.
    timeout: 140
    py: |
      import json, os, shlex, subprocess, tempfile

      allowed = {"doctor", "runs list", "runs show", "runs properties",
                 "runs logs", "runs events", "runs search"}
      timeout = 120
      limit = 60000

      def run(raw):
          try:
              args = shlex.split(raw)
          except ValueError as err:
              return {"error": f"cannot split arguments: {err}"}
          command = " ".join(args[:2] if args[:1] == ["runs"] else args[:1])
          if command not in allowed:
              return {"error": f"`snouty {command}` is not allowed; allowed: {sorted(allowed)}"}
          if any(a.split("=")[0] in ("--settings", "--profile") for a in args):
              return {"error": "--settings and --profile are not allowed"}
          result = {"command": "snouty " + shlex.join(args)}
          # Settings can redirect the key to another host. Hence no --settings
          # or --profile above, and a cwd with no `./.snouty.toml` in it.
          try:
              with tempfile.TemporaryDirectory() as cwd:
                  p = subprocess.run([os.environ["SNOUTY"], *args], cwd=cwd,
                                     capture_output=True, text=True, timeout=timeout)
          except subprocess.TimeoutExpired:
              return {**result, "error": f"timed out after {timeout}s"}
          return {**result, "exit_code": p.returncode, "stdout": p.stdout[:limit],
                  "stderr": p.stderr[:limit],
                  "truncated": max(len(p.stdout), len(p.stderr)) > limit}

      print(json.dumps(run(inputs.get("args", ""))))
    env:
      SNOUTY: ${{ runner.temp }}/snouty/snouty
      ANTITHESIS_TENANT: ${{ secrets.STAGING_ANTITHESIS_TENANT }}
      ANTITHESIS_API_KEY: ${{ secrets.STAGING_ANTITHESIS_API_KEY }}

# The agent can write to its job's workspace, so a binary built there could be
# swapped for one that prints the API key, and a
# planted `target/` would reach the next night's build through the cache.
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09 # v5.1.0
        with:
          persist-credentials: false

      - uses: dtolnay/rust-toolchain@02cb101ec7c40f2c49e1d9714d64511d8e1b74de # v1
        with:
          toolchain: stable

      - uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6 # v2.9.2

      - name: Build snouty
        run: cargo build --locked

      - uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: snouty
          path: target/debug/snouty
          retention-days: 1

# The binary lives under the runner's temp directory, which the agent cannot
# write.
steps:
  - uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
    with:
      name: snouty
      path: ${{ runner.temp }}/snouty

  - name: Collect comments
    run: |
      chmod +x "$RUNNER_TEMP/snouty/snouty"
      mkdir -p /tmp/gh-aw/agent/todo-audit
      python3 scripts/collect-todos.py > /tmp/gh-aw/agent/todo-audit/todos.json

  # The agent compares each comment's release with the tenant's, so a missing
  # release stops the run. Doctor exits non-zero for checks that do not matter
  # here, such as an unset container registry.
  - name: Read tenant release
    env:
      ANTITHESIS_TENANT: ${{ secrets.STAGING_ANTITHESIS_TENANT }}
      ANTITHESIS_API_KEY: ${{ secrets.STAGING_ANTITHESIS_API_KEY }}
    working-directory: /tmp/gh-aw/agent/todo-audit
    run: |
      "$RUNNER_TEMP/snouty/snouty" doctor --json > doctor.json || true
      jq -er '.checks[] | select(.name == "api") | .notes[]?.text
        | select(startswith("tenant release version: "))
        | ltrimstr("tenant release version: ")' doctor.json > release.txt || {
        echo "snouty doctor reported no tenant release version" >&2
        cat doctor.json >&2
        exit 1
      }
      echo "Tenant release: $(cat release.txt)" >> "$GITHUB_STEP_SUMMARY"

safe-outputs:
  create-issue:
    labels: [nightly-audit]
    max: 5
    expires: false
  add-comment:
    target: "*"
    required-labels: [nightly-audit]
    max: 5
---

# Audit TODOs and server-behavior notes

Many of snouty's comments record a server quirk seen on one tenant release, or
a TODO that waits on a server change. They go stale silently when the API
changes. Find the ones that no longer hold, and report them.

## Inputs

- `/tmp/gh-aw/agent/todo-audit/todos.json` lists every `TODO`, `FIXME` and
  server-behavior comment block in the repository at this commit, with its
  `text` and `marker`.
- `/tmp/gh-aw/agent/todo-audit/release.txt` holds the tenant's release version.
- The repository is checked out in the working directory. Read the code around
  each comment, and the code that exists only because of it.

## Check each comment

For each entry, decide whether the condition it states still holds.

- **A server behavior** ("observed on orbitinghail, release 64.0", "the API
  reports `unknown`", "the server ignores `limit`"): reproduce it with the
  `snouty` tool against the tenant. Prefer `--json`. `--verbose` logs each
  request and response to stderr. Pick a run to test against with `runs list --json`. Compare the
  release in the comment with the current release: a quirk that still
  reproduces on a later release still holds.
- **A TODO that waits on something** ("when a run records them", "once the API
  reports `in_progress`"): check whether that thing has happened, on the tenant
  or in the repository.
- **A TODO or FIXME with no external trigger**: check whether the code already
  does what it asks.

The `snouty` tool runs only read-only commands. When a check needs `launch`,
`debug` or `runs exec`, record the comment as unverifiable.

A comment is **stale** only when you have evidence: a command whose output
contradicts the comment, or code that already does what the TODO asks. When the
evidence is ambiguous, or a check fails for an unrelated reason, the comment is
not stale.

## Deduplicate

Before filing for a stale comment, search this repository's issues and pull
requests, open and closed, for:

1. its `marker` value, quoted (every issue this workflow files embeds it);
2. its file and line, such as `src/api.rs:606`;
3. a distinctive phrase from its `text`.

Then:

- An **open issue** with the `nightly-audit` label matches: add a comment to it
  with tonight's evidence and release. Do not open a new issue.
- An **open issue** without that label matches: do nothing, and name it in the
  summary.
- A **closed issue** matches: do nothing, however it was closed.
- An **open or merged pull request** already removes or changes the comment: do
  nothing.
- Nothing matches: file a new issue.

## File issues

File at most 5 new issues and at most 5 comments. When more comments are stale,
file the ones with the strongest evidence and list the rest in the summary.

Title each issue `<file>:<line>: <what changed>`, for example
`src/api.rs:606: repeat cancel now answers 409`. The body must contain:

- the comment's text and location;
- the evidence: each command you ran with the `snouty` tool, the relevant part
  of its output, and any trace or request ID it printed;
- the tenant release from `release.txt`, and the release the comment names;
- the change you propose: the comment to update or remove, and any code that
  exists only for the workaround, such as a retry loop or a fallback branch;
- the `marker` value on its own line, exactly as `todos.json` gives it.

## Summary

Write a short summary to `/tmp/gh-aw/agent-step-summary.md`, which becomes the
run's step summary: the release, how many comments you checked, and for each
one whether it holds, is stale (with the issue or comment you filed), or could
not be checked and why. If you file no issue and no comment, call the `noop`
tool with that summary.
