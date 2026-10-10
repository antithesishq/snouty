---
description: Nightly agent audit of snouty's TODOs and server-behavior notes against the live tenant
on:
  schedule: daily
  workflow_dispatch:

permissions:
  contents: read
  issues: read
  pull-requests: read

engine: claude
timeout-minutes: 45

tools:
  edit: false
  bash: [cat, grep, head, tail, sed, wc, ls, jq, "git log:*", "git blame:*", "git show:*"]
  github:
    toolsets: [issues, pull_requests]

# The agent reaches the tenant only through this tool. The API key stays on the
# runner, and the tool runs only the read-only commands below: `launch`,
# `debug` and `runs exec` start billable sessions, and `runs cancel` mutates.
mcp-scripts:
  snouty:
    description: >-
      Run a read-only snouty command against the Antithesis tenant, with snouty
      built from the commit under test. Allowed: `doctor`, and `runs list`,
      `runs show`, `runs properties`, `runs logs`, `runs events`, `runs search`.
      Pass the arguments after `snouty`, e.g. `runs list --limit 5 --json`.
    inputs:
      args:
        type: string
        required: true
        description: Arguments after `snouty`, split like a POSIX shell would.
    timeout: 120
    py: |
      import json, os, shlex, subprocess

      allowed = {"doctor", "runs list", "runs show", "runs properties",
                 "runs logs", "runs events", "runs search"}
      timeout = 110

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
          try:
              p = subprocess.run([os.environ["SNOUTY"], *args], capture_output=True,
                                 text=True, timeout=timeout)
          except subprocess.TimeoutExpired:
              return {**result, "error": f"timed out after {timeout}s"}
          return {**result, "exit_code": p.returncode, "stdout": p.stdout[-60000:],
                  "stderr": p.stderr[-20000:]}

      print(json.dumps(run(inputs.get("args", ""))))
    env:
      SNOUTY: ${{ github.workspace }}/target/debug/snouty
      ANTITHESIS_TENANT: ${{ secrets.STAGING_ANTITHESIS_TENANT }}
      ANTITHESIS_API_KEY: ${{ secrets.STAGING_ANTITHESIS_API_KEY }}

steps:
  - uses: dtolnay/rust-toolchain@02cb101ec7c40f2c49e1d9714d64511d8e1b74de # v1
    with:
      toolchain: stable

  # Save the cache even when the agent fails, so the next night skips
  # rebuilding the vendored OpenSSL and SQLite.
  - uses: Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6 # v2.9.2
    with:
      cache-on-failure: true

  - name: Build snouty
    run: cargo build --locked

  - name: Collect comments
    run: |
      mkdir -p /tmp/gh-aw/agent/todo-audit
      python3 scripts/collect-todos.py > /tmp/gh-aw/agent/todo-audit/todos.json

  # The agent needs the release to tell "still true on 64.0" from "fixed in
  # 65.0". Without it the API checks mean nothing, so a failure stops the run.
  - name: Read tenant release
    env:
      ANTITHESIS_TENANT: ${{ secrets.STAGING_ANTITHESIS_TENANT }}
      ANTITHESIS_API_KEY: ${{ secrets.STAGING_ANTITHESIS_API_KEY }}
    working-directory: /tmp/gh-aw/agent/todo-audit
    run: |
      "$GITHUB_WORKSPACE/target/debug/snouty" doctor --json > doctor.json || true
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
    labels: [todo-audit]
    max: 5
    expires: false
  add-comment:
    target: "*"
    max: 5
---

# Audit TODOs and server-behavior notes

Many of snouty's comments record a server quirk seen on one tenant release, or
a TODO that waits on a server change. They go stale silently when the API
changes. Find the ones that no longer hold, and file an issue for each.

## Inputs

- `/tmp/gh-aw/agent/todo-audit/todos.json` lists every `TODO`, `FIXME` and
  "observed on" comment block in the repository at this commit. Each entry has
  `file`, `line`, `end_line`, `kinds`, `text` and `marker`.
- `/tmp/gh-aw/agent/todo-audit/release.txt` holds the tenant's release version,
  read from `snouty doctor` at the start of this run.
- The repository is checked out in the working directory. Read the code around
  each comment, and the code that exists only because of it.

## Check each comment

For each entry, decide whether the condition it states still holds.

- **A server behavior** ("observed on orbitinghail, release 64.0", "the API
  reports `unknown`", "the server ignores `limit`"): reproduce it with the
  `snouty` tool against the tenant. Prefer `--json` and `--verbose`, which log
  each request. Pick a run to test against with `runs list --json`. Compare the
  release in the comment with the current release: a quirk that still
  reproduces on a later release still holds.
- **A TODO that waits on something** ("when a run records them", "once the API
  reports `in_progress`"): check whether that thing has happened, on the tenant
  or in the repository.
- **A TODO or FIXME with no external trigger**: check whether the code already
  does what it asks.

The `snouty` tool is read-only on purpose. Do not try to launch runs, start
debugging sessions, or run `runs exec`. When a comment can only be checked with
one of those, record it as unverifiable and move on.

A comment is **stale** only when you have evidence: a command whose output
contradicts the comment, or code that already does what the TODO asks. When the
evidence is ambiguous, or a check fails for an unrelated reason, the comment is
not stale. Also flag code that exists only to support a workaround that is no
longer needed, such as a retry loop or a fallback branch.

## Deduplicate

Before filing for a stale comment, search this repository's issues and pull
requests, open and closed, for:

1. its `marker` value, quoted (every issue this workflow files embeds it);
2. its file and line, such as `src/api.rs:606`;
3. a distinctive phrase from its `text`.

Then:

- An **open issue** matches: add a comment to it with tonight's evidence and
  release. Do not open a new issue.
- A **closed issue** matches: do nothing. That includes issues closed as
  completed, won't-fix, or not planned. Never reopen one.
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
  goes with it;
- the `marker` value on its own line, exactly as `todos.json` gives it.

## Summary

Write a short summary to `$GITHUB_STEP_SUMMARY`: the release, how many comments
you checked, and for each one whether it holds, is stale (with the issue or
comment you filed), or could not be checked and why. If nothing is stale, call
the `noop` tool with that summary.
