---
description: Nightly agent audit of snouty's TODOs and server-behavior notes against the live tenant
on:
  schedule: daily
  workflow_dispatch:

permissions:
  contents: read
  issues: read
  pull-requests: read
  id-token: write

# Anthropic Workload Identity Federation: a GitHub OIDC token in place of an
# ANTHROPIC_API_KEY secret.
engine:
  id: claude
  model: claude-opus-5-5
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

# The agent reaches the tenant only through this tool, and the API key stays on
# the runner. The tool offers all of snouty but enforces a nightly budget for
# what costs money or touches other people's runs: 2 launches of at most 15
# minutes, 1 debugging session, and 30 `runs exec` calls. It records each run it
# creates in a ledger on the runner, and the post-step cancels them all.
mcp-scripts:
  snouty:
    description: >-
      Run snouty, built from the commit under test, against the Antithesis
      tenant. Pass the arguments after `snouty`, e.g. `runs list --limit 5
      --json`. Each night allows 2 `launch` runs of at most 15 minutes, 1
      `debug` session and 30 `runs exec` calls; the result's `budget` shows
      what is left. Runs the audit creates are cancelled when it ends.
      `runs exec` refuses other people's debugging sessions, and `runs cancel`
      refuses live runs the audit did not start. Output past 60000 characters
      is cut, and `truncated` says so.
    inputs:
      args:
        type: string
        required: true
        description: Arguments after `snouty`, split like a POSIX shell would.
    # Claude drops an MCP call after 60s. Raising that with tools.timeout breaks
    # the MCP gateway's config in gh-aw v0.91.7, so stay under it.
    timeout: 55
    py: |
      import fcntl, json, os, re, shlex, subprocess, tempfile

      # Per-night budget. A launch or debug call that times out counts as created,
      # because it may have created a run the tool never saw.
      LIMITS = {"launch": 2, "debug": 1, "exec": 30}
      ATTEMPTS = 6
      MAX_MINUTES = 15
      TIMEOUT = 50
      LIMIT = 60000
      # `validate` and `launch -c` read a compose directory, which the agent can
      # write under /tmp/gh-aw, and would build and run its images on the runner.
      DENIED = {"login", "settings", "update", "completions", "validate"}
      TERMINAL = {"completed", "cancelled", "incomplete"}
      VALUED = {"--container", "--source-run-id", "--source-session-id", "--timeout"}
      TAG = "nightly_audit.workflow_run=" + os.environ.get("AUDIT_RUN", "")

      def snouty(args):
          # Settings can redirect the key to another host. Hence no --settings or
          # --profile, and a cwd with no `./.snouty.toml` in it.
          with tempfile.TemporaryDirectory() as cwd:
              return subprocess.run([os.environ["SNOUTY"], *args], cwd=cwd,
                                    capture_output=True, text=True, timeout=TIMEOUT)

      def ledger(update):
          with open(os.environ["AUDIT_LEDGER"], "a+") as f:
              fcntl.flock(f, fcntl.LOCK_EX)
              f.seek(0)
              data = json.loads(f.read() or "{}")
              try:
                  return update(data)
              finally:
                  f.seek(0)
                  f.truncate()
                  json.dump(data, f)

      def budget(data):
          return {k: f"{data.get(k, {}).get('created', 0)}/{n}" if k != "exec"
                  else f"{data.get('exec', 0)}/{n}" for k, n in LIMITS.items()}

      def execute(args):
          result = {"command": "snouty " + shlex.join(args)}
          try:
              p = snouty(args)
          except subprocess.TimeoutExpired:
              return {**result, "error": f"timed out after {TIMEOUT}s"}
          return {**result, "exit_code": p.returncode, "stdout": p.stdout[:LIMIT],
                  "stderr": p.stderr[:LIMIT],
                  "truncated": max(len(p.stdout), len(p.stderr)) > LIMIT}

      def minutes(value):
          m = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m?)?", value)
          if not value or not m:
              return None
          return int(m.group(1) or 0) * 60 + int(m.group(2) or 0)

      def option(args, name):
          for i, a in enumerate(args):
              if a == name and i + 1 < len(args):
                  return args[i + 1]
              if a.startswith(name + "="):
                  return a.split("=", 1)[1]
          return None

      def positional(words):
          skip = False
          for w in words:
              if skip:
                  skip = False
              elif w.startswith("-"):
                  skip = w in VALUED
              else:
                  return w
          return None

      def created_id(stdout):
          try:
              obj = json.loads(stdout)
          except json.JSONDecodeError:
              return None
          return obj.get("runId") or obj.get("run_id") if isinstance(obj, dict) else None

      def create(kind, args):
          def go(data):
              used = data.setdefault(kind, {"created": 0, "attempts": 0})
              if used["created"] >= LIMITS[kind]:
                  return {"error": f"tonight's budget of {LIMITS[kind]} {kind} is spent"}
              if used["attempts"] >= ATTEMPTS:
                  return {"error": f"tonight's {ATTEMPTS} {kind} attempts are spent"}
              used["attempts"] += 1
              result = execute(args)
              if "error" in result:
                  used["created"] += 1
                  data.setdefault("uncertain", []).append(kind)
              elif result["exit_code"] == 0:
                  used["created"] += 1
                  run_id = created_id(result["stdout"])
                  if run_id:
                      data.setdefault("runs", []).append({"id": run_id, "kind": kind})
                  else:
                      data.setdefault("uncertain", []).append(kind)
              return {**result, "budget": budget(data)}
          return ledger(go)

      def launch(args):
          if any(a.split("=")[0] in ("-c", "--config") or a.startswith("-c") and len(a) > 2 for a in args):
              return {"error": "launch from --config-image or a webhook, not a local --config directory"}
          if any("antithesis.duration" in a for a in args):
              return {"error": "set the duration with --duration, at most 15 minutes"}
          duration = option(args, "--duration")
          if duration is None:
              args += ["--duration", str(MAX_MINUTES)]
          elif (minutes(duration) or MAX_MINUTES + 1) > MAX_MINUTES:
              return {"error": f"--duration must be at most {MAX_MINUTES} minutes, such as 15 or 15m"}
          if "--ephemeral" not in args:
              args.append("--ephemeral")
          if "--json" not in args:
              args.append("--json")
          return create("launch", args + ["--param", TAG])

      def guarded(sub, args, words):
          run_id = positional(words[2:])
          tracked = ledger(lambda d: [r["id"] for r in d.get("runs", [])])
          if run_id and run_id not in tracked:
              try:
                  p = snouty(["runs", "show", run_id, "--json"])
              except subprocess.TimeoutExpired:
                  return {"error": f"could not look up {run_id} to check it is safe to {sub}"}
              if p.returncode == 0:
                  info = json.loads(p.stdout)
                  if sub == "exec" and info.get("launcher") == "debugging":
                      return {"error": "that debugging session is not the audit's own; open one with `debug`"}
                  if sub == "cancel" and info.get("status") not in TERMINAL:
                      return {"error": "that run is live and the audit did not start it"}
          if sub != "exec":
              return execute(args)
          def go(data):
              if data.get("exec", 0) >= LIMITS["exec"]:
                  return {"error": f"tonight's budget of {LIMITS['exec']} exec calls is spent"}
              data["exec"] = data.get("exec", 0) + 1
              return {**execute(args), "budget": budget(data)}
          return ledger(go)

      def run(raw):
          try:
              args = shlex.split(raw)
          except ValueError as err:
              return {"error": f"cannot split arguments: {err}"}
          if any(a.split("=")[0] in ("--settings", "--profile", "--stdin") for a in args):
              return {"error": "--settings, --profile and --stdin are not allowed"}
          # --json and --verbose are global flags, so they may come first.
          words = [a for a in args if a not in ("--json", "--verbose")]
          top = words[0] if words else ""
          sub = words[1] if top == "runs" and len(words) > 1 else ""
          if top in DENIED:
              return {"error": f"`snouty {top}` is not available to the audit"}
          if "-h" in words or "--help" in words:
              return execute(args)
          if top == "launch":
              return launch(args)
          if top == "debug":
              return create("debug", args + ([] if "--json" in args else ["--json"]))
          if sub in ("exec", "cancel"):
              return guarded(sub, args, words)
          return execute(args)

      print(json.dumps(run(inputs.get("args", ""))))
    # The tenant is a plain value, not the STAGING_ANTITHESIS_TENANT secret: gh-aw
    # redacts every secret's value from what the agent files, and the tenant's
    # name appears throughout the comments the agent quotes.
    env:
      SNOUTY: ${{ runner.temp }}/snouty/snouty
      AUDIT_LEDGER: ${{ runner.temp }}/nightly-audit/ledger.json
      AUDIT_RUN: ${{ github.run_id }}
      ANTITHESIS_TENANT: orbitinghail
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

  # Cancel every run the audit created: those in the ledger, and any launch
  # tagged with this workflow run, in case the tool timed out before it saw
  # the run's ID.
  teardown:
    needs: [agent]
    if: always() && needs.agent.result != 'skipped'
    runs-on: ubuntu-latest
    permissions: {}
    steps:
      - uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        with:
          name: snouty
          path: ${{ runner.temp }}/snouty

      - uses: actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c # v8.0.1
        continue-on-error: true
        with:
          name: nightly-audit-ledger
          path: ${{ runner.temp }}/nightly-audit

      - name: Cancel the audit's runs
        env:
          ANTITHESIS_TENANT: orbitinghail
          ANTITHESIS_API_KEY: ${{ secrets.STAGING_ANTITHESIS_API_KEY }}
          AUDIT_RUN: ${{ github.run_id }}
        run: |
          chmod +x "$RUNNER_TEMP/snouty/snouty"
          python3 - <<'EOF'
          import json, os, subprocess

          snouty = os.path.join(os.environ["RUNNER_TEMP"], "snouty", "snouty")
          path = os.path.join(os.environ["RUNNER_TEMP"], "nightly-audit", "ledger.json")
          ledger = json.load(open(path)) if os.path.exists(path) else {}
          ids = {r["id"] for r in ledger.get("runs", [])}
          listed = subprocess.run([snouty, "runs", "list", "--limit", "100", "--json"],
                                  capture_output=True, text=True, check=True)
          for line in listed.stdout.splitlines():
              run = json.loads(line)
              params = run.get("parameters") or {}
              if params.get("nightly_audit.workflow_run") == os.environ["AUDIT_RUN"]:
                  ids.add(run["run_id"])
          lines = ["## Runs the audit created", ""]
          for run_id in sorted(ids):
              p = subprocess.run([snouty, "runs", "cancel", run_id], capture_output=True, text=True)
              outcome = "cancelled" if p.returncode == 0 else (p.stderr.strip().splitlines() or ["?"])[0]
              lines.append(f"- `{run_id}`: {outcome}")
          if ledger.get("uncertain"):
              lines.append(f"- a {', '.join(ledger['uncertain'])} call timed out or printed no run ID; check the tenant")
          if not ids and not ledger.get("uncertain"):
              lines.append("None.")
          print("\n".join(lines))
          with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
              f.write("\n".join(lines) + "\n")
          EOF

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
      mkdir -p /tmp/gh-aw/agent/todo-audit "$RUNNER_TEMP/nightly-audit"
      python3 scripts/collect-todos.py > /tmp/gh-aw/agent/todo-audit/todos.json

  # The agent compares each comment's release with the tenant's, so a missing
  # release stops the run. Doctor exits non-zero for checks that do not matter
  # here, such as an unset container registry.
  - name: Read tenant release
    env:
      ANTITHESIS_TENANT: orbitinghail
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

# The repo's Claude Code settings hold contributor hooks: a Stop hook runs
# cargo, which the sandbox cannot install, and it keeps the agent from
# finishing.
pre-agent-steps:
  - name: Drop contributor Claude Code settings
    run: rm -f .claude/settings.json

# The teardown job cancels the runs in this ledger. Post-steps may not hold
# secrets, so this one only hands the ledger over.
post-steps:
  - name: Upload the run ledger
    if: always()
    uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
    with:
      name: nightly-audit-ledger
      path: ${{ runner.temp }}/nightly-audit/ledger.json
      if-no-files-found: ignore
      retention-days: 1

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

# Nightly Audit

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
  `snouty` MCP tool, passing everything after `snouty` as its `args` input.
  Prefer `--json`. `--verbose` logs each request and response to stderr. Pick
  a run to test against with `runs list --json`. Compare the release in the
  comment with the current release: a quirk that still reproduces on a later
  release still holds.
- **A TODO that waits on something** ("when a run records them", "once the API
  reports `in_progress`"): check whether that thing has happened, on the tenant
  or in the repository.
- **A TODO or FIXME with no external trigger**: check whether the code already
  does what it asks.

The `snouty` tool can also launch runs, open a debugging session and run
`runs exec`, within a nightly budget: 2 launches of at most 15 minutes, 1
debugging session and 30 exec calls. Spend it on comments that need it, such as
cancel semantics, launch responses, `runs exec` behavior and session readiness,
and plan the checks that share one session before you open it. A completed run
whose test name starts with `snouty` has an `antithesis.config_image` parameter
you can launch with. Everything the audit creates is cancelled when it ends, so
you do not need to clean up. When the budget is spent, record the rest as
unverifiable.

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
