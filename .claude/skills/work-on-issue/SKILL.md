---
name: Work On Issue
description: This skill should be used when the user says "work on issue #N", "fix #N", "take issue N", or gives a GitHub issue URL. End-to-end workflow - branch from origin/main, implement, run the simplify and code-review skills in subagents, fix comments with the simplify-comments skill, open the PR, and watch it until it closes.
---

# Work On Issue

Take a GitHub issue from number to merged PR.

## 1. Read the issue and branch

1. Read the issue and its comments: `gh issue view <N> --comments`.
2. Fetch and branch: `git fetch origin && git switch -c issue-<N>-<slug> origin/main`.
3. Restate the acceptance criteria in one or two sentences before you write
   code. If the issue is ambiguous, ask on the issue before you implement.

## 2. Implement

Follow AGENTS.md, section "AI Coding Workflow": specs for new subcommands or
flags, tests for all user-visible changes, gallery updates, targeted mutation
testing, then `cargo nextest run`, `cargo clippy`, `cargo fmt`. Commit as you
go.

## 3. Pre-PR gauntlet

Run these steps in order. Each step may change the tree, so commit after each
one so the next step reviews a clean diff.

1. **Simplify**: launch a subagent (Agent tool, general-purpose) with the
   prompt "Invoke the simplify skill on the current branch's diff against
   origin/main and apply the fixes." Review and commit what it changed.
2. **Code review**: launch a subagent with the prompt "Invoke the code-review
   skill on the current branch's diff against origin/main and report the
   findings." Fix the real findings yourself and commit.
3. **Comments**: invoke the simplify-comments skill. It checks the comment
   feedback in your memory and in AGENTS.md, and fixes every comment in the
   branch diff that does not match.
4. Re-run `cargo nextest run`, `cargo clippy`, `cargo fmt`, and commit any
   remaining changes.

## 4. Submit the PR

1. Push the branch: `git push -u origin <branch>`.
2. Write the PR body as prose that explains the change and why, per the
   user's PR style. Put `fixes #<N>` on its own line so the merge closes the
   issue.
3. Create the PR with that body. If `gh pr create` fails with HTTP 403 (a
   proxy that blocks GraphQL writes), create it over REST:

   ```sh
   gh api -X POST repos/OWNER/REPO/pulls \
     -f title="..." -f head=BRANCH -f base=main -F body=@body.md --jq '.html_url'
   ```

## 5. Watch until close

Run the PR watcher in the background — through the Monitor tool with
`persistent: true` when the harness has it, otherwise through a background
Bash task:

```
uv run scripts/watch-prs.py <PR>
```

The script prints one line per event and exits when every watched PR is
merged or closed. On the first poll it reports every already-completed check
once; treat that batch as a baseline, not as news. Flags: `-i seconds` sets
the poll interval (default 45), `-u login` suppresses events from a login,
`-R owner/repo` overrides the repository. Event lines:

```
PR #123 comment by <login>: <first 300 chars>
PR #123 review comment by <login> on <path>: <first 300 chars>
PR #123 review by <login>: <APPROVED|CHANGES_REQUESTED|COMMENTED> <body>
PR #123 check <name>: <success|failure|skipped|cancelled>
PR #123 merged
PR #123 closed without merge
```

React to each event:

- **Review comment or review**: triage it. If it asks for a change, make the
  change, push, reply on the thread with the commit hash, and resolve the
  thread (the `resolveReviewThread` GraphQL mutation works). If the comment
  is unclear, ask on the thread instead of guessing.
- **Issue comment**: answer questions; treat change requests like review
  comments.
- **Check failure**: read the failing log with
  `gh run view <run-id> --log-failed`, fix it, and push.
- **Merged or closed**: the script exits on its own. Stop related work.

Ignore events you caused yourself: your own pushes re-run CI, and your own
replies appear as comments.
