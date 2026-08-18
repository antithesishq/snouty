---
name: Watch PRs
description: This skill should be used when the user asks to "watch the PR", "babysit PR #N", "monitor the pull request", or when another skill needs to track a PR until it closes. Arms a background poller on one or more PRs and reacts to each event — new comments, reviews, CI check results, close or merge.
---

# Watch PRs

Watch one or more pull requests with the bundled poller. React to each event
until every watched PR is closed.

## Arm the watcher

Run `watch-prs.sh` (next to this file) through the Monitor tool with
`persistent: true`:

```
command: .claude/skills/watch-pr/watch-prs.sh 123 456
description: events on PR #123, #456
```

The script prints one line per event and exits when every watched PR is
closed or merged. Flags:

- `-i seconds` — poll interval. The default is 45. Keep it at 30 or more to
  respect API rate limits.
- `-R owner/repo` — target repository. The default is the current
  directory's repository.
- `-u login` — suppress events from this login. Use it when you know the
  login your own comments post as; otherwise omit it and skip events that
  quote your own replies.

The script uses `gh`, so it needs the same `GH_HOST`/auth setup as any other
`gh` call in this environment.

On the first poll the script reports the current result of every completed
check once. Treat that first batch as a baseline, not as news.

## Event lines

```
PR #123 comment by <login>: <first 300 chars>
PR #123 review comment by <login> on <path>: <first 300 chars>
PR #123 review by <login>: <APPROVED|CHANGES_REQUESTED|COMMENTED> <body>
PR #123 check <name>: <pass|fail|skipping|cancel>
PR #123 merged
PR #123 closed without merge
```

## React to events

- **Review comment or review**: triage it. If it asks for a change, make the
  change, push, reply on the thread with the commit hash, and resolve the
  thread (the `resolveReviewThread` GraphQL mutation works). If the comment
  is unclear, ask on the thread instead of guessing.
- **Issue comment**: answer questions; treat change requests like review
  comments.
- **Check fail**: read the failure with `gh run view <run-id> --log-failed`,
  fix it, and push.
- **Merged or closed**: the script exits on its own. Stop any related work.

Ignore events you caused yourself: your own pushes re-run CI, and your own
replies appear as comments.
