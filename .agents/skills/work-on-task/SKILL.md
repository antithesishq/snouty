---
name: Work On Task
description: Use this skill when the user asks for a change that ends in a pull request — "work on issue #N", "fix #N", a GitHub issue URL, or a plain description of the work such as "add a --json flag to snouty list".
---

# Work On Task

Take a task from its description to a merged PR. The task arrives in one of
two forms:

- **An issue reference**: an issue number, `#N`, or a GitHub issue URL.
- **A plain text prompt**: a description of the work to do.

Both forms follow the same steps. The only differences are how you read the
task in step 1 and whether the PR closes an issue in step 4.

## 1. Read the task and branch

For an issue:

1. Read the issue and its comments: `gh issue view <N> --comments`.
2. Fetch and branch: `git fetch origin && git switch -c issue-<N>-<slug> origin/main`.

For a plain text prompt:

1. The prompt is the specification. Read the code it names to learn the
   current behavior.
2. Fetch and branch: `git fetch origin && git switch -c <slug> origin/main`,
   where `<slug>` is two to four words that name the change.

In both cases, restate the acceptance criteria in one or two sentences
before you write code. If the task is ambiguous, ask the user before you
implement.

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
3. **Comments**: launch a subagent with the prompt "Invoke the
   simplify-comments skill on the current branch's diff against origin/main
   and report the findings." Fix the real findings yourself and commit.
4. Re-run `cargo nextest run`, `cargo clippy`, `cargo fmt`, and commit any
   remaining changes.

## 4. Submit the PR

Invoke the `open-pr` skill. It pushes the branch, writes the body, and
creates the PR. When the task came from an issue, tell it the issue number so
the body carries `fixes #<N>` and the merge closes the issue.

## 5. Watch until close

Invoke the `watch-pr` skill with the new PR number. It runs the watcher and
tells you how to react to each event.
