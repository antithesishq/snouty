---
name: Open PR
description: Use this skill to turn the current branch into a pull request.
---

# Open PR

Push the current branch and open a pull request for it. The branch already
holds the finished commits; this skill only publishes them.

## 1. Check the branch

1. Confirm the working tree is clean: `git status --porcelain` returns empty.
   Commit or stash anything left over first.
2. Read what the PR will contain:
   `git fetch origin && git log --oneline origin/main..HEAD` and
   `git diff origin/main...HEAD --stat`.
3. Confirm the branch is not `main`.

## 2. Push the branch

```
git push -u origin <branch>
```

## 3. Write the body

Write the body to a file, `pr-body.md`, so the shell never mangles it.

Write prose that explains the change and why. Bullets are fine for listing
specific changes; section headers usually are not. Do not add a `## Summary`
header, a `## Test plan` section, or any other template boilerplate. Do not
list outstanding work, test checklists, or follow-up TODOs.

When the work closes an issue, put `fixes #<N>` on its own line, one line per
issue.

The title is one line in the same voice as the commit messages on the branch.

## 4. Create the PR

```
gh pr create --title "<title>" --body-file pr-body.md --base main
```

Some GitHub proxies reject GraphQL writes with HTTP 403, which fails
`gh pr create`. Create the PR over REST instead:

```
gh api -X POST repos/antithesishq/snouty/pulls \
  -f title="<title>" -f head=<branch> -f base=main \
  -F body=@pr-body.md --jq '.html_url'
```

Delete `pr-body.md` once the PR exists, and report the PR number and URL.

## 5. Watch it

A larger workflow watches the PR in its own step. When nothing else does —
the user asked only for a PR — watch it with the `watch-pr` skill.
