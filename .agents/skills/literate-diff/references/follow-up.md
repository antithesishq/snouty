# Follow-up walkthroughs and explainer reports

## Follow-up walkthrough

After you address a review, show the reviewer only what changed since the
commit that they reviewed. Organize the page around their comments.

### The delta

The reviewed head is in the footer of the page they reviewed. Call it `R`.
The new head is `H`.

- When `R` is an ancestor of `H` and there is no merge between them
  (`git merge-base --is-ancestor R H`, and
  `git log --merges --oneline R..H` prints nothing), the delta is
  `git diff R H`.
- When the branch merged or rebased onto a newer base, a plain diff also
  shows the base's changes. Put the reviewed code on the new base first:

  ```sh
  B=$(git merge-base origin/<base> H)
  git merge-tree --write-tree R "$B" > /tmp/merge-tree.txt; echo "exit $?"
  T=$(head -n 1 /tmp/merge-tree.txt)
  git diff "$T" H
  ```

  `git merge-tree --write-tree` needs git 2.38 or later. Exit status 1
  means conflicts: the first line is still a tree, but the files that the
  rest of the output names hold conflict markers. Diff those files by hand,
  and say on the page that their delta can include base changes.

In the generator, use `R` (or `T`) as `BASE`, and assert the case that you
chose, so that a later rebuild fails when the branch changes shape. The left
line numbers are those of the reviewed code, and the right ones those of the
new head. Say so in the footer.

### The steps

1. **Overview.** The commits since `R`, each with what it does and which
   comments drove it. Then a table: comment number, the ask in short, the
   outcome, and the step. Mark each outcome with a pill: `as asked`,
   `decision for you` (the change reads the ask in one way, or departs from
   it), or `defect` (a problem you found while checking). Say how you
   checked the page: what you ran, and what you only read.
2. **One step for each comment**, or for a group of comments on one topic.
   Each step starts with the comment, quoted: its number, its location at
   `R` (step and `file:line`), the quoted code or prose, and the body. Then
   what changed and why, the delta hunks, terminal output from a run when
   it shows the new behavior, and background panels. Mark each snippet as
   new, partly changed, or unchanged since the review: compare its lines
   with the added rows of the delta. Use a `Decision for you` callout
   wherever the change interprets or departs from the ask, and a
   `Found while checking` callout for a defect.
3. **Changes from other reviewers and bots**, which the reviewer has not
   seen, each with its source and the reason.
4. **Checklist**, as in the first walkthrough.

A comment that you answered without a code change still gets a step, with
the answer.

### The new round

Use a new namespace and a new page directory, such as `<ns>-r2` and
`<name>-r2`. Change the API base and the `localStorage` key in the page.
Add the namespace to the server (see "Reuse what is there" in
`references/hosting.md`). The first review stays as it was. Test the page as
for the first one, then start the watcher on `<ns>-r2/SUBMITTED`.

## Explainer reports

When the reviewer asks for detail on one issue, write a short standalone
page in its own directory. It needs no comments and no server.

- **Where the code is**: the code, quoted from git with `file:line`, with the
  lines that matter highlighted.
- **Who reaches it**: the inputs and paths that lead to the code.
- **What the user sees**: a table with one row for each situation: the
  input, the behavior at each commit of interest, and the correct behavior.
  Color each cell: wrong, correct, or doubtful.
- **Impact**: who is affected, and how badly.
- **Fix direction**: the change that you suggest, and its trade-offs.
- **Method**: what you ran and how, and what you only read.

Run the code where you can: build the commit in a scratch worktree, and run
it with a throwaway home directory and mock servers. Put the real output in
the report. Use the same tokens, dark mode and phone width as the
walkthrough.
