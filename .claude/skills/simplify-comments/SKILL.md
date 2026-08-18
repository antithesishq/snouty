---
name: Simplify Comments
description: This skill should be used before a PR is submitted, or when the user asks to "fix the comments", "simplify comments", or "audit comments". Brings every comment touched by the current branch up to the project's comment bar — sparse, true, minimal; no narration, no discarded alternatives.
---

# Simplify Comments

Bring every comment the current branch touches up to the project's comment
bar. Scope: comments added or changed in `git diff origin/main...HEAD`, plus
comments made stale by code the branch changed. Do not sweep the rest of the
repository.

## Sources of truth

1. AGENTS.md, section "Comments, changelog, and prose".
2. Your persistent memory. Check it for comment-related feedback. Newer
   feedback wins over this skill.

## The bar

A comment earns its place only when it states something the code cannot show:

- a precondition or an invariant
- a non-obvious reason for a decision that is visible in the code
- a TODO with a concrete trigger ("TODO: remove when X ships")
- an observed server response — endpoint, tenant, shape — that justifies the
  handling (see AGENTS.md, "The server boundary")

Delete everything else. In particular delete:

- narration of what the next line does
- narration of how the change was investigated (that belongs in the PR body)
- discarded alternatives and alternate realities ("instead of X",
  "a Y would have ...")
- descriptions of another module's behavior
- comments that argue with a reviewer or justify the change to them
- restatements of a name, a type, or a signature

The same bar applies to doc comments. A doc comment on a public item states
the contract. It does not narrate the implementation.

## Procedure

1. List every comment line in `git diff origin/main...HEAD`.
2. Judge each line against the bar. Default to delete. Keep a comment only
   with a reason.
3. Rewrite the survivors: one short sentence where possible, ASD-STE100
   (active voice, present tense, one idea per sentence). Each survivor must
   still be true after the change merges.
4. Run `cargo fmt` when a Rust file changed.
