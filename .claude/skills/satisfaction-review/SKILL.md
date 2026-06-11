---
name: Satisfaction Review
description: This skill should be used when the user asks to "review snouty's output", "run a satisfaction review", "judge the gallery", "how good is snouty's CLI output", or wants an LLM-driven subjective review of snouty's human-facing command output. Reviews each gallery story for user satisfaction (correctness, follow-up readiness, terminal rendering) and writes a per-story + overall report.
---

# Satisfaction Review

This is an **LLM-run test of snouty's subjective, human-facing output**. The
automated checks in `scripts/gen-gallery.py` already prove each command is
*correct* (non-empty table, right status, clean error). Your job is different
and complementary: judge whether a real human, with the goal stated in each
story, would be **satisfied** with what snouty actually printed.

You review a "gallery": one Markdown file per story, each containing a user
goal, a judgment rubric, the exact command, the captured output, and the
automated PASS/FAIL verdict. It's produced by `scripts/gen-gallery.py` (see that
script's module docstring for how stories, checks, and discovery work).

Produce a report that is useful to **both a human and a coding agent** — someone
should be able to read it and either start a discussion about what to change or
immediately go fix the glaring issues.

## Step 1 — Obtain the gallery

The user either points you at a pre-generated gallery or asks you to make one.

**If the user provided a directory path** (in their message or as an argument),
and it exists and contains `*.md` files, use it directly. This is preferred —
generation is slow and needs live API credentials.

**Otherwise, generate one.** Generation hits the live Antithesis API and takes
several minutes (~10+). Run it into a known directory so you can review the
output regardless of exit code:

```bash
OUT=$(mktemp -d -t snouty-gallery.XXXXXX)
uv run scripts/gen-gallery.py --out "$OUT"
```

- Run this **in the background** (it can exceed the foreground timeout) and wait
  for it to finish.
- The script needs `ANTITHESIS_*` credentials, normally loaded from the repo's
  direnv `.envrc`. If it exits non-zero with an auth/credentials/DNS error and
  produces **zero** `.md` files in `$OUT`, do not guess — stop and ask the user
  to run `uv run scripts/gen-gallery.py` themselves and hand you the printed
  output directory.
- A non-zero exit with `.md` files still present means some stories *failed
  their automated check* but were still written. **Review them anyway** — a
  failed automated check is itself a strong satisfaction signal, and the partial
  gallery is exactly what you want to inspect.

Record the gallery directory and the number of stories (`ls "$OUT"/*.md | wc -l`)
for the report header.

## Step 2 — Read the stories

Each `*.md` file has this shape:

```
# <title>
**User goal:** <what the user is trying to accomplish>
**Judge satisfaction by:** <the story's own rubric>
```shell
$ snouty <args>
<captured output>
```
_Automated check: PASS|FAIL — <detail>_
```

Read every story file. For the **large** outputs (e.g. `runs-build-logs`,
`runs-logs-incomplete` can be tens/hundreds of KB) do **not** read the whole
file — measure its width with the tool below and read only the head and tail to
judge structure. For everything else, read the full file.

## Step 3 — Judge each story on three axes

For each story, rate these three axes and give a one-line rationale for each.
Use ✅ satisfied / ⚠️ partial / ❌ unsatisfied.

### Axis A — Correctness & usefulness
Did the command return the **right** and **useful** output for the stated goal?
Go beyond the automated check (which only proves coarse correctness). Ask: would
a human with this goal feel the question was actually answered? Is anything
misleading, redundant, mis-sorted, or missing context that the goal implies?
For error stories: is the message clear about *what went wrong* and ideally
*what to do next*, not just technically non-crashing?

### Axis B — Follow-up readiness
Does the output give the user what they need to run the **obvious next
command**? Concretely:
- `runs list` / `runs` → are full run IDs present and copyable (not truncated)
  so you can paste one into `runs show` / `runs properties`?
- `runs events` → are the `hash` and `vtime` shown so you can feed them into
  `runs logs`? Are sources/streams legible enough to build a `--source` /
  `--stream` filter?
- `runs properties` → are property names complete enough to pass to
  `runs property`?
- error / ambiguous / not-found → does it suggest valid alternatives or the
  correct form (e.g. the "did you mean one of:" list)?
If a needed coordinate is truncated, omitted, or buried, that's a follow-up
failure even if the command "worked".

### Axis C — Terminal rendering at ~100 columns
Assume an average terminal is **100 characters wide**. Measure the widest line
of actual command output per story:

```bash
for f in "$GALLERY"/*.md; do
  awk -v fn="$(basename "$f")" '
    /^```/        { infence = !infence; next }
    infence       { n++; if (length($0) > max) max = length($0) }
    END           { printf "%-34s width=%-4d lines=%d\n", fn, max+0, n+0 }
  ' "$f"
done | sort -t= -k2 -rn
```

Interpret the widest-line width:
- **≤ 100** → fits cleanly (✅).
- **101–120** → wraps a little; usually ⚠️ unless the wrap mangles a table.
- **> 120** → likely wraps badly; tables become unreadable, columns desync (❌).

Also judge rendering quality beyond raw width:
- **Wasted width**: mostly-empty columns padded to a huge fixed width (e.g. a
  `GROUP` column padded ~45 chars when most rows are blank), or full-precision
  floats (e.g. `vtime` printed as `18.310608921106905`) eating horizontal space.
- **Column alignment**: do columns line up, or does variable-width content
  (bracketed `[source:stream]` labels, signed hashes) push later columns out of
  alignment?
- **Truncation**: is `…`-truncation hiding information the user needs (Axis B
  overlap), or is it reasonable?
- **Density**: walls of near-identical lines, redundant repetition, no grouping.

### Per-story verdict
Combine the three axes into one verdict — **satisfied**, **partial**, or
**unsatisfied** — and, when not fully satisfied, a **concrete suggested fix**
(e.g. "round vtime to 3 decimals", "drop the GROUP column when empty",
"print full run ID, truncate the description instead").

## Step 4 — Write the report

Write `SATISFACTION-REVIEW.md` into the gallery directory, and also print the
Summary + Glaring Issues sections back to the user. Structure:

```markdown
# Snouty Satisfaction Review

Gallery: <dir> — <N> stories — generated <date/time if known>
Reviewed by: Claude (<model>), <date>

## Overall

<one paragraph verdict>. Satisfied: A/N · Partial: B/N · Unsatisfied: C/N.

## Glaring issues (prioritized, actionable)

1. **<short title>** — affects: `<slug>`, `<slug>`. <what's wrong, human impact>.
   _Suggested fix:_ <concrete change>.
2. ...

## Summary table

| Story (slug) | Correct | Follow-up | Render (width) | Verdict | Top issue |
|---|---|---|---|---|---|
| runs-list | ✅ | ⚠️ | ✅ (98) | partial | desc truncated with … |
| ... |

## Per-story detail

### <slug> — <verdict>
`$ snouty <args>`
- **Correct:** <✅/⚠️/❌> — <rationale>
- **Follow-up:** <✅/⚠️/❌> — <rationale>
- **Render:** <✅/⚠️/❌> (widest <N> cols) — <rationale>
- **Fix:** <concrete suggestion, or "none — satisfied">
```

Guidance for a high-signal report:
- Lead with **Overall** and **Glaring issues** — that's what a human skims and
  what an agent acts on. Order glaring issues by how much they hurt real use,
  and group the same root cause across stories into one entry (e.g. one
  "vtime precision" item, not one per story).
- Keep the per-story detail terse. Satisfied stories can be a single line; spend
  words on the partial/unsatisfied ones.
- Every non-satisfied story must carry a concrete, implementable fix — name the
  command, column, or format. Avoid vague advice like "improve formatting".
- Note where your subjective verdict **disagrees** with the automated check
  (e.g. automated PASS but you judge it partial/unsatisfied, or vice versa) —
  that gap is the most valuable output of this review.
- Be honest and specific. The point is to surface real problems, not to rubber-
  stamp. If output is genuinely good, say so plainly.

## Notes

- This review is **read-only** — it never edits snouty or the gallery; it only
  reads the gallery and writes the one report file.
- The gallery is pinned to whatever live run IDs were fresh at generation time,
  so exact IDs/values differ between runs. Judge the *shape and quality* of the
  output, not the specific data.
- Width and rendering are judged for a plain ~100-col terminal; snouty may
  detect a wider TTY when run interactively, but the gallery captures
  non-interactive output, which is the conservative case worth reviewing.
