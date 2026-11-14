# The generator and the page

## The generator

Write it in any language. Python with only the standard library is
sufficient. Split it into three parts:

- `gen.py`: reads git, renders blocks, writes the page.
- `content.py`: the steps, as data. Only prose and references to code.
- `page.css`, `page.js`, `review.css`, `review.js`: static parts, inlined
  into the page at build time.

Put its settings at the top: the repository path, the base and head refs,
the output path, the review namespace, and the API base path
(`/review-api/<ns>`). Print the output path and size at the end.

### Cut code from git

Read the changed files with `git diff --numstat -z -M <base> <head>`. A
renamed file gives both of its paths. Diff a renamed file with both paths:
git applies the pathspec before it finds renames, so with only the new path
the whole file shows as added.

Get each changed file as one hunk with
`git diff -M -U1000000 <base> <head> -- <old path> <path>`. Parse it into
rows of `(kind, old line, new line, text)`. A diff block names a file and a
range of new-side line numbers. A deleted row has no new number, so give it
the new number of the next kept row, or the last new number when no kept
row follows. Include it when that number is in the range:

```python
def changed_files():
    """{path at head: (path at base, added, deleted)}"""
    f, out, i = git("diff", "--numstat", "-z", "-M", BASE, HEAD).split("\0"), {}, 0
    while f[i]:
        added, deleted, path = f[i].split("\t")
        if path:
            out[path] = (path, added, deleted); i += 1
        else:  # a rename: the old and new paths follow
            out[f[i + 2]] = (f[i + 1], added, deleted); i += 3
    return out

FILES = changed_files()

def rows(path):
    out, old, new, in_hunk = [], 0, 0, False
    diff = git("diff", "-M", "-U1000000", BASE, HEAD, "--", FILES[path][0], path)
    for line in diff.split("\n"):
        if line.startswith("@@"):
            in_hunk, old, new = True, 1, 1
        elif not in_hunk or line.startswith("\\"):
            continue
        elif line.startswith("+"):
            out.append(("add", None, new, line[1:])); new += 1
        elif line.startswith("-"):
            out.append(("del", old, None, line[1:])); old += 1
        else:
            out.append(("ctx", old, new, line[1:])); old += 1; new += 1
    if out and out[-1] == ("ctx", old - 1, new - 1, ""):
        out.pop()  # the empty string after the last newline
    return out

def slice_rows(path, a, b):
    rs = rows(path)
    side = 2 if any(r[2] for r in rs) else 1  # a deleted file has only old numbers
    pos, nxt = [], max((r[side] for r in rs if r[side]), default=0)
    for r in reversed(rs):
        nxt = r[side] if r[side] is not None else nxt
        pos.append(nxt)
    pos.reverse()
    out = [r for r, p in zip(rs, pos) if a <= p <= b]
    assert out, (path, a, b)
    return out
```

For a first walkthrough, `BASE` is the merge base. For a follow-up, see
`references/follow-up.md`. For a new file, `-U1000000` gives one hunk of
added rows. A deleted file has no new-side numbers, so `slice_rows` takes
its range on the old side. Show a renamed file under its new path, and
name the old path in the caption.

Cut background snippets from `git show <head>:<path>`, lines `a` to `b`.
Assert that the range exists. These asserts fail the build when the branch
moves under a range. Then fix the range. Do not remove the assert.

Build the file tree from `FILES`. Assert that each file in scope appears in
at least one diff block.

### Content as data

`content.py` has `STEPS`, `FILES` (the file tree: directories and files with
depth) and `SKIPPED` (what the page does not cover). Each step is a dict
with `nav`, `title`, `lede` and `blocks`. Each block is a tuple:

| Block | Fields | Renders as |
|---|---|---|
| `p`, `h` | html | a paragraph, a subheading |
| `html` | html | free markup: lists, tables, before/after columns |
| `diff` | file, first line, last line, optional note | a diff figure |
| `bg` | title, html, list of snippets `(file, a, b)` or `("html", text)` | a closed background panel |
| `decision`, `edge`, `look` | html | a callout |
| `diagram` | none | the inline SVG |
| `out` | caption, text | terminal output, with the command and commit in the caption |

Write helpers for repeated markup, such as `table(head, rows)` and
`ul(*items)`, and `go(n, text)` for a link to another step.

## What each block shows

- **Prose** says what changed and why, in plain words, for this reviewer.
- **A diff** shows only the lines that matter, with a few lines of context.
  One function can take two or three diff blocks with prose between them.
- **A background panel** holds unchanged code that the reader needs to
  follow the diff: a caller, a type, a helper, the old behavior. It starts
  with a paragraph that says why it matters. Its summary line shows the
  `file:line` references. It is closed by default.
- **A callout** marks a decision or trade-off, an edge case, or a place the
  reviewer must look at closely. Say what the reviewer must decide or
  check.
- **A table** works well for case analysis: situation, behavior, the code
  that does it.
- **The diagram** shows the real mechanism: decisions, branches, where each
  path stops, and what is written where. Generate it as inline SVG from code.
  Use a `viewBox` and `width: 100%`, colors from CSS classes that use the
  page's tokens, and a `<title>`.

## The page

One HTML file. All CSS and JavaScript are inline. Load scripts only from
cdnjs.cloudflare.com, cdn.jsdelivr.net or unpkg.com, with an exact version,
and fonts only from Google Fonts. The page shares its origin with the review
API, so a changed CDN file could read the code and submit a review. Give
each external script an `integrity` hash and `crossorigin="anonymous"`.
Make the hash from the file that you check:

```sh
u=https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js
echo "sha384-$(curl -sf "$u" | openssl dgst -sha384 -binary | openssl base64 -A)"
```

### Layout

- **Header**: the PR name and a subtitle, the `base..head` short SHAs, a
  `Review` button with the count of pending comments, a theme button
  (system, light, dark), a progress bar, and an empty slot for the offline
  banner.
- **Sidebar**: the steps (number, nav title, comment count), the file tree
  (each file with its `+`/`−` counts and a chip for each step that diffs it;
  mark the files of the current step), a closed `Not covered` list, and
  short help on comments and keys. On a narrow screen the sidebar is a
  drawer that a header button opens.
- **Main**: one section for each step. Only the current one shows. A pager
  at the bottom has previous and next buttons with the step titles. A footer
  tells which commit the code comes from, which diff the hunks are, and
  which side the line numbers belong to.
- **Review drawer**: see `references/comments.md`.

### Markup contract

The comment code depends on these attributes. Keep them exact.

```html
<section class="step" id="step-3" data-step="3" hidden>
  <header class="step-head">… <p class="lede" data-pa="s3.lede">…</p></header>
  <p data-pa="s3.b0.0">…</p>
  <figure class="code diff" data-kind="diff" data-file="src/a.rs" data-step="3">
    <figcaption>src/a.rs:10–24 <span class="stat">+5 −2</span></figcaption>
    <table class="lines" data-lang="rust"><tbody>
      <tr class="del" data-o="12"><td class="ln">12</td><td class="ln"></td>
        <td class="sg">−</td><td class="cd">old text</td></tr>
      <tr class="add" data-n="12"><td class="ln"></td><td class="ln">12</td>
        <td class="sg">+</td><td class="cd">new text</td></tr>
    </tbody></table>
  </figure>
  <details class="bg"><summary>Background: … src/b.rs:40</summary>
    <div class="bg-body"><p data-pa="s3.b2.0">…</p>
      <figure class="code snip" data-kind="bg" data-file="src/b.rs">
        <table class="lines" data-lang="rust"><tbody>
          <tr class="ctx" data-l="40"><td class="ln">40</td><td class="cd">…</td></tr>
        </tbody></table></figure></div></details>
</section>
```

- A diff row has `data-o` (old number) when it has an old side, and `data-n`
  (new number) when it has a new side. A context row has both.
- A background row has `data-l`.
- The build gives each `p`, `li`, `h3`, table body row and callout body a
  prose anchor, `data-pa="s<step>.b<block>.<n>"`: the step number, the index
  of the block in the step, and the index of the element in the block. Prose
  in a background panel's snippet list adds `.h<k>`. The lede is
  `s<step>.lede`. Do it with a regular expression over the rendered markup
  of each block. The anchors stay the same when you rebuild the same content.
- Escape all code text. Never insert code as markup.

### Behavior

- `#step-N` in the URL selects a step, and the back button works.
- Left and right arrow keys change the step, except while the focus is in
  a text field. `b` opens or closes all background panels of the step.
- Highlight code with highlight.js (for example 11.9.0 from cdnjs, plus a
  language file for each language that the common build does not have).
  Highlight each `td.cd` cell on its own, with
  `hljs.highlight(text, {language, ignoreIllegals: true})`, so that rows
  stay rows. Highlight a step when it first shows, and a panel when it first
  opens. Leave unknown languages as plain text, and let them wrap.

### Style

- Define colors as tokens on `:root`. Define the dark values under
  `@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {…} }`
  and again under `:root[data-theme="dark"]`. Give `body` an explicit
  background. The theme button sets `data-theme` and keeps the choice in
  `localStorage`, inside `try`/`catch`.
- Added and deleted rows need enough contrast in both themes. So do the
  callout colors.
- At 900 px and below: the sidebar becomes a drawer, the side gutter is
  16 px, and code scrolls inside its figure. The page itself never scrolls
  sideways.
- Respect `prefers-reduced-motion`.
