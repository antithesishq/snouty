# The comment UI

The comments work like a pending review on GitHub. The reviewer adds
comments to lines and paragraphs. They stay pending, and the server keeps
them, until the reviewer selects `Finish review` and submits. The client
talks to the server described in `references/server-api.md`, at the API
base `/review-api/<ns>`, on the same origin as the page.

## Adding a comment

- **On code**: each diff row and background row has a `+` button in its
  gutter. It shows on hover, and it is always faintly visible on touch
  screens. A click opens a form below the row.
- **On a range**: drag the `+` down or up across rows of one table, or
  click one `+` and shift-click another in the same table. Highlight the
  selected rows. The form opens below the last row.
- **On prose**: each `[data-pa]` element has a `+` in its left margin, or at
  its right edge on a narrow screen.
- **The form**: a text area, `Cancel`, and `Add review comment`. Ctrl+Enter
  or Cmd+Enter adds; Esc cancels. The add button is off while the text is
  blank.

## Showing comments

- Show each pending comment as a card below its anchor: below the last row
  of its range, or below its paragraph. Mark the anchored rows.
- A card shows the body as plain text (`textContent`, `white-space:
  pre-wrap`), the time, `Edit` and `Delete`. `Edit` swaps in the form with
  the old text. `Delete` asks for confirmation.
- The step nav shows a count for each step. The `Review` button shows the
  total.
- Do not render again while a form is open: wait until it closes. A render
  must never lose text that the reviewer is writing.

## The review drawer

The `Review` button opens a drawer:

- **Pending comments**, sorted by step and then by position on the page.
  Each entry shows the location (`src/a.rs:R10–R14`, `(background)`, or
  `paragraph`), the first line of the quote, and the start of the body. A
  click goes to the step, opens a closed panel, scrolls to the anchor, and
  flashes it. An anchor that the page no longer has still lists, with its
  quote, marked as not on this page.
- **`Finish review`** opens a summary text area and three verdicts:
  `Comment`, `Request changes` and `Approve`, each with a one-line
  explanation. `Submit review` sends them. On success, clear the pending
  list, show the review ID, and refresh past reviews. On an error, show the
  server's message in the form.
- **Past reviews**: the submitted reviews, newest first, with verdict, time
  and comment count.

## Anchors

The client builds the anchor of a comment from the markup that the build
wrote (see "Markup contract" in `references/page.md`):

| Field | Value |
|---|---|
| `kind` | `diff`, `bg` or `prose` |
| `id` | see below |
| `step`, `step_title` | the step number and its nav title |
| `pos` | the index of the anchor element among the step's rows and `[data-pa]` elements, for sorting |
| `quote` | diff: the rows, each with its `+`, `-` or space; bg: the rows; prose: the element's text, at most 1500 characters |
| `file`, `line_start`, `line_end` | diff and bg only |
| `side`, `start_side` | diff only: `R` for a row with `data-n` (new side), `L` for a deleted row (`data-o`) |
| `para` | prose only: the `data-pa` value |

The IDs:

- diff: `s<step>:diff:<file>:<start side><start>:<end side><end>`, such as
  `s4:diff:src/login.rs:R348:R352`
- bg: `s<step>:bg:<file>:<start>:<end>`, such as `s3:bg:src/api.rs:2068:2086`
- prose: `s<step>:prose:<para>`, such as `s2:prose:s2.b7.0`

To place a comment after a load, find the section of its step, then the
figure with its `data-kind` and `data-file`, then the row with
`data-n`/`data-o`/`data-l` equal to `line_end`. For prose, find the
`[data-pa]` element. Strip the comment UI's own elements before you read
an element's text for a quote.

## Sync and the offline mode

- The client makes each comment ID itself: `c` and 16 hex characters from
  `crypto.getRandomValues`. It saves with `PUT /comments/<id>`, which can be
  sent again safely. It deletes with `DELETE /comments/<id>`.
- Keep the comments and a queue of operations (`put` or `del`, by ID) in
  `localStorage`, under a key for the namespace. Apply each change to the
  page at once, add it to the queue, and then sync.
- To sync, send the queued operations in order, then load
  `GET /review`. The server's list replaces the local one.
- A network error, an HTTP 502, 503 or 504, or a body that is not JSON
  means that the server is unreachable. Keep the queue, show a banner (the
  server is unreachable; comments stay in this browser and save when it is
  back), and try again every 10 seconds.
- A 400, 404 or 409 is final. Drop the operation and show a short message.
- Sync every 30 seconds while the page is open, so that two tabs agree.
- Wrap each `localStorage` call in `try`/`catch`. The page must work
  without it.
