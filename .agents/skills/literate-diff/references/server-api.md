# The review server

One file, Python standard library only (`http.server.ThreadingHTTPServer`,
`json`, `tempfile`, `threading`). About 400 lines. Keep it outside any
repository, such as `~/review-comments/server.py`, so that all reviews on
the machine share it.

## Command line

```text
server.py --root DIR --port PORT --ns NAME=TITLE [--ns NAME=TITLE ...]
          [--static DIR]
```

- `--root`: the data directory. Each namespace keeps its data in
  `<root>/<NAME>/`.
- `--ns`: a namespace, one for each review round, such as
  `myrepo-pr42=myrepo PR #42` and `myrepo-pr42-r2=myrepo PR #42, follow-up`.
  A name matches `^[A-Za-z0-9_-]{1,64}$`, and is not `health` or
  `review-api`. The title goes in the Markdown heading.
- `--static`: also serve the files in this directory, so that one local
  server serves the page and the API (see `references/hosting.md`).
- Bind to `127.0.0.1`.

At start, create `reviews/` and `submitted/` and an empty `SUBMITTED` file
for each namespace. Existing data stays.

## Routing

Remove a leading `/review-api` from the path, if it is there. A reverse
proxy can remove it first; both work. Then the first path segment is the
namespace. An unknown namespace is a 404. With `--static`, a `GET` that is
not an API path serves a file from the static directory: `index.html` for a
directory, a 404 for a path that resolves outside the directory.

## Endpoints

All bodies are JSON. Each response has `Content-Type: application/json` and
`Cache-Control: no-store`. An error is `{"error": "<message>"}`.

| Method and path | Request | Response |
|---|---|---|
| `GET /health` | | `200 {"ok": true, "namespaces": [...]}` |
| `GET /<ns>/review` | | `200 {"review": <review>}`, the pending review; with none, `{"id": null, "state": "pending", "comments": [], "comment_count": 0}` |
| `POST /<ns>/comments` | `{"anchor", "body"}` | `201 {"comment", "review_id"}`; the server makes the ID |
| `PUT /<ns>/comments/<id>` | `{"body", "anchor"?}` | `201` when new (needs `anchor`), `200` when updated; `{"comment", "review_id"}` |
| `DELETE /<ns>/comments/<id>` | | `200 {"deleted": "<id>"}`; `404` when no pending comment has the ID |
| `POST /<ns>/review/submit` | `{"verdict", "summary", "review_id"?}` | `200 {"review": <review>}` |
| `GET /<ns>/reviews` | | `200 {"reviews": [...]}`, newest first, without comments |
| `GET /<ns>/reviews/<id>` | | `200 {"review": <review>}` with comments; `404` when unknown |

Rules:

- At most one review is pending. The first comment creates it. After a
  submit, the next comment starts a new pending review in the same
  namespace, and the reviewer can submit it too.
- A comment or review ID matches `^[A-Za-z0-9_-]{1,64}$`. A new review ID is
  the UTC time and 6 hex characters: `20260101T120000Z-a1b2c3`.
- A `PUT` or `DELETE` of a comment in a submitted review is `409`.
- `verdict` is `comment`, `request_changes` or `approve`. When
  `review_id` is given and is not the pending review, the answer is `409`.
- A submit with no pending review is allowed only with a summary or the
  `approve` verdict. `request_changes` needs a summary or a comment.
- Validation failures are `400`. A body over 1 MiB is `413`. Any other
  exception is logged and answered `500`, and the server continues.
- One lock covers each request from reading files to writing them.

## Records

```json
{
  "id": "20260101T120000Z-a1b2c3",
  "state": "pending",
  "verdict": null,
  "summary": "",
  "created_at": "2026-01-01T12:00:00Z",
  "updated_at": "2026-01-01T12:00:00Z",
  "submitted_at": null,
  "markdown": null,
  "comment_count": 1,
  "comments": [
    {
      "id": "c0123456789abcdef",
      "status": "pending",
      "body": "Rename this.",
      "created_at": "2026-01-01T12:00:00Z",
      "updated_at": "2026-01-01T12:00:00Z",
      "anchor": {
        "kind": "diff", "id": "s4:diff:src/a.rs:R348:R352",
        "step": 4, "step_title": "The check", "pos": 17,
        "file": "src/a.rs", "line_start": 348, "line_end": 352,
        "side": "R", "start_side": "R",
        "quote": "+    let api = build(settings)?;"
      }
    }
  ]
}
```

Validate the anchor field by field: `kind` is `diff`, `bg` or `prose`; `id`
is not blank and at most 400 characters; `step` and the line numbers are
integers from 0 to 10,000,000; `file` is required for `diff` and `bg`;
`side` and `start_side` are `L` or `R` for `diff`; `para` is required for
`prose`. Swap `line_start` and `line_end` when they are reversed, but for
`diff` only when `start_side` equals `side`: numbers on two sides cannot be
compared. Cut `quote` to 8000 characters. `body` is not blank and at most
20,000 characters. `summary` is at most 50,000 characters. Keep only the
known fields.

## Storage

```text
<root>/<ns>/reviews/<review id>.json      one file for each review
<root>/<ns>/submitted/<UTC time>-<last 6 of review id>.md
<root>/<ns>/SUBMITTED                     one line for each submitted review
```

Write each file atomically: write a temporary file in the same directory,
`fsync` it, then `os.replace` it onto the name.

On submit, in this order: set the state, verdict, summary and times, and the
status of each comment to `submitted`; write the Markdown file; save the
review JSON with the Markdown path in `markdown`; then append
`<review id> <absolute markdown path>` and a newline to `SUBMITTED`, and
`fsync` it. A watcher reads `SUBMITTED`, so the Markdown file must exist
before its line does.

## The Markdown file

Sort the comments by step, then `pos`, then creation time. Put each quote
in a code fence longer than any run of backticks in it. Use `diff` as the
fence language for a diff quote, and the file's language for a background
quote. Quote prose with `>`.

````markdown
# myrepo PR #42: review 20260101T120000Z-a1b2c3

- Verdict: **Request changes**
- Submitted: 2026-01-01T12:30:00Z
- Comments: 2

## Summary

_(no summary)_

## Comments

### 1. Step 4 (The check), `src/a.rs:348-352`, in the diff

```diff
+    let api = build(settings)?;
```

Rename this.

### 2. Step 5 (Errors), prose paragraph `s5.b3.0`

> The error names the tenant.

Say what the user can do instead.
````

A deleted-side location adds ` (old side)`. A range whose two ends are on
different sides names each side, such as `src/a.rs:L100-R95`. A background
location ends with `, in a background panel`.
