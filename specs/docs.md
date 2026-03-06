# snouty docs

Search and inspect cached Antithesis documentation from the command line.

## User Story

As a developer, I want to search and read Antithesis documentation from the
command line so that I can quickly find relevant docs without leaving my
terminal.

## Shared Behavior

1. The command is exposed as `snouty docs` with `search`, `show`, `tree`, and `sqlite`
   subcommands.
2. Unless `--offline` is passed to `snouty docs`, the command checks for an
   updated documentation database before running the requested subcommand.
3. Documentation updates are downloaded from
   `https://antithesis.com/docs/sqlite.db` by default. `ANTITHESIS_DOCS_URL`
   overrides the base URL and `ANTITHESIS_DOCS_DB_PATH` overrides the local
   database path. When `ANTITHESIS_DOCS_DB_PATH` is set, Snouty treats docs
   access as offline and does not attempt to update the database first. The
   downloaded database is marked readonly via filesystem permissions to prevent
   unintentional modifications.
4. Documentation update requests send a `User-Agent` header in the form
   `snouty/<version> (<os>; <arch>; rust<rust-version>)` where os and arch
   reflect the system which compiled Snouty.
5. Cached documentation is stored at `docs.db` under Snouty's cache directory,
   along with an ETag file used to avoid re-downloading an unchanged database.
6. If a docs update fails and a cached database already exists, the command
   prints a warning to stderr and continues with the cached database. If no
   database exists yet, the command fails.
7. After any automatic update attempt, if the documentation database is still
   missing then `search`, `show`, and `sqlite` fail with guidance tailored to
   the reason updates were skipped: remove `--offline`, or point
   `ANTITHESIS_DOCS_DB_PATH` at an existing file.

## `snouty docs search`

1. `snouty docs search` requires at least one query term. Multiple positional
   terms are joined with spaces into a single full-text search query.
2. `snouty docs search` supports `--json`/`-j`, `--limit`/`-n <n>`, and `--list`/`-l`,
   with default plain-text output and a default limit of `10`.
3. `snouty docs search` uses full-text search over the documentation database
   and ranks title matches above body-only matches when the query is simple
   enough to support title boosting. For simple conversational queries, filler
   words such as `what` and `is` do not outweigh the content-bearing terms when
   ranking search results.
4. When `search` finds matches in plain format, it prints one result at a time
   with the page path, the page title, and a wrapped snippet containing the
   matched terms.
5. When `search` runs with `--json`/`-j`, stdout is always JSON. If `--json` is
   present, any non-JSON stdout is a bug.
6. In JSON mode, `search` prints a JSON array. Each result object includes
   `path`, `title`, and `snippet`, unless another option narrows the JSON value
   to a different JSON shape.
7. When `search` runs with `--list`/`-l` and `--json`/`-j` is not present, it
   returns only the matching page paths.
8. When `search` finds no matches and `--json`/`-j` is present, it exits
   successfully and prints an empty JSON array to stdout.
9. When `search` finds no matches and `--json`/`-j` is not present, it exits
   successfully and prints a "No results found" message to stderr.

## `snouty docs show`

1. `snouty docs show <path>` prints the full page as markdown, prefixed by a
   level-1 heading containing the page title.
2. `show` normalizes the requested path by trimming leading and trailing `/`
   characters and removing an optional leading `docs/` prefix before looking up
   the page.
3. If `show` cannot find an exact page, it fails with the normalized `docs/...`
   path in the error message and includes up to 10 similar page-path
   suggestions when available.

## `snouty docs tree`

1. `snouty docs tree [filter]` reads all stored documentation page paths and
   prints a directory-like tree derived from those paths.
2. The command renders the tree with high-quality Unicode tree-drawing
   characters via a dedicated tree-rendering library rather than ad hoc ASCII
   formatting.
3. The printed tree omits the synthetic `docs` root. Top-level documentation
   sections appear as the root nodes in the output.
4. `snouty docs tree` supports `--depth <n>`. When `--depth` is omitted, the
   command prints the full tree. When provided, it limits output to nodes at
   depth `n` or shallower, where top-level sections are depth 1.
5. `snouty docs tree [filter]` accepts an optional positional filter string.
   When present, the command includes any page whose normalized path or page
   title contains the filter string using case-insensitive substring matching,
   and includes the ancestor nodes required to place matching pages in the
   tree.
6. Tree output is page-path based: it does not inspect markdown headings or
   infer an intra-page outline.
7. Leaf nodes show both the page path segment and the page title. Internal
   nodes that correspond to real pages also show both the segment and the page
   title. Internal nodes that correspond only to path groupings are shown
   without synthesized titles.
8. When a path segment is both a page and a parent of other pages, the node is
   rendered once using the path segment and page title, with child pages nested
   beneath it.
9. If the filter matches no pages, the command exits successfully and prints a
   "No results found" message to stderr.

## `snouty docs sqlite`

1. `snouty docs sqlite` prints the path to the cached SQLite database for
   direct usage by consumers.
