# snouty docs

Search and inspect cached Antithesis documentation from the command line.

## User Story

As a developer, I want to search and read Antithesis documentation from the
command line so that I can quickly find relevant docs without leaving my
terminal.

## Behavior

1. The command is exposed as `snouty docs` with `search`, `show`, and `sqlite`
   subcommands.
2. Unless `--offline` is passed to `snouty docs`, the command checks for an
   updated documentation database before running the requested subcommand.
3. Documentation updates are downloaded from `https://antithesis.com/docs/sqlite.db`
   by default. `ANTITHESIS_DOCS_URL` overrides the base URL and
   `ANTITHESIS_DOCS_DB_PATH` overrides the local database path. When
   `ANTITHESIS_DOCS_DB_PATH` is set, Snouty treats docs access as offline and
   does not attempt to update the database first.
4. Cached documentation is stored at `docs.db` under Snouty's cache directory,
   along with an ETag file used to avoid re-downloading an unchanged database.
5. If a docs update fails and a cached database already exists, the command
   prints a warning to stderr and continues with the cached database. If no
   database exists yet, the command fails.
6. All docs reads open the SQLite database read-only. If the database is
   missing, `search` and `show` fail with guidance to run a docs command
   without `--offline`.
7. `snouty docs search` requires at least one query term. Multiple positional
   terms are joined with spaces into a single full-text search query.
8. `snouty docs search` supports `--format plain|json` and `--limit <n>`, with
   defaults of `plain` and `10`.
9. `snouty docs search` uses full-text search over the documentation database
   and ranks title matches above body-only matches when the query is simple
   enough to support title boosting.
10. When `search` finds matches in plain format, it prints one result at a time
    with the page path, the page title, and a wrapped snippet containing the
    matched terms.
11. When `search` finds matches in JSON format, it prints a JSON array. Each
    result object includes `path`, `title`, and `snippet`.
12. When `search` finds no matches, it exits successfully and prints a "No
    results found" message to stderr.
13. `snouty docs show <path>` prints the full page as markdown, prefixed by a
    level-1 heading containing the page title.
14. `show` normalizes the requested path by trimming leading and trailing `/`
    characters and removing an optional leading `docs/` prefix before looking up
    the page.
15. If `show` cannot find an exact page, it fails with the normalized `docs/...`
    path in the error message and includes up to 10 similar page-path
    suggestions when available.
16. `snouty docs sqlite` prints the path to the cached SQLite database when it
    exists.
17. If `sqlite` is run before the documentation database has been downloaded, it
    exits successfully and prints guidance to stderr explaining how to download
    the docs database.
