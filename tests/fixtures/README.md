# Docs fixture

`docs.db` is generated from the markdown files in `docs-src/`.

Rebuild it with:

```sh
./tests/fixtures/build_docs_fixture.py
```

The generator creates the minimal schema expected by `snouty docs`:

- `pages(path, title, content)`
- `pages_fts` as an FTS5 index over `title` and `content`
- an insert trigger that keeps `pages_fts` in sync

Each markdown file becomes one page:

- the relative path under `docs-src/` becomes `/docs/<path-without-.md>/`
- the first `# Heading` becomes the page title
- the remaining markdown becomes the page content

# Manually testing the docs fixture

You can point Snouty at the docs fixture to manually test it by exporting the environment variable `ANTITHESIS_DOCS_DB_PATH`.

```
export ANTITHESIS_DOCS_DB_PATH=$PWD/tests/fixtures/docs.db
snouty docs search ...
```
