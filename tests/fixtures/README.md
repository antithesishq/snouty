# Docs fixture

`docs.db` is generated from the markdown files in `docs-src/`.

Rebuild it with:

```sh
uv run scripts/build_docs_fixture.py
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

`snouty docs` reads its database from `<XDG_CACHE_HOME>/snouty/docs.db` (falling
back to `$HOME/.cache/snouty/docs.db`). To exercise the fixture without touching
the network, seed a throwaway cache home with it and run with `--offline`:

```sh
export XDG_CACHE_HOME=$(mktemp -d)
mkdir -p "$XDG_CACHE_HOME/snouty"
cp tests/fixtures/docs.db "$XDG_CACHE_HOME/snouty/docs.db"
snouty docs --offline search ...
```
