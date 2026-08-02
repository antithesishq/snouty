# snouty cookbook

Composable patterns for scripting against snouty. Each recipe is the smallest
sketch that gets the job done — no error handling, no defensive checks. Fill in
the details your script actually needs.

Recipes read snouty's `--json` output, which is the stable surface for
automation. Human output is for humans and may be reformatted at any time.

## Recipes

- [Stream a run's logs at its failure moment](#stream-a-runs-logs-at-its-failure-moment)

## Stream a run's logs at its failure moment

*snouty 0.6.1 · 2026-07-29 · source: [#179](https://github.com/antithesishq/snouty/pull/179)*

`runs show --json` carries the run's failure moment; `runs logs` streams a
timeline up to a moment. Compose the two instead of copying the moment by hand.

```sh
run=<run id>

snouty runs show "$run" --json \
  | jq -r '.failure_moment | "\(.input_hash) \(.vtime)"' \
  | xargs snouty runs logs "$run"
```

The moment is `{"input_hash": ..., "vtime": ...}` and feeds `runs logs` in that
order. The `vtime` is a JSON number carrying the moment's exact value: pass it
through unchanged, and compare it numerically — never as text, where
`"1000.0" < "9.0"`.

Not every run has one. The `failure_moment` key is absent when the run has no
moment-pinned failure, and a run that timed out or was killed reports the
placeholder `{"input_hash": "0", "vtime": 0}` — which streams nothing.
`snouty runs show` treats that placeholder as "no moment"; do the same with a
numeric check (`.vtime != 0`), and skip the log fetch rather than streaming an
empty timeline.
