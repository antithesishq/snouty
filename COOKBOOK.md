# snouty cookbook

Composable patterns for scripting against snouty. Each recipe is the smallest
sketch that gets the job done — no error handling, no defensive checks. Fill in
the details your script actually needs.

Recipes read snouty's `--json` output, which is the stable surface for
automation. Human output is for humans and may be reformatted at any time.

## Recipes

- [Stream a run's logs at its failure moment](#stream-a-runs-logs-at-its-failure-moment)
- [Copy private images into your repository](#copy-private-images-into-your-repository)

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

## Copy private images into your repository

*snouty 0.7.2 · 2026-09-23 · source: [#298](https://github.com/antithesishq/snouty/issues/298)*

If your compose file uses images from a private registry, the test run can't
pull them, because it doesn't have your credentials. snouty won't catch this at
launch: it checks each image with your local container engine, which is logged
in as you, so the private image looks fine. The run then fails when Antithesis
tries to pull it.

The fix is to copy each private image into your Antithesis repository and point
the compose file at the copy:

```sh
repo=$(snouty doctor --offline --json | jq -r .settings.repository)

for image in ghcr.io/your-org/app:v1 ghcr.io/your-org/worker:v1; do
  host=$(echo "${image%%/*}" | sed 's/:/__/')
  copy="$repo/snouty-mirror/$host/${image#*/}"
  docker pull --platform linux/amd64 "$image"
  docker tag "$image" "$copy"
  docker push "$copy"
  echo "image: $copy"
done
```

Replace each private image in `docker-compose.yaml` with the `image:` line the
script prints, then launch as usual. snouty sees that the copy is already in
your repository and uses it without pushing it again. Rerun the script whenever
a private image changes.

The copy keeps the registry host in its path, so `ghcr.io/your-org/app:v1`
becomes `$repo/snouty-mirror/ghcr.io/your-org/app:v1`. This is the same path
snouty uses when it pushes an image itself, and it keeps images from two
registries apart. A port's `:` becomes `__`, because a path can't contain a
colon. The script assumes every image names its registry host.

Docker and podman keep separate image stores, and snouty picks podman when both
are installed. Either launch with `SNOUTY_CONTAINER_ENGINE=docker`, or swap
`docker` for `podman` in the script.
