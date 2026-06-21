# neither — not a config directory

This directory contains neither a `docker-compose.yaml` nor a `manifests/`
subdirectory, so `snouty validate` can't classify it as a Compose or Kubernetes
config. (This README is also what keeps the otherwise-empty directory tracked
by git.)

```sh
snouty validate tests/fixtures/validate/neither
```

Expected: exit 1 with

```
directory '…/neither' does not contain a docker-compose.yaml file or a manifests/ subdirectory
```
