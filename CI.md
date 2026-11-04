# Running snouty in CI

`snouty launch` runs in any CI system. In every CI system it records the checked-out commit in run metadata. It also records the branch, when HEAD is on a branch, and the repository and a commit link, when the `origin` remote is on github.com. In GitHub Actions, it records more: see [GitHub Actions](#github-actions).

Each CI job needs these environment variables:

- `ANTITHESIS_TENANT`: your tenant.
- `ANTITHESIS_REPOSITORY`: the container registry for your images, with `--config`. Log in to it in the job before the launch. On the tenant registry, `registry.<tenant>.antithesis.com`, snouty pushes with its own credential instead. That needs Docker 28.3.0 or later, or the [Docker credential helper](README.md#docker-credential-helper). The tenant registry must be enabled on your tenant: contact Antithesis support to request it.
- `ANTITHESIS_API_KEY`: an API key, stored as a secret. GitHub Actions can use [OIDC](#github-actions-oidc) instead.

snouty does not choose a `--source` for you. A run without `--source` is ephemeral: its results do not go into the property history of any source. To record history for a branch, pass `--source` only in the jobs that build that branch, such as pushes to `main` and nightly schedules. Do not pass a pull request's branch name as the source: a pull request from a fork can use any branch name, such as `main`.

`--config` builds the config image and pushes the images, so the job needs Docker or Podman. It does not build or pull the service images: run `docker compose build` and `docker compose pull --ignore-buildable` first.

## GitHub Actions

`snouty launch` can replace the [Antithesis Trigger Action](https://github.com/antithesishq/antithesis-trigger-action). In GitHub Actions, it records the same metadata as the action: the repository, the branch, the commit, the pull request (`vcs.pr_id`, `vcs.pr_link`, `vcs.pr_owner` and `vcs.pr_title`), the user who started the workflow (`run.creator_name`), and the cron schedule of a scheduled workflow (`run.cron_schedule`). It records `run.caller_type` as `snouty`. On a pull request, it records the head commit of the pull request, not the merge commit that the workflow checks out. When the job checks out a different commit, for example another ref or another repository, the launch reads the git checkout instead.

This workflow starts a run on the default branch every 12 hours, and on a pull request when someone adds the `antithesis` label. Only the scheduled runs record property history. A pull request from a fork gets no secrets, so the job skips it:

```yaml
name: Antithesis
on:
  # Every 12 hours, on the default branch.
  schedule:
    - cron: "0 */12 * * *"
  # When someone adds the `antithesis` label to a pull request.
  pull_request:
    types: [labeled]

jobs:
  antithesis:
    # A pull request from another repository (a fork) gets no secrets, so the
    # launch could only fail. Skip it.
    if: >-
      github.event_name == 'schedule' ||
      (github.event.label.name == 'antithesis' &&
       github.event.pull_request.head.repo.full_name == github.repository)
    runs-on: ubuntu-latest
    permissions:
      contents: read
    env:
      ANTITHESIS_TENANT: my-tenant
      ANTITHESIS_REPOSITORY: us-central1-docker.pkg.dev/molten-verve-216720/my-tenant-repository
      ANTITHESIS_API_KEY: ${{ secrets.ANTITHESIS_API_KEY }}
      # The runner has Podman too, and snouty prefers it by default.
      SNOUTY_CONTAINER_ENGINE: docker
    steps:
      - uses: actions/checkout@v5

      - name: Install snouty
        run: curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh

      - name: Log in to the container registry
        env:
          GCP_REGISTRY_KEY: ${{ secrets.GCP_REGISTRY_KEY }}
        run: echo "$GCP_REGISTRY_KEY" | docker login us-central1-docker.pkg.dev --username _json_key --password-stdin

      # snouty launches only images in the local image store: it never builds
      # or pulls them.
      - name: Build and pull images
        run: |
          docker compose -f antithesis/docker-compose.yaml build
          docker compose -f antithesis/docker-compose.yaml pull --ignore-buildable

      # Starts the cluster on the runner and waits for setup-complete, so a
      # broken harness fails here instead of in Antithesis.
      - name: Validate the harness
        run: snouty validate ./antithesis

      - name: Launch an Antithesis run
        env:
          ANTITHESIS_GITHUB_TOKEN: ${{ secrets.GH_PAT }}
          PR_NUMBER: ${{ github.event.pull_request.number }}
          PR_TITLE: ${{ github.event.pull_request.title }}
        run: |
          if [ "$GITHUB_EVENT_NAME" = pull_request ]; then
            # Pull request runs are ephemeral.
            source_flag=()
            test_name="pull-request"
            description="PR #$PR_NUMBER: $PR_TITLE"
          else
            # Scheduled runs record property history for the branch.
            source_flag=(--source "$GITHUB_REF_NAME")
            test_name="$GITHUB_REF_NAME"
            description="Scheduled run of $GITHUB_REF_NAME"
          fi
          snouty launch \
            --launcher basic_test \
            --config ./antithesis \
            --test-name "$test_name" \
            --description "$description" \
            --duration 30m \
            "${source_flag[@]}"
```

Replace `my-tenant` with your tenant, and `antithesis` with the folder that holds your `docker-compose.yaml`. Then create an `antithesis` label in the repository.

The workflow needs these secrets:

- `ANTITHESIS_API_KEY`: your Antithesis API key. With [GitHub Actions OIDC](#github-actions-oidc), you need no key.
- `GCP_REGISTRY_KEY`: the JSON key of the service account for your container registry.
- `GH_PAT`: a [fine-grained personal access token](https://github.com/settings/tokens?type=beta) for the repository, with read access to metadata and read and write access to commit statuses. `secrets.GITHUB_TOKEN` does not work, because Antithesis uses the token after the job ends.

The launch sets the `continuous-testing/antithesis (<test-name>)` commit status to pending, and Antithesis sets the result when the run ends. Each test name gets its own status.

To run a pull request again, remove the `antithesis` label and add it back. If your system takes more than 2 minutes to start, add `--timeout 10m` to `snouty validate`.

A `--param` replaces the detected parameter with the same key. `--no-vcs` turns off the `vcs.*` detection, but not the commit status.

### GitHub Actions OIDC

In a GitHub Actions workflow, snouty can authenticate with the workflow's own OIDC token instead of an API key, so you store no secret.

> [!IMPORTANT]
> Your Antithesis tenant must trust GitHub Actions OIDC tokens from your repository before this works. Ask Antithesis support to configure it.

Give the job permission to mint the token, and leave `ANTITHESIS_API_KEY` unset, because an API key takes precedence:

```yaml
permissions:
  id-token: write
  contents: read
```

A pull request from a fork gets no OIDC token, just as it gets no secrets.

### Replacing the Antithesis Trigger Action

Each input of the trigger action has a snouty equivalent:

| Trigger action input | snouty |
|---|---|
| `tenant` | `ANTITHESIS_TENANT` |
| `api_key` | `ANTITHESIS_API_KEY`, or GitHub Actions OIDC |
| `username`, `password` | `ANTITHESIS_USERNAME`, `ANTITHESIS_PASSWORD` (deprecated) |
| `github_token` | `ANTITHESIS_GITHUB_TOKEN` |
| `notebook_name` | `--launcher` |
| `config_image` | `--config-image`, or `--config` to build and push the config |
| `images` | `--param antithesis.images=...` |
| `description` | `--description` |
| `test_name` | `--test-name` |
| `email_recipients` | `--recipients` |
| `team` | `--param run.team=...` |
| `system_name` | `--param vcs.system_name=...` |
| `additional_parameters` | one `--param key=value` for each parameter |

The action always sends the branch as `antithesis.source`. With snouty, pass `--source` yourself, as in the workflow above.

## Other CI systems

Like the GitHub Actions workflow, these examples log in to the container registry, build and pull the images, then run `snouty validate`. snouty launches only local images, and validate catches a broken harness in CI. Validate waits 2 minutes for setup-complete; for a slower system, add `--timeout 10m`.

Validate mounts a directory under `$TMPDIR` into the containers. If Docker runs elsewhere, such as in Docker-in-Docker, set `TMPDIR` to a directory Docker can see, as the GitLab CI example does.

### GitLab CI

GitLab CI checks out a detached HEAD, so the launch records the commit but no branch. This job records history for the default branch only. Docker runs in the `docker:dind` service, which shares only the project directory with the job, so `snouty validate` keeps its temporary files there. Store `ANTITHESIS_API_KEY` as a masked CI/CD variable. GitLab cannot mask a JSON key, so store the registry key base64-encoded (`base64 -w0 key.json`) as the masked variable `GCP_REGISTRY_KEY_BASE64`:

```yaml
antithesis:
  image: docker:latest
  services: [docker:dind]
  variables:
    ANTITHESIS_TENANT: my-tenant
    ANTITHESIS_REPOSITORY: us-central1-docker.pkg.dev/molten-verve-216720/my-tenant-repository
  before_script:
    - apk add --no-cache curl git xz
    - curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh
    - export PATH="$HOME/.local/bin:$PATH"
  script:
    - echo "$GCP_REGISTRY_KEY_BASE64" | docker login us-central1-docker.pkg.dev --username _json_key_base64 --password-stdin
    - docker compose -f antithesis/docker-compose.yaml build
    - docker compose -f antithesis/docker-compose.yaml pull --ignore-buildable
    - export TMPDIR="$CI_PROJECT_DIR/.snouty-tmp" && mkdir -p "$TMPDIR"
    - snouty validate ./antithesis
    - |
      # Only the default branch records property history.
      source_flag=""
      if [ "$CI_COMMIT_BRANCH" = "$CI_DEFAULT_BRANCH" ]; then
        source_flag="--source $CI_COMMIT_BRANCH"
      fi
      snouty launch \
        --launcher basic_test \
        --config ./antithesis \
        --duration 30m \
        $source_flag
```

### Buildkite

Buildkite checks out a detached HEAD, so the launch records the commit but no branch. For a repository on github.com, it also records the repository and a commit link. The agent needs Docker with Compose. Give it `ANTITHESIS_API_KEY` and `GCP_REGISTRY_KEY`, the JSON key of the service account for your container registry, from your secrets store. `$$` keeps Buildkite from replacing a variable when it uploads the pipeline:

```yaml
steps:
  - label: Antithesis
    env:
      ANTITHESIS_TENANT: my-tenant
      ANTITHESIS_REPOSITORY: us-central1-docker.pkg.dev/molten-verve-216720/my-tenant-repository
    command: |
      curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh
      export PATH="$$HOME/.local/bin:$$PATH"
      echo "$$GCP_REGISTRY_KEY" | docker login us-central1-docker.pkg.dev --username _json_key --password-stdin
      docker compose -f antithesis/docker-compose.yaml build
      docker compose -f antithesis/docker-compose.yaml pull --ignore-buildable
      snouty validate ./antithesis
      # Only the default branch records property history.
      source_flag=""
      if [ "$$BUILDKITE_BRANCH" = "$$BUILDKITE_PIPELINE_DEFAULT_BRANCH" ] && [ "$$BUILDKITE_PULL_REQUEST" = "false" ]; then
        source_flag="--source $$BUILDKITE_BRANCH"
      fi
      snouty launch \
        --launcher basic_test \
        --config ./antithesis \
        --duration 30m \
        $$source_flag
```

### CircleCI

CircleCI checks out the branch of a branch build, so the launch records the commit and the branch. For a repository on github.com, it also records the repository and a commit link. Store `ANTITHESIS_API_KEY` and `GCP_REGISTRY_KEY`, the JSON key of the service account for your container registry, in a context or as project environment variables:

```yaml
version: 2.1
jobs:
  antithesis:
    machine:
      image: ubuntu-2404:current
    environment:
      ANTITHESIS_TENANT: my-tenant
      ANTITHESIS_REPOSITORY: us-central1-docker.pkg.dev/molten-verve-216720/my-tenant-repository
    steps:
      - checkout
      - run: |
          curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh
          export PATH="$HOME/.local/bin:$PATH"
          echo "$GCP_REGISTRY_KEY" | docker login us-central1-docker.pkg.dev --username _json_key --password-stdin
          docker compose -f antithesis/docker-compose.yaml build
          docker compose -f antithesis/docker-compose.yaml pull --ignore-buildable
          snouty validate ./antithesis
          # Only main records property history.
          source_flag=""
          if [ "$CIRCLE_BRANCH" = main ] && [ -z "$CIRCLE_PULL_REQUEST" ]; then
            source_flag="--source main"
          fi
          snouty launch \
            --launcher basic_test \
            --config ./antithesis \
            --duration 30m \
            $source_flag
workflows:
  antithesis:
    jobs: [antithesis]
```

### Commit status from another CI system

Only GitHub Actions sends the GitHub callback params for you. For a repository on GitHub that another CI system builds, pass them yourself. Then the launch also sets the commit status to pending:

```sh
snouty launch --launcher basic_test --config ./antithesis \
  --param "antithesis.integrations.github.callback_url=https://api.github.com/repos/OWNER/REPO/statuses/$COMMIT" \
  --param "antithesis.integrations.github.token=$GITHUB_PAT"
```
