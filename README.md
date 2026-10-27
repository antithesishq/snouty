# snouty

A CLI for the [Antithesis](https://antithesis.com) API. See the [webhook documentation](https://antithesis.com/docs/webhook/) for details on available endpoints and parameters.

## Usage

Snouty provides the following subcommands. Invoke `snouty <command> --help` to find out more.

- `snouty login`: sign in and store your tenant, repository, and credentials.
- `snouty launch`: push images and kick off an Antithesis run.
- `snouty runs`: list and inspect Antithesis test runs and their results.
  - `snouty runs list`: list runs, with status/launcher/date filters.
  - `snouty runs show <run_id>`: show details for a single run.
  - `snouty runs wait [--until ready|complete] <run_id>`: poll a run until it is ready to use or complete.
  - `snouty runs cancel <run_id>`: cancel a run that is starting or in progress.
  - `snouty runs properties <run_id>`: list property (assertion) results.
  - `snouty runs build-logs <run_id>`: stream a run's build logs.
  - `snouty runs logs <run_id> <hash> [vtime]`: stream a run's logs along one branch.
  - `snouty runs events <run_id> -m <needle>`: search events in a run.
  - `snouty runs search <run_id> <query>`: run an event-set DSL query against a run's events.
  - `snouty runs exec <run_id> <hash> <vtime> [script]`: run a bash script at a given moment in a run that is in progress (a test run or a debugging session). Needs tenant release 64.0 or newer.
- `snouty debug`: start a debug session.
- `snouty validate`: locally run and validate your docker-compose.yaml setup.
- `snouty doctor`: check your environment is configured correctly.
- `snouty docs`: search the Antithesis documentation locally (auto-refreshes the local copy over the network; pass `--offline` to skip).
- `snouty docker-credential <action>`: the Docker and Podman credential helper for your tenant registry. See [Docker credential helper](#docker-credential-helper).
- `snouty completions <shell>`: generate shell completion scripts.
- `snouty version`: print version and build information.
- `snouty update`: install the latest version. Set `update_channel = "unstable"` (or `SNOUTY_UPDATE_CHANNEL=unstable`) to also consider pre-releases; override the setting for one run with `--channel stable|unstable`.

Add `--json` for machine-readable output. See [COOKBOOK.md](COOKBOOK.md) for worked recipes.

## Requirements

Commands that work with `docker-compose.yaml` files (e.g. `launch`, `validate`) require:

- **Docker Compose v2** — snouty drives either the standalone `docker-compose` binary or the `docker compose` CLI plugin (bundled with Docker Desktop/Engine), whichever it finds on your `PATH`. podman-compose is no longer supported.
  - Linux: [Install Docker Compose](https://docs.docker.com/compose/install/) or check your package manager.
  - macOS: [Install Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) (includes Compose v2) or `brew install docker-compose`.
- **A container engine** — Docker or Podman, used to build and push images.

If both engines are installed, Podman is preferred. Override the engine with `SNOUTY_CONTAINER_ENGINE=docker` or `container_engine` in a settings file (see below); an explicit `DOCKER_HOST` in your environment is always respected.

### VM-backed and remote container engines

`snouty validate` bind-mounts a temp directory from this machine into each container. It watches this directory for the setup-complete event. Some container engines run inside a VM or on another machine. If the engine does not share this machine's temp directory, the engine creates the bind source on its own side and reports no error. The directory on this machine stays empty. Validate then fails with `timed out waiting for setup-complete event`, even though the system under test emits the event.

There are two fixes:

- Share this machine's temp directory with the engine. See the mount documentation for your engine.
- Set `SNOUTY_TEMP_DIR` to a directory under a path the engine already shares with write access:

  ```sh
  SNOUTY_TEMP_DIR=/path/shared/with/the/vm/snouty snouty validate ./config
  ```

  `SNOUTY_TEMP_DIR` must point to an empty or non-existent directory. This prevents validate from reading events that a previous run left behind. Snouty does not remove the directory after the run, so remove it before the next run.

## Install snouty

Each install method puts two binaries in the same directory: `snouty`, and `docker-credential-snouty`, the [Docker credential helper](#docker-credential-helper).

### Install prebuilt binaries via shell script

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh
```

This also allows you to update via `snouty update` later on.

### Install prebuilt binaries via cargo binstall

```sh
cargo binstall snouty
```

### Install snouty from source

```sh
cargo install snouty
```

### Download prebuilt binaries

| File                                                                                                                                                 | Platform            | Checksum                                                                                                                    |
| ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| [snouty-aarch64-apple-darwin.tar.xz](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-apple-darwin.tar.xz)             | Apple Silicon macOS | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-apple-darwin.tar.xz.sha256)       |
| [snouty-aarch64-unknown-linux-musl.tar.xz](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-unknown-linux-musl.tar.xz) | ARM64 Linux         | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-unknown-linux-musl.tar.xz.sha256) |
| [snouty-x86_64-unknown-linux-musl.tar.xz](https://github.com/antithesishq/snouty/releases/latest/download/snouty-x86_64-unknown-linux-musl.tar.xz)   | x64 Linux           | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-x86_64-unknown-linux-musl.tar.xz.sha256)  |

### Uninstalling

```
cargo uninstall snouty || rm -f "$(which snouty)" "$(which snouty-update)" "$(which docker-credential-snouty)"
```

If you set up [shell completions](#shell-completions), also remove the snouty line from your shell's startup file.

## Configuration

Using the API requires at least a **tenant** and a credential. `snouty launch` also needs a **repository** — the container registry it pushes the config image to. Docs commands need no configuration at all.

The quickest way is `snouty login`, which asks for each value and stores the result:

```sh
snouty login
```

Settings can also come from environment variables or a TOML settings file. An environment variable always takes precedence.

```sh
export ANTITHESIS_TENANT="your-tenant"
export ANTITHESIS_REPOSITORY="us-central1-docker.pkg.dev/your-project/your-repo"
export ANTITHESIS_API_KEY="your-api-key"
```

Run `snouty settings show` at any point to see what snouty resolves, and which settings files it reads. Run `snouty doctor` to check that your environment works.

### Settings files

Settings can instead live in a TOML file. Snouty reads two, the first taking precedence:

1. A **project** settings file — `./.snouty.toml` by default. Point elsewhere with the global `--settings <path>` flag or the `SNOUTY_SETTINGS_PATH` environment variable.
2. A **global** settings file — `settings.toml` under `$XDG_CONFIG_HOME/snouty/` (falling back to `$HOME/.config/snouty/`).

```toml
# .snouty.toml
tenant = "your-tenant"
repository = "us-central1-docker.pkg.dev/your-project/your-repo"
```

A matching environment variable always overrides the file. The supported keys and their environment-variable equivalents are:

| Settings key                | Environment variable              | Purpose                                                                          |
| --------------------------- | --------------------------------- | -------------------------------------------------------------------------------- |
| `tenant`                    | `ANTITHESIS_TENANT`               | Your Antithesis tenant. Becomes the API host, `https://<tenant>.antithesis.com`.  |
| `repository`                | `ANTITHESIS_REPOSITORY`           | Container registry that `snouty launch` pushes the config image to.              |
| `base_url`                  | `ANTITHESIS_BASE_URL`             | API URL, replacing the one derived from the tenant. See the proxy section below. |
| `container_engine`          | `SNOUTY_CONTAINER_ENGINE`         | Force `docker` or `podman` instead of auto-detecting.                            |
| `update_channel`            | `SNOUTY_UPDATE_CHANNEL`           | `stable` (default) or `unstable`, for `snouty update`.                           |
| `api_cache_max_file_size`   | `SNOUTY_API_CACHE_MAX_FILE_SIZE`  | Largest response the API cache stores, as `10 MB` or a byte count.               |
| `api_cache_respect_headers` | `SNOUTY_API_CACHE_RESPECT_HEADERS`| Set `false` to ignore server caching directives.                                 |

A credential is never read from a settings file. `snouty login` keeps credentials apart from settings — see [Authentication](#authentication).

Run `snouty settings show` to print the value that snouty resolved for each setting, and the settings files it read.

### Trusting a project settings file

snouty uses `./.snouty.toml` only after you trust it. Until you trust or deny the file, each command that reads the settings stops with an error. Review the file, then decide:

```sh
snouty settings trust   # use ./.snouty.toml while its content stays the same
snouty settings deny    # ignore ./.snouty.toml
```

Each command also takes the path of a settings file. When a trusted file changes, you must trust it again. snouty keeps the decisions in `trust.toml`, next to the global settings file.

A file that you name with `--settings` or `SNOUTY_SETTINGS_PATH` needs no trust, because you chose it. To use every project settings file without a check, for example in CI, set `SNOUTY_SETTINGS_TRUST=all`.

### Profiles

A settings file can define named profiles for switching between environments. Select one with the global `--profile <name>` flag or the `ANTITHESIS_PROFILE` environment variable (the flag wins):

```toml
# .snouty.toml
tenant = "default-tenant"
repository = "registry.example.com/default"

[profile.staging]
tenant = "staging-tenant"
repository = "registry.example.com/staging"
```

```sh
snouty --profile staging runs list
```

For any one setting, snouty uses the first value it finds, highest precedence first:

1. environment variable
2. the selected profile in the project settings file
3. the selected profile in the global settings file
4. the top-level default in the project settings file
5. the top-level default in the global settings file

### Unstable features

When a subcommand, or the Antithesis API it calls, is still changing shape, snouty can keep it behind a gate, turned on by naming its feature in `SNOUTY_UNSTABLE_FEATURES` (a comma-separated list). Anything behind this gate can change its behavior, its flags, or its id, or go away, in any release.

| Feature | Enables                                                                    |
| ------- | -------------------------------------------------------------------------- |
| `mcp`   | `snouty mcp`, the MCP server. Its tools and their output can still change. |

An id that snouty does not know, such as the retired `runs-exec`, has no effect.

## MCP server

`snouty mcp` serves most of snouty's commands to AI agents as [Model Context Protocol](https://modelcontextprotocol.io) tools, over the stdio transport. It is an unstable feature: set `SNOUTY_UNSTABLE_FEATURES=mcp` to turn it on. The agent starts the server itself. For example, in Claude Code:

```sh
claude mcp add snouty -e SNOUTY_UNSTABLE_FEATURES=mcp -- snouty mcp
```

A JSON config for a stdio server looks like this one. The key names can be different in your client, so refer to its documentation:

```json
{
  "mcpServers": {
    "snouty": {
      "command": "snouty",
      "args": ["mcp"],
      "env": { "SNOUTY_UNSTABLE_FEATURES": "mcp" }
    }
  }
}
```

Run `snouty mcp --help` to see all tools.

The server sets no time limit and no output limit on a tool call. The client sets them: when it cancels a call or closes stdin, the server kills the call's command. A command also exits when its server is gone. Up to 8 tool calls run at the same time, and `--max-concurrent-calls` changes that number.

The server reads settings, profiles, and credentials as the other commands do, so `--settings`, `--profile`, `--verbose`, and the `ANTITHESIS_*` environment variables apply to every tool call. A client cannot set them.

### Debugging the MCP server

1. Set `RUST_LOG=debug` in the server's environment, and add `--verbose` to its args. For example: `claude mcp add snouty -e SNOUTY_UNSTABLE_FEATURES=mcp -e RUST_LOG=debug -- snouty --verbose mcp`.
2. Read the server's stderr in the client. In Claude Code, start `claude --debug` and use `/mcp` to see the server's state. The server logs each command line that it runs, as `running: snouty --json …`.
3. Run the logged command yourself, in a terminal. It prints what the tool call returned. Set `RUST_LOG=debug` on it too, to see its own debug log: the server does not pass `RUST_LOG` to its tool calls.

## Authentication

Snouty accepts four kinds of credentials: OAuth tokens from a browser sign-in, an API key, a GitHub Actions OIDC token, and a deprecated username and password. On a workstation, run `snouty login` and let snouty store a credential for you. In CI and in scripts, use environment variables or GitHub Actions OIDC tokens.

Snouty looks for a credential in this order, and uses the first one it finds:

1. `ANTITHESIS_API_KEY` in the environment.
2. `ANTITHESIS_USERNAME` and `ANTITHESIS_PASSWORD` in the environment (deprecated).
3. The credential stored for the selected `--profile`: the system keychain first, the credentials file second.
4. The credential stored for the default profile: the system keychain first, the credentials file second.
5. A GitHub Actions OIDC token, when the workflow exposes one.

An environment variable always wins over a stored credential. Unset `ANTITHESIS_API_KEY` when you want snouty to use what `snouty login` stored.

Run `snouty doctor` to see which credential snouty resolves, and where it comes from. doctor also warns when more than one source holds a credential, and names the action that hands the run to the next source.

### Browser sign-in with `snouty login`

`snouty login` asks for your tenant and your repository, then asks how you want to authenticate. It writes the tenant and the repository to the global settings file, and it stores the credential separately.

```sh
snouty login
snouty login --tenant "your-tenant" --repository "us-central1-docker.pkg.dev/your-project/your-repo"
snouty login --profile staging
```

Pick **Single sign-on (OAuth)** to sign in through your identity provider. Snouty binds a loopback server, opens a browser, and waits up to five minutes for the redirect to come back. It prints the sign-in URL as well, so you can open the URL by hand when it cannot open a browser for you. The exchange uses PKCE, so no shared secret is needed.

Two limits are worth knowing:

- The menu offers single sign-on only when your tenant enables CLI OAuth. Without it, `snouty login` asks for an API key straight away.
- The browser must run on the same machine as snouty, because the redirect goes to `http://localhost:<port>/callback`. On a headless machine or a remote VM, either forward that port to your workstation, or use another authentication method.

### Where stored credentials live

On macOS, snouty stores the credential in the keychain, under the service name `snouty`. The entry is named `_default_`, or `profile_<name>` for a named profile. Set `SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE=1` to use the credentials file instead.

On every other platform, snouty writes `credentials.toml` next to the global settings file, in `$XDG_CONFIG_HOME/snouty/` (falling back to `$HOME/.config/snouty/`).

### API key

An API key works with every command, and needs no browser. Ask Antithesis support for one if you do not have one.

```sh
export ANTITHESIS_API_KEY="your-api-key"
```

`snouty login` also stores an API key for you, which keeps the key out of your shell history and out of your environment.

### GitHub Actions OIDC

In a GitHub Actions workflow, snouty authenticates with the workflow's own OIDC token, so you store no secret. Give the job permission to mint the token:

```yaml
permissions:
  id-token: write
  contents: read
```

> [!IMPORTANT]
> Your Antithesis tenant must trust GitHub Actions OIDC tokens from your repository before this works. Ask Antithesis support to configure it.

### Credential injection by an HTTPS proxy

Some VM and development platforms hold your credentials for you, and inject them into outbound requests. The platform gives you a hostname that stands in for `https://<tenant>.antithesis.com`. That hostname terminates TLS, replaces the `Authorization` header with the real credential, and forwards the request. Your machine never holds the secret. Replit and exe.dev work this way; check your platform's documentation for the hostname it gives you.

Point snouty at that hostname, and give it a placeholder API key:

```sh
export ANTITHESIS_BASE_URL="https://antithesis.int.example.com"
export ANTITHESIS_API_KEY="replaced-by-proxy"
```

Three things to know:

- `ANTITHESIS_BASE_URL` replaces the URL that snouty derives from the tenant, so every API request goes to the proxy instead. Snouty takes the value as given, and only trims a trailing `/` or `/api/v1`.
- Snouty still needs *a* credential, or it stops before it sends a request. Snouty never inspects the value, so any non-empty string works. Use an obvious placeholder such as `replaced-by-proxy`, so a reader sees that the real secret is elsewhere. An empty value counts as unset, and does not work.
- Keep `ANTITHESIS_TENANT` set. Only the derived base URL uses it, and `ANTITHESIS_BASE_URL` replaces that, but `snouty doctor` reports a missing tenant as a failure.

`snouty doctor` confirms the whole setup: it reports the API key as provided, and it contacts the API through the proxy to report the API and tenant versions.

`ANTITHESIS_EXTRA_HEADERS` adds headers to every API request, as one `Name: value` pair per line. Use it when the platform expects a header of its own.

```sh
export ANTITHESIS_EXTRA_HEADERS="X-Proxy-Token: abc123"
```

### Docker credential helper

Your tenant has a container registry at `registry.<tenant>.antithesis.com`. Docker and Podman can sign in to it with your snouty credential, through the `docker-credential-snouty` helper. `docker-credential-snouty` must be on your `PATH`. It contains the helper code of snouty, and does not run `snouty` or another program. To use the helper, add the registry to `credHelpers` in the file that your engine reads:

```json
{
  "credHelpers": {
    "registry.<tenant>.antithesis.com": "snouty"
  }
}
```

Then `docker push`, `docker pull`, `podman push` and `podman pull` sign in to the registry, and `docker login` or `podman login` is not necessary.

- **Docker** reads `~/.docker/config.json`, or `$DOCKER_CONFIG/config.json` when `DOCKER_CONFIG` is set.
- **Podman** reads `${XDG_RUNTIME_DIR}/containers/auth.json` (Linux only), then `~/.config/containers/auth.json`, then `~/.docker/config.json`. It uses the first file that has an entry for the registry, so a username and password from `podman login` in an earlier file win over `credHelpers` in a later file. Put the entry in `~/.config/containers/auth.json`, because the system clears `${XDG_RUNTIME_DIR}` when you log out. When `REGISTRY_AUTH_FILE` or `--authfile` names a file, Podman reads only that file. When `DOCKER_CONFIG` is set, Podman reads only `$DOCKER_CONFIG/config.json`.
- **Podman on macOS** (`podman machine`): the `podman` command on your Mac gets the credential before it sends the push or pull to the machine. Install snouty on your Mac, not in the machine, and put the entry in `~/.config/containers/auth.json` on your Mac. This `podman` gets the credential for each registry in `credHelpers` before each push and pull. Thus, when the helper fails, for example because your login expired, every push and pull fails, also for other registries. Run `snouty login` to repair it.

The username is `snouty-token`. The password is your API key, or the OAuth access token from `snouty login`. Snouty refreshes the OAuth token each time the engine asks for it, so a long push has the full lifetime of the token. The registry does not accept a GitHub Actions OIDC token or a username and password.

Snouty owns the credential, so `docker login`, and a `podman login` with a username, fail with a message that tells you to run `snouty login` or to set `ANTITHESIS_API_KEY`. A `podman login` with no username succeeds, because it gets the credential from snouty. `docker logout` and `podman logout` succeed and change nothing: the engine continues to get the credential from snouty.

The engine gives the helper no profile, so snouty uses the profile whose `repository` is on the requested registry, and whose `tenant` is the tenant of that registry. The selected profile wins when it agrees, then the top-level defaults, then the one named profile that agrees. If more than one named profile agrees, set `ANTITHESIS_PROFILE` to select one. The engine starts the helper in any working directory, such as a cloned repository, so the helper does not read a `./.snouty.toml`. Put the registry settings in the global `settings.toml`, or name a settings file with `SNOUTY_SETTINGS_PATH`.

To debug the helper, run `snouty docker-credential`. It runs the same code as `docker-credential-snouty`, and it also takes `--profile` and `--settings`:

```sh
echo registry.<tenant>.antithesis.com | snouty docker-credential get
```

### Username and password (deprecated)

Username and password authentication is deprecated. It works with `snouty launch` and `snouty debug` only, and both print a warning that points to `snouty login`. Every other command that talks to the API refuses it. Use `snouty login` or an API key instead.

`snouty login` does not set up a username and password. Set `ANTITHESIS_USERNAME` and `ANTITHESIS_PASSWORD` instead. A username and password that an older snouty stored in the credentials file still works.

## Shell Completions

Snouty supports tab completions for bash, zsh, fish, elvish, and PowerShell. Have your shell generate them at startup, as below. A completion script saved to a file describes the snouty that wrote it, and goes stale when `snouty update` installs a new version. Open a new shell after each update to load the new completions.

### Bash

```sh
# Add to ~/.bashrc
eval "$(snouty completions bash)"
```

### Zsh

```sh
# Add to ~/.zshrc, after compinit
autoload -Uz compinit
compinit
source <(snouty completions zsh)
```

### Fish

```fish
# Put this line in ~/.config/fish/completions/snouty.fish
snouty completions fish | source
```

### Elvish

```elvish
# Add to ~/.config/elvish/rc.elv
if (has-external snouty) { eval (snouty completions elvish | slurp) }
```

### PowerShell

```powershell
# Add to $PROFILE
snouty completions powershell | Out-String | Invoke-Expression
```

# Credits

This project was originally developed by [orbitinghail](https://orbitinghail.dev) for use by [Graft](https://github.com/orbitinghail/graft). It was donated to Antithesis for the benefit of everyone on Feb 27, 2026.
