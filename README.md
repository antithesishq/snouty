# snouty

A CLI for humans and agents to use [Antithesis](https://antithesis.com).

## Quickstart

### With an agent

Give this prompt to your coding agent:

```text
Set up Antithesis testing for this project.
1. Install snouty (https://github.com/antithesishq/snouty) and the Antithesis plugin (https://github.com/antithesishq/antithesis-plugin).
2. Ask me to run `snouty login` in my own terminal. When I confirm, run `snouty doctor`.
3. Use the plugin to write and test an Antithesis workload for this system.
4. Walk me through using `snouty` to analyze and debug this system.
```

### Manually

1. Install snouty, and the [Antithesis plugin](https://github.com/antithesishq/antithesis-plugin) if you use an agent:

   ```sh
   curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh
   ```

2. Sign in. Run this in your own terminal:

   ```sh
   snouty login
   ```

3. Write a workload for your system, with the plugin or from the [Antithesis docs](https://antithesis.com/docs/). Put its `docker-compose.yaml` in `./config`.

4. Check the setup, then launch a run:

   ```sh
   snouty validate ./config
   snouty launch --launcher basic_test --config ./config
   ```

5. Monitor the run and triage failures:

   ```sh
   snouty runs wait <run_id>
   snouty runs properties --failing <run_id>
   snouty runs show --web <run_id>
   ```

Run `snouty --help` to list the commands, and `snouty <command> --help` for details. Add `--json` for machine-readable output. See [COOKBOOK.md](COOKBOOK.md) for worked recipes.

## Requirements

Commands that work with `docker-compose.yaml` files (e.g. `launch`, `validate`) require:

- **Docker Compose v2** — snouty drives either the standalone `docker-compose` binary or the `docker compose` CLI plugin (bundled with Docker Desktop/Engine).
  - Linux: [Install Docker Compose](https://docs.docker.com/compose/install/) or check your package manager.
  - macOS: [Install Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) (includes Compose v2) or `brew install docker-compose`.
- **A container engine** — Docker or Podman, used to build and push images.

If both engines are installed, Podman is preferred. You can make an explicit choice via the `container_engine` setting.

## Install snouty

```sh
# Shell script. This install can update itself, see Updating.
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh

# Prebuilt binary with cargo binstall
cargo binstall snouty

# Prebuilt binary with npm
npm install -g snouty

# From source
cargo install snouty

# Nix, from nixpkgs-unstable (may lag snouty releases)
nix profile install github:NixOS/nixpkgs/nixpkgs-unstable#snouty
```

### Updating

If you installed with the [shell script](#install-snouty), run:

```sh
snouty update            # latest release on your channel
snouty update 0.6.0      # a specific release, including pre-releases
```

The `stable` channel (the default) installs releases only. The `unstable` channel also installs pre-releases. Set the channel with `update_channel` in a settings file or `SNOUTY_UPDATE_CHANNEL`. Override it for one update with `--channel`:

```sh
snouty update --channel unstable
```

To install an older version than the current one, add `--force`.

If you used a different install method, update with the same tool.

### Uninstalling

```
cargo uninstall snouty || rm -f "$(which snouty)" "$(which snouty-update)" "$(which docker-credential-snouty)"
```

For a Nix install, run `nix profile remove snouty` after `nix profile install`, or `nix-env -e snouty` after `nix-env -i`.

If you set up [shell completions](#shell-completions), also remove the snouty line from your shell's startup file.

## Configuration

Snouty can be configured interactively via `snouty login`.

Run `snouty settings show` at any point to see what snouty resolves and which settings files it reads. Run `snouty doctor` to check that your environment works.

Settings are stored in `~/.config/snouty/settings.toml` (or `$XDG_CONFIG_HOME/snouty/settings.toml` if set)

```toml
tenant = "your-tenant"
container_engine = "docker"
```

### Settings

Environment variables always take precedence.

| Settings key                | Environment variable              | Purpose                                                                          |
| --------------------------- | --------------------------------- | -------------------------------------------------------------------------------- |
| `tenant`                    | `ANTITHESIS_TENANT`               | Your Antithesis tenant. Becomes the API host, `https://<tenant>.antithesis.com`.  |
| `registry`                  | `ANTITHESIS_REGISTRY`             | Container registry that `snouty launch` pushes the config image to. Defaults to the registry that your tenant reports. |
| `base_url`                  | `ANTITHESIS_BASE_URL`             | API URL, replacing the one derived from the tenant. See the proxy section below. |
| `https_proxy`               | `ANTITHESIS_HTTPS_PROXY`          | URL of an HTTP(S) proxy for snouty's API requests. SOCKS is not supported.      |
| `container_engine`          | `SNOUTY_CONTAINER_ENGINE`         | Force `docker` or `podman` instead of auto-detecting.                            |
| `update_channel`            | `SNOUTY_UPDATE_CHANNEL`           | `stable` (default) or `unstable`, for `snouty update`.                           |

### Environment variables

These environment variables have no settings key:

| Environment variable                         | Purpose                                                                                                         |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `ANTITHESIS_API_KEY`                         | Your API key. It takes precedence over a credential that `snouty login` stored.                                 |
| `ANTITHESIS_GITHUB_TOKEN`                    | A GitHub token that can write commit statuses. Used by `snouty launch` to update the commit status with the Run results. |
| `ANTITHESIS_EXTRA_HEADERS`                   | Headers to add to every API request, one `Name: value` pair per line. For example, `X-Proxy-Token: abc123` for a proxy that expects its own header. |
| `SNOUTY_SETTINGS_PATH`                       | A settings file that overrides the global settings file. The file must exist.                                  |
| `SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE` | On macOS, set to `1` to store credentials in a file, not in the keychain.                                        |

### Docker credential helper

If you use Docker older than 28.3.0, add snouty as the credential helper for your tenant registry in `~/.docker/config.json`:

```json
{
  "credHelpers": {
    "registry.<tenant>.antithesis.com": "snouty"
  }
}
```

`docker-credential-snouty` must be on your `PATH`. The shell script, npm and Nix installs put it there. If it is not on your `PATH`, make a symlink to snouty in a directory on your `PATH`:

```sh
ln -s "$(command -v snouty)" ~/.local/bin/docker-credential-snouty
```

## Authentication

Snouty accepts three credential types: OAuth tokens, Antithesis API keys, or GitHub Actions OIDC tokens. On a workstation, run `snouty login` and let Snouty store a credential for you. In CI and in scripts, use an API key or GitHub Actions OIDC tokens.

Run `snouty doctor` to see which credential you are using or to debug authentication problems.

### Credential injection by an HTTPS proxy

Some VM and development platforms hold your credentials for you, and inject them into outbound requests. The platform gives you a hostname that stands in for `https://<tenant>.antithesis.com`. That hostname terminates TLS, replaces the `Authorization` header with the real credential, and forwards the request. Your machine never holds the secret. Replit and exe.dev work this way; check your platform's documentation for the hostname it gives you.

To configure Snouty to use a API proxy:

```sh
snouty login --base-url https://antithesis.int.example.com
```

## MCP server

`snouty mcp` serves most of snouty's commands to AI agents as [Model Context Protocol](https://modelcontextprotocol.io) tools, over the stdio transport.

Run `snouty login` once before you use `snouty mcp`. The server uses snouty's credentials and configuration to access Antithesis. The MCP server has no authentication of its own and communicates with clients only over `stdio` (stdin, stdout).

Run `snouty mcp --help` to see all tools and usage details.

snouty is listed in the [Github MCP Registry](https://github.com/mcp/com.antithesis/snouty) and the [official MCP Registry](https://registry.modelcontextprotocol.io).

**Adding `snouty mcp` to your agent:**

```sh
# Claude Code
claude mcp add snouty --scope user -- snouty mcp

# Codex
codex mcp add snouty -- snouty mcp
```

## Running in CI

`snouty launch` runs in any CI system. See [CI.md](https://github.com/antithesishq/snouty/blob/main/CI.md) for complete workflows for GitHub Actions, GitLab CI, Buildkite and CircleCI.

## Shell Completions

Snouty supports tab completions for bash, zsh, fish, elvish, and PowerShell.

```sh
# Bash: add to ~/.bashrc
eval "$(snouty completions bash)"

# Zsh: add to ~/.zshrc, after compinit
autoload -Uz compinit
compinit
source <(snouty completions zsh)

# Fish: put this line in ~/.config/fish/completions/snouty.fish
snouty completions fish | source

# Elvish: add to ~/.config/elvish/rc.elv
if (has-external snouty) { eval (snouty completions elvish | slurp) }

# PowerShell: add to $PROFILE
snouty completions powershell | Out-String | Invoke-Expression
```

## Credits

This project was originally developed by [orbitinghail](https://orbitinghail.dev) for use by [Graft](https://github.com/orbitinghail/graft). It was donated to Antithesis for the benefit of everyone on Feb 27, 2026.
