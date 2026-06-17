# snouty

A CLI for the [Antithesis](https://antithesis.com) API. See the [webhook documentation](https://antithesis.com/docs/webhook/) for details on available endpoints and parameters.

> [!NOTE]
> Snouty is new and experimental. Stuff is going to change in the early days. Even so, we hope you'll try it out!

## Install snouty

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
cargo uninstall snouty || rm -f "$(which snouty)" "$(which snouty-update)"
```

## Requirements

Commands that work with `docker-compose.yaml` files (e.g. `launch --config`, `validate`) require Docker or Podman.

If both are installed, Podman is preferred. You can override via the environment (`SNOUTY_CONTAINER_ENGINE=docker`) or with `container_engine` in a settings file (see below).

## Configuration

Using the API requires at least a **tenant** and a **repository**. These (and other settings) can be supplied via environment variables or a TOML settings file; environment variables always take precedence. Docs commands require no configuration at the moment.

The quickest way is environment variables:

```sh
export ANTITHESIS_TENANT="your-tenant"
export ANTITHESIS_REPOSITORY="us-central1-docker.pkg.dev/your-project/your-repo"
```

### Settings files

Settings can instead live in a TOML file. Snouty reads two, the first taking precedence:

1. A **project** settings file — `./.snouty.toml` by default. Point elsewhere with the global `--settings <path>` flag or the `SNOUTY_SETTINGS_PATH` environment variable. (This is unrelated to `launch --config`, which is the docker-compose directory.)
2. A **global** settings file — `settings.toml` under `$XDG_CONFIG_HOME/snouty/` (falling back to `$HOME/.config/snouty/`).

```toml
# .snouty.toml
tenant = "your-tenant"
repository = "us-central1-docker.pkg.dev/your-project/your-repo"
```

A matching environment variable always overrides the file. The supported keys and their environment-variable equivalents are:

| Settings key       | Environment variable      |
| ------------------ | ------------------------- |
| `tenant`           | `ANTITHESIS_TENANT`       |
| `repository`       | `ANTITHESIS_REPOSITORY`   |
| `base_url`         | `ANTITHESIS_BASE_URL`     |
| `container_engine` | `SNOUTY_CONTAINER_ENGINE` |

Authentication (below) is read from the environment only, never from a settings file.

### Profiles

A settings file can define named profiles for switching between environments. Select one with the global `--profile <name>` flag or the `ANTITHESIS_PROFILE` environment variable (the environment variable wins):

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

### Authentication

Antithesis supports two forms of authentication, supplied via environment variables only. An API key works with every command and is the recommended option:

```sh
export ANTITHESIS_API_KEY="your-api-key"
```

Username/password authentication is only supported when launching runs (`snouty launch`, `snouty debug`, and `snouty api webhook`). All other commands that talk to the API — such as `snouty runs` — require an API key.

```sh
export ANTITHESIS_USERNAME="your-username"
export ANTITHESIS_PASSWORD="your-password"
```

If you don't have an API key, ask Antithesis support for one.

## Usage

Snouty provides the following subcommands. Invoke `snouty <command> --help` to find out more.

- `snouty launch`: push images and kick off an Antithesis run.
- `snouty runs`: list and inspect Antithesis test runs and their results.
  - `snouty runs list`: list runs, with status/launcher/date filters.
  - `snouty runs show <run_id>`: show details for a single run.
  - `snouty runs properties <run_id>`: list property (assertion) results.
  - `snouty runs build-logs <run_id>`: stream a run's build logs.
  - `snouty runs logs <run_id> <hash> <vtime>`: stream logs for a moment.
  - `snouty runs events <run_id> <query>`: search events in a run.
- `snouty debug`: start a debug session.
- `snouty validate`: locally run and validate your docker-compose.yaml setup.
- `snouty doctor`: check your environment is configured correctly.
- `snouty docs`: fast, local search of the Antithesis documentation.
- `snouty completions <shell>`: generate shell completion scripts.
- `snouty update`: install the latest version.

## Shell Completions

Snouty supports tab completions for bash, zsh, fish, and elvish.

### Bash

```sh
# Add to ~/.bashrc
eval "$(snouty completions bash)"
```

### Zsh

```sh
# Add to ~/.zshrc
eval "$(snouty completions zsh)"
```

### Fish

```sh
snouty completions fish > ~/.config/fish/completions/snouty.fish
```

### Elvish

```sh
snouty completions elvish > ~/.config/elvish/lib/snouty.elv
```

# Credits

This project was originally developed by [orbitinghail](https://orbitinghail.dev) for use by [Graft](https://github.com/orbitinghail/graft). It was donated to Antithesis for the benefit of everyone on Feb 27, 2026.
