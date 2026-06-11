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

If both are installed, Podman is preferred. You can override via environment `SNOUTY_CONTAINER_ENGINE=docker`.

## Configuration

At a minimum Snouty requires tenant and repository to be provided as environment variables when using the API. Docs commands require no configuration at the moment.

```sh
export ANTITHESIS_TENANT="your-tenant"
export ANTITHESIS_REPOSITORY="us-central1-docker.pkg.dev/your-project/your-repo"
```

Antithesis supports two forms of authentication. An API key works with every command and is the recommended option:

```sh
export ANTITHESIS_API_KEY="your-api-key"
```

Username/password authentication is only supported when launching runs (`snouty launch`, `snouty debug`, and `snouty api webhook`). All other commands that talk to the API — such as `snouty runs` — require an API key.

```sh
export ANTITHESIS_USERNAME="your-username"
export ANTITHESIS_PASSWORD="your-password"
```

If you don't have an API key, ask your Antithesis contact for one.

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
- `snouty api webhook`: send a raw request to an Antithesis webhook endpoint.
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
