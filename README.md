# snouty

A CLI for the [Antithesis](https://antithesis.com) API. See the [webhook documentation](https://antithesis.com/docs/webhook/) for details on available endpoints and parameters.

> [!NOTE]
> Snouty is new and experimental. Stuff is going to change in the early days. Even so, we hope you'll try it out!

## Install snouty

### Install prebuilt binaries via shell script

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh
```

### Install prebuilt binaries via cargo binstall

```sh
cargo binstall snouty
```

### Install snouty from source

```sh
cargo install snouty
```

### Download prebuilt binaries

| File                                                                                                                                               | Platform            | Checksum                                                                                                                   |
| -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| [snouty-aarch64-apple-darwin.tar.xz](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-apple-darwin.tar.xz)           | Apple Silicon macOS | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-apple-darwin.tar.xz.sha256)      |
| [snouty-aarch64-unknown-linux-gnu.tar.xz](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-unknown-linux-gnu.tar.xz) | ARM64 Linux         | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-unknown-linux-gnu.tar.xz.sha256) |
| [snouty-x86_64-unknown-linux-gnu.tar.xz](https://github.com/antithesishq/snouty/releases/latest/download/snouty-x86_64-unknown-linux-gnu.tar.xz)   | x64 Linux           | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-x86_64-unknown-linux-gnu.tar.xz.sha256)  |

### Uninstalling

```
cargo uninstall snouty || rm -f "$(which snouty)" "$(which snouty-update)"
```

## Requirements

Commands that work with `docker-compose.yaml` files (e.g. `run --config`, `validate`) require Docker or Podman. When using Podman, [`podman-compose`](https://github.com/containers/podman-compose) **1.1.0 or later** must be installed.

## Configuration

Set the following environment variables:

```sh
export ANTITHESIS_USERNAME="your-username"
export ANTITHESIS_PASSWORD="your-password"
export ANTITHESIS_TENANT="your-tenant"
export ANTITHESIS_REPOSITORY="us-central1-docker.pkg.dev/your-project/your-repo"
```

## Usage

Snouty provides the following subcommands. Invoke `snouty <command> --help` to find out more.

- `snouty run`: push images and kick off an Antithesis run.
- `snouty validate`: locally run and validate your docker-compose.yaml setup.
- `snouty docs`: fast, local search of the Antithesis documentation.
- `snouty debug`: start a debug session.
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
