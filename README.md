# snouty

A CLI for the [Antithesis](https://antithesis.com) API. See the [webhook documentation](https://antithesis.com/docs/webhook/) for details on available endpoints and parameters.

> [!NOTE]
> Snouty is new and experimental. Stuff is going to change in the early days. Even so, we hope you'll try it out!

## Install snouty

### Install prebuilt binaries via shell script

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.sh | sh
```

### Install prebuilt binaries via powershell script

```sh
powershell -ExecutionPolicy Bypass -c "irm https://github.com/antithesishq/snouty/releases/latest/download/snouty-installer.ps1 | iex"
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
| [snouty-x86_64-pc-windows-msvc.zip](https://github.com/antithesishq/snouty/releases/latest/download/snouty-x86_64-pc-windows-msvc.zip)             | x64 Windows         | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-x86_64-pc-windows-msvc.zip.sha256)       |
| [snouty-aarch64-unknown-linux-gnu.tar.xz](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-unknown-linux-gnu.tar.xz) | ARM64 Linux         | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-aarch64-unknown-linux-gnu.tar.xz.sha256) |
| [snouty-x86_64-unknown-linux-gnu.tar.xz](https://github.com/antithesishq/snouty/releases/latest/download/snouty-x86_64-unknown-linux-gnu.tar.xz)   | x64 Linux           | [checksum](https://github.com/antithesishq/snouty/releases/latest/download/snouty-x86_64-unknown-linux-gnu.tar.xz.sha256)  |

## Configuration

Set the following environment variables:

```sh
export ANTITHESIS_USERNAME="your-username"
export ANTITHESIS_PASSWORD="your-password"
export ANTITHESIS_TENANT="your-tenant"
```

## Usage

The `-w`/`--webhook` flag specifies which webhook to call. Common values are `basic_test` (Docker environment) or `basic_k8s_test` (Kubernetes environment), unless you have a custom webhook registered with Antithesis.

### Launch a test run

```
snouty run -w basic_test \
  --antithesis.test_name "my-test" \
  --antithesis.description "nightly test run" \
  --antithesis.config_image config:latest \
  --antithesis.images app:latest \
  --antithesis.duration 30 \
  --antithesis.report.recipients "team@example.com"
```

Parameters can also be passed via stdin as JSON:

```sh
echo '{"antithesis.description": "test", ...}' | snouty run -w basic_test --stdin
```

### Launch a debugging session

Using CLI arguments:

```sh
snouty debug \
  --antithesis.debugging.session_id f89d5c11f5e3bf5e4bb3641809800cee-44-22 \
  --antithesis.debugging.input_hash 6057726200491963783 \
  --antithesis.debugging.vtime 329.8037810830865 \
  --antithesis.report.recipients "team@example.com"
```

Snouty can handle passing in a `Moment.from` via stdin:

```sh
echo 'Moment.from({ session_id: "...", input_hash: "...", vtime: ... })' | \
  snouty debug --stdin --antithesis.report.recipients "team@example.com"
```

## Shell Completions

Snouty supports tab completions for bash, zsh, fish, powershell, and elvish.

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

### PowerShell

```powershell
# Add to your PowerShell profile
snouty completions powershell | Out-String | Invoke-Expression
```

### Elvish

```sh
snouty completions elvish > ~/.config/elvish/lib/snouty.elv
```

# Credits

This project was originally developed by [orbitinghail](https://orbitinghail.dev) for use by [Graft](https://github.com/orbitinghail/graft). It was donated to Antithesis for the benefit of everyone on Feb 27, 2026.
