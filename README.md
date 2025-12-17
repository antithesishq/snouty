# snouty

A CLI for the [Antithesis](https://antithesis.com) API. See the [webhook documentation](https://antithesis.com/docs/webhook/) for details on available endpoints and parameters.

## Installation

### From GitHub Releases

Download the latest binary from the [releases page](https://github.com/carl/snouty/releases/latest).

### Using cargo

```sh
cargo install snouty
```

### Using cargo-binstall

```sh
cargo binstall snouty
```

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
  --antithesis.debugging.vtime 329.8037810830865
```

Using `Moment.from` (copy directly from a triage report):

```sh
echo 'Moment.from({ session_id: "...", input_hash: "...", vtime: ... })' | snouty debug --stdin
```
