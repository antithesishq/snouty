# snouty debug

Launch an Antithesis multiverse debugging session.

## User Story

As a developer, I want to launch a debugging session from the command line using details from a triage report so that I can reproduce and investigate a specific moment in time.

## Behavior

1. Parameters are provided as `--key value` pairs (e.g. `--antithesis.debugging.session_id ...`).
2. Parameters can alternatively be read from stdin via `--stdin`, as JSON, JSON5, or Moment.from format.
3. Moment.from format (e.g. `Moment.from({ session_id: "...", input_hash: "...", vtime: 329.8 })`) is auto-detected on stdin. Keys are mapped to `antithesis.debugging.*` and numeric values are converted to strings.
4. When both stdin and CLI args are provided, CLI args take priority over stdin values.
5. At least one source of parameters must be provided; otherwise the command fails.
6. Parameters are validated against the `debuggingParams` schema before making any API call. Required fields: `session_id`, `input_hash`, and `vtime`.
7. The command authenticates with the Antithesis API using `ANTITHESIS_USERNAME`, `ANTITHESIS_PASSWORD`, and `ANTITHESIS_TENANT` environment variables.
8. On success, the API response body is printed to stdout and a human-readable ETA for the debugging session email is printed to stderr.
9. On API failure, the command exits with an error showing the HTTP status and response body.
10. Before sending, the resolved parameters are printed to stderr with sensitive values redacted.
