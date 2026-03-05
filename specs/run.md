# snouty run

Launch an Antithesis test run.

## User Story

As a developer, I want to launch an Antithesis test run from the command line so
that I can trigger testing without using the web UI.

## Behavior

1. The user specifies a webhook endpoint name via `-w` / `--webhook` (required).
2. Parameters are provided as `--key value` pairs (e.g. `--antithesis.duration 30`).
3. Parameters can alternatively be read from stdin via `--stdin`, as JSON or
   JSON5.
4. When both stdin and CLI args are provided, CLI args take priority over stdin
   values.
5. At least one source of parameters must be provided; otherwise the command
   fails.
6. Parameters are validated against the `testParams` schema before making any
   API call.
7. The command authenticates with the Antithesis API using
   `ANTITHESIS_USERNAME`, `ANTITHESIS_PASSWORD`, and `ANTITHESIS_TENANT`
   environment variables.
8. On success, the command prints a human-readable ETA for the report email to
   stderr.
9. On API failure, the command exits with an error showing the HTTP status and
   response body.
10. Before sending, the resolved parameters are printed to stderr with sensitive
    values (tokens, email recipients) redacted.
