# Contributing

`snouty` is open source software and maintained by employees of Antithesis.
Pull-requests from the community are welcome but please be aware that
the maintainers may reject your contribution as out of scope.

## Scope

The CLI is meant to make interacting with the platform easier. Whatever functionality
it has should err on the side of having few false negatives. We want to
support the 80% use case, not every single customization and optimisation there is.

## Backwards compatibility

While there is no guarantee of backwards compatibility, we should strive to
not break existing command line invocations.

If that is not possible, old invocations should fail with a deprecation message
and an explanation what else to do.

## Documentation

When writing examples, prefer to use long flag names (`--webhook`) instead of
short ones (`-w`). This communicates the intent more clearly.

## Testing

New commands and options must be accompanied by [expect style tests](tests/cli_general.rs).
Having to change an existing test is a good sign of backwards incompatible breakage,
which will be subject to extra review.

## Dependencies & distribution

This tool supports amd64 and arm64 processors on Linux and macOS. Keep third party dependencies minimal, it
is fine to turn a transitive dependency into a direct dependency if necessary.
Try to keep the implementation OS agnostic, if that is not possible return an
error from the respective command.
