# snouty mcp

`snouty mcp` serves snouty to AI agents over the Model Context Protocol (streamable HTTP). It runs in the foreground until it is stopped.

## Command

- `--host` (default `127.0.0.1`) and `--port` set the listen address.
- `--allowed-host` (repeatable) adds a Host header value the server accepts. By default, it accepts only the address it listens on.
- When the server is ready, it prints `Listening on $HOST:$PORT`.
- Logs one line per request/response with the timestamp, status code, and json-rpc method call. Request params are visible when `snouty --verbose` is set.
- On Ctrl-C, SIGTERM or SIGHUP, it stops and ends all work in progress.
- Global snouty flags `--settings` and `--profile` as well as environment variables apply to `snouty mcp` like any oither command.

## Tools

- Snouty MCP directly executes the underlying subcommand logic. Snouty MCP does not recursively exec itself to run subcommands. Refactor the implementations of subcommands as needed to expose the underlying logic to the MCP tool calls.
- All of the snouty mcp tools are marked read only. Even tools like exec which implicitely create a standalone branch in the multiverse which can't affect other things.
- All tool output is emitted in a single TextContent ContentBlock. Tools either output JSON or JSONL, depending on what the associated subcommand would output when the `--json` flag is specified. Structured output and output schemas are not used.
- A tool failure should mark `isError` on the result and include any error text in the output. It's fine if failures do not return JSON, match what the associated snouty subcommand does on error.
- For streaming endpoints which may error in the middle of the stream, the MCP server will buffer the result, and mark isError accordingly.
- Tools validate their input params before starting work.
- Tools should share the same logic under the hood as their associated snouty subcommand. Not all snouty subcommands are exposed through MCP.

### Tool descriptions

Each MCP tool should re-use as much of the associated subcommand's long-help text. However any CLI specific portions of the description should be omitted from the MCP description.

To aid in this, extract all Snouty long-help text into templated text files which are able to include each other via template expressions and omit/include certain sub-sets of the text based on context (mcp|cli). Ideally find a rust crate that can help you do this. The goal is that we can keep all of the snouty help text organized in files, and make it easy to render different versions of the help text based on use case (currently mcp or cli).

This will also make maintaining snouty help text easier. I may eventually use this to generate snouty man pages, so keep that in mind as a future path.

Ensure that the mcp rendered descriptions focus on understanding how to use the tool and parsing the results. Omit things like documenting human-output or how to enable `--json` as the output of MCP tools is always JSON.

### runs_list

Params:

- status: filter by status
- launcher: filter by launcher name
- created_after: filter by runs created after ISO 8601 timestamp
- created_before: filter by runs created before ISO 8601 timestamp
- limit: limit the number of runs to return (default: 10)

Returns JSONL formatted list of runs. Same output as `snouty runs list --json`.

### runs_show

Params:

- run_id (required): the run to show

Returns a JSON object with the run's metadata. Same output as `snouty runs show --json`.

### runs_properties

Params:

- run_id (required): the run to list properties for
- passing: only passing properties
- failing: only failing properties
- name: only properties whose name contains this substring (case-insensitive)
- group: only properties whose group contains this substring (case-insensitive)

Returns JSONL formatted list of properties, including their examples and counterexample moments. Same output as `snouty runs properties --json`.

### runs_build_logs

Params:

- run_id (required): the run to get build logs for

Returns JSONL formatted build and setup log lines. Same output as `snouty runs build-logs --json`.

### runs_logs

Params:

- run_id (required): the run to get logs for
- input_hash (required): the timeline to stream
- vtime: end the logs at this virtual time (if not specified, the timeline's current end)
- begin_vtime: start from this virtual time instead of the root

Returns JSONL formatted events, one per line. Same output as `snouty runs logs --json`.

### runs_search

Params:

- run_id (required): the run to search
- query (required): an event-set DSL query
- limit: the maximum number of events to return (default: 50, maximum: 999)

Returns JSONL formatted matching events. Same output as `snouty runs search --json`.

### runs_exec

Only available when the `runs-exec` unstable feature is set.

Params:

- run_id (required): the run with a live session
- input_hash (required): the input hash of the moment to execute at
- vtime (required): the virtual time of the moment to execute at
- script (required): the bash script to execute
- timeout: seconds to wait for the script to exit (default: 30)

Returns JSONL formatted output frames. Same output as `snouty runs exec --json`.

### docs_tree

Params:

- filter: case-insensitive filter applied on page paths and titles
- depth: limit output to nodes at this depth or shallower

Returns a JSON object matching this schema:

```json
{
  "type": "object",
  "required": ["children"],
  "properties": {
    "children": { "type": "array", "items": { "$ref": "#/$defs/node" } }
  },
  "$defs": {
    "node": {
      "type": "object",
      "required": ["name", "path"],
      "properties": {
        "name": {
          "type": "string",
          "description": "Last path segment, e.g. \"cpp\""
        },
        "path": {
          "type": "string",
          "description": "Path to pass to docs_show, e.g. \"reference/sdk/cpp\""
        },
        "title": {
          "type": "string",
          "description": "Page title. Absent when the node is only a directory"
        },
        "children": {
          "type": "array",
          "items": { "$ref": "#/$defs/node" },
          "description": "Child nodes, sorted by name. Absent when the node has no children or depth cut them off"
        }
      }
    }
  }
}
```

### docs_show

Supports any `path` form accepted by `snouty runs show <path>`.

Params:

- path (required): the docs page to show.

Returns the pages markdown content. This is an exception from all other commands which return JSON.

### docs_search

Params:

- query (required): the search text
- limit: the maximum number of results to return (default: 10)
- match: treat the query as a raw SQLite FTS5 expression instead of literal text

Returns a JSON array of matches with path, title and snippet. Same output as `snouty docs search --json`.

### doctor

Params:

- offline: skip the Antithesis API connectivity check

Returns a JSON object with the result of each check. Same output as `snouty doctor --json`.
