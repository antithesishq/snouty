# snouty mcp: browser demo

A web page that shows `snouty mcp` at work. A Node server starts `snouty mcp`,
connects to it with the official MCP client, and gives its tools to a Claude
agent. The page shows three things side by side:

- **Tools**: the `tools/list` result. Select a tool to call it yourself, with a
  form made from its input schema, or to read its `tool_help`.
- **Chat**: the agent. Each tool call shows its arguments, the snouty command
  that the server ran, the result and the time it took.
- **Inspector**: every JSON-RPC message on the stdio transport, with request
  and response latency, and the server's own log of each command it runs.

```
browser ──WebSocket──▶ server/index.ts ──stdio JSON-RPC──▶ snouty mcp ──▶ snouty --json <command> ──▶ Antithesis API
                             │
                             └── server/agent.ts ──▶ Claude Messages API (streaming, tools = snouty's MCP tools)
```

The agent asks you before it calls a tool that the server does not mark
`readOnlyHint` (today, only `debug`, which starts a debugging session).

## Run it

You need Node 22 or later, a snouty build, Antithesis credentials that snouty
can use (`snouty doctor` checks them), and an Anthropic API key in
`ANTHROPIC_API_KEY` (and `ANTHROPIC_BASE_URL` for a proxy).

```sh
cargo build                 # the demo uses target/release or target/debug, else snouty on PATH
cd demo/mcp
npm ci
npm run demo                # builds the page, then serves it on port 8017
```

Open <http://localhost:8017/>. Set `SNOUTY_BIN` to use another snouty binary,
and `PORT` to use another port. The page also works behind a reverse proxy
under a path prefix, for example at `/snouty-mcp/`. The proxy must pass
WebSocket upgrades for `<prefix>/ws`.

## Check it before a demo

With the server running:

```sh
npm test                    # MCP handshake, direct tool calls, one agent turn, a replay
test/browser.sh             # renders the page in headless Chrome (agent-browser), screenshots in test/screenshots
```

## Demo walkthrough

1. **The server.** Select **Restart MCP**. The inspector shows the handshake
   live: `initialize`, `notifications/initialized`, `tools/list`. The
   **Server** tab shows the server's instructions and capabilities.
2. **A tool, by hand.** Select `runs_list` in the tools list and call it. This
   is a plain `tools/call`, with no model in the loop. Then select
   `tool_help`: snouty renders the command's long help for an agent.
3. **What's live right now?** Select the first suggestion. The agent calls
   `runs_list` twice in parallel. Point at the inspector: the two `tools/call`
   requests, the `snouty --json runs list …` lines from the server log, and
   the latency of each response.
4. **Triage a failed run.** Select the second suggestion. The agent walks from
   the run to its failing properties, to one property's counterexamples, to
   the logs at the failing moment, and explains the bug. Expand a tool call to
   show the arguments, the exact command and the JSON that came back. This
   takes about 1 to 2 minutes.
5. **Approval.** Ask the agent to open a debugging session at the failing
   moment. It stops and asks before it calls `debug`. Decline (or approve, to
   start a real session).
6. **Cancellation.** Ask a question that makes the agent wait, such as "Call
   runs_wait on run `<an in-progress run>` until it is complete." Select
   **Stop**. The inspector shows `notifications/cancelled`, and snouty kills
   the command.

The other two suggestions (core-hours, docs) are good for questions from the
audience.

## If the network fails

The server saves each chat turn that finishes without an error in
`recordings/`. On the empty chat, **Replay a recorded session** plays one back
with its original timing, including the inspector traffic. Run the walkthrough
once before the demo, so that the replays exist. This also fills snouty's API
cache, so the log fetches in step 4 are fast in the live run.

## Files

- `server/mcp.ts`: starts `snouty mcp` and reports each JSON-RPC message.
- `server/agent.ts`: the agent loop on the Claude Messages API.
- `server/index.ts`: the HTTP and WebSocket server, recordings and replay.
- `src/`: the page, in React with [Anta](https://anta.design) and its Antithesis
  theme.
- `test/`: the pre-demo checks.
