// A pre-demo check against a running demo server: the MCP handshake, direct
// tool calls, one real agent turn, and a replay. Usage:
//
//   npm test                       # ws://127.0.0.1:8017/ws
//   npm test -- ws://host:port/ws

import WebSocket from "ws";

const url = process.argv[2] ?? `ws://127.0.0.1:${process.env.PORT ?? 8017}/ws`;
type Msg = { type: string; [k: string]: any };

const ws = new WebSocket(url);
const inbox: Msg[] = [];
const waiters: Array<() => void> = [];
ws.on("message", (data) => {
  inbox.push(JSON.parse(data.toString()));
  waiters.splice(0).forEach((w) => w());
});

/** Waits for the first message after `from` that matches, and returns its index. */
async function waitFor(pred: (m: Msg) => boolean, timeoutMs: number, from = 0): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const i = inbox.findIndex((m, j) => j >= from && pred(m));
    if (i >= 0) return i;
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise<void>((resolve) => {
      waiters.push(resolve);
      setTimeout(resolve, 250);
    });
  }
}

let failures = 0;
async function check(name: string, f: () => Promise<string | void>) {
  const started = Date.now();
  try {
    const note = await f();
    console.log(`  ok    ${name} (${((Date.now() - started) / 1000).toFixed(1)}s)${note ? ` — ${note}` : ""}`);
  } catch (e) {
    failures++;
    console.log(`  FAIL  ${name}: ${e instanceof Error ? e.message : e}`);
  }
}

function send(msg: Msg) {
  ws.send(JSON.stringify(msg));
}

let reqId = 0;
async function call(name: string, args: Record<string, unknown>) {
  const id = ++reqId;
  send({ type: "call_tool", reqId: id, name, args });
  const i = await waitFor((m) => m.type === "call_result" && m.reqId === id, 60_000);
  return inbox[i];
}

await new Promise<void>((resolve, reject) => {
  ws.once("open", () => resolve());
  ws.once("error", reject);
});
console.log(`demo server at ${url}`);

let hello: Msg | undefined;
await check("hello reports a connected snouty mcp", async () => {
  hello = inbox[await waitFor((m) => m.type === "hello", 5_000)];
  if (hello.status.state !== "connected") {
    await waitFor((m) => m.type === "mcp_status" && m.status.state === "connected", 15_000);
  }
  return `tenant ${hello.tenant}, ${hello.recordings.length} recordings`;
});

await check("tools/list has the run, docs and debug tools", async () => {
  const info = hello!.status.info ?? inbox.findLast((m) => m.type === "mcp_status")?.status.info;
  const names: string[] = info.tools.map((t: { name: string }) => t.name);
  for (const n of ["runs_list", "runs_properties", "runs_logs", "docs_search", "debug", "tool_help"]) {
    if (!names.includes(n)) throw new Error(`missing ${n}`);
  }
  return `${names.length} tools`;
});

await check("tools/call settings_show", async () => {
  const r = await call("settings_show", {});
  if (r.isError) throw new Error(r.text);
  return `${r.ms}ms`;
});

await check("tools/call runs_list returns runs from the tenant", async () => {
  const r = await call("runs_list", { limit: 3 });
  if (r.isError) throw new Error(r.text);
  const runs = r.text.trim().split("\n").map((l: string) => JSON.parse(l));
  if (runs.length === 0) throw new Error("no runs");
  return `${runs.length} runs, newest ${runs[0].run_id}`;
});

await check("tools/call with a bad argument fails as isError", async () => {
  const r = await call("runs_show", { run_id: "not-a-run" });
  if (!r.isError) throw new Error("expected isError");
});

await check("an agent turn calls a tool and answers", async () => {
  const from = inbox.length;
  send({ type: "chat", record: false, text: "Use runs_list to fetch the 3 most recent runs. Reply with only their run IDs, one per line." });
  await waitFor((m) => m.type === "idle", 120_000, from);
  const events = inbox.slice(from).filter((m) => m.type === "agent").map((m) => m.event);
  const error = events.find((e) => e.type === "error");
  if (error) throw new Error(error.message);
  const calls = events.filter((e) => e.type === "tool_call").map((e) => e.name);
  if (!calls.includes("runs_list")) throw new Error(`no runs_list call (calls: ${calls.join(", ")})`);
  const failed = events.filter((e) => e.type === "tool_result" && e.isError);
  if (failed.length) throw new Error(`tool error: ${failed[0].text}`);
  const text = events.filter((e) => e.type === "text_delta").map((e) => e.text).join("");
  if (!/[0-9a-f]{32}-\d+-\d+/.test(text)) throw new Error(`no run ID in the answer: ${text.slice(0, 200)}`);
  return `tools: ${calls.join(", ")}`;
});

send({ type: "reset" });

await check("a recorded session replays", async () => {
  const recordings = (inbox.findLast((m) => m.type === "recordings") ?? hello)!.recordings;
  if (!recordings.length) return "skipped: no recordings yet";
  const from = inbox.length;
  send({ type: "replay", name: recordings[0].name });
  await waitFor((m) => m.type === "replay_start", 5_000, from);
  // Replays keep their rhythm; stop after the first event to keep this fast.
  await waitFor((m) => m.type === "agent", 15_000, from);
  send({ type: "stop" });
  await waitFor((m) => m.type === "idle", 5_000, from);
  return recordings[0].prompt.slice(0, 50);
});

ws.close();
console.log(failures ? `\n${failures} check(s) failed` : "\nall checks passed");
process.exit(failures ? 1 : 0);
