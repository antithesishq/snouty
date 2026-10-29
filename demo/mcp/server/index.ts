import { createReadStream, existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { WebSocketServer, type WebSocket } from "ws";
import { AgentSession, EFFORTS, MODELS, type AgentEvent, type Effort } from "./agent.ts";
import { resultText, SnoutyMcp, type WireEvent } from "./mcp.ts";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..");
const repo = resolve(root, "../..");
const dist = join(root, "dist");
const recordings = join(root, "recordings");
const PORT = Number(process.env.PORT ?? 8017);
// The WebSocket can call any tool with your credentials, so listen on
// loopback only, behind a reverse proxy, unless HOST says otherwise.
const HOST = process.env.HOST ?? "127.0.0.1";

function findSnouty(): string {
  if (process.env.SNOUTY_BIN) return process.env.SNOUTY_BIN;
  for (const p of [join(repo, "target/release/snouty"), join(repo, "target/debug/snouty")]) {
    if (existsSync(p)) return p;
  }
  return "snouty";
}

function tenant(bin: string): string | undefined {
  try {
    const out = execFileSync(bin, ["--json", "settings", "show"], { encoding: "utf8", timeout: 10_000 });
    const s = JSON.parse(out) as { settings?: { tenant?: string | null } };
    return s.settings?.tenant ?? undefined;
  } catch {
    return process.env.ANTITHESIS_TENANT;
  }
}

const bin = findSnouty();
const tenantName = tenant(bin);
const clients = new Set<Client>();
// Recent wire traffic, so that a browser that connects late still sees the
// handshake.
const history: WireEvent[] = [];
const HISTORY = 300;

const mcp = new SnoutyMcp(bin, (e) => {
  if (e.kind !== "status") {
    history.push(e);
    if (history.length > HISTORY) history.shift();
  }
  for (const c of clients) c.wire(e);
});

/** What happened in one chat turn, for replay when the network fails. */
interface Recording {
  prompt: string;
  model: string;
  savedAt: string;
  events: Array<{ t: number; e: unknown }>;
}

class Client {
  agent: AgentSession;
  private recording: Recording | null = null;
  private recordStart = 0;
  private replaying: AbortController | null = null;
  private chatting = false;
  private chatSeq = 0;

  constructor(private ws: WebSocket) {
    this.agent = new AgentSession(mcp, (e) => this.agentEvent(e));
    this.hello();
  }

  send(msg: unknown) {
    if (this.ws.readyState === this.ws.OPEN) this.ws.send(JSON.stringify(msg));
  }

  private forward(msg: unknown) {
    if (this.recording) this.recording.events.push({ t: Date.now() - this.recordStart, e: msg });
    this.send(msg);
  }

  wire(e: WireEvent) {
    if (e.kind === "status") {
      this.send({ type: "mcp_status", status: e.status });
      return;
    }
    this.forward({ type: "wire", event: e });
  }

  private agentEvent(e: AgentEvent) {
    this.forward({ type: "agent", event: e });
  }

  hello() {
    this.send({
      type: "hello",
      status: mcp.status,
      tenant: tenantName,
      models: MODELS,
      model: this.agent.model,
      effort: this.agent.effort,
      recordings: listRecordings(),
    });
    for (const e of history) this.send({ type: "wire", event: e });
  }

  async onMessage(raw: string) {
    let m: Record<string, unknown>;
    try {
      m = JSON.parse(raw);
    } catch {
      return;
    }
    switch (m.type) {
      case "chat": {
        const text = String(m.text ?? "").trim();
        if (!text || this.chatting || this.replaying) return;
        // A reset or a newer chat makes this one stale: it no longer owns
        // the recording or the idle message.
        const seq = ++this.chatSeq;
        this.chatting = true;
        const rec: Recording = { prompt: text, model: this.agent.model, savedAt: "", events: [] };
        this.recording = rec;
        this.recordStart = Date.now();
        if (mcp.status.state !== "connected") await mcp.connect().catch(() => {});
        if (seq !== this.chatSeq) return;
        const ok = await this.agent.send(text);
        if (seq !== this.chatSeq) return;
        this.chatting = false;
        this.recording = null;
        if (ok && m.record !== false) {
          saveRecording(rec);
          this.send({ type: "recordings", recordings: listRecordings() });
        }
        this.send({ type: "idle" });
        return;
      }
      case "stop":
        this.agent.stop();
        this.stopReplay();
        return;
      case "reset":
        this.chatSeq++;
        this.chatting = false;
        this.recording = null;
        this.agent.reset();
        this.stopReplay();
        return;
      case "approve":
        this.agent.approve(String(m.id), m.ok === true);
        return;
      case "set_model":
        if (MODELS.some((x) => x.id === m.model)) this.agent.model = String(m.model);
        if (EFFORTS.includes(m.effort as Effort)) this.agent.effort = m.effort as Effort;
        return;
      case "restart_mcp":
        await mcp.restart().catch(() => {});
        return;
      case "call_tool": {
        const started = Date.now();
        try {
          const r = await mcp.callTool(String(m.name), (m.args ?? {}) as Record<string, unknown>);
          this.send({
            type: "call_result",
            reqId: m.reqId,
            isError: r.isError === true,
            text: resultText(r),
            ms: Date.now() - started,
          });
        } catch (e) {
          this.send({ type: "call_result", reqId: m.reqId, isError: true, text: String(e), ms: Date.now() - started });
        }
        return;
      }
      case "replay":
        await this.replay(String(m.name));
        return;
    }
  }

  private async replay(name: string) {
    if (this.chatting || this.replaying) return;
    const file = join(recordings, name);
    if (!name.endsWith(".json") || name.includes("/") || !existsSync(file)) return;
    const rec = readRecording(file);
    if (!rec) return;
    const replayId = ++replaySeq;
    const abort = new AbortController();
    this.replaying = abort;
    this.send({ type: "replay_start", prompt: rec.prompt, model: rec.model });
    let last = 0;
    for (const { t, e } of rec.events) {
      // Keep the rhythm of the original run, but never stall the audience.
      const gap = Math.min(t - last, 1500);
      last = t;
      if (gap > 0) await sleep(gap, abort.signal);
      if (abort.signal.aborted) break;
      this.send(withReplayIds(e, replayId));
    }
    // A reset during the replay already let a new chat or replay start.
    if (this.replaying !== abort) return;
    this.replaying = null;
    this.send({ type: "idle" });
  }

  private stopReplay() {
    if (!this.replaying) return;
    this.replaying.abort();
    this.replaying = null;
    this.send({ type: "idle" });
  }

  close() {
    this.agent.stop();
    this.replaying?.abort();
  }
}

function sleep(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve) => {
    const t = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(t);
      resolve();
    });
  });
}

function saveRecording(rec: Recording) {
  mkdirSync(recordings, { recursive: true });
  rec.savedAt = new Date().toISOString();
  const slug = rec.prompt.toLowerCase().replace(/[^a-z0-9]+/g, "-").slice(0, 48).replace(/-+$/, "");
  writeFileSync(join(recordings, `${rec.savedAt.replace(/[:.]/g, "-")}-${slug}.json`), JSON.stringify(rec));
}

/** Reads a recording; returns undefined for a file that is not one. */
function readRecording(file: string): Recording | undefined {
  try {
    const rec = JSON.parse(readFileSync(file, "utf8")) as Partial<Recording>;
    const ok =
      typeof rec.prompt === "string" &&
      typeof rec.model === "string" &&
      typeof rec.savedAt === "string" &&
      Array.isArray(rec.events);
    return ok ? (rec as Recording) : undefined;
  } catch {
    return undefined;
  }
}

function listRecordings() {
  if (!existsSync(recordings)) return [];
  return readdirSync(recordings)
    .filter((f) => f.endsWith(".json"))
    .flatMap((f) => {
      const rec = readRecording(join(recordings, f));
      return rec ? [{ name: f, prompt: rec.prompt, model: rec.model, savedAt: rec.savedAt }] : [];
    })
    .sort((a, b) => b.savedAt.localeCompare(a.savedAt));
}

let replaySeq = 0;

/** Gives the ids in a replayed agent event a prefix, so that two replays of
 * one recording, or a replay and a live turn, never share an id. */
function withReplayIds(msg: unknown, replayId: number): unknown {
  const m = msg as { type?: string; event?: Record<string, unknown> };
  if (m.type !== "agent" || !m.event) return msg;
  const event = { ...m.event };
  for (const key of ["turn", "block", "id"]) {
    if (typeof event[key] === "string") event[key] = `r${replayId}-${event[key]}`;
  }
  return { ...m, event };
}

const TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".css": "text/css",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".woff2": "font/woff2",
  ".json": "application/json",
};

const http = createServer((req, res) => {
  const url = new URL(req.url ?? "/", "http://x");
  if (url.pathname === "/healthz") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, mcp: mcp.status.state, clients: clients.size }));
    return;
  }
  let path = dist;
  try {
    path = join(dist, decodeURIComponent(url.pathname));
  } catch {
    // A malformed escape; serve the page.
  }
  if (!path.startsWith(dist) || !existsSync(path) || statSync(path).isDirectory()) {
    path = join(dist, "index.html");
  }
  if (!existsSync(path)) {
    res.writeHead(503, { "content-type": "text/plain" });
    res.end("The UI is not built. Run `npm run build` in demo/mcp.\n");
    return;
  }
  res.writeHead(200, { "content-type": TYPES[extname(path)] ?? "application/octet-stream", "cache-control": "no-cache" });
  createReadStream(path).pipe(res);
});

// Browsers let any page open a WebSocket to loopback, and the socket can
// call any tool, so only the demo's own page may connect. The Host check
// stops DNS rebinding; the Origin check stops other sites. A reverse proxy
// names the public host in X-Forwarded-Host, which a page cannot set.
const ALLOWED_HOSTS = new Set([
  "localhost",
  "127.0.0.1",
  "[::1]",
  ...(process.env.ALLOWED_HOSTS ?? "").split(",").map((h) => h.trim()).filter(Boolean),
]);

function hostname(hostHeader: string | undefined): string | undefined {
  try {
    return hostHeader ? new URL(`http://${hostHeader}`).hostname : undefined;
  } catch {
    return undefined;
  }
}

function allowedUpgrade(req: import("node:http").IncomingMessage): boolean {
  const host = hostname(req.headers.host);
  if (!host || !ALLOWED_HOSTS.has(host)) return false;
  const origin = req.headers.origin;
  // Only browsers send Origin; other local clients can reach the tenant anyway.
  if (!origin) return true;
  let originHost: string;
  try {
    originHost = new URL(origin).hostname;
  } catch {
    return false;
  }
  const forwarded = hostname(String(req.headers["x-forwarded-host"] ?? "").split(",")[0].trim());
  return ALLOWED_HOSTS.has(originHost) || originHost === forwarded;
}

const wss = new WebSocketServer({ noServer: true });
http.on("upgrade", (req, socket, head) => {
  if (!new URL(req.url ?? "/", "http://x").pathname.endsWith("/ws") || !allowedUpgrade(req)) {
    socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    const client = new Client(ws);
    clients.add(client);
    ws.on("message", (data) =>
      client.onMessage(data.toString()).catch((e) => console.error(`message failed: ${e}`)),
    );
    ws.on("close", () => {
      clients.delete(client);
      client.close();
    });
  });
});

// A ping keeps idle sockets alive through proxies.
setInterval(() => {
  for (const ws of wss.clients) ws.ping();
}, 20_000).unref();

mcp.connect().catch((e) => console.error(`cannot start ${bin} mcp: ${e}`));
http.listen(PORT, HOST, () => {
  console.log(`snouty mcp demo on http://${HOST}:${PORT}/ (snouty: ${bin}, tenant: ${tenantName ?? "?"})`);
});

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, async () => {
    await mcp.close();
    process.exit(0);
  });
}
