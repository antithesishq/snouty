// One `snouty mcp` child process, reached over stdio through the official MCP
// client. Every JSON-RPC message in either direction, and every stderr line,
// goes to `onEvent` so the browser can show the wire.

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import type { Transport, TransportSendOptions } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage, Tool } from "@modelcontextprotocol/sdk/types.js";

export type WireEvent =
  | { kind: "wire"; dir: "out" | "in"; msg: JSONRPCMessage; ts: number }
  | { kind: "stderr"; line: string; ts: number }
  | { kind: "note"; line: string; ts: number }
  | { kind: "status"; status: McpStatus; ts: number };

export type McpStatus =
  | { state: "connecting" }
  | { state: "connected"; info: ServerInfo }
  | { state: "disconnected"; error?: string };

export interface ServerInfo {
  name: string;
  version: string;
  protocolVersion?: string;
  instructions?: string;
  capabilities: unknown;
  tools: Tool[];
  command: string;
  pid?: number;
}

/** Forwards to the stdio transport and reports each message it carries. */
class TapTransport implements Transport {
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: Transport["onmessage"];

  constructor(
    private inner: StdioClientTransport,
    private tap: (dir: "out" | "in", msg: JSONRPCMessage) => void,
  ) {}

  async start() {
    this.inner.onmessage = (msg) => {
      this.tap("in", msg);
      this.onmessage?.(msg);
    };
    this.inner.onclose = () => this.onclose?.();
    this.inner.onerror = (e) => this.onerror?.(e);
    await this.inner.start();
  }

  async send(msg: JSONRPCMessage, _options?: TransportSendOptions) {
    this.tap("out", msg);
    await this.inner.send(msg);
  }

  async close() {
    await this.inner.close();
  }
}

export class SnoutyMcp {
  client: Client | null = null;
  status: McpStatus = { state: "disconnected" };
  private connecting: Promise<void> | null = null;

  constructor(
    private bin: string,
    private onEvent: (e: WireEvent) => void,
  ) {}

  private setStatus(status: McpStatus) {
    this.status = status;
    this.onEvent({ kind: "status", status, ts: Date.now() });
  }

  connect(): Promise<void> {
    this.connecting ??= this.doConnect().finally(() => {
      this.connecting = null;
    });
    return this.connecting;
  }

  private async doConnect() {
    this.setStatus({ state: "connecting" });
    const inner = new StdioClientTransport({
      command: this.bin,
      args: ["mcp"],
      env: {
        ...(process.env as Record<string, string>),
        SNOUTY_UNSTABLE_FEATURES: "mcp",
        // The server logs each command line it runs at debug level.
        RUST_LOG: "snouty::mcp=debug",
      },
      stderr: "pipe",
    });
    let buf = "";
    inner.stderr?.on("data", (chunk: Buffer) => {
      buf += chunk.toString();
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl).trimEnd();
        buf = buf.slice(nl + 1);
        if (line) this.onEvent({ kind: "stderr", line: stripAnsi(line), ts: Date.now() });
      }
    });

    let protocolVersion: string | undefined;
    const transport = new TapTransport(inner, (dir, msg) => {
      const result = "result" in msg ? (msg.result as { protocolVersion?: unknown }) : undefined;
      if (typeof result?.protocolVersion === "string") protocolVersion = result.protocolVersion;
      this.onEvent({ kind: "wire", dir, msg, ts: Date.now() });
    });
    const client = new Client({ name: "snouty-mcp-demo", version: "1.0.0" });
    client.onclose = () => {
      // `restart` and `close` clear `this.client` first; anything else is a
      // crash, so start a new server.
      if (this.client === client) {
        this.client = null;
        this.setStatus({ state: "disconnected", error: "snouty mcp exited" });
        this.onEvent({ kind: "note", line: "snouty mcp exited; restarting it", ts: Date.now() });
        setTimeout(() => void this.connect().catch(() => {}), 1500);
      }
    };
    try {
      await client.connect(transport);
      const { tools } = await client.listTools();
      const version = client.getServerVersion();
      this.client = client;
      this.onEvent({ kind: "note", line: `started snouty mcp (pid ${inner.pid})`, ts: Date.now() });
      this.setStatus({
        state: "connected",
        info: {
          name: version?.name ?? "snouty",
          version: version?.version ?? "?",
          protocolVersion,
          instructions: client.getInstructions(),
          capabilities: client.getServerCapabilities(),
          tools,
          command: `SNOUTY_UNSTABLE_FEATURES=mcp ${this.bin} mcp`,
          pid: inner.pid ?? undefined,
        },
      });
    } catch (e) {
      await client.close().catch(() => {});
      this.setStatus({ state: "disconnected", error: String(e) });
      throw e;
    }
  }

  async restart() {
    const old = this.client;
    this.client = null;
    await old?.close().catch(() => {});
    await this.connect();
  }

  async close() {
    const old = this.client;
    this.client = null;
    await old?.close().catch(() => {});
  }

  get tools(): Tool[] {
    return this.status.state === "connected" ? this.status.info.tools : [];
  }

  async callTool(name: string, args: Record<string, unknown>, signal?: AbortSignal) {
    if (!this.client) await this.connect();
    const client = this.client!;
    return client.callTool({ name, arguments: args }, undefined, {
      signal,
      // `runs_wait` and `runs_logs` can take minutes; the SDK default is 60s.
      timeout: 10 * 60_000,
    });
  }
}

function stripAnsi(s: string) {
  // eslint-disable-next-line no-control-regex
  return s.replace(/\x1b\[[0-9;]*m/g, "");
}
