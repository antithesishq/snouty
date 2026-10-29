// Browser state: one reducer folds every server message into the chat, the
// wire log and the tool stats.

export interface Tool {
  name: string;
  description?: string;
  inputSchema: {
    properties?: Record<string, JsonSchema>;
    required?: string[];
  };
  annotations?: { readOnlyHint?: boolean; destructiveHint?: boolean };
}

export interface JsonSchema {
  type?: string;
  description?: string;
  enum?: string[];
  default?: unknown;
  items?: { type?: string };
}

/** The agent asks the user before it calls a tool that is not marked read-only. */
export function needsApproval(tool: Tool) {
  return tool.annotations?.readOnlyHint !== true;
}

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

export type McpStatus =
  | { state: "connecting" }
  | { state: "connected"; info: ServerInfo }
  | { state: "disconnected"; error?: string };

export interface Recording {
  name: string;
  prompt: string;
  model: string;
  savedAt: string;
}

export type ToolStatus = "pending" | "approval" | "running" | "ok" | "error" | "declined" | "cancelled";

// The tool results that server/agent.ts writes for calls that did not run.
const DECLINED = "The user declined this call.";
const CANCELLED = "The user cancelled this call.";

export type ChatItem =
  | { kind: "user"; id: string; text: string; replay?: boolean }
  | { kind: "thinking"; id: string; text: string; done: boolean }
  | { kind: "text"; id: string; text: string }
  | {
      kind: "tool";
      id: string;
      name: string;
      input?: Record<string, unknown>;
      status: ToolStatus;
      result?: string;
      ms?: number;
      truncated?: boolean;
      command?: string;
      started: number;
    }
  | { kind: "error"; id: string; text: string };

export interface WireEntry {
  seq: number;
  ts: number;
  kind: "request" | "response" | "notification" | "error" | "log" | "note";
  dir: "out" | "in" | "log";
  method?: string;
  rpcId?: string | number;
  latency?: number;
  msg?: unknown;
  line?: string;
}

interface Usage {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  requests: number;
}

export interface State {
  socket: "connecting" | "open" | "closed";
  mcp: McpStatus;
  tenant?: string;
  models: Array<{ id: string; label: string }>;
  model: string;
  effort: string;
  recordings: Recording[];
  chat: ChatItem[];
  busy: boolean;
  replaying: boolean;
  wire: WireEntry[];
  wireSeq: number;
  pendingRequests: Record<string, { ts: number; method: string }>;
  toolCounts: Record<string, number>;
  usage: Usage;
  turnStarted?: number;
  lastTurnMs?: number;
}

export const initialState: State = {
  socket: "connecting",
  mcp: { state: "connecting" },
  models: [],
  model: "",
  effort: "medium",
  recordings: [],
  chat: [],
  busy: false,
  replaying: false,
  wire: [],
  wireSeq: 0,
  pendingRequests: {},
  toolCounts: {},
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, requests: 0 },
};

const MAX_WIRE = 600;

export type Action =
  | { type: "socket"; state: State["socket"] }
  | { type: "server"; msg: ServerMsg }
  | { type: "local_user"; text: string }
  | { type: "local_settings"; model?: string; effort?: string }
  | { type: "clear_chat" };

// Messages from the demo server. See server/index.ts.
export type ServerMsg = { type: string; [k: string]: unknown };

type AgentEvent = { type: string; [k: string]: unknown };

function updateTool(chat: ChatItem[], id: string, f: (t: Extract<ChatItem, { kind: "tool" }>) => ChatItem) {
  return chat.map((c) => (c.kind === "tool" && c.id === id ? f(c) : c));
}

/** The snouty subcommand that a tool runs: `runs_properties` → `runs properties`. */
function commandWords(tool: string) {
  return tool.replace(/_/g, " ");
}

/** The debug line that `snouty mcp` logs for each command it runs. */
export const RUNNING_CMD = /running: (snouty .*)$/;

function finishThinking(chat: ChatItem[]): ChatItem[] {
  return chat.map((c) => (c.kind === "thinking" ? { ...c, done: true } : c));
}

function agent(state: State, e: AgentEvent): State {
  switch (e.type) {
    case "turn_start":
      return { ...state, busy: true, turnStarted: state.turnStarted ?? Date.now() };
    case "block_start": {
      const kind = e.kind as "thinking" | "text";
      const item: ChatItem =
        kind === "thinking"
          ? { kind, id: String(e.block), text: "", done: false }
          : { kind, id: String(e.block), text: "" };
      return { ...state, chat: [...state.chat, item] };
    }
    case "thinking_delta":
    case "text_delta": {
      const id = String(e.block);
      const text = String(e.text);
      return {
        ...state,
        chat: state.chat.map((c) =>
          (c.kind === "thinking" || c.kind === "text") && c.id === id ? { ...c, text: c.text + text } : c,
        ),
      };
    }
    case "tool_pending": {
      return {
        ...state,
        chat: [...finishThinking(state.chat), { kind: "tool", id: String(e.id), name: String(e.name), status: "pending", started: Date.now() }],
      };
    }
    case "tool_call": {
      const name = String(e.name);
      return {
        ...state,
        toolCounts: { ...state.toolCounts, [name]: (state.toolCounts[name] ?? 0) + 1 },
        chat: updateTool(state.chat, String(e.id), (t) => ({
          ...t,
          input: e.input as Record<string, unknown>,
          status: t.status === "pending" ? "running" : t.status,
          started: Date.now(),
        })),
      };
    }
    case "approval":
      return { ...state, chat: updateTool(state.chat, String(e.id), (t) => ({ ...t, status: "approval" })) };
    case "tool_result": {
      const text = String(e.text);
      return {
        ...state,
        chat: updateTool(state.chat, String(e.id), (t) => ({
          ...t,
          status: text === DECLINED ? "declined" : text === CANCELLED ? "cancelled" : e.isError ? "error" : "ok",
          result: text,
          ms: Number(e.ms),
          truncated: Boolean(e.truncated),
        })),
      };
    }
    case "usage":
      return {
        ...state,
        usage: {
          input: state.usage.input + Number(e.input),
          output: state.usage.output + Number(e.output),
          cacheRead: state.usage.cacheRead + Number(e.cacheRead),
          cacheWrite: state.usage.cacheWrite + Number(e.cacheWrite),
          requests: state.usage.requests + 1,
        },
      };
    case "turn_end":
      return { ...state, chat: finishThinking(state.chat) };
    case "error":
      return {
        ...state,
        chat: [
          ...state.chat.map((c) =>
            c.kind === "thinking"
              ? { ...c, done: true }
              : c.kind === "tool" && ["pending", "running", "approval"].includes(c.status)
                ? { ...c, status: "cancelled" as const, result: c.result ?? String(e.message) }
                : c,
          ),
          { kind: "error", id: `err${Date.now()}`, text: String(e.message) },
        ],
      };
    default:
      return state;
  }
}

function wire(state: State, e: { kind: string; dir?: "out" | "in"; msg?: Record<string, unknown>; line?: string; ts: number }): State {
  const seq = state.wireSeq + 1;
  let entry: WireEntry;
  let pending = state.pendingRequests;
  let chat = state.chat;

  if (e.kind === "note") {
    entry = { seq, ts: e.ts, kind: "note", dir: "log", line: e.line };
  } else if (e.kind === "stderr") {
    entry = { seq, ts: e.ts, kind: "log", dir: "log", line: e.line };
    // `running: snouty --json runs list ...` names the command of a tool
    // call. Give it to the oldest running call of that tool that has none.
    const m = RUNNING_CMD.exec(e.line ?? "");
    if (m) {
      const cmd = m[1];
      const target = chat.find(
        (c) => c.kind === "tool" && !c.command && c.status === "running" && new RegExp(` ${commandWords(c.name)}( |$)`).test(cmd),
      );
      if (target) chat = chat.map((c) => (c === target ? { ...c, command: cmd } : c));
    }
  } else {
    const msg = e.msg ?? {};
    const id = msg.id as string | number | undefined;
    const method = msg.method as string | undefined;
    const key = `${e.dir}:${id}`;
    if (method && id !== undefined) {
      entry = { seq, ts: e.ts, kind: "request", dir: e.dir!, method, rpcId: id, msg };
      pending = { ...pending, [key]: { ts: e.ts, method } };
    } else if (method) {
      entry = { seq, ts: e.ts, kind: "notification", dir: e.dir!, method, msg };
    } else {
      const reqKey = `${e.dir === "in" ? "out" : "in"}:${id}`;
      const req = pending[reqKey];
      if (req) {
        pending = { ...pending };
        delete pending[reqKey];
      }
      entry = {
        seq,
        ts: e.ts,
        kind: "error" in msg ? "error" : "response",
        dir: e.dir!,
        method: req?.method,
        rpcId: id,
        latency: req ? e.ts - req.ts : undefined,
        msg,
      };
    }
  }
  const all = [...state.wire, entry];
  return {
    ...state,
    chat,
    wireSeq: seq,
    pendingRequests: pending,
    wire: all.length > MAX_WIRE ? all.slice(all.length - MAX_WIRE) : all,
  };
}

export function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "socket":
      return action.state === "closed" ? { ...state, socket: "closed", busy: false } : { ...state, socket: action.state };
    case "local_user":
      return {
        ...state,
        busy: true,
        turnStarted: Date.now(),
        chat: [...state.chat, { kind: "user", id: `u${Date.now()}`, text: action.text }],
      };
    case "local_settings":
      return { ...state, model: action.model ?? state.model, effort: action.effort ?? state.effort };
    case "clear_chat":
      return {
        ...state,
        chat: [],
        busy: false,
        replaying: false,
        toolCounts: {},
        usage: initialState.usage,
        turnStarted: undefined,
        lastTurnMs: undefined,
      };
    case "server": {
      const m = action.msg;
      switch (m.type) {
        case "hello":
          return {
            ...state,
            mcp: m.status as McpStatus,
            tenant: m.tenant as string | undefined,
            models: m.models as State["models"],
            model: m.model as string,
            effort: m.effort as string,
            recordings: m.recordings as Recording[],
          };
        case "mcp_status":
          return { ...state, mcp: m.status as McpStatus };
        case "recordings":
          return { ...state, recordings: m.recordings as Recording[] };
        case "agent":
          return agent(state, m.event as AgentEvent);
        case "wire":
          return wire(state, m.event as Parameters<typeof wire>[1]);
        case "replay_start":
          return {
            ...state,
            busy: true,
            replaying: true,
            turnStarted: Date.now(),
            chat: [...state.chat, { kind: "user", id: `u${Date.now()}`, text: String(m.prompt), replay: true }],
          };
        case "idle":
          return {
            ...state,
            busy: false,
            replaying: false,
            lastTurnMs: state.turnStarted ? Date.now() - state.turnStarted : state.lastTurnMs,
            turnStarted: undefined,
          };
        default:
          return state;
      }
    }
  }
}
