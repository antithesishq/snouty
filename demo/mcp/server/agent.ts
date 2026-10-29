// A Claude agent whose only tools are the tools of `snouty mcp`. The loop is
// a manual streaming loop so that every delta, tool call and tool result can
// go to the browser as it happens.

import Anthropic from "@anthropic-ai/sdk";
import { resultText, type SnoutyMcp } from "./mcp.ts";

export const MODELS = [
  { id: "claude-opus-5-5", label: "Claude Opus 5.5" },
  { id: "claude-sonnet-5-5", label: "Claude Sonnet 5.5" },
  { id: "claude-haiku-5-5", label: "Claude Haiku 5.5" },
] as const;

export const EFFORTS = ["low", "medium", "high"] as const;
export type Effort = (typeof EFFORTS)[number];

// src/state.ts matches these to show a call as declined or cancelled.
const DECLINED = "The user declined this call.";
const CANCELLED = "The user cancelled this call.";

/** The most tool output that goes back to the model in one result. */
const MAX_RESULT_CHARS = 60_000;

export type AgentEvent =
  | { type: "turn_start"; turn: string; model: string }
  | { type: "block_start"; turn: string; block: string; kind: "thinking" | "text" }
  | { type: "thinking_delta"; block: string; text: string }
  | { type: "text_delta"; block: string; text: string }
  | { type: "tool_pending"; turn: string; id: string; name: string }
  | { type: "tool_call"; turn: string; id: string; name: string; input: unknown }
  | { type: "approval"; id: string; name: string; input: unknown }
  | {
      type: "tool_result";
      id: string;
      isError: boolean;
      text: string;
      ms: number;
      truncated: boolean;
    }
  | { type: "usage"; turn: string; input: number; output: number; cacheRead: number; cacheWrite: number }
  | { type: "turn_end"; turn: string; stopReason: string | null }
  | { type: "error"; message: string };

const SYSTEM = `You are an Antithesis triage assistant inside a live demo of \`snouty mcp\`, the MCP server of snouty, the Antithesis CLI. Your only tools are the tools of that server. Each tool runs one snouty command against the user's Antithesis tenant and returns its JSON output.

Work like an experienced Antithesis engineer:
- Use the tools to answer. Do not guess at run IDs, property names or numbers. Read them.
- Call independent tools in parallel.
- To explain a failure, go from the run to its failing properties, to one property's examples, to the logs at the failing moment (run_id, input_hash, vtime).
- Use docs_search and docs_show for questions about Antithesis itself, and cite the doc path.
- Call tool_help when you are unsure of a tool's params or output.
- Do not call \`debug\` unless the user asks for it: it starts a session on the tenant. The user must approve each call.

Write for a live audience on a projector: short paragraphs, markdown headings and tables when they help, run IDs and property names in backticks. Lead with the answer. Each user message opens with a <context> block that gives the current time.`;

export class AgentSession {
  messages: Anthropic.MessageParam[] = [];
  model: string = MODELS[0].id;
  effort: Effort = "medium";
  private abort: AbortController | null = null;
  private approvals = new Map<string, (ok: boolean) => void>();
  private anthropic = new Anthropic();
  private turnSeq = 0;

  constructor(
    private mcp: SnoutyMcp,
    private emit: (e: AgentEvent) => void,
  ) {}

  get running() {
    return this.abort !== null;
  }

  reset() {
    this.stop();
    this.messages = [];
  }

  stop() {
    this.abort?.abort();
    for (const resolve of this.approvals.values()) resolve(false);
    this.approvals.clear();
  }

  approve(id: string, ok: boolean) {
    this.approvals.get(id)?.(ok);
    this.approvals.delete(id);
  }

  /** Runs one user turn; returns false when it failed or was stopped. */
  async send(text: string): Promise<boolean> {
    if (this.abort) throw new Error("the agent is already running");
    const abort = new AbortController();
    this.abort = abort;
    // On a failed or stopped turn, roll back to here so that the history
    // stays a valid alternation of user and assistant messages.
    const checkpoint = this.messages.length;
    // The model has no clock, and run timestamps mean little without one.
    const now = new Date().toISOString().replace(/\.\d+Z$/, "Z");
    this.messages.push({
      role: "user",
      content: [
        { type: "text", text: `<context>The time is ${now}.</context>` },
        { type: "text", text },
      ],
    });
    try {
      await this.loop(abort.signal);
      return true;
    } catch (e) {
      this.messages.length = checkpoint;
      if (abort.signal.aborted) {
        this.emit({ type: "error", message: "Stopped by the user." });
      } else {
        this.emit({ type: "error", message: describeError(e) });
      }
      return false;
    } finally {
      this.abort = null;
    }
  }

  private async loop(signal: AbortSignal) {
    const tools: Anthropic.Tool[] = this.mcp.tools.map((t) => ({
      name: t.name,
      description: t.description ?? "",
      input_schema: t.inputSchema as Anthropic.Tool.InputSchema,
    }));

    for (;;) {
      const turn = `t${++this.turnSeq}`;
      this.emit({ type: "turn_start", turn, model: this.model });
      const stream = this.anthropic.messages.stream(
        {
          model: this.model,
          max_tokens: 32_000,
          system: SYSTEM,
          tools,
          messages: this.messages,
          thinking: { type: "adaptive", display: "summarized" },
          output_config: { effort: this.effort },
          cache_control: { type: "ephemeral" },
        },
        { signal },
      );

      const blockIds = new Map<number, string>();
      for await (const ev of stream) {
        if (ev.type === "content_block_start") {
          const block = `${turn}b${ev.index}`;
          blockIds.set(ev.index, block);
          const cb = ev.content_block;
          if (cb.type === "thinking" || cb.type === "text") {
            this.emit({ type: "block_start", turn, block, kind: cb.type });
          } else if (cb.type === "tool_use") {
            this.emit({ type: "tool_pending", turn, id: cb.id, name: cb.name });
          }
        } else if (ev.type === "content_block_delta") {
          const block = blockIds.get(ev.index)!;
          if (ev.delta.type === "thinking_delta") {
            this.emit({ type: "thinking_delta", block, text: ev.delta.thinking });
          } else if (ev.delta.type === "text_delta") {
            this.emit({ type: "text_delta", block, text: ev.delta.text });
          }
        }
      }
      const message = await stream.finalMessage();
      this.messages.push({ role: "assistant", content: message.content });
      const u = message.usage;
      this.emit({
        type: "usage",
        turn,
        input: u.input_tokens,
        output: u.output_tokens,
        cacheRead: u.cache_read_input_tokens ?? 0,
        cacheWrite: u.cache_creation_input_tokens ?? 0,
      });

      const calls = message.content.filter((b): b is Anthropic.ToolUseBlock => b.type === "tool_use");
      if (message.stop_reason !== "tool_use" || calls.length === 0) {
        this.emit({ type: "turn_end", turn, stopReason: message.stop_reason });
        return;
      }
      for (const call of calls) {
        this.emit({ type: "tool_call", turn, id: call.id, name: call.name, input: call.input });
      }
      const results = await Promise.all(calls.map((call) => this.runTool(call, signal)));
      if (signal.aborted) throw new Error("aborted");
      this.messages.push({ role: "user", content: results });
      this.emit({ type: "turn_end", turn, stopReason: "tool_use" });
    }
  }

  private async runTool(
    call: Anthropic.ToolUseBlock,
    signal: AbortSignal,
  ): Promise<Anthropic.ToolResultBlockParam> {
    const started = Date.now();
    const finish = (text: string, isError: boolean): Anthropic.ToolResultBlockParam => {
      const truncated = text.length > MAX_RESULT_CHARS;
      const forModel = truncated
        ? `${text.slice(0, MAX_RESULT_CHARS)}\n\n[output truncated: ${text.length} chars in total. Narrow the query to see the rest.]`
        : text;
      this.emit({ type: "tool_result", id: call.id, isError, text, ms: Date.now() - started, truncated });
      return { type: "tool_result", tool_use_id: call.id, content: forModel, is_error: isError };
    };

    // The server marks the tools that only read with readOnlyHint. The user
    // approves each call of any other tool.
    const tool = this.mcp.tools.find((t) => t.name === call.name);
    if (tool?.annotations?.readOnlyHint !== true) {
      this.emit({ type: "approval", id: call.id, name: call.name, input: call.input });
      const ok = await new Promise<boolean>((resolve) => this.approvals.set(call.id, resolve));
      if (!ok) return finish(DECLINED, true);
    }

    // The MCP SDK keeps its abort listener after a response, so a turn-wide
    // signal would cancel calls that already finished. Each call gets its own.
    const callAbort = new AbortController();
    const cancel = () => callAbort.abort("stopped by the user");
    signal.addEventListener("abort", cancel, { once: true });
    try {
      const result = await this.mcp.callTool(call.name, call.input as Record<string, unknown>, callAbort.signal);
      return finish(resultText(result), result.isError === true);
    } catch (e) {
      if (callAbort.signal.aborted) return finish(CANCELLED, true);
      return finish(`The MCP call failed: ${describeError(e)}`, true);
    } finally {
      signal.removeEventListener("abort", cancel);
    }
  }
}

function describeError(e: unknown): string {
  if (e instanceof Anthropic.APIError) return `Claude API error ${e.status ?? ""}: ${e.message}`;
  if (e instanceof Error) return e.message;
  return String(e);
}
