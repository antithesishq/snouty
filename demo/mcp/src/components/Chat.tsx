import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { Button, Card, Expander, Icon, Input, Loader, Menu, MenuItem, Tag, Text, Title, Tooltip } from "@antadesign/anta/bundle";
import type { ChatItem, Recording, State } from "../state.ts";
import { Code } from "./Code.tsx";
import { Markdown } from "./Markdown.tsx";

export const SUGGESTIONS = [
  {
    icon: "runs-history",
    title: "What's live right now?",
    prompt: "What's running on the tenant right now? Give me a quick table of the in-progress runs and how long each has been going.",
  },
  {
    icon: "bug",
    title: "Triage a failed run",
    prompt:
      "Why did the most recent Hashicorp Raft run fail? Start from its failing properties, pick the most serious safety violation, and dig down to the logs at the failing moment.",
  },
  {
    icon: "timer",
    title: "Where did the compute go?",
    prompt: "How many core-hours have we used over the last 7 days, and which runs used the most?",
  },
  {
    icon: "book-open",
    title: "Ask the docs",
    prompt: "How do I write a Sometimes assertion with the Go SDK, and when should I use it instead of Always? Cite the docs.",
  },
] as const;

interface Props {
  state: State;
  onSend: (text: string) => void;
  onStop: () => void;
  onApprove: (id: string, ok: boolean) => void;
  onReplay: (name: string) => void;
}

function argSummary(input?: Record<string, unknown>) {
  if (!input) return "";
  return Object.entries(input)
    .map(([k, v]) => {
      let s = typeof v === "string" ? v : JSON.stringify(v);
      if (s.length > 28) s = `${s.slice(0, 26)}…`;
      return `${k}=${s}`;
    })
    .join("  ");
}

function Elapsed({ since }: { since: number }) {
  const [, tick] = useState(0);
  useEffect(() => {
    const t = setInterval(() => tick((x) => x + 1), 100);
    return () => clearInterval(t);
  }, []);
  return <>{((Date.now() - since) / 1000).toFixed(1)}s</>;
}

function ToolCall({ item, onApprove }: { item: Extract<ChatItem, { kind: "tool" }>; onApprove: Props["onApprove"] }) {
  const live = item.status === "pending" || item.status === "running";
  const icon = live ? (
    <Loader size={14} tone="brand" />
  ) : item.status === "ok" ? (
    <Icon shape="circle-check" className="ok-icon" />
  ) : item.status === "approval" ? (
    <Icon shape="hat-glasses" className="warn-icon" />
  ) : item.status === "cancelled" ? (
    <Icon shape="x" className="muted-icon" />
  ) : (
    <Icon shape="warning-diamond" className="err-icon" />
  );
  const tone = item.status === "error" || item.status === "declined" ? "critical" : item.status === "approval" ? "warning" : undefined;

  return (
    <div className={`tool-call status-${item.status}`} data-testid="tool-call" data-tool={item.name}>
      <Expander
        priority="secondary"
        tone={tone}
        round={10}
        title={
          <span className="tool-call-title">
            {icon}
            <code className="tool-call-name">{item.name}</code>
            <span className="tool-call-args">{argSummary(item.input)}</span>
          </span>
        }
        actions={
          <span className="tool-call-meta">
            {item.status === "running" && (
              <Text size="small" priority="tertiary">
                <Elapsed since={item.started} />
              </Text>
            )}
            {item.ms !== undefined && <Tag size="small" value={`${(item.ms / 1000).toFixed(2)}s`} />}
            {item.status === "error" && <Tag size="small" tone="critical" value="isError" />}
            {item.status === "declined" && <Tag size="small" tone="critical" value="declined" />}
            {item.status === "cancelled" && <Tag size="small" value="cancelled" />}
            {item.truncated && <Tag size="small" tone="warning" value="truncated for model" />}
          </span>
        }
      >
        <div className="tool-call-body">
          {item.command && (
            <div className="kv">
              <Text size="small" priority="tertiary">
                snouty ran
              </Text>
              <code className="cmd">$ {item.command}</code>
            </div>
          )}
          <div className="kv">
            <Text size="small" priority="tertiary">
              arguments
            </Text>
            <Code text={JSON.stringify(item.input ?? {})} />
          </div>
          {item.result !== undefined && (
            <div className="kv">
              <Text size="small" priority="tertiary">
                result
              </Text>
              <Code text={item.result} maxHeight={360} />
            </div>
          )}
        </div>
      </Expander>
      {item.status === "approval" && (
        <Card tone="warning" size="small" className="approval" data-testid="approval">
          <div className="approval-row">
            <Text size="small">
              Claude wants to call <code>{item.name}</code>. It starts a session on the tenant.
            </Text>
            <span className="approval-buttons">
              <Button size="small" label="Decline" onClick={() => onApprove(item.id, false)} />
              <Button size="small" priority="primary" tone="warning" label="Approve" onClick={() => onApprove(item.id, true)} />
            </span>
          </div>
        </Card>
      )}
    </div>
  );
}

function Thinking({ item }: { item: Extract<ChatItem, { kind: "thinking" }> }) {
  if (item.done && !item.text.trim()) return null;
  return (
    <div className="thinking" data-testid="thinking">
      <Expander
        priority="tertiary"
        outdent
        title={
          <span className="thinking-title">
            {item.done ? <Icon shape="sparkles" /> : <Loader size={14} />}
            <Text size="small" priority="tertiary">
              {item.done ? "Thought" : "Thinking…"}
            </Text>
          </span>
        }
      >
        <Text size="small" priority="tertiary" className="thinking-text">
          {item.text || "…"}
        </Text>
      </Expander>
    </div>
  );
}

function EmptyState({ state, onSend, onReplay }: Pick<Props, "state" | "onSend" | "onReplay">) {
  const ready = state.mcp.state === "connected";
  return (
    <div className="empty">
      <div className="empty-hero">
        <Tag tone="brand" icon="sparkles" label="Live demo" />
        <Title level={1}>Ask your Antithesis tenant anything.</Title>
        <Text size="large" priority="secondary">
          Claude is connected to <code>snouty mcp</code> over stdio. Each tool call is a real snouty command against{" "}
          {state.tenant ? <code>{state.tenant}</code> : "your tenant"}. Watch the JSON-RPC on the right.
        </Text>
      </div>
      <div className="suggestions">
        {SUGGESTIONS.map((s) => (
          <button
            key={s.title}
            type="button"
            className="suggestion"
            disabled={!ready}
            onClick={() => onSend(s.prompt)}
            data-testid="suggestion"
          >
            <Card icon={s.icon} header={s.title} size="small">
              {s.prompt}
            </Card>
          </button>
        ))}
      </div>
      {state.recordings.length > 0 && <ReplayMenu recordings={state.recordings} onReplay={onReplay} />}
    </div>
  );
}

function ReplayMenu({ recordings, onReplay }: { recordings: Recording[]; onReplay: (name: string) => void }) {
  return (
    <div className="replay">
      <Button size="small" priority="tertiary" icon="history" label={`Replay a recorded session (${recordings.length})`} data-testid="replay-trigger" />
      <Menu placement="bottom-start" autoWidth>
        {recordings.slice(0, 12).map((r) => (
          <MenuItem
            key={r.name}
            value={r.name}
            label={r.prompt.length > 70 ? `${r.prompt.slice(0, 68)}…` : r.prompt}
            hint={`${new Date(r.savedAt).toLocaleString()} · ${r.model}`}
            onSelect={() => onReplay(r.name)}
          />
        ))}
      </Menu>
    </div>
  );
}

export function Chat({ state, onSend, onStop, onApprove, onReplay }: Props) {
  const [draft, setDraft] = useState("");
  const scroller = useRef<HTMLDivElement>(null);
  const stick = useRef(true);

  useLayoutEffect(() => {
    const el = scroller.current;
    if (el && stick.current) el.scrollTop = el.scrollHeight;
  });

  const send = () => {
    const text = draft.trim();
    if (!text || state.busy) return;
    stick.current = true;
    onSend(text);
    setDraft("");
  };

  const ready = state.socket === "open" && state.mcp.state === "connected";
  const last = state.chat[state.chat.length - 1];
  const waiting = state.busy && (!last || last.kind === "user" || (last.kind === "tool" && last.status !== "approval" && last.status !== "running" && last.status !== "pending"));

  return (
    <main className="pane chat-pane" aria-label="Chat">
      <div
        className="chat-scroll"
        ref={scroller}
        onScroll={(e) => {
          const el = e.currentTarget;
          stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
        }}
        data-testid="chat-scroll"
      >
        {state.chat.length === 0 ? (
          <EmptyState state={state} onSend={onSend} onReplay={onReplay} />
        ) : (
          <div className="messages">
            {state.chat.map((item) => {
              switch (item.kind) {
                case "user":
                  return (
                    <div key={item.id} className="msg-user" data-testid="user-message">
                      <div className="bubble">
                        <Text>{item.text}</Text>
                      </div>
                      {item.replay && (
                        <Text size="small" priority="quaternary">
                          replayed from a recording
                        </Text>
                      )}
                    </div>
                  );
                case "thinking":
                  return <Thinking key={item.id} item={item} />;
                case "text":
                  return item.text ? (
                    <div key={item.id} className="msg-agent" data-testid="agent-text">
                      <Markdown text={item.text} />
                    </div>
                  ) : null;
                case "tool":
                  return <ToolCall key={item.id} item={item} onApprove={onApprove} />;
                case "error": {
                  const stopped = item.text === "Stopped by the user.";
                  return (
                    <div key={item.id} className={`chat-notice${stopped ? "" : " is-error"}`} data-testid="chat-error">
                      <Icon shape={stopped ? "x" : "warning-diamond"} />
                      <Text size="small" tone={stopped ? undefined : "critical"} priority={stopped ? "tertiary" : "secondary"}>
                        {item.text}
                      </Text>
                    </div>
                  );
                }
              }
            })}
            {waiting && (
              <div className="working">
                <Loader size={16} tone="brand" />
                <Text size="small" priority="tertiary">
                  Claude is working…
                </Text>
              </div>
            )}
            {!state.busy && state.lastTurnMs !== undefined && (
              <div className="turn-stats" data-testid="turn-stats">
                <Text size="small" priority="quaternary">
                  {(state.lastTurnMs / 1000).toFixed(1)}s · {state.chat.filter((c) => c.kind === "tool").length} tool calls ·{" "}
                  {state.usage.requests} model requests · {Math.round((state.usage.input + state.usage.cacheRead + state.usage.cacheWrite) / 1000)}k in /{" "}
                  {(state.usage.output / 1000).toFixed(1)}k out tokens
                </Text>
              </div>
            )}
          </div>
        )}
      </div>

      <div className="composer">
        <Input
          multiline
          maxRows={6}
          size="large"
          placeholder={ready ? "Ask about your runs, a failure, or the docs…" : "Waiting for snouty mcp…"}
          aria-label="Message"
          value={draft}
          disabled={!ready && !state.busy}
          onInput={(e) => setDraft((e.target as HTMLInputElement).value)}
          onKeyDown={(e: React.KeyboardEvent) => {
            if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
              e.preventDefault();
              send();
            }
          }}
          data-testid="composer"
        />
        {state.busy ? (
          <Button size="large" icon="x" label="Stop" tone="critical" onClick={onStop} data-testid="stop">
            <Tooltip>Cancels the model stream and any MCP call in flight</Tooltip>
          </Button>
        ) : (
          <Button size="large" priority="primary" icon="send" label="Send" disabled={!ready || !draft.trim()} onClick={send} data-testid="send" />
        )}
      </div>
    </main>
  );
}
