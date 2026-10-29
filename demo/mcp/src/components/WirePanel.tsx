import { useLayoutEffect, useRef, useState } from "react";
import { Icon, Switch, TabPanel, Tabs, Tag, Text, Title } from "@antadesign/anta/bundle";
import type { State, WireEntry } from "../state.ts";
import { Code } from "./Code.tsx";

function time(ts: number) {
  const d = new Date(ts);
  return `${d.toLocaleTimeString([], { hour12: false })}.${String(d.getMilliseconds()).padStart(3, "0")}`;
}

/** A one-line gist of the message: the tool name of a call, the size of a result. */
function gist(e: WireEntry): string {
  const msg = e.msg as { params?: Record<string, unknown>; result?: Record<string, unknown>; error?: { message?: string } } | undefined;
  if (!msg) return "";
  if (e.method === "tools/call" && e.kind === "request") return String(msg.params?.name ?? "");
  if (e.method === "notifications/cancelled") return String(msg.params?.reason ?? "");
  if (e.kind === "error") return msg.error?.message ?? "";
  if (e.kind === "response" && msg.result) {
    if (e.method === "tools/list") return `${(msg.result.tools as unknown[] | undefined)?.length ?? 0} tools`;
    if (e.method === "initialize") {
      const info = msg.result.serverInfo as { name?: string; version?: string } | undefined;
      return `${info?.name ?? ""} ${info?.version ?? ""}`;
    }
    if (e.method === "tools/call") {
      const content = (msg.result.content as Array<{ text?: string }> | undefined) ?? [];
      const bytes = content.reduce((n, c) => n + (c.text?.length ?? 0), 0);
      return `${msg.result.isError ? "isError · " : ""}${bytes < 1024 ? `${bytes} B` : `${(bytes / 1024).toFixed(1)} KB`}`;
    }
  }
  return "";
}

function WireRow({ e }: { e: WireEntry }) {
  const [open, setOpen] = useState(false);
  if (e.kind === "note") {
    return (
      <div className="wire-note" data-testid="wire-note">
        <Text size="small" priority="tertiary">
          {time(e.ts)} · {e.line}
        </Text>
      </div>
    );
  }
  if (e.kind === "log") {
    const m = /running: (snouty .*)$/.exec(e.line ?? "");
    return (
      <div className={`wire-row wire-log${m ? " is-cmd" : ""}`} data-testid="wire-log">
        <span className="wire-time">{time(e.ts)}</span>
        <span className="wire-dir log" aria-label="server log">
          <Icon shape="scroll-text" />
        </span>
        <code className="wire-line">{m ? `$ ${m[1]}` : e.line}</code>
      </div>
    );
  }
  const out = e.dir === "out";
  return (
    <div className={`wire-entry${open ? " open" : ""}`} data-testid="wire-entry" data-kind={e.kind}>
      <button type="button" className={`wire-row kind-${e.kind}`} onClick={() => setOpen(!open)} aria-expanded={open}>
        <span className="wire-time">{time(e.ts)}</span>
        <span className={`wire-dir ${out ? "out" : "in"}`} aria-label={out ? "client to server" : "server to client"}>
          <Icon shape={out ? "arrow-right" : "arrow-left"} />
        </span>
        <code className="wire-method">{e.method ?? "response"}</code>
        {e.rpcId !== undefined && <span className="wire-id">#{e.rpcId}</span>}
        <span className="wire-gist">{gist(e)}</span>
        {e.latency !== undefined && (
          <Tag size="small" tone={e.kind === "error" ? "critical" : undefined} value={e.latency < 1000 ? `${e.latency}ms` : `${(e.latency / 1000).toFixed(1)}s`} />
        )}
      </button>
      {open && (
        <div className="wire-json">
          <Code text={JSON.stringify(e.msg)} maxHeight={320} />
        </div>
      )}
    </div>
  );
}

function Wire({ entries }: { entries: WireEntry[] }) {
  const [logs, setLogs] = useState(true);
  const scroller = useRef<HTMLDivElement>(null);
  const stick = useRef(true);
  const shown = logs ? entries : entries.filter((e) => e.kind !== "log");
  const messages = entries.filter((e) => e.kind !== "log" && e.kind !== "note").length;
  const calls = entries.filter((e) => e.method === "tools/call" && e.kind === "request").length;
  const answered = entries.filter((e) => e.method === "tools/call" && e.latency !== undefined);
  const avg = answered.length ? answered.reduce((n, e) => n + (e.latency ?? 0), 0) / answered.length : 0;

  useLayoutEffect(() => {
    const el = scroller.current;
    if (el && stick.current) el.scrollTop = el.scrollHeight;
  });

  return (
    <div className="wire">
      <div className="wire-toolbar">
        <span className="wire-stats">
          <Tag size="small" label="messages" value={String(messages)} />
          <Tag size="small" label="tool calls" value={String(calls)} />
          {answered.length > 0 && <Tag size="small" label="avg" value={`${(avg / 1000).toFixed(2)}s`} />}
        </span>
        <Switch size="small" label="server log" checked={logs} onStateChange={(_e, { next }) => setLogs(next)} />
      </div>
      <div
        className="wire-scroll"
        ref={scroller}
        onScroll={(e) => {
          const el = e.currentTarget;
          stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 60;
        }}
        data-testid="wire-scroll"
      >
        {shown.length === 0 ? (
          <div className="wire-empty">
            <Text size="small" priority="tertiary">
              No traffic yet. Ask a question, or restart the server to watch the handshake.
            </Text>
          </div>
        ) : (
          shown.map((e) => <WireRow key={e.seq} e={e} />)
        )}
      </div>
    </div>
  );
}

function Server({ state }: { state: State }) {
  const { mcp } = state;
  if (mcp.state !== "connected") {
    return (
      <div className="server-info">
        <Text priority="tertiary">{mcp.state === "connecting" ? "Starting snouty mcp…" : `Not connected. ${mcp.error ?? ""}`}</Text>
      </div>
    );
  }
  const { info } = mcp;
  return (
    <div className="server-info">
      <dl className="facts">
        <dt>server</dt>
        <dd>
          <code>
            {info.name} {info.version}
          </code>
        </dd>
        <dt>protocol</dt>
        <dd>
          <code>{info.protocolVersion ?? "?"}</code>
        </dd>
        <dt>transport</dt>
        <dd>stdio{info.pid ? <> · pid <code>{info.pid}</code></> : null}</dd>
        <dt>command</dt>
        <dd>
          <code className="cmd">{info.command}</code>
        </dd>
        <dt>tools</dt>
        <dd>
          {info.tools.length} ({info.tools.filter((t) => t.annotations?.readOnlyHint).length} marked read-only)
        </dd>
      </dl>
      <Title level={6} font="sans-serif">
        Instructions
      </Title>
      <Text size="small" priority="secondary">
        {info.instructions}
      </Text>
      <Title level={6} font="sans-serif">
        Capabilities
      </Title>
      <Code text={JSON.stringify(info.capabilities)} />
    </div>
  );
}

export function WirePanel({ state }: { state: State }) {
  return (
    <aside className="pane wire-pane" aria-label="MCP inspector">
      <Tabs
        size="small"
        priority="tertiary"
        defaultValue="wire"
        label="Inspector view"
        className="inspector-tabs"
        options={[
          { value: "wire", label: "JSON-RPC wire", icon: "braces" },
          { value: "server", label: "Server", icon: "info" },
        ]}
      >
        <TabPanel value="wire" className="tabpanel">
          <Wire entries={state.wire} />
        </TabPanel>
        <TabPanel value="server" className="tabpanel">
          <Server state={state} />
        </TabPanel>
      </Tabs>
    </aside>
  );
}
