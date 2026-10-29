import { useState } from "react";
import { Icon, Input, Tag, Text, Title, Tooltip } from "@antadesign/anta/bundle";
import { needsApproval, type ChatItem, type Tool } from "../state.ts";

interface Props {
  tools: Tool[];
  counts: Record<string, number>;
  chat: ChatItem[];
  onOpen: (tool: Tool) => void;
}

const GROUPS: Array<{ title: string; match: (n: string) => boolean }> = [
  { title: "Runs", match: (n) => n.startsWith("runs_") },
  { title: "Debugging", match: (n) => n === "debug" },
  { title: "Docs", match: (n) => n.startsWith("docs_") },
  { title: "Environment", match: () => true },
];

export function ToolList({ tools, counts, chat, onOpen }: Props) {
  const [query, setQuery] = useState("");
  const running = new Set(
    chat.filter((c) => c.kind === "tool" && (c.status === "running" || c.status === "approval")).map((c) => (c as { name: string }).name),
  );
  const q = query.trim().toLowerCase();
  const shown = tools.filter((t) => !q || t.name.includes(q) || t.description?.toLowerCase().includes(q));
  const seen = new Set<string>();

  return (
    <aside className="pane tools-pane" aria-label="MCP tools">
      <div className="pane-head">
        <Title level={5} font="sans-serif">
          Tools
        </Title>
        <Text size="small" priority="tertiary">
          from <code>tools/list</code> · click to try one
        </Text>
        <Input
          size="small"
          priority="secondary"
          placeholder="Filter tools"
          aria-label="Filter tools"
          leading={<Icon shape="search" />}
          clearable
          value={query}
          onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
          onClearInput={() => setQuery("")}
        />
      </div>
      <div className="pane-scroll">
        {GROUPS.map((g) => {
          const items = shown.filter((t) => !seen.has(t.name) && g.match(t.name));
          items.forEach((t) => seen.add(t.name));
          if (items.length === 0) return null;
          return (
            <section key={g.title} className="tool-group">
              <Text size="small" priority="tertiary" className="group-title">
                {g.title}
              </Text>
              {items.map((t) => {
                const isRunning = running.has(t.name);
                const count = counts[t.name] ?? 0;
                const isGated = needsApproval(t);
                return (
                  <button
                    key={t.name}
                    type="button"
                    className={`tool-row${isRunning ? " is-running" : ""}${count ? " is-used" : ""}`}
                    onClick={() => onOpen(t)}
                    data-testid={`tool-${t.name}`}
                  >
                    <span className="tool-row-top">
                      <span className={`tool-dot${isRunning ? " live" : ""}`} aria-hidden />
                      <code className="tool-name">{t.name}</code>
                      {isGated && (
                        <span className="tool-lock">
                          <Icon shape="hat-glasses" />
                          <Tooltip>Not marked readOnlyHint, so the agent asks before it calls this tool.</Tooltip>
                        </span>
                      )}
                      {count > 0 && <Tag size="small" tone="brand" value={`×${count}`} />}
                    </span>
                    <Text size="small" priority="tertiary" truncate={2}>
                      {t.description?.replace(/ Returns what .*$/, "")}
                    </Text>
                  </button>
                );
              })}
            </section>
          );
        })}
      </div>
    </aside>
  );
}
