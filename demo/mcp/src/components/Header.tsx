import { Button, Select, Tag, Text, Title, Tooltip } from "@antadesign/anta/bundle";
import type { State } from "../state.ts";

interface Props {
  state: State;
  dark: boolean;
  onToggleDark: () => void;
  onModel: (model: string) => void;
  onEffort: (effort: string) => void;
  onRestart: () => void;
  onNewChat: () => void;
}

export function Header({ state, dark, onToggleDark, onModel, onEffort, onRestart, onNewChat }: Props) {
  const { mcp, socket } = state;
  const info = mcp.state === "connected" ? mcp.info : null;

  let statusTag;
  if (socket !== "open") {
    statusTag = <Tag tone="critical" icon="warning-diamond" label="Demo server" value="offline" />;
  } else if (mcp.state === "connected") {
    statusTag = <Tag tone="success" icon="circle-small-solid" label="MCP" value="connected" />;
  } else if (mcp.state === "connecting") {
    statusTag = <Tag tone="warning" icon="hourglass" label="MCP" value="connecting" />;
  } else {
    statusTag = <Tag tone="critical" icon="warning-diamond" label="MCP" value="stopped" />;
  }

  return (
    <header className="header">
      <div className="brand">
        <img src="./favicon.svg" alt="" width={28} height={28} />
        <div className="brand-text">
          <Title level={4} font="sans-serif">
            snouty mcp
          </Title>
          <Text size="small" priority="tertiary" truncate>
            The Antithesis CLI as an MCP server, driven by Claude
          </Text>
        </div>
      </div>

      <div className="header-status" data-testid="header-status">
        {statusTag}
        {info && (
          <>
            <Tag label={info.name} value={info.version.split(" ")[0]} />
            <Tag label="tools" value={String(info.tools.length)} />
          </>
        )}
        {state.tenant && <Tag tone="brand" icon="cube" label="tenant" value={state.tenant} />}
        {state.replaying && <Tag tone="info" priority="primary" icon="play" label="Replaying recording" />}
      </div>

      <div className="header-actions">
        <Select
          size="small"
          aria-label="Model"
          className="model-select"
          value={state.model || null}
          options={state.models.map((m) => ({ value: m.id, label: m.label }))}
          onValueChange={(v) => v && onModel(String(v))}
          indicator="check"
        />
        <Select
          size="small"
          aria-label="Effort"
          className="effort-select"
          leading={<Text size="small" priority="tertiary">effort</Text>}
          value={state.effort}
          options={["low", "medium", "high"]}
          onValueChange={(v) => v && onEffort(String(v))}
          indicator="check"
        />
        <Button size="small" icon="refresh" label="Restart MCP" onClick={onRestart} data-testid="restart-mcp">
          <Tooltip>Kill and respawn `snouty mcp` to show the handshake on the wire</Tooltip>
        </Button>
        <Button size="small" icon="plus" label="New chat" onClick={onNewChat} data-testid="new-chat" />
        <Button size="small" priority="tertiary" icon={dark ? "sun" : "moon"} aria-label="Toggle dark mode" onClick={onToggleDark}>
          <Tooltip>{dark ? "Light mode" : "Dark mode"}</Tooltip>
        </Button>
      </div>
    </header>
  );
}
