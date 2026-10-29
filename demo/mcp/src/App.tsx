import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { Chat } from "./components/Chat.tsx";
import { Header } from "./components/Header.tsx";
import { ToolDialog, type CallResult } from "./components/ToolDialog.tsx";
import { ToolList } from "./components/ToolList.tsx";
import { WirePanel } from "./components/WirePanel.tsx";
import { initialState, reducer, type ServerMsg, type Tool } from "./state.ts";

function socketUrl() {
  const url = new URL("ws", location.href);
  url.protocol = location.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

function useServer(dispatch: (a: Parameters<typeof reducer>[1]) => void) {
  const ws = useRef<WebSocket | null>(null);
  const calls = useRef(new Map<number, (r: CallResult) => void>());
  const seq = useRef(0);

  useEffect(() => {
    let closed = false;
    let retry: ReturnType<typeof setTimeout>;
    const open = () => {
      dispatch({ type: "socket", state: "connecting" });
      const sock = new WebSocket(socketUrl());
      ws.current = sock;
      sock.onopen = () => dispatch({ type: "socket", state: "open" });
      sock.onmessage = (ev) => {
        const msg = JSON.parse(ev.data) as ServerMsg;
        if (msg.type === "call_result") {
          const reqId = typeof msg.reqId === "number" ? msg.reqId : NaN;
          const resolve = calls.current.get(reqId);
          calls.current.delete(reqId);
          if (typeof resolve === "function") resolve(msg as unknown as CallResult);
          return;
        }
        dispatch({ type: "server", msg });
      };
      sock.onclose = () => {
        dispatch({ type: "socket", state: "closed" });
        for (const resolve of calls.current.values()) {
          resolve({ isError: true, text: "Lost the connection to the demo server.", ms: 0 });
        }
        calls.current.clear();
        if (!closed) retry = setTimeout(open, 1500);
      };
    };
    open();
    return () => {
      closed = true;
      clearTimeout(retry);
      ws.current?.close();
    };
  }, [dispatch]);

  const send = useCallback((msg: Record<string, unknown>) => {
    if (ws.current?.readyState !== WebSocket.OPEN) return false;
    ws.current.send(JSON.stringify(msg));
    return true;
  }, []);

  const call = useCallback(
    (name: string, args: Record<string, unknown>) =>
      new Promise<CallResult>((resolve) => {
        const reqId = ++seq.current;
        calls.current.set(reqId, resolve);
        if (!send({ type: "call_tool", reqId, name, args })) {
          calls.current.delete(reqId);
          resolve({ isError: true, text: "Not connected to the demo server.", ms: 0 });
        }
      }),
    [send],
  );

  return { send, call };
}

export function App() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const { send, call } = useServer(dispatch);
  const [dark, setDark] = useState(() => document.documentElement.classList.contains("dark"));
  const [openTool, setOpenTool] = useState<Tool | null>(null);
  // Stable, so that memoized tool cards skip re-rendering.
  const onApprove = useCallback((id: string, ok: boolean) => send({ type: "approve", id, ok }), [send]);

  useEffect(() => {
    const root = document.documentElement;
    root.classList.toggle("dark", dark);
    root.classList.toggle("light", !dark);
    try {
      localStorage.setItem("theme", dark ? "dark" : "light");
    } catch {
      // Private windows can refuse storage; the theme still applies.
    }
  }, [dark]);

  const tools = state.mcp.state === "connected" ? state.mcp.info.tools : [];

  return (
    <div className="app">
      <Header
        state={state}
        dark={dark}
        onToggleDark={() => setDark(!dark)}
        onModel={(model) => {
          dispatch({ type: "local_settings", model });
          send({ type: "set_model", model, effort: state.effort });
        }}
        onEffort={(effort) => {
          dispatch({ type: "local_settings", effort });
          send({ type: "set_model", model: state.model, effort });
        }}
        onRestart={() => send({ type: "restart_mcp" })}
        onNewChat={() => {
          send({ type: "reset" });
          dispatch({ type: "clear_chat" });
        }}
      />
      <div className="main">
        <ToolList tools={tools} counts={state.toolCounts} chat={state.chat} onOpen={setOpenTool} />
        <Chat
          state={state}
          onSend={(text) => {
            dispatch({ type: "local_user", text });
            send({ type: "chat", text });
          }}
          onStop={() => send({ type: "stop" })}
          onApprove={onApprove}
          onReplay={(name) => {
            send({ type: "reset" });
            dispatch({ type: "clear_chat" });
            send({ type: "replay", name });
          }}
        />
        <WirePanel state={state} />
      </div>
      <ToolDialog tool={openTool} onClose={() => setOpenTool(null)} call={call} />
    </div>
  );
}
