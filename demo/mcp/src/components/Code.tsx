// Pretty JSON (and JSON Lines) with token colors. Output that is not JSON
// shows as plain text.

import { useMemo, useState } from "react";
import { Button } from "@antadesign/anta/bundle";

const TOKEN = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;

function highlight(src: string) {
  const out: React.ReactNode[] = [];
  let last = 0;
  let i = 0;
  for (const m of src.matchAll(TOKEN)) {
    if (m.index > last) out.push(src.slice(last, m.index));
    const cls = m[1] ? (m[2] ? "tok-key" : "tok-str") : m[3] ? "tok-lit" : "tok-num";
    out.push(
      <span key={i++} className={cls}>
        {m[1] ?? m[0]}
      </span>,
    );
    if (m[2]) out.push(m[2]);
    last = m.index + m[0].length;
  }
  out.push(src.slice(last));
  return out;
}

/** Formats JSON or JSON Lines; returns null for anything else. */
export function prettyJson(text: string): string | null {
  const t = text.trim();
  if (!t || !"{[".includes(t[0])) return null;
  try {
    return JSON.stringify(JSON.parse(t), null, 2);
  } catch {
    const lines = t.split("\n").filter(Boolean);
    try {
      return lines.map((l) => JSON.stringify(JSON.parse(l), null, 2)).join("\n");
    } catch {
      return null;
    }
  }
}

const PREVIEW = 6000;

export function Code({ text, json = true, maxHeight }: { text: string; json?: boolean; maxHeight?: number }) {
  const [all, setAll] = useState(false);
  const pretty = useMemo(() => (json ? prettyJson(text) : null), [text, json]);
  const body = pretty ?? text;
  const long = body.length > PREVIEW;
  const shown = all || !long ? body : body.slice(0, PREVIEW);
  return (
    <div className="code-wrap">
      <pre className="code" style={maxHeight ? { maxHeight } : undefined}>
        <code>{pretty ? highlight(shown) : shown}</code>
        {long && !all && <span className="code-more">…</span>}
      </pre>
      {long && (
        <Button
          size="small"
          priority="tertiary"
          label={all ? "Show less" : `Show all ${(body.length / 1024).toFixed(0)} KB`}
          onClick={() => setAll(!all)}
        />
      )}
    </div>
  );
}
