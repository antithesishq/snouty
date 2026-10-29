// Calls one tool directly, with no model in the loop: a form built from the
// tool's input schema, and the raw result.

import { useEffect, useState } from "react";
import { Button, Dialog, Input, Select, Switch, Tag, Text, Title } from "@antadesign/anta/bundle";
import { needsApproval, type JsonSchema, type Tool } from "../state.ts";
import { Code } from "./Code.tsx";

export interface CallResult {
  isError: boolean;
  text: string;
  ms: number;
}

interface Props {
  tool: Tool | null;
  onClose: () => void;
  call: (name: string, args: Record<string, unknown>) => Promise<CallResult>;
}

function coerce(schema: JsonSchema, raw: unknown): unknown {
  if (raw === "" || raw === undefined) return undefined;
  switch (schema.type) {
    case "integer":
    case "number": {
      const n = Number(raw);
      return Number.isFinite(n) ? n : undefined;
    }
    case "array":
      return String(raw)
        .split("\n")
        .map((s) => s.trim())
        .filter(Boolean);
    default:
      return raw;
  }
}

export function ToolDialog({ tool, onClose, call }: Props) {
  const [values, setValues] = useState<Record<string, unknown>>({});
  const [result, setResult] = useState<CallResult | null>(null);
  const [help, setHelp] = useState<string | null>(null);
  const [busy, setBusy] = useState<"call" | "help" | null>(null);

  useEffect(() => {
    setValues({});
    setResult(null);
    setHelp(null);
  }, [tool?.name]);

  if (!tool) return null;
  const props = Object.entries(tool.inputSchema.properties ?? {});
  const required = new Set(tool.inputSchema.required ?? []);
  const args = Object.fromEntries(
    props.map(([k, s]) => [k, coerce(s, values[k])]).filter(([, v]) => v !== undefined && v !== false),
  );
  const missing = [...required].filter((k) => args[k] === undefined);

  const run = async () => {
    setBusy("call");
    setResult(await call(tool.name, args));
    setBusy(null);
  };
  const loadHelp = async () => {
    setBusy("help");
    const r = await call("tool_help", { tool: tool.name });
    setHelp(r.text);
    setBusy(null);
  };

  return (
    <Dialog
      open
      onStateChange={(_e, { next }) => !next && onClose()}
      className="tool-dialog"
      header={
        <div className="dialog-head">
          <Title level={4} font="monospace">
            {tool.name}
          </Title>
          {tool.annotations?.readOnlyHint && <Tag size="small" tone="success" value="readOnlyHint" />}
          {needsApproval(tool) && <Tag size="small" tone="warning" value="starts a session on the tenant" />}
        </div>
      }
      footer={
        <>
          <Button label="tool_help" icon="book-open" loading={busy === "help"} onClick={loadHelp} data-testid="tool-help" />
          <Button
            priority="primary"
            icon="play"
            label="Call tool"
            loading={busy === "call"}
            disabled={missing.length > 0}
            onClick={run}
            data-testid="call-tool"
          />
        </>
      }
    >
      <div className="tool-dialog-body">
        <Text priority="secondary">{tool.description}</Text>
        {props.length > 0 ? (
          <div className="tool-form">
            {props.map(([name, schema]) => {
              const label = `${name}${required.has(name) ? " *" : ""}`;
              if (schema.type === "boolean") {
                return (
                  <Switch
                    key={name}
                    label={name}
                    hint={schema.description}
                    checked={values[name] === true}
                    onStateChange={(_e, { next }) => setValues({ ...values, [name]: next })}
                  />
                );
              }
              if (schema.enum) {
                return (
                  <Select
                    key={name}
                    label={label}
                    hint={schema.description}
                    clearable
                    placeholder={schema.default !== undefined ? `default: ${schema.default}` : "—"}
                    value={(values[name] as string) ?? null}
                    options={schema.enum}
                    onValueChange={(v) => setValues({ ...values, [name]: v ?? undefined })}
                  />
                );
              }
              return (
                <Input
                  key={name}
                  label={label}
                  hint={schema.description}
                  multiline={schema.type === "array" || name === "script" || name === "query"}
                  maxRows={6}
                  placeholder={
                    schema.default !== undefined ? `default: ${schema.default}` : schema.type === "array" ? "one value per line" : undefined
                  }
                  value={String(values[name] ?? "")}
                  onInput={(e) => setValues({ ...values, [name]: (e.target as HTMLInputElement).value })}
                  data-testid={`field-${name}`}
                />
              );
            })}
          </div>
        ) : (
          <Text size="small" priority="tertiary">
            This tool takes no params.
          </Text>
        )}
        <div className="kv">
          <Text size="small" priority="tertiary">
            tools/call arguments
          </Text>
          <Code text={JSON.stringify(args)} />
        </div>
        {result && (
          <div className="kv" data-testid="call-result">
            <span className="result-head">
              <Text size="small" priority="tertiary">
                result
              </Text>
              <Tag size="small" tone={result.isError ? "critical" : "success"} value={result.isError ? "isError" : "ok"} />
              <Tag size="small" value={`${(result.ms / 1000).toFixed(2)}s`} />
            </span>
            <Code text={result.text} maxHeight={420} />
          </div>
        )}
        {help && (
          <div className="kv" data-testid="help-result">
            <Text size="small" priority="tertiary">
              tool_help
            </Text>
            <Code text={help} json={false} maxHeight={420} />
          </div>
        )}
      </div>
    </Dialog>
  );
}
