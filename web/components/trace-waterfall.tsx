"use client";

// Distributed-trace waterfall rendered from Jaeger's query API: the journey
// of one expression across browser, gateway, broker and workers — on the
// dashboard itself.

import { useEffect, useMemo, useState } from "react";
import { api, type JaegerTrace, type JaegerSpan } from "@/lib/api";
import { fmtMs } from "@/lib/utils";

const serviceColors: Record<string, string> = {
  "web-browser": "var(--accent)",
  web: "#a78bfa",
  orchestrator: "var(--cyan)",
  agent: "#f472b6",
  audit: "#60a5fa",
};

interface Row {
  span: JaegerSpan;
  service: string;
  depth: number;
}

/** flatten orders spans parent-first (depth for indentation). */
function flatten(trace: JaegerTrace): Row[] {
  const children = new Map<string, JaegerSpan[]>();
  const all = new Map(trace.spans.map((s) => [s.spanID, s]));
  const roots: JaegerSpan[] = [];

  for (const span of trace.spans) {
    const parent = span.references.find((r) => r.refType === "CHILD_OF")?.spanID;
    if (parent && all.has(parent)) {
      children.set(parent, [...(children.get(parent) ?? []), span]);
    } else {
      roots.push(span);
    }
  }
  const bySt = (a: JaegerSpan, b: JaegerSpan) => a.startTime - b.startTime;

  const rows: Row[] = [];
  const walk = (span: JaegerSpan, depth: number) => {
    rows.push({ span, service: trace.processes[span.processID]?.serviceName ?? "?", depth });
    (children.get(span.spanID) ?? []).sort(bySt).forEach((c) => walk(c, depth + 1));
  };
  roots.sort(bySt).forEach((r) => walk(r, 0));
  return rows;
}

export function TraceWaterfall({ traceId }: { traceId: string }) {
  const [trace, setTrace] = useState<JaegerTrace | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let stop = false;
    const load = async () => {
      try {
        const res = await api.getTrace(traceId);
        if (!stop && res.data?.[0]) setTrace(res.data[0]);
        if (!stop && !res.data?.[0]) setError("trace not found yet — spans may still be exporting");
      } catch (e) {
        if (!stop) setError(e instanceof Error ? e.message : "failed to load trace");
      }
    };
    void load();
    const t = setInterval(load, 3000); // late spans keep arriving
    return () => {
      stop = true;
      clearInterval(t);
    };
  }, [traceId]);

  const rows = useMemo(() => (trace ? flatten(trace) : []), [trace]);

  if (error && !trace) {
    return <p className="py-6 text-center text-sm text-muted">{error}</p>;
  }
  if (!trace || rows.length === 0) {
    return <p className="py-6 text-center text-sm text-muted">Loading trace…</p>;
  }

  const t0 = Math.min(...rows.map((r) => r.span.startTime));
  const t1 = Math.max(...rows.map((r) => r.span.startTime + r.span.duration));
  const span = Math.max(t1 - t0, 1);

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs text-muted">
        <span>
          {rows.length} spans · {fmtMs(span / 1000)} total
        </span>
        <a
          href={`http://localhost:16686/trace/${traceId}`}
          target="_blank"
          className="text-accent hover:underline"
        >
          Open in Jaeger →
        </a>
      </div>
      <div className="space-y-px">
        {rows.map(({ span: s, service, depth }) => {
          const left = ((s.startTime - t0) / span) * 100;
          const width = Math.max((s.duration / span) * 100, 0.5);
          const color = serviceColors[service] ?? "var(--muted)";
          return (
            <div key={s.spanID} className="group flex items-center gap-2 text-xs">
              <div
                className="w-64 shrink-0 truncate font-mono"
                style={{ paddingLeft: depth * 12 }}
                title={s.operationName}
              >
                <span style={{ color }}>●</span> {s.operationName}
              </div>
              <div className="relative h-5 flex-1 rounded bg-surface-2">
                <div
                  className="absolute top-0.5 h-4 rounded transition-all"
                  style={{ left: `${left}%`, width: `${width}%`, background: color, opacity: 0.85 }}
                />
              </div>
              <span className="w-16 shrink-0 text-right font-mono text-muted">
                {fmtMs(s.duration / 1000)}
              </span>
            </div>
          );
        })}
      </div>
      <div className="flex gap-4 pt-2 text-xs text-muted">
        {Object.entries(serviceColors).map(([name, color]) => (
          <span key={name}>
            <span style={{ color }}>●</span> {name}
          </span>
        ))}
      </div>
    </div>
  );
}
