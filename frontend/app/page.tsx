"use client";

// Dashboard: submit expressions, watch them evaluate live.

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { api, type Expression } from "@/lib/api";
import { subscribeEvents, type LiveEvent } from "@/lib/sse";
import { withSpan } from "@/lib/otel-client";
import { Button, Card, Input, ProgressBar, StatusBadge } from "@/components/ui";
import { shortId } from "@/lib/utils";

// Client-side mirror of pkg/validator — instant feedback; the server remains
// the authority.
function validate(raw: string): string | null {
  const s = raw.trim();
  if (!s) return "Enter an expression";
  if (s.length > 512) return "Too long (max 512 chars)";
  if (!/^[\d\s+\-*/().]+$/.test(s)) return "Only numbers, + - * / and parentheses";
  let depth = 0;
  for (const ch of s) {
    if (ch === "(") depth++;
    if (ch === ")" && --depth < 0) return "Unbalanced parentheses";
  }
  if (depth !== 0) return "Unbalanced parentheses";
  if (!/\d/.test(s)) return "No numbers in expression";
  if (/[+\-*/]\s*$/.test(s)) return "Ends with an operator";
  return null;
}

export default function Dashboard() {
  const [raw, setRaw] = useState("");
  const [hint, setHint] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expressions, setExpressions] = useState<Expression[]>([]);
  const listRef = useRef<Expression[]>([]);
  listRef.current = expressions;

  const refresh = useCallback(async () => {
    try {
      const res = await api.listExpressions();
      setExpressions(res.expressions ?? []);
    } catch {
      /* transient — SSE keeps things fresh */
    }
  }, []);

  useEffect(() => {
    void refresh();
    // Live updates: any expression state change refreshes its card.
    return subscribeEvents((ev: LiveEvent) => {
      if (ev.kind === "expression.updated") void refresh();
      if (ev.kind === "task.updated") {
        // Progress ticks: refetch just the touched expression.
        const cur = listRef.current;
        if (cur.some((e) => e.id === ev.expression_id)) void refresh();
      }
    });
  }, [refresh]);

  const submit = async () => {
    const problem = validate(raw);
    if (problem) {
      setHint(problem);
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      // The trace root: this span's ID travels browser → nginx → gateway →
      // gRPC → RabbitMQ → agent and back.
      await withSpan("ui.submit_expression", async () => {
        await api.submit(raw.trim());
      });
      setRaw("");
      setHint(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "submit failed");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="mx-auto max-w-5xl space-y-6">
      <header>
        <h1 className="text-2xl font-bold">Distributed Calculator</h1>
        <p className="text-sm text-muted">
          Every operation becomes a task, fanned out over RabbitMQ to auto-scaling worker
          pools. Click an expression to watch its DAG and its distributed trace.
        </p>
      </header>

      <Card className="space-y-3">
        <div className="flex gap-3">
          <Input
            value={raw}
            placeholder="(2 + 2) * 3 - 10 / 4"
            onChange={(e) => {
              setRaw(e.target.value);
              setHint(validate(e.target.value) && e.target.value ? validate(e.target.value) : null);
            }}
            onKeyDown={(e) => e.key === "Enter" && !submitting && void submit()}
            aria-label="Expression"
          />
          <Button onClick={() => void submit()} disabled={submitting || !!validate(raw)}>
            {submitting ? "Submitting…" : "Compute"}
          </Button>
        </div>
        {hint && <p className="text-xs text-warn">{hint}</p>}
        {error && <p className="text-xs text-err">{error}</p>}
      </Card>

      <section className="grid grid-cols-1 gap-4 md:grid-cols-2">
        {expressions.map((expr) => (
          <Link key={expr.id} href={`/expressions/${expr.id}`}>
            <Card className="space-y-3 transition-colors hover:border-accent">
              <div className="flex items-center justify-between gap-2">
                <code className="truncate font-mono text-sm">{expr.raw}</code>
                <StatusBadge status={expr.status} />
              </div>
              <ProgressBar
                done={expr.progress?.done ?? 0}
                total={expr.progress?.total ?? 0}
              />
              <div className="flex items-center justify-between text-xs text-muted">
                <span>#{shortId(expr.id)}</span>
                {expr.status === "EXPRESSION_STATUS_DONE" && (
                  <span className="font-mono text-base font-bold text-ok">
                    = {expr.result}
                  </span>
                )}
                {expr.status === "EXPRESSION_STATUS_FAILED" && (
                  <span className="truncate text-err">{expr.error}</span>
                )}
              </div>
            </Card>
          </Link>
        ))}
        {expressions.length === 0 && (
          <p className="col-span-2 py-12 text-center text-sm text-muted">
            Nothing computed yet — try <code className="text-accent">(2 + 2) * 3</code>
          </p>
        )}
      </section>
    </div>
  );
}
