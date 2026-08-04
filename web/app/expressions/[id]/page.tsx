"use client";

// Expression detail: the animated DAG and the distributed-trace waterfall.

import Link from "next/link";
import { use, useCallback, useEffect, useState } from "react";
import { api, type Expression, type TaskNode } from "@/lib/api";
import { subscribeEvents } from "@/lib/sse";
import { Card, StatusBadge, ProgressBar } from "@/components/ui";
import { DagGraph } from "@/components/dag-graph";
import { TraceWaterfall } from "@/components/trace-waterfall";
import { cn } from "@/lib/utils";

export default function ExpressionPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [expr, setExpr] = useState<Expression | null>(null);
  const [tasks, setTasks] = useState<TaskNode[]>([]);
  const [tab, setTab] = useState<"dag" | "trace">("dag");
  const [notFound, setNotFound] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [e, g] = await Promise.all([api.getExpression(id), api.getGraph(id)]);
      setExpr(e);
      setTasks(g.tasks ?? []);
    } catch {
      setNotFound(true);
    }
  }, [id]);

  useEffect(() => {
    void refresh();
    // Server-side filtered SSE: only this expression's events arrive.
    return subscribeEvents(() => void refresh(), id);
  }, [id, refresh]);

  if (notFound) {
    return (
      <div className="py-20 text-center text-muted">
        Expression not found. <Link href="/" className="text-accent">← back</Link>
      </div>
    );
  }
  if (!expr) {
    return <div className="py-20 text-center text-muted">Loading…</div>;
  }

  return (
    <div className="mx-auto max-w-6xl space-y-6">
      <Link href="/" className="text-sm text-muted hover:text-foreground">
        ← Dashboard
      </Link>

      <Card className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <code className="text-xl font-bold">{expr.raw}</code>
          <div className="flex items-center gap-3">
            {expr.status === "EXPRESSION_STATUS_DONE" && (
              <span className="font-mono text-2xl font-black text-ok">= {expr.result}</span>
            )}
            <StatusBadge status={expr.status} />
          </div>
        </div>
        {expr.status === "EXPRESSION_STATUS_FAILED" && (
          <p className="text-sm text-err">{expr.error}</p>
        )}
        <ProgressBar done={expr.progress?.done ?? 0} total={expr.progress?.total ?? 0} />
        <p className="text-xs text-muted">
          trace <code>{expr.traceId || "—"}</code>
        </p>
      </Card>

      <div className="flex gap-1 border-b border-border-c">
        {(["dag", "trace"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={cn(
              "rounded-t-lg px-4 py-2 text-sm font-medium",
              tab === t
                ? "border border-b-0 border-border-c bg-surface text-foreground"
                : "text-muted hover:text-foreground",
            )}
          >
            {t === "dag" ? "Task graph" : "Distributed trace"}
          </button>
        ))}
      </div>

      <Card>
        {tab === "dag" ? (
          <DagGraph tasks={tasks} />
        ) : expr.traceId ? (
          <TraceWaterfall traceId={expr.traceId} />
        ) : (
          <p className="py-6 text-center text-sm text-muted">
            No trace recorded for this expression.
          </p>
        )}
      </Card>
    </div>
  );
}
