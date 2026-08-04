"use client";

// DAG visualization: layered SVG layout computed from task dependencies.
// Nodes glow while running, edges light up as dependencies resolve.

import { useMemo } from "react";
import type { TaskNode } from "@/lib/api";
import { shortId } from "@/lib/utils";

interface Positioned extends TaskNode {
  x: number;
  y: number;
  depth: number;
}

const NODE_W = 150;
const NODE_H = 64;
const GAP_X = 70;
const GAP_Y = 28;

const statusColor: Record<string, string> = {
  TASK_STATUS_PENDING: "var(--muted)",
  TASK_STATUS_READY: "var(--cyan)",
  TASK_STATUS_RUNNING: "var(--run)",
  TASK_STATUS_DONE: "var(--ok)",
  TASK_STATUS_FAILED: "var(--err)",
};

/** layout assigns each task a column by dependency depth (leaves left). */
function layout(tasks: TaskNode[]): Positioned[] {
  const byId = new Map(tasks.map((t) => [t.id, t]));
  const depthMemo = new Map<string, number>();

  const depthOf = (t: TaskNode): number => {
    const cached = depthMemo.get(t.id);
    if (cached !== undefined) return cached;
    let d = 0;
    for (const dep of [t.arg1TaskId, t.arg2TaskId]) {
      if (dep && byId.has(dep)) d = Math.max(d, depthOf(byId.get(dep)!) + 1);
    }
    depthMemo.set(t.id, d);
    return d;
  };

  const columns = new Map<number, TaskNode[]>();
  for (const t of tasks) {
    const d = depthOf(t);
    columns.set(d, [...(columns.get(d) ?? []), t]);
  }

  const maxRows = Math.max(...[...columns.values()].map((col) => col.length), 1);
  const out: Positioned[] = [];
  for (const [depth, col] of columns) {
    col.forEach((t, row) => {
      const colHeight = col.length * (NODE_H + GAP_Y);
      const offset = ((maxRows * (NODE_H + GAP_Y)) - colHeight) / 2;
      out.push({
        ...t,
        depth,
        x: depth * (NODE_W + GAP_X),
        y: offset + row * (NODE_H + GAP_Y),
      });
    });
  }
  return out;
}

export function DagGraph({ tasks }: { tasks: TaskNode[] }) {
  const nodes = useMemo(() => layout(tasks), [tasks]);
  const byId = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);

  if (nodes.length === 0) {
    return <p className="py-8 text-center text-sm text-muted">Single literal — no tasks needed.</p>;
  }

  const width = Math.max(...nodes.map((n) => n.x)) + NODE_W + 8;
  const height = Math.max(...nodes.map((n) => n.y)) + NODE_H + 8;

  return (
    <div className="overflow-x-auto">
      <svg width={width} height={height} className="min-w-full">
        {/* Edges first (under the nodes). */}
        {nodes.flatMap((n) =>
          [n.arg1TaskId, n.arg2TaskId]
            .filter((dep): dep is string => !!dep && byId.has(dep))
            .map((dep) => {
              const from = byId.get(dep)!;
              const resolved = from.status === "TASK_STATUS_DONE";
              return (
                <path
                  key={`${dep}->${n.id}`}
                  d={`M ${from.x + NODE_W} ${from.y + NODE_H / 2}
                      C ${from.x + NODE_W + GAP_X / 2} ${from.y + NODE_H / 2},
                        ${n.x - GAP_X / 2} ${n.y + NODE_H / 2},
                        ${n.x} ${n.y + NODE_H / 2}`}
                  fill="none"
                  stroke={resolved ? "var(--ok)" : "var(--border)"}
                  strokeWidth={resolved ? 2 : 1.5}
                  className={resolved ? "edge-live" : undefined}
                />
              );
            }),
        )}

        {nodes.map((n) => {
          const color = statusColor[n.status] ?? "var(--muted)";
          const arg1 = n.hasArg1Value ? n.arg1Value : "•";
          const arg2 = n.hasArg2Value ? n.arg2Value : "•";
          return (
            <g
              key={n.id}
              transform={`translate(${n.x}, ${n.y})`}
              className={n.status === "TASK_STATUS_RUNNING" ? "task-running" : undefined}
            >
              <rect
                width={NODE_W}
                height={NODE_H}
                rx={10}
                fill="var(--surface)"
                stroke={color}
                strokeWidth={n.isRoot ? 2.5 : 1.5}
              />
              <text x={12} y={26} fill="var(--foreground)" fontSize={15} fontFamily="monospace">
                {arg1} {n.op} {arg2}
                {n.hasResult ? ` = ${n.result}` : ""}
              </text>
              <text x={12} y={48} fill="var(--muted)" fontSize={10} fontFamily="monospace">
                {shortId(n.id)}
                {n.workerId ? ` · ${n.workerId}` : ""}
              </text>
              {n.isRoot && (
                <text x={NODE_W - 14} y={16} fill={color} fontSize={11}>
                  ★
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
