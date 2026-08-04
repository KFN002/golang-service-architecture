"use client";

// Workers: live pool-size curves per agent instance, reconstructed from
// pool.scaled audit events — autoscaling made visible.

import { useEffect, useMemo, useState } from "react";
import {
  CartesianGrid, Legend, Line, LineChart, ResponsiveContainer,
  Tooltip, XAxis, YAxis,
} from "recharts";
import { api, type AuditEvent } from "@/lib/api";
import { Card, Stat } from "@/components/ui";
import { fmtTime } from "@/lib/utils";

const lineColors = ["#7c6cf6", "#22d3ee", "#f472b6", "#34d399", "#fbbf24"];

export default function WorkersPage() {
  const [events, setEvents] = useState<AuditEvent[]>([]);

  useEffect(() => {
    const load = async () => {
      try {
        const res = await api.queryAudit({ eventType: "pool.scaled", pageSize: "200" });
        setEvents((res.events ?? []).reverse()); // oldest first for the chart
      } catch {
        /* audit may still be booting */
      }
    };
    void load();
    const t = setInterval(load, 3000);
    return () => clearInterval(t);
  }, []);

  const { series, instances, current } = useMemo(() => {
    const instances = [...new Set(events.map((e) => e.entityId))];
    const current: Record<string, number> = {};
    const series = events.map((e) => {
      const to = Number(e.payload?.to ?? 0);
      current[e.entityId] = to;
      return {
        at: fmtTime(e.occurredAt),
        // one column per instance; only the emitting instance changes value
        ...Object.fromEntries(instances.map((i) => [i, i === e.entityId ? to : undefined])),
      };
    });
    return { series, instances, current };
  }, [events]);

  return (
    <div className="mx-auto max-w-5xl space-y-6">
      <header>
        <h1 className="text-2xl font-bold">Worker fleet</h1>
        <p className="text-sm text-muted">
          Each agent runs an auto-scaling goroutine pool: it grows with queue backlog and
          shrinks when idle. Every resize is an audit event — this page replays them.
        </p>
      </header>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        {Object.entries(current).map(([inst, size]) => (
          <Stat key={inst} label={inst} value={`${size} workers`} tone="text-accent" />
        ))}
        {instances.length === 0 && (
          <p className="col-span-4 py-6 text-center text-sm text-muted">
            No scaling events yet — submit some expressions to wake the fleet.
          </p>
        )}
      </div>

      {series.length > 0 && (
        <Card>
          <h2 className="mb-4 text-sm font-semibold text-muted">Pool size over time</h2>
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={series}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
              <XAxis dataKey="at" stroke="var(--muted)" fontSize={11} />
              <YAxis stroke="var(--muted)" fontSize={11} allowDecimals={false} />
              <Tooltip
                contentStyle={{
                  background: "var(--surface-2)", border: "1px solid var(--border)",
                  borderRadius: 8, fontSize: 12,
                }}
              />
              <Legend />
              {instances.map((inst, i) => (
                <Line
                  key={inst}
                  dataKey={inst}
                  stroke={lineColors[i % lineColors.length]}
                  dot={false}
                  strokeWidth={2}
                  connectNulls
                  type="stepAfter"
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </Card>
      )}
    </div>
  );
}
