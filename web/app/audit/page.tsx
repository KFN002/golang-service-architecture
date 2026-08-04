"use client";

// Audit explorer: the append-only log, filterable, keyset-paginated.

import { useCallback, useEffect, useState } from "react";
import { api, type AuditEvent, type AuditStats } from "@/lib/api";
import { Button, Card, Input, Stat } from "@/components/ui";
import { fmtTime, shortId } from "@/lib/utils";

const typeTones: Record<string, string> = {
  "expression.submitted": "text-accent",
  "expression.done": "text-ok",
  "expression.failed": "text-err",
  "task.done": "text-ok",
  "task.failed": "text-err",
  "pool.scaled": "text-cyan-c",
};

export default function AuditPage() {
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [stats, setStats] = useState<AuditStats | null>(null);
  const [typeFilter, setTypeFilter] = useState("");
  const [traceFilter, setTraceFilter] = useState("");
  const [cursor, setCursor] = useState<{ ts: string; id: string } | null>(null);
  const [exhausted, setExhausted] = useState(false);

  const load = useCallback(
    async (append: boolean, cur: { ts: string; id: string } | null) => {
      const params: Record<string, string> = { pageSize: "50" };
      if (typeFilter) params.eventType = typeFilter;
      if (traceFilter) params.traceId = traceFilter;
      if (append && cur) {
        params.cursorTs = cur.ts;
        params.cursorId = cur.id;
      }
      try {
        const res = await api.queryAudit(params);
        const page = res.events ?? [];
        setEvents((prev) => (append ? [...prev, ...page] : page));
        setExhausted(page.length < 50);
        if (res.nextCursorTs && res.nextCursorId) {
          setCursor({ ts: res.nextCursorTs, id: res.nextCursorId });
        }
      } catch {
        /* audit booting */
      }
    },
    [typeFilter, traceFilter],
  );

  useEffect(() => {
    void load(false, null);
    void api.auditStats().then(setStats).catch(() => undefined);
    const t = setInterval(() => void api.auditStats().then(setStats).catch(() => undefined), 5000);
    return () => clearInterval(t);
  }, [load]);

  return (
    <div className="mx-auto max-w-6xl space-y-6">
      <header>
        <h1 className="text-2xl font-bold">Audit log</h1>
        <p className="text-sm text-muted">
          Append-only, daily-partitioned, immutable by trigger. Ingested via micro-batched
          group commits from RabbitMQ; queried with keyset pagination.
        </p>
      </header>

      <div className="grid grid-cols-3 gap-4">
        <Stat label="Total events" value={stats?.total ?? "—"} />
        <Stat label="Ingest (last minute)" value={stats?.ingestLastMinute ?? "—"} tone="text-cyan-c" />
        <Stat label="Event types" value={String(Object.keys(stats?.byType ?? {}).length || "—")} />
      </div>

      <Card className="flex flex-wrap items-end gap-3">
        <div className="min-w-48 flex-1">
          <label className="mb-1 block text-xs text-muted">Event type</label>
          <Input
            list="event-types"
            value={typeFilter}
            placeholder="task.done"
            onChange={(e) => setTypeFilter(e.target.value)}
          />
          <datalist id="event-types">
            {Object.keys(stats?.byType ?? {}).map((t) => (
              <option key={t} value={t} />
            ))}
          </datalist>
        </div>
        <div className="min-w-64 flex-1">
          <label className="mb-1 block text-xs text-muted">Trace ID</label>
          <Input
            value={traceFilter}
            placeholder="hex trace id"
            onChange={(e) => setTraceFilter(e.target.value)}
          />
        </div>
        <Button onClick={() => void load(false, null)}>Filter</Button>
      </Card>

      <Card className="overflow-x-auto p-0">
        <table className="w-full text-left text-sm">
          <thead className="border-b border-border-c text-xs uppercase tracking-wider text-muted">
            <tr>
              <th className="px-4 py-3">Time</th>
              <th className="px-4 py-3">Type</th>
              <th className="px-4 py-3">Service</th>
              <th className="px-4 py-3">Entity</th>
              <th className="px-4 py-3">Payload</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border-c font-mono text-xs">
            {events.map((ev) => (
              <tr key={ev.id} className="hover:bg-surface-2">
                <td className="whitespace-nowrap px-4 py-2 text-muted">{fmtTime(ev.occurredAt)}</td>
                <td className={`px-4 py-2 font-semibold ${typeTones[ev.eventType] ?? ""}`}>
                  {ev.eventType}
                </td>
                <td className="px-4 py-2">{ev.service}</td>
                <td className="px-4 py-2 text-muted">
                  {ev.entityType}/{shortId(ev.entityId)}
                </td>
                <td className="max-w-md truncate px-4 py-2 text-muted">
                  {ev.payload ? JSON.stringify(ev.payload) : "—"}
                </td>
              </tr>
            ))}
            {events.length === 0 && (
              <tr>
                <td colSpan={5} className="px-4 py-10 text-center text-muted">
                  No events match.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>

      {!exhausted && events.length > 0 && (
        <div className="text-center">
          <Button variant="ghost" onClick={() => void load(true, cursor)}>
            Load older ↓
          </Button>
        </div>
      )}
    </div>
  );
}
