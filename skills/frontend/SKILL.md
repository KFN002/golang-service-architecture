---
name: frontend
description: Use when building a real-time dashboard on Next.js — app-router structure, design tokens, hand-rolled component primitives, SSE with reconnect, custom SVG data visualization, browser OpenTelemetry, and same-origin API architecture. Grounded in this repo's dark dashboard.
---

# Frontend Engineering — a live systems dashboard on Next.js 16

The dashboard in `frontend/` renders a distributed system's inner life in real time: a DAG
that lights up as agents compute, a trace waterfall built from Jaeger data, autoscaling
charts, an audit explorer. This skill covers the architecture that keeps it small, fast,
honest about data freshness — and pleasant to look at without a component library.

## The rules

1. **Server components by default; `"use client"` only where state or browser APIs live.**
   WHY: the tour page ships zero JS; the dashboard ships exactly the JS it needs.
2. **All theming through design tokens (CSS variables).** Hex values appear in exactly one
   file. WHY: a theme you can't change in one place isn't a theme.
3. **Build UI primitives when you need six of them; install a library when you need sixty.**
   WHY: this app's entire kit (Card/Button/Input/Badge/Stat/ProgressBar) is one 100-line
   file — a dependency would cost more to configure than it saves.
4. **Live data flows through one subscription abstraction with reconnect built in.** WHY:
   every ad-hoc `new EventSource` is a leaked connection waiting to happen.
5. **The API client is typed, thin, and relative-path only.** WHY: same-origin in dev
   (rewrites) and prod (nginx) means no CORS, no env-specific base URLs, no leaks.
6. **Client-side validation mirrors the server; the server remains the authority.** WHY:
   instant feedback is UX; enforcement is the backend's job.
7. **Custom visualizations are custom code.** A DAG and a span waterfall are 100 lines of
   SVG each — a charting library would fight you the whole way.

## Structure: islands of liveness in a server-rendered sea

```
app/
├── layout.tsx            server: nav shell, fonts, TelemetryProvider mount
├── page.tsx              client: submit + live expression cards (SSE)
├── expressions/[id]/     client: DAG + trace tabs (filtered SSE)
├── workers/  audit/      client: polling charts / cursor-paged explorer
└── tour/                 server: pure content, zero client JS
components/ ui.tsx dag-graph.tsx trace-waterfall.tsx telemetry-provider.tsx
lib/ api.ts sse.ts otel-client.ts utils.ts
```

The boundary discipline: `layout.tsx` and `tour/` are server components (static HTML out of
the box); anything holding `useState`/`EventSource`/OTel is explicitly `"use client"`. When
a server page needs one live widget, mount a small client island inside it — don't flip the
whole route.

## Design tokens: dark-first, one source of truth

`frontend/app/globals.css` — raw variables, then mapped into Tailwind v4's theme:

```css
:root {
  --background: #0a0a0f;
  --surface: #12121a;
  --border: #26263a;
  --foreground: #e4e4ef;
  --muted: #8a8aa3;
  --accent: #7c6cf6;
  --ok: #34d399;  --warn: #fbbf24;  --err: #f87171;  --run: #f59e0b;
}

@theme inline {
  --color-surface: var(--surface);
  --color-accent: var(--accent);
  /* ... */
}
```

Now `bg-surface`, `text-accent`, `border-border-c` work as utilities *and* the same
variables drive inline SVG (`fill="var(--surface)"`, `stroke="var(--ok)"`). One palette,
three consumers (Tailwind, SVG, keyframes), zero drift. Status colors are semantic
(`--ok`, `--run`, `--err`) not literal (`--green`) — the day you rebrand, meanings survive.

Animation lives beside the tokens as named keyframes:

```css
@keyframes pulse-glow {
  0%, 100% { filter: drop-shadow(0 0 2px var(--run)); }
  50% { filter: drop-shadow(0 0 10px var(--run)); }
}
.task-running { animation: pulse-glow 1.2s ease-in-out infinite; }
```

State → class → animation. React decides *what* something is; CSS decides how that looks
and moves. No JS animation loops for ambient state.

## The 100-line component kit

`frontend/components/ui.tsx` — shadcn-*style* primitives without the dependency. The idiom:
`cn()` merge + token classes + tone maps:

```tsx
const badgeTones: Record<string, string> = {
  pending: "bg-surface-2 text-muted",
  running: "bg-run/15 text-run",
  done: "bg-ok/15 text-ok",
  failed: "bg-err/15 text-err",
};

export function StatusBadge({ status }: { status: string }) {
  const key = status.replace("EXPRESSION_STATUS_", "").replace("TASK_STATUS_", "").toLowerCase();
  return (
    <span className={cn("inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold uppercase",
      badgeTones[key] ?? "bg-surface-2 text-muted")}>
```

Note the normalization: proto enum strings (`EXPRESSION_STATUS_DONE`) and entity strings
(`done`) collapse to one key — the tone map is wire-format-agnostic. When you *do* need
sixty components with a11y baked in, take shadcn/ui properly; the mistake is installing it
for six.

## Live data: one SSE abstraction

`frontend/lib/sse.ts` — reconnect, cleanup, and server-side filtering in ~40 lines:

```ts
export function subscribeEvents(onEvent: (ev: LiveEvent) => void, expressionId?: string): () => void {
  let source: EventSource | null = null;
  let closed = false;
  const url = expressionId
    ? `/api/v1/events?expression_id=${encodeURIComponent(expressionId)}`
    : "/api/v1/events";

  const connect = () => {
    if (closed) return;
    source = new EventSource(url);
    source.onmessage = (msg) => { try { onEvent(JSON.parse(msg.data)); } catch { /* skip frame */ } };
    source.onerror = () => { source?.close(); if (!closed) retryTimer = setTimeout(connect, 2000); };
  };
  connect();
  return () => { closed = true; if (retryTimer) clearTimeout(retryTimer); source?.close(); };
}
```

Usage is always the same three lines inside `useEffect`, returning the unsubscribe as the
cleanup. Design points:
- **Server-side filtering** (`?expression_id=`): the detail page receives only its own
  events — the server filters (see `SSEHub.Handler`), the browser doesn't parse a firehose.
- **Events are invalidation signals, not state.** On event → refetch the resource. The API
  stays the single source of truth; you never reconcile partial event payloads into stale
  client state. Slightly chattier, dramatically less buggy.
- Malformed frames are skipped, never fatal — a stream glitch shouldn't white-screen a
  dashboard.

Decision table for live channels:

| Need | Use |
|---|---|
| Server→client stream, dashboards | **SSE** ← this repo (auto-reconnect free, plain HTTP through nginx) |
| Bidirectional interaction | WebSocket |
| Rarely-changing aggregates | polling with backoff (workers/audit pages, 3–5s) |

## Custom SVG: the DAG and the waterfall

**DAG** (`frontend/components/dag-graph.tsx`): layout is computed, not hand-tuned — column =
dependency depth (memoized recursion), rows centered per column:

```tsx
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
```

Edges are cubic Béziers between computed anchors; node stroke = `statusColor[status]`;
running nodes get `className="task-running"` (the pulse keyframe); resolved edges get the
draw-in animation. React renders SVG like any markup — for a domain-shaped visualization,
that beats bending a chart library every time. Recharts appears exactly once in this app
(the workers time-series — a *generic* chart, which is what chart libraries are for).

**Waterfall** (`frontend/components/trace-waterfall.tsx`): consumes Jaeger's query API
(nginx-proxied), rebuilds the tree (CHILD_OF references → parent-first flatten with depth),
then renders bars positioned by `(startTime - t0) / totalSpan`:

```tsx
  const left = ((s.startTime - t0) / span) * 100;
  const width = Math.max((s.duration / span) * 100, 0.5);
```

The `0.5%` floor keeps microsecond spans visible next to 2-second computes. It re-polls
every 3s because late spans keep arriving from the collector — render what exists, note
that more may come, never block on completeness.

## Browser telemetry (the short version)

`lib/otel-client.ts` boots a WebTracerProvider once (guarded by a module flag), mounted via
a null-rendering client component in the layout. User actions get named root spans
(`withSpan("ui.submit_expression", ...)`); fetch auto-instrumentation carries `traceparent`
same-origin only; spans export to `/v1/traces` through nginx. Full detail in the
observability skill — frontend-specific takeaways: init exactly once, keep the collector
private behind your origin, and never propagate trace headers cross-origin.

## API client: thin, typed, relative

`frontend/lib/api.ts` — one `request<T>` helper, typed interfaces mirroring the gateway
JSON, and relative paths everywhere. The dev/prod parity trick is in
`frontend/next.config.ts`:

```ts
  async rewrites() {
    const api = process.env.API_PROXY ?? "http://localhost:8080";
    return [
      { source: "/api/:path*", destination: `${api}/api/:path*` },
      { source: "/jaeger-api/:path*", destination: `${jaeger}/api/:path*` },
      { source: "/v1/traces", destination: `${otlp}/v1/traces` },
    ];
  },
```

Dev: Next rewrites proxy to localhost services. Prod: nginx serves the *same paths*. The
browser code never knows the difference — no `NEXT_PUBLIC_API_URL`, no CORS config, no
"works in dev" drift. Plus `output: "standalone"` and `poweredByHeader: false` for the
runtime image.

## Anti-patterns seen in the wild

**The client-side-everything app.**
```tsx
// ❌ "use client" in layout.tsx — every route now ships the full bundle
```
One directive at the root silently opts your whole tree out of server rendering. Push
`"use client"` down to the leaves that need it.

**Reconciling event payloads into state.** Applying partial SSE payloads with
`setState(merge(...))` invites drift the moment a field is added server-side. Events
invalidate; fetches hydrate.

**Hardcoded hex in components.** `className="bg-[#12121a]"` scattered around means the
redesign is a 200-file PR. Tokens or nothing.

**Absolute API URLs.** `fetch("http://localhost:8080/api/...")` works until the first
deploy, then becomes a CORS config, then becomes an exposed internal hostname. Relative
paths + proxy layers.

**Chart library for a bespoke diagram.** Forcing a DAG into a force-graph package: 40 config
options deep, still wrong-shaped, 300 KB heavier. If the visualization *is* your domain,
own the SVG.

## PR review checklist

- [ ] New route: server component unless it demonstrably needs client state
- [ ] No hex colors outside `globals.css`; semantic token names for states
- [ ] Live views subscribe via `subscribeEvents`, return cleanup from `useEffect`
- [ ] Event handlers refetch; no partial-payload state merging
- [ ] All fetches relative-path through `lib/api.ts`; new backend routes added to dev rewrites
- [ ] Client validation changes mirrored from server rules, commented as UX-only
- [ ] New visualization: computed layout (no magic coordinates), tokens for color, CSS for motion
- [ ] `npm run build` passes — type errors and RSC violations surface there

## How to verify

```bash
cd frontend && npm run build     # types + RSC boundaries + bundle
make web-dev                     # against `make up-infra` backends
# then: submit an expression, watch the DAG animate; kill the orchestrator,
# watch SSE silently reconnect when it returns — no user-visible error.
```
