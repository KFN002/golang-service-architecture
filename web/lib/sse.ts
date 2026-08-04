"use client";

// SSE client with automatic reconnect. One EventSource per subscription;
// the browser reconnects on drops, and we back that up with our own retry.

export interface LiveEvent {
  kind: "expression.updated" | "task.updated";
  expression_id: string;
  task_id?: string;
  status: string;
  result?: number;
  error?: string;
  worker_id?: string;
  at: string;
}

export function subscribeEvents(
  onEvent: (ev: LiveEvent) => void,
  expressionId?: string,
): () => void {
  let source: EventSource | null = null;
  let closed = false;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;

  const url = expressionId
    ? `/api/v1/events?expression_id=${encodeURIComponent(expressionId)}`
    : "/api/v1/events";

  const connect = () => {
    if (closed) return;
    source = new EventSource(url);
    source.onmessage = (msg) => {
      try {
        onEvent(JSON.parse(msg.data) as LiveEvent);
      } catch {
        // Malformed frame — ignore; the stream itself is fine.
      }
    };
    source.onerror = () => {
      source?.close();
      if (!closed) retryTimer = setTimeout(connect, 2000);
    };
  };

  connect();
  return () => {
    closed = true;
    if (retryTimer) clearTimeout(retryTimer);
    source?.close();
  };
}
