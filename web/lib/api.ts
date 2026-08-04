// Typed client for the gateway API. All paths are relative: nginx (prod) or
// Next rewrites (dev) route them to the right service.

export type ExpressionStatus =
  | "EXPRESSION_STATUS_PENDING"
  | "EXPRESSION_STATUS_IN_PROGRESS"
  | "EXPRESSION_STATUS_DONE"
  | "EXPRESSION_STATUS_FAILED"
  | "EXPRESSION_STATUS_UNSPECIFIED";

export interface Progress {
  total: number;
  done: number;
}

export interface Expression {
  id: string;
  raw: string;
  status: ExpressionStatus;
  hasResult: boolean;
  result: number;
  error: string;
  traceId: string;
  createdAt: string;
  doneAt?: string;
  progress?: Progress;
}

export interface TaskNode {
  id: string;
  op: string;
  status: string;
  hasArg1Value: boolean;
  arg1Value: number;
  arg1TaskId: string;
  hasArg2Value: boolean;
  arg2Value: number;
  arg2TaskId: string;
  hasResult: boolean;
  result: number;
  workerId: string;
  attempt: number;
  isRoot: boolean;
  queuedAt?: string;
  startedAt?: string;
  finishedAt?: string;
}

export interface AuditEvent {
  id: string;
  occurredAt: string;
  eventType: string;
  service: string;
  entityType: string;
  entityId: string;
  traceId: string;
  actor: string;
  payload?: Record<string, unknown>;
}

export interface AuditStats {
  total: string;
  byType: Record<string, string>;
  ingestLastMinute: string;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status}: ${body || res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  submit: (raw: string) =>
    request<Expression>("/api/v1/expressions", {
      method: "POST",
      body: JSON.stringify({ raw }),
    }),

  getExpression: (id: string) => request<Expression>(`/api/v1/expressions/${id}`),

  listExpressions: (pageSize = 30, page = 0) =>
    request<{ expressions: Expression[]; total: string }>(
      `/api/v1/expressions?pageSize=${pageSize}&page=${page}`,
    ),

  getGraph: (id: string) =>
    request<{ expressionId: string; tasks: TaskNode[] }>(
      `/api/v1/expressions/${id}/graph`,
    ),

  queryAudit: (params: Record<string, string>) =>
    request<{ events: AuditEvent[]; nextCursorTs?: string; nextCursorId?: string }>(
      `/api/v1/audit/events?${new URLSearchParams(params)}`,
    ),

  auditStats: () => request<AuditStats>("/api/v1/audit/stats"),

  getTrace: (traceId: string) =>
    request<{ data: JaegerTrace[] }>(`/jaeger-api/traces/${traceId}`),
};

// ---- Jaeger query API shapes (subset we render) ----------------------------

export interface JaegerSpan {
  spanID: string;
  operationName: string;
  references: Array<{ refType: string; spanID: string }>;
  startTime: number; // microseconds
  duration: number; // microseconds
  processID: string;
  tags: Array<{ key: string; value: unknown }>;
}

export interface JaegerTrace {
  traceID: string;
  spans: JaegerSpan[];
  processes: Record<string, { serviceName: string }>;
}
