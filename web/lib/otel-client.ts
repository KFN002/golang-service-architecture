"use client";

// Browser-side OpenTelemetry: the trace of an expression starts at the click.
//
// - fetch auto-instrumentation injects W3C traceparent into every API call,
//   so browser → nginx → gateway → gRPC → RabbitMQ → agent is ONE trace.
// - spans export over OTLP/HTTP to /v1/traces, which nginx (or the dev
//   rewrite) proxies to the private collector.

import { context, trace, type Span } from "@opentelemetry/api";
import { ZoneContextManager } from "@opentelemetry/context-zone";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { FetchInstrumentation } from "@opentelemetry/instrumentation-fetch";
import { registerInstrumentations } from "@opentelemetry/instrumentation";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchSpanProcessor, WebTracerProvider } from "@opentelemetry/sdk-trace-web";

let initialized = false;

export function initBrowserTracing(): void {
  if (initialized || typeof window === "undefined") return;
  initialized = true;

  const provider = new WebTracerProvider({
    resource: resourceFromAttributes({ "service.name": "web-browser" }),
    spanProcessors: [
      new BatchSpanProcessor(new OTLPTraceExporter({ url: "/v1/traces" })),
    ],
  });

  provider.register({ contextManager: new ZoneContextManager() });

  registerInstrumentations({
    instrumentations: [
      new FetchInstrumentation({
        // Same-origin only: never leak trace headers to third parties.
        propagateTraceHeaderCorsUrls: [],
        clearTimingResources: true,
      }),
    ],
  });
}

/** withSpan wraps a user action in a root span (e.g. ui.submit_expression). */
export async function withSpan<T>(name: string, fn: (span: Span) => Promise<T>): Promise<T> {
  const tracer = trace.getTracer("web-ui");
  return tracer.startActiveSpan(name, async (span) => {
    try {
      return await fn(span);
    } finally {
      span.end();
    }
  });
}

/** currentTraceId returns the active trace id, if tracing is live. */
export function currentTraceId(): string | undefined {
  const span = trace.getSpan(context.active());
  const id = span?.spanContext().traceId;
  return id && id !== "00000000000000000000000000000000" ? id : undefined;
}
