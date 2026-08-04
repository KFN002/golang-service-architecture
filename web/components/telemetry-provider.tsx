"use client";

import { useEffect } from "react";
import { initBrowserTracing } from "@/lib/otel-client";

/** Boots browser tracing exactly once, client-side. Renders nothing. */
export function TelemetryProvider() {
  useEffect(() => {
    initBrowserTracing();
  }, []);
  return null;
}
