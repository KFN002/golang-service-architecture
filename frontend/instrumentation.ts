// Next.js server-side OpenTelemetry: SSR fetches to the API join the same
// trace tree via @vercel/otel (NodeSDK under the hood).
import { registerOTel } from "@vercel/otel";

export function register() {
  registerOTel({
    serviceName: "web",
    // OTLP endpoint comes from OTEL_EXPORTER_OTLP_ENDPOINT in compose.
  });
}
