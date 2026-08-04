import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Standalone output → the runtime image ships only what it executes.
  output: "standalone",
  poweredByHeader: false,
  async rewrites() {
    // Local development convenience: proxy API + telemetry to the compose
    // stack so the browser stays same-origin. In production nginx does this.
    const api = process.env.API_PROXY ?? "http://localhost:8080";
    const jaeger = process.env.JAEGER_PROXY ?? "http://localhost:16686";
    const otlp = process.env.OTLP_PROXY ?? "http://localhost:4318";
    return [
      { source: "/api/v1/audit/:path*", destination: `${process.env.AUDIT_PROXY ?? "http://localhost:8081"}/api/v1/audit/:path*` },
      { source: "/api/:path*", destination: `${api}/api/:path*` },
      { source: "/jaeger-api/:path*", destination: `${jaeger}/api/:path*` },
      { source: "/v1/traces", destination: `${otlp}/v1/traces` },
    ];
  },
};

export default nextConfig;
