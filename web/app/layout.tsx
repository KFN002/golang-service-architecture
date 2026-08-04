import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";
import { TelemetryProvider } from "@/components/telemetry-provider";

export const metadata: Metadata = {
  title: "Distributed Calculator",
  description:
    "Watch mathematical expressions travel through a distributed Go system: DAG scheduling, worker pools, RabbitMQ, and end-to-end traces.",
};

const nav = [
  { href: "/", label: "Dashboard", icon: "◇" },
  { href: "/workers", label: "Workers", icon: "⚙" },
  { href: "/audit", label: "Audit log", icon: "☰" },
  { href: "/tour", label: "Tour", icon: "✦" },
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen antialiased">
        <TelemetryProvider />
        <div className="flex min-h-screen">
          <aside className="fixed inset-y-0 flex w-56 flex-col border-r border-border-c bg-surface px-4 py-6">
            <Link href="/" className="mb-8 flex items-center gap-2 px-2">
              <span className="text-xl font-black text-accent">Σ</span>
              <span className="text-sm font-bold tracking-wide">
                distributed<span className="text-accent">.calc</span>
              </span>
            </Link>
            <nav className="flex flex-col gap-1">
              {nav.map((item) => (
                <Link
                  key={item.href}
                  href={item.href}
                  className="flex items-center gap-3 rounded-lg px-3 py-2 text-sm text-muted transition-colors hover:bg-surface-2 hover:text-foreground"
                >
                  <span className="text-accent">{item.icon}</span>
                  {item.label}
                </Link>
              ))}
            </nav>
            <div className="mt-auto space-y-1 px-3 text-xs text-muted">
              <p className="font-semibold uppercase tracking-widest">Backstage</p>
              <a className="block hover:text-foreground" href="http://localhost:16686" target="_blank">Jaeger →</a>
              <a className="block hover:text-foreground" href="http://localhost:3001" target="_blank">Grafana →</a>
              <a className="block hover:text-foreground" href="http://localhost:15672" target="_blank">RabbitMQ →</a>
            </div>
          </aside>
          <main className="ml-56 flex-1 px-8 py-6">{children}</main>
        </div>
      </body>
    </html>
  );
}
