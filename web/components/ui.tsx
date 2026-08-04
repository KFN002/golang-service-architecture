"use client";

// Minimal shadcn-style primitives on the design tokens. One file — the app
// needs exactly these six.

import { cn } from "@/lib/utils";
import type { ButtonHTMLAttributes, HTMLAttributes, InputHTMLAttributes } from "react";

export function Card({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "rounded-xl border border-border-c bg-surface p-4 shadow-lg shadow-black/20",
        className,
      )}
      {...props}
    />
  );
}

export function Button({
  className,
  variant = "default",
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "default" | "ghost" | "danger" }) {
  return (
    <button
      className={cn(
        "inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors",
        "disabled:cursor-not-allowed disabled:opacity-50",
        variant === "default" && "bg-accent text-white hover:brightness-110",
        variant === "ghost" && "border border-border-c bg-transparent hover:bg-surface-2",
        variant === "danger" && "bg-err/20 text-err hover:bg-err/30",
        className,
      )}
      {...props}
    />
  );
}

export function Input({ className, ...props }: InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      className={cn(
        "w-full rounded-lg border border-border-c bg-surface-2 px-3 py-2 text-sm",
        "placeholder:text-muted focus:border-accent focus:outline-none",
        className,
      )}
      {...props}
    />
  );
}

const badgeTones: Record<string, string> = {
  pending: "bg-surface-2 text-muted",
  ready: "bg-cyan-c/15 text-cyan-c",
  running: "bg-run/15 text-run",
  in_progress: "bg-run/15 text-run",
  done: "bg-ok/15 text-ok",
  failed: "bg-err/15 text-err",
};

export function StatusBadge({ status }: { status: string }) {
  const key = status.replace("EXPRESSION_STATUS_", "").replace("TASK_STATUS_", "").toLowerCase();
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold uppercase tracking-wide",
        badgeTones[key] ?? "bg-surface-2 text-muted",
      )}
    >
      {key.replace("_", " ")}
    </span>
  );
}

export function Stat({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <Card className="flex flex-col gap-1">
      <span className="text-xs uppercase tracking-widest text-muted">{label}</span>
      <span className={cn("text-2xl font-bold", tone)}>{value}</span>
    </Card>
  );
}

export function ProgressBar({ done, total }: { done: number; total: number }) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  return (
    <div className="h-1.5 w-full overflow-hidden rounded-full bg-surface-2">
      <div
        className="h-full rounded-full bg-accent transition-all duration-500"
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}
