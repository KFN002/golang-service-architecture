---
name: security
description: Use when hardening a Go service and its deployment — input validation as a boundary, layered rate limiting, container and network hardening, SQL-injection-proof persistence, tamper-evident storage, and supply-chain checks. Grounded in this repo's defense-in-depth.
---

# Security Hardening — defense in depth you can point at

Security in `perfect-go-service` is not a review-day checklist; it's structural. This skill
maps every ring — from nginx to the database trigger — with the reasoning, the code, and the
way each control fails *safe*.

## Overview: the rings

```
①  nginx        limit_req/limit_conn, body caps, timeouts, header hygiene
②  Fiber        security headers, per-IP token buckets, body limit, timeouts
③  gRPC         per-client interceptor limiter, typed status mapping
④  validation   strict allow-list before any allocation-heavy work
⑤  business     bulkheads, load shedding, task-count caps
⑥  persistence  100% parameterized (sqlc), append-only audit w/ trigger guard
⑦  container    scratch, non-root, no-new-privileges, read-only, netsegments
⑧  supply chain gosec, govulncheck, pinned tools, codegen drift checks
```

A request must survive all eight to do damage. Each ring assumes the ones above it failed.

## The rules

1. **Validate with allow-lists, never deny-lists.** Enumerate what's legal; reject the rest.
   WHY: you can't enumerate attacks, only your own grammar.
2. **Reject before you allocate.** Validation runs before parsing, parsing before planning,
   caps at every stage. WHY: the cheapest DoS defense is refusing work early.
3. **Rate limiting is layered or it's theater.** Edge, app, and RPC each catch what the
   previous ring can't see. WHY: one ring is one bypass away from zero rings.
4. **The database user should be *unable* to do what the app must never do.** Structural
   prevention (triggers, no such method) beats behavioral prevention (code review).
5. **Containers get the least of everything**: no shell, no root, no capability escalation,
   no writable filesystem, no network route they don't need.
6. **Secrets live in env, defaults are dev-only, examples are documented.** A credential in
   git is compromised the moment it's committed.
7. **The supply chain is code too.** Pin tools, scan deps, drift-check generated code.

## Ring ④ first, because it's the one you own completely

`backend/pkg/validator/validator.go` — the character-level allow-list:

```go
func (s *exprScan) check(r rune) error {
	switch {
	case r >= '0' && r <= '9', r == '.':
		s.digits = true
	case r == '(':
		s.depth++
		s.maxDepth = max(s.maxDepth, s.depth)
	...
	case r == ' ' || r == '\t':
		return nil // allowed whitespace; do not update prev
	default:
		return apperrors.Newf(apperrors.CodeInvalidInput, "illegal character %q", r)
	}
```

Everything not explicitly legal is rejected with the offending rune named. And the limits
around it are DoS caps, not style (`backend/pkg/constants/constants.go`):

```go
	MaxExpressionLength = 512
	MaxParenDepth       = 64      // recursion depth cap
	MaxTasksPerExpr     = 256     // fan-out amplification cap
	MaxBodyBytes        = 64 << 10 // request body cap, mirrored at nginx
```

Each cap kills a specific attack: length caps the scanner, depth caps stack/parse cost,
task-count caps *amplification* (one small request must not enqueue unbounded distributed
work — checked again in the planner where tasks actually materialize), body caps the read.
The unit test throws the classics at it — `"1; DROP TABLE tasks"` is a test case with the
name `injection attempt`.

Client-side validation exists too (`frontend/app/page.tsx` mirrors the rules) — but it's UX,
not security. The server never trusts it. Say this out loud in reviews.

## Rings ①–③: throttling in depth

nginx (`deploy/config/nginx.conf`) — cheapest rejection, before your process wakes:

```nginx
    server_tokens off;                       # hide version
    client_max_body_size 64k;                # mirror app body cap
    limit_req_zone  $binary_remote_addr zone=api:10m   rate=50r/s;
    limit_conn_zone $binary_remote_addr zone=perip:10m;
    ...
    location /api/ {
        limit_req zone=api burst=100 nodelay;
        limit_conn perip 50;
```

App ring (`backend/internal/controller/http/v1/middleware.go`) — sharded token buckets with
`Retry-After`, keyed by the *first* X-Forwarded-For hop (set by our nginx, so spoofing the
header from outside doesn't help). gRPC ring: the same limiter behind an interceptor,
catching direct-gRPC callers who never met nginx.

Why three: nginx sees pre-auth volume; the app sees post-routing identity; the interceptor
sees the path REST never touches. Each layer's blind spot is another's coverage. Note also
the SSE and OTLP endpoints get *dedicated* zones — long-lived streams and browser telemetry
have different abuse profiles than the JSON API.

Security headers on every response (ring ②):

```go
		c.Set("X-Content-Type-Options", "nosniff")
		c.Set("X-Frame-Options", "DENY")
		c.Set("Referrer-Policy", "no-referrer")
		c.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
```

That CSP is for the *API* (which serves JSON — `default-src 'none'` is correct and free);
the dashboard sets its own. Blanket-copying an API CSP onto an HTML app breaks it; scoping
them separately is the actual skill.

## Ring ⑥: the database can't be talked into it

**SQLi:** there is no string-built SQL to inject. Every query is sqlc-generated with
positional parameters; the one dynamic DDL (partition creation) goes through `format('%I')`
inside a plpgsql function with a `date` parameter. Grep for `fmt.Sprintf` near `Exec` in
review — the count must stay zero.

**Tamper evidence:** the audit log is append-only *in the engine*, not in the app
(`backend/db/audit/migrations/00001_init.sql`):

```sql
-- Immutability guard: the audit log is legally append-only. Any UPDATE or
-- DELETE — regardless of role — is rejected at the trigger level.
CREATE FUNCTION audit_events_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit_events is append-only: % rejected', TG_OP;
END;
$$;

CREATE TRIGGER trg_audit_immutable
    BEFORE UPDATE OR DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION audit_events_immutable();
```

Belt and suspenders: the Go adapter (`repo/auditstore`) simply *has no* update or delete
method — the capability doesn't exist in the codebase. A compromised app credential can
spam inserts (rate-limited, deduped) but cannot rewrite history. The integration test
literally attempts `UPDATE` and `DELETE` and asserts both fail.

## Ring ⑦: containers with nothing to give an attacker

`deploy/docker/go.Dockerfile` — the whole runtime image is one binary, CA certs, and a
passwd entry:

```dockerfile
# Non-root user compiled into the final image's /etc/passwd.
RUN echo "app:x:10001:10001::/:/sbin/nologin" > /out/passwd

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/passwd /etc/passwd
COPY --from=build /out/service /service
USER app
```

`scratch` means: no shell for a reverse shell, no package manager to pull tools, no libc to
exploit, 33–46 MB total. The passwd trick is how `USER app` works with no useradd.

Compose hardening (`deploy/docker-compose.yml`), per service:

```yaml
  security_opt:
    - no-new-privileges:true      # setuid escalation off
  read_only: true                 # rootfs immutable
  deploy:
    resources:
      limits: { cpus: "1.0", memory: 256M }   # a compromised pod can't eat the host
```

Network segmentation: `edge` / `backend` / `telemetry` networks — nginx is the only
host-exposed service; **databases publish no ports at all** (reachable only on the backend
network); ops UIs (Jaeger, Grafana, RabbitMQ mgmt) bind `127.0.0.1` only. An attacker on
the edge network cannot route to PG. Draw this before an auditor asks you to.

## Ring ⑧: supply chain

- **gosec runs inside `make lint`** — with *all* golangci linters on. Findings are fixed or
  suppressed with a written justification (`//nolint:gosec // clamped in defaults`) — the
  justification is the review artifact.
- **govulncheck** (`make vuln`) against the Go vuln DB — call-graph aware, so it flags what
  you actually *reach*.
- **Pinned tools**: every generator/linter runs via `go run tool@vX.Y.Z` in the Makefile —
  no "works on my machine" drift, no floating `@latest` in the build path.
- **Codegen drift check**: `make generate-check` fails CI if committed generated code
  doesn't match the inputs — nobody can slip logic into `gen/` by hand.
- **CI hygiene** (`.github/workflows/ci.yml`): `permissions: contents: read`, zero
  interpolation of event data into `run:` lines (only static `make` targets), pinned action
  majors.

Secrets: real values via env/`.env` (gitignored); `.env.example` documents shape with
dev-only defaults; compose reads `${VAR:-devdefault}`. The grep test: no credential in git
history, ever — history doesn't forget.

## Decision table: where does a new control go?

| Threat | First ring | Backstop |
|---|---|---|
| Volumetric flood | nginx `limit_req` | app limiter, shedding bulkheads |
| Malformed/hostile input | `pkg/validator` allow-list | parser errors, task caps |
| Injection | sqlc parameterization (structural) | least-privilege DB user |
| History tampering | DB trigger (structural) | adapter has no write method |
| Container escape attempt | scratch + non-root + no-new-privileges | read-only fs, netsegments |
| Bad dependency | govulncheck in CI | pinned versions, drift checks |
| Trace/data leak via browser | same-origin-only propagation, no CORS holes | nginx routes, private collector |

Prefer the **structural** column when both exist: controls that make the bad state
*unrepresentable* don't rot the way behavioral controls do.

## Anti-patterns seen in the wild

**Sanitization instead of validation.**
```go
// ❌ strip "bad" characters, keep going
clean := strings.ReplaceAll(input, "'", "")
```
You'll miss an encoding. Reject invalid input outright with a typed error; never "fix" it.

**One big rate limiter.** A single global limiter either starves legitimate users (too
tight) or admits floods (too loose). Per-IP keys, separate zones per endpoint class
(API vs SSE vs telemetry), layered rings.

**The root Alpine "slim" image.** `FROM alpine` + root user + writable fs is a workbench
for an attacker who lands. If the app doesn't need a shell — and a Go binary doesn't —
ship `scratch`.

**Compensating in app code for a DB that allows everything.** "We never call UPDATE on
audit rows" is a promise; the trigger is a fact. Encode invariants where they can't be
forgotten by the next hire.

**`#nosec` without a reason.** A bare suppression is a security finding with a gag order.
This repo's convention: `//nolint:gosec // clamped in defaults` — the justification is
grep-able and reviewable.

## PR review checklist

- [ ] New input: allow-list validated, size/depth/count-capped *before* expensive work
- [ ] New endpoint: covered by an nginx zone + app limiter; correct CSP scope
- [ ] New query: sqlc-generated; zero string-built SQL (grep `Sprintf.*Exec`)
- [ ] New table with integrity needs: structural guard (trigger/grants), not app promises
- [ ] New service/container: scratch or minimal, non-root, `no-new-privileges`, read-only,
      resource limits, correct network membership, no unneeded published ports
- [ ] New secret: env-injected, `.env.example` updated, never a real value in git
- [ ] Suppressions (`#nosec`/`nolint`) carry written justifications
- [ ] `make lint vuln` clean

## How to verify

```bash
make lint vuln                                    # static rings
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost/api/v1/expressions \
  -H 'Content-Type: application/json' -d '{"raw":"1; DROP TABLE tasks"}'   # 400
for i in $(seq 1 200); do curl -s -o /dev/null -w '%{http_code}\n' \
  localhost/api/v1/expressions; done | sort | uniq -c                      # expect 429s
docker exec perfect-go-service-postgres-audit-1 \
  psql -U audit -c "UPDATE audit_events SET actor='x'"                     # trigger rejects
docker inspect perfect-go-service-orchestrator-1-1 \
  --format '{{.HostConfig.ReadonlyRootfs}} {{.HostConfig.SecurityOpt}}'    # true [no-new-privileges:true]
```
