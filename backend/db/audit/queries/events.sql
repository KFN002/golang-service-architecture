-- name: InsertAuditEvents :copyfrom
INSERT INTO audit_events (
    id, occurred_at, event_type, service,
    entity_type, entity_id, trace_id, actor, payload
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: InsertAuditEventsIdempotent :execrows
-- unnest-based multi-row insert with conflict skip: near-COPY throughput AND
-- exactly-once effect under at-least-once delivery (retried batches no-op).
INSERT INTO audit_events (
    id, occurred_at, event_type, service,
    entity_type, entity_id, trace_id, actor, payload
)
SELECT unnest($1::uuid[]), unnest($2::timestamptz[]), unnest($3::text[]),
       unnest($4::text[]), unnest($5::text[]), unnest($6::text[]),
       unnest($7::text[]), unnest($8::text[]), unnest($9::jsonb[])
ON CONFLICT DO NOTHING;

-- name: QueryAuditEvents :many
-- Keyset pagination over (occurred_at, id) DESC; every filter is optional.
SELECT * FROM audit_events
WHERE (sqlc.narg('from_ts')::timestamptz IS NULL OR occurred_at >= sqlc.narg('from_ts'))
  AND (sqlc.narg('to_ts')::timestamptz   IS NULL OR occurred_at <  sqlc.narg('to_ts'))
  AND (sqlc.narg('event_type')::text     IS NULL OR event_type  = sqlc.narg('event_type'))
  AND (sqlc.narg('entity_type')::text    IS NULL OR entity_type = sqlc.narg('entity_type'))
  AND (sqlc.narg('entity_id')::text      IS NULL OR entity_id   = sqlc.narg('entity_id'))
  AND (sqlc.narg('trace_id')::text       IS NULL OR trace_id    = sqlc.narg('trace_id'))
  AND (sqlc.narg('cursor_ts')::timestamptz IS NULL
       OR (occurred_at, id) < (sqlc.narg('cursor_ts')::timestamptz, sqlc.narg('cursor_id')::uuid))
ORDER BY occurred_at DESC, id DESC
LIMIT sqlc.arg('page_size');

-- name: CountAuditEvents :one
SELECT count(*) FROM audit_events;

-- name: AuditStatsByType :many
SELECT event_type, count(*) AS n
FROM audit_events
GROUP BY event_type
ORDER BY n DESC;

-- name: AuditIngestLastMinute :one
SELECT count(*) FROM audit_events WHERE occurred_at > now() - interval '1 minute';
