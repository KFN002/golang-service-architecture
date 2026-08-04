-- name: InsertOutbox :exec
INSERT INTO outbox (kind, payload) VALUES ($1, $2);

-- name: SelectOutboxBatch :many
-- Claimed inside a transaction: SKIP LOCKED lets orchestrator replicas relay
-- concurrently without contending on the same rows.
SELECT id, kind, payload FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: MarkOutboxPublished :exec
UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[]);

-- name: PruneOutbox :execrows
DELETE FROM outbox
WHERE published_at IS NOT NULL AND published_at < now() - interval '1 hour';
