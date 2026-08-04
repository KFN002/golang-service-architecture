-- name: InsertExpression :one
INSERT INTO expressions (id, raw, status, trace_id)
VALUES ($1, $2, 'pending', $3)
RETURNING *;

-- name: GetExpression :one
SELECT * FROM expressions WHERE id = $1;

-- name: ListExpressions :many
SELECT * FROM expressions
ORDER BY created_at DESC, id DESC
LIMIT $1 OFFSET $2;

-- name: CountExpressions :one
SELECT count(*) FROM expressions;

-- name: MarkExpressionInProgress :exec
UPDATE expressions SET status = 'in_progress'
WHERE id = $1 AND status = 'pending';

-- name: FinalizeExpressionDone :exec
UPDATE expressions
SET status = 'done', result = $2, done_at = now()
WHERE id = $1 AND status IN ('pending', 'in_progress');

-- name: FinalizeExpressionFailed :exec
UPDATE expressions
SET status = 'failed', error = $2, done_at = now()
WHERE id = $1 AND status IN ('pending', 'in_progress');

-- name: ExpressionProgress :one
SELECT
    count(*)                                            AS total,
    count(*) FILTER (WHERE status = 'done')             AS done
FROM tasks WHERE expression_id = $1;
