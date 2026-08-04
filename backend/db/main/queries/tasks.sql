-- name: InsertTask :exec
INSERT INTO tasks (
    id, expression_id, op,
    arg1_value, arg1_task_id, arg2_value, arg2_task_id,
    unmet_deps, status, is_root
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);

-- name: GetTasksByExpression :many
SELECT * FROM tasks WHERE expression_id = $1 ORDER BY id;

-- name: MarkTaskReady :exec
UPDATE tasks SET status = 'ready', queued_at = now()
WHERE id = $1 AND status = 'pending';

-- name: MarkTaskRunning :exec
UPDATE tasks
SET status = 'running', worker_id = $2, attempt = $3, started_at = now()
WHERE id = $1 AND status IN ('ready', 'running');

-- name: CompleteTask :one
-- Idempotent: only transitions once; a duplicate result finds no row.
UPDATE tasks
SET status = 'done', result = $2, finished_at = now()
WHERE id = $1 AND status NOT IN ('done', 'failed')
RETURNING id, expression_id, is_root;

-- name: FailTask :one
UPDATE tasks
SET status = 'failed', finished_at = now()
WHERE id = $1 AND status NOT IN ('done', 'failed')
RETURNING id, expression_id, is_root;

-- name: FillArg1FromResult :many
-- Fan-in: propagate a finished task's result into dependents' first argument.
UPDATE tasks
SET arg1_value = $2, unmet_deps = unmet_deps - 1
WHERE arg1_task_id = $1 AND status = 'pending'
RETURNING id, op, arg1_value, arg2_value, unmet_deps;

-- name: FillArg2FromResult :many
UPDATE tasks
SET arg2_value = $2, unmet_deps = unmet_deps - 1
WHERE arg2_task_id = $1 AND status = 'pending'
RETURNING id, op, arg1_value, arg2_value, unmet_deps;

-- name: BumpTaskAttempt :one
UPDATE tasks SET attempt = attempt + 1 WHERE id = $1 RETURNING attempt;
