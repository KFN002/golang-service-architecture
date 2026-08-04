-- +goose Up
CREATE TYPE expr_status AS ENUM ('pending', 'in_progress', 'done', 'failed');
CREATE TYPE task_status AS ENUM ('pending', 'ready', 'running', 'done', 'failed');

CREATE TABLE expressions (
    id         uuid PRIMARY KEY,
    raw        text        NOT NULL,
    status     expr_status NOT NULL DEFAULT 'pending',
    result     double precision,
    error      text,
    trace_id   text        NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    done_at    timestamptz
);

CREATE INDEX idx_expressions_created ON expressions (created_at DESC);
CREATE INDEX idx_expressions_status  ON expressions (status);

CREATE TABLE tasks (
    id            uuid PRIMARY KEY,
    expression_id uuid        NOT NULL REFERENCES expressions (id) ON DELETE CASCADE,
    op            text        NOT NULL,
    arg1_value    double precision,
    arg1_task_id  uuid,
    arg2_value    double precision,
    arg2_task_id  uuid,
    unmet_deps    int         NOT NULL DEFAULT 0,
    status        task_status NOT NULL DEFAULT 'pending',
    result        double precision,
    attempt       int         NOT NULL DEFAULT 0,
    worker_id     text        NOT NULL DEFAULT '',
    is_root       boolean     NOT NULL DEFAULT false,
    queued_at     timestamptz,
    started_at    timestamptz,
    finished_at   timestamptz
);

CREATE INDEX idx_tasks_expression ON tasks (expression_id);
CREATE INDEX idx_tasks_arg1_dep   ON tasks (arg1_task_id) WHERE arg1_task_id IS NOT NULL;
CREATE INDEX idx_tasks_arg2_dep   ON tasks (arg2_task_id) WHERE arg2_task_id IS NOT NULL;

-- Transactional outbox: rows are written in the same transaction as domain
-- state and relayed to the broker by a background publisher.
CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind         text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE tasks;
DROP TABLE expressions;
DROP TYPE task_status;
DROP TYPE expr_status;
