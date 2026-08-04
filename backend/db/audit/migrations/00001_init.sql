-- +goose Up

-- Append-only audit log. Partitioned by day; INSERT-only enforced by trigger.
CREATE TABLE audit_events (
    id          uuid        NOT NULL,
    occurred_at timestamptz NOT NULL,
    event_type  text        NOT NULL,
    service     text        NOT NULL,
    entity_type text        NOT NULL DEFAULT '',
    entity_id   text        NOT NULL DEFAULT '',
    trace_id    text        NOT NULL DEFAULT '',
    actor       text        NOT NULL DEFAULT '',
    payload     jsonb       NOT NULL DEFAULT '{}',
    PRIMARY KEY (occurred_at, id)
) PARTITION BY RANGE (occurred_at);

-- BRIN suits append-only time-ordered data: tiny index, fast range scans.
CREATE INDEX idx_audit_occurred_brin ON audit_events USING brin (occurred_at);
CREATE INDEX idx_audit_entity        ON audit_events (entity_type, entity_id, occurred_at DESC);
CREATE INDEX idx_audit_trace         ON audit_events (trace_id) WHERE trace_id <> '';
CREATE INDEX idx_audit_type          ON audit_events (event_type, occurred_at DESC);

-- +goose StatementBegin
-- Immutability guard: the audit log is legally append-only. Any UPDATE or
-- DELETE — regardless of role — is rejected at the trigger level.
CREATE FUNCTION audit_events_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit_events is append-only: % rejected', TG_OP
        USING ERRCODE = 'raise_exception';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_audit_immutable
    BEFORE UPDATE OR DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION audit_events_immutable();

-- +goose StatementBegin
-- ensure_audit_partition creates the daily partition for the given date if it
-- does not exist. Called by the partition maintainer ahead of time.
CREATE FUNCTION ensure_audit_partition(day date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    part_name text := 'audit_events_' || to_char(day, 'YYYYMMDD');
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF audit_events FOR VALUES FROM (%L) TO (%L)',
            part_name, day, day + 1
        );
    END IF;
END;
$$;
-- +goose StatementEnd

-- Bootstrap: today and tomorrow always exist.
SELECT ensure_audit_partition(now()::date);
SELECT ensure_audit_partition(now()::date + 1);

-- +goose Down
DROP TABLE audit_events;
DROP FUNCTION ensure_audit_partition(date);
DROP FUNCTION audit_events_immutable();
