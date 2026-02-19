-- Your SQL goes here
CREATE TABLE
    operator_points_ledger (
        id BIGSERIAL PRIMARY KEY,
        operator BYTEA NOT NULL,
        task_id BIGINT NOT NULL REFERENCES tasks (id),
        role TEXT NOT NULL CHECK (role IN ('performer', 'attestor')),
        points DOUBLE PRECISION NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        UNIQUE (task_id, operator, role)
    );

CREATE INDEX idx_op_points_operator ON operator_points_ledger (operator);

CREATE INDEX idx_op_points_time ON operator_points_ledger (created_at);

CREATE INDEX idx_op_points_operator_time ON operator_points_ledger (operator, created_at);