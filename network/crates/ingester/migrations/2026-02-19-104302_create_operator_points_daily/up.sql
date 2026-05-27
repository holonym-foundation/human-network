-- Your SQL goes here
CREATE TABLE operator_points_daily (
    operator BYTEA NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    cumulative_points DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (operator, snapshot_time)
);

CREATE INDEX idx_op_daily_time
ON operator_points_daily(snapshot_time);

CREATE INDEX idx_op_daily_operator
ON operator_points_daily(operator);
