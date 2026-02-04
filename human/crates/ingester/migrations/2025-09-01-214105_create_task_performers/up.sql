-- Your SQL goes here
CREATE TABLE task_performers (
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    performer BYTEA NOT NULL,                  -- address (20 bytes)
    PRIMARY KEY (task_id, performer)
);
