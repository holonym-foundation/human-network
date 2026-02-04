-- Your SQL goes here
CREATE TABLE task_attestors (
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attestor BYTEA NOT NULL,                   -- address (20 bytes)
    PRIMARY KEY (task_id, attestor)
);
