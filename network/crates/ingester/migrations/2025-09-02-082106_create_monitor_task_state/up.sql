-- Your SQL goes here
CREATE TABLE monitor_task_state (
id INTEGER PRIMARY KEY,
last_processed_block BIGINT NOT NULL
);