-- Your SQL goes here
CREATE TABLE tasks (
    id BIGSERIAL PRIMARY KEY,                  -- internal DB id
    task_number INTEGER NOT NULL,              -- on-chain task_number
    task_definition_id INTEGER NOT NULL,
    task_size BIGINT NOT NULL,
    proof_of_task TEXT NOT NULL,
    operator BYTEA NOT NULL,                   -- operator address (20 bytes)
    block_number BIGINT NOT NULL,
    transaction_hash BYTEA NOT NULL,           -- 32 bytes
    log_index BIGINT NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),       -- or derive from block metadata
    UNIQUE (transaction_hash, log_index)       -- ensures no dupes
);