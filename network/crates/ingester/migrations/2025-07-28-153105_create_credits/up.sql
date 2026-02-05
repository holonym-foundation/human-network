-- Your SQL goes here
CREATE TABLE user_credits (
    id BIGSERIAL PRIMARY KEY,
    kafka_timestamp TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    user_address TEXT NOT NULL,
    method TEXT NOT NULL,
    exhausted_credits NUMERIC NOT NULL,
    UNIQUE (user_address, method)
);