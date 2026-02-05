-- Your SQL goes here
CREATE TABLE quorum_resharing_info (
    id BIGSERIAL PRIMARY KEY,
    kafka_timestamp TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    success BOOLEAN NOT NULL
);