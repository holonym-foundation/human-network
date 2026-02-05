-- Your SQL goes here
CREATE TABLE peer_reachability_quic (
    id BIGSERIAL PRIMARY KEY,
    kafka_timestamp TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    multiplier_peer_id TEXT UNIQUE NOT NULL,
    success BOOLEAN NOT NULL,
    duration_micros BIGINT
);