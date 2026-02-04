-- Your SQL goes here
CREATE TABLE peer_reachability_tcp (
    id BIGSERIAL PRIMARY KEY,
    kafka_timestamp TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    multiplier_peer_id TEXT UNIQUE NOT NULL,
    success BOOLEAN NOT NULL,
    rpc_url TEXT NOT NULL
);