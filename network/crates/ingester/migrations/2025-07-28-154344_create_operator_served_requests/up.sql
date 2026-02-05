-- Your SQL goes here
CREATE TABLE multiplier_served_requests (
    id BIGSERIAL PRIMARY KEY,
    kafka_timestamp TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    multiplier_peer_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    CONSTRAINT fk_multiplier_request_id FOREIGN KEY (request_id) REFERENCES requests (request_id) ON DELETE CASCADE
);
