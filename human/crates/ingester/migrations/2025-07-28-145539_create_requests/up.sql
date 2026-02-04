-- Your SQL goes here
CREATE TABLE requests (
    id BIGSERIAL PRIMARY KEY,
    kafka_timestamp TIMESTAMP WITH TIME ZONE,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    request_id TEXT UNIQUE NOT NULL, -- Corresponds to Kafka key (request_id.to_string())
    user_address TEXT NOT NULL, -- `ethers::abi::Address` can be stored as TEXT/VARCHAR
    method TEXT NOT NULL -- `Method` enum converted to string
);