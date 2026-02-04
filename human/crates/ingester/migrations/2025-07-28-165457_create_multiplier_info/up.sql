-- Your SQL goes here
CREATE TABLE multiplier_info (
    id BIGSERIAL PRIMARY KEY,
    quorum_info_id BIGINT NOT NULL, -- Foreign key to quorum_resharing_info
    multiplier_evm_address TEXT NOT NULL,
    multiplier_peer_id TEXT NOT NULL,
    multi_address TEXT NOT NULL, -- Multiaddr
    rpc_address TEXT NOT NULL,
    human_pub_key TEXT NOT NULL, -- Stored as TEXT
    rsa_pub_key TEXT NOT NULL, -- Stored as TEXT
    voting_power TEXT NOT NULL, -- U256 stored as TEXT to preserve full precision
    node_idx INTEGER NOT NULL,
    CONSTRAINT fk_quorum_info
        FOREIGN KEY (quorum_info_id)
        REFERENCES quorum_resharing_info (id)
        ON DELETE CASCADE
);
