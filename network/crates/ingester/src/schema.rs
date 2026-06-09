// @generated automatically by Diesel CLI.

diesel::table! {
    monitor_task_state (id) {
        id -> Int4,
        last_processed_block -> Int8,
    }
}

diesel::table! {
    multiplier_info (id) {
        id -> Int8,
        quorum_info_id -> Int8,
        multiplier_evm_address -> Text,
        multiplier_peer_id -> Text,
        multi_address -> Text,
        rpc_address -> Text,
        human_pub_key -> Text,
        rsa_pub_key -> Text,
        voting_power -> Text,
        node_idx -> Int4,
    }
}

diesel::table! {
    multiplier_served_requests (id) {
        id -> Int8,
        kafka_timestamp -> Nullable<Timestamptz>,
        ingestion_timestamp -> Nullable<Timestamptz>,
        multiplier_peer_id -> Text,
        request_id -> Text,
    }
}

diesel::table! {
    operator_points_daily (operator, snapshot_time) {
        operator -> Bytea,
        snapshot_time -> Timestamptz,
        cumulative_points -> Float8,
    }
}

diesel::table! {
    operator_points_ledger (id) {
        id -> Int8,
        operator -> Bytea,
        task_id -> Int8,
        role -> Text,
        points -> Float8,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    peer_reachability_quic (id) {
        id -> Int8,
        kafka_timestamp -> Nullable<Timestamptz>,
        ingestion_timestamp -> Nullable<Timestamptz>,
        multiplier_peer_id -> Text,
        success -> Bool,
        duration_micros -> Nullable<Int8>,
    }
}

diesel::table! {
    peer_reachability_tcp (id) {
        id -> Int8,
        kafka_timestamp -> Nullable<Timestamptz>,
        ingestion_timestamp -> Nullable<Timestamptz>,
        multiplier_peer_id -> Text,
        success -> Bool,
        rpc_url -> Text,
        version -> Nullable<Text>,
    }
}

diesel::table! {
    quorum_resharing_info (id) {
        id -> Int8,
        kafka_timestamp -> Nullable<Timestamptz>,
        ingestion_timestamp -> Nullable<Timestamptz>,
        success -> Bool,
    }
}

diesel::table! {
    requests (id) {
        id -> Int8,
        kafka_timestamp -> Nullable<Timestamptz>,
        ingestion_timestamp -> Nullable<Timestamptz>,
        request_id -> Text,
        user_address -> Text,
        method -> Text,
    }
}

diesel::table! {
    task_attestors (task_id, attestor) {
        task_id -> Int8,
        attestor -> Bytea,
    }
}

diesel::table! {
    task_performers (task_id, performer) {
        task_id -> Int8,
        performer -> Bytea,
    }
}

diesel::table! {
    tasks (id) {
        id -> Int8,
        task_number -> Int4,
        task_definition_id -> Int4,
        task_size -> Int8,
        proof_of_task -> Text,
        operator -> Bytea,
        block_number -> Int8,
        transaction_hash -> Bytea,
        log_index -> Int8,
        timestamp -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    user_credits (id) {
        id -> Int8,
        kafka_timestamp -> Nullable<Timestamptz>,
        ingestion_timestamp -> Nullable<Timestamptz>,
        user_address -> Text,
        method -> Text,
        exhausted_credits -> Numeric,
    }
}

diesel::joinable!(multiplier_info -> quorum_resharing_info (quorum_info_id));
diesel::joinable!(operator_points_ledger -> tasks (task_id));
diesel::joinable!(task_attestors -> tasks (task_id));
diesel::joinable!(task_performers -> tasks (task_id));

diesel::allow_tables_to_appear_in_same_query!(
    monitor_task_state,
    multiplier_info,
    multiplier_served_requests,
    operator_points_daily,
    operator_points_ledger,
    peer_reachability_quic,
    peer_reachability_tcp,
    quorum_resharing_info,
    requests,
    task_attestors,
    task_performers,
    tasks,
    user_credits,
);
