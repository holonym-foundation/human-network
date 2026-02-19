use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::schema::{monitor_task_state, operator_points_ledger, task_attestors, task_performers, tasks};

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::requests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Requests {
    pub id: i64,
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub ingestion_timestamp: Option<DateTime<Utc>>,
    pub request_id: String,
    pub user_address: String,
    pub method: String,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::user_credits)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct UserCredits {
    pub id: i64,
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub ingestion_timestamp: Option<DateTime<Utc>>,
    pub user_address: String,
    pub method: String,
    pub exhausted_credits: bigdecimal::BigDecimal,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::multiplier_served_requests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct MultiplierServedRequests {
    pub id: i64,
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub ingestion_timestamp: Option<DateTime<Utc>>,
    pub multiplier_peer_id: String,
    pub request_id: String,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::peer_reachability_tcp)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct PeerReachabilityTcp {
    pub id: i64,
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub ingestion_timestamp: Option<DateTime<Utc>>,
    pub multiplier_peer_id: String,
    pub success: bool,
    pub rpc_url: String,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::peer_reachability_quic)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct PeerReachabilityQuic {
    pub id: i64,
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub ingestion_timestamp: Option<DateTime<Utc>>,
    pub multiplier_peer_id: String,
    pub success: bool,
    pub duration_micros: Option<i64>,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::quorum_resharing_info)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct QuorumResharingInfoDb {
    pub id: i64,
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub ingestion_timestamp: Option<DateTime<Utc>>,
    pub success: bool,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::multiplier_info)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct MultiplierInfo {
    pub id: i64,
    pub quorum_info_id: i64,
    pub multiplier_evm_address: String,
    pub multiplier_peer_id: String,
    pub multi_address: String,
    pub rpc_address: String,
    pub human_pub_key: String,
    pub rsa_pub_key: String,
    pub voting_power: String,
    pub node_idx: i32,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Task {
    pub id: i64,
    pub task_number: i32,
    pub task_definition_id: i32,
    pub task_size: i64,
    pub proof_of_task: String,
    pub operator: Vec<u8>,
    pub block_number: i64,
    pub transaction_hash: Vec<u8>,
    pub log_index: i64,
    pub timestamp: Option<DateTime<Utc>>,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::task_performers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskPerformers {
    pub task_id: i64,
    pub performer: Vec<u8>,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::task_attestors)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskAttestors {
    pub task_id: i64,
    pub attestor: Vec<u8>,
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = crate::schema::operator_points_ledger)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct OperatorPointsLedger {
    pub id: i64,
    pub operator: Vec<u8>,
    pub task_id: i64,
    pub role: String,
    pub points: f64,
    pub created_at: DateTime<Utc>,
}

// --- Insertable Structs (for writing to DB) ---

#[derive(Insertable, Debug)]
#[diesel(table_name = crate::schema::requests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewRequest {
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub request_id: String,
    pub user_address: String,
    pub method: String,
}

#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = crate::schema::user_credits)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewUserCredit {
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub user_address: String,
    pub method: String,
    pub exhausted_credits: bigdecimal::BigDecimal,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = crate::schema::multiplier_served_requests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewMultiplierServedRequest {
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub multiplier_peer_id: String,
    pub request_id: String,
}

#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = crate::schema::peer_reachability_tcp)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewPeerReachabilityTcp {
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub multiplier_peer_id: String,
    pub success: bool,
    pub rpc_url: String,
}

#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = crate::schema::peer_reachability_quic)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewPeerReachabilityQuic {
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub multiplier_peer_id: String,
    pub success: bool,
    pub duration_micros: Option<i64>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = crate::schema::quorum_resharing_info)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewQuorumResharingInfo {
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub success: bool,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = crate::schema::multiplier_info)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewMultiplierInfo {
    pub quorum_info_id: i64,
    pub multiplier_evm_address: String,
    pub multiplier_peer_id: String,
    pub multi_address: String,
    pub rpc_address: String,
    pub human_pub_key: String,
    pub rsa_pub_key: String,
    pub voting_power: String,
    pub node_idx: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewTask {
    pub task_number: i32,
    pub task_definition_id: i32,
    pub task_size: i64,
    pub proof_of_task: String,
    pub operator: Vec<u8>,
    pub block_number: i64,
    pub transaction_hash: Vec<u8>,
    pub log_index: i64,
    pub timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = task_performers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewTaskPerformer {
    pub task_id: i64,
    pub performer: Vec<u8>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = task_attestors)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewTaskAttestor {
    pub task_id: i64,
    pub attestor: Vec<u8>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = monitor_task_state)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewMonitorTaskState {
    pub id: i32,
    pub last_processed_block: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = operator_points_ledger)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewOperatorPointsLedger {
    pub operator: Vec<u8>,
    pub task_id: i64,
    pub role: String,
    pub points: f64,
    pub created_at: DateTime<Utc>,
}
