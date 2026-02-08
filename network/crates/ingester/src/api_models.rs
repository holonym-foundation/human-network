use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use bigdecimal::BigDecimal;

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub status: String,
    pub data: T,
}

#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub total_items: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Serialize)]
pub struct QuorumMemberResponse {
    pub multiplier_evm_address: String,
    pub multiplier_peer_id: String,
    pub multi_address: String,
    pub rpc_address: String,
    pub human_pub_key: String,
    pub rsa_pub_key: String,
    pub voting_power: String,
    pub node_idx: i32,
}

#[derive(Debug, Serialize)]
pub struct CurrentQuorumResponse {
    pub id: String,
    pub success: bool,
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub members: Vec<QuorumMemberResponse>,
}

#[derive(Debug, Serialize)]
pub struct QuorumElectionResponse {
    pub id: String,
    pub status: bool,
    pub kafka_timestamp: Option<DateTime<Utc>>,
    pub members: Vec<String>, // List of member peer IDs
}

#[derive(Debug, Serialize)]
pub struct RequestResponse {
    pub request_id: String,
    pub user_address: String,
    pub method: String,
    pub kafka_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct TotalKeysGenerated {
    pub count: i64,
}

#[derive(Debug, Serialize)]
pub struct KeyCount {
    pub timestamp: DateTime<Utc>,
    pub count: i64,
}

#[derive(Debug, Serialize)]
pub struct KeysGeneratedChartData  {
    pub data_points: Vec<KeyCount>,
}

#[derive(Debug, Serialize)]
pub struct UserCreditsResponse {
    pub user_address: String,
    pub method: String,
    pub exhausted_credits: BigDecimal,
    pub kafka_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct MultiplierComputedRequests {
    pub multiplier_peer_id: String,
    pub request_count: i64,
}

#[derive(Debug, Serialize)]
pub struct MultiplierQuorumElections {
    pub multiplier_peer_id: String,
    pub elected_quorum_count: i64,
}

#[derive(Debug, Serialize)]
pub struct PeerReachabilityStatus {
    pub peer_id: String,
    pub last_checked_timestamp: Option<DateTime<Utc>>,
    pub success: bool,
    pub details: String, // e.g., rpc_url for TCP, duration_micros for QUIC
}

#[derive(Debug, Serialize)]
pub struct OperatorPointsResponse {
    pub address: String,
    pub points: f64,
}

#[derive(Debug, Serialize)]
pub struct TaskResponse {
    pub task_number: i32,
    pub task_definition_id: i32,
    pub task_size: i64,
    pub proof_of_task: String,
    pub performers: Vec<String>, // List of performer addresses in hex
    pub attestors: Vec<String>,  // List of attestor addresses in hex
    pub transaction_hash: String, // Hex representation of transaction hash
    pub timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct TotalNetworkTvl {
    pub tvl_usd: f64,
    pub warnings: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct SymbioticResponse {
    #[serde(rename = "stakeUsd")]
    pub stake_usd: f64,
}

#[derive(Deserialize)]
pub struct EigenResponse {
    pub tvl: EigenTvl,
}
#[derive(Deserialize)]
pub struct EigenTvl {
    pub tvl: f64,
}

#[derive(Deserialize)]
pub struct CmcResponse {
    pub data: CmcData,
}
#[derive(Deserialize)]
pub struct CmcData {
    #[serde(rename = "1027")] // 1027 is Ethereum's ID
    pub ethereum: CmcEthereum,
}
#[derive(Deserialize)]
pub struct CmcEthereum {
    pub quote: CmcQuote,
}
#[derive(Deserialize)]
pub struct CmcQuote {
    #[serde(rename = "USD")]
    pub usd: CmcUsd,
}
#[derive(Deserialize)]
pub struct CmcUsd {
    pub price: f64,
}
