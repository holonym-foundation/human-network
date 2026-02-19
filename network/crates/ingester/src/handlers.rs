use std::collections::HashMap;
use std::env;

use alloy::hex;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Duration, TimeZone, Utc};
use diesel::dsl::{count_distinct, sql};
use diesel::prelude::*;
use diesel::sql_types::{Double, Timestamptz};
use tracing::{error, info, instrument, warn};

use crate::api_models::{
    ApiResponse, CmcResponse, CurrentQuorumResponse, EigenResponse, KeyCount, KeysGeneratedChartData, MultiplierComputedRequests, MultiplierQuorumElections, OperatorPointsResponse, PaginatedResponse,
    PeerReachabilityStatus, QuorumElectionResponse, QuorumMemberResponse, RequestResponse, SymbioticResponse, TaskResponse, TotalKeysGenerated, TotalNetworkTvl, UserCreditsResponse,
};
use crate::models::{MultiplierInfo, PeerReachabilityQuic, PeerReachabilityTcp, QuorumResharingInfoDb, Requests, Task, TaskAttestors, TaskPerformers, UserCredits};
use crate::schema::{
    multiplier_info, multiplier_served_requests, operator_points_ledger, peer_reachability_quic, peer_reachability_tcp, quorum_resharing_info, requests, task_attestors, task_performers, tasks,
    user_credits,
};
use crate::AppState;

/// Represents pagination parameters from query string.
#[derive(Debug, serde::Deserialize)]
pub struct PaginationParams {
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_page_size")]
    pub page_size: i64,
}

fn default_page() -> i64 { 1 }
fn default_page_size() -> i64 { 20 }

/// Represents timeframes for chart data.
#[derive(Debug, serde::Deserialize)]
pub struct ChartTimeframeParams {
    #[serde(default = "default_timeframe_days")]
    pub days: i64,
    #[serde(default = "default_all_time")]
    pub all_time: bool,
}

fn default_timeframe_days() -> i64 { 7 }
fn default_all_time() -> bool { false }
// Custom error types for better error handling
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Database connection failed: {0}")]
    DatabaseConnection(String),

    #[error("Database query failed: {0}")]
    DatabaseQuery(String),

    #[error("Invalid pagination parameters: {0}")]
    InvalidPagination(String),

    #[error("Invalid timeframe parameters: {0}")]
    InvalidTimeframe(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Upstream API request failed: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<ApiError> for (StatusCode, String) {
    fn from(error: ApiError) -> Self {
        match error {
            ApiError::DatabaseConnection(msg) => {
                error!("Database connection error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "Database connection failed".to_string())
            }
            ApiError::DatabaseQuery(msg) => {
                error!("Database query error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "Database operation failed".to_string())
            }
            ApiError::InvalidPagination(msg) => {
                warn!("Invalid pagination parameters: {}", msg);
                (StatusCode::BAD_REQUEST, msg)
            }
            ApiError::InvalidTimeframe(msg) => {
                warn!("Invalid timeframe parameters: {}", msg);
                (StatusCode::BAD_REQUEST, msg)
            }
            ApiError::NotFound(msg) => {
                info!("Resource not found: {}", msg);
                (StatusCode::NOT_FOUND, msg)
            }
            ApiError::Reqwest(e) => {
                error!("Upstream API error: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Upstream API request failed: {}", e))
            }
            ApiError::Internal(msg) => {
                error!("Internal error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, msg)
            }
        }
    }
}

// Helper function to validate pagination parameters
fn validate_pagination(params: &PaginationParams) -> Result<(), ApiError> {
    if params.page < 1 {
        return Err(ApiError::InvalidPagination("Page number must be greater than 0".to_string()));
    }

    if params.page_size < 1 || params.page_size > 100 {
        return Err(ApiError::InvalidPagination("Page size must be between 1 and 100".to_string()));
    }

    Ok(())
}

fn validate_timeframe(params: &ChartTimeframeParams) -> Result<(), ApiError> {
    if params.days < 1 || params.days > 365 {
        return Err(ApiError::InvalidTimeframe("Days parameter must be between 1 and 365".to_string()));
    }
    Ok(())
}

// Helper function to create success response
fn success_response<T>(data: T) -> Json<ApiResponse<T>> { Json(ApiResponse { status: "success".to_string(), data }) }

/// GET /api/v1/current_quorum
/// Returns the current successful quorum.
#[instrument(name = "get_current_quorum", skip(pool))]
pub async fn get_current_quorum(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<CurrentQuorumResponse>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    // Find the latest successful quorum resharing info
    let current_quorum_info: QuorumResharingInfoDb = match quorum_resharing_info::table
        .filter(quorum_resharing_info::success.eq(true))
        .order(quorum_resharing_info::kafka_timestamp.desc())
        .first::<QuorumResharingInfoDb>(&mut conn)
    {
        Ok(info) => {
            info!("Found current quorum with ID: {}", info.id);
            info
        }
        Err(diesel::NotFound) => {
            info!("No successful quorum found");
            return Ok(success_response(CurrentQuorumResponse {
                id: "".to_string(),
                success: false,
                kafka_timestamp: None,
                members: vec![],
            }));
        }
        Err(e) => {
            return Err(ApiError::DatabaseQuery(format!("Failed to query current quorum: {}", e)).into());
        }
    };

    // Fetch all multipliers associated with the current quorum info
    let multipliers: Vec<MultiplierInfo> = multiplier_info::table
        .filter(multiplier_info::quorum_info_id.eq(current_quorum_info.id))
        .order(multiplier_info::node_idx.asc())
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query multipliers for quorum {}: {}", current_quorum_info.id, e)))?;

    info!(quorum_id = current_quorum_info.id, member_count = multipliers.len(), "Successfully retrieved current quorum");

    let quorum_members_response: Vec<QuorumMemberResponse> = multipliers
        .into_iter()
        .map(|p| QuorumMemberResponse {
            multiplier_evm_address: p.multiplier_evm_address,
            multiplier_peer_id: p.multiplier_peer_id,
            multi_address: p.multi_address,
            rpc_address: p.rpc_address,
            human_pub_key: p.human_pub_key,
            rsa_pub_key: p.rsa_pub_key,
            voting_power: p.voting_power,
            node_idx: p.node_idx,
        })
        .collect();

    Ok(success_response(CurrentQuorumResponse {
        id: current_quorum_info.id.to_string(),
        success: current_quorum_info.success,
        kafka_timestamp: current_quorum_info.kafka_timestamp,
        members: quorum_members_response,
    }))
}

/// GET /api/v1/quorum_elections
/// Returns all quorum elections, with pagination.
#[instrument(
    name = "get_quorum_elections",
    skip(pool),
    fields(page = %params.page, page_size = %params.page_size)
)]
pub async fn get_quorum_elections(
    State(AppState { pool, .. }): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<QuorumElectionResponse>>>, (StatusCode, String)> {
    validate_pagination(&params)?;

    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let offset = (params.page - 1) * params.page_size;

    let total_elections: i64 = quorum_resharing_info::table
        .count()
        .get_result(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to count elections: {}", e)))?;

    // Early return if no data
    if total_elections == 0 {
        info!("No quorum elections found");
        return Ok(success_response(PaginatedResponse {
            items: vec![],
            total_items: 0,
            page: params.page,
            page_size: params.page_size,
        }));
    }

    // Calculate and validate page bounds
    let total_pages = (total_elections + params.page_size - 1) / params.page_size;
    if params.page > total_pages {
        return Err(ApiError::InvalidPagination(format!("Page {} does not exist. Total pages: {}", params.page, total_pages)).into());
    }

    let quorum_elections: Vec<QuorumResharingInfoDb> = quorum_resharing_info::table
        .order(quorum_resharing_info::kafka_timestamp.desc())
        .offset(offset)
        .limit(params.page_size)
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query elections: {}", e)))?;

    // Batch fetch multiplier info to avoid N+1 queries
    let quorum_ids: Vec<i64> = quorum_elections.iter().map(|q| q.id).collect();
    let multiplier_infos: Vec<MultiplierInfo> = multiplier_info::table
        .filter(multiplier_info::quorum_info_id.eq_any(&quorum_ids))
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query multiplier info: {}", e)))?;

    // Group members by quorum for efficient lookup
    let mut members_by_quorum: HashMap<i64, Vec<String>> = HashMap::new();
    for multiplier in multiplier_infos {
        members_by_quorum.entry(multiplier.quorum_info_id).or_insert_with(Vec::new).push(multiplier.multiplier_peer_id);
    }

    let election_responses: Vec<QuorumElectionResponse> = quorum_elections
        .into_iter()
        .map(|e| QuorumElectionResponse {
            id: e.id.to_string(),
            status: e.success,
            kafka_timestamp: e.kafka_timestamp,
            members: members_by_quorum.get(&e.id).cloned().unwrap_or_default(),
        })
        .collect();

    info!(total_elections = total_elections, returned_items = election_responses.len(), "Successfully retrieved quorum elections");

    Ok(success_response(PaginatedResponse {
        items: election_responses,
        total_items: total_elections,
        page: params.page,
        page_size: params.page_size,
    }))
}

/// GET /api/v1/requests
/// Returns all requests, with pagination.
#[instrument(
    name = "get_requests",
    skip(pool),
    fields(page = %params.page, page_size = %params.page_size)
)]
pub async fn get_requests(
    State(AppState { pool, .. }): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ApiResponse<PaginatedResponse<RequestResponse>>>, (StatusCode, String)> {
    validate_pagination(&params)?;

    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let offset = (params.page - 1) * params.page_size;

    let total_requests: i64 = requests::table
        .count()
        .get_result(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to count requests: {}", e)))?;

    if total_requests == 0 {
        info!("No requests found");
        return Ok(success_response(PaginatedResponse {
            items: vec![],
            total_items: 0,
            page: params.page,
            page_size: params.page_size,
        }));
    }

    let total_pages = (total_requests + params.page_size - 1) / params.page_size;
    if params.page > total_pages {
        return Err(ApiError::InvalidPagination(format!("Page {} does not exist. Total pages: {}", params.page, total_pages)).into());
    }

    let fetched_requests: Vec<Requests> = requests::table
        .order(requests::kafka_timestamp.desc())
        .offset(offset)
        .limit(params.page_size)
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query requests: {}", e)))?;

    let request_responses: Vec<RequestResponse> = fetched_requests
        .into_iter()
        .map(|r| RequestResponse {
            request_id: r.request_id,
            user_address: r.user_address,
            method: r.method,
            kafka_timestamp: r.kafka_timestamp,
        })
        .collect();

    info!(total_requests = total_requests, returned_items = request_responses.len(), "Successfully retrieved requests");

    Ok(success_response(PaginatedResponse {
        items: request_responses,
        total_items: total_requests,
        page: params.page,
        page_size: params.page_size,
    }))
}

/// GET /api/v1/total_keys_generated
/// Returns the total count of unique request_ids.
#[instrument(name = "get_total_keys_generated", skip(pool))]
pub async fn get_total_keys_generated(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<TotalKeysGenerated>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let count: i64 = requests::table
        .count()
        .get_result(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to count total keys: {}", e)))?;

    info!(total_keys = count, "Successfully retrieved total keys generated");

    Ok(success_response(TotalKeysGenerated { count }))
}

/// GET /api/v1/keys_generated_chart
/// Returns the chart data for keys generated.
#[instrument(name = "get_keys_generated_chart", skip(pool))]
pub async fn get_keys_generated_chart(
    State(AppState { pool, .. }): State<AppState>,
    Query(params): Query<ChartTimeframeParams>,
) -> Result<Json<ApiResponse<KeysGeneratedChartData>>, (StatusCode, String)> {
    if !params.all_time {
        validate_timeframe(&params)?;
    }
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;
    // 1. Calculate the cutoff date based on params.days
    let cutoff_date = if params.all_time {
        Utc.timestamp_opt(0, 0).unwrap()
    } else {
        Utc::now() - Duration::days(params.days)
    };
    let chart_data = requests::table
        .filter(requests::kafka_timestamp.gt(cutoff_date))
        .select((sql::<Timestamptz>("DATE_TRUNC('day', kafka_timestamp)"), count_distinct(requests::request_id)))
        .group_by(sql::<Timestamptz>("DATE_TRUNC('day', kafka_timestamp)"))
        .order_by(sql::<Timestamptz>("DATE_TRUNC('day', kafka_timestamp)").asc())
        .load::<(DateTime<Utc>, i64)>(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query keys generated chart: {}", e)))?
        .into_iter()
        .map(|(timestamp, count)| KeyCount { timestamp, count })
        .collect();

    info!(chart_data = ?chart_data, "Successfully retrieved keys generated chart");

    Ok(success_response(KeysGeneratedChartData { data_points: chart_data }))
}

/// GET /api/v1/user_credits
/// Returns exhausted credits per method per user.
#[instrument(name = "get_user_credits", skip(pool))]
pub async fn get_user_credits(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<Vec<UserCreditsResponse>>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let credits: Vec<UserCredits> = user_credits::table
        .order(user_credits::kafka_timestamp.desc()) // Add ordering for consistency
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query user credits: {}", e)))?;

    let user_credits_response: Vec<UserCreditsResponse> = credits
        .into_iter()
        .map(|uc| UserCreditsResponse {
            user_address: uc.user_address,
            method: uc.method,
            exhausted_credits: uc.exhausted_credits,
            kafka_timestamp: uc.kafka_timestamp,
        })
        .collect();

    info!(user_count = user_credits_response.len(), "Successfully retrieved user credits");

    Ok(success_response(user_credits_response))
}

/// GET /api/v1/multiplier_computed_requests
/// Returns how many requests each multiplier has computed.
#[instrument(name = "get_multiplier_computed_requests", skip(pool))]
pub async fn get_multiplier_computed_requests(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<Vec<MultiplierComputedRequests>>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let result = multiplier_served_requests::table
        .group_by(multiplier_served_requests::multiplier_peer_id)
        .select((multiplier_served_requests::multiplier_peer_id, diesel::dsl::count(multiplier_served_requests::id)))
        .order(diesel::dsl::count(multiplier_served_requests::id).desc()) // Order by request count
        .load::<(String, i64)>(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query multiplier computed requests: {}", e)))?;

    let response: Vec<MultiplierComputedRequests> = result
        .into_iter()
        .map(|(peer_id, count)| MultiplierComputedRequests {
            multiplier_peer_id: peer_id,
            request_count: count,
        })
        .collect();

    info!(
        multiplier_count = response.len(),
        total_requests = response.iter().map(|r| r.request_count).sum::<i64>(),
        "Successfully retrieved multiplier computed requests"
    );

    Ok(success_response(response))
}

/// GET /api/v1/multiplier_elected_quorums
/// Returns for how many successful quorums each multiplier has been elected.
#[instrument(name = "get_multiplier_elected_quorums", skip(pool))]
pub async fn get_multiplier_elected_quorums(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<Vec<MultiplierQuorumElections>>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let result = multiplier_info::table
        .inner_join(quorum_resharing_info::table)
        .filter(quorum_resharing_info::success.eq(true))
        .group_by(multiplier_info::multiplier_peer_id)
        .select((multiplier_info::multiplier_peer_id, diesel::dsl::count_distinct(multiplier_info::quorum_info_id)))
        .order(diesel::dsl::count_distinct(multiplier_info::quorum_info_id).desc())
        .load::<(String, i64)>(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query multiplier elected quorums: {}", e)))?;

    let response: Vec<MultiplierQuorumElections> = result
        .into_iter()
        .map(|(peer_id, count)| MultiplierQuorumElections {
            multiplier_peer_id: peer_id,
            elected_quorum_count: count,
        })
        .collect();

    info!(
        multiplier_count = response.len(),
        total_elections = response.iter().map(|r| r.elected_quorum_count).sum::<i64>(),
        "Successfully retrieved multiplier elected quorums"
    );

    Ok(success_response(response))
}

/// GET /api/v1/peers_reachability_status/tcp
/// Returns a list of peers with TCP reachability status.
#[instrument(name = "get_peers_reachability_status_tcp", skip(pool))]
pub async fn get_peers_reachability_status_tcp(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<Vec<PeerReachabilityStatus>>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let peers: Vec<PeerReachabilityTcp> = peer_reachability_tcp::table
        .order(peer_reachability_tcp::kafka_timestamp.desc())
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query TCP reachability: {}", e)))?;

    let response: Vec<PeerReachabilityStatus> = peers
        .into_iter()
        .map(|p| PeerReachabilityStatus {
            peer_id: p.multiplier_peer_id,
            last_checked_timestamp: p.kafka_timestamp,
            success: p.success,
            details: p.rpc_url,
        })
        .collect();

    let successful_count = response.iter().filter(|p| p.success).count();
    info!(total_peers = response.len(), successful_peers = successful_count, "Successfully retrieved TCP reachability status");

    Ok(success_response(response))
}

/// GET /api/v1/peers_reachability_status/quic
/// Returns a list of peers with QUIC reachability status.
#[instrument(name = "get_peers_reachability_status_quic", skip(pool))]
pub async fn get_peers_reachability_status_quic(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<Vec<PeerReachabilityStatus>>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let peers: Vec<PeerReachabilityQuic> = peer_reachability_quic::table
        .order(peer_reachability_quic::kafka_timestamp.desc())
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query QUIC reachability: {}", e)))?;

    let response: Vec<PeerReachabilityStatus> = peers
        .into_iter()
        .map(|p| PeerReachabilityStatus {
            peer_id: p.multiplier_peer_id,
            last_checked_timestamp: p.kafka_timestamp,
            success: p.success,
            details: p.duration_micros.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string()),
        })
        .collect();

    let successful_count = response.iter().filter(|p| p.success).count();

    info!(total_peers = response.len(), successful_peers = successful_count, "Successfully retrieved QUIC reachability status");

    Ok(success_response(response))
}

// Helper to format addresses for display
fn format_address(bytes: &[u8]) -> String { format!("0x{}", hex::encode(bytes)) }

/// Handler to display the points accumulated by each operator.
#[instrument(name = "get_operator_points", skip(pool))]
pub async fn get_operator_points(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<Vec<OperatorPointsResponse>>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(e.to_string()))?;

    // Query 1: Get points for performers
    let performer_points_query: Vec<(Vec<u8>, i64)> = task_performers::table
        .inner_join(tasks::table)
        .group_by(task_performers::performer)
        .select((task_performers::performer, sql::<diesel::sql_types::BigInt>("SUM(task_size)::BIGINT")))
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(e.to_string()))?;

    // Query 2: Get points for attestors
    let attestor_points_query: Vec<(Vec<u8>, i64)> = task_attestors::table
        .inner_join(tasks::table)
        .group_by(task_attestors::attestor)
        .select((task_attestors::attestor, sql::<diesel::sql_types::BigInt>("SUM(task_size)::BIGINT")))
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(e.to_string()))?;

    // Combine the results into a single map
    let mut operator_points: HashMap<Vec<u8>, f64> = HashMap::new();

    // Add performer points (1x multiplier)
    for (addr, size) in performer_points_query {
        *operator_points.entry(addr).or_insert(0.0) += size as f64;
    }

    // Add attestor points (0.1x multiplier)
    for (addr, size) in attestor_points_query {
        *operator_points.entry(addr).or_insert(0.0) += 0.1 * size as f64;
    }

    // Convert the points map to the final response format
    let mut response_data: Vec<OperatorPointsResponse> = operator_points
        .into_iter()
        .map(|(addr_bytes, points)| OperatorPointsResponse {
            address: format_address(&addr_bytes),
            points,
        })
        .collect();

    // Sort for a consistent output
    response_data.sort_by(|a, b| b.points.partial_cmp(&a.points).unwrap_or(std::cmp::Ordering::Equal));

    Ok(success_response(response_data))
}

/// Handler to display the points accumulated by each operator (ledger-based).
#[instrument(name = "get_operator_points_v2", skip(pool))]
pub async fn get_operator_points_v2(State(AppState { pool, .. }): State<AppState>) -> Result<Json<ApiResponse<Vec<OperatorPointsResponse>>>, (StatusCode, String)> {
    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(e.to_string()))?;

    // Aggregate directly from ledger
    let results: Vec<(Vec<u8>, f64)> = operator_points_ledger::table
        .group_by(operator_points_ledger::operator)
        .select((operator_points_ledger::operator, sql::<Double>("SUM(points)")))
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(e.to_string()))?;

    // Convert into response format
    let mut response_data: Vec<OperatorPointsResponse> = results
        .into_iter()
        .map(|(addr_bytes, points)| OperatorPointsResponse {
            address: format_address(&addr_bytes),
            points,
        })
        .collect();

    // Sort descending
    response_data.sort_by(|a, b| b.points.partial_cmp(&a.points).unwrap_or(std::cmp::Ordering::Equal));

    Ok(success_response(response_data))
}

#[instrument(
    name = "get_tasks",
    skip(pool),
    fields(page = %params.page, page_size = %params.page_size)
)]
pub async fn get_tasks(State(AppState { pool, .. }): State<AppState>, Query(params): Query<PaginationParams>) -> Result<Json<ApiResponse<PaginatedResponse<TaskResponse>>>, (StatusCode, String)> {
    validate_pagination(&params)?;

    let mut conn = pool.get().map_err(|e| ApiError::DatabaseConnection(format!("Failed to get connection: {}", e)))?;

    let offset = (params.page - 1) * params.page_size;

    let total_tasks: i64 = tasks::table
        .count()
        .get_result(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to count tasks: {}", e)))?;

    if total_tasks == 0 {
        info!("No tasks found");
        return Ok(success_response(PaginatedResponse {
            items: vec![],
            total_items: 0,
            page: params.page,
            page_size: params.page_size,
        }));
    }

    let total_pages = (total_tasks + params.page_size - 1) / params.page_size;
    if params.page > total_pages {
        return Err(ApiError::InvalidPagination(format!("Page {} does not exist. Total pages: {}", params.page, total_pages)).into());
    }

    let fetched_tasks: Vec<Task> = tasks::table
        .order(tasks::timestamp.desc())
        .offset(offset)
        .limit(params.page_size)
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(format!("Failed to query tasks: {}", e)))?;

    // Load performers and attestors for the fetched tasks
    let task_ids: Vec<i64> = fetched_tasks.iter().map(|t| t.id).collect();

    let all_performers: Vec<TaskPerformers> = task_performers::table
        .filter(task_performers::task_id.eq_any(&task_ids))
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(e.to_string()))?;

    let all_attestors: Vec<TaskAttestors> = task_attestors::table
        .filter(task_attestors::task_id.eq_any(&task_ids))
        .load(&mut conn)
        .map_err(|e| ApiError::DatabaseQuery(e.to_string()))?;

    // Create hashmaps for efficient lookup
    let performers_map: HashMap<i64, Vec<Vec<u8>>> = all_performers.into_iter().fold(HashMap::new(), |mut acc, p| {
        acc.entry(p.task_id).or_default().push(p.performer);
        acc
    });

    let attestors_map: HashMap<i64, Vec<Vec<u8>>> = all_attestors.into_iter().fold(HashMap::new(), |mut acc, a| {
        acc.entry(a.task_id).or_default().push(a.attestor);
        acc
    });

    // Map data to the response struct
    let task_responses: Vec<TaskResponse> = fetched_tasks
        .into_iter()
        .map(|task| {
            let performers: Vec<String> = performers_map.get(&task.id).unwrap_or(&Vec::new()).iter().map(|p| format_address(p)).collect();

            let attestors: Vec<String> = attestors_map.get(&task.id).unwrap_or(&Vec::new()).iter().map(|a| format_address(a)).collect();

            TaskResponse {
                task_number: task.task_number,
                timestamp: task.timestamp,
                task_definition_id: task.task_definition_id,
                task_size: task.task_size,
                proof_of_task: task.proof_of_task,
                performers,
                attestors,
                transaction_hash: format_address(&task.transaction_hash),
            }
        })
        .collect();

    info!(total_tasks = total_tasks, returned_items = task_responses.len(), "Successfully retrieved tasks");

    Ok(success_response(PaginatedResponse {
        items: task_responses,
        total_items: total_tasks,
        page: params.page,
        page_size: params.page_size,
    }))
}

/// Handler to display the cumulative TVL across EL and symbiotic.
/// Optimized with concurrency and 1-day caching.
#[instrument(name = "get_network_tvl", skip(app_state))]
pub async fn get_network_tvl(State(mut app_state): State<AppState>) -> Result<Json<ApiResponse<TotalNetworkTvl>>, (StatusCode, String)> {
    let mut warnings = Vec::new();

    // 1. Check cache (Return immediately if fresh)
    if let Some(cached_tvl) = app_state.tvl_cache.get(&()) {
        info!("Returning cached TVL value");
        return Ok(success_response(TotalNetworkTvl { tvl_usd: *cached_tvl, warnings: None }));
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8)) // Don't let one API hang the whole request
        .build()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let eigen_api_key = env::var("EIGEN_API_KEY").unwrap_or_default();
    let cmc_api_key = env::var("CMC_API_KEY").unwrap_or_default();

    // 2. Concurrent Fetch
    let (symbiotic_result, eigen_result, cmc_result) = tokio::join!(fetch_symbiotic_tvl(&client), fetch_eigen_tvl(&client, &eigen_api_key), fetch_eth_price(&client, &cmc_api_key));

    // 3. Selective Unwrapping
    let symbiotic_usd = match symbiotic_result {
        Ok(val) => val,
        Err(e) => {
            warnings.push(format!("Symbiotic data unavailable: {}", e));
            0.0
        }
    };

    let eigen_eth = match eigen_result {
        Ok(val) => val,
        Err(e) => {
            warnings.push(format!("EigenLayer data unavailable: {}", e));
            0.0
        }
    };

    let eth_price = match cmc_result {
        Ok(val) => val,
        Err(e) => {
            warnings.push(format!("CoinMarketCap ETH price data unavailable: {}", e));
            0.0
        }
    };

    // 4. Final Calculation
    let total_restake_usd = (eigen_eth * eth_price) + symbiotic_usd;

    // 5. Intelligent Caching
    // Only update the cache if we actually got a full set of data.
    // If warnings is NOT empty, we provide the best-effort result but don't overwrite
    // the previous "good" cache.
    if warnings.is_empty() {
        app_state.tvl_cache.insert((), total_restake_usd);
    }

    info!("New TVL value calculated and cached: {}", total_restake_usd);
    Ok(success_response(TotalNetworkTvl {
        tvl_usd: total_restake_usd,
        warnings: Some(warnings),
    }))
}

#[instrument(skip(client))]
async fn fetch_symbiotic_tvl(client: &reqwest::Client) -> Result<f64, ApiError> {
    let response = client
        .get("https://app.symbiotic.fi/api/v2/networks/0x42F15F9E4dF4994317453477e80e24797CC1A929")
        .send()
        .await?
        .json::<SymbioticResponse>()
        .await?;
    Ok(response.stake_usd)
}

#[instrument(skip(client, api_key))]
async fn fetch_eigen_tvl(client: &reqwest::Client, api_key: &str) -> Result<f64, ApiError> {
    let response = client
        .get("https://api.eigenexplorer.com/avs/0x42F15F9E4dF4994317453477e80e24797CC1A929?withTvl=true")
        .header("X-API-Token", api_key)
        .send()
        .await?
        .json::<EigenResponse>()
        .await?;
    Ok(response.tvl.tvl)
}

#[instrument(skip(client, api_key))]
async fn fetch_eth_price(client: &reqwest::Client, api_key: &str) -> Result<f64, ApiError> {
    let response = client
        .get("https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?id=1027") // ID 1027 is Ethereum
        .header("X-CMC_PRO_API_KEY", api_key)
        .header("Accept", "application/json")
        .send()
        .await?
        .json::<CmcResponse>()
        .await?;
    Ok(response.data.ethereum.quote.usd.price)
}

#[cfg(test)]
mod tests {
    use dotenv::dotenv;

    use super::*;

    #[test]
    fn test_pagination_validation() {
        assert!(validate_pagination(&PaginationParams { page: 0, page_size: 20 }).is_err());
        assert!(validate_pagination(&PaginationParams { page: 1, page_size: 0 }).is_err());
        assert!(validate_pagination(&PaginationParams { page: 1, page_size: 101 }).is_err());
        assert!(validate_pagination(&PaginationParams { page: 1, page_size: 50 }).is_ok());
    }

    #[test]
    fn test_error_conversion() {
        let error = ApiError::InvalidPagination("test".to_string());
        let (status, message) = error.into();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(message, "test");
    }
    #[tokio::test]
    async fn test_fetch_network_tvl() {
        dotenv().ok();
        let app_state = AppState::default();
        let result = get_network_tvl(State(app_state)).await;
        println!("{:?}", result);
        assert!(result.is_ok());
    }
}
