//! This simple axum server checks for a valid Clerk JWT for the Silk App and rate-limits per IP address.
//! If these conditions are met, it will sign the JWT, send it to Human Network, and return the response.
//! If the conditions are not met, it will return an error.
use anyhow::anyhow;
use axum::body::Body;
use axum::extract::{ConnectInfo, FromRequestParts, State};
use axum::http::request::{self, Parts};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Json;
use axum::{routing::get, Router};
use ethers::signers::{LocalWallet, Signer, Wallet};
use http::{Response, StatusCode};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use lazy_static::lazy_static;
use messages::jwt::verify_enclave_jwt;
use messages::network_utils::{Method, RequestToNetwork};
use messages::types::{NodeResponse, StateRequest, StateResponse};
use human_crypto::curve::EncodedBabyJubJubPoint;
use human_crypto::zkinjmask::JWTClaims;
use human_crypto::{Curve, Secp256k1};
use redis::Commands;
use redis::Connection;
use rpc_trait::rpc::HumanRpcClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn, debug};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod datadog_client;
use crate::datadog_client::{DatadogClient, init_datadog_client, LogLevel};

/// Allows caching the jwk endpoint to reduce request latency and prevent rate limiting
/// It is important JWK_CACHE_SEC is short for security and UX at the time of key rotation
const JWK_CACHE_SEC: u64 = 600;

lazy_static! {
    static ref ISS_DEV: String = env::var("ISS_DEV")
        .unwrap_or_else(|_| "https://epic-eft-18.clerk.accounts.dev".to_string());
    static ref ISS_STAGING: String = env::var("ISS_STAGING")
        .unwrap_or_else(|_| "https://clerk.staging-silkysignon.com".to_string());
    static ref ISS_PROD: String = env::var("ISS_PROD")
        .unwrap_or_else(|_| "https://clerk.humansignon.com".to_string());
    static ref AZP_WHITELIST: Vec<String> = {
        env::var("AZP_WHITELIST")
            .map(|s| s.split(',').map(|url| url.trim().to_string()).collect())
            .unwrap_or_else(|_| vec![
                "http://127.0.0.1:3000".to_string(),
                "http://localhost:3000".to_string(),
            ])
    };
}

/// Since the Clerk setup only has one key, this is always 0 (the first key)
const JWK_KEY_IDX: usize = 0;

// 100 is just for development. We should set this lower in production.
const RATE_LIMIT_NUM_REQUESTS: u64 = 100;
const RATE_LIMIT_TIME_INTERVAL: u64 = 86400;

// Constants for request retry logic
const MAX_RETRIES: u32 = 5;
const RETRY_DELAY_MS: u64 = 300;
// The HN relayer responds with "Submitted" after 60s. If we do not get a response after 65s,
// we can be confident that the request failed.
const HUMAN_NETWORK_REQ_TIMEOUT_SECS: u64 = 65;

/// Stores the state of the application between threads and routes. Note that Redis connection is not necessarily thread safe, so we wrap it in a Mutex.
#[derive(Clone)]
pub struct AppState {
    pub client: HttpClient,
    pub wallet: LocalWallet,
    pub redis_conn: Arc<Mutex<Connection>>,
    pub next_request_number_jwtprf: Arc<Mutex<u128>>,
    pub next_request_number_oprf_secp256k1: Arc<Mutex<u128>>,
    pub next_request_number_oprf_babyjubjub: Arc<Mutex<u128>>,
    pub epoch_num: Arc<Mutex<u32>>,
    pub allowed_methods: HashSet<Method>,
    pub datadog_client: Option<DatadogClient>,
}
pub trait AppStateTrait {
    fn redis_connection(&self) -> Arc<Mutex<Connection>>;
    fn datadog_client(&self) -> Option<DatadogClient>;
}
impl AppStateTrait for AppState {
    fn redis_connection(&self) -> Arc<Mutex<Connection>> { self.redis_conn.clone() }
    fn datadog_client(&self) -> Option<DatadogClient> { self.datadog_client.clone() }
}

#[derive(Debug)]
enum SignerEnv {
    Dev,
    Staging,
    Prod,
}
impl FromStr for SignerEnv {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "dev" => Ok(SignerEnv::Dev),
            "staging" => Ok(SignerEnv::Staging),
            "prod" => Ok(SignerEnv::Prod),
            _ => Err(format!("Invalid SignerEnv: {}", s)),
        }
    }
}

fn get_azp_whitelist() -> Vec<String> {
    AZP_WHITELIST.clone()
}

/// Retrieves the jwk endpoint, caching the result for JWK_CACHE_SEC seconds
/// It is important for security and UX around key rotation that JWK_CACHE_SEC is short
pub async fn get_jwk(issuer: &str, conn: Arc<Mutex<Connection>>) -> Result<Value, anyhow::Error> {
    let cached: Option<String> = conn.lock().await.get(format!("jwk:issuer:{}", issuer))?;
    match cached {
        Some(cached) => Ok(serde_json::from_str(&cached)?),
        None => {
            debug!(issuer = %issuer, "Fetching JWK from issuer (cache miss)");
            let jwk_endpoint = format!("{}/.well-known/jwks.json", issuer);
            let jwk: Value = reqwest::Client::new().get(&jwk_endpoint).send().await?.json().await?;
            // cache the result
            let _: Option<()> = conn.lock().await.set_ex(format!("jwk:issuer:{}", issuer), serde_json::to_string(&jwk).unwrap(), JWK_CACHE_SEC).ok();
            Ok(jwk)
        }
    }
}

pub async fn verify_jwt(jwt: &str, jwk: &Value) -> Result<String, anyhow::Error> {
    let key: &Value = jwk
        .get("keys")
        .ok_or(anyhow!("JWK lacks 'keys' field"))?
        .as_array()
        .ok_or(anyhow!("JWK 'keys' is not an array"))?
        .get(JWK_KEY_IDX)
        .ok_or(anyhow!("JWK key index is out of boudns"))?;
    let (modulus, exponent) = (
        key.get("n")
            .ok_or(anyhow!("JWK key (assuming RSA) is missing modulus"))?
            .as_str()
            .ok_or(anyhow!("key has invalid format"))?,
        key.get("e")
            .ok_or(anyhow!("JWK key (assuming RSA) is missing exponent"))?
            .as_str()
            .ok_or(anyhow!("key has invalid format"))?,
    );
    let key = DecodingKey::from_rsa_components(&modulus, &exponent)?;
    let validation = Validation::new(Algorithm::RS256);
    let token: jsonwebtoken::TokenData<_> = decode::<JWTClaims>(&jwt, &key, &validation)?;
    if token.claims.iss != ISS_PROD.as_str() && token.claims.iss != ISS_DEV.as_str() && token.claims.iss != ISS_STAGING.as_str() {
        return Err(anyhow!("Invalid issuer"));
    }
    let azp = token.claims.azp.as_ref().unwrap().as_str();
    if !get_azp_whitelist().iter().any(|url| url.as_str() == azp) {
        return Err(anyhow!("Invalid azp"));
    }
    Ok(token.claims.sub)
}

pub struct ClerkSession {
    pub user_id: String,
}

impl ClerkSession {
    async fn from_session_token_staging(session_token: &str, conn: Arc<Mutex<Connection>>) -> Result<Self, anyhow::Error> {
        // Bypass if from enclave
        let unvalidated = JWTClaims::from_raw_token_unchecked(session_token)?;
        if unvalidated.from_enclave.unwrap_or(false) {
            // verify it and if successful, return the unique string and public mask
            verify_enclave_jwt(session_token)?;
            return Ok(ClerkSession { user_id: unvalidated.sub });
        }
        // The following, where we verify the JWT for each environment, is just for devnet. We should not do
        // this in production. In production, we should just fetch the prod JWK.
        let prod_jwk = get_jwk(ISS_PROD.as_str(), conn.clone()).await.map_err(|e| anyhow!(format!("Failed to get JWK: {}", e.to_string())))?;
        let prod_user_id = verify_jwt(session_token, &prod_jwk).await.map_err(|e| anyhow!(format!("Could not verify JWT: {}", e.to_string())));
        let dev_jwk = get_jwk(ISS_DEV.as_str(), conn.clone()).await.map_err(|e| anyhow!(format!("Failed to get JWK: {}", e.to_string())))?;
        let dev_user_id = verify_jwt(session_token, &dev_jwk).await.map_err(|e| anyhow!(format!("Could not verify JWT: {}", e.to_string())));
        let staging_jwk = get_jwk(ISS_STAGING.as_str(), conn.clone()).await.map_err(|e| anyhow!(format!("Failed to get JWK: {}", e.to_string())))?;
        let staging_user_id = verify_jwt(session_token, &staging_jwk).await.map_err(|e| anyhow!(format!("Could not verify JWT: {}", e.to_string())));
        // Return a list of errors as a single error if all attempts at JWT verification failed
        if prod_user_id.is_err() && dev_user_id.is_err() && staging_user_id.is_err() {
            let prod_err_msg = prod_user_id.err().map_or("No error".to_string(), |e| e.to_string());
            let dev_err_msg = dev_user_id.err().map_or("No error".to_string(), |e| e.to_string());
            let staging_err_msg = staging_user_id.err().map_or("No error".to_string(), |e| e.to_string());
            error!(
                prod_error = %prod_err_msg,
                dev_error = %dev_err_msg, 
                staging_error = %staging_err_msg,
                "JWT verification failed across all environments"
            );
            return Err(anyhow!("Could not verify JWT. Errors: prod: {}, dev: {}, staging: {}", prod_err_msg, dev_err_msg, staging_err_msg));
        }
        let user_id = match (prod_user_id, dev_user_id, staging_user_id) {
            (Ok(prod_user_id), Err(_), Err(_)) => {
                debug!(user_id = %prod_user_id, environment = "prod", "User authenticated successfully");
                prod_user_id
            },
            (Err(_), Ok(dev_user_id), Err(_)) => {
                debug!(user_id = %dev_user_id, environment = "dev", "User authenticated successfully");
                dev_user_id
            },
            (Err(_), Err(_), Ok(staging_user_id)) => {
                debug!(user_id = %staging_user_id, environment = "staging", "User authenticated successfully");
                staging_user_id
            },
            _ => return Err(anyhow!("Could not determine user ID. This user ID might exist in multiple environments.")),
        };
        Ok(ClerkSession { user_id })
    }

    async fn from_session_token_prod(session_token: &str, conn: Arc<Mutex<Connection>>) -> Result<Self, anyhow::Error> {
        // The following, where we verify the JWT for each environment, is just for devnet. We should not do
        // this in production. In production, we should just fetch the prod JWK.
        let jwk = get_jwk(ISS_PROD.as_str(), conn.clone()).await.map_err(|e| anyhow!(format!("Failed to get JWK: {}", e.to_string())))?;
        let user_id = verify_jwt(session_token, &jwk).await.map_err(|e| anyhow!(format!("Could not verify JWT: {}", e.to_string())))?;
        Ok(ClerkSession { user_id })
    }

    pub async fn from_session_token(session_token: &str, conn: Arc<Mutex<Connection>>) -> Result<Self, anyhow::Error> {
        match get_signer_env() {
            // For dev and staging, we can use the same function
            SignerEnv::Dev | SignerEnv::Staging => Self::from_session_token_staging(session_token, conn).await,
            SignerEnv::Prod => Self::from_session_token_prod(session_token, conn).await,
        }
    }
}

struct OptionalClerkSession {
    session: Option<ClerkSession>
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for OptionalClerkSession
where
    S: AppStateTrait + Send + Sync,
{
    type Rejection = String;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        if let Some(session_token) = parts.headers.get("X-Silk-Clerk-JWT") {
            let session_token = session_token.to_str().map_err(|_| "No session token provided".to_string())?;
            let clerk_session = ClerkSession::from_session_token(session_token, state.redis_connection()).await.map_err(|e| e.to_string())?;
            Ok(Self { session: Some(clerk_session) })
        } else {
            Ok(Self { session: None })
        }
    }
}

fn parse_allowed_methods() -> HashSet<Method> {
    use std::env;
    let mut set = HashSet::new();
    if let Ok(methods) = env::var("ALLOWED_METHODS") {
        for m in methods.split(',') {
            let m = m.trim();
            if let Some(method) = Method::from_str(m) {
                set.insert(method);
            }
        }
    }
    set
}

// Identifier can either be the IP address, the user_id, or a combination of both
async fn rate_limit(identifier: String, conn: Arc<Mutex<Connection>>) -> Result<(), anyhow::Error> {
    let key = format!("rate_limit:{}", identifier);
    let rate_limit_time_interval: u64 = env::var("RATE_LIMIT_TIME_INTERVAL")
        .unwrap_or_else(|_| RATE_LIMIT_TIME_INTERVAL.to_string())
        .parse()
        .expect("RATE_LIMIT_TIME_INTERVAL must be a valid u64");
    let rate_limit_num_requests: u64 = env::var("RATE_LIMIT_NUM_REQUESTS")
        .unwrap_or_else(|_| RATE_LIMIT_NUM_REQUESTS.to_string())
        .parse()
        .expect("RATE_LIMIT_NUM_REQUESTS must be a valid u64");
    // Set the key if it doesn't exist using the SET command with the NX and EX options:
    redis::cmd("SET").arg(&key).arg(0).arg("NX").arg("EX").arg(rate_limit_time_interval).execute(&mut *conn.lock().await);
    let count: u64 = conn.lock().await.incr(&key, 1)?;
    if count > rate_limit_num_requests {
        return Err(anyhow!("Rate limit exceeded"));
    }
    Ok(())
}

async fn timeout_threshold_mul(
    client: &HttpClient,
    datadog_client: &Option<DatadogClient>,
    signed_request: RequestToNetwork,
) -> Result<NodeResponse, AppErrorWithStatus> {
    let timeout_result = timeout(
        Duration::from_secs(HUMAN_NETWORK_REQ_TIMEOUT_SECS),
        client.threshold_mul(signed_request)
    ).await;
    match timeout_result {
        Ok(result) => result.map_err(AppErrorWithStatus::from),
        Err(err) => {
            warn!(
                error = ?err,
                "Network request (threshold_mul) to Human Network timeout after {} seconds", HUMAN_NETWORK_REQ_TIMEOUT_SECS
            );
            dd_log!(
                datadog_client,
                LogLevel::Warning,
                error = ?err;
                format!("Network request (threshold_mul) to Human Network timeout after {} seconds", HUMAN_NETWORK_REQ_TIMEOUT_SECS)
            );
            Err(AppErrorWithStatus::from(err))
        }
    }
}

async fn timeout_fetch_threshold_mul_result(
    client: &HttpClient,
    datadog_client: &Option<DatadogClient>,
    request_id: String,
) -> Result<NodeResponse, AppErrorWithStatus> {
    let timeout_result = timeout(
        Duration::from_secs(HUMAN_NETWORK_REQ_TIMEOUT_SECS),
        client.fetch_threshold_mul_result(request_id)
    ).await;
    match timeout_result {
        Ok(result) => result.map_err(AppErrorWithStatus::from),
        Err(err) => {
            warn!(
                error = ?err,
                "Network request (fetch_threshold_mul_result) to Human Network timeout after {} seconds", HUMAN_NETWORK_REQ_TIMEOUT_SECS
            );
            dd_log!(
                datadog_client,
                LogLevel::Warning,
                error = ?err;
                format!("Network request (fetch_threshold_mul_result) to Human Network timeout after {} seconds", HUMAN_NETWORK_REQ_TIMEOUT_SECS)
            );
            Err(AppErrorWithStatus::from(err))
        }
    }
}

enum JsonResponse {
    VerifiedProofSecp256k1(Json<<Secp256k1 as Curve<32>>::Point>),
    VerifiedProofBabyJubJub(Json<EncodedBabyJubJubPoint>),
}

impl IntoResponse for JsonResponse {
    fn into_response(self) -> axum::response::Response {
        match self {
            JsonResponse::VerifiedProofSecp256k1(json) => json.into_response(),
            JsonResponse::VerifiedProofBabyJubJub(json) => json.into_response(),
        }
    }
}
/// This is the main handler for the server. It will check the JWT, rate limit, and sign the JWT if all conditions are met.
/// Then it will submit the request to the network, parse the response, and return it.
#[axum::debug_handler]
#[tracing::instrument(skip(state, addr, optional_session, rtn))]
async fn handle_secp256k1_request_to_network(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    optional_session: OptionalClerkSession,
    Json(rtn): Json<RequestToNetwork>,
) -> Result<JsonResponse, AppErrorWithStatus> {
    let session = optional_session.session;
    if state.allowed_methods.is_empty() || !state.allowed_methods.contains(&rtn.method) {
        return Err(AppErrorWithStatus::bad_request(format!("Method not allowed: {:?}", rtn.method)));
    }
    if matches!(rtn.method, Method::JWTPRFSecp256k1) && session.is_none() {
        return Err(AppErrorWithStatus::bad_request("Authentication required for JWTPRFSecp256k1".to_string()));
    }
    let stable_ip = addr.ip().to_string(); // Unlike IP, this does not include an ephemeral port so is consistent as a key for redis
    debug!(client_ip = %stable_ip, method = ?rtn.method, "Processing client request");
    if is_rate_limit_enabled() {
        // Rate limit User ID
        if let Some(session) = session {
            if let Err(e) = rate_limit(session.user_id.clone(), state.redis_connection()).await {
                warn!(
                    user_id = %session.user_id,
                    client_ip = %stable_ip,
                    method = ?rtn.method,
                    error = ?e,
                    "Rate limit exceeded for user"
                );
                
                // Send to Datadog
                dd_log!(
                    &state.datadog_client,
                    LogLevel::Warning,
                    user_id = %session.user_id,
                    client_ip = %stable_ip,
                    method = ?rtn.method,
                    error = ?e;
                    "Rate limit exceeded for user"
                );
                
                return Err(anyhow!("Rate limit exceeded for user_id: {}", session.user_id).into());
            }
        }
        // Rate limit IP
        if let Err(e) = rate_limit(stable_ip.clone(), state.redis_connection()).await {
            warn!(
                client_ip = %stable_ip,
                method = ?rtn.method, 
                error = ?e,
                "Rate limit exceeded for IP address"
            );
            
            // Send to Datadog
            dd_log!(
                &state.datadog_client,
                LogLevel::Warning,
                client_ip = %stable_ip,
                method = ?rtn.method,
                error = ?e;
                "Rate limit exceeded for IP address"
            );
            
            return Err(anyhow!("Rate limit exceeded for IP: {}", stable_ip).into());
        }
    }
    // Set request number
    let mut rtn = rtn.clone();
    match rtn.method {
        Method::JWTPRFSecp256k1 => {
            {
                let mut request_number = state.next_request_number_jwtprf.lock().await;
                *request_number += 1;
                rtn.request_per_user = *request_number;
            }
            // Braces autodrop the lock after the block!
            {
                rtn.epoch = *state.epoch_num.lock().await;
            }
            // Sign the JWT
            let signed = rtn.signed(state.wallet).await?;
            // Send the request to the network with retry logic
            let mut res = timeout_threshold_mul(&state.client, &state.datadog_client, signed.clone()).await?;
            
            // Retry logic for Submitted responses
            let mut retry_count = 0;
            
            while retry_count < MAX_RETRIES {
                match &res {
                    NodeResponse::VerifiedProofSecp256k1 { request_id, reconstructed_point } => {
                        info!(
                            method = ?rtn.method,
                            retries = retry_count,
                            "Request completed successfully"
                        );
                        
                        // Send success to Datadog
                        dd_log!(
                            &state.datadog_client,
                            LogLevel::Info,
                            method = ?rtn.method,
                            retries = %retry_count,
                            client_ip = %stable_ip;
                            "Request completed successfully"
                        );
                        
                        return Ok(JsonResponse::VerifiedProofSecp256k1(Json(reconstructed_point.clone())));
                    }
                    NodeResponse::Submitted { request_id } => {
                        if retry_count < MAX_RETRIES {
                            sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                            retry_count += 1;
                            res = timeout_fetch_threshold_mul_result(&state.client, &state.datadog_client, request_id.clone()).await?;
                        }
                    }
                    _ => break,
                }
            }
            
            // If we still have a Submitted response after retries, return an error
            if let NodeResponse::Submitted { request_id } = res {
                error!(
                    method = ?rtn.method,
                    retries = MAX_RETRIES,
                    "Request still pending after maximum retries"
                );
                Err(AppErrorWithStatus::bad_request(format!("Request still pending after {} retries", MAX_RETRIES)))
            } else if let NodeResponse::VerifiedProofSecp256k1 { request_id, reconstructed_point } = res {
                Ok(JsonResponse::VerifiedProofSecp256k1(Json(reconstructed_point)))
            } else {
                // TODO: Handle this case differently: "Something went wrong: Unexpected response from Human Network: Error { request_id: \"d40594aa-2eee-4624-abe6-85bed1a064b9\", message: \"Usr: 0x6a40…2e49 Error: No credits for a request: acquired 0 credits but used 0\" }"
                // We should implement email alerts or something like that for it.
                error!(
                    response = ?res,
                    method = ?rtn.method,
                    "Unexpected response from Human Network - possible service issue"
                );
                dd_log!(
                    &state.datadog_client,
                    LogLevel::Error,
                    response = ?res,
                    method = ?rtn.method;
                    "Unexpected response from Human Network - possible service issue"
                );
                Err(AppErrorWithStatus::bad_request(format!("Unexpected response from Human Network: {:?}", res)))
            }
        }
        Method::OPRFSecp256k1 => {
            {
                let mut request_number = state.next_request_number_oprf_secp256k1.lock().await;
                *request_number += 1;
                rtn.request_per_user = *request_number;
            }
            // Braces autodrop the lock after the block!
            {
                rtn.epoch = *state.epoch_num.lock().await;
            }
            let signed = match rtn.signed(state.wallet).await {
                Ok(signed) => signed,
                Err(e) => {
                    error!(error = ?e, method = ?rtn.method, "Failed to sign request");
                    return Err(e.into());
                }
            };
            let mut res = timeout_threshold_mul(&state.client, &state.datadog_client, signed.clone()).await?;
            
            // Retry logic for Submitted responses
            let mut retry_count = 0;
            
            while retry_count < MAX_RETRIES {
                match &res {
                    NodeResponse::VerifiedProofSecp256k1 { request_id, reconstructed_point } => {
                        info!(
                            method = ?rtn.method,
                            retries = retry_count,
                            "Request completed successfully"
                        );
                        return Ok(JsonResponse::VerifiedProofSecp256k1(Json(reconstructed_point.clone())));
                    }
                    NodeResponse::Submitted { request_id } => {
                        if retry_count < MAX_RETRIES {
                            sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                            retry_count += 1;
                            res = timeout_fetch_threshold_mul_result(&state.client, &state.datadog_client, request_id.clone()).await?;
                        }
                    }
                    _ => break,
                }
            }
            
            // If we still have a Submitted response after retries, return an error
            if let NodeResponse::Submitted { request_id } = res {
                Err(AppErrorWithStatus::bad_request(format!("Request still pending after {} retries", MAX_RETRIES)))
            } else if let NodeResponse::VerifiedProofSecp256k1 { request_id, reconstructed_point } = res {
                Ok(JsonResponse::VerifiedProofSecp256k1(Json(reconstructed_point)))
            } else {
                // TODO: Handle this case differently: "Something went wrong: Unexpected response from Human Network: Error { request_id: \"d40594aa-2eee-4624-abe6-85bed1a064b9\", message: \"Usr: 0x6a40…2e49 Error: No credits for a request: acquired 0 credits but used 0\" }"
                // We should implement email alerts or something like that for it.
                error!(
                    response = ?res,
                    method = ?rtn.method,
                    "Unexpected response from Human Network - possible service issue"
                );
                dd_log!(
                    &state.datadog_client,
                    LogLevel::Error,
                    response = ?res,
                    method = ?rtn.method;
                    "Unexpected response from Human Network - possible service issue"
                );
                Err(AppErrorWithStatus::bad_request(format!("Unexpected response from Human Network: {:?}", res)))
            }
        }
        Method::OPRFBabyJubJub => {
            {
                let mut request_number = state.next_request_number_oprf_babyjubjub.lock().await;
                *request_number += 1;
                rtn.request_per_user = *request_number;
            }
            // Braces autodrop the lock after the block!
            {
                rtn.epoch = *state.epoch_num.lock().await;
            }
            let signed = match rtn.signed(state.wallet).await {
                Ok(signed) => signed,
                Err(e) => {
                    error!(error = ?e, method = ?rtn.method, "Failed to sign request");
                    return Err(e.into());
                }
            };
            let mut res = timeout_threshold_mul(&state.client, &state.datadog_client, signed.clone()).await?;
            
            // Retry logic for Submitted responses
            let mut retry_count = 0;
            
            while retry_count < MAX_RETRIES {
                match &res {
                    NodeResponse::VerifiedProofBabyJubJub { request_id, reconstructed_point } => {
                        info!(
                            method = ?rtn.method,
                            retries = retry_count,
                            "Request completed successfully"
                        );
                        return Ok(JsonResponse::VerifiedProofBabyJubJub(Json(reconstructed_point.clone())));
                    }
                    NodeResponse::Submitted { request_id } => {
                        if retry_count < MAX_RETRIES {
                            sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                            retry_count += 1;
                            res = timeout_fetch_threshold_mul_result(&state.client, &state.datadog_client, request_id.clone()).await?;
                        }
                    }
                    _ => break,
                }
            }
            
            // If we still have a Submitted response after retries, return an error
            if let NodeResponse::Submitted { request_id } = res {
                error!(
                    method = ?rtn.method,
                    retries = MAX_RETRIES,
                    "Request still pending after maximum retries"
                );
                Err(AppErrorWithStatus::bad_request(format!("Request still pending after {} retries", MAX_RETRIES)))
            } else if let NodeResponse::VerifiedProofBabyJubJub { request_id, reconstructed_point } = res {
                Ok(JsonResponse::VerifiedProofBabyJubJub(Json(reconstructed_point)))
            } else {
                // TODO: Handle this case differently: "Something went wrong: Unexpected response from Human Network: Error { request_id: \"d40594aa-2eee-4624-abe6-85bed1a064b9\", message: \"Usr: 0x6a40…2e49 Error: No credits for a request: acquired 0 credits but used 0\" }"
                // We should implement email alerts or something like that for it.
                error!(
                    response = ?res,
                    method = ?rtn.method,
                    "Unexpected response from Human Network - possible service issue"
                );
                dd_log!(
                    &state.datadog_client,
                    LogLevel::Error,
                    response = ?res,
                    method = ?rtn.method;
                    "Unexpected response from Human Network - possible service issue"
                );
                Err(AppErrorWithStatus::bad_request(format!("Unexpected response from Human Network: {:?}", res)))
            }
        }
        _ => {
            return Err(AppErrorWithStatus::bad_request("Unsupported Human Network Method".to_string()));
        }
    }
}
fn get_signer_env() -> SignerEnv {
    let env = env::var("SIGNER_ENV").expect("SIGNER_ENV must be set");
    env.parse().expect("Failed to parse SignerEnv from SIGNER_ENV")
}
fn is_rate_limit_enabled() -> bool { env::var("RATE_LIMIT_ENABLED").unwrap_or_else(|_| "true".to_string()) == "true" }
#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "signer=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let env = get_signer_env();
    info!(environment = ?env, "Signer service starting");
    let rpc_url = env::var("HUMAN_RPC_URL").expect("HUMAN_RPC_URL must be set");
    info!(rpc_url = %rpc_url, "Connecting to Human Network");
    let client = HttpClientBuilder::default().build(&rpc_url).unwrap();
    let ping_result = client.ping().await.unwrap();
    info!(ping_result = ?ping_result, "Network connectivity verified");
    let wallet: LocalWallet = env::var("HUMAN_SIGNER_PRIVATE_KEY")
        .expect("HUMAN_SIGNER_PRIVATE_KEY must be set")
        .parse()
        .expect("Invalid private key");
    let redis_conn = Arc::new(Mutex::new(redis::Client::open("redis://localhost:6379").unwrap().get_connection().unwrap()));
    let is_rate_limit_enabled = is_rate_limit_enabled();
    info!(rate_limiting_enabled = is_rate_limit_enabled, "Rate limiting configuration");
    let signer_port = env::var("SIGNER_PORT").unwrap_or_else(|_| "3030".to_string()).parse::<u16>().expect("SIGNER_PORT must be a valid u16");
    let allowed_methods = parse_allowed_methods();
    
    // Initialize Datadog client if API key is provided
    let datadog_client = init_datadog_client();

    // let oprf_babyjub_requests = Arc::new(Mutex::new(0u128));
    // let oprf_secp256k1_requests = Arc::new(Mutex::new(0u128));

    let next_request_number_jwtprf = Arc::new(Mutex::new(1u128));
    let next_request_number_oprf_secp256k1 = Arc::new(Mutex::new(1u128));
    let next_request_number_oprf_babyjubjub = Arc::new(Mutex::new(1u128));
    let epoch_num = Arc::new(Mutex::new(0u32));

    if let StateResponse::Success {
        mut epoch, mut requests_from_user, ..
    } = client
        .fetch_state(StateRequest {
            method: Method::JWTPRFSecp256k1,
            user: wallet.address(),
        })
        .await
        .unwrap()
    {
        *epoch_num.lock().await = epoch;
        *next_request_number_jwtprf.lock().await = requests_from_user;
    }

    if let StateResponse::Success {
        mut epoch, mut requests_from_user, ..
    } = client
        .fetch_state(StateRequest {
            method: Method::OPRFSecp256k1,
            user: wallet.address(),
        })
        .await
        .unwrap()
    {
        *epoch_num.lock().await = epoch;
        *next_request_number_oprf_secp256k1.lock().await = requests_from_user;
    }

    if let StateResponse::Success {
        mut epoch, mut requests_from_user, ..
    } = client
        .fetch_state(StateRequest {
            method: Method::OPRFBabyJubJub,
            user: wallet.address(),
        })
        .await
        .unwrap()
    {
        *epoch_num.lock().await = epoch;
        *next_request_number_oprf_babyjubjub.lock().await = requests_from_user;
    }

    let state = AppState {
        client,
        wallet,
        redis_conn,
        next_request_number_jwtprf,
        next_request_number_oprf_secp256k1,
        next_request_number_oprf_babyjubjub,
        epoch_num,
        allowed_methods,
        datadog_client,
    };

    let cors = CorsLayer::new()
        // allow `GET` and `POST` when accessing the resource
        .allow_methods([http::Method::GET, http::Method::POST])
        // allow requests from any origin
        .allow_origin(Any)
        // allow any headers
        .allow_headers(Any);

    let app = Router::new().route("/", post(handle_secp256k1_request_to_network)).layer(cors).with_state(state);

    // Start the server
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", signer_port)).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    info!(address = %local_addr, port = signer_port, "Signer server listening");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}

// Error handling:
// Make our own error that wraps `anyhow::Error` and allows custom status codes.
struct AppErrorWithStatus {
    error: anyhow::Error,
    status: StatusCode,
}

impl AppErrorWithStatus {
    fn bad_request(msg: String) -> Self {
        AppErrorWithStatus {
            error: anyhow::anyhow!(msg),
            status: StatusCode::BAD_REQUEST,
        }
    }
    fn internal_error<E: Into<anyhow::Error>>(err: E) -> Self {
        AppErrorWithStatus {
            error: err.into(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// Tell axum how to convert `AppErrorWithStatus` into a response.
impl IntoResponse for AppErrorWithStatus {
    fn into_response(self) -> Response<Body> {
        (self.status, format!("Something went wrong: {}", self.error)).into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppErrorWithStatus>`. That way you don't need to do that manually.
impl<E> From<E> for AppErrorWithStatus
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        AppErrorWithStatus::internal_error(err)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // warning -- this will clear all redis data, even outside this application. this is only run for tests
    fn clear_redis() {
        if env::var("TEST_CLEAR_REDIS").unwrap_or("false".to_string()) != "true" {
            return;
        }
        let client = redis::Client::open("redis://localhost:6379").unwrap();
        let mut con = client.get_connection().unwrap();
        let _: () = redis::cmd("FLUSHALL").query(&mut con).unwrap();
    }

    #[tokio::test]
    async fn test_ip_rate_limit() {
        clear_redis();
        let redis_conn = Arc::new(Mutex::new(redis::Client::open("redis://localhost:6379").unwrap().get_connection().unwrap()));
        let ip = "1.2.3.4.5";
        for _ in 0..RATE_LIMIT_NUM_REQUESTS {
            rate_limit(ip.to_string(), redis_conn.clone()).await.expect("Rate limit exceeded");
        }
        assert!(rate_limit(ip.to_string(), redis_conn.clone()).await.is_err());
    }
    #[tokio::test]
    async fn integration_test_rate_limit_with_http_requests() { todo!() }
}
