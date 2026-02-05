use anyhow::anyhow;
use axum::{extract::State, routing::post, Json, Router};
use ethers::providers::{Http, Provider};
use messages::{
    network_utils::AVSError,
    task_proofs::{IPFSTaskProof, TaskProof, VerifierResponse},
};
use redis::Connection;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
#[derive(Clone)]
struct AppState {
    // current_epoch: Arc<Mutex<u32>>, // Always 0 for the first testnet
    // /// Number of nodes with keyshares
    // n: u32,
    // /// Threshold of nodes
    // t: u32,
    // /// Extra nodes that should be queried in addition to t for redundancy
    // epsilon: u32,
    // prover_metatadata: ProverMetadata,
    // keyshares: Keyshares,
    ipfs_host: String,
    redis: Arc<Mutex<Connection>>,
    eth: Provider<Http>,
}
// /// Maps prover index as a string to its public keys
// /// Unfortunately this is not typed because serializing and desieralizign is easier when it is not typed
// type ProverMetadata = Value;
// /// Key is a string representation of the tuple (Curve::NAME, Method enum variant)
// /// Value is the public key for that curve and method
// type PublicKeys = Map<String, Vec<u8>>;
#[tokio::main]
async fn main() {
    let state = AppState {
        redis: Arc::new(Mutex::new(
            redis::Client::open(env::var("REDIS_URL").expect("REDIS_URL not set"))
                .expect("Failed to open Redis connection")
                .get_connection()
                .expect("Failed to get Redis connection"),
        )),
        // current_epoch: Arc::new(Mutex::new(0)),
        // n: env::var("THRESHOLD_N").expect("THRESHOLD_N not set").parse().expect("Failed to parse THRESHOLD_N"),
        // t: env::var("THRESHOLD_T").expect("THRESHOLD_T not set").parse().expect("Failed to parse THRESHOLD_T"),
        // epsilon: env::var("THRESHOLD_EPSILON").expect("THRESHOLD_EPSILON not set").parse().expect("Failed to parse THRESHOLD_EPSILON"),
        ipfs_host: env::var("IPFS_HOST").expect("IPFS_HOST not set"),
        eth: Provider::<Http>::try_from(env::var("L1_RPC").expect("L1_RPC note set")).expect("Failed to instatiate ETH provider"),
    };
    let app = Router::new().route("/task/validate", post(verify_task)).layer(TraceLayer::new_for_http()).with_state(state);
    tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();
    // run it
    let listener = tokio::net::TcpListener::bind("0.0.0.0:4002").await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
#[axum::debug_handler]
async fn verify_task(
    // ConnectInfo(socketaddr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(ipfs_task_proof): Json<IPFSTaskProof>,
) -> Result<Json<VerifierResponse>, AVSError> {
    println!("Recieved task proof: {}", serde_json::to_string(&ipfs_task_proof).unwrap());
    let proof: TaskProof = surf::get(format!("{}{}", state.ipfs_host, ipfs_task_proof.proof_of_task))
        .recv_json()
        .await
        .map_err(|_| anyhow!("Failed to get task proof from IPFS"))?;
    proof.verify().await?;
    Ok(Json(VerifierResponse { data: ipfs_task_proof }))
}
