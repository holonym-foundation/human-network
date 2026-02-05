use axum::{
    extract::{ConnectInfo, State},
    routing::post,
    Json, Router,
};
use messages::network_utils::{AVSError, NodeData, NodeDataWithoutIP};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
#[derive(Clone)]
struct AppState {
    active_provers: Arc<Mutex<Vec<NodeData>>>,
}
#[tokio::main]
async fn main() {
    let state = AppState {
        active_provers: Arc::new(Mutex::new(vec![])),
    };
    let app = Router::new()
        .route("/register-prover/", post(register_prover))
        .route("/log/", post(|| async { "Not yet implemented" }))
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();
    // run it
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
/// Logs a single request
#[axum::debug_handler]
async fn register_prover(ConnectInfo(socketaddr): ConnectInfo<SocketAddr>, State(state): State<AppState>, Json(without_ip): Json<NodeDataWithoutIP>) -> Result<(), AVSError> {
    let ip = socketaddr.ip().to_string();
    (*state.active_provers.lock().await).push(NodeData {
        idx: without_ip.idx,
        uri: format!("{}:PORT", ip),
        pubkeys: without_ip.pubkeys,
    });
    println!(
        "Registered prover: {}\nProvers are now: {}",
        without_ip.idx,
        serde_json::to_string(&*state.active_provers.lock().await).unwrap()
    );
    Ok(())
}
