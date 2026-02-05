use anyhow::anyhow;
use axum::{debug_handler, extract::State, http::Uri, routing::post, Json, Router};
use ethers::{
    providers::{Http, Provider},
    signers::LocalWallet,
};
use futures::future::join_all;
use itertools::Itertools;
use messages::{
    network_utils::{AVSError, Method, NodeData, RequestToNetwork, StateRequest, StateResponse},
    task_proofs::{Pinata, TaskProof, TASK_SIZE},
};
use human_crypto::{reconstruct::threshold_unchecked, BabyJubJub, Curve, DLEQProof, PointTrait, Secp256k1};
use redis::Connection;
use std::{collections::HashMap, env, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{select, sync::Mutex};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{debug, info};
#[derive(Clone)]
struct AppState {
    current_epoch: Arc<Mutex<u32>>, // Always 0 for the first testnet
    active_provers: Arc<Mutex<Vec<NodeData>>>,
    /// Number of nodes with keyshares
    n: u32,
    /// Threshold of nodes
    t: u32,
    /// Extra nodes that should be queried in addition to t for redundancy
    epsilon: u32,
    /// How many requests are currently in the batch. When it reaches TASK_SIZE, the batch will be finalized and made into a task
    batch_size: Arc<Mutex<u32>>,
    /// For signing requests to the Othentic RPC:
    wallet: LocalWallet,
    // keyshares: Keyshares,
    redis: Arc<Mutex<Connection>>,
    othentic_rpc_url: String,
    eth: Provider<Http>,
    pinata: Pinata,
}
#[tokio::main]
async fn main() {
    let privkey = env::var("RELAYER_SIGNING_KEY").expect("RELAYER_SIGNING_KEY not set");
    let wallet = LocalWallet::from_str(&privkey).expect("Failed to parse private key");
    let pinata = Pinata {
        api_key: env::var("PINATA_API_KEY").expect("PINATA_API_KEY not set"),
        secret_api_key: env::var("PINATA_SECRET_API_KEY").expect("PINATA_SECRET_API_KEY not set"),
    };
    pinata.check_working().await.expect("Pinata API not working");
    let state = AppState {
        redis: Arc::new(Mutex::new(
            redis::Client::open(env::var("REDIS_URL").expect("REDIS_URL not set"))
                .expect("Failed to open Redis connection")
                .get_connection()
                .expect("Failed to get Redis connection"),
        )),
        current_epoch: Arc::new(Mutex::new(0)),
        active_provers: Arc::new(Mutex::new(
            serde_json::from_str(env::var("ACTIVE_PROVERS").expect("ACTIVE_PROVERS not set").as_str()).expect("Failed to parse active_provers"),
        )),
        // active_node_ipaddrs: Arc::new(Mutex::new(serde_json::from_str(env::var("ACTIVE_NODE_IPADDRS").expect("ACTIVE_NODE_IPADDRS not set").as_str()).expect("Failed to parse ACTIVE_NODE_IPADDRS"))),
        // active_node_pubkeys: Arc::new(Mutex::new(serde_json::from_str(env::var("ACTIVE_NODE_PUBKEYS").expect("ACTIVE_NODE_PUBKEYS not set").as_str()).expect("Failed to parse ACTIVE_NODE_PUBKEYS").map(|x|hex::decode(x).expect("Failed to decode hex string for address")))),
        n: env::var("THRESHOLD_N").expect("THRESHOLD_N not set").parse().expect("Failed to parse THRESHOLD_N"),
        t: env::var("THRESHOLD_T").expect("THRESHOLD_T not set").parse().expect("Failed to parse THRESHOLD_T"),
        epsilon: env::var("THRESHOLD_EPSILON").expect("THRESHOLD_EPSILON not set").parse().expect("Failed to parse THRESHOLD_EPSILON"),
        othentic_rpc_url: env::var("OTHENTIC_RPC_URL").expect("OTHENTIC_RPC_URL not set").parse().expect("Failed to parse RELAYER_IP"),
        eth: Provider::<Http>::try_from(env::var("L1_RPC").expect("L1_RPC note set")).expect("Failed to instatiate ETH provider"),
        batch_size: Arc::new(Mutex::new(0)),
        wallet,
        pinata,
    };
    let app = Router::new()
        .route("/user-state/", post(user_state))
        .route(&format!("/relay-{}", Method::OPRFSecp256k1.as_str()), post(relay::<32, Secp256k1>))
        .route(&format!("/relay-{}", Method::JWTPRFSecp256k1.as_str()), post(relay::<32, Secp256k1>))
        .route(&format!("/relay-{}", Method::DecryptBabyJubJub.as_str()), post(relay::<32, BabyJubJub>))
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();
    // run it
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
#[debug_handler]
async fn user_state(State(state): State<AppState>, Json(req): Json<StateRequest>) -> Result<Json<StateResponse>, AVSError> {
    let result: Option<i64> = redis::cmd("GET")
        .arg(format!("total_count:{}:0x{}", req.method.as_str(), hex::encode(req.user.0)))
        .query(&mut state.redis.lock().await)?;
    Ok(Json(StateResponse {
        epoch: state.current_epoch.lock().await.clone(),
        method: req.method,
        requests_from_user: result.unwrap_or(0).try_into()?,
    }))
}
/// Sends a request to a node. Does not return anything.
/// If the task is successful, appends to the parallel_task_results and parallel_task_success_indices,
/// which are thread-safe. These structs can be shared across requests without race conditions or memory problems
/// If the request is successful and enough other tasks have been as well, this will cancel the pending tasks with the same CancellationToken.
async fn request_to_node<const N: usize, C: Curve<N>>(
    node: NodeData,
    ct: CancellationToken,
    parallel_task_success_indices: Arc<Mutex<Vec<u32>>>,
    parallel_task_results: Arc<Mutex<Vec<Vec<u8>>>>,
    req: Arc<RequestToNetwork>,
    threshold: usize,
) where
    DLEQProof<N, C>: Send,
{
    debug!("Sending request {:?} to node {:?}", req, node.uri);
    let res = surf::post(format!("http://{}/proof/{}", node.uri, req.method.as_str())).body_json(&*req).unwrap().await;
    debug!("Got response from node {:?}", res);
    if let Ok(mut succ) = res {
        let proof = succ.body_json::<DLEQProof<N, C>>().await;
        if let Ok(p) = proof {
            // Strange way of handling errors: if there is an issue, we just use the zero point or some other useless value
            // so it will fail the DLEQ proof later and be discarded
            // This is because there is no error handling within this closure, so this is easier.
            let masked = C::Point::from_encoded(&req.point).unwrap_or(C::ZERO);
            let pubkey = C::Point::from_encoded(&node.pubkeys.get(C::NAME).unwrap_or(&HashMap::new()).get(req.method.as_str()).unwrap_or(&vec![]).clone()).unwrap_or(C::ZERO);
            if let Ok(out) = p.verify_and_get_output(&pubkey, &masked) {
                // At this point, we can finally trust that the output is correct. So we can add it to the success list and the results list
                parallel_task_results.lock().await.push(out.encode());
                let mut indices = parallel_task_success_indices.lock().await;
                indices.push(node.idx.clone());
                // If the number of successful responses is sufficient, we can cancel the other tasks
                if indices.len() >= threshold {
                    ct.cancel();
                }
            }
        }
    }
}
///  If a request is invalid, it will return an error. Otherwise, this function relays a request to the appropriate nodes and returns their response.
/// It caches requests until the highest request numbers across all users increment by a total of TASK_SIZE, upon which it will send a TaskProof of these cached tasks to the verifier.
// #[axum_macros::debug_handler]
async fn relay<const N: usize, C: Curve<N>>(uri: Uri, State(state): State<AppState>, Json(request): Json<RequestToNetwork>) -> Result<String, AVSError>
where
    DLEQProof<N, C>: Send,
{
    if request.epoch != *state.current_epoch.lock().await {
        return Err(anyhow!("Invalid epoch").into());
    }
    if "/relay-".to_owned() + request.method.as_str() != uri.path() {
        return Err(anyhow!("Invalid method").into());
    }
    let mut conn = state.redis.lock().await;
    debug!("Counting credits");
    let incr_by = request.account_request_from_signer(&mut conn, &state.eth).await?;
    debug!("Credit change: {:?}", incr_by);
    redis::cmd("RPUSH").arg("request_batch").arg(request.clone()).query(&mut conn)?;
    let mut batch_size = state.batch_size.lock().await;
    *batch_size += incr_by;
    let completing_task = *batch_size >= TASK_SIZE;
    drop(conn);
    drop(batch_size);
    if completing_task {
        debug!("Completing task");
        let task_proof = TaskProof {
            data: redis::cmd("LRANGE").arg("request_batch").arg(0).arg(-1).query(&mut state.redis.lock().await)?,
        };
        let _ = task_proof
            .pin_ipfs(&state.pinata.api())
            .await?
            .post_to_othentic_nodes(state.wallet, state.othentic_rpc_url.clone())
            .await?;
        // IMPORTANT: this reset should not happen unless the submisison was successful
        // reset the batch and batch_size
        redis::cmd("DEL").arg("request_batch").query(&mut state.redis.lock().await)?;
        *state.batch_size.lock().await = 0;
    }
    // Now, send the request to the provers and return a single aggregated point if successful:
    let active_provers: Vec<NodeData>;
    // Drop the lock before sending requests to nodes since otherwise we would be holding the lock for a long time
    {
        active_provers = state.active_provers.lock().await.clone();
    }
    // ---------------- Request to nodes and parse their responses in parrallel ----- //
    debug!("Sending request to nodes");
    // This will allow returning early / cancelling the pending tasks when enough nodes respond
    let cancellation_token = CancellationToken::new(); //Arc::new(Mutex::new(CancellationToken::new());
                                                       // send parrellel tasks to t+epsilon nodes
    let mut parallel_tasks = vec![];
    // keep track of indices of nodes which responded successfully
    let parallel_task_success_indices: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![])); // Indices of nodes that responded successfully
                                                                                            // keep track of results frmo nodes which responded successfully
    let parallel_task_results: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(vec![]));
    // Put the request in an Arc so we can clone it for each task
    let req = Arc::new(request.clone());
    // Get the indices of the nodes to send to
    let indices = req.get_nodes(state.n, state.t + state.epsilon, &active_provers.iter().map(|x| x.idx).collect());
    let threshold = state.t as usize;
    for node in active_provers {
        debug!("Sending request to node {:?}?", node.uri);
        if !indices.contains(&node.idx) {
            continue;
        }
        let req = req.clone();
        let parallel_task_results = parallel_task_results.clone();
        let parallel_task_success_indices = parallel_task_success_indices.clone();
        let ct = cancellation_token.clone();
        parallel_tasks.push(tokio::spawn(async move {
            select! {
                // If the task is cancelled, we do nothing
                _ = ct.cancelled() => { }
                // Otherwise, we send the request to the node and append its response if successful.
                // if enough are successful, we can cancel the other tasks
                _ = request_to_node(node, ct.clone(), parallel_task_success_indices, parallel_task_results, req, threshold) => { }
            }
        }));
    }
    join_all(parallel_tasks).await;
    let indices = parallel_task_success_indices.lock().await.clone();
    let points = parallel_task_results.lock().await.clone();
    // If we have enough successful responses, we can interpolate and return them to the user
    if indices.len() >= threshold {
        // get them out of the Arc:
        let points = points.iter().map(|p| C::Point::from_encoded(&p).unwrap()).collect_vec();
        Ok(hex::encode(threshold_unchecked::<N, C>(points, indices).encode()))
    } else {
        Err(anyhow!("Not enough successful responses").into())
    }
}
