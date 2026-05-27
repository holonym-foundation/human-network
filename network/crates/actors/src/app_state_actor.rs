//! This actor defines the `AppStateActor`, which manages the application state for a relay node
//! handling OPRF requests. It processes received DLEQ proofs for OPRF and performs threshold verification.
//! Additionally, it includes functionality to handle multiplication requests from Prover nodes.

// Standard library imports
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;
use std::{env, fmt};
// Third-party library imports
use crate::cancellation_context::CancellationContext;
use async_trait::async_trait;
use delay_queue::{Delay, DelayQueue};
use ethers::providers::{Http, Provider};
use ethers::signers::LocalWallet;
use ethers::types::H160;
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use lru_time_cache::LruCache;
use messages::kafka::{KafkaProducer, KafkaTopic, QuorumResharingInfo, RequestInfo};
use messages::utils::MonitorEvent;
use messages::NETWORK_TOPIC;
use pubsub_rs::Pubsub;
use redis::{Commands, Connection};
use rpc_trait::rpc::HumanRpcClient;
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::*;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{oneshot::Sender, Mutex};
use tokio::{task, time};
use tracing::{debug, error, info, trace, warn};
// Project-specific imports
use crate::election_actor::{build_http_client, ElectionEngineError};
use crate::gossip_engine_actor::GossipEngineError;
use crate::{cast_message, group_changed, DKG_RESHARING_WAIT_TIME, MAX_RESHARING_ATTEMPTS_AT_A_TIME, MUL_REPONSE_WAIT_TIME, RESHARING_REATTEMPT_WAIT_TIME, STATE_DIR, TRIGGER_RESHARING_DURATION};
use messages::actor_type::ActorType;
use messages::message::{AppStateChangeMessage, ElectionEngineMessage, ElectionInfo, GossipEngineMessage, Message, PrivkeyShares, ProverInfo, PubkeyShares};
use messages::network_utils::{Method, RequestToNetwork, RequestToNetworkWithProofs};
use messages::task_proofs::{Pinata, TaskProof, TASK_SIZE};
use messages::types::{ElectionResponse, NodeResponse, Response, StateRequest, StateResponse};
use human_crypto::reconstruct::threshold_unchecked;
use human_crypto::{BabyJubJub, Curve, DLEQProof, PointTrait, ScalarTrait, Secp256k1};
use network::utils::NodeType;
use network::utils::{fetch_provers_v2, send_response};
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, SupervisionEvent};
use sled::Db;
type ActiveProvers = Arc<Mutex<Vec<ProverInfo>>>;
use lazy_static::lazy_static;

lazy_static! {
    static ref DB_INSTANCES: Mutex<HashMap<String, Arc<Db>>> = Mutex::new(HashMap::new());
}

// Define a struct to hold the API token and its usage flag
pub struct ApiToken {
    pub token: String,
    pub used: bool,
}

lazy_static::lazy_static! {
    pub static ref API_TOKEN: Mutex<ApiToken> = Mutex::new(ApiToken {
        token: "".to_string(),
        used: false,
    });
}
/// A generic error type to propagate errors from this actor
/// and other actors that interact with it
#[derive(Debug, Clone, Error)]
pub enum AppStateEngineError {
    #[error("Error occurred in app engine: {0}")]
    Custom(String),
}
impl Default for AppStateEngineError {
    fn default() -> Self { AppStateEngineError::Custom("AppStateEngine unable to acquire actor".to_string()) }
}

/// The actor struct for the AppState Engine actor
#[derive(Clone, Debug, Default)]
pub struct AppStateEngineActor;
impl AppStateEngineActor {
    pub fn new() -> Self { Self }
}

#[derive(Clone, Serialize)]
pub struct CommonState {
    pub peer_id: PeerId,
    pub relay_peer_id: PeerId,
    #[serde(skip_serializing)]
    current_epoch: Arc<Mutex<u32>>,
    #[serde(skip_serializing)]
    redis: Arc<Mutex<Connection>>,
    #[serde(skip_serializing)]
    eth: Provider<Http>,
}

#[derive(Clone, Debug)]
pub struct StoredMultResult {
    num_nodes: u32,
    mul_results: Vec<(u32, Vec<u8>)>,
    reconstructed_point: Option<Vec<u8>>,
    curve: String,
}
impl StoredMultResult {
    /// Calculates and stores `reconstructed_point` from stored multiplication results. If it succeeds, it also modifies `data`'s state to show its success
    async fn process_verification<const N: usize, C: Curve<N>>(
        &mut self,
        data: &mut RequestRecord,
        request_id: &str,
        threshold: u32,
        common_state: &mut CommonState,
        batch_size: &tokio::sync::Mutex<u32>,
        othentic_rpc_url: String,
        pinata: Pinata,
        request_hash: String,
        batch_cache: &mut LruCache<String, ()>,
        // usr_req_cache: &mut LruCache<String, ()>,
        pubsub_cache: &mut LruCache<String, Pubsub<String, NodeResponse>>,
        active_provers: &mut Arc<Mutex<Vec<ProverInfo>>>,
        kafka: &KafkaProducer,
    ) {
        debug!("Verification check - nodes: {}, threshold: {}, sufficient: {}", self.num_nodes, threshold, self.num_nodes >= threshold);

        if self.num_nodes >= threshold {
            if let Ok(points) = self.mul_results.iter().map(|(_, p)| C::Point::from_encoded(p)).collect::<Result<Vec<_>, _>>() {
                let indices: Vec<u32> = self.mul_results.iter().map(|(idx, _)| *idx).collect();
                let indices_for_multipliers = indices.clone();
                let point = &threshold_unchecked::<N, C>(points, indices);
                self.reconstructed_point = Some(point.encode());
                data.status = TxState::Verified;
                // if let Ok(usr) = data.request.recover() {
                //     if usr_req_cache.contains_key(&usr.to_string()) {
                //         trace!("Removing user request from cache: {}", usr.to_string());
                //         usr_req_cache.remove(&usr.to_string());
                //     }
                // }

                info!("Successfully verified proof for request: {}", request_id);

                if pubsub_cache.contains_key(request_id) {
                    let pubsub: &mut Pubsub<String, NodeResponse> = pubsub_cache.get_mut(request_id).unwrap();
                    trace!("Notifying pubsub subscribers for request: {}", request_id);
                    fetch_reconstructed_point_v2(data.clone(), self.clone(), pubsub.clone(), request_id).await;
                    pubsub_cache.remove(request_id);

                    let active_provers = active_provers.lock().await;
                    let served_by_multipliers = indices_for_multipliers
                        .iter()
                        .filter_map(|&i| {
                            active_provers
                                .iter()
                                .find(|p| p.idx == i as usize)
                                .map(|p| p.peer_id.clone())
                        })
                        .collect();
                    let request_info = RequestInfo {
                        method: data.request.method.clone(),
                        address: match data.request.recover() {
                            Ok(addr) => addr,
                            Err(_) => {
                                error!("Failed to recover address for request: {}", request_id);
                                H160::zero()
                            }
                        },
                        served_by_multipliers,
                    };
                    kafka.send(KafkaTopic::Requests, request_id.to_string(), request_info).await;
                }
                //Store it within redis
                if !batch_cache.contains_key(&request_hash) {
                    info!("Initiating batch processing for hash: {}", request_hash);
                    batch_cache.insert(request_hash.clone(), ());
                    process_batch(common_state, batch_size, othentic_rpc_url, pinata, request_hash, (*active_provers.lock().await).clone()).await;
                }
            } else {
                warn!("Failed to decode points during verification for request: {}", request_id);
            }
        } else {
            debug!("Insufficient nodes for verification ({} < {})", self.num_nodes, threshold);
        }
    }
}

async fn process_batch(common_state: &CommonState, batch_size: &tokio::sync::Mutex<u32>, othentic_rpc_url: String, pinata: Pinata, request_hash: String, current_elected_provers: Vec<ProverInfo>) {
    let mut conn = common_state.redis.lock().await;

    info!("Processing batch request: {}", request_hash);

    // Step 1: RPUSH the request to batch
    match conn.get::<_, RequestToNetworkWithProofs>(request_hash.clone()) {
        Ok(value) => {
            if let Err(err) = redis::cmd("RPUSH").arg("request_batch").arg(&value).query::<()>(&mut *conn) {
                error!("Failed to batch request {}: {}", request_hash, err);
            }
        }
        Err(err) => {
            warn!("Request {} not found in Redis: {}", request_hash, err);
        }
    }

    // Clean up processed key
    if let Err(err) = redis::cmd("DEL").arg(request_hash.clone()).query::<()>(&mut *conn) {
        error!("Failed to clean up request {}: {}", request_hash, err);
    }

    // Check if batch is ready for processing
    let mut batch_size = batch_size.lock().await;
    let should_process_batch = *batch_size >= TASK_SIZE;

    if should_process_batch {
        debug!("Batch reached threshold ({} items), processing...", TASK_SIZE);

        let private_key = env::var("PRIVATE_KEY").expect("PRIVATE_KEY environment variable not set");

        match redis::cmd("LRANGE").arg("request_batch").arg(0).arg(-1).query(&mut *conn) {
            Ok(data) => {
                let task_proof = TaskProof { data };
                debug!("Created task proof with {} items", task_proof.data.len());

                let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

                rt.block_on(async {
                    // Pin to IPFS
                    match task_proof.pin_ipfs(&pinata.api()).await {
                        Ok(result) => {
                            info!("Pinned batch to IPFS, submitting to Othentic nodes...");

                            match tokio::time::timeout(Duration::from_secs(5), result.post_to_othentic_nodes(othentic_rpc_url.clone(), private_key, current_elected_provers.iter().map(|p| p.evm_address).collect())).await {
                                Ok(Ok(_)) => info!("Successfully submitted batch to Othentic"),
                                Ok(Err(e)) => error!("Othentic submission failed: {}", e),
                                Err(_) => warn!("Othentic submission timed out after 5s"),
                            }
                        }
                        Err(e) => {
                            error!("IPFS pinning failed: {}", e);
                        }
                    }
                });

                // Clean up batch
                if let Err(e) = redis::cmd("DEL").arg("request_batch").query::<()>(&mut *conn) {
                    error!("Failed to clean up batch: {}", e);
                }
                *batch_size = 0;
                info!("Batch processing completed, counter reset");
            }
            Err(e) => {
                error!("Failed to retrieve batch from Redis: {}", e);
            }
        }
    } else {
        trace!("Batch not ready (current: {}, needed: {})", *batch_size, TASK_SIZE);
    }
}

/// Different roles a node can take, each with its own specific data requirements
/// and responsibilities. The node state can be `Relay`, `Prover`, or `Verifier`, with each variant containing
/// the relevant information and settings for that state.
pub enum NodeState {
    /// Represents a node functioning as a relay.
    ///
    /// The relay node is responsible for managing and coordinating requests and responses between
    /// different prover nodes in the network. It handles tasks such as batch processing, communicating
    /// with Othentic RPC, and maintaining caches.
    Relay {
        /// The shared state information for the node.
        common_state: Box<CommonState>,

        /// A collection of currently active provers.
        active_provers: ActiveProvers,

        /// The total number of nodes with key shares.
        n: u32,

        /// The threshold number of nodes required for a particular operation.
        t: u32,

        /// The number of extra nodes to query in addition to the threshold for redundancy.
        epsilon: u32,

        /// The number of requests currently in the batch, managed as an atomic reference.
        batch_size: Arc<Mutex<u32>>,

        /// The local wallet used for signing requests to the Othentic RPC.
        wallet: Box<LocalWallet>,

        /// The URL for the Othentic RPC service.
        othentic_rpc_url: String,

        /// The Pinata client used for interacting with IPFS.
        pinata: Pinata,

        /// The cache of incoming requests, using an LRU cache mechanism.
        req_cache: Box<LruCache<String, RequestRecord>>,

        /// The cache of stored multiplication results, also using an LRU cache.
        res_cache: Box<LruCache<String, StoredMultResult>>,

        /// The public key shares associated with each peer.
        pubkey_shares: HashMap<PeerId, PubkeyShares>,

        /// Indicates whether the relay node is ready for processing operations.
        ready: bool,

        /// A set of node IDs that are considered ready.
        ready_nodes: HashSet<u128>,

        /// A list of newly cached elected provers.
        new_cached_elected_provers: Vec<ProverInfo>,

        /// A map of old tstar provers with additional information and indices.
        tstar_provers: Box<HashMap<PeerId, (ProverInfo, usize)>>,

        /// A map of new provers with additional information and indices.
        new_provers: Box<HashMap<PeerId, (ProverInfo, usize)>>,

        /// A map of finalized group public keys.
        finalized_group_public_keys: HashMap<String, Vec<u8>>,

        /// A list of received group public keys from other nodes.
        received_group_public_keys: Vec<HashMap<String, Vec<u8>>>,

        no_of_shares_received: Arc<AtomicUsize>,

        monitor_request_queue: DelayQueue<Delay<String>>,

        batch_cache: Box<LruCache<String, ()>>,

        // usr_req_cache: Box<LruCache<String, ()>>,
        pubsub_cache: Box<LruCache<String, Pubsub<String, NodeResponse>>>,

        all_provers_cache: Box<LruCache<PeerId, ProverInfo>>,

        /// flag to indicate if resharing is enabled
        is_resharing_enabled: Arc<AtomicBool>,

        /// resharing failure count
        resharing_failure_count: Arc<AtomicUsize>,

        /// The Kafka producer used for sending messages to the Kafka topic.
        kafka: Arc<KafkaProducer>,
    },

    /// Represents a node functioning as a prover.
    ///
    /// The prover node is responsible for participating in cryptographic operations and providing
    /// proofs as part of the network. It maintains its own private key shares and other related
    /// information.
    Prover {
        /// The shared state information for the node.
        common_state: Box<CommonState>,

        /// The index of the node within the network.
        my_node_idx: u32,

        /// The total number of nodes with key shares.
        n: u32,

        /// The threshold number of nodes required for a particular operation.
        t: u32,

        /// The number of extra nodes to query in addition to the threshold for redundancy.
        epsilon: u32,

        /// The private key shares associated with the prover node.
        private_keyshares: PrivkeyShares,

        /// A map of new provers with additional information and indices.
        new_provers: Box<HashMap<PeerId, (ProverInfo, usize)>>,

        /// Multiaddr of the relay node
        relay_multi_addr: Multiaddr,
    },

    /// Represents a node functioning as a verifier.
    ///
    /// The verifier node is responsible for validating cryptographic proofs and operations performed
    /// by other nodes in the network. It connects to IPFS for data storage and retrieval.
    Verifier {
        /// The shared state information for the node.
        common_state: Box<CommonState>,

        /// The host URL for the IPFS service used by the verifier.
        ipfs_host: String,
    },
}

pub struct AppEngineState {
    pub node_type: NodeType,
    pub state: Option<NodeState>,
}
impl fmt::Debug for AppEngineState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct("AppEngineState").field("peer_id", &self.node_type).finish() }
}

impl AppEngineState {}
#[derive(Clone, Debug, Serialize)]
pub struct RequestRecord {
    request: RequestToNetwork,
    #[serde(skip_serializing)]
    status: TxState,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TxState {
    Submitted,
    ConstructedProof,
    Verified,
}

impl fmt::Display for TxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxState::Submitted => write!(f, "Submitted"),
            TxState::ConstructedProof => write!(f, "ConstructedProof"),
            TxState::Verified => write!(f, "Verified"),
        }
    }
}

/// Creates a match branch for a particular request method to do the scalar multiplication with it
macro_rules! handle_request {
    ( $n: expr, $c: ty, $keyshare: expr, $request_id: ident, $request: ident, $tx: ident, $my_node_idx: expr, $method: expr, $common_state: ident ) => {{
        if let Ok(partial_share) = <$c as Curve<$n>>::Scalar::from_bytes($keyshare) {
            match <$c as Curve<$n>>::Point::from_encoded(&$request.point) {
                Ok(masked_point) => match DLEQProof::<$n, $c>::new(masked_point, partial_share) {
                    Ok(proof) => {
                        send_response::<NodeResponse>(
                            $tx,
                            NodeResponse::new_constructed_proof($my_node_idx, $common_state.peer_id, $request_id.to_string(), proof, $method).unwrap(), //unwrap is pretty safe here because the curve type has already been validated and that's the only thing that could have caused an error
                            "Error sending Constructed Proof Response to the channel.",
                        );
                    }
                    Err(e) => {
                        error!("Error constructing DLEQ proof for Secp256k1. Error: {:?}", e);
                        send_response($tx, NodeResponse::Error { request_id:$request_id.to_string(),message: format!("Error constructing DLEQ proof for Secp256k1. Error: {:?}", e) }, "Error sending Constructed Proof Response to the channel.");
                    }
                },
                Err(e) => {
                    error!("Error decoding masked point for Secp256k1. Error: {:?}", e);
                    send_response($tx, NodeResponse::Error { request_id:$request_id.to_string(),message: format!("Error decoding masked point for Secp256k1. Error: {:?}", e) }, "Error sending OPRF Constructed Proof Response to the channel.");
                }
            }
        }
    }};
}
use once_cell::sync::OnceCell;

static DB_INSTANCE: OnceCell<Arc<Db>> = OnceCell::new();

fn get_db(db_name: &str) -> Result<Arc<Db>, sled::Error> {
    DB_INSTANCE
        .get_or_try_init(|| {
            sled::open(db_name).map(Arc::new) // Store it inside Arc
        })
        .map(|db| Arc::clone(db)) // Clone Arc to return
}
pub fn save_state<T>(db_name: &str, key: &str, value: T)
where
    T: Serialize,
{
    let mut db_path = PathBuf::from(STATE_DIR);
    db_path.push(db_name);
    let db = match get_db(db_path.to_str().unwrap()) {
        Ok(db) => db,
        Err(e) => {
            error!("Failed to open database {}: {}", db_name, e);
            return;
        }
    };

    let json_string = match serde_json::to_string_pretty(&value) {
        Ok(json) => json,
        Err(e) => {
            error!("Serialization error for key {}: {}", key, e);
            return;
        }
    };

    if let Err(e) = db.insert(key.as_bytes(), json_string.as_bytes()) {
        error!("Failed to insert data into db for key {}: {}", key, e);
        return;
    }

    if let Err(e) = db.flush() {
        error!("Failed to flush the database for key {}: {}", key, e);
    }

    info!("Data saved to Sled db: {} with key: {}", db_name, key);
}

pub fn get_state<T>(db_name: &str, key: &str) -> Option<T>
where
    T: DeserializeOwned + Default,
{
    let mut db_path = PathBuf::from(STATE_DIR);
    db_path.push(db_name);
    let db = match get_db(db_path.as_path().to_str().unwrap()) {
        Ok(db) => db,
        Err(e) => {
            error!("Failed to open database {}: {}", db_name, e);
            return None;
        }
    };

    match db.get(key.as_bytes()) {
        Ok(Some(serialized_value)) => match serde_json::from_slice(&serialized_value) {
            Ok(value) => Some(value),
            Err(e) => {
                error!("Failed to deserialize value for key {}: {}", key, e);
                None
            }
        },
        _ => {
            info!("Key {} not found in database: {}", key, db_name);
            None
        }
    }
}

async fn ping_nodes(new_set: &mut HashMap<PeerId, (ProverInfo, usize)>) -> Result<HashSet<u128>, String> {
    let mut reachable_nodes = HashSet::new();
    for (_, (peer, size)) in new_set.iter_mut() {
        let rpcaddr = &peer.rpcaddr;
        info!("Attempting to ping peer at {}", rpcaddr);

        let client = build_http_client(rpcaddr);
        let response_result = client.ping().await;

        match response_result {
            Ok(response) => {
                info!("Ping successful: {:?}", response);
                reachable_nodes.insert(*size as u128);
            }
            Err(e) => {
                error!("Ping failed for {}: {:?}", rpcaddr, e);
            }
        }
    }

    if !reachable_nodes.is_empty() {
        Ok(reachable_nodes)
    } else {
        Err("No Reachable Nodes found".to_string())
    }
}

#[async_trait]
impl Actor for AppStateEngineActor {
    type Msg = AppStateChangeMessage;
    type State = AppEngineState;
    type Arguments = (PeerId, PeerId, NodeType, String, Multiaddr, Option<Arc<KafkaProducer>>);
    /// Initializes the actor's state and sets up periodic tasks based on node type.
    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        // Define cache expiration duration
        let time_to_live = Duration::from_secs(600);
        // Initialize caches for request records and results with expiry
        let req_cache = Box::new(LruCache::<String, RequestRecord>::with_expiry_duration(time_to_live));
        let res_cache = Box::new(LruCache::<String, StoredMultResult>::with_expiry_duration(time_to_live));
        let batch_cache = Box::new(LruCache::<String, ()>::with_expiry_duration(time_to_live));
        // let usr_req_cache = Box::new(LruCache::<String, ()>::with_expiry_duration(Duration::from_secs(60)));
        let pubsub_cache = Box::new(LruCache::<String, Pubsub<String, NodeResponse>>::with_expiry_duration(Duration::from_secs(60)));
        let all_provers_cache = Box::new(LruCache::<PeerId, ProverInfo>::with_expiry_duration(Duration::from_secs(86400))); // expires in 1 day
                                                                                                                            // Load private key from environment and create wallet
                                                                                                                            //let privkey = env::var("RELAYER_SIGNING_KEY").expect("RELAYER_SIGNING_KEY not set");
                                                                                                                            // Load and parse RPC URL from environment
        let othentic_rpc_url = env::var("OTHENTIC_RPC_URL").expect("OTHENTIC_RPC_URL not set").parse().expect("Failed to parse OTHENTIC_RPC_URL");
        let queue = DelayQueue::new();

        // Set up common state
        let common_state = Box::new(CommonState {
            peer_id: args.0,
            relay_peer_id: args.1,
            current_epoch: Arc::new(Mutex::new(0)),
            redis: Arc::new(Mutex::new(
                redis::Client::open(args.3)
                    .expect("Failed to open Redis connection")
                    .get_connection()
                    .expect("Failed to get Redis connection"),
            )),
            eth: Provider::<Http>::try_from(env::var("L1_RPC").expect("L1_RPC not set")).expect("Failed to instantiate ETH provider"),
        });
        // save data to sled
        save_state(&common_state.peer_id.to_string(), "peer_id", common_state.peer_id);
        save_state(&common_state.peer_id.to_string(), "relay_peer_id", common_state.relay_peer_id);
        save_state(&common_state.peer_id.to_string(), "current_epoch", *common_state.current_epoch.lock().await);
        // Define the node state based on the node type
        let state = match args.2 {
            NodeType::Relay => {
                // Load Pinata API credentials and check API functionality
                let api_key = env::var("PINATA_API_KEY").expect("PINATA_API_KEY must be set in .env file");
                let secret_api_key = env::var("PINATA_SECRET_API_KEY").expect("PINATA_SECRET_API_KEY must be set in .env file");
                let pinata = Pinata { api_key, secret_api_key };
                //pinata.check_working().await.expect("Pinata API not working");
                let kafka = args.5.expect("Kafka producer not initialized");
                let n = get_state(&args.0.to_string(), "n").unwrap_or_default();
                let t = get_state(&args.0.to_string(), "t").unwrap_or_default();
                let epsilon = get_state(&args.0.to_string(), "epsilon").unwrap_or_default();

                let new_provers: HashMap<PeerId, (ProverInfo, usize)> = get_state(&args.0.to_string(), "new_provers").unwrap_or_default();

                let tstar_provers: HashMap<PeerId, (ProverInfo, usize)> = get_state(&args.0.to_string(), "tstar_provers").unwrap_or_default();

                let pubkey_shares = get_state(&args.0.to_string(), "pubkey_shares").unwrap_or_default();

                let finalized_group_public_keys = get_state(&args.0.to_string(), "finalized_group_public_keys").unwrap_or_default();

                let active_provers: Vec<ProverInfo> = new_provers.values().map(|(prover_info, _)| prover_info.clone()).collect();

                let mut ready_nodes: HashSet<u128> = HashSet::new();
                let mut ready = false;
                if !new_provers.is_empty() {
                    match ping_nodes(&mut new_provers.clone()).await {
                        Ok(nodes) => {
                            ready_nodes = nodes;
                            ready = ready_nodes.len() >= t as usize;
                        }
                        Err(_) => {
                            ready_nodes = HashSet::new();
                            ready = false;
                        }
                    }
                }
                let privkey = env::var("PRIVATE_KEY").expect("PRIVATE_KEY for Relay node not set");
                let wallet = Box::new(LocalWallet::from_str(&privkey).expect("Failed to parse private key"));
                // Create Relay-specific state
                Some(NodeState::Relay {
                    common_state,
                    active_provers: Arc::new(Mutex::new(active_provers)),
                    n,
                    t,
                    epsilon,
                    batch_size: Arc::new(Mutex::new(0)),
                    wallet,
                    othentic_rpc_url,
                    pinata,
                    req_cache,
                    res_cache,
                    pubkey_shares,
                    ready,
                    ready_nodes,
                    new_cached_elected_provers: Vec::default(),
                    tstar_provers: Box::new(tstar_provers),
                    new_provers: Box::new(new_provers),
                    received_group_public_keys: Vec::new(),
                    finalized_group_public_keys,
                    no_of_shares_received: Arc::new(AtomicUsize::new(0)),
                    monitor_request_queue: queue.clone(),
                    batch_cache,
                    // usr_req_cache,
                    pubsub_cache,
                    all_provers_cache,
                    is_resharing_enabled: Arc::new(AtomicBool::new(true)),  // resharing is enabled by default
                    resharing_failure_count: Arc::new(AtomicUsize::new(0)), // tracks the number of resharing failures
                    kafka,
                })
            }
            NodeType::Prover => {
                let private_keyshares: HashMap<String, Vec<u8>> = get_state(&args.0.to_string(), "private_keyshares").unwrap_or_default();

                let idx = get_state(&args.0.to_string(), "idx").unwrap_or_default();

                let n = get_state(&args.0.to_string(), "n").unwrap_or_default();
                let t = get_state(&args.0.to_string(), "t").unwrap_or_default();
                let epsilon = get_state(&args.0.to_string(), "epsilon").unwrap_or_default();

                let new_provers = get_state(&args.0.to_string(), "new_provers").unwrap_or_default();
                // Create Prover-specific state
                Some(NodeState::Prover {
                    common_state,
                    my_node_idx: idx,
                    n,
                    t,
                    epsilon,
                    private_keyshares,
                    new_provers,
                    relay_multi_addr: args.4,
                })
            }
            NodeType::Verifier => {
                // Create Verifier-specific state
                Some(NodeState::Verifier {
                    common_state,
                    ipfs_host: String::from(""),
                })
            }
            _ => {
                // Log error for unsupported node types
                error!("Appstate not supported for NodeType: {:?}", args.2);
                None
            }
        };
        let is_resharing_enabled = match &state {
            Some(NodeState::Relay { is_resharing_enabled, .. }) => Some(is_resharing_enabled.clone()),
            _ => None,
        };
        let resharing_failure_count = match &state {
            Some(NodeState::Relay { resharing_failure_count, .. }) => Some(resharing_failure_count.clone()),
            _ => None,
        };
        // Start a periodic task for resharing based on node type
        if args.2 == NodeType::Relay {
            task::spawn(async move {
                // Define an interval for periodic resharing
                let mut interval = tokio::time::interval(Duration::from_secs(TRIGGER_RESHARING_DURATION));
                // Setup a signal handler for SIGHUP if the local_test_net feature is enabled
                let mut sighup = signal(SignalKind::hangup()).expect("Failed to create signal stream");

                let mut backup_sig_usr1 = signal(SignalKind::user_defined1()).expect("Failed to create userdefined1 signal stream");

                // Flag to track if the first resharing has been triggered
                let mut first_resharing_triggered = false;
                //   #[cfg(feature = "local_test_net")]
                // Flag to track if the first resharing has been triggered
                loop {
                    // Handle periodic tasks based on the feature flag
                    // #[cfg(not(feature = "local_test_net"))]
                    // {
                    //     tokio::select! {
                    //         _ = interval.tick() => {
                    //             if first_resharing_triggered {
                    //                 info!("Time for resharing ,Occured while ticking");
                    //                 cast_message!(
                    //                     ActorType::AppStateEngine,
                    //                     AppStateChangeMessage::PreResharing,
                    //                     AppStateEngineError
                    //                 );
                    //            } else {
                    //                 info!("Skipping ticker for first time");
                    //                 first_resharing_triggered = true;
                    //             }
                    //         },
                    //         _ = backup_sig_usr1.recv() => {
                    //              info!("Received userdefined1, Triggering backup");
                    //             cast_message!(
                    //                     ActorType::AppStateEngine,
                    //                     AppStateChangeMessage::BackupState,
                    //                     AppStateEngineError
                    //                 );
                    //         }
                    //     }
                    // }
                    //  #[cfg(feature = "local_test_net")]
                    //  #[cfg(not(feature = "local_test_net"))]
                    {
                        tokio::select! {
                            _ = interval.tick() => {
                                if first_resharing_triggered {
                                    // Only trigger resharing if enabled
                                    if let Some(enabled) = is_resharing_enabled.as_ref() {
                                        if enabled.load(SeqCst) {
                                            info!("Periodic resharing triggered by timer");
                                            // resetting the resharing failure count
                                            if let Some(counter) = resharing_failure_count.as_ref() {
                                                counter.store(0, SeqCst);
                                            }
                                            cast_message!(
                                                ActorType::AppStateEngine,
                                                AppStateChangeMessage::PreResharing,
                                                AppStateEngineError
                                            );
                                        } else {
                                            info!("Resharing is disabled.");
                                        }
                                    }
                                } else {
                                    trace!("Skipping initial resharing timer tick");
                                    first_resharing_triggered = true;
                                }
                            },
                            _ = sighup.recv() => {
                                info!("Received SIGHUP signal, initiating resharing");
                                cast_message!(
                                    ActorType::AppStateEngine,
                                    AppStateChangeMessage::PreResharing,
                                    AppStateEngineError
                                );
                            }
                            _ = backup_sig_usr1.recv() => {
                                info!("Received SIGUSR1 signal, initiating backup");
                                cast_message!(
                                    ActorType::AppStateEngine,
                                    AppStateChangeMessage::BackupState,
                                    AppStateEngineError
                                );
                           }
                        }
                    }
                }
            });
        }
        if args.2 == NodeType::Prover {
            task::spawn(async move {
                let mut backup_sig_usr1 = signal(SignalKind::user_defined1()).expect("Failed to create userdefined1 signal stream");
                loop {
                    {
                        tokio::select! {
                            _ = backup_sig_usr1.recv() => {
                                info!("Received SIGUSR1 signal, initiating backup for prover");
                                cast_message!(
                                    ActorType::AppStateEngine,
                                    AppStateChangeMessage::BackupState,
                                    AppStateEngineError
                                );
                           }
                        }
                    }
                }
            });
        }
        Ok(AppEngineState { node_type: args.2, state })
    }
    async fn handle(&self, _myself: ActorRef<Self::Msg>, message: Self::Msg, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match message {
            AppStateChangeMessage::StateRequest(req, sender) => {
                handle_state_request(req, sender, state).await;
            }
            AppStateChangeMessage::MulRequest(request, pub_sub, request_id) => handle_multiplication_request(request, pub_sub, state, vec![], request_id).await,
            AppStateChangeMessage::UpdatedThreshold(election_info) => {
                update_threshold(election_info, state);
            }
            AppStateChangeMessage::UpdateElectedProvers(request, is_cache_prover) => {
                update_elected_provers(request, state, is_cache_prover).await;
            }
            AppStateChangeMessage::ProcessMulRequest(request_id, request, tx) => {
                process_request(&mut state.state, &request_id, request, tx).await;
            }
            AppStateChangeMessage::UpdateNodeIdx(idx) => {
                update_node_idx(&mut state.state, idx);
            }

            AppStateChangeMessage::StoreKeyShares(privkey_shares) => {
                if let Some(NodeState::Prover { private_keyshares, .. }) = &mut state.state {
                    *private_keyshares = privkey_shares;
                    // save_state(&common_state.peer_id.to_string(), "private_keyshares", &private_keyshares);
                }
            }
            AppStateChangeMessage::StoreKSet(peer_id, pubkey_shares, idx, group_public_keys) => {
                info!("Received KSet from node {}", idx);
                store_kset(&mut state.state, peer_id, pubkey_shares, idx, group_public_keys).await;
            }
            AppStateChangeMessage::StoreQuorumMap(tstar_provers, new_quorum_provers) => {
                store_quorum_map(&mut state.state, tstar_provers, new_quorum_provers).await;
            }
            AppStateChangeMessage::ResharingStoreKSet(peer_id, pubkey_shares, idx) => {
                info!("Received Reshared KSet from node {}", idx);
                store_resharing_kset(&mut state.state, peer_id, pubkey_shares, idx).await;
            }
            AppStateChangeMessage::FetchReconstructedPoint(request_id, sender) => {
                fetch_reconstructed_point(&mut state.state, &request_id, sender);
            }
            AppStateChangeMessage::ProcessReceivedProofSecp256k1(node_idx, peer_id, request_id, proof, method) => {
                process_received_proof(&mut state.state, node_idx, peer_id, &request_id, proof, method).await;
            }
            AppStateChangeMessage::ProcessReceivedProofBabyJubJub(node_idx, peer_id, request_id, proof, method) => {
                process_received_proof(&mut state.state, node_idx, peer_id, &request_id, proof, method).await;
            }
            AppStateChangeMessage::PreResharing => {
                if let Some(NodeState::Relay {
                    active_provers,
                    no_of_shares_received,
                    ..
                }) = &mut state.state
                {
                    info!("Relection Triggered");
                    // resetting the shares received to 0
                    no_of_shares_received.store(0, Ordering::SeqCst);
                    let prev_elected_provers = active_provers.lock().await.clone();
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::TriggerReElection(prev_elected_provers), ElectionEngineError);
                }
            }
            AppStateChangeMessage::ClearShares => {
                if let Some(NodeState::Relay {
                    ref mut pubkey_shares,
                    ref mut ready,
                    ref mut ready_nodes,
                    ..
                }) = &mut state.state
                {
                    info!("Clearing the shares before triggering resharing");
                    //Only clear when resharing is triggered
                    pubkey_shares.clear();
                    *ready = false;
                    ready_nodes.clear();
                }
            }
            AppStateChangeMessage::ForwardReElectedProvers(election_info, tstar_quorum_map, new_quorum_map, drand_response) => {
                if let Some(NodeState::Relay { ref finalized_group_public_keys, .. }) = &mut state.state {
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(
                            Message::ReElectedProvers(election_info, tstar_quorum_map, new_quorum_map, finalized_group_public_keys.clone(), drand_response,),
                            NETWORK_TOPIC.to_string(),
                        ),
                        GossipEngineError
                    );
                }
            }
            AppStateChangeMessage::UpdateCachedProversMap(new_provers_map) => {
                if let Some(NodeState::Prover { ref mut new_provers, .. } | NodeState::Relay { ref mut new_provers, .. }) = &mut state.state {
                    // save_state(&common_state.peer_id.to_string(), "new_provers", &new_provers_map);
                    **new_provers = new_provers_map;
                }
            }

            AppStateChangeMessage::BackupState => {
                if let Some(state) = &mut state.state {
                    let save_common = |common_state: &CommonState, new_provers: &_, n: &_, t: &_, epsilon: &_, epoch: _| {
                        save_state(&common_state.peer_id.to_string(), "new_provers", new_provers);
                        save_state(&common_state.peer_id.to_string(), "peer_id", common_state.peer_id);
                        save_state(&common_state.peer_id.to_string(), "relay_peer_id", common_state.relay_peer_id);
                        save_state(&common_state.peer_id.to_string(), "current_epoch", epoch);
                        save_state(&common_state.peer_id.to_string(), "n", *n);
                        save_state(&common_state.peer_id.to_string(), "t", *t);
                        save_state(&common_state.peer_id.to_string(), "epsilon", *epsilon);
                    };
                    match state {
                        NodeState::Prover {
                            ref mut new_provers,
                            common_state,
                            private_keyshares,
                            n,
                            t,
                            epsilon,
                            my_node_idx,
                            ..
                        } => {
                            let epoch = *common_state.current_epoch.lock().await;
                            save_common(common_state, new_provers, n, t, epsilon, epoch);
                            save_state(&common_state.peer_id.to_string(), "private_keyshares", private_keyshares);
                            save_state(&common_state.peer_id.to_string(), "idx", my_node_idx);
                        }
                        NodeState::Relay {
                            ref mut new_provers,
                            common_state,
                            n,
                            t,
                            epsilon,
                            finalized_group_public_keys,
                            pubkey_shares,
                            ..
                        } => {
                            let epoch = *common_state.current_epoch.lock().await;
                            save_common(common_state, new_provers, n, t, epsilon, epoch);
                            save_state(&common_state.peer_id.to_string(), "finalized_group_public_keys", finalized_group_public_keys);
                            save_state(&common_state.peer_id.to_string(), "pubkey_shares", pubkey_shares);
                        }
                        NodeState::Verifier { .. } => {
                            info!("No state to backup for verifiers");
                        }
                    }
                }
            }
            AppStateChangeMessage::MonitorQuorumFormation(event) => {
                let cancel_token: CancellationContext = CancellationContext::new();
                if let Some(NodeState::Relay {
                    active_provers,
                    no_of_shares_received,
                    resharing_failure_count,
                    kafka,
                    ..
                }) = &mut state.state
                {
                    let no_of_shares_to_monitor = active_provers.lock().await.len();
                    let no_of_shares_received_cloned = no_of_shares_received.clone();
                    let resharing_failure_count_cloned = resharing_failure_count.clone();
                    let kafka_cloned = kafka.clone();
                    let active_provers_cloned = active_provers.clone();
                    task::spawn(async move {
                        monitor_quorum_formation(
                            no_of_shares_to_monitor,
                            no_of_shares_received_cloned,
                            resharing_failure_count_cloned,
                            cancel_token,
                            event,
                            active_provers_cloned,
                            kafka_cloned,
                        )
                        .await;
                    });
                }
            }
            AppStateChangeMessage::FetchKeyShare(sender) => {
                if let Some(NodeState::Prover { common_state, .. }) = &mut state.state {
                    let private_keyshares: HashMap<String, Vec<u8>> = get_state(&common_state.peer_id.to_string(), "private_keyshares").unwrap_or_default();
                    let private_keyshares_str: HashMap<String, String> = private_keyshares.into_iter().map(|(k, v)| (k, hex::encode(v))).collect();
                    send_response::<NodeResponse>(sender, NodeResponse::Keyshare(private_keyshares_str), "Error sending response");
                }
            }
            AppStateChangeMessage::FetchFinalizedGroupPubkeys(sender) => {
                if let Some(NodeState::Relay { finalized_group_public_keys, .. }) = &mut state.state {
                    let pubkeys_hex: HashMap<String, String> = finalized_group_public_keys
                        .iter()
                        .map(|(k, v)| (k.clone(), hex::encode(v)))
                        .collect();
                    send_response::<NodeResponse>(sender, NodeResponse::Keyshare(pubkeys_hex), "Error sending FetchFinalizedGroupPubkeys response");
                } else {
                    send_response::<NodeResponse>(
                        sender,
                        NodeResponse::Error { request_id: "".to_string(), message: "Not a relay node or pubkeys not yet finalized".to_string() },
                        "Error sending FetchFinalizedGroupPubkeys response",
                    );
                }
            }
            AppStateChangeMessage::RestoreKeyShare(keyshares, sender) => {
                if let Some(NodeState::Prover { common_state, private_keyshares, .. }) = &mut state.state {
                    let restored_keyshares: HashMap<String, Vec<u8>> = keyshares
                        .into_iter()
                        .filter_map(|(k, v)| match hex::decode(v) {
                            Ok(bytes) => Some((k, bytes)),
                            Err(e) => {
                                error!("Failed to decode hex string for key {}: {}", k, e);
                                None
                            }
                        })
                        .collect();

                    *private_keyshares = restored_keyshares.clone();

                    // Save to persistent storage
                    save_state(&common_state.peer_id.to_string(), "private_keyshares", &restored_keyshares);

                    info!("Successfully restored keyshares");
                    send_response::<NodeResponse>(sender, NodeResponse::Restored, "Error sending response");
                }
            }
            AppStateChangeMessage::FetchElectionState(peer_id, sender) => {
                if let Some(NodeState::Relay { new_provers, n, t, epsilon, .. }) = &mut state.state {
                    match new_provers.get(&peer_id).map(|(_, size)| *size) {
                        Some(size) => {
                            send_response::<NodeResponse>(
                                sender,
                                NodeResponse::ElectionState {
                                    n: *n,
                                    t: *t,
                                    epsilon: *epsilon,
                                    node_idx: size,
                                    new_provers: *new_provers.clone(),
                                },
                                "Error sending response",
                            );
                        }
                        None => {
                            error!("invalid peer id");
                        }
                    };
                }
            }
            AppStateChangeMessage::RestoreElectionState(sender) => {
                if let Some(NodeState::Prover {
                    common_state,
                    n,
                    t,
                    epsilon,
                    my_node_idx,
                    new_provers,
                    relay_multi_addr,
                    ..
                }) = &mut state.state
                {
                    let mut addr_iter = relay_multi_addr.into_iter();

                    let host;

                    match addr_iter.next().unwrap() {
                        Protocol::Ip6(ip) => {
                            host = ip.to_string();
                        }
                        Protocol::Ip4(ip) => {
                            host = ip.to_string();
                        }
                        Protocol::Dns(dns) => {
                            host = dns.to_string();
                        }
                        Protocol::Dns4(dns) => {
                            host = dns.to_string();
                        }
                        Protocol::Dns6(dns) => {
                            host = dns.to_string();
                        }
                        Protocol::Dnsaddr(dns) => {
                            host = dns.to_string();
                        }
                        _ => unimplemented!(),
                    }

                    let port = match addr_iter.next().unwrap() {
                        Protocol::Tcp(p) => p,
                        Protocol::Udp(p) => p,
                        _ => unimplemented!(),
                    };
                    let relay_rpc_url = format!("http://{}:{}", host, port);
                    let client = build_http_client(&relay_rpc_url); // relay node rpc url
                    let response_result = client.fetch_election_state(common_state.peer_id.to_string()).await;
                    match response_result {
                        Ok(response) => {
                            match response {
                                NodeResponse::ElectionState {
                                    node_idx,
                                    n: new_n,
                                    t: new_t,
                                    epsilon: new_epsilon,
                                    new_provers: received_provers,
                                } => {
                                    // Update the values in state
                                    *n = new_n;
                                    *t = new_t;
                                    *epsilon = new_epsilon;
                                    *my_node_idx = node_idx as u32;
                                    **new_provers = received_provers;

                                    // Save to persistent storage
                                    save_state(&common_state.peer_id.to_string(), "n", &n);
                                    save_state(&common_state.peer_id.to_string(), "t", &t);
                                    save_state(&common_state.peer_id.to_string(), "epsilon", &epsilon);
                                    save_state(&common_state.peer_id.to_string(), "idx", my_node_idx);

                                    info!("Successfully restored election state and stored in sled");
                                    send_response::<NodeResponse>(sender, NodeResponse::Restored, "Error sending response");
                                }
                                NodeResponse::Error { request_id, message } => {
                                    error!("Request Id :{} Received error response: {}", request_id, message);
                                }
                                _ => {
                                    error!("Unexpected response type");
                                }
                            }
                        }
                        Err(e) => {
                            error!("unable to fetch election state from the relay node: {:?}", e);
                        }
                    }
                }
            }
            AppStateChangeMessage::SyncPeerData(sender) => {
                match &mut state.state {
                    Some(node_state) => {
                        if let NodeState::Relay {
                            active_provers,
                            new_cached_elected_provers,
                            tstar_provers,
                            new_provers,
                            all_provers_cache,
                            ..
                        } = node_state
                        {
                            let provers = fetch_provers_v2().await;

                            info!("Starting syncing peer data");
                            fn update_prover(prover: &mut ProverInfo, provers: &[ProverInfo]) {
                                provers.iter().find(|p| p.peer_id == prover.peer_id).map(|new_prover| {
                                    prover.rsa_pub_key = new_prover.rsa_pub_key.clone();
                                    prover.address = new_prover.address.clone();
                                });
                            }

                            let mut active_provers_lock = active_provers.lock().await;
                            active_provers_lock.iter_mut().for_each(|prover| update_prover(prover, &provers));
                            new_cached_elected_provers.iter_mut().for_each(|prover| update_prover(prover, &provers));
                            tstar_provers.values_mut().for_each(|(prover, _)| update_prover(prover, &provers));
                            new_provers.values_mut().for_each(|(prover, _)| update_prover(prover, &provers));

                            // Clear the cache and update it with the new provers
                            all_provers_cache.clear();
                            for prover in &provers {
                                all_provers_cache.insert(prover.peer_id.clone(), prover.clone());
                            }
                            sender
                                .send(Response::new(Some("Peer data synced successfully".to_string()), true, String::from("Peer data synced successfully")))
                                .map_err(|e| {
                                    error!("Failed to send response: {:?}", e);
                                    AppStateEngineError::Custom("Failed to send response".to_string())
                                })?;
                            info!("Synced peer data successfully");
                        } else {
                            // If the node is not a relay node, send an error response
                            sender.send(Response::new(None, false, "Not a relay node".to_string())).map_err(|e| {
                                error!("Failed to send response: {:?}", e);
                                AppStateEngineError::Custom("Failed to send response".to_string())
                            })?;
                        }
                    }
                    None => {
                        // If the state is not initialized, send an error response
                        sender.send(Response::new(None, false, "Node state not initialized".to_string())).map_err(|e| {
                            error!("Failed to send response: {:?}", e);
                            AppStateEngineError::Custom("Failed to send response".to_string())
                        })?;
                    }
                }
            }
            AppStateChangeMessage::FetchElectionInfo(peer_id, sender) => {
                match &mut state.state {
                    Some(node_state) => {
                        if let NodeState::Relay { new_provers, t, n, .. } = node_state {
                            let peer_ids: Vec<PeerId> = new_provers.keys().cloned().collect();
                            let election_info_response = ElectionResponse::new(peer_ids, *n, *t, new_provers.contains_key(&peer_id), true, "".to_string());
                            sender.send(election_info_response).map_err(|e| {
                                error!("Failed to send response: {:?}", e);
                                AppStateEngineError::Custom("Failed to send response".to_string())
                            })?;
                        } else {
                            // If the node is not a relay node, send an error response
                            sender.send(ElectionResponse::err_response("Not a relay node".to_string())).map_err(|e| {
                                error!("Failed to send response: {:?}", e);
                                AppStateEngineError::Custom("Failed to send response".to_string())
                            })?;
                        }
                    }
                    None => {
                        // If the state is not initialized, send an error response
                        sender.send(ElectionResponse::err_response("Node state not initialized".to_string())).map_err(|e| {
                            error!("Failed to send response: {:?}", e);
                            AppStateEngineError::Custom("Failed to send response".to_string())
                        })?;
                    }
                }
            }
            AppStateChangeMessage::RollbackToPreviousState => {
                if let Some(NodeState::Relay {
                    common_state,
                    new_provers,
                    tstar_provers,
                    pubkey_shares,
                    finalized_group_public_keys,
                    active_provers,
                    ready_nodes,
                    ready,
                    n,
                    t,
                    epsilon,
                    ..
                }) = &mut state.state
                {
                    let saved_new_provers: HashMap<PeerId, (ProverInfo, usize)> = get_state(&common_state.peer_id.to_string(), "new_provers").unwrap_or_default();

                    let saved_n = get_state(&common_state.peer_id.to_string(), "n").unwrap_or_default();

                    let saved_t = get_state(&common_state.peer_id.to_string(), "t").unwrap_or_default();

                    let saved_epsilon = get_state(&common_state.peer_id.to_string(), "epsilon").unwrap_or_default();

                    let saved_tstar_provers = get_state(&common_state.peer_id.to_string(), "tstar_provers").unwrap_or_default();

                    let saved_pubkey_shares = get_state(&common_state.peer_id.to_string(), "pubkey_shares").unwrap_or_default();

                    let saved_finalized_group_public_keys = get_state(&common_state.peer_id.to_string(), "finalized_group_public_keys").unwrap_or_default();

                    let saved_active_provers = saved_new_provers.values().map(|(prover_info, _)| prover_info.clone()).collect();

                    let mut ready_nodes_set: HashSet<u128> = HashSet::new();
                    let mut ready_status = false;
                    if !saved_new_provers.is_empty() {
                        match ping_nodes(&mut saved_new_provers.clone()).await {
                            Ok(nodes) => {
                                ready_nodes_set = nodes;
                                ready_status = ready_nodes.len() >= saved_t as usize;
                            }
                            Err(_) => {
                                ready_nodes_set = HashSet::new();
                                ready_status = false;
                            }
                        }
                    }

                    *n = saved_n;
                    *t = saved_t;
                    *epsilon = saved_epsilon;
                    **new_provers = saved_new_provers;
                    *tstar_provers = saved_tstar_provers;
                    *pubkey_shares = saved_pubkey_shares;
                    *finalized_group_public_keys = saved_finalized_group_public_keys;
                    *active_provers = Arc::new(Mutex::new(saved_active_provers));
                    *ready_nodes = ready_nodes_set;
                    *ready = ready_status;
                }
                if let Some(NodeState::Prover {
                    common_state,
                    new_provers,
                    private_keyshares,
                    n,
                    t,
                    epsilon,
                    my_node_idx,
                    ..
                }) = &mut state.state
                {
                    let saved_n = get_state(&common_state.peer_id.to_string(), "n").unwrap_or_default();

                    let saved_t = get_state(&common_state.peer_id.to_string(), "t").unwrap_or_default();

                    let saved_epsilon = get_state(&common_state.peer_id.to_string(), "epsilon").unwrap_or_default();

                    let saved_idx = get_state(&common_state.peer_id.to_string(), "idx").unwrap_or_default();

                    let saved_private_keyshares = get_state(&common_state.peer_id.to_string(), "private_keyshares").unwrap_or_default();

                    let saved_new_provers = get_state(&common_state.peer_id.to_string(), "new_provers").unwrap_or_default();

                    *n = saved_n;
                    *t = saved_t;
                    *epsilon = saved_epsilon;
                    *my_node_idx = saved_idx;
                    *private_keyshares = saved_private_keyshares;
                    *new_provers = saved_new_provers;
                }
            }
            AppStateChangeMessage::SaveCurrentState => {
                if let Some(NodeState::Relay {
                    common_state,
                    new_provers,
                    tstar_provers,
                    pubkey_shares,
                    finalized_group_public_keys,
                    n,
                    t,
                    epsilon,
                    ..
                }) = &mut state.state
                {
                    save_state(&common_state.peer_id.to_string(), "n", *n);
                    save_state(&common_state.peer_id.to_string(), "t", *t);
                    save_state(&common_state.peer_id.to_string(), "epsilon", *epsilon);
                    save_state(&common_state.peer_id.to_string(), "finalized_group_public_keys", &finalized_group_public_keys);
                    save_state(&common_state.peer_id.to_string(), "pubkey_shares", &pubkey_shares);
                    save_state(&common_state.peer_id.to_string(), "new_provers", &new_provers);
                    save_state(&common_state.peer_id.to_string(), "tstar_provers", &tstar_provers);
                }
                if let Some(NodeState::Prover {
                    common_state,
                    new_provers,
                    private_keyshares,
                    n,
                    t,
                    epsilon,
                    my_node_idx,
                    ..
                }) = &mut state.state
                {
                    save_state(&common_state.peer_id.to_string(), "n", *n);
                    save_state(&common_state.peer_id.to_string(), "t", *t);
                    save_state(&common_state.peer_id.to_string(), "epsilon", *epsilon);
                    save_state(&common_state.peer_id.to_string(), "idx", my_node_idx);
                    save_state(&common_state.peer_id.to_string(), "new_provers", &new_provers);
                    save_state(&common_state.peer_id.to_string(), "private_keyshares", &private_keyshares);
                }
            }
            AppStateChangeMessage::QuicPing(peer_id, sender) => {
                match &mut state.state {
                    Some(node_state) => {
                        if let NodeState::Relay { all_provers_cache, .. } = node_state {
                            if all_provers_cache.is_empty() {
                                info!("Populating the all_provers_cache");
                                let provers = fetch_provers_v2().await;
                                for prover in provers {
                                    all_provers_cache.insert(prover.peer_id.clone(), prover);
                                }
                            }
                            let prover_info = all_provers_cache.get(&peer_id);
                            match prover_info {
                                Some(info) => {
                                    let multiaddr = info.address.clone();
                                    cast_message!(ActorType::GossipEngine, GossipEngineMessage::Ping(peer_id, multiaddr, sender), GossipEngineError);
                                }
                                None => {
                                    error!(
                                        "Peer Id: {} not found in all_provers_cache. Available peer ids in all_provers_cache: {:?}",
                                        peer_id,
                                        all_provers_cache.iter().map(|(k, _)| k).collect::<Vec<_>>()
                                    );
                                    send_response::<Response>(sender, Response::new(None, false, "peer id not found".to_string()), "Error sending response");
                                }
                            }
                        } else {
                            // If the node is not a relay node, send an error response
                            if let Err(e) = sender.send(Response::new(None, false, "Not a relay node".to_string())) {
                                error!("Failed to send response: {:?}", e);
                            }
                        }
                    }
                    None => {
                        // If the state is not initialized, send an error response
                        if let Err(e) = sender.send(Response::new(None, false, "Node state not initialized".to_string())) {
                            error!("Failed to send response: {:?}", e);
                        }
                    }
                }
            }
            AppStateChangeMessage::FetchVotingPower(peer_id, sender) => {
                match &mut state.state {
                    Some(node_state) => {
                        if let NodeState::Relay { all_provers_cache, .. } = node_state {
                            if all_provers_cache.is_empty() {
                                info!("Populating the all_provers_cache");
                                let provers = fetch_provers_v2().await;
                                for prover in provers {
                                    all_provers_cache.insert(prover.peer_id.clone(), prover);
                                }
                            }
                            let prover_info = all_provers_cache.get(&peer_id);
                            match prover_info {
                                Some(info) => {
                                    send_response::<NodeResponse>(
                                        sender,
                                        NodeResponse::VotingPower {
                                            voting_power: info.voting_power.to_string(),
                                            peer_id,
                                            node_idx: info.idx,
                                        },
                                        "Error sending response",
                                    );
                                }
                                None => {
                                    error!(
                                        "Peer Id: {} not found in all_provers_cache. Available peer ids in all_provers_cache: {:?}",
                                        peer_id,
                                        all_provers_cache.iter().map(|(k, _)| k).collect::<Vec<_>>()
                                    );
                                    send_response::<NodeResponse>(
                                        sender,
                                        NodeResponse::Error {
                                            request_id: String::new(),
                                            message: "peer id not found".to_string(),
                                        },
                                        "Error sending response",
                                    );
                                }
                            }
                        } else {
                            // If the node is not a relay node, send an error response
                            error!("Not a relay node :{:?}", peer_id);
                            send_response::<NodeResponse>(
                                sender,
                                NodeResponse::Error {
                                    request_id: String::new(),
                                    message: "Not a relay node".to_string(),
                                },
                                "Error sending response",
                            );
                        }
                    }
                    None => {
                        error!("Node state not initialized :{:?}", peer_id);
                        // If the state is not initialized, send an error response
                        send_response::<NodeResponse>(
                            sender,
                            NodeResponse::Error {
                                request_id: String::new(),
                                message: "Node state not initialized".to_string(),
                            },
                            "Error sending response",
                        );
                    }
                }
            }
            AppStateChangeMessage::ForwardErrorResponse(request_id, response) => {
                if let Some(NodeState::Relay { pubsub_cache, .. }) = &mut state.state {
                    if pubsub_cache.contains_key(&request_id) {
                        let pubsub: &mut Pubsub<String, NodeResponse> = pubsub_cache.get_mut(&request_id).unwrap();
                        pubsub.publish(request_id.to_string(), response).await;
                    }
                }
            }
            AppStateChangeMessage::SetResharingEnabled(value, sender) => {
                if let Some(NodeState::Relay { is_resharing_enabled, .. }) = &mut state.state {
                    is_resharing_enabled.store(value, SeqCst);
                    info!("is_resharing_enabled set to {}", value);
                    send_response::<NodeResponse>(sender, NodeResponse::UpdatedIsResharingEnabled, "Error sending response");
                }
            }
        }
        Ok(())
    }
}
/// Handles a state request by querying the current state and responding with the appropriate data.
///
/// This function processes a `StateRequest` by checking the current node state. If the node is in the
/// `Relay` state, it queries Redis to get the total count of requests from the specified user for
/// the given method. It then sends a `StateResponse` with the current epoch, method, and request count.
///
/// If the node state is not `Relay`, an error response is sent indicating that the query was made to
/// an invalid node.
///
/// # Parameters
///
/// - `req`: The `StateRequest` containing the method and user information.
/// - `sender`: A `Sender` for sending the `StateResponse` back to the requester.
/// - `state`: A mutable reference to the `AppEngineState`, which holds the current state of the node.
#[tracing::instrument(name = "handle_state_request", skip(sender, state))]
async fn handle_state_request(req: StateRequest, sender: Sender<StateResponse>, state: &mut AppEngineState) {
    // Log the received state request, including the user information
    debug!("Received state request for user 0x{}", hex::encode(req.user.0));
    // Check if the current node state is of type Relay
    if let Some(NodeState::Relay { common_state, .. }) = &mut state.state {
        // Query Redis to get the total count of requests from the user for the specified method
        let requests_from_user = redis::cmd("GET")
            .arg(format!("total_count:{}:0x{}", req.method.as_str(), hex::encode(req.user.0)))
            .query(&mut common_state.redis.lock().await)
            .unwrap_or(0);
        // Send a success response containing the current epoch, method, and request count
        send_response::<StateResponse>(
            sender,
            StateResponse::Success {
                epoch: *common_state.current_epoch.lock().await,
                method: req.method,
                requests_from_user,
            },
            "Error sending response",
        );
    } else {
        // If the node state is not Relay, send an error response indicating an invalid node query
        send_response::<StateResponse>(
            sender,
            StateResponse::Error {
                message: "Queried to invalid node ".to_string(),
            },
            "Error sending response",
        );
    }
}

/// Handles a multiplication request in the context of a network protocol.
///
/// This function processes a request for a multiplication operation in a network. It performs various checks and operations, including verifying the network state, processing account requests, managing request batches, and interacting with external services such as IPFS and Othentic nodes.
///
/// # Arguments
///
/// * `request` - The request data to be processed, encapsulated in `RequestToNetwork`.
/// * `sender` - The sender for sending responses back to the requester, of type `Sender<NodeResponse>`.
/// * `state` - A mutable reference to the application state, of type `&mut AppEngineState`.
///
/// # Behavior
///
/// 1. **State Validation**: Checks if the current state is of type `Relay`. If not, logs an error and returns.
///
/// 2. **Node Validation**: Logs the no of ready nodes and peer IDs for active provers. If the number of ready nodes is less than the required number (`n`), sends an error response and returns.
///
/// 3. **Epoch Validation**: Ensures that the request epoch matches the current epoch. If not, sends an error response and returns.
///
/// 4. **Account Request Processing**: Processes the account request and determines the increment value. If there's an error, logs it and sends an error response.
///
/// 5. **Batch Management**: Pushes the request to a Redis batch queue and updates the batch size. If the batch size reaches the `TASK_SIZE`, processes the batch:
///    - **IPFS Interaction**: Pins the task proof to IPFS using Pinata and posts it to Othentic nodes. Handles timeouts and errors appropriately.
///    - **Batch Reset**: Resets the batch and batch size if the submission is successful.
///
/// 6. **Node Selection**: Prepares the list of selected nodes to forward the OPRF request based on active provers and new provers.
///
/// 7. **Message Forwarding**: Generates a UUID for tracking the request, forwards the OPRF request to the `GossipEngine`, and updates the request cache.
///
/// 8. **Response**: Sends a response indicating that the request has been submitted.
#[tracing::instrument(name = "handle_multiplication_request", skip(pubsub, state))]
async fn handle_multiplication_request(request: RequestToNetwork, pubsub: Pubsub<String, NodeResponse>, state: &mut AppEngineState, forward_to_indexes: Vec<u32>, request_id: String) {
    // Check if the current state is of type Relay and destructure its fields.
    if let Some(NodeState::Relay {
        ref mut req_cache,
        active_provers,
        t,
        epsilon,
        common_state,
        ready_nodes,
        new_cached_elected_provers,
        new_provers,
        monitor_request_queue,
        // usr_req_cache,
        pubsub_cache,
        batch_size,
        kafka,
        ..
    }) = &mut state.state
    {
        debug!("Handling multiplication request {}", request_id);
        trace!("Current ready nodes count: {}", ready_nodes.len());

        if ready_nodes.len() < *t as usize {
            debug!("Insufficient ready nodes ({} < {}), checking node liveness", ready_nodes.len(), t);
            if !new_provers.is_empty() {
                match ping_nodes(&mut new_provers.clone()).await {
                    Ok(nodes) => {
                        debug!("Node ping successful, updated ready nodes count: {}", nodes.len());
                        *ready_nodes = nodes;
                    }
                    Err(e) => {
                        warn!("Failed to ping nodes: {:?}, clearing ready nodes", e);
                        ready_nodes.clear();
                    }
                }
            }
        }

        pubsub_cache.insert(request_id.clone(), pubsub.clone());
        trace!("Added request {} to pubsub cache", request_id);

        for peer in active_provers.lock().await.iter() {
            trace!("Active prover peer ID: {}", peer.peer_id);
        }

        compare_provers(&active_provers, new_cached_elected_provers).await;

        // Check if the number of ready nodes is below the required number of nodes.
        if ready_nodes.len() < *t as usize {
            warn!("Network not ready ({} nodes available, need {})", ready_nodes.len(), t);
            pubsub
                .publish(
                    request_id.to_string(),
                    NodeResponse::Error {
                        request_id,
                        message: format!("Network is not ready. Total ready nodes {}", ready_nodes.len()),
                    },
                )
                .await;
            return;
        }

        // Verify that the request epoch matches the current epoch.
        if request.epoch != *common_state.current_epoch.lock().await {
            warn!("Invalid epoch in request (got {}, expected {})", request.epoch, *common_state.current_epoch.lock().await);
            pubsub
                .publish(
                    request_id.to_string(),
                    NodeResponse::Error {
                        request_id,
                        message: "Invalid Epoch".to_string(),
                    },
                )
                .await;
            return;
        }

        if let Ok(usr) = request.recover() {
            debug!("Processing request for user: {}", usr);

            // if usr_req_cache.contains_key(&usr.to_string()) {
            //     warn!("Duplicate request for user {}", usr);
            //     pubsub
            //         .publish(
            //             request_id.to_string(),
            //             NodeResponse::Error {
            //                 request_id,
            //                 message: format!("Waiting for previous request completion for usr {}", usr.to_string()),
            //             },
            //         )
            //         .await;
            //     return;
            // } else {
            //     trace!("Adding user {} to request cache", usr);
            //     usr_req_cache.insert(usr.to_string(), ());
            // }

            // Validate the request against the signer.
            let incr_by = match request.account_request_from_signer(&mut common_state.redis.lock().await, &common_state.eth, &kafka, true).await {
                Ok(incr) => {
                    trace!("Request validation successful, increment by: {}", incr);
                    incr
                }
                Err(err) => {
                    error!("Error validating account request: {:?}", err);
                    pubsub
                        .publish(
                            request_id.to_string(),
                            NodeResponse::Error {
                                request_id: request_id.clone(),
                                message: format!("Usr: {} Error: {}", usr.to_string(), err.to_string()),
                            },
                        )
                        .await;
                    return;
                }
            };

            let mut batch_size = batch_size.lock().await;
            *batch_size += incr_by;
            debug!("Updated batch size to: {}", *batch_size);
        }

        // Select nodes to forward the request to
        let mut tmp = vec![];
        let mut new_prover_idxes: Vec<usize> = new_provers.iter().map(|(_, (_, idx))| *idx).collect();
        new_prover_idxes.sort();

        let indices = if !forward_to_indexes.is_empty() {
            debug!("Using predefined forward indexes: {:?}", forward_to_indexes);
            forward_to_indexes
        } else {
            let selected = request.get_nodes(*t + *epsilon, new_prover_idxes);
            debug!("Selected nodes via algorithm: {:?}", selected);
            selected
        };

        info!("Forwarding request to nodes with indices: {:?}", indices);

        let active_provers_lock = active_provers.lock().await;
        let mut selected_nodes: Vec<(PeerId, Multiaddr)> = active_provers_lock
            .iter()
            .enumerate()
            .filter_map(|(_, prover_info)| {
                let idx = if let Some(val) = new_provers.get(&prover_info.peer_id) { val.1 } else { 0 };
                if indices.contains(&(idx as u32)) {
                    tmp.push(idx);
                    Some((prover_info.peer_id, prover_info.address.clone()))
                } else {
                    None
                }
            })
            .collect();

        if selected_nodes.len() as u32 <= *t {
            warn!("Insufficient active provers ({} <= {}), forwarding to all nodes", selected_nodes.len(), t);
            tmp.clear();
            selected_nodes = active_provers_lock
                .iter()
                .enumerate()
                .map(|(idx, prover_info)| {
                    tmp.push(idx);
                    (prover_info.peer_id, prover_info.address.clone())
                })
                .collect();
        }

        info!("Final selected nodes count: {}", selected_nodes.len());
        trace!("Selected nodes details: {:?}", selected_nodes);

        // Forward the request to selected nodes
        cast_message!(
            ActorType::GossipEngine,
            GossipEngineMessage::Forward(Message::ForwardMulRequest(request_id.clone(), request.clone()), selected_nodes),
            GossipEngineError
        );

        req_cache.entry(request_id.clone()).or_insert(RequestRecord { request, status: TxState::Submitted });
        debug!("Added request {} to request cache", request_id);

        monitor_request_queue.push(Delay::for_duration(request_id.to_string(), Duration::from_secs(MUL_REPONSE_WAIT_TIME)));
        info!("Request {} added to monitoring queue", request_id);
    } else {
        error!("Invalid node state - only Relay nodes can process multiplication requests");
    }
}
/// Updates the threshold parameters for the network based on the provided election information.
///
/// This function updates the number of total provers (`n`), the threshold (`t`), and the epsilon (`epsilon`) values in the application state based on the `ElectionInfo` provided. It is applicable to nodes in either the `Relay` or `Prover` state.
fn update_threshold(election_info: ElectionInfo, state: &mut AppEngineState) {
    // Log the start of threshold update
    debug!("Starting threshold update with election info: {:?}", election_info);

    if let Some(NodeState::Relay { n, t, epsilon, .. }) | Some(NodeState::Prover { n, t, epsilon, .. }) = &mut state.state {
        trace!("Current values - n: {}, t: {}, epsilon: {}", *n, *t, *epsilon);

        *n = election_info.total_provers;
        *t = election_info.threshold;
        *epsilon = election_info.epsilon;

        debug!("New values - n: {}, t: {}, epsilon: {}", *n, *t, *epsilon);

        // Validate the new parameters
        if *t >= *n {
            error!("Invalid threshold configuration: t ({}) must be less than n ({}). Threshold update rejected.", *t, *n);
            // Revert to previous safe values or handle error appropriately
            return;
        }

        if *t < 2 {
            error!("Invalid threshold: t ({}) must be at least 2 for security. Threshold update rejected.", *t);
            return;
        }

        if *n < 3 {
            error!("Invalid node count: n ({}) must be at least 3 for a distributed system. Threshold update rejected.", *n);
            return;
        }

        if *epsilon > *n - *t {
            error!("Invalid epsilon: {} cannot be greater than n-t ({}). Threshold update rejected.", *epsilon, *n - *t);
            return;
        }

        // Log successful update
        info!("Successfully updated threshold parameters - n: {}, t: {}, epsilon: {}", *n, *t, *epsilon);

        // If you had state persistence, you would do it here
        // debug!("Persisting new threshold state...");
        // save_state(&common_state.peer_id.to_string(), "n", *n);
        // save_state(&common_state.peer_id.to_string(), "t", *t);
        // save_state(&common_state.peer_id.to_string(), "epsilon", *epsilon);
    } else {
        warn!("Attempted to update thresholds on a node with invalid state (not Relay or Prover)");
    }
}

/// Asynchronously updates the list of elected provers in the application state.
#[tracing::instrument(skip(state, request))]
async fn update_elected_provers(request: Vec<ProverInfo>, state: &mut AppEngineState, is_cache_prover: bool) {
    debug!("Updating {} provers", if is_cache_prover { "cached" } else { "active" });
    trace!("Received {} prover infos", request.len());

    if let Some(NodeState::Relay {
        active_provers,
        new_cached_elected_provers,
        ..
    }) = &mut state.state
    {
        if is_cache_prover {
            *new_cached_elected_provers = request.clone();
            info!("Updated cached provers with {} entries. New cache size: {}", request.len(), new_cached_elected_provers.len());
            trace!("Cached provers details: {:?}", new_cached_elected_provers);
        } else {
            let mut active_provers = active_provers.lock().await;
            let prev_count = active_provers.len();
            *active_provers = request.clone();
            info!("Replaced active provers ({} -> {} entries)", prev_count, active_provers.len());
            debug!("New active provers: {:?}", active_provers);
        }
    } else {
        warn!("Attempted to update provers on a node that isn't a Relay");
    }
}

#[tracing::instrument(skip(state))]
fn update_node_idx(state: &mut Option<NodeState>, idx: u32) {
    debug!("Attempting to update node index to {}", idx);

    if let Some(NodeState::Prover { my_node_idx, .. }) = state {
        let old_idx = *my_node_idx;
        *my_node_idx = idx;
        info!("Updated node index from {} to {}", old_idx, idx);

        // If state persistence was enabled:
        // debug!("Persisting new node index...");
        // save_state(&common_state.peer_id.to_string(), "idx", idx);
    } else {
        warn!("Attempted to update node index on a non-Prover node");
    }
}

#[tracing::instrument(skip(data))]
pub fn check_memory_usage(data: &HashMap<String, Vec<u8>>, max_memory_usage: usize, data_type: &str) -> Result<(), String> {
    debug!("Checking memory usage for {} (max allowed: {} bytes)", data_type, max_memory_usage);
    trace!("Total entries to check: {}", data.len());

    let mut total_usage = 0;

    for (key, value) in data {
        let entry_size = value.len();
        total_usage += entry_size;

        trace!("{} entry '{}' size: {} bytes", data_type, key, entry_size);

        if entry_size > max_memory_usage {
            error!("{} entry '{}' exceeds limit ({} > {} bytes)", data_type, key, entry_size, max_memory_usage);
            return Err(format!("A {} entry exceeds the maximum allowed size ({} > {} bytes).", data_type, entry_size, max_memory_usage));
        }
    }

    info!("Memory check passed for {}: {} entries, total {} bytes (max {})", data_type, data.len(), total_usage, max_memory_usage);

    Ok(())
}

/// Stores a set of public keys, if and only if the node state is a Relay
//#[tracing::instrument(name = "store_kset", skip(state), fields(pubkey_shares_to_store = ?debug_key_shares(&pubkey_shares_to_store),group_public_keys = ?debug_key_shares(&group_public_keys)))]
#[tracing::instrument(skip(state, pubkey_shares_to_store, group_public_keys))]
async fn store_kset(state: &mut Option<NodeState>, peer_id: PeerId, pubkey_shares_to_store: PubkeyShares, idx: u128, group_public_keys: HashMap<String, Vec<u8>>) {
    debug!("Starting kset storage for peer: {}", peer_id);
    trace!("Received idx: {}, group_public_keys count: {}", idx, group_public_keys.len());

    if let Some(NodeState::Relay {
        pubkey_shares,
        ready_nodes,
        n,
        received_group_public_keys,
        finalized_group_public_keys,
        active_provers,
        no_of_shares_received,
        ..
    }) = state.as_mut()
    {
        let is_active = active_provers.lock().await.iter().any(|prover| prover.peer_id == peer_id);
        if !is_active {
            error!("Rejected kset from unauthorized peer: {} (not in active_provers list)", peer_id);
            return;
        }

        if let Err(e) = check_memory_usage(&pubkey_shares_to_store, 100, "pubkey_shares") {
            error!("Invalid pubkey_shares: {}", e);
            return;
        }

        if let Err(e) = check_memory_usage(&group_public_keys, 200, "group_public_keys") {
            error!("Invalid group_public_keys: {}", e);
            return;
        }

        let prev_shares_count = pubkey_shares.len();
        pubkey_shares.insert(peer_id, pubkey_shares_to_store);
        info!("Stored keyset from peer {} ({} -> {} total shares)", peer_id, prev_shares_count, pubkey_shares.len());

        ready_nodes.insert(idx);
        received_group_public_keys.push(group_public_keys);

        debug!(
            "Updated ready nodes ({} nodes, need {}), total received key sets: {}",
            ready_nodes.len(),
            *n,
            received_group_public_keys.len()
        );

        trace!("Current ready peers: {:?}", pubkey_shares.keys().cloned().collect::<Vec<PeerId>>());

        // Check if we've reached threshold
        if ready_nodes.len() as u32 == *n {
            info!("Required threshold of {} nodes reached, finalizing keys", *n);

            *finalized_group_public_keys = resolve_conflicts_between_group_keys(received_group_public_keys.clone());

            info!("Successfully finalized group public keys:");
            for (key_type, key) in finalized_group_public_keys.iter() {
                debug!("Key type '{}': {} bytes ({})", key_type, key.len(), hex::encode(&key[..8.min(key.len())]) + "...");
            }

            // If state persistence was enabled:
            // debug!("Persisting finalized keys...");
            // save_state(&common_state.peer_id.to_string(), "finalized_group_public_keys", &finalized_group_public_keys);
            // save_state(&common_state.peer_id.to_string(), "pubkey_shares", &pubkey_shares);
        }

        // Update shares counter
        no_of_shares_received.store(pubkey_shares.len(), Ordering::SeqCst);
        trace!("Updated shares counter to {}", pubkey_shares.len());
    } else {
        warn!("Attempted to store kset on a node that isn't a Relay");
    }
}

#[tracing::instrument(skip(group_public_keys))]
fn resolve_conflicts_between_group_keys(group_public_keys: Vec<HashMap<String, Vec<u8>>>) -> HashMap<String, Vec<u8>> {
    debug!("Resolving conflicts between {} group key sets", group_public_keys.len());
    let mut finalized_group_public_keys = HashMap::new();
    let mut value_count = HashMap::new();

    for (i, keys_map) in group_public_keys.iter().enumerate() {
        trace!("Processing key set {} with {} entries", i, keys_map.len());
        for (key, value) in keys_map {
            let entry = value_count.entry(key.clone()).or_insert_with(HashMap::new);
            *entry.entry(value.clone()).or_insert(0) += 1;
            trace!("Counted value for key '{}' (len: {})", key, value.len());
        }
    }

    for (key, counts) in value_count {
        let (max_value, max_count) = counts.into_iter().max_by_key(|&(_, count)| count).unwrap();

        debug!("Selected value for key '{}' (occurrences: {}, length: {})", key, max_count, max_value.len());

        finalized_group_public_keys.insert(key, max_value);
    }

    info!("Resolved conflicts, finalized {} group public keys", finalized_group_public_keys.len());
    finalized_group_public_keys
}

#[tracing::instrument(skip(state, received_tstar_provers_map, received_new_provers_map))]
async fn store_quorum_map(state: &mut Option<NodeState>, received_tstar_provers_map: HashMap<PeerId, (ProverInfo, usize)>, received_new_provers_map: HashMap<PeerId, (ProverInfo, usize)>) {
    debug!(
        "Storing quorum maps (T* provers: {}, new provers: {})",
        received_tstar_provers_map.len(),
        received_new_provers_map.len()
    );

    if let Some(NodeState::Relay { tstar_provers, new_provers, .. }) = state.as_mut() {
        let old_tstar_count = tstar_provers.len();
        let old_new_count = new_provers.len();

        **tstar_provers = received_tstar_provers_map;
        **new_provers = received_new_provers_map;

        info!(
            "Updated prover maps - T*: {} -> {}, New: {} -> {}",
            old_tstar_count,
            tstar_provers.len(),
            old_new_count,
            new_provers.len()
        );

        // If state persistence was enabled:
        // debug!("Persisting new prover maps...");
        // save_state(&common_state.peer_id.to_string(), "new_provers", &new_provers);
        // save_state(&common_state.peer_id.to_string(), "tstar_provers", &tstar_provers);
    } else {
        warn!("Attempted to store quorum maps on non-Relay node");
    }
}

#[tracing::instrument(
    name = "store_resharing_kset",
    skip(state),
    fields(
        peer_id = %peer_id,
        idx = idx,
        shares_count = pubkey_shares_to_store.len()
    )
)]
async fn store_resharing_kset(state: &mut Option<NodeState>, peer_id: PeerId, pubkey_shares_to_store: PubkeyShares, idx: u128) {
    debug!("Storing resharing kset from peer {}", peer_id);

    if let Some(NodeState::Relay {
        pubkey_shares,
        active_provers,
        ready_nodes,
        n,
        new_cached_elected_provers,
        no_of_shares_received,
        ..
    }) = state.as_mut()
    {
        if let Err(e) = check_memory_usage(&pubkey_shares_to_store, 100, "pubkey_shares") {
            error!("Invalid pubkey shares from {}: {}", peer_id, e);
            return;
        }

        let in_quorum = new_cached_elected_provers.iter().any(|prover| prover.peer_id == peer_id);

        if in_quorum {
            let prev_shares_count = pubkey_shares.len();
            pubkey_shares.insert(peer_id, pubkey_shares_to_store);
            ready_nodes.insert(idx);

            info!(
                "Stored resharing kset from {} ({} -> {} shares, {} ready nodes)",
                peer_id,
                prev_shares_count,
                pubkey_shares.len(),
                ready_nodes.len()
            );

            trace!("Current participants: {:?}", pubkey_shares.keys().collect::<Vec<_>>());

            if ready_nodes.len() as u32 == *n {
                info!("Threshold reached ({} nodes), updating active provers ({} provers)", *n, new_cached_elected_provers.len());

                *(active_provers.lock().await) = new_cached_elected_provers.clone();
            }
        } else {
            warn!("Rejected kset from {} - not in current quorum (quorum size: {})", peer_id, new_cached_elected_provers.len());
        }

        let new_count = pubkey_shares.len();
        no_of_shares_received.store(new_count, Ordering::SeqCst);
        debug!("Updated shares counter to {}", new_count);
    } else {
        warn!("Attempted to store resharing kset on non-Relay node");
    }
}

/// Fetches the reconstructed point for a given request ID
#[tracing::instrument(
    name = "fetch_reconstructed_point",
    skip(state, sender),
    fields(request_id = %request_id)
)]
fn fetch_reconstructed_point(state: &mut Option<NodeState>, request_id: &str, sender: Sender<NodeResponse>) {
    debug!("Fetching reconstructed point for request {}", request_id);

    if let Some(NodeState::Relay { req_cache, res_cache, .. }) = state {
        match req_cache.get(request_id) {
            Some(status) => {
                let response = res_cache.get(request_id);
                debug!("Found request status: {:?}, response present: {}", status.status, response.is_some());

                let msg = if let TxState::Verified = status.status {
                    response.and_then(|r: &StoredMultResult| {
                        trace!("Processing verified result for curve: {}", r.curve);
                        if let Some(point) = r.reconstructed_point.clone() {
                            match r.curve.as_str() {
                                Secp256k1::NAME => {
                                    debug!("Decoding Secp256k1 point");
                                    Some(NodeResponse::VerifiedProofSecp256k1 {
                                        request_id: request_id.to_string(),
                                        reconstructed_point: <Secp256k1 as Curve<32>>::Point::from_encoded(&point)
                                            .map_err(|e| {
                                                error!("Failed to decode Secp256k1 point: {}", e);
                                                e
                                            })
                                            .expect("Invalid point encoding"),
                                    })
                                }
                                BabyJubJub::NAME => {
                                    debug!("Decoding BabyJubJub point");
                                    Some(NodeResponse::VerifiedProofBabyJubJub {
                                        request_id: request_id.to_string(),
                                        reconstructed_point: bincode::deserialize(&point)
                                            .map_err(|e| {
                                                error!("Failed to deserialize BabyJubJub point: {}", e);
                                                e
                                            })
                                            .expect("Invalid point encoding"),
                                    })
                                }
                                curve => {
                                    warn!("Unrecognized curve type: {}", curve);
                                    Some(NodeResponse::Error {
                                        request_id: request_id.to_string(),
                                        message: format!("Error: Curve '{}' not recognized", curve),
                                    })
                                }
                            }
                        } else {
                            warn!("No reconstructed point available");
                            Some(NodeResponse::Error {
                                request_id: request_id.to_string(),
                                message: "Error: Point is not reconstructed".to_string(),
                            })
                        }
                    })
                } else {
                    debug!("Request not yet verified (status: {:?})", status.status);
                    Some(NodeResponse::Error {
                        request_id: request_id.to_string(),
                        message: "Not yet verified".to_string(),
                    })
                };

                let response_msg = msg.unwrap_or_else(|| {
                    error!("Failed to construct response message");
                    NodeResponse::Error {
                        request_id: request_id.to_string(),
                        message: "Internal server error".to_string(),
                    }
                });

                if let Err(e) = sender.send(response_msg) {
                    error!("Failed to send response: {:?}", e);
                }
            }
            None => {
                warn!("Request ID not found in cache");
                let _ = sender.send(NodeResponse::Error {
                    request_id: request_id.to_string(),
                    message: "Request ID not found".to_string(),
                });
            }
        }
    } else {
        error!("Attempted to fetch reconstructed point from non-Relay node");
        let _ = sender.send(NodeResponse::Error {
            request_id: request_id.to_string(),
            message: "Only Relay nodes can process this request".to_string(),
        });
    }
}

/// Fetches the reconstructed point for a given request ID (v2)
#[tracing::instrument(
    name = "fetch_reconstructed_point_v2",
    skip(data, r, pubsub),
    fields(request_id = %request_id)
)]
async fn fetch_reconstructed_point_v2(data: RequestRecord, r: StoredMultResult, pubsub: Pubsub<String, NodeResponse>, request_id: &str) {
    debug!("Processing reconstructed point (v2) for request {}", request_id);
    trace!("Request data: {:?}, Result data: {:?}", data, r);

    let msg = if let TxState::Verified = data.status {
        debug!("Request is verified, processing point reconstruction");
        if let Some(point) = r.reconstructed_point.clone() {
            trace!("Found reconstructed point (length: {})", point.len());
            match r.curve.as_str() {
                Secp256k1::NAME => {
                    debug!("Decoding Secp256k1 point");
                    Some(NodeResponse::VerifiedProofSecp256k1 {
                        request_id: request_id.to_string(),
                        reconstructed_point: <Secp256k1 as Curve<32>>::Point::from_encoded(&point)
                            .map_err(|e| {
                                error!("Secp256k1 point decoding failed: {}", e);
                                e
                            })
                            .expect("Invalid point encoding"),
                    })
                }
                BabyJubJub::NAME => {
                    debug!("Decoding BabyJubJub point");
                    Some(NodeResponse::VerifiedProofBabyJubJub {
                        request_id: request_id.to_string(),
                        reconstructed_point: bincode::deserialize(&point)
                            .map_err(|e| {
                                error!("BabyJubJub point deserialization failed: {}", e);
                                e
                            })
                            .expect("Invalid point encoding"),
                    })
                }
                curve => {
                    warn!("Unsupported curve type: {}", curve);
                    Some(NodeResponse::Error {
                        request_id: request_id.to_string(),
                        message: format!("Unsupported curve type: {}", curve),
                    })
                }
            }
        } else {
            warn!("No reconstructed point available");
            Some(NodeResponse::Error {
                request_id: request_id.to_string(),
                message: "Point reconstruction not available".to_string(),
            })
        }
    } else {
        debug!("Request not yet verified (status: {:?})", data.status);
        Some(NodeResponse::Error {
            request_id: request_id.to_string(),
            message: format!("Request not yet verified (status: {:?})", data.status),
        })
    };

    let response = msg.unwrap_or_else(|| {
        error!("Failed to construct response message");
        NodeResponse::Error {
            request_id: request_id.to_string(),
            message: "Internal server error".to_string(),
        }
    });
    pubsub.publish(request_id.to_string(), response).await
}

/// Processes a received DLEQ proof and updates the relevant caches.
///
/// This function processes a DLEQ proof received from a peer, verifies it using the associated public key shares, and updates the caches with the results. The function operates if the node state is a `Relay` and manages the verification and storage of proofs based on the method used.
///
/// # Type Parameters
///
/// * `N` - The size of the curve's scalar field.
/// * `C` - The curve type implementing the `Curve` trait.
///
/// # Arguments
///
/// * `state` - A mutable reference to an `Option<NodeState>`. The function operates if `state` is `Some(NodeState::Relay)`.
/// * `node_idx` - The index of the node receiving the proof.
/// * `peer_id` - The ID of the peer that sent the proof.
/// * `request_id` - A string slice representing the ID of the request for which the proof is being processed.
/// * `proof` - A boxed `DLEQProof` containing the proof data to be verified.
/// * `method` - The method used for the proof verification, which determines how the public key share should be retrieved.
///
/// # Behavior
///
/// 1. **State Check**: Validates that the `state` is `Some(NodeState::Relay)`. If not, the function does nothing.
///
/// 2. **Proof Processing**
#[tracing::instrument(
    name = "process_received_proof",
    skip(state, proof),
    fields(
        node_idx,
        peer_id = %peer_id,
        request_id
    )
)]
async fn process_received_proof<const N: usize, C: Curve<N>>(state: &mut Option<NodeState>, node_idx: u32, peer_id: PeerId, request_id: &str, proof: Box<DLEQProof<N, C>>, method: Method) {
    debug!("Processing received proof for request {}", request_id);
    trace!("Proof details: {:?}", proof);

    if let Some(NodeState::Relay {
        res_cache,
        req_cache,
        pubkey_shares,
        t,
        common_state,
        pinata,
        batch_size,
        othentic_rpc_url,
        batch_cache,
        // usr_req_cache,
        pubsub_cache,
        kafka,
        active_provers,
        ..
    }) = state
    {
        debug!(
            "Cache sizes - Req: {}, Res: {}, PubSub: {}, Batch: {}",
            req_cache.len(),
            res_cache.len(),
            pubsub_cache.len(),
            batch_cache.len()
        );

        match req_cache.get_mut(request_id) {
            Some(data) => {
                info!("Processing proof from peer {} for request {}", peer_id, request_id);

                // Verify peer has valid public key shares
                let pubkey_shares_for_peer = match pubkey_shares.get(&peer_id) {
                    Some(shares) => shares,
                    None => {
                        error!("No public key shares found for peer: {}", peer_id);
                        return;
                    }
                };

                // Get appropriate public key based on method
                let pub_key_share = match method {
                    Method::OPRFSecp256k1 => "OPRFSecp256k1",
                    Method::OPRFBabyJubJub => "OPRFBabyJubJub",
                    Method::JWTPRFSecp256k1 => "JWTPRFSecp256k1",
                    Method::DecryptBabyJubJub => "DecryptBabyJubjub",
                };

                let pub_key_share = match pubkey_shares_for_peer.get(pub_key_share) {
                    Some(key) => key,
                    None => {
                        error!("No public key found for method {}", pub_key_share);
                        return;
                    }
                };

                // Decode points and verify proof
                let masked_point = match C::Point::from_encoded(&data.request.point) {
                    Ok(point) => point,
                    Err(e) => {
                        error!("Failed to decode masked point: {}", e);
                        return;
                    }
                };

                let pub_key_share_point = match C::Point::from_encoded(pub_key_share) {
                    Ok(point) => point,
                    Err(e) => {
                        error!("Cannot decode pub key share for node {}: {}", node_idx, e);
                        return;
                    }
                };

                // Verify the DLEQ proof
                match proof.verify_and_get_output(&pub_key_share_point, &masked_point) {
                    Err(e) => {
                        warn!("Proof verification failed: {:?}", e);
                    }
                    Ok(result) => {
                        info!("Proof verification successful for request {}", request_id);

                        // Update result cache
                        let collection = res_cache.entry(request_id.to_string()).or_insert_with(|| StoredMultResult {
                            num_nodes: 0,
                            mul_results: Vec::new(),
                            reconstructed_point: None,
                            curve: C::NAME.to_string(),
                        });

                        collection.num_nodes += 1;
                        collection.mul_results.push((node_idx, result.encode()));
                        debug!("Updated proof collection - total proofs: {}", collection.num_nodes);

                        let serialized_proof = match serde_json::to_vec(&proof) {
                            Ok(data) => data,
                            Err(e) => {
                                error!("Failed to serialize proof: {}", e);
                                return;
                            }
                        };

                        let mut conn = common_state.redis.lock().await;
                        let serde_request_slice = match serde_json::to_vec(&data.request) {
                            Ok(data) => data,
                            Err(e) => {
                                error!("Failed to serialize request: {}", e);
                                return;
                            }
                        };

                        let request_hash = hex::encode(human_crypto::hash256(serde_request_slice));
                        debug!("Generated request hash: {}", request_hash);

                        if batch_cache.contains_key(&request_hash) {
                            data.status = TxState::Verified;
                            warn!("Proofs for batch {} already processed - ignoring new proof", request_hash);
                            return;
                        } else {
                            data.status = TxState::ConstructedProof;
                        }

                        match redis::cmd("GET").arg(&request_hash).query::<Option<RequestToNetworkWithProofs>>(&mut conn) {
                            Ok(Some(mut existing)) => {
                                existing.proofs.push((
                                    hex::encode(&serialized_proof),
                                    hex::encode(pub_key_share_point.encode()),
                                    hex::encode(masked_point.encode()),
                                    peer_id.to_string(),
                                ));

                                debug!("Updating Redis with additional proof for {}", request_hash);

                                if let Err(err) = redis::cmd("SET").arg(&request_hash).arg(&existing).query::<()>(&mut conn) {
                                    error!("Redis update failed: {}", err);
                                    return;
                                }
                            }
                            Ok(None) => {
                                let response_data = RequestToNetworkWithProofs {
                                    base: data.request.clone(),
                                    proofs: vec![(
                                        hex::encode(&serialized_proof),
                                        hex::encode(pub_key_share_point.encode()),
                                        hex::encode(masked_point.encode()),
                                        peer_id.to_string(),
                                    )],
                                };

                                debug!("Creating new Redis entry for {}", request_hash);

                                if let Err(err) = redis::cmd("SET").arg(&request_hash).arg(&response_data).query::<()>(&mut conn) {
                                    error!("Redis insert failed: {}", err);
                                    return;
                                }
                            }
                            Err(err) => {
                                error!("Redis lookup failed: {}", err);
                                return;
                            }
                        }

                        drop(conn);

                        collection
                            .process_verification::<N, C>(
                                data,
                                request_id,
                                *t,
                                common_state,
                                batch_size,
                                othentic_rpc_url.clone(),
                                pinata.clone(),
                                request_hash,
                                batch_cache,
                                // usr_req_cache,
                                pubsub_cache,
                                active_provers,
                                kafka,
                            )
                            .await;
                    }
                }
            }
            None => {
                warn!("Request {} not found in cache - cannot process proof", request_id);
            }
        }
    } else {
        error!("Attempted to process proof on non-Relay node");
    }
}

/// Processes an incoming request and handles it based on the request method.
///
/// This asynchronous function processes a request received by a node. It verifies that the request is valid for the current node state, checks for epoch consistency, and validates the request against a signer. Based on the method specified in the request, it delegates the handling to the appropriate function.
#[tracing::instrument(
    name = "process_request",
    skip(state, tx),
    fields(
        request_id,
        epoch = request.epoch
    )
)]
async fn process_request(state: &mut Option<NodeState>, request_id: &str, request: RequestToNetwork, tx: Sender<NodeResponse>) {
    debug!("Processing request {}", request_id);

    if let Some(NodeState::Prover {
        my_node_idx,
        n,
        t,
        epsilon,
        common_state,
        private_keyshares,
        new_provers,
        ..
    }) = state
    {
        let new_provers_idxes: Vec<usize> = new_provers.values().map(|(_, idx)| *idx).collect();
        debug!("Checking node eligibility with indexes: {:?}", new_provers_idxes);

        if !request.is_for_my_node(*t + *epsilon, *my_node_idx, new_provers_idxes) {
            error!("Request not for this node (idx: {}). Network params - n: {}, t: {}, ε: {}", my_node_idx, n, t, epsilon);

            let response = NodeResponse::Error {
                request_id: request_id.to_string(),
                message: "Request not for this node".to_string(),
            };

            if let Err(e) = tx.send(response) {
                error!("Failed to send rejection response: {:?}", e);
            }
            return;
        }

        // Validate request epoch
        let current_epoch = common_state.current_epoch.lock().await;
        if request.epoch != *current_epoch {
            warn!("Invalid epoch (got: {}, expected: {})", request.epoch, *current_epoch);

            let response = NodeResponse::Error {
                request_id: request_id.to_string(),
                message: "Invalid epoch".to_string(),
            };

            if let Err(e) = tx.send(response) {
                error!("Failed to send epoch error response: {:?}", e);
            }
            return;
        }
        drop(current_epoch);

        // match request.account_request_from_signer(&mut common_state.redis.lock().await, &common_state.eth, true).await {
        //     Ok(_) => debug!("Request signature validated"),
        //     Err(e) => {
        //         error!("Request validation failed: {}", e);

        //         let response = NodeResponse::Error {
        //             request_id: request_id.to_string(),
        //             message: format!("Validation error: {:?}", e),
        //         };

        //         if let Err(e) = tx.send(response) {
        //             error!("Failed to send validation error response: {:?}", e);
        //         }
        //         return;
        //     }
        // };

        info!("Processing valid request for method {:?}", request.method);

        // Handle request based on method
        match request.method {
            Method::OPRFSecp256k1 => handle_request!(
                32,
                Secp256k1,
                private_keyshares.get("OPRFSecp256k1").expect("Secp256k1 key share not found"),
                request_id,
                request,
                tx,
                *my_node_idx,
                request.method,
                common_state
            ),
            Method::OPRFBabyJubJub => handle_request!(
                32,
                BabyJubJub,
                private_keyshares.get("OPRFBabyJubJub").expect("BabyJubJub key share not found"),
                request_id,
                request,
                tx,
                *my_node_idx,
                request.method,
                common_state
            ),
            Method::JWTPRFSecp256k1 => handle_request!(
                32,
                Secp256k1,
                private_keyshares.get("JWTPRFSecp256k1").expect("JWTPRF key share not found"),
                request_id,
                request,
                tx,
                *my_node_idx,
                request.method,
                common_state
            ),
            Method::DecryptBabyJubJub => handle_request!(
                32,
                BabyJubJub,
                private_keyshares.get("DecryptBabyJubjub").expect("Decryption key share not found"),
                request_id,
                request,
                tx,
                *my_node_idx,
                request.method,
                common_state
            ),
        }
    } else {
        error!("Attempted to process request on non-Prover node");
    }
}

#[tracing::instrument(
    name = "monitor_quorum_formation",
    skip(no_of_shares_received, cancel_ctx, kafka),
    fields(
        target_shares = no_of_shares_to_monitor,
        event = ?event
    )
)]
pub async fn monitor_quorum_formation(
    no_of_shares_to_monitor: usize,
    no_of_shares_received: Arc<AtomicUsize>,
    resharing_failure_count: Arc<AtomicUsize>,
    cancel_ctx: CancellationContext,
    event: MonitorEvent,
    new_elected_active_provers: Arc<Mutex<Vec<ProverInfo>>>,
    kafka: Arc<KafkaProducer>,
) {
    info!("Starting quorum monitoring for {:?}", event);

    let cancel_token = cancel_ctx.token();
    match event {
        MonitorEvent::DKG | MonitorEvent::Resharing => {
            let mut interval = time::interval(Duration::from_secs(1));
            let mut time_elapsed = 0;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let current_shares = no_of_shares_received.load(Ordering::SeqCst);
                        debug!("Shares progress: {}/{}", current_shares, no_of_shares_to_monitor);

                        if current_shares == no_of_shares_to_monitor {
                            info!("Quorum formation successful!");

                            debug!("Triggering state persistence");
                            cast_message!(
                                ActorType::AppStateEngine,
                                AppStateChangeMessage::SaveCurrentState,
                                AppStateEngineError
                            );
                            cast_message!(
                                ActorType::GossipEngine,
                                GossipEngineMessage::Gossip(
                                    Message::SaveCurrentState(),
                                    NETWORK_TOPIC.to_string(),
                                ),
                                GossipEngineError
                            );
                            kafka.send(
                                KafkaTopic::QuorumResharingStatus,
                                "current_quorum",
                                QuorumResharingInfo {
                                    success: true,
                                    multipliers: new_elected_active_provers.lock().await.clone(),
                                }
                            ).await;
                            break;
                        } else {
                            time_elapsed += 1;

                            if time_elapsed >= DKG_RESHARING_WAIT_TIME {
                                warn!(
                                    "Quorum formation timeout after {} seconds ({} shares received)",
                                    DKG_RESHARING_WAIT_TIME,
                                    current_shares
                                );

                                no_of_shares_received.store(0, Ordering::SeqCst);

                                match event {
                                    MonitorEvent::DKG => {
                                        info!("Restarting DKG process");
                                        cast_message!(
                                            ActorType::ElectionEngine,
                                            ElectionEngineMessage::FirstElection,
                                            ElectionEngineError
                                        );
                                    },
                                    MonitorEvent::Resharing => {
                                        info!("Rolling back to previous state");
                                        cast_message!(
                                            ActorType::AppStateEngine,
                                            AppStateChangeMessage::RollbackToPreviousState,
                                            AppStateEngineError
                                        );
                                        cast_message!(
                                            ActorType::GossipEngine,
                                            GossipEngineMessage::Gossip(
                                                Message::RollbackToPreviousState(),
                                                NETWORK_TOPIC.to_string(),
                                            ),
                                            GossipEngineError
                                        );
                                        if resharing_failure_count.load(Ordering::SeqCst) <= MAX_RESHARING_ATTEMPTS_AT_A_TIME as usize {
                                            resharing_failure_count.fetch_add(1, Ordering::SeqCst);
                                            tokio::spawn(async move {
                                                    tokio::time::sleep(std::time::Duration::from_secs(RESHARING_REATTEMPT_WAIT_TIME)).await;
                                                    cast_message!(
                                                        ActorType::AppStateEngine,
                                                        AppStateChangeMessage::PreResharing,
                                                        AppStateEngineError
                                                    );
                                            });
                                        } else {
                                            info!("resharing failure count exceeded, stopping resharing attempts. Failed attempts: {}", resharing_failure_count.load(Ordering::SeqCst));
                                        }
                                        kafka.send(
                                            KafkaTopic::QuorumResharingStatus,
                                            "current_quorum",
                                            QuorumResharingInfo {
                                                success: false,
                                                multipliers: new_elected_active_provers.lock().await.clone(),
                                            }
                                        ).await;
                                    },
                                    _ => unreachable!()
                                }
                                break;
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        let reason = cancel_ctx.get_reason()
                            .unwrap_or_else(|| "unspecified".to_string());
                        info!("Monitoring cancelled: {}", reason);
                        break;
                    }
                }
            }
        }
        MonitorEvent::MultiplicationResult(_) => {
            warn!("Invalid use of quorum monitoring for multiplication result");
        }
    }
}
pub struct AppStateSupervisor {
    panic_tx: tokio::sync::mpsc::Sender<ActorCell>,
}
impl AppStateSupervisor {
    pub fn new(panic_tx: tokio::sync::mpsc::Sender<ActorCell>) -> Self { Self { panic_tx } }
}
#[derive(Debug, Error, Default)]
pub enum AppStateSupervisorError {
    #[default]
    #[error("failed to acquire AppStateSupervisorError from registry")]
    RactorRegistryError,
}
#[async_trait]
impl Actor for AppStateSupervisor {
    type Msg = AppStateChangeMessage;
    type State = ();
    type Arguments = ();
    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, _args: ()) -> Result<Self::State, ActorProcessingErr> { Ok(()) }
    async fn handle_supervisor_evt(&self, _myself: ActorRef<Self::Msg>, message: SupervisionEvent, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        tracing::warn!("Received a supervision event: {:?}", message);
        match message {
            SupervisionEvent::ActorStarted(actor) => {
                tracing::info!("Actor started: {:?}, status: {:?}", actor.get_name(), actor.get_status());
            }
            SupervisionEvent::ActorFailed(who, reason) => {
                tracing::error!("Actor panicked: {:?}, err: {:?}", who.get_name(), reason);
                let _ = self.panic_tx.send(who).await;
            }
            SupervisionEvent::ActorTerminated(who, _, reason) => {
                tracing::error!("Actor terminated: {:?}, err: {:?}", who.get_name(), reason);
            }
            SupervisionEvent::PidLifecycleEvent(event) => {
                tracing::info!("A process lifecycle event occurred: {:?}", event);
            }
            SupervisionEvent::ProcessGroupChanged(m) => {
                tracing::info!("A subscribed process group changed");
                group_changed(m);
            }
        }
        Ok(())
    }
}

pub async fn compare_provers(active_provers: &Arc<Mutex<Vec<ProverInfo>>>, new_cached_elected_provers: &Vec<ProverInfo>) {
    let active_provers_locked = active_provers.lock().await;
    debug!("Starting comparison of active and new elected provers");

    let active_peer_ids: HashSet<_> = active_provers_locked.iter().map(|p| &p.peer_id).collect();
    let new_peer_ids: HashSet<_> = new_cached_elected_provers.iter().map(|p| &p.peer_id).collect();

    trace!("Active provers count: {}, New elected provers count: {}", active_peer_ids.len(), new_peer_ids.len());

    let active_not_in_new: HashSet<_> = active_peer_ids.difference(&new_peer_ids).collect();
    let new_not_in_active: HashSet<_> = new_peer_ids.difference(&active_peer_ids).collect();

    if !active_not_in_new.is_empty() {
        warn!(
            "Found {} peer IDs in active_provers that are not in new elected provers: {:?}",
            active_not_in_new.len(),
            active_not_in_new
        );
    } else {
        debug!("No peer IDs found in active_provers that are not in new elected provers");
    }

    if !new_not_in_active.is_empty() {
        warn!(
            "Found {} peer IDs in new elected provers that are not in active_provers: {:?}",
            new_not_in_active.len(),
            new_not_in_active
        );
    } else {
        debug!("No peer IDs found in new elected provers that are not in active_provers");
    }

    // Compare sizes and log
    let active_count = active_provers_locked.len();
    let new_count = new_cached_elected_provers.len();

    if active_count > new_count {
        info!("Active provers list is larger: {} entries vs {} in new elected provers", active_count, new_count);
    } else if active_count < new_count {
        info!("New elected provers list is larger: {} entries vs {} in active provers", new_count, active_count);
    } else {
        info!("Active and new elected provers lists have equal size: {} entries", active_count);
    }

    // Log completion
    debug!("Completed comparison of active and new elected provers");
}
