//! This actor defines the `ElectionEngineActor`, which manages the election process
//! in a decentralized network. It handles election-related messages, manages election
//! state, and determines quorum formation based on available provers.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
// Third-party library imports
use async_trait::async_trait;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use lazy_static::lazy_static;
use libp2p::{Multiaddr, PeerId};
use human_crypto::node_selection::{get_epsilon, get_min_nodes, get_threshold, set_epsilon, set_min_nodes, set_threshold, weighted_sample_n_no_replace};
use ractor::{
    concurrency::{sleep, Duration},
    Actor, ActorCell, ActorProcessingErr, ActorRef, SupervisionEvent,
};
use thiserror::*;
use tokio::{runtime::Runtime, sync::Mutex};
use tracing::{debug, error, info, trace, warn};
// Project-specific imports
use messages::{
    actor_type::ActorType,
    drand::{fetch_drand_data, verify_drand, DrandResponse},
    kafka::{KafkaProducer, PeerReachabilityTCPStatus},
    message::{AppStateChangeMessage, DkgEngineMessage, ElectionEngineMessage, ElectionInfo, GossipEngineMessage, Message, ProverInfo},
    types::NodeResponse,
    ElectionState, NETWORK_TOPIC,
};
use network::utils::{fetch_provers_v2, send_response, NodeType};
use rand::Rng;
use rpc_trait::rpc::HumanRpcClient;
// Local crate imports
use crate::{
    app_state_actor::{get_state, save_state, AppStateEngineError},
    cast_message,
    dkg_engine_actor::DKGEngineError,
    election_context::ReElectionCheckContext,
    gossip_engine_actor::GossipEngineError,
    group_changed, SLEEP_PING, TOTAL_BOOTSTRAP_RELAY_NODES,
};

pub const DRAND_ROUND_THRESHOLD_TOLERNACE: u64 = 3;

lazy_static! {
    /// A list of peer IDs that are temporarily excluded from elections
    pub static ref TEMP_EXCLUSION_PEER_LIST: Mutex<HashSet<PeerId>> = Mutex::new(HashSet::new());
}

/// A generic error type to propagate errors from this actor
/// and other actors that interact with it
#[derive(Debug, Clone, Error)]
pub enum ElectionEngineError {
    #[error("Error occurred in election engine: {0}")]
    Custom(String),
}
impl Default for ElectionEngineError {
    fn default() -> Self { ElectionEngineError::Custom("ElectionEngine unable to acquire actor".to_string()) }
}
/// The actor struct for the Election Engine actor
#[derive(Clone, Debug, Default)]
pub struct ElectionEngineActor;
impl ElectionEngineActor {
    pub fn new() -> Self { Self }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelectionStatus {
    ReElected,
    ResharingInit,
    ResharingRound1,
    ResharingRound2,
    ResharingRound3,
}
#[derive(Clone, Debug)]
pub struct ElectionEngineState {
    pub node_type: NodeType,
    pub peer_id: PeerId,
    pub relay_node: (PeerId, Multiaddr),
    pub current_state: ElectionState,
    pub tstar_provers: Vec<ProverInfo>,
    pub elected_provers: Vec<ProverInfo>,
    pub current_threshold: u32,
    pub election_status: HashSet<PeerId>,
    pub re_election_status: HashMap<PeerId, RelectionStatus>,
    pub dkg_init_status: HashSet<PeerId>,
    pub re_sharing_init_status: HashMap<PeerId, RelectionStatus>,
    pub dkg_round1_status: HashSet<PeerId>,
    pub dkg_round2_status: HashSet<PeerId>,
    pub re_sharing_round1_status: HashMap<PeerId, RelectionStatus>,
    pub re_sharing_round2_status: HashMap<PeerId, RelectionStatus>,
    pub re_election_check_context: Option<ReElectionCheckContext>,
    pub kafka: Option<Arc<KafkaProducer>>,
}
impl ElectionEngineState {
    /// Conduct an election process and update the state with results
    #[tracing::instrument]
    pub async fn conduct_election(&mut self) -> Result<(), ElectionEngineError> {
        debug!("Initiating election process for ElectionEngine");

        trace!("Transitioning to TriggerElection state");
        self.current_state = ElectionState::TriggerElection;
        debug!("State updated to TriggerElection: {:?}", self.current_state);

        trace!("Fetching provers for election");
        self.elected_provers = fetch_provers_v2().await;
        trace!("Filtering elected provers list");
        self.elected_provers = self.elected_provers[2..].to_vec();
        debug!("Fetched and filtered {} elected provers", self.elected_provers.len());

        trace!("Transitioning to QuorumElected state");
        self.current_state = ElectionState::QuorumElected;
        debug!("State updated to QuorumElected");

        debug!("Election completed successfully");
        trace!("Logging elected provers");
        for (i, prover) in self.elected_provers.iter().enumerate() {
            trace!("Prover {}: index {}, details {:?}", i, prover.idx, prover);
        }

        Ok(())
    }

    /// Calculate the threshold based on the number of provers and threshold percentage
    fn calculate_threshold(n: usize, threshold_percentage: f64) -> u32 {
        // Trace: Log function entry
        trace!("Entering calculate_threshold with n={}, threshold_percentage={}", n, threshold_percentage);

        // Validate inputs
        if threshold_percentage < 0.0 || threshold_percentage > 100.0 {
            error!("Invalid threshold_percentage: {}. Must be between 0.0 and 100.0", threshold_percentage);
            return 0;
        }

        if n == 0 {
            warn!("Input n is 0, returning threshold of 0");
            return 0;
        }

        let intermediate = n as f64 * threshold_percentage / 100.0;
        debug!("Intermediate value before ceiling: {}", intermediate);

        let result = intermediate.ceil() as u32;

        if result == 0 && intermediate > 0.0 {
            error!("Result overflowed u32 or unexpected zero result for n={}, threshold_percentage={}", n, threshold_percentage);
            return 0;
        }

        info!("Calculated threshold: {} for n={}, threshold_percentage={}", result, n, threshold_percentage);

        trace!("Exiting calculate_threshold with result={}", result);

        result
    }
}

pub fn build_http_client(rpc_url: &str) -> HttpClient { HttpClientBuilder::default().build(rpc_url).expect("Failed to build client") }

/// Ping peers to check their reachability
fn ping_peers(new_set: &mut [ProverInfo], kafka: Option<Arc<KafkaProducer>>) -> Result<Vec<ProverInfo>, String> {
    trace!("Entering ping_peers with {} peers", new_set.len());

    if new_set.is_empty() {
        warn!("Empty peer set provided");
        return Err("No peers provided".to_string());
    }

    debug!("Processing peers: {:?}", new_set.iter().map(|p| &p.rpcaddr).collect::<Vec<_>>());

    let runtime = match Runtime::new() {
        Ok(runtime) => {
            debug!("Successfully created Tokio runtime");
            runtime
        }
        Err(e) => {
            error!("Failed to create Tokio runtime: {:?}", e);
            return Err(format!("Failed to create runtime: {:?}", e));
        }
    };

    let mut reachable_peers = Vec::new();

    runtime.block_on(async {
        for peer in new_set.iter_mut() {
            trace!("Processing peer at {}", peer.rpcaddr);

            let client = build_http_client(&peer.rpcaddr);

            let response_result = client.ping().await;
            match response_result {
                Ok(response) => {
                    info!("Peer at {} is reachable: {:?}", peer.rpcaddr, response);
                    reachable_peers.push(peer.clone());
                    if let Some(kafka) = kafka.clone() {
                        let status = PeerReachabilityTCPStatus {
                            success: true,
                            rpc_url: peer.rpcaddr.clone(),
                        };
                        kafka.send(messages::kafka::KafkaTopic::PeerReachabilityTCP, peer.peer_id.to_string(), status).await;
                    }
                }
                Err(e) => {
                    warn!("Unreachable peer at {}: {:?}", peer.rpcaddr, e);
                    if let Some(kafka) = kafka.clone() {
                        let status = PeerReachabilityTCPStatus {
                            success: false,
                            rpc_url: peer.rpcaddr.clone(),
                        };
                        kafka.send(messages::kafka::KafkaTopic::PeerReachabilityTCP, peer.peer_id.to_string(), status).await;
                    }
                }
            }
            debug!("Current reachable peers count: {}", reachable_peers.len());
        }
    });

    trace!("Finished processing peers. Found {} reachable peers", reachable_peers.len());

    // Check if any peers were reachable
    if !reachable_peers.is_empty() {
        info!("Found {} reachable peers", reachable_peers.len());
        Ok(reachable_peers)
    } else {
        error!("No reachable peers found among {} peers", new_set.len());
        Err("No reachable peers found".to_string())
    }
}

#[async_trait]
impl Actor for ElectionEngineActor {
    type Msg = ElectionEngineMessage;
    type State = ElectionEngineState;
    type Arguments = (NodeType, PeerId, PeerId, Multiaddr, Option<Arc<KafkaProducer>>);
    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> {
        let mut exclusion_list = TEMP_EXCLUSION_PEER_LIST.lock().await;
        *exclusion_list = get_state::<HashSet<PeerId>>(&args.1.to_string(), "temp_exclusion_peer_list").unwrap_or_default();

        if let Some(val) = get_state::<f64>(&args.1.to_string(), "election_threshold") {
            set_threshold(val);
        }

        if let Some(val) = get_state::<usize>(&args.1.to_string(), "election_min_nodes") {
            set_min_nodes(val);
        }

        if let Some(val) = get_state::<u32>(&args.1.to_string(), "election_epsilon") {
            set_epsilon(val);
        }

        Ok(ElectionEngineState {
            node_type: args.0,
            peer_id: args.1,
            relay_node: (args.2, args.3),
            current_state: ElectionState::default(),
            elected_provers: Vec::default(),

            tstar_provers: Vec::default(),
            current_threshold: 0,
            re_election_status: HashMap::default(),
            election_status: HashSet::default(),
            re_election_check_context: None,
            dkg_init_status: HashSet::default(),
            re_sharing_init_status: HashMap::default(),
            re_sharing_round1_status: HashMap::default(),
            dkg_round1_status: HashSet::default(),
            re_sharing_round2_status: HashMap::default(),
            dkg_round2_status: HashSet::default(),
            kafka: args.4,
        })
    }

    /// Handles different messages related to an election engine, triggering actions based on the current state.
    async fn handle(&self, _myself: ActorRef<Self::Msg>, message: Self::Msg, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match message {
            ElectionEngineMessage::TriggerReElection(prev_elected_provers) => {
                trace!("Entering TriggerReElection handler with {} previous elected provers", prev_elected_provers.len());

                // Clear previous election-related state
                info!("Clearing previous reelection sync status");
                state.election_status.clear();
                state.re_election_status.clear();
                state.re_sharing_init_status.clear();
                state.re_sharing_round1_status.clear();
                state.re_sharing_round2_status.clear();
                state.dkg_init_status.clear();
                state.dkg_round1_status.clear();
                state.dkg_round2_status.clear();
                info!("Previous reelection sync status cleared");

                let mut participating_provers: Vec<_> = fetch_provers_v2().await;
                debug!("Fetched {} participating provers", participating_provers.len());

                // Wait until enough provers are available
                loop {
                    if participating_provers.len() <= TOTAL_BOOTSTRAP_RELAY_NODES {
                        warn!("Insufficient participating provers: {} (required > {})", participating_provers.len(), TOTAL_BOOTSTRAP_RELAY_NODES);
                        info!("Sleeping for {} seconds before retrying", SLEEP_PING);
                        sleep(Duration::from_secs(SLEEP_PING)).await;
                        participating_provers = fetch_provers_v2().await;
                        debug!("Retried fetch, now have {} participating provers", participating_provers.len());
                    } else {
                        info!("Sufficient participating provers: {}", participating_provers.len());
                        break;
                    }
                }

                // Filter provers
                participating_provers = participating_provers[2..].to_vec();
                let exclusion_list = TEMP_EXCLUSION_PEER_LIST.lock().await;
                let participating_provers: Vec<ProverInfo> = participating_provers.into_iter().filter(|p| !exclusion_list.contains(&p.peer_id)).collect();
                info!("Filtered participating provers: {} remaining", participating_provers.len());
                if participating_provers.is_empty() {
                    error!("No valid participating provers after filtering");
                    return Err("No valid participating provers".to_string().into());
                }

                let indexed_provers_map: HashMap<ProverInfo, usize> = participating_provers.iter().cloned().map(|prover| (prover.clone(), prover.idx)).collect();
                debug!("Created indexed provers map with {} entries", indexed_provers_map.len());

                let min_nodes = get_min_nodes();
                // Ping peers to check reachability
                info!("Pinging sampled peers");
                match ping_peers(&mut participating_provers.clone(), state.kafka.clone()) {
                    Ok(reachable_peers) => {
                        info!("Found {} reachable peers", reachable_peers.len());
                        if reachable_peers.len() < min_nodes {
                            warn!("Insufficient reachable peers: {} (required >= {})", reachable_peers.len(), min_nodes);
                            return Ok(());
                        }
                        // Fetch DRAND data for randomness
                        info!("Fetching DRAND data for random number");
                        let mut rand_num = rand::thread_rng().gen::<[u8; 32]>();
                        let mut drand_response = DrandResponse::default();
                        match fetch_drand_data().await {
                            Ok(data) => {
                                debug!(
                                    "Received DRAND data: round={}, randomness={}, previous_signature={}, signature={}",
                                    data.round, data.randomness, data.previous_signature, data.signature
                                );

                                // Verify DRAND data
                                let prev_sig = match hex::decode(data.previous_signature.clone()) {
                                    Ok(sig) => sig,
                                    Err(e) => {
                                        error!("Failed to decode previous DRAND signature: {:?}", e);
                                        return Err(format!("Invalid DRAND previous signature: {:?}", e).into());
                                    }
                                };
                                let sig = match hex::decode(data.signature.clone()) {
                                    Ok(sig) => sig,
                                    Err(e) => {
                                        error!("Failed to decode DRAND signature: {:?}", e);
                                        return Err(format!("Invalid DRAND signature: {:?}", e).into());
                                    }
                                };
                                if verify_drand(data.round, &prev_sig, &sig) {
                                    info!("DRAND verification successful, using randomness: {}", data.randomness);
                                    match hex::decode(data.randomness.clone()) {
                                        Ok(randomness) => {
                                            if randomness.len() >= 32 {
                                                rand_num = randomness[0..32].try_into().map_err(|e| {
                                                    error!("Failed to convert DRAND randomness to [u8; 32]: {:?}", e);
                                                    format!("Invalid DRAND randomness length: {:?}", e)
                                                })?;
                                            } else {
                                                warn!("DRAND randomness too short ({} bytes), using fallback random number", randomness.len());
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Failed to decode DRAND randomness: {:?}, using fallback random number", e);
                                        }
                                    }
                                    drand_response = data;
                                } else {
                                    warn!("DRAND verification failed, using fallback random number: {}", hex::encode(rand_num));
                                }
                            }
                            Err(e) => {
                                warn!("Failed to fetch DRAND data: {:?}, using fallback random number: {}", e, hex::encode(rand_num));
                            }
                        }
                        debug!("Selected random number: {}", hex::encode(rand_num));
                        match weighted_sample_n_no_replace(reachable_peers.clone(), min_nodes, rand_num) {
                            Some(new_set) => {
                                info!("Successfully sampled {} provers", new_set.len());

                                // Process quorums
                                let prev_elected_peer_ids: Vec<_> = prev_elected_provers.iter().map(|prover| &prover.peer_id).cloned().collect();
                                debug!("Previous elected provers peer IDs: {:?}", prev_elected_peer_ids);

                                let mut t_star_quorum = Vec::new();
                                let new_quorum = new_set;
                                for reachable_peer in reachable_peers.iter().cloned() {
                                    if prev_elected_peer_ids.contains(&reachable_peer.peer_id) {
                                        t_star_quorum.push(reachable_peer.clone());
                                    }
                                }
                                debug!("T* quorum size: {}, New quorum size: {}", t_star_quorum.len(), new_quorum.len());

                                if t_star_quorum.len() >= state.current_threshold as usize {
                                    info!("Proceeding with resharing, T* quorum size: {}", t_star_quorum.len());

                                    let mut tstar_quorum_map = HashMap::new();
                                    for prover in t_star_quorum.iter() {
                                        tstar_quorum_map.insert(prover.peer_id, (prover.clone(), *indexed_provers_map.get(prover).unwrap()));
                                    }
                                    let mut new_quorum_map = HashMap::new();
                                    let new_quorum: Vec<_> = new_quorum.iter().take(min_nodes).cloned().collect();
                                    for prover in new_quorum.iter() {
                                        new_quorum_map.insert(prover.peer_id, (prover.clone(), *indexed_provers_map.get(prover).unwrap()));
                                    }

                                    info!("T* quorum:");
                                    for (peer_id, (_, idx)) in tstar_quorum_map.iter() {
                                        info!("PeerId: {} Idx: {}", peer_id.to_string(), idx);
                                    }
                                    info!("New quorum:");
                                    for (peer_id, (_, idx)) in new_quorum_map.iter() {
                                        info!("PeerId: {} Idx: {}", peer_id.to_string(), idx);
                                    }

                                    state.tstar_provers = t_star_quorum.clone();
                                    state.elected_provers = new_quorum.clone();

                                    // Clear shares and update state
                                    trace!("Casting message: ClearShares");
                                    cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::ClearShares, AppStateEngineError);
                                    trace!("Casting message: UpdateElectedProvers");
                                    cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdateElectedProvers(new_quorum.clone(), true), AppStateEngineError);
                                    trace!("Casting message: StoreQuorumMap");
                                    cast_message!(
                                        ActorType::AppStateEngine,
                                        AppStateChangeMessage::StoreQuorumMap(tstar_quorum_map.clone(), new_quorum_map.clone()),
                                        AppStateEngineError
                                    );

                                    // Calculate threshold
                                    let threshold = ElectionEngineState::calculate_threshold(new_quorum.len(), get_threshold());
                                    debug!("Calculated threshold: {}", threshold);
                                    // Cast message to update the threshold in the AppStateEngine
                                    cast_message!(
                                        ActorType::AppStateEngine,
                                        AppStateChangeMessage::UpdatedThreshold(ElectionInfo::new(threshold, get_epsilon(), state.elected_provers.len() as u32)),
                                        AppStateEngineError
                                    );
                                    // Forward re-elected provers
                                    trace!("Casting message: ForwardReElectedProvers");
                                    cast_message!(
                                        ActorType::AppStateEngine,
                                        AppStateChangeMessage::ForwardReElectedProvers(
                                            ElectionInfo::new(threshold, get_epsilon(), state.elected_provers.len() as u32),
                                            tstar_quorum_map,
                                            new_quorum_map,
                                            drand_response
                                        ),
                                        AppStateEngineError
                                    );

                                    // Monitor quorum formation
                                    trace!("Casting message: MonitorQuorumFormation");
                                    cast_message!(
                                        ActorType::AppStateEngine,
                                        AppStateChangeMessage::MonitorQuorumFormation(messages::utils::MonitorEvent::Resharing),
                                        AppStateEngineError
                                    );
                                } else {
                                    error!("Insufficient T* provers for resharing: {} (required >= {})", t_star_quorum.len(), state.current_threshold);
                                    return Ok(());
                                }
                            }
                            None => {
                                warn!("Failed to sample provers: total weight is 0");
                                info!("Sleeping for {} seconds before retrying", SLEEP_PING);
                                sleep(Duration::from_secs(SLEEP_PING)).await;
                                trace!("Casting message: MonitorQuorumFormation");
                                cast_message!(
                                    ActorType::AppStateEngine,
                                    AppStateChangeMessage::MonitorQuorumFormation(messages::utils::MonitorEvent::Resharing),
                                    AppStateEngineError
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to ping peers: {:?}", e);
                        return Err(format!("Peer ping failed: {:?}", e).into());
                    }
                }
                trace!("Exiting TriggerReElection handler");
                Ok(())
            }
            ElectionEngineMessage::FirstElection => handle_trigger_election(state).await,
            ElectionEngineMessage::CheckElectedStatus(peer_ids) => handle_check_elected_status(state, peer_ids).await,
            ElectionEngineMessage::CheckReElectedStatus(election_info, tstar_provers, new_provers, group_keys, drand_response) => {
                if let Ok(data) = messages::drand::fetch_drand_data().await {
                    if messages::drand::verify_drand(data.round, &hex::decode(&data.previous_signature).unwrap(), &hex::decode(&data.signature).unwrap()) {
                        if data.round - drand_response.round >= DRAND_ROUND_THRESHOLD_TOLERNACE {
                            error!("Drand Round is too old. Latest round {}. Gossiped Round {}", data.round, drand_response.round);
                            return Ok(());
                        }
                        info!("Drand Round Check Succeeded");
                        if !messages::drand::verify_drand(
                            drand_response.round,
                            &hex::decode(&drand_response.previous_signature).unwrap(),
                            &hex::decode(&drand_response.signature).unwrap(),
                        ) {
                            error!("Drand Round Verification Failed");
                            return Ok(());
                        }
                        info!("Drand Verification Succeeded");
                    } else {
                        error!("Drand Verification Failed");
                    }
                }
                handle_check_re_elected_status(state, election_info, tstar_provers, new_provers, group_keys).await
            }
            ElectionEngineMessage::RecordElectionNodeStatus(peer_id) => {
                info!("Storing Election status for peer: {:?}", peer_id);
                let elected_peer_ids: HashSet<PeerId> = state.elected_provers.iter().map(|prover| prover.peer_id.clone()).collect();
                if elected_peer_ids.contains(&peer_id) {
                    state.election_status.insert(peer_id);
                }

                //Check if all elected nodes have reported their status
                if state.election_status.len() == elected_peer_ids.len() {
                    info!("All elected nodes have reported their status");
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::StartDKGInit(), NETWORK_TOPIC.to_string()),
                        GossipEngineError
                    );
                    info!("Gossip to all nodes to starting with init of DKG");
                } else {
                    let missing_peer_ids_in_election_status: Vec<PeerId> = state
                        .elected_provers
                        .iter()
                        .filter_map(|prover| {
                            // Extract PeerId from ProverInfo and check if it's in the election_status
                            if !state.election_status.contains(&prover.peer_id) {
                                Some(prover.peer_id)
                            } else {
                                None
                            }
                        })
                        .collect();

                    info!("Waiting for {:?} Peers to report their election status", missing_peer_ids_in_election_status);
                }

                Ok(())
            }
            ElectionEngineMessage::RecordRelectionNodeStatus(peer_id) => {
                if state.re_election_status.contains_key(&peer_id) && state.re_election_status.get(&peer_id) == Some(&RelectionStatus::ReElected) {
                    warn!("Peer {:?} already recorded in re-election status, skipping re-election status update", peer_id);
                    return Ok(());
                }
                info!("Storing re-election status for peer: {:?}", peer_id);
                let list_resharing_init_peers: HashSet<PeerId> = state.elected_provers.iter().chain(state.tstar_provers.iter()).map(|prover| prover.peer_id).collect();
                if list_resharing_init_peers.contains(&peer_id) {
                    state.re_election_status.insert(peer_id, RelectionStatus::ReElected);
                }

                //Check if all relected nodes have reported their status
                if state.re_election_status.len() == list_resharing_init_peers.len() {
                    info!("All re-elected nodes have reported their status");
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::StartResharingInit(), NETWORK_TOPIC.to_string()),
                        GossipEngineError
                    );
                    info!("Gossip to all nodes to starting with init of resharing");
                } else {
                    let missing_peer_ids_in_re_election_status: Vec<PeerId> = state
                        .elected_provers
                        .iter()
                        .filter_map(|prover| {
                            // Extract PeerId from ProverInfo and check if it's in the re_election_status
                            if !state.re_election_status.contains_key(&prover.peer_id) {
                                Some(prover.peer_id)
                            } else {
                                None
                            }
                        })
                        .collect();

                    info!("Waiting for {:?} Peers to report their re-election status", missing_peer_ids_in_re_election_status);
                }

                Ok(())
            }
            ElectionEngineMessage::StartDKGInit => handle_trigger_init_dkg(state).await,
            ElectionEngineMessage::StartResharingInit => handle_trigger_init_resharing(state).await,
            ElectionEngineMessage::RecordDKGInitNodeStatus(peer_id) => {
                info!("Storing DKG Init status for peer: {:?}", peer_id);

                if state.elected_provers.iter().any(|prover| prover.peer_id == peer_id) {
                    state.dkg_init_status.insert(peer_id);
                }

                //Check if all elected nodes have reported their status
                if state.dkg_init_status.len() == state.elected_provers.len() {
                    info!("All elected nodes have reported their dkg init status");
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::StartDKGRound1(), NETWORK_TOPIC.to_string()),
                        GossipEngineError
                    );
                    info!("Gossip to all nodes to starting with start round 1 of dkg");
                } else {
                    let missing_peer_ids_in_dkg_init_status: Vec<PeerId> = state
                        .elected_provers
                        .iter()
                        .filter_map(|prover| {
                            // Extract PeerId from ProverInfo and check if it's in the dkg_init_status
                            if !state.dkg_init_status.contains(&prover.peer_id) {
                                Some(prover.peer_id)
                            } else {
                                None
                            }
                        })
                        .collect();

                    info!("Waiting for {:?} Peers to report their dkg status", missing_peer_ids_in_dkg_init_status);
                }

                Ok(())
            }
            ElectionEngineMessage::RecordResharingInitNodeStatus(peer_id) => {
                if state.re_sharing_init_status.contains_key(&peer_id) && state.re_sharing_init_status.get(&peer_id) == Some(&RelectionStatus::ResharingInit) {
                    warn!("Peer {:?} already recorded in resharing init status, skipping resharing init status update", peer_id);
                    return Ok(());
                }
                info!("Storing Resharing Init status for peer: {:?}", peer_id);

                // Create a unique list of provers based on peer_id
                let unique_provers: Vec<ProverInfo> = {
                    let mut unique_peer_ids = HashSet::new();
                    state
                        .tstar_provers
                        .iter()
                        .chain(state.elected_provers.iter())
                        .filter(|prover| unique_peer_ids.insert(prover.peer_id))
                        .cloned()
                        .collect()
                };

                // Check if peer_id exists in the unique list
                if unique_provers.iter().any(|prover| prover.peer_id == peer_id) {
                    state.re_sharing_init_status.insert(peer_id, RelectionStatus::ResharingInit);
                }

                // Check if all unique nodes have reported their status
                if state.re_sharing_init_status.len() == unique_provers.len() {
                    info!("All relected nodes have reported their resharing init status");
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::StartResharingRound1(), NETWORK_TOPIC.to_string()),
                        GossipEngineError
                    );
                    info!("Gossip to all nodes to starting with start round 1 of resharing");
                } else {
                    let missing_peer_ids_in_re_sharing_init_status: Vec<PeerId> = {
                        let mut unique_peer_ids = HashSet::new();
                        state
                            .tstar_provers
                            .iter()
                            .chain(state.elected_provers.iter())
                            .filter_map(|prover| {
                                // Ensure uniqueness while collecting missing peer IDs
                                if unique_peer_ids.insert(prover.peer_id) && !state.re_sharing_init_status.contains_key(&prover.peer_id) {
                                    Some(prover.peer_id)
                                } else {
                                    None
                                }
                            })
                            .collect()
                    };

                    info!("Waiting for {:?} Peers to report their re-sharing init  status", missing_peer_ids_in_re_sharing_init_status);
                }

                Ok(())
            }

            ElectionEngineMessage::RecordDKGRound1NodeStatus(peer_id) => {
                info!("Storing DKG Round 1 status for peer: {:?}", peer_id);

                if state.elected_provers.iter().any(|prover| prover.peer_id == peer_id) {
                    state.dkg_round1_status.insert(peer_id);
                }

                //Check if all elected nodes have reported their status
                if state.dkg_round1_status.len() == state.elected_provers.len() {
                    info!("All relected nodes have reported their dkg round 1 status");
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::StartDKGRound2(), NETWORK_TOPIC.to_string()),
                        GossipEngineError
                    );
                    info!("Gossip to all nodes to starting with dkg round 2 of dkg");
                } else {
                    let missing_peer_ids_in_dkg_round1_status: Vec<PeerId> = state
                        .elected_provers
                        .iter()
                        .filter_map(|prover| {
                            // Extract PeerId from ProverInfo and check if it's in the dkg_round1_status
                            if !state.dkg_round1_status.contains(&prover.peer_id) {
                                Some(prover.peer_id)
                            } else {
                                None
                            }
                        })
                        .collect();

                    info!("Waiting for {:?} Peers to report their dkg round 1  status", missing_peer_ids_in_dkg_round1_status);
                }

                Ok(())
            }
            ElectionEngineMessage::RecordResharingRound1NodeStatus(peer_id) => {
                if state.re_sharing_round1_status.contains_key(&peer_id) && state.re_sharing_round1_status.get(&peer_id) == Some(&RelectionStatus::ResharingRound1) {
                    warn!("Peer {:?} already recorded in resharing round1 status, skipping resharing round1 status update", peer_id);
                    return Ok(());
                }
                info!("Storing Resharing Round 1 status for peer: {:?}", peer_id);

                if state.elected_provers.iter().any(|prover| prover.peer_id == peer_id) {
                    state.re_sharing_round1_status.insert(peer_id, RelectionStatus::ResharingRound1);
                }

                //Check if all relected nodes have reported their status
                if state.re_sharing_round1_status.len() == state.elected_provers.len() {
                    info!("All relected nodes have reported their resharing round 1 status");
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::StartResharingRound2(), NETWORK_TOPIC.to_string()),
                        GossipEngineError
                    );
                    info!("Gossip to all nodes to starting with resharing round 2 of resharing");
                } else {
                    let missing_peer_ids_in_re_sharing_round1_status: Vec<PeerId> = state
                        .elected_provers
                        .iter()
                        .filter_map(|prover| {
                            // Extract PeerId from ProverInfo and check if it's in the resharing_round1_status
                            if !state.re_sharing_round1_status.contains_key(&prover.peer_id) {
                                Some(prover.peer_id)
                            } else {
                                None
                            }
                        })
                        .collect();

                    info!("Waiting for {:?} Peers to report their re-sharing round 1  status", missing_peer_ids_in_re_sharing_round1_status);
                }

                Ok(())
            }
            ElectionEngineMessage::RecordDKGRound2NodeStatus(peer_id) => {
                info!("Storing DKG Round 2 status for peer: {:?}", peer_id);

                if state.elected_provers.iter().any(|prover| prover.peer_id == peer_id) {
                    state.dkg_round2_status.insert(peer_id);
                }

                //Check if all elected nodes have reported their status
                if state.dkg_round2_status.len() == state.elected_provers.len() {
                    info!("All relected nodes have reported their dkg round 2 status");
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::StartDKGRound3(), NETWORK_TOPIC.to_string()),
                        GossipEngineError
                    );
                    info!("Gossip to all nodes to starting with dkg round 3 of dkg");
                } else {
                    let missing_peer_ids_in_dkg_round2_status: Vec<PeerId> = state
                        .elected_provers
                        .iter()
                        .filter_map(|prover| {
                            // Extract PeerId from ProverInfo and check if it's in the dkg_round2_status
                            if !state.dkg_round2_status.contains(&prover.peer_id) {
                                Some(prover.peer_id)
                            } else {
                                None
                            }
                        })
                        .collect();

                    info!("Waiting for {:?} Peers to report their dkg round 2  status", missing_peer_ids_in_dkg_round2_status);
                }

                Ok(())
            }
            ElectionEngineMessage::RecordResharingRound2NodeStatus(peer_id) => {
                if state.re_sharing_round2_status.contains_key(&peer_id) && state.re_sharing_round2_status.get(&peer_id) == Some(&RelectionStatus::ResharingRound2) {
                    warn!("Peer {:?} already recorded in resharing round2 status, skipping resharing round2 status update", peer_id);
                    return Ok(());
                }
                info!("Storing Resharing Round 2 status for peer: {:?}", peer_id);

                if state.elected_provers.iter().any(|prover| prover.peer_id == peer_id) {
                    state.re_sharing_round2_status.insert(peer_id, RelectionStatus::ResharingRound2);
                }

                //Check if all relected nodes have reported their status
                if state.re_sharing_round2_status.len() == state.elected_provers.len() {
                    info!("All relected nodes have reported their resharing round 2 status");
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::StartResharingRound3(), NETWORK_TOPIC.to_string()),
                        GossipEngineError
                    );
                    info!("Gossip to all nodes to starting with resharing round 3 of resharing");
                } else {
                    let missing_peer_ids_in_re_sharing_round2_status: Vec<PeerId> = state
                        .elected_provers
                        .iter()
                        .filter_map(|prover| {
                            // Extract PeerId from ProverInfo and check if it's in the resharing_round2_status
                            if !state.re_sharing_round2_status.contains_key(&prover.peer_id) {
                                Some(prover.peer_id)
                            } else {
                                None
                            }
                        })
                        .collect();

                    info!("Waiting for {:?} Peers to report their re-sharing round 2  status", missing_peer_ids_in_re_sharing_round2_status);
                }

                Ok(())
            }
            ElectionEngineMessage::AddToExclusionList(peer_id, sender) => {
                if state.peer_id == state.relay_node.0 {
                    info!("Adding peer to exclusion list: {:?}", peer_id);
                    let mut exclusion_list = TEMP_EXCLUSION_PEER_LIST.lock().await;
                    exclusion_list.insert(peer_id);

                    // save the updated exclusion list to the database
                    save_state(&state.peer_id.to_string(), "temp_exclusion_peer_list", exclusion_list.clone());

                    send_response::<NodeResponse>(sender, NodeResponse::AddedToExclusionList { peer_id }, "Error sending response");
                    Ok(())
                } else {
                    // If the node is not a relay node, send an error response
                    error!("Not a relay node");
                    send_response::<NodeResponse>(
                        sender,
                        NodeResponse::Error {
                            request_id: String::new(),
                            message: "Not a relay node".to_string(),
                        },
                        "Error sending response",
                    );
                    return Err("Not a relay node".into());
                }
            }
            ElectionEngineMessage::RemoveFromExclusionList(peer_id, sender) => {
                if state.peer_id == state.relay_node.0 {
                    info!("Removing peer from exclusion list: {:?}", peer_id);
                    let mut exclusion_list = TEMP_EXCLUSION_PEER_LIST.lock().await;
                    exclusion_list.remove(&peer_id);

                    // save the updated exclusion list to the database
                    save_state(&state.peer_id.to_string(), "temp_exclusion_peer_list", exclusion_list.clone());

                    send_response::<NodeResponse>(sender, NodeResponse::RemovedFromExclusionList { peer_id }, "Error sending response");
                    Ok(())
                } else {
                    // If the node is not a relay node, send an error response
                    error!("Not a relay node");
                    send_response::<NodeResponse>(
                        sender,
                        NodeResponse::Error {
                            request_id: String::new(),
                            message: "Not a relay node".to_string(),
                        },
                        "Error sending response",
                    );
                    return Err("Not a relay node".into());
                }
            }
            ElectionEngineMessage::GetExclusionList(sender) => {
                if state.peer_id == state.relay_node.0 {
                    info!("Fetching exclusion list");
                    let exclusion_list = TEMP_EXCLUSION_PEER_LIST.lock().await;
                    send_response::<NodeResponse>(
                        sender,
                        NodeResponse::CurrentExcludedPeers {
                            excluded_peers: exclusion_list.clone(),
                        },
                        "Error sending response",
                    );
                    Ok(())
                } else {
                    // If the node is not a relay node, send an error response
                    error!("Not a relay node");
                    send_response::<NodeResponse>(
                        sender,
                        NodeResponse::Error {
                            request_id: String::new(),
                            message: "Not a relay node".to_string(),
                        },
                        "Error sending response",
                    );
                    return Err("Not a relay node".into());
                }
            }
            ElectionEngineMessage::UpdateElectionParams(params, sender) => {
                if state.peer_id == state.relay_node.0 {
                    info!("Updating election parameters to: {:?}", params);

                    if let Some(threshold_val) = params.threshold {
                        set_threshold(threshold_val);
                    }
                    if let Some(epsilon_val) = params.epsilon {
                        set_epsilon(epsilon_val);
                    }
                    if let Some(min_nodes_val) = params.min_nodes {
                        set_min_nodes(min_nodes_val);
                    }

                    let threshold = get_threshold();
                    let epsilon = get_epsilon();
                    let min_nodes = get_min_nodes();

                    send_response::<NodeResponse>(sender, NodeResponse::ElectionParams { threshold, epsilon, min_nodes }, "Error sending response");

                    // save updated params to the database
                    save_state(&state.peer_id.to_string(), "election_threshold", threshold);
                    save_state(&state.peer_id.to_string(), "election_epsilon", epsilon);
                    save_state(&state.peer_id.to_string(), "election_min_nodes", min_nodes);

                    Ok(())
                } else {
                    // If the node is not a relay node, send an error response
                    error!("Not a relay node");
                    send_response::<NodeResponse>(
                        sender,
                        NodeResponse::Error {
                            request_id: String::new(),
                            message: "Not a relay node".to_string(),
                        },
                        "Error sending response",
                    );
                    Err("Not a relay node".into())
                }
            }
            ElectionEngineMessage::GetElectionParams(sender) => {
                if state.peer_id == state.relay_node.0 {
                    info!("Fetching current election parameters");
                    let current_threshold = get_threshold();
                    let current_epsilon = get_epsilon();
                    let current_min_nodes = get_min_nodes();
                    send_response::<NodeResponse>(
                        sender,
                        NodeResponse::ElectionParams {
                            min_nodes: current_min_nodes,
                            epsilon: current_epsilon,
                            threshold: current_threshold,
                        },
                        "Error sending response",
                    );
                    Ok(())
                } else {
                    // If the node is not a relay node, send an error response
                    error!("Not a relay node");
                    send_response::<NodeResponse>(
                        sender,
                        NodeResponse::Error {
                            request_id: String::new(),
                            message: "Not a relay node".to_string(),
                        },
                        "Error sending response",
                    );
                    return Err("Not a relay node".into());
                }
            }
            _ => Ok(()),
        }
    }
}

/// Handles triggering an election and updating the state based on the election results.
async fn handle_trigger_election(state: &mut ElectionEngineState) -> Result<(), ActorProcessingErr> {
    state.conduct_election().await?;
    if state.current_state == ElectionState::QuorumElected {
        state.dkg_init_status.clear();
        state.dkg_round1_status.clear();
        state.dkg_round2_status.clear();
        loop {
            match ping_peers(&mut state.elected_provers, state.kafka.clone()) {
                // Attempt to ping peers and get reachable peers
                Ok(reachable_peers) => {
                    let min_nodes = get_min_nodes();
                    if reachable_peers.len() < min_nodes {
                        info!("Not enough reachable peers ({}/{}) for DKG, retrying in 20s", reachable_peers.len(), min_nodes);
                        info!("Waiting for 20 secs ..Before next pings");
                        sleep(Duration::from_secs(20)).await;
                    } else {
                        // Update elected provers with the reachable peers
                        state.elected_provers = reachable_peers.iter().take(min_nodes).cloned().collect();
                        let indexed_provers_map: HashMap<ProverInfo, usize> = reachable_peers.iter().cloned().map(|prover| (prover.clone(), prover.idx)).collect();
                        // Create a new quorum map( Nodes with their indexes)
                        let mut new_quorum_map = HashMap::new();
                        for prover in state.elected_provers.iter() {
                            new_quorum_map.insert(prover.peer_id, (prover.clone(), *indexed_provers_map.get(prover).unwrap()));
                        }
                        info!("Updating quorum map with reachable peers");
                        cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdateCachedProversMap(new_quorum_map), AppStateEngineError);
                        break;
                    }
                }
                Err(err) => {
                    error!("Error pinging peers: {:?}", err);
                    sleep(Duration::from_secs(20)).await;
                }
            }
        }
        // Cast message to update elected provers in the AppStateEngine
        debug!("Elected Provers :{:#?}", state.elected_provers);
        for prover in state.elected_provers.iter() {
            info!("Elected Prover Idx {} PeerID : {:?}", prover.idx, prover.peer_id);
        }
        cast_message!(
            ActorType::AppStateEngine,
            AppStateChangeMessage::UpdateElectedProvers(state.elected_provers.clone(), false),
            AppStateEngineError
        );
        // Calculate the threshold
        let threshold = ElectionEngineState::calculate_threshold(state.elected_provers.len(), get_threshold());
        state.current_threshold = threshold;

        // Cast message to update the threshold in the AppStateEngine
        cast_message!(
            ActorType::AppStateEngine,
            AppStateChangeMessage::UpdatedThreshold(ElectionInfo::new(threshold, get_epsilon(), state.elected_provers.len() as u32)),
            AppStateEngineError
        );

        // Cast message to gossip the elected provers to the GossipEngine
        cast_message!(
            ActorType::GossipEngine,
            GossipEngineMessage::Gossip(
                Message::ElectedProvers(ElectionInfo::new(threshold, get_epsilon(), state.elected_provers.len() as u32), state.elected_provers.clone(),),
                NETWORK_TOPIC.to_string(),
            ),
            GossipEngineError
        );
        cast_message!(
            ActorType::AppStateEngine,
            AppStateChangeMessage::MonitorQuorumFormation(messages::utils::MonitorEvent::DKG),
            AppStateEngineError
        );
    }
    Ok(())
}

/// Handles checking the elected status and initializing the DKG process if the node is elected.
async fn handle_check_elected_status(state: &mut ElectionEngineState, peer_ids: Vec<ProverInfo>) -> Result<(), ActorProcessingErr> {
    // Find the index of the current node in the list of peer IDs
    if let Some(prover_info) = peer_ids.iter().find(|prover_info| prover_info.peer_id == state.peer_id) {
        let index_of_peer = prover_info.idx;
        debug!("Elected for the Quorum, Index: {}", index_of_peer);
        state.elected_provers = peer_ids.clone();
        //All the provers will have their indexes > 2, since 1,2 are reserved for bootstrap node and relay node for test net
        //For main net it will vary
        let provers: HashMap<PeerId, (ProverInfo, usize)> = peer_ids.into_iter().map(|prover_info| (prover_info.peer_id, (prover_info.clone(), prover_info.idx))).collect();

        // Cast a message to the AppStateEngine to update the node index
        cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdateNodeIdx(index_of_peer as u32), AppStateEngineError);
        //Update for prover
        cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdateCachedProversMap(provers), AppStateEngineError);
        info!("Reporting election status for Quorum");
        cast_message!(
            ActorType::GossipEngine,
            GossipEngineMessage::Gossip(Message::ReportElectionNodeStatus(state.peer_id), NETWORK_TOPIC.to_string(),),
            GossipEngineError
        );
    } else {
        info!("Not Elected for the Quorum Occured during checking of election status");
    }
    Ok(())
}

/// Handles the status checking for re-election and manages the DKG resharing process.
async fn handle_check_re_elected_status(
    state: &mut ElectionEngineState,
    election_info: ElectionInfo,
    tstar_provers: HashMap<PeerId, (ProverInfo, usize)>,
    new_provers: HashMap<PeerId, (ProverInfo, usize)>,
    group_keys: HashMap<String, Vec<u8>>,
) -> Result<(), ActorProcessingErr> {
    state.re_election_check_context = Some(ReElectionCheckContext {
        tstar_provers: tstar_provers.clone(),
        new_provers: new_provers.clone(),
        group_keys,
        election_info: election_info.clone(),
    });
    // Flag to track if the current peer has participated in resharing
    let mut has_participated = false;

    // Clear any existing shares in AppStateEngine and DkgEngine
    cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::ClearShares, AppStateEngineError);
    cast_message!(ActorType::DkgEngine, DkgEngineMessage::ClearShares, DKGEngineError);

    // Check if the current peer is part of the T* provers
    if tstar_provers.contains_key(&state.peer_id) {
        info!("Elected for the Quorum for Resharing Round 1");
        info!("Proceeding for Reporting Node Re-Election Status");

        //Report the re-election status to the Relay Engine
        cast_message!(
            ActorType::GossipEngine,
            GossipEngineMessage::Gossip(Message::ReportReElectionNodeStatus(state.peer_id), NETWORK_TOPIC.to_string(),),
            GossipEngineError
        );
        has_participated = true;
    }

    // Check if the current peer is part of the new provers
    if new_provers.contains_key(&state.peer_id) && !has_participated {
        let idx = new_provers.get(&state.peer_id).unwrap().1;
        info!("Elected for the Quorum, Index: {} for resharing round 1", idx);
        info!("Proceeding for Reporting Node Re-Election Status");

        //Report the re-election status to the Relay Engine
        cast_message!(
            ActorType::GossipEngine,
            GossipEngineMessage::Gossip(Message::ReportReElectionNodeStatus(state.peer_id), NETWORK_TOPIC.to_string(),),
            GossipEngineError
        );
        // Update the node index in the AppStateEngine
        cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdateNodeIdx(idx as u32), AppStateEngineError);
    }
    if !tstar_provers.contains_key(&state.peer_id) && !new_provers.contains_key(&state.peer_id) {
        info!("Not Elected for Quorum after Re-Election");
    }
    //Update for prover
    cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdateCachedProversMap(new_provers), AppStateEngineError);
    Ok(())
}

async fn handle_trigger_init_dkg(state: &mut ElectionEngineState) -> Result<(), ActorProcessingErr> {
    let peer_ids = state.elected_provers.clone();
    // Find the index of the current node in the list of peer IDs
    if let Some(prover_info) = peer_ids.iter().find(|prover_info| prover_info.peer_id == state.peer_id) {
        let threshold: u32 = ElectionEngineState::calculate_threshold(peer_ids.len(), get_threshold());
        let index_of_peer = prover_info.idx;
        info!("Proceeding for DKG with size: {}, threshold: {}", peer_ids.len(), threshold);
        //All the provers will have their indexes > 2, since 1,2 are reserved for bootstrap node and relay node for test net
        //For main net it will vary
        let provers: HashMap<PeerId, (ProverInfo, usize)> = peer_ids.into_iter().map(|prover_info| (prover_info.peer_id, (prover_info.clone(), prover_info.idx))).collect();
        // Cast a message to the DkgEngine to initialize the DKG process
        cast_message!(ActorType::DkgEngine, DkgEngineMessage::Init(index_of_peer, state.peer_id, provers.clone(), threshold), DKGEngineError);
    } else {
        info!("Not Elected for the Quorum");
    }
    Ok(())
}

async fn handle_trigger_init_resharing(state: &mut ElectionEngineState) -> Result<(), ActorProcessingErr> {
    match &state.re_election_check_context {
        Some(re_election_check_context) => {
            let mut has_participated = false;
            if re_election_check_context.tstar_provers.contains_key(&state.peer_id) {
                let idx = re_election_check_context.tstar_provers.get(&state.peer_id).unwrap().1;
                info!("Elected for the Quorum for Resharing Round 1");
                info!(
                    "Proceeding for Resharing Init with size: {}, threshold: {}",
                    re_election_check_context.tstar_provers.len(),
                    re_election_check_context.election_info.threshold
                );

                // Initialize resharing for T* provers
                cast_message!(
                    ActorType::DkgEngine,
                    DkgEngineMessage::ResharingInit(
                        idx,
                        re_election_check_context.tstar_provers.clone(),
                        re_election_check_context.new_provers.clone(),
                        re_election_check_context.election_info.clone(),
                        state.peer_id,
                        re_election_check_context.group_keys.clone()
                    ),
                    DKGEngineError
                );
                has_participated = true;
            }
            if re_election_check_context.new_provers.contains_key(&state.peer_id) && !has_participated {
                let idx = re_election_check_context.new_provers.get(&state.peer_id).unwrap().1;
                info!("Elected for the Quorum, Index: {} for resharing round 1", idx);
                info!(
                    "Proceeding for Resharing for new Quorum with size: {}, threshold: {}",
                    re_election_check_context.new_provers.len(),
                    re_election_check_context.election_info.threshold
                );

                // Initialize resharing for new provers
                cast_message!(
                    ActorType::DkgEngine,
                    DkgEngineMessage::ResharingInit(
                        idx,
                        re_election_check_context.tstar_provers.clone(),
                        re_election_check_context.new_provers.clone(),
                        re_election_check_context.election_info.clone(),
                        state.peer_id,
                        re_election_check_context.group_keys.clone()
                    ),
                    DKGEngineError
                );

                // Update the node index in the AppStateEngine
                cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdateNodeIdx(idx as u32), AppStateEngineError);
            }
            if !re_election_check_context.tstar_provers.contains_key(&state.peer_id) && !re_election_check_context.new_provers.contains_key(&state.peer_id) {
                info!("Not Elected for Quorum after Re-Election");
            }
        }
        None => {
            info!("No re-election check context available.");
        }
    }
    Ok(())
}

pub struct ElectionEngineSupervisor {
    panic_tx: tokio::sync::mpsc::Sender<ActorCell>,
}
impl ElectionEngineSupervisor {
    pub fn new(panic_tx: tokio::sync::mpsc::Sender<ActorCell>) -> Self { Self { panic_tx } }
}
#[derive(Debug, Error, Default)]
pub enum ElectionEngineSupervisorError {
    #[default]
    #[error("failed to acquire ElectionEngineSupervisorError from registry")]
    RactorRegistryError,
}
#[async_trait]
impl Actor for ElectionEngineSupervisor {
    type Msg = ElectionEngineMessage;
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
#[cfg(test)]
mod tests {
    use messages::drand::fetch_drand_data;
    use network::utils::fetch_provers_v2;
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
    use std::collections::HashMap;

    use crate::election_actor::ElectionEngineState;

    #[tokio::test]
    async fn fetch_provers_test() {
        let mut provers = fetch_provers_v2().await;
        provers = provers[2..].to_vec();
        for prover in provers.iter() {
            println!("provers :{:#?}  : {:?}", prover.peer_id, prover.voting_power);
        }
    }

    trait ElectionStrategy: Send + Sync {
        fn calculate_adjusted_weights(&self, weights: &[f64], last_elected: &HashMap<usize, usize>) -> Vec<f64>;
    }

    trait AsyncElectionStrategy: Send + Sync {
        async fn select_operators(&self, weights: &[f64], num_selections: usize) -> Vec<usize>;
    }

    #[derive(Clone)]
    struct WeightedPenaltyStrategy {
        penalty_factors: HashMap<usize, f64>,
    }

    impl WeightedPenaltyStrategy {
        fn new(penalty_factors: HashMap<usize, f64>) -> Self { Self { penalty_factors } }
    }

    impl ElectionStrategy for WeightedPenaltyStrategy {
        fn calculate_adjusted_weights(&self, weights: &[f64], last_elected: &HashMap<usize, usize>) -> Vec<f64> {
            weights
                .iter()
                .enumerate()
                .map(|(i, &w)| {
                    let log_weight = (w + 1.0).ln();
                    let penalty = last_elected.get(&i).map(|&index| self.penalty_factors.get(&index).unwrap_or(&1.0)).unwrap_or(&1.0);
                    log_weight * penalty
                })
                .collect()
        }
    }

    impl AsyncElectionStrategy for WeightedPenaltyStrategy {
        async fn select_operators(&self, adjusted_weights: &[f64], num_selections: usize) -> Vec<usize> {
            let mut rng = rand::thread_rng();

            // Collect all machines with their adjusted weights
            let candidates: Vec<(usize, f64)> = adjusted_weights.iter().enumerate().map(|(i, &w)| (i, w)).collect();

            if candidates.is_empty() || num_selections == 0 {
                return Vec::new();
            }

            // let seed = match fetch_drand_data().await {
            //     Ok(data) => {
            //         let prev_sig = hex::decode(data.previous_signature).unwrap_or_default();
            //         let sig = hex::decode(data.signature).unwrap_or_default();

            //         if messages::drand::verify_drand(data.round, &prev_sig, &sig) {
            //             println!("Choosing drand rand_number: {}", data.randomness);
            //             hex::decode(data.randomness)
            //                 .ok()
            //                 .and_then(|decoded| decoded.get(0..32).map(|slice| slice.try_into().unwrap()))
            //                 .unwrap_or_else(|| rng.gen::<[u8; 32]>())
            //         } else {
            //             rng.gen::<[u8; 32]>()
            //         }
            //         rng.gen::<[u8; 32]>()
            //     }
            //     Err(_) => {
            //         rng.gen::<[u8; 32]>()
            //     }
            // };

            let mut rng = StdRng::from_seed(rng.gen::<[u8; 32]>());

            // Select all machines using weighted random sampling
            let selected: Vec<usize> = candidates
                .choose_multiple_weighted(&mut rng, num_selections.min(candidates.len()), |(_, w)| *w)
                .unwrap()
                .map(|(i, _)| *i)
                .collect();

            println!("Adjusted Weights: {:?}", adjusted_weights);
            println!("Candidates : {:?}", candidates);
            println!("Selected: {:?}", selected);

            selected
        }
    }

    struct SimulationBuilder {
        num_machines: usize,
        weights: Vec<f64>,
        penalty_factors: HashMap<usize, f64>,
        num_iterations: usize,
        num_selections: usize,
    }

    impl SimulationBuilder {
        fn new() -> Self {
            Self {
                num_machines: 10,
                weights: vec![],
                penalty_factors: HashMap::new(),
                num_iterations: 100,
                num_selections: 4,
            }
        }

        fn with_weights(mut self, weights: Vec<f64>) -> Self {
            self.weights = weights;
            self
        }

        fn with_penalties(mut self, penalties: HashMap<usize, f64>) -> Self {
            self.penalty_factors = penalties;
            self
        }

        fn with_iterations(mut self, num: usize) -> Self {
            self.num_iterations = num;
            self
        }

        fn with_selections(mut self, num: usize) -> Self {
            self.num_selections = num;
            self
        }

        fn build(self) -> Simulation {
            let machines = (1..=self.num_machines).map(|i| format!("Node_{i}")).collect();

            let strategy = WeightedPenaltyStrategy::new(self.penalty_factors);

            Simulation {
                machines,
                weights: self.weights,
                strategy: strategy.clone(),
                async_strategy: strategy,
                num_iterations: self.num_iterations,
                num_selections: self.num_selections,
            }
        }
    }

    struct Simulation {
        machines: Vec<String>,
        weights: Vec<f64>,
        strategy: WeightedPenaltyStrategy,
        async_strategy: WeightedPenaltyStrategy,
        num_iterations: usize,
        num_selections: usize,
    }

    impl Simulation {
        async fn run(&self) {
            println!("=== Starting Election Simulation ===");
            println!("Total machines: {}", self.machines.len());
            println!("Selections per iteration: {}", self.num_selections);
            println!("Total iterations: {}", self.num_iterations);

            let mut last_elected = HashMap::new();

            for iteration in 0..self.num_iterations {
                println!("\n=== Iteration {} ===", iteration + 1);

                // Calculate adjusted weights
                let adjusted_weights = self.strategy.calculate_adjusted_weights(&self.weights, &last_elected);

                let selected_indices = self.async_strategy.select_operators(&adjusted_weights, self.num_selections).await;

                println!("Elected nodes:");
                for (position, &idx) in selected_indices.iter().enumerate() {
                    println!("  Position {}: {} (weight: {:.2})", position + 1, self.machines[idx], self.weights[idx]);
                }

                last_elected.clear();
                for (pos, &idx) in selected_indices.iter().enumerate() {
                    last_elected.insert(idx, pos + 1);
                }
            }

            println!("\n=== Simulation Complete ===");
        }
    }

    #[tokio::test]
    async fn test_simulation() {
        let simulation = SimulationBuilder::new()
            .with_weights(vec![
                1204245457452658813116.0,
                1339126636083952561113.0,
                15661399815724904088.0,
                70265037818952298496022.0,
                7725052903059354338338.0,
                54736119501161017716.0,
                66622244391713189002403.0,
                8829830593840228815413.0,
                14559447802281987709577.0,
                13766110648979752253232.0,
            ])
            .with_penalties([(1, 1.0 / 1000.0), (2, 1.0 / 500.0), (3, 1.0 / 100.0), (4, 1.0 / 50.0)].into())
            .with_iterations(10)
            .with_selections(4)
            .build();

        simulation.run().await;
    }
    #[tokio::test]
    async fn check_threshold_calculation() {
        let total_nodes = 8;
        let threshold_ratio: f64 = 60.0;
        let calculated_threshold = ElectionEngineState::calculate_threshold(total_nodes, threshold_ratio);
        println!("Total Nodes: {}, Threshold Ratio: {}, Calculated Threshold: {}", total_nodes, threshold_ratio, calculated_threshold);
        assert_eq!(calculated_threshold, 5);
    }
}
