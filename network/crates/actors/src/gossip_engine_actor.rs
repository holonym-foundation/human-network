///! The `GossipEngineListener` struct and related functionality for a gossip-based
///! network engine within a distributed system. It integrates various components such as network management,
///! message processing, and actor-based concurrency. The main components include:
///!
///! - `GossipEngineListener`: Manages network communication and message handling in a gossip protocol.
///! - `GossipEngineActor`: Defines the actor responsible for processing gossip engine messages and managing state.
///! - `GossipEngineSupervisor`: Supervises the `GossipEngineActor` to handle failure and restart scenarios.
///!
///! Key functionalities include:
///! - Handling network events such as message receipt, peer connection, and topic subscriptions.
///! - Processing incoming and outgoing messages, including serialization and deserialization.
///! - Managing cached data for delayed message processing and forwarding requests.
///! - Implementing actor-based communication for efficient and scalable message handling.
// Standard library imports
use std::{collections::HashMap, sync::Arc, time::Duration};
// Third-party library imports
use async_trait::async_trait;
use jsonrpsee::types::response;
use libp2p::{
    futures::StreamExt,
    gossipsub::{Event, IdentTopic},
    identify,
    identity::Keypair,
    kad::{QueryResult, RoutingUpdate},
    request_response::{self, Event as RequestResonseEvent},
    swarm::{Swarm, SwarmEvent},
    Multiaddr, PeerId,
};
use lru_time_cache::LruCache;
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, SupervisionEvent};
use thiserror::*;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time,
};
use tokio::{sync::oneshot, time::timeout};
use tracing::{debug, error, info, warn};
// Project-specific imports
use messages::{
    actor_type::ActorType,
    kafka::{KafkaProducer, PeerReachabilityQuicStatus},
    message::{AppStateChangeMessage, DkgEngineMessage, ElectionEngineMessage, GossipEngineMessage, Message},
    types::{NodeResponse, PeerScore, Response},
    NETWORK_TOPIC,
};
use network::{
    behaviour::{MyBehaviour, MyBehaviourEvent},
    utils::{is_dialable_address, send_response, NodeType},
};
// Local crate imports
use crate::{
    app_state_actor::{get_state, AppStateEngineError},
    cast_message,
    dkg_engine_actor::DKGEngineError,
    election_actor::ElectionEngineError,
    group_changed, ProcessMessageError, PEER_DISCOVERY_WAIT_TIME,
};
use libp2p::ping;

#[derive(Debug, Clone, Error)]
pub enum GossipEngineError {
    #[error("Error occurred in gossip engine: {0}")]
    Custom(String),
}

impl Default for GossipEngineError {
    fn default() -> Self { GossipEngineError::Custom("GossipEngine unable to acquire actor".to_string()) }
}
/// The `GossipEngineListener` struct in Rust represents a listener for a gossip engine with specific
/// fields and data structures.
///
/// Properties:
///
/// * `swarm`: The `swarm` property is of type `Swarm<MyBehaviour>` and is used to manage the collection
///   of peers and handle communication between them in a gossip protocol implementation.
/// * `sender`: The `sender` property in the `GossipEngineListener` struct is of type
///   `UnboundedSender<(Message, String)>`. It is used to send messages along with a string identifier to
///   the receiver.
/// * `receiver`: The `receiver` property in the `GossipEngineListener` struct is of type
///   `UnboundedReceiver<(Message, String)>`. It is used to receive messages along with their associated
///   topics from other peers in the network.
/// * `topics_subscribed`: The `topics_subscribed` property is a HashMap that stores the topics
///   subscribed by each peer in the network. The keys are `PeerId` representing the peer, and the values
///   are HashSet of strings representing the topics subscribed by that peer.
/// * `bootstrap_address`: The `bootstrap_address` property in the `GossipEngineListener` struct is a
///   HashSet that stores the addresses of peers that can be used as initial contact points for
///   bootstrapping the gossip protocol. These addresses are typically used to join the network and
///   discover other peers to communicate with.
/// * `node_type`: The `node_type` property in the `GossipEngineListener` struct represents the type of
///   node in the gossip network. It could be a specific type of node such as a bootstrap node, relay
///   node, prover node, or any other categorization based on the behavior or role within the network.
/// * `polled`: The `polled` property in the `GossipEngineListener` struct is a boolean value that
/// indicates whether the listener has been polled or not. It is used to track whether the listener has
/// been checked for any new messages or events since the last time it was polled.
/// * `cached_data`: The `cached_data` property is a HashMap that stores messages sent by peers. The
/// keys are `PeerId` values representing the peer who sent the messages, and the values are vectors of
/// `Message` structs containing the actual message data. This allows the `GossipEngineListener` to
/// cache messages
pub struct GossipEngineListener {
    pub peer_id: PeerId,
    pub swarm: Swarm<MyBehaviour>,
    _sender: UnboundedSender<MessageWithMultiaddr>,
    receiver: UnboundedReceiver<MessageWithMultiaddr>,
    node_type: NodeType,
    relay_peer_id: PeerId,
    polled: bool,
    cached_data: LruCache<PeerId, Vec<Message>>,
    bootstrap_addresses: Vec<(PeerId, Multiaddr)>,
    ping_sender: LruCache<PeerId, tokio::sync::mpsc::Sender<Response>>,
    kafka: Option<Arc<KafkaProducer>>,
    successful_kafka_ping_cache: LruCache<PeerId, ()>,
}

#[derive(Clone, Debug, Default)]
pub struct GossipEngineActor;
impl GossipEngineActor {
    pub fn new() -> Self { Self }
}
type MessageWithMultiaddr = (Message, String, Option<Vec<(PeerId, Multiaddr)>>);
impl GossipEngineListener {
    pub async fn new(
        keypair: Keypair,
        bootstrap_addresses: Option<Vec<(PeerId, Multiaddr)>>,
        multi_addr: Multiaddr,
        sender: UnboundedSender<MessageWithMultiaddr>,
        receiver: UnboundedReceiver<MessageWithMultiaddr>,
        node_type: NodeType,
        relay_peer_id: PeerId,
        kafka: Option<Arc<KafkaProducer>>,
    ) -> Result<Self, GossipEngineError> {
        let peer_id = keypair.public().to_peer_id();
        let bootstrap_addresses_cloned = bootstrap_addresses.clone();
        let swarm = network::network::init_swarm(Some(keypair), bootstrap_addresses.clone(), multi_addr)
            .await
            .map_err(|e| GossipEngineError::Custom(e.to_string()))?;
        let mut bootstrap_addresses = Vec::new();
        if let Some(b_addrs) = bootstrap_addresses_cloned {
            bootstrap_addresses = b_addrs;
        }
        let pubkey_shares: HashMap<PeerId, HashMap<String, Vec<u8>>> = get_state(&peer_id.to_string(), "pubkey_shares").unwrap_or_default();
        let polled = if pubkey_shares.is_empty() {
            info!("No shares found in the state");
            false
        } else {
            info!("Shares found in the state: {:#?}", pubkey_shares);
            true
        };

        Ok(Self {
            peer_id,
            swarm,
            _sender: sender,
            receiver,
            node_type,
            relay_peer_id,
            polled,
            cached_data: LruCache::<PeerId, Vec<Message>>::with_expiry_duration(Duration::from_secs(600)),
            bootstrap_addresses,
            ping_sender: LruCache::<PeerId, tokio::sync::mpsc::Sender<Response>>::with_expiry_duration(Duration::from_secs(60)),
            kafka,
            successful_kafka_ping_cache: LruCache::<PeerId, ()>::with_expiry_duration(Duration::from_secs(12 * 60 * 60)), // 12 hours
        })
    }
    /// function that represents the main loop for handling events in the gossip engine.
    pub async fn start(&mut self) {
        let mut discover_tick = time::interval(Duration::from_secs(PEER_DISCOVERY_WAIT_TIME));
        loop {
            select! {
                event = self.swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(Event::Message {
                        propagation_source: peer_id,
                        message_id: _,
                        message,
                    })) => {
                        let msg = Message::deserialize_from_bytes(&message.data);
                        let source_peer_id = message.source;
                        match msg {
                            Ok(msg) => {
                                info!("Received Gossiped Message {:?} PeerId :{:?} from source: {:?}", msg.get_type(), peer_id, source_peer_id);
                                if let Err(e) = self.process_message(msg,peer_id) {
                                    error!(
                                        "Error processing message: {:?} from peer: {:?}, propagation source: {:?} Error: {:?}",
                                        message.data,
                                        source_peer_id,
                                        peer_id,
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Received Invalid Message '{:?}' from peer: {:?}, source: {:?}, Size: {}, error: {}",
                                    String::from_utf8_lossy(&message.data),
                                    peer_id,
                                    source_peer_id,
                                    message.data.len(),
                                    e
                                );
                            }
                        }
                    }
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Local node is listening on {}", address);
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(Event::Subscribed { topic, peer_id })) => {
                        info!("Peer_ID: {:?} Topic Subscribed: {:?}", peer_id, topic);
                        if self.node_type == NodeType::Relay
                        && !self.polled
                    {
                            info!("Triggering First Election");
                            cast_message!(
                                   ActorType::ElectionEngine,
                                   ElectionEngineMessage::FirstElection,ElectionEngineError);
                                self.polled = true;
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(Event::Unsubscribed { topic, peer_id })) => {
                        info!("Peer_ID: {:?} Topic Unsubscribed: {:?}", peer_id, topic);
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::RequestResponseBehaviour(
                        RequestResonseEvent::Message { peer,message, .. },
                    )) => {
                        match message {
                            request_response::Message::Request { request, channel, .. } => {
                                let (tx, rx) = oneshot::channel::<NodeResponse>();
                                if let Message::ForwardMulRequest(request_id, request_to_network) = request {
                                    let _ = self.process_message(Message::ProcessMulRequest(request_id, request_to_network, tx),peer);
                                    match rx.await {
                                        Ok(resp) => {
                                            // Sending the OPRF Response back to the relay node.
                                            let _ = self.swarm.behaviour_mut().request_response_behaviour.send_response(channel, resp);
                                        }
                                        Err(e) => {
                                            error!(
                                                "Error occurred while sending processing OPRF request: {:?}",
                                                e
                                            );
                                        }
                                    };
                                }
                            }
                            request_response::Message::Response { response, .. } => {
                                // Processing of all forwarded responses
                                if let NodeResponse::ConstructedProofSecp256k1 { node_idx, peer_id, request_id, proof, method } = response {
                                    info!(
                                        "Proof Received for request id: {:?} from node: {}",
                                        request_id, node_idx
                                    );
                                    cast_message!(
                                        ActorType::AppStateEngine,
                                        AppStateChangeMessage::ProcessReceivedProofSecp256k1(node_idx, peer_id, request_id, Box::new(proof), method),
                                        AppStateEngineError
                                    );

                                }else if let NodeResponse::ConstructedProofBabyJubJub { node_idx, peer_id, request_id, proof, method } = response {
                                    info!(
                                        "Proof Received for request id: {:?} from node: {}",
                                        request_id, node_idx
                                    );
                                    cast_message!(
                                        ActorType::AppStateEngine,
                                        AppStateChangeMessage::ProcessReceivedProofBabyJubJub(node_idx, peer_id, request_id, Box::new(proof), method),
                                        AppStateEngineError
                                    );
                                }
                                else if let NodeResponse::Error { request_id,message} = response.clone() {
                                    info!(
                                        "Error occured from node: {} from peer :{}",message,peer
                                    );
                                    // cast_message!(
                                    //     ActorType::AppStateEngine,
                                    //     AppStateChangeMessage::ForwardErrorResponse(request_id,response),
                                    //     AppStateEngineError
                                    // );
                                }
                            }
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        tracing::info!(peer = %peer_id, ?endpoint, "Established new connection");
                        if let Some(messages) = self.cached_data.get(&peer_id) {
                            for message in messages {
                                let status = self.swarm.behaviour_mut().request_response_behaviour.send_request(
                                    &peer_id,
                                    message.clone(),
                                );
                                info!("Forwarding message to peer. Status: {:?}", status);
                            }
                            self.cached_data.remove(&peer_id);
                        }
                    },
                    SwarmEvent::ConnectionClosed { peer_id,cause ,..} => {
                        info!("Connection closed with peer: {:?} Cause {:?}", peer_id,cause);
                        if let Some((_, _)) = self.bootstrap_addresses.iter().find(|(id, _)| id == &peer_id) {
                            info!("Lost connection to bootstrap node: {} Cause {:?}", peer_id,cause);
                            // Adding bootstrapping just in case of safety
                            let status =self.swarm.behaviour_mut().kademlia.bootstrap();
                            info!("Status of bootstrapping :{:?}",status);
                        }else{
                            info!("Connection closed with peer: {:?} Cause {:?}", peer_id,cause);
                        }
                    }
                    SwarmEvent::IncomingConnectionError {
                        connection_id,
                        local_addr,
                        send_back_addr,
                        error,
                        ..
                    } => {
                        tracing::error!(
                            "Incoming connection error for connection ID {:?} from local address {:?} to send-back address {:?} with error: {:?}.",
                            connection_id,
                            local_addr,
                            send_back_addr,
                            error
                        );
                    },
                    SwarmEvent::OutgoingConnectionError {
                        connection_id,
                        peer_id,
                        error,
                        ..
                    } => {
                        tracing::error!(
                            "Failed to establish outgoing connection. Connection ID: {:?}, Peer ID: {:?}, Error: {:?}.",
                            connection_id,
                            peer_id,
                            error
                        );
                        peer_id.and_then(|id| self.cached_data.remove(&id));
                        if let Some(peer_id)=peer_id{
                            if let Some((_, _)) = self.bootstrap_addresses.iter().find(|(id, _)| id == &peer_id) {
                                info!("Triggering connecting to bootstrap node");
                                // Adding bootstrapping just in case of safety
                                let status =self.swarm.behaviour_mut().kademlia.bootstrap();
                                info!("Status of bootstrapping :{:?}",status);
                            }
                        }


                    },

                    SwarmEvent::Behaviour(MyBehaviourEvent::Ping(ping::Event { peer, result, .. }))=> match result {
                            Ok(duration) => {
                                info!("Ping successful with {peer}, duration: {:?}", duration);
                                if let Some(tx)=self.ping_sender.get(&peer){
                                    if let Err(e)=tx.send(Response::new(None, true, format!("Ping successful with {peer}, duration: {:?}", duration))).await{
                                        error!("Error sending ping response back to the receiver :{}",e.to_string());
                                    };
                                    self.ping_sender.remove(&peer);
                                }
                                if !self.successful_kafka_ping_cache.contains_key(&peer) {
                                    // Cache the successful ping to avoid sending duplicate messages
                                    self.successful_kafka_ping_cache.insert(peer.clone(), ());
                                    info!("Successfully pinged peer: {:?}, sending to Kafka", peer);
                                    // Send to Kafka
                                    if let Some(kafka) = self.kafka.clone() {
                                        kafka.send(messages::kafka::KafkaTopic::PeerReachabilityQUIC, peer.to_string(), PeerReachabilityQuicStatus {
                                            success: true,
                                            duration: Some(duration),
                                        }).await;
                                    }
                                } else {
                                    debug!("Ping success for {} skipped due to debounce.", peer);
                                }
                            }
                            Err(e) => {
                                error!("Ping failed with {peer}: {:?}", e);
                                if let Some(tx)=self.ping_sender.get(&peer){
                                    if let Err(e)=tx.send(Response::new(None, false, format!("Ping failed with {peer}: {:?}", e))).await{
                                        error!("Error sending ping response back to the receiver :{}",e.to_string());
                                    };
                                    self.ping_sender.remove(&peer);
                                }
                                if let Some(kafka) = self.kafka.clone() {
                                    kafka.send(messages::kafka::KafkaTopic::PeerReachabilityQUIC, peer.to_string(), PeerReachabilityQuicStatus {
                                        success: false,
                                        duration: None,
                                    }).await;
                                 }
                            }
                        },
                    SwarmEvent::Behaviour(MyBehaviourEvent::Identify(event)) => {
                        match event {
                            identify::Event::Received { peer_id, info, .. } => {
                                info!(
                                    "Received identify info from peer: {:?}, agent: {}, protocol: {:?}, listen_addrs: {:?}",
                                    peer_id, info.agent_version, info.protocol_version, info.listen_addrs
                                );

                                if info.listen_addrs.is_empty() {
                                    warn!("No listen addresses found in identify info for peer: {:?}", peer_id);
                                } else {
                                    for multiaddr in &info.listen_addrs {
                                        if !is_dialable_address(multiaddr) {
                                            warn!("Skipping non-dialable address from {:?}: {:?}", peer_id, multiaddr);
                                            continue;
                                        }
                                        // Add to Kademlia
                                        let agent_routing = self.swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr.clone());
                                        match agent_routing {
                                            RoutingUpdate::Failed => error!("IdentifyReceived: Failed to register address to Kademlia"),
                                            RoutingUpdate::Pending => warn!("IdentifyReceived: Register address pending"),
                                            RoutingUpdate::Success => {
                                                info!("Added peer {:?} with address {:?} to Kademlia DHT", peer_id, multiaddr);
                                            }
                                        }

                                        // Dial the peer if not already connected
                                        if !self.swarm.is_connected(&peer_id) {
                                            match self.swarm.dial(multiaddr.clone()) {
                                                Ok(_) => info!("Dialing peer {:?} at {:?}", peer_id, multiaddr),
                                                Err(e) => warn!("Failed to dial peer {:?} at {:?}: {:?}", peer_id, multiaddr, e),
                                            }
                                        } else {
                                            debug!("Peer {:?} already connected, skipping dial", peer_id);
                                        }
                                    }
                                }

                                debug!(
                                    "Peer {:?} supports protocols: {:?} listen_addrs: {:?}",
                                    peer_id, info.protocols, info.listen_addrs
                                );
                            }

                            identify::Event::Sent { peer_id, .. } => {
                                debug!("Sent identify info to peer: {:?}", peer_id);
                            }
                            identify::Event::Pushed { peer_id, .. } => {
                                debug!("Pushed identify info to peer: {:?}", peer_id);
                            }
                            identify::Event::Error { peer_id, error, .. } => {
                                error!("Identify protocol error with peer {:?}: {:?}", peer_id, error);
                            }
                        }
                    },
                    SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(event)) => {
                        match event {
                            libp2p::kad::Event::OutboundQueryProgressed { result, .. } => {
                                if let QueryResult::GetClosestPeers(Ok(result)) = result {
                                    debug!("DHT discovered peers: {:?}", result.peers);
                                }
                            }
                            _ => {
                                debug!("received Kademlia event: {:?}", event);
                            }
                        }
                    },
                    SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                        if &peer_id == self.swarm.local_peer_id() {
                            debug!(%peer_id, %address, "ignoring our own external address");
                        } else {
                            debug!(%peer_id, %address, "new external address of peer");

                            let result = self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, address.clone());

                                match result {
                                    RoutingUpdate::Success => {
                                        debug!(%peer_id, %address, "added peer address to kademlia");
                                    }
                                    RoutingUpdate::Failed => {
                                        warn!(%peer_id, %address, "failed to add peer address to kademlia");
                                    }
                                    RoutingUpdate::Pending => {
                                        debug!(%peer_id, %address, "request to add peer address to kademlia is pending");
                                    }
                                }
                        }
                    }
                    _ => {
                        info!("Received event: {:?}", event);
                    }
                },
                Some(msg) = self.receiver.recv() => match msg.0 {
                        Message::ElectedProvers(_,_)
                        |Message::ReElectedProvers(_,_,_,_,_)
                        |Message::Round1Out(_,_)
                        |Message::ResharingRound1Out(_,_)
                        | Message::Round2Out(_,_)
                        |Message::ResharingRound2Out(_,_)
                        |Message::ReElection()
                        |Message::Ready(_,_,_,_)
                        |Message::ResharingPrepared(_,_,_)
                        |Message::ReportElectionNodeStatus(_)
                        |Message::ReportReElectionNodeStatus(_)
                        |Message::StartDKGInit()
                        |Message::StartResharingInit()
                        |Message::DKGInitCompleted(_)
                        |Message::ResharingInitCompleted(_)
                        |Message::StartDKGRound1()
                        |Message::StartResharingRound1()
                        |Message::DKGRound1Completed(_)
                        |Message::ResharingRound1Completed(_)
                        |Message::StartDKGRound2()
                        |Message::StartResharingRound2()
                        |Message::DKGRound2Completed(_)
                        |Message::ResharingRound2Completed(_)
                        |Message::StartDKGRound3()
                        |Message::StartResharingRound3()
                        |Message::SaveCurrentState()
                        |Message::RollbackToPreviousState()=> {
                            let new_msg = msg.0.clone();
                            match new_msg.serialize_to_bytes() {
                                Ok(serialized_msg) => {
                                    if let Err(e) = self.swarm.behaviour_mut().gossipsub.publish(IdentTopic::new(msg.1.clone()), serialized_msg) {
                                        error!("Publish error: {:?}", e);
                                    }
                            },
                            Err(e) => {
                                error!("Serialization error: {:?} Msg :{:?}", e,msg);
                            }
                        }
                    }
                        Message::Subscribe(topic) => {
                            let result = self.swarm.behaviour_mut().gossipsub.subscribe(&IdentTopic::new(topic.clone()));
                            match result {
                                Ok(success) => {
                                    if success {
                                        info!("Successfully subscribed to topic: {:?}", topic);
                                    } else {
                                        warn!("Already subscribed to topic: {:?}", topic);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to subscribe to topic: {:?}. Error: {:?}", topic, e);
                                }
                            }
                        },
                        Message::ForwardMulRequest(request_id,request)=>{
                          if let Some(peers)=msg.2{
                              for (peer_id,multi_address) in peers.into_iter() {
                                  info!("Forwarding oprf request to peer: {:?} , Multiaddress :{:?}", peer_id,multi_address);
                                  let status=self.swarm.dial(multi_address);
                                  info!("Connection status :{:?}",status);

                            if let Some(messages) = self.cached_data.get_mut(&peer_id) {
                                messages.push(Message::ForwardMulRequest(request_id.clone(), request.clone()));
                            } else {
                                self.cached_data.insert(peer_id.clone(), vec![Message::ForwardMulRequest(request_id.clone(), request.clone())]);
                            }

                            }
                        }
                        },
                        Message::AddToBlacklist(peer_id)=>{
                            self.swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                        }
                        Message::RemoveFromBlacklist(peer_id)=>{
                            self.swarm.behaviour_mut().gossipsub.remove_blacklisted_peer(&peer_id);
                        }
                        Message::Ping(peer_id, addr, tx)=>{
                            if let Err(e)= self.swarm.dial(addr.clone()) {
                                error!("Failed to dial {addr} error :{}", e.to_string());
                            }
                            self.ping_sender.insert(peer_id, tx);
                        },
                        Message::GetConnectedPeers(tx) => {
                            let connected_peers: Vec<PeerId> = self.swarm.connected_peers().cloned().collect();
                            let _ = tx.send(connected_peers);
                        }
                        Message::GetMeshPeers(tx) => {
                            let mesh_peers: Vec<PeerId> = self.swarm.behaviour_mut().gossipsub.mesh_peers(&IdentTopic::new(NETWORK_TOPIC).hash())
                                .cloned()
                                .collect();
                            let _ = tx.send(mesh_peers);
                        }
                        Message::GetPeerScores(tx) => {
                            let gossipsub = &mut self.swarm.behaviour_mut().gossipsub;
                            let known_peers: Vec<_> = gossipsub.all_peers().collect();
                            debug!("Known peers: {:?}", known_peers);

                            let mut peer_scores = Vec::new();
                            for (peer_id, _peer_info) in &known_peers {
                                let score = gossipsub.peer_score(peer_id);
                                peer_scores.push(PeerScore { peer_id: **peer_id, score });
                            }

                            let _ = tx.send(peer_scores);
                        }
                        _ => {
                          info!("Different message received: {:?}", msg);
                        }
                    },
                    // Time for network discovery
                    _ = discover_tick.tick() => {
                            debug!("Performing periodic peer discovery");
                            self.swarm.behaviour_mut().kademlia.get_closest_peers(PeerId::random());
                        },
            }
        }
    }

    // #[tracing::instrument(skip(self, message))]
    fn process_message(&self, message: Message, _peer_id: PeerId) -> Result<(), ProcessMessageError> {
        match message {
            Message::ElectedProvers(election_info, elected_provers) => {
                cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::CheckElectedStatus(elected_provers), ElectionEngineError);
                cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdatedThreshold(election_info), ElectionEngineError);
                Ok(())
            }
            Message::ReElectedProvers(election_info, tstar_provers, new_provers, group_keys, drand_response) => {
                if self.peer_id != self.relay_peer_id {
                    debug!("Received Relection Details :{:?}", election_info);
                    cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::UpdatedThreshold(election_info.clone()), ElectionEngineError);
                    cast_message!(
                        ActorType::ElectionEngine,
                        ElectionEngineMessage::CheckReElectedStatus(election_info, tstar_provers, new_provers, group_keys, drand_response),
                        ElectionEngineError
                    );
                } else {
                    info!("Cannot process re-elected provers since :{:?} Relay Peer ID :{:?}", self.peer_id, self.relay_peer_id);
                }
                Ok(())
            }
            Message::Round1Out(id, outputs) => {
                cast_message!(ActorType::DkgEngine, DkgEngineMessage::Round1Out(id, outputs), DKGEngineError);
                Ok(())
            }
            Message::ResharingRound1Out(id, outputs) => {
                cast_message!(ActorType::DkgEngine, DkgEngineMessage::ResharingRound1Out(id, outputs), DKGEngineError);
                Ok(())
            }
            Message::Round2Out(idx, pubkeys) => {
                cast_message!(ActorType::DkgEngine, DkgEngineMessage::StoreReceivedPublicKey(idx, pubkeys), DKGEngineError);
                Ok(())
            }
            Message::ResharingRound2Out(idx, pubkeys) => {
                cast_message!(ActorType::DkgEngine, DkgEngineMessage::StoreResharedReceivedPublicKey(idx, pubkeys), DKGEngineError);
                Ok(())
            }
            Message::Ready(idx, peer_id, pubkey_shares, group_public_keys) => {
                info!("Prover {:?} is in ready state", idx);
                cast_message!(
                    ActorType::AppStateEngine,
                    AppStateChangeMessage::StoreKSet(peer_id, pubkey_shares, idx, group_public_keys),
                    AppStateEngineError
                );
                Ok(())
            }
            Message::ResharingPrepared(idx, peer_id, pubkey_shares) => {
                info!("Resharing finished for Prover {:?}", idx);
                cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::ResharingStoreKSet(peer_id, pubkey_shares, idx), AppStateEngineError);
                Ok(())
            }
            Message::Subscribe(_) => todo!(),
            Message::Unsubscribe(_) => todo!(),
            Message::ProcessMulRequest(request_id, request, tx) => {
                cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::ProcessMulRequest(request_id, request, tx), AppStateEngineError);
                Ok(())
            }
            Message::ReElection() => {
                if self.node_type == NodeType::Prover {
                    info!("Forwarding Particpating to relay peer for resharing");
                    info!("Take backup of shares");
                }
                Ok(())
            }
            Message::ReportElectionNodeStatus(peer_id) => {
                if self.peer_id == self.relay_peer_id {
                    info!("Received details to report relection node status for Peer ID: {:?}", peer_id);
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::RecordElectionNodeStatus(peer_id), ElectionEngineError);
                } else {
                    info!(
                        "Unable to process the report for election node status. Current Peer ID: {:?}, Relay Peer ID: {:?}",
                        self.peer_id, self.relay_peer_id
                    );
                }
                Ok(())
            }
            Message::ReportReElectionNodeStatus(peer_id) => {
                if self.peer_id == self.relay_peer_id {
                    info!("Received details to report re-election node status for Peer ID: {:?}", peer_id);
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::RecordRelectionNodeStatus(peer_id), ElectionEngineError);
                } else {
                    info!(
                        "Unable to process the report for re-election node status. Current Peer ID: {:?}, Relay Peer ID: {:?}",
                        self.peer_id, self.relay_peer_id
                    );
                }
                Ok(())
            }
            Message::StartDKGInit() => {
                if self.peer_id != self.relay_peer_id {
                    info!("Start DKG Init received from Relay Node.");
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::StartDKGInit, ElectionEngineError);
                } else {
                    info!("The relay node cannot initiate DKG. This is the relay node.");
                }
                Ok(())
            }
            Message::StartResharingInit() => {
                if self.peer_id != self.relay_peer_id {
                    info!("Start Resharing Init received from Relay Node.");
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::StartResharingInit, ElectionEngineError);
                } else {
                    info!("The relay node cannot initiate resharing. This is the relay node.");
                }
                Ok(())
            }
            Message::DKGInitCompleted(peer_id) => {
                if self.peer_id == self.relay_peer_id {
                    info!("Received details to report dkg init status for Peer ID: {:?}", peer_id);
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::RecordDKGInitNodeStatus(peer_id), ElectionEngineError);
                } else {
                    info!(
                        "Unable to process the report for dkg init status. Current Peer ID: {:?}, Relay Peer ID: {:?}",
                        self.peer_id, self.relay_peer_id
                    );
                }
                Ok(())
            }
            Message::ResharingInitCompleted(peer_id) => {
                if self.peer_id == self.relay_peer_id {
                    info!("Received details to report resharing init status for Peer ID: {:?}", peer_id);
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::RecordResharingInitNodeStatus(peer_id), ElectionEngineError);
                } else {
                    info!(
                        "Unable to process the report for resharing init status. Current Peer ID: {:?}, Relay Peer ID: {:?}",
                        self.peer_id, self.relay_peer_id
                    );
                }
                Ok(())
            }
            Message::StartDKGRound1() => {
                if self.peer_id != self.relay_peer_id {
                    info!("Start DKG Round 1 received from Relay Node.");
                    cast_message!(ActorType::DkgEngine, DkgEngineMessage::StartDKGRound1, DKGEngineError);
                } else {
                    info!("The relay node cannot initiate dkg round 1. This is the relay node.");
                }
                Ok(())
            }
            Message::StartResharingRound1() => {
                if self.peer_id != self.relay_peer_id {
                    info!("Start Resharing Round 1 received from Relay Node.");
                    cast_message!(ActorType::DkgEngine, DkgEngineMessage::StartResharingRound1, DKGEngineError);
                } else {
                    info!("The relay node cannot initiate resharing round 1. This is the relay node.");
                }
                Ok(())
            }
            Message::DKGRound1Completed(peer_id) => {
                if self.peer_id == self.relay_peer_id {
                    info!("Received details to report dkg round 1 completion status for Peer ID: {:?}", peer_id);
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::RecordDKGRound1NodeStatus(peer_id), ElectionEngineError);
                } else {
                    info!(
                        "Unable to process the report for dkg round 1 completion status. Current Peer ID: {:?}, Relay Peer ID: {:?}",
                        self.peer_id, self.relay_peer_id
                    );
                }
                Ok(())
            }
            Message::ResharingRound1Completed(peer_id) => {
                if self.peer_id == self.relay_peer_id {
                    info!("Received details to report resharing round 1 completion status for Peer ID: {:?}", peer_id);
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::RecordResharingRound1NodeStatus(peer_id), ElectionEngineError);
                } else {
                    info!(
                        "Unable to process the report for resharing round 1 completion status. Current Peer ID: {:?}, Relay Peer ID: {:?}",
                        self.peer_id, self.relay_peer_id
                    );
                }
                Ok(())
            }
            Message::StartDKGRound2() => {
                if self.peer_id != self.relay_peer_id {
                    info!("Start DKG Round 2 received from Relay Node.");
                    cast_message!(ActorType::DkgEngine, DkgEngineMessage::StartDKGRound2, DKGEngineError);
                } else {
                    info!("The relay node cannot initiate DKG round 2. This is the relay node.");
                }
                Ok(())
            }
            Message::StartResharingRound2() => {
                if self.peer_id != self.relay_peer_id {
                    info!("Start Resharing Round 2 received from Relay Node.");
                    cast_message!(ActorType::DkgEngine, DkgEngineMessage::StartResharingRound2, DKGEngineError);
                } else {
                    info!("The relay node cannot initiate resharing round 2. This is the relay node.");
                }
                Ok(())
            }
            Message::DKGRound2Completed(peer_id) => {
                if self.peer_id == self.relay_peer_id {
                    info!("Received details to report dkg round 2 completion status for Peer ID: {:?}", peer_id);
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::RecordDKGRound2NodeStatus(peer_id), ElectionEngineError);
                } else {
                    info!(
                        "Unable to process the report for dkg round 2 completion status. Current Peer ID: {:?}, Relay Peer ID: {:?}",
                        self.peer_id, self.relay_peer_id
                    );
                }
                Ok(())
            }
            Message::ResharingRound2Completed(peer_id) => {
                if self.peer_id == self.relay_peer_id {
                    info!("Received details to report resharing round 2 completion status for Peer ID: {:?}", peer_id);
                    cast_message!(ActorType::ElectionEngine, ElectionEngineMessage::RecordResharingRound2NodeStatus(peer_id), ElectionEngineError);
                } else {
                    info!(
                        "Unable to process the report for resharing round 2 completion status. Current Peer ID: {:?}, Relay Peer ID: {:?}",
                        self.peer_id, self.relay_peer_id
                    );
                }
                Ok(())
            }
            Message::StartDKGRound3() => {
                if self.peer_id != self.relay_peer_id {
                    info!("Start DKG Round 3 received from Relay Node.");
                    cast_message!(ActorType::DkgEngine, DkgEngineMessage::StartDKGRound3, DKGEngineError);
                } else {
                    info!("The relay node cannot initiate DKG round 3. This is the relay node.");
                }
                Ok(())
            }
            Message::StartResharingRound3() => {
                if self.peer_id != self.relay_peer_id {
                    info!("Start Resharing Round 3 received from Relay Node.");
                    cast_message!(ActorType::DkgEngine, DkgEngineMessage::StartResharingRound3, DKGEngineError);
                } else {
                    info!("The relay node cannot initiate resharing round 3. This is the relay node.");
                }
                Ok(())
            }
            Message::RollbackToPreviousState() => {
                info!("Start rollback to previous state");
                cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::RollbackToPreviousState, AppStateEngineError);
                Ok(())
            }
            Message::SaveCurrentState() => {
                info!("Start saving current state");
                cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::SaveCurrentState, AppStateEngineError);
                Ok(())
            }
            _ => todo!(),
        }
    }
}

type SenderReceiver = (
    UnboundedSender<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>,
    UnboundedReceiver<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>,
);

#[async_trait]
impl Actor for GossipEngineActor {
    type Msg = GossipEngineMessage;
    type State = SenderReceiver;
    type Arguments = SenderReceiver;
    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> { Ok(args) }
    //#[tracing::instrument(name = "Handle GOSSIP Event ", skip(self, _myself, state, message))]
    async fn handle(&self, _myself: ActorRef<Self::Msg>, message: Self::Msg, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match message {
            GossipEngineMessage::Gossip(message, topic) => {
                let _ = state.0.send((message.clone(), topic.clone(), None));
            }
            GossipEngineMessage::Subscribe(topic) => {
                let _ = state.0.send((Message::Subscribe(topic), "".to_string(), None));
            }
            GossipEngineMessage::Forward(message, peer_ids) => {
                let _ = state.0.send((message.clone(), String::from(""), Some(peer_ids)));
            }
            GossipEngineMessage::Ping(peer_id, addr, tx) => {
                let (tx_ping, mut rx_ping) = tokio::sync::mpsc::channel(10);
                let _ = state.0.send((Message::Ping(peer_id, addr, tx_ping), "".to_string(), None));
                let timeout_duration = Duration::from_secs(5); // for example, 5 seconds

                // Use timeout to receive from the channel
                match timeout(timeout_duration, rx_ping.recv()).await {
                    Ok(Some(message)) => {
                        info!("Received ping response: {:?}", message);
                        if let Err(e) = tx.send(message) {
                            error!("Failed to send response back to the receiver :{:?}", e)
                        };
                    }
                    Ok(None) => {
                        error!("Ping channel closed unexpectedly.");
                    }
                    Err(_) => {
                        error!("Timeout while waiting for ping response.");
                    }
                }
            }
            GossipEngineMessage::GetConnectedPeers(tx) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let _ = state.0.send((Message::GetConnectedPeers(resp_tx), "".to_string(), None));

                let timeout_duration = Duration::from_secs(5); // for example, 5 seconds

                match timeout(timeout_duration, resp_rx).await {
                    Ok(Ok(peers)) => {
                        send_response::<NodeResponse>(
                            tx,
                            NodeResponse::ConnectedPeers {
                                count: peers.len(),
                                connected_peers: peers,
                            },
                            "Error sending response",
                        );
                    }
                    Ok(Err(e)) => {
                        error!("Failed to receive connected peers from listener: {:?}", e);
                        send_response::<NodeResponse>(
                            tx,
                            NodeResponse::Error {
                                request_id: "".into(),
                                message: format!("Failed to receive connected peers: {:?}", e),
                            },
                            "Error sending response",
                        );
                    }
                    Err(_) => {
                        error!("Timeout while waiting for connected peers response.");
                        send_response::<NodeResponse>(
                            tx,
                            NodeResponse::Error {
                                request_id: "".into(),
                                message: "Timeout while waiting for connected peers response".into(),
                            },
                            "Error sending response",
                        );
                    }
                }
            }
            GossipEngineMessage::GetPeerScores(tx) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let _ = state.0.send((Message::GetPeerScores(resp_tx), "".to_string(), None));

                let timeout_duration = Duration::from_secs(5); // for example, 5 seconds

                match timeout(timeout_duration, resp_rx).await {
                    Ok(Ok(scores)) => {
                        send_response::<NodeResponse>(tx, NodeResponse::PeerScores { scores }, "Error sending response");
                    }
                    Ok(Err(e)) => {
                        error!("Failed to receive peer score from listener: {:?}", e);
                        send_response::<NodeResponse>(
                            tx,
                            NodeResponse::Error {
                                request_id: "".into(),
                                message: format!("Failed to receive peer score: {:?}", e),
                            },
                            "Error sending response",
                        );
                    }
                    Err(_) => {
                        error!("Timeout while waiting for peer score response.");
                        send_response::<NodeResponse>(
                            tx,
                            NodeResponse::Error {
                                request_id: "".into(),
                                message: "Timeout while waiting for peer score response".into(),
                            },
                            "Error sending response",
                        );
                    }
                }
            }
            GossipEngineMessage::GetMeshPeers(tx) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let _ = state.0.send((Message::GetMeshPeers(resp_tx), "".to_string(), None));

                let timeout_duration = Duration::from_secs(5); // for example, 5 seconds

                match timeout(timeout_duration, resp_rx).await {
                    Ok(Ok(peers)) => {
                        send_response::<NodeResponse>(
                            tx,
                            NodeResponse::MeshPeers {
                                count: peers.len(),
                                mesh_peers: peers,
                            },
                            "Error sending response",
                        );
                    }
                    Ok(Err(e)) => {
                        error!("Failed to receive mesh peers from listener: {:?}", e);
                        send_response::<NodeResponse>(
                            tx,
                            NodeResponse::Error {
                                request_id: "".into(),
                                message: format!("Failed to receive mesh peers: {:?}", e),
                            },
                            "Error sending response",
                        );
                    }
                    Err(_) => {
                        error!("Timeout while waiting for mesh peers response.");
                        send_response::<NodeResponse>(
                            tx,
                            NodeResponse::Error {
                                request_id: "".into(),
                                message: "Timeout while waiting for mesh peers response".into(),
                            },
                            "Error sending response",
                        );
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

pub struct GossipEngineSupervisor {
    panic_tx: tokio::sync::mpsc::Sender<ActorCell>,
}

impl GossipEngineSupervisor {
    pub fn new(panic_tx: tokio::sync::mpsc::Sender<ActorCell>) -> Self { Self { panic_tx } }
}

#[derive(Debug, Error, Default)]
pub enum GossipEngineSupervisorError {
    #[default]
    #[error("failed to acquire GossipEngineSupervisorError from registry")]
    RactorRegistryError,
}

#[async_trait]
impl Actor for GossipEngineSupervisor {
    type Msg = GossipEngineMessage;
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
