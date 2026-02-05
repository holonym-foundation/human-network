use libp2p::gossipsub::{IdentTopic, PeerScoreParams, PeerScoreThresholds, TopicScoreParams};
use libp2p::identify;
use libp2p::identity::Keypair;
use libp2p::kad::store::MemoryStore;
use libp2p::kad::Mode;
use libp2p::request_response::Config;
use libp2p::swarm::StreamProtocol;
use libp2p::{gossipsub, kad, swarm::NetworkBehaviour};
use libp2p::{
    relay,
    request_response::{cbor, ProtocolSupport},
};
use messages::message::Message;
use messages::types::NodeResponse;
use messages::{GOSSIP_SUB_PROTOCOL_NAME, IDENTIFY_PROTOCOL_NAME, KAD_PROTOCOL_NAME, NETWORK_TOPIC, REQUEST_RESPONSE_PROTOCOL_NAME};
use tokio::io;

/// `MyBehaviour` implements custom network behaviors for Gossipsub, Kademlia, Relay, and Request-Response protocols.
///
/// # Fields
/// - `gossipsub`: Manages Gossipsub pub/sub messaging behavior.
/// - `kademlia`: Handles Kademlia DHT behavior for peer-to-peer node discovery.
/// - `relay_client`: Handles client-side relay behavior for relayed communications.
/// - `request_response_behaviour`: Manages request/response protocol using CBOR encoding between nodes.
#[derive(NetworkBehaviour)]
pub struct MyBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    relay_client: relay::client::Behaviour,
    pub request_response_behaviour: cbor::Behaviour<Message, NodeResponse>,
    pub ping: libp2p::ping::Behaviour,
    pub identify: identify::Behaviour,
}

impl MyBehaviour {
    /// Creates a new instance of `MyBehaviour` with custom configurations for Gossipsub and Kademlia.
    ///
    /// # Arguments
    /// - `key`: The node's `Keypair` used for generating peer identity and signing messages.
    /// - `relay_behaviour`: Relay behavior for handling relayed connections.
    ///
    /// # Returns
    /// A result containing `MyBehaviour` if successful, or an error wrapped in a `Box<dyn std::error::Error>`.
    ///
    /// # Errors
    /// This function will return an error if there is an issue with Gossipsub or Kademlia configuration.
    pub fn new(key: Keypair, relay_behaviour: relay::client::Behaviour) -> Result<Self, Box<dyn std::error::Error>> {
        // Generate the peer ID from the provided Keypair
        let peer_id = key.public().to_peer_id();

        // Set up Gossipsub configuration with heartbeat interval and message validation
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .protocol_id_prefix(GOSSIP_SUB_PROTOCOL_NAME)
            .build()
            .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?;

        // Set up Peer Scoring
        let mut peer_score_params = PeerScoreParams::default();
        peer_score_params.topics.insert(IdentTopic::new(NETWORK_TOPIC).hash(), TopicScoreParams::default());

        let peer_score_thresholds = PeerScoreThresholds::default();
        // Create Gossipsub behavior with message authenticity signed using the provided key
        let mut gossipsub = gossipsub::Behaviour::new(gossipsub::MessageAuthenticity::Signed(key.clone()), gossipsub_config)?;
        gossipsub.with_peer_score(peer_score_params, peer_score_thresholds).expect("Valid score params and thresholds");

        // Set up Kademlia configuration with a query timeout of 5 minutes
        let kad_config = kad::Config::new(StreamProtocol::new(KAD_PROTOCOL_NAME));

        // Create an in-memory Kademlia DHT store
        let store = kad::store::MemoryStore::new(peer_id);
        let mut kademlia = kad::Behaviour::with_config(peer_id, store, kad_config);
        kademlia.set_mode(Some(Mode::Server));
        // Initialize and return the behavior with Gossipsub, Kademlia, Relay, and Request-Response protocols
        Ok(Self {
            gossipsub,
            kademlia,
            relay_client: relay_behaviour,
            request_response_behaviour: cbor::Behaviour::new([(StreamProtocol::new(REQUEST_RESPONSE_PROTOCOL_NAME), ProtocolSupport::Full)], Config::default()),
            ping: libp2p::ping::Behaviour::new(libp2p::ping::Config::new()),
            identify: identify::Behaviour::new(identify::Config::new(IDENTIFY_PROTOCOL_NAME.to_string(), key.public())),
        })
    }
}
