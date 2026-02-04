use libp2p::PeerId;
use messages::message::{ElectionInfo, ProverInfo};
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ResharingRound1Context {
    pub tstar_provers: HashMap<PeerId, (ProverInfo, usize)>,
    pub new_provers: HashMap<PeerId, (ProverInfo, usize)>,
    pub group_keys: HashMap<String, Vec<u8>>,
    pub election_info: ElectionInfo,
}

/// Context for initializing resharing operations in the DKG engine.
///
/// This struct holds the necessary references and data needed to start the resharing process.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ResharingInitContext {
    pub rsa_private_key: RsaPrivateKey,

    /// Information about the current election, including details required for resharing.
    pub election_info: ElectionInfo,

    /// The peer ID of the node associated with this resharing operation.
    pub node_peer_id: PeerId,

    /// A map of group public keys, where the key is a key type
    pub group_keys: HashMap<String, Vec<u8>>,
}
