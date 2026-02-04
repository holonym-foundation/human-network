use serde::{Deserialize, Serialize};
use std::fmt;
pub mod actor_type;
pub mod drand;
pub mod jwt;
pub mod message;
pub mod network_utils;
pub mod task_proofs;
pub mod types;
pub mod utils;
pub mod kafka;
pub const NETWORK_TOPIC: &str = "main-net-alpha";
pub const GOSSIP_SUB_PROTOCOL_NAME: &str = "/human-network/gossipsub/0.1";
pub const KAD_PROTOCOL_NAME: &str = "/human-network/kad/0.1";
pub const REQUEST_RESPONSE_PROTOCOL_NAME: &str = "/String";
pub const IDENTIFY_PROTOCOL_NAME: &str = "/human-network/identify/0.1";
#[derive(Debug, Serialize, Deserialize)]
pub enum NodeState {
    Bootstrap,
    WaitingForElection,
    QuorumElected,
    DkgRound1,
    DkgRound2,
    DkgRound3,
}
pub type RSAPublicKey = String;
impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Bootstrap => write!(f, "Bootstrap"),
            NodeState::WaitingForElection => write!(f, "WaitingForElection"),
            NodeState::QuorumElected => write!(f, "QuorumElected"),
            NodeState::DkgRound1 => write!(f, "DKG-Round1"),
            NodeState::DkgRound2 => write!(f, "DKG-Round2"),
            NodeState::DkgRound3 => write!(f, "DKG-Round3"),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ElectionState {
    #[default]
    CollectingProposals,
    CollectedProposals,
    TriggerElection,
    QuorumElected,
}
impl fmt::Display for ElectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElectionState::CollectingProposals => write!(f, "CollectingProposals"),
            ElectionState::CollectedProposals => write!(f, "CollectedProposals"),
            ElectionState::QuorumElected => write!(f, "QuorumElected"),
            ElectionState::TriggerElection => write!(f, "TriggerElection"),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DkgState {
    #[default]
    DkgInit,
}
impl fmt::Display for DkgState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            &DkgState::DkgInit => write!(f, "DKGInit"),
        }
    }
}
