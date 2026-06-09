use crate::drand::DrandResponse;
use crate::network_utils::{Method, RequestToNetwork};
use crate::types::*;
use crate::utils::{HumanPublicKey, MonitorEvent};
use ethers::types::{Address, U256};
use libp2p::{Multiaddr, PeerId};
use human_crypto::{node_selection::HasWeight, BabyJubJub, DLEQProof, Secp256k1};
use num_bigint::BigUint;
use pubsub_rs::Pubsub;
use ractor_cluster::RactorMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::oneshot::Sender;

pub enum CurveType {
    Secp256k1,
    BabyJubJub,
}
type IsRelected = bool;
type IsCacheProvers = bool;
type NodeIdx = usize;
type NodeIdxU128 = u128;
type Threshold = u32;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionParams {
    pub threshold: Option<f64>,
    pub epsilon: Option<u32>,
    pub min_nodes: Option<usize>,
}
/// DKG Engine Messages
#[derive(Debug, RactorMessage)]
pub enum DkgEngineMessage {
    Init(NodeIdx, PeerId, HashMap<PeerId, (ProverInfo, NodeIdx)>, Threshold),
    Round1,
    Round1Out(NodeIdxU128, Round1Outputs),
    Round2,
    StoreReceivedPublicKey(NodeIdxU128, PubkeyShares),
    StoreResharedReceivedPublicKey(NodeIdxU128, PubkeyShares),
    Round3,
    ResharingInit(
        NodeIdx,
        HashMap<PeerId, (ProverInfo, NodeIdx)>,
        HashMap<PeerId, (ProverInfo, NodeIdx)>,
        ElectionInfo,
        PeerId,
        HashMap<String, Vec<u8>>,
    ),
    ResharingRound1(ElectionInfo, IsRelected),
    ResharingRound1Out(NodeIdxU128, ResharingRound1Outputs),
    ResharingRound2,
    ResharingRound3,
    ClearShares,
    StartDKGRound1,
    StartDKGRound2,
    StartDKGRound3,
    StartResharingRound1,
    StartResharingRound2,
    StartResharingRound3,
}
/// Election Engine Messages
#[derive(Debug, RactorMessage)]
pub enum ElectionEngineMessage {
    Participating(PeerId, IsRelected),
    FirstElection,
    TriggerReElection(Vec<ProverInfo>),
    CheckElectedStatus(Vec<ProverInfo>),
    CheckReElectedStatus(
        ElectionInfo,
        HashMap<PeerId, (ProverInfo, NodeIdx)>,
        HashMap<PeerId, (ProverInfo, NodeIdx)>,
        HashMap<String, Vec<u8>>,
        DrandResponse,
    ),
    RecordElectionNodeStatus(PeerId),
    RecordRelectionNodeStatus(PeerId),
    StartDKGInit,
    StartResharingInit,
    RecordDKGInitNodeStatus(PeerId),
    RecordResharingInitNodeStatus(PeerId),
    RecordDKGRound1NodeStatus(PeerId),
    RecordResharingRound1NodeStatus(PeerId),
    RecordDKGRound2NodeStatus(PeerId),
    RecordResharingRound2NodeStatus(PeerId),
    AddToExclusionList(PeerId, Sender<NodeResponse>),
    RemoveFromExclusionList(PeerId, Sender<NodeResponse>),
    GetExclusionList(Sender<NodeResponse>),
    UpdateElectionParams(ElectionParams, Sender<NodeResponse>),
    GetElectionParams(Sender<NodeResponse>),
}
/// App State Change Messages
#[derive(RactorMessage)]
pub enum AppStateChangeMessage {
    StateRequest(StateRequest, Sender<StateResponse>),
    MulRequest(RequestToNetwork, Pubsub<String, NodeResponse>, String),
    ProcessMulRequest(RequestID, RequestToNetwork, Sender<NodeResponse>),
    UpdateElectedProvers(Vec<ProverInfo>, IsCacheProvers),
    UpdatedThreshold(ElectionInfo),
    UpdateNodeIdx(u32),
    StoreKeyShares(PrivkeyShares),
    ProcessReceivedProofSecp256k1(u32, PeerId, RequestID, Box<DLEQProof<32, Secp256k1>>, Method),
    ProcessReceivedProofBabyJubJub(u32, PeerId, RequestID, Box<DLEQProof<32, BabyJubJub>>, Method),
    StoreKSet(PeerId, PubkeyShares, u128, GroupPublicKeys),
    ResharingStoreKSet(PeerId, PubkeyShares, u128),
    FetchReconstructedPoint(RequestID, Sender<NodeResponse>),
    StoreQuorumMap(HashMap<PeerId, (ProverInfo, usize)>, HashMap<PeerId, (ProverInfo, usize)>),
    PreResharing,
    ClearShares,
    ForwardReElectedProvers(ElectionInfo, HashMap<PeerId, (ProverInfo, usize)>, HashMap<PeerId, (ProverInfo, usize)>, DrandResponse),
    UpdateCachedProversMap(HashMap<PeerId, (ProverInfo, usize)>),
    BackupState,
    MonitorQuorumFormation(MonitorEvent),
    FetchKeyShare(Sender<NodeResponse>),
    RestoreKeyShare(HashMap<String, String>, Sender<NodeResponse>),
    FetchElectionState(PeerId, Sender<NodeResponse>),
    RestoreElectionState(Sender<NodeResponse>),
    SyncPeerData(Sender<Response>),
    FetchElectionInfo(PeerId, Sender<ElectionResponse>),
    RollbackToPreviousState,
    SaveCurrentState,
    QuicPing(PeerId, Sender<Response>),
    FetchVotingPower(PeerId, Sender<NodeResponse>),
    ForwardErrorResponse(String,NodeResponse),
    SetResharingEnabled(bool, Sender<NodeResponse>),
    FetchFinalizedGroupPubkeys(Sender<NodeResponse>),
}
pub type Round1Output = (HashMap<u128, Vec<u8>>, Vec<Vec<u8>>, Vec<u8>);
pub type Round2Output = Vec<u8>;
pub type ResharingRound1Output = (HashMap<u128, Vec<u8>>, Vec<Vec<u8>>);
pub type ResharingRound2Output = Vec<u8>;
/// A dictionary of Method => Round1Output
pub type Round1Outputs = HashMap<String, Round1Output>;
pub type ResharingRound1Outputs = HashMap<String, ResharingRound1Output>;
/// A dictionary of Method => Round2Output
pub type Round2Outputs = HashMap<String, Round2Output>;
pub type ResharingRound2Outputs = HashMap<String, ResharingRound2Output>;
/// A dictionary of Method => private key share
pub type PrivkeyShares = HashMap<String, Vec<u8>>;
/// A dictionary of Method => public key share
pub type PubkeyShares = HashMap<String, Vec<u8>>;
/// A dictionary of Method => group public key
pub type GroupPublicKeys = HashMap<String, Vec<u8>>;
#[derive(Debug, Serialize, Deserialize, RactorMessage)]
pub enum Message {
    // Prover Elections
    ElectedProvers(ElectionInfo, Vec<ProverInfo>),
    ReElectedProvers(
        ElectionInfo,
        HashMap<PeerId, (ProverInfo, NodeIdx)>,
        HashMap<PeerId, (ProverInfo, NodeIdx)>,
        HashMap<String, Vec<u8>>,
        DrandResponse,
    ),
    ReElection(),

    // Round Outputs
    Round1Out(u128, Round1Outputs),
    ResharingRound1Out(u128, ResharingRound1Outputs),
    Round2Out(u128, Round2Outputs),
    ResharingRound2Out(u128, ResharingRound2Outputs),

    // Ready State
    Ready(u128, PeerId, PubkeyShares, GroupPublicKeys),

    ResharingPrepared(u128, PeerId, PubkeyShares),

    // Subscription Management
    Subscribe(Topic),
    Unsubscribe(Topic),

    // General Messaging
    Message(String),
    ForwardMulRequest(RequestID, RequestToNetwork),

    // Processing Messages (skipped serialization)
    #[serde(skip)]
    ProcessMulRequest(String, RequestToNetwork, Sender<NodeResponse>),

    AddToBlacklist(PeerId),
    RemoveFromBlacklist(PeerId),

    ReportElectionNodeStatus(PeerId),
    ReportReElectionNodeStatus(PeerId),
    StartDKGInit(),
    StartResharingInit(),
    DKGInitCompleted(PeerId),
    ResharingInitCompleted(PeerId),
    StartDKGRound1(),
    StartResharingRound1(),
    DKGRound1Completed(PeerId),
    ResharingRound1Completed(PeerId),
    StartDKGRound2(),
    StartResharingRound2(),
    DKGRound2Completed(PeerId),
    ResharingRound2Completed(PeerId),
    StartDKGRound3(),
    StartResharingRound3(),
    #[serde(skip)]
    Ping(PeerId, Multiaddr, tokio::sync::mpsc::Sender<Response>),
    RollbackToPreviousState(),
    SaveCurrentState(),
    #[serde(skip)]
    GetConnectedPeers(tokio::sync::oneshot::Sender<Vec<PeerId>>),
    #[serde(skip)]
    GetPeerScores(tokio::sync::oneshot::Sender<Vec<PeerScore>>),
    #[serde(skip)]
    GetMeshPeers(tokio::sync::oneshot::Sender<Vec<PeerId>>),
}
/// Information about an election round
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ElectionInfo {
    pub threshold: u32,
    pub epsilon: u32,
    pub total_provers: u32,
}
impl ElectionInfo {
    pub fn new(threshold: u32, epsilon: u32, total_provers: u32) -> Self { ElectionInfo { threshold, epsilon, total_provers } }
}
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, Deserialize)]
pub struct ProverInfoWithoutStake {
    pub evm_address: Address,
    pub peer_id: PeerId,
    pub address: Multiaddr,
    pub rpcaddr: String,
    // Serialize as `mishti_pub_key` for wire compatibility with old (V3.2.0) nodes, which
    // emit that field name; accept both names so mixed-version networks interoperate.
    #[serde(rename = "mishti_pub_key", alias = "human_pub_key")]
    pub human_pub_key: HumanPublicKey,
    pub rsa_pub_key: RSAPublicKey,
    pub idx: usize,
}
/// A struct with the information necessary to identify and contact a prover.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, Deserialize, PartialOrd, Ord)]
pub struct ProverInfo {
    pub evm_address: Address,
    pub peer_id: PeerId,
    pub address: Multiaddr,
    pub rpcaddr: String,
    // Serialize as `mishti_pub_key` for wire compatibility with old (V3.2.0) nodes, which
    // emit that field name; accept both names so mixed-version networks interoperate.
    #[serde(rename = "mishti_pub_key", alias = "human_pub_key")]
    pub human_pub_key: HumanPublicKey,
    pub rsa_pub_key: RSAPublicKey,
    pub voting_power: U256,
    pub idx: usize,
}
impl HasWeight for ProverInfo {
    fn weight(&self) -> num_bigint::BigUint {
        let mut bytes = Vec::new();
        for &value in &self.voting_power.0 {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        BigUint::from_bytes_le(&bytes)
    }
}
impl Clone for Message {
    fn clone(&self) -> Self {
        match self {
            Message::ElectedProvers(election_info, provers_info) => Message::ElectedProvers(election_info.clone(), provers_info.clone()),
            Message::ReElectedProvers(election_info, tstar_provers, new_provers, group_keys, drand_response) => {
                Message::ReElectedProvers(election_info.clone(), tstar_provers.clone(), new_provers.clone(), group_keys.clone(), drand_response.clone())
            }
            Message::Round1Out(id, outputs) => Message::Round1Out(*id, outputs.clone()),
            Message::ResharingRound1Out(id, outputs) => Message::ResharingRound1Out(*id, outputs.clone()),
            Message::ResharingRound2Out(id, outputs) => Message::ResharingRound2Out(*id, outputs.clone()),

            Message::Round2Out(id, outputs) => Message::Round2Out(*id, outputs.clone()),
            Message::Ready(data, peer_id, pubkey_shares, group_public_keys) => Message::Ready(*data, *peer_id, pubkey_shares.clone(), group_public_keys.clone()),
            Message::ResharingPrepared(data, peer_id, pubkey_shares) => Message::ResharingPrepared(*data, *peer_id, pubkey_shares.clone()),

            Message::Subscribe(data) => Message::Subscribe(data.clone()),
            Message::Unsubscribe(data) => Message::Unsubscribe(data.clone()),
            Message::Message(data) => Message::Message(data.clone()),
            Message::ForwardMulRequest(request_id, data) => Message::ForwardMulRequest(request_id.clone(), data.clone()),
            Message::ProcessMulRequest(_, _, _) => {
                panic!("Attempted to clone ProcessOprf variant, which is not allowed");
            }
            Message::ReElection() => Message::ReElection(),
            Message::AddToBlacklist(peer_id) => Message::AddToBlacklist(*peer_id),
            Message::RemoveFromBlacklist(peer_id) => Message::RemoveFromBlacklist(*peer_id),
            Message::ReportElectionNodeStatus(peer_id) => Message::ReportElectionNodeStatus(*peer_id),
            Message::ReportReElectionNodeStatus(peer_id) => Message::ReportReElectionNodeStatus(peer_id.clone()),
            Message::StartDKGInit() => Message::StartDKGInit(),
            Message::StartResharingInit() => Message::StartResharingInit(),
            Message::DKGInitCompleted(peer_id) => Message::DKGInitCompleted(*peer_id),
            Message::ResharingInitCompleted(peer_id) => Message::ResharingInitCompleted(*peer_id),
            Message::StartDKGRound1() => Message::StartDKGRound1(),
            Message::StartResharingRound1() => Message::StartResharingRound1(),
            Message::DKGRound1Completed(peer_id) => Message::DKGRound1Completed(*peer_id),
            Message::ResharingRound1Completed(peer_id) => Message::ResharingRound1Completed(*peer_id),
            Message::StartDKGRound2() => Message::StartDKGRound2(),
            Message::StartResharingRound2() => Message::StartResharingRound2(),
            Message::DKGRound2Completed(peer_id) => Message::DKGRound2Completed(*peer_id),
            Message::ResharingRound2Completed(peer_id) => Message::ResharingRound2Completed(*peer_id),
            Message::StartDKGRound3() => Message::StartDKGRound3(),
            Message::StartResharingRound3() => Message::StartResharingRound3(),
            Message::Ping(peer_id, addr, sender) => Message::Ping(*peer_id, addr.clone(), sender.clone()),
            Message::RollbackToPreviousState() => Message::RollbackToPreviousState(),
            Message::SaveCurrentState() => Message::SaveCurrentState(),
            Message::GetConnectedPeers(_) => {
                panic!("Attempted to clone GetConnectedPeers variant, which is not allowed");
            },
            Message::GetPeerScores(_) => {
                panic!("Attempted to clone GetPeerScores variant, which is not allowed");
            },
            Message::GetMeshPeers(_) => {
                panic!("Attempted to clone GetMeshPeers variant, which is not allowed");
            },
        }
    }
}
impl Message {
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> { serde_json::to_vec(self) }
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> { serde_json::from_slice(bytes) }
    pub fn get_type(&self) -> &str {
        match self {
            Message::ElectedProvers(_, _) => "ElectedProvers",
            Message::ReElectedProvers(_, _, _, _, _) => "ReElectedProvers",
            Message::Round1Out(_, _) => "Round1Out",
            Message::ResharingRound1Out(_, _) => "ResharingRound1Out",
            Message::Round2Out(_, _) => "Round2Output",
            Message::ResharingRound2Out(_, _) => "ResharingRound2Out",
            Message::Ready(_, _, _, _) => "Ready",
            Message::ResharingPrepared(_, _, _) => "ResharingPrepared",
            Message::Subscribe(_) => "Subscribe",
            Message::Unsubscribe(_) => "Unsubscribe",
            Message::Message(_) => "Message",
            Message::ForwardMulRequest(_, _) => "ForwardMulRequest",
            Message::ProcessMulRequest(_, _, _) => "ProcessOprf",
            Message::ReElection() => "ReElection",
            Message::AddToBlacklist(_) => "AddToBlackList",
            Message::RemoveFromBlacklist(_) => "RemoveFromBlacklist",
            Message::ReportReElectionNodeStatus(_) => "ReportReElectionNodeStatus",
            Message::ReportElectionNodeStatus(_) => "ReportElectionNodeStatus",
            Message::StartDKGInit() => "StartDKGInit",
            Message::StartResharingInit() => "StartResharingInit",
            Message::DKGInitCompleted(_) => "DKGInitCompleted",
            Message::ResharingInitCompleted(_) => "ResharingInitCompleted",
            Message::StartDKGRound1() => "StartDKGRound1",
            Message::StartResharingRound1() => "StartResharingRound1",
            Message::DKGRound1Completed(_) => "DKGRound1Completed",
            Message::ResharingRound1Completed(_) => "ResharingRound1Completed",
            Message::StartDKGRound2() => "StartDKGRound2",
            Message::StartResharingRound2() => "StartResharingRound2",
            Message::DKGRound2Completed(_) => "DKGRound2Completed",
            Message::ResharingRound2Completed(_) => "ResharingRound2Completed",
            Message::StartDKGRound3() => "StartDKGRound3",
            Message::StartResharingRound3() => "StartResharingRound3",
            Message::Ping(_, _, _) => "SwarmPing",
            Message::RollbackToPreviousState() => "RollbackToPreviousState",
            Message::SaveCurrentState() => "SaveCurrentState",
            Message::GetConnectedPeers(_) => "GetConnectedPeers",
            Message::GetPeerScores(_) => "GetPeerScores",
            Message::GetMeshPeers(_) => "GetMeshPeers",
        }
    }
}
/// Gossip Engine Messages
#[derive(Debug, RactorMessage)]
pub enum GossipEngineMessage {
    Gossip(Message, Topic),
    ForwardToRelay(PeerId),
    Forward(Message, Vec<(PeerId, Multiaddr)>),
    Subscribe(Topic),
    Unsubscribe(Topic),
    Block(PeerId),
    Ping(PeerId, Multiaddr, Sender<Response>),
    GetConnectedPeers(Sender<NodeResponse>),
    GetPeerScores(Sender<NodeResponse>),
    GetMeshPeers(Sender<NodeResponse>),
}

#[cfg(test)]
mod prover_info_wire_compat_tests {
    use super::*;
    use crate::utils::HumanPublicKey;
    use libp2p::identity::Keypair;

    fn sample_prover() -> ProverInfo {
        let public = Keypair::generate_ed25519().public();
        ProverInfo {
            evm_address: Address::zero(),
            peer_id: public.to_peer_id(),
            address: "/ip4/127.0.0.1/tcp/8000".parse().unwrap(),
            rpcaddr: "http://127.0.0.1:8000".to_string(),
            human_pub_key: HumanPublicKey(public),
            rsa_pub_key: "rsa".to_string(),
            voting_power: U256::from(10u64),
            idx: 0,
        }
    }

    // The pubkey field must travel on the wire as `mishti_pub_key` (what old V3.2.0
    // nodes emit), and deserialization must accept both `mishti_pub_key` (old + new)
    // and `human_pub_key` (un-reverted V3.3.0 stragglers) so mixed-version networks work.
    #[test]
    fn prover_info_emits_mishti_and_accepts_both_field_names() {
        let prover = sample_prover();
        let json = serde_json::to_value(&prover).unwrap();
        assert!(json.get("mishti_pub_key").is_some(), "must serialize as mishti_pub_key");
        assert!(json.get("human_pub_key").is_none(), "must not serialize as human_pub_key");

        // Old-node wire format round-trips.
        let from_mishti: ProverInfo = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(from_mishti, prover);

        // human_pub_key alias still deserializes.
        let mut obj = json.as_object().unwrap().clone();
        let pk = obj.remove("mishti_pub_key").unwrap();
        obj.insert("human_pub_key".to_string(), pk);
        let from_human: ProverInfo = serde_json::from_value(serde_json::Value::Object(obj)).unwrap();
        assert_eq!(from_human, prover);
    }
}
