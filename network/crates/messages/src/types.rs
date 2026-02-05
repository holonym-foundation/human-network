use std::collections::{HashMap, HashSet};

use crate::{message::ProverInfo, network_utils::Method};
use ark_ed_on_bn254::EdwardsAffine;
use ethers::abi::Address;
use k256::AffinePoint;
use libp2p::PeerId;
use human_crypto::{curve::EncodedBabyJubJubPoint, BabyJubJub, Curve, DLEQProof, Secp256k1};
use serde::{Deserialize, Serialize};
pub type Topic = String;
pub type RequestID = String;
pub type RSAPublicKey = String;
/// State Request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateRequest {
    pub user: Address,
    pub method: Method,
}
/// Peer Score Struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerScore {
    pub peer_id: PeerId,
    pub score: Option<f64>,
}

/// State Response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateResponse {
    Success { epoch: u32, method: Method, requests_from_user: u128 },
    Error { message: String },
}
pub trait AffinePointTrait {}
impl AffinePointTrait for AffinePoint {}
impl AffinePointTrait for EdwardsAffine {}
/// OPRF Response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeResponse {
    Submitted {
        request_id: String,
    },
    ConstructedProofSecp256k1 {
        node_idx: u32,
        peer_id: PeerId,
        request_id: String,
        proof: DLEQProof<32, Secp256k1>,
        method: Method,
    },
    ConstructedProofBabyJubJub {
        node_idx: u32,
        peer_id: PeerId,
        request_id: String,
        proof: DLEQProof<32, BabyJubJub>,
        method: Method,
    },
    VerifiedProofSecp256k1 {
        request_id: String,
        reconstructed_point: <Secp256k1 as Curve<32>>::Point,
    },
    VerifiedProofBabyJubJub {
        request_id: String,
        reconstructed_point: EncodedBabyJubJubPoint,
    },
    Participated,
    Error {
        request_id:String,
        message: String,
    },
    Keyshare(HashMap<String, String>),
    Restored,
    ElectionState {
        node_idx: usize,
        n: u32,
        t: u32,
        epsilon: u32,
        new_provers: HashMap<PeerId, (ProverInfo, usize)>,
    },
    VotingPower {
        node_idx: usize,
        voting_power: String,
        peer_id: PeerId,
    },
    UpdatedIsResharingEnabled,
    Version {
        version: String,
    },
    AddedToExclusionList {
        peer_id: PeerId,
    },
    RemovedFromExclusionList {
        peer_id: PeerId,
    },
    CurrentExcludedPeers {
        excluded_peers: HashSet<PeerId>,
    },
    ConnectedPeers {
        count: usize,
        connected_peers: Vec<PeerId>,
    },
    PeerScores {
        scores: Vec<PeerScore>,
    },
    MeshPeers {
        count: usize,
        mesh_peers: Vec<PeerId>,
    },
    ElectionParams {
        min_nodes: usize,
        epsilon: u32,
        threshold: f64,
    },
}
impl NodeResponse {
    pub fn new_constructed_proof<const N: usize, C: Curve<N>>(node_idx: u32, peer_id: PeerId, request_id: String, proof: DLEQProof<N, C>, method: Method) -> Result<Self, anyhow::Error> {
        // Hacky but safe and effective way at getting past the annoying type constraints: serializing and deserializing to the correct type
        match C::NAME {
            "Secp256k1" => Ok(Self::ConstructedProofSecp256k1 {
                node_idx,
                peer_id,
                request_id,
                method,
                proof: serde_json::from_str(&serde_json::to_string(&proof)?)?,
            }),
            "BabyJubJub" => Ok(Self::ConstructedProofBabyJubJub {
                node_idx,
                peer_id,
                request_id,
                method,
                proof: serde_json::from_str(&serde_json::to_string(&proof)?)?,
            }),
            _ => Err(anyhow::anyhow!("Invalid curve")),
        }
    }
}
/// Response Struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub data: Option<String>,
    pub success: bool,
    pub message: String,
}
impl Response {
    pub fn new(data: Option<String>, success: bool, message: String) -> Self { Response { data, success, message } }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ElectionResponse {
    pub peers: Vec<PeerId>,
    pub n: u32,
    pub t: u32,
    pub is_peer_elected: bool,
    pub status: bool,
    pub message: String,
}
impl ElectionResponse {
    pub fn new(peers: Vec<PeerId>, n: u32, t: u32, is_peer_elected: bool, status: bool, message: String) -> Self {
        ElectionResponse {
            peers,
            n,
            t,
            is_peer_elected,
            status,
            message,
        }
    }

    pub fn err_response(message: String) -> Self {
        let mut response = ElectionResponse::default();
        response.message = message;
        response
    }
}
