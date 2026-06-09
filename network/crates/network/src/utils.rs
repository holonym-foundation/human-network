//! This library provides utilities for generating and managing libp2p keypairs for testing purposes.
use ::env::environment::ENVIRONMENT;
use core::fmt;
use ethers::{
    contract::{abigen, ContractError},
    providers::{Http, Provider},
    types::{H160, U256},
};
use libp2p::{identity::secp256k1 as libp2p_secp256k1, multiaddr::Protocol};
use libp2p::identity::Keypair;
use libp2p::identity::PublicKey;
use libp2p::{Multiaddr, PeerId};
use log::info;
use messages::message::{ProverInfo, ProverInfoWithoutStake};
use messages::types::RSAPublicKey;
use messages::utils::HumanPublicKey;
use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use std::{env, panic};
use tokio::sync::oneshot::Sender;
use tracing::error;
use rand::Rng;
/// A struct representing a keypair name and its associated multiaddr.
#[derive(Clone, Serialize, Deserialize)]
struct KeypairName {
    name: String,
    multiaddr: String,
    rpcaddr: String,
    node_type: NodeType,
    public_key: String,
    rsa_public_key: String,
    stake: u16,
}
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    Bootstrap,
    Relay,
    Prover,
    Verifier,
}
impl FromStr for NodeType {
    type Err = String;
    fn from_str(input: &str) -> Result<NodeType, Self::Err> {
        match input {
            "Bootstrap" => Ok(NodeType::Bootstrap),
            "Relay" => Ok(NodeType::Relay),
            "Prover" => Ok(NodeType::Prover),
            "Verifier" => Ok(NodeType::Verifier),
            _ => Err(format!("Invalid NodeType: {}", input)),
        }
    }
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let node_type_str = match self {
            NodeType::Bootstrap => "Bootstrap Node",
            NodeType::Relay => "Relay Node",
            NodeType::Prover => "Prover Node",
            NodeType::Verifier => "Verifier Node",
        };
        write!(f, "{}", node_type_str)
    }
}
abigen!(
    PeerRegistry,
    r#"[
        {
            "inputs": [],
            "name": "getPeers",
            "outputs": [{
                "components": [
                    {
                        "internalType": "address",
                        "name": "addr",
                        "type": "address"
                    },
                    {
                        "internalType": "string",
                        "name": "peerID",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "multiaddr",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "rpc_addr",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "libp2pPubkey",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "rsaPubkey",
                        "type": "string"
                    }
                ],
                "internalType": "struct Peer[]",
                "name": "",
                "type": "tuple[]"
            }],
            "stateMutability": "view",
            "type": "function"
        }
    ]"#
);
abigen!(
    AvsGovernance,
    r#"[
        function votingPower(address _operator) external view returns (uint256)
    ]"#
);
/// Checks if a specific port is available for binding.
///
/// This function attempts to bind a TCP listener on the provided port. If the binding is successful,
/// it means the port is available. Otherwise, the port is considered unavailable.
///
/// # Arguments
///
/// * `port` - The port number to check.
///
/// # Returns
///
/// True if the port is available, False otherwise.
pub fn is_port_available(port: u16) -> bool {
    match TcpListener::bind(("0.0.0.0", port)) {
        Ok(listener) => {
            // The port is available because binding succeeded
            drop(listener); // Close the listener
            true
        }
        Err(_) => false, // Failed to bind, so the port is not available
    }
}
pub fn fetch_node_details() -> (Keypair, RsaPrivateKey, NodeType, Multiaddr) { (fetch_secp256k1_keypair(), fetch_rsa_key(), fetch_node_type(), fetch_multiaddr()) }
pub fn fetch_secp256k1_keypair() -> Keypair {
    let env = &*ENVIRONMENT;
    let seed = env.secp256k1_seed.clone();
    let seed_bytes = hex::decode(seed.clone()).unwrap();
    // tracing::info!("Seed :{:?}", seed);
    let seed_bytes: [u8; 32] = seed_bytes.try_into().expect("SECP256K1_SEED must be 32 bytes");
    let mut rng = ChaCha8Rng::from_seed(seed_bytes);
    let secret_key = secp256k1::SecretKey::new(&mut rng);
    let libp2p_secret_key = libp2p_secp256k1::SecretKey::try_from_bytes(secret_key.secret_bytes()).expect("Failed to create libp2p secret key");
    let secp256k1_keypair = libp2p_secp256k1::Keypair::from(libp2p_secret_key);
    Keypair::from(secp256k1_keypair)
}
/// TODO: Remove use of expect() and unwrap() from this function.
pub fn fetch_rsa_key() -> RsaPrivateKey {
    let seed = env::var("RSA_SEED").expect("RSA_SEED must be set");
    let seed_bytes = hex::decode(seed).unwrap();
    let seed_bytes: [u8; 32] = seed_bytes.try_into().expect("RSA_SEED must be 32 bytes");
    // From RngCore docs: "Algorithmic generators implementing SeedableRng
    // [e.g., ChaCha8Rng] should normally have portable, reproducible
    // output, i.e. fix Endianness when converting values to avoid
    // platform differences, and avoid making any changes which affect
    // output (except by communicating that the release has breaking changes)."
    // From StdRng docs: "For a secure reproducible generator, we
    // recommend use of the rand_chacha crate directly."
    let rng = ChaCha8Rng::from_seed(seed_bytes);
    RsaPrivateKey::new(&mut rng.clone(), 2048).unwrap()
}
pub fn fetch_node_type() -> NodeType {
    let node_type = env::var("NODE_TYPE").expect("NODE_TYPE must be set");
    NodeType::from_str(&node_type).unwrap()
}
pub fn fetch_multiaddr() -> Multiaddr {
    // TODO: Is there a way for a node to know its own multiaddr after starting up?
    let multiaddr = env::var("NODE_MULTIADDR").expect("NODE_MULTIADDR must be set");
    multiaddr.parse().unwrap()
}
fn parse_prover_info(idx: usize, prover: Peer) -> Result<ProverInfoWithoutStake, ()> {
    let peer_id = PeerId::from_str(prover.peer_id.as_str()).map_err(|_| info!("Failed to parse PeerId: {}", prover.peer_id))?;
    let multiaddr = Multiaddr::from_str(prover.multiaddr.as_str()).map_err(|_| info!("Failed to parse multiaddr: {}", prover.multiaddr))?;
    let peer_pub_key = hex::decode(&prover.libp_2p_pubkey).map_err(|_| info!("Failed to hex decode libp2p public key: {}", prover.libp_2p_pubkey))?;
    let peer_pub_key = PublicKey::try_decode_protobuf(&peer_pub_key).map_err(|_| info!("Failed to decode libp2p public key: {}", prover.libp_2p_pubkey))?;
    let rsa_pub_key = RSAPublicKey::from_str(prover.rsa_pubkey.as_str()).map_err(|_| info!("Failed to parse RSA public key: {}", prover.rsa_pubkey))?;
    Ok(ProverInfoWithoutStake {
        evm_address: prover.addr,
        peer_id,
        address: multiaddr,
        rpcaddr: prover.rpc_addr,
        human_pub_key: HumanPublicKey(peer_pub_key),
        rsa_pub_key,
        idx,
    })
}
pub async fn fetch_provers_v2() -> Vec<ProverInfo> {
    let peers = get_peers().await;
    // let l1_rpc = Provider::<Http>::try_from(env::var("L1_RPC").expect("L1_RPC note set")).expect("Failed to instatiate ETH provider");
    // let avs_gov_address = env::var("AVS_GOVERNANCE_ADDRESS").expect("AVS_GOVERNANCE_ADDRESS not set").parse::<H160>().unwrap();
    // let avs_gov_contract = AvsGovernance::new(avs_gov_address, Arc::new(l1_rpc.clone()));
    let mut prover_info = vec![];
    for (idx, prover) in peers.iter().enumerate() {
        // Try to parse prover info. If parsing fails, ignore this peer.
        info!("Fetched PEERS IDX :{:?}", idx);
        let result = parse_prover_info(idx, prover.clone());
        match result {
            Ok(info) => {
                let stake_result = get_prover_stake(info.clone()).await;
                match stake_result {
                    Ok(info_with_stake) => prover_info.push(info_with_stake),
                    Err(err) => {
                        tracing::error!("{}", err);
                        tracing::error!("Failed to fetch stake for prover: {:?}", info.evm_address);
                        continue;
                    }
                }
            }
            Err(_) => continue,
        }
    }
    if prover_info.is_empty() {
        panic!("No valid prover information was found. Prover info: {:?}", prover_info);
    }
    prover_info
}

pub async fn get_peers() -> Vec<Peer> {
    // TODO: Use environment crate instead of env::var
    let anvil_url = env::var("ANVIL_RPC_FOR_PEER_REGISTRY");

    info!("Anvil URL:{:?}", anvil_url);
    let rpc_url = if anvil_url.is_ok() {
        anvil_url.clone().unwrap()
    } else {
        env::var("L1_RPC").expect("L1_RPC note set")
    };
    info!("RPC url :{:?}", rpc_url);
    let rpc_for_peer_registry = Provider::<Http>::try_from(rpc_url).expect("Failed to instatiate ETH provider");
    let env = &*ENVIRONMENT;
    let address = if anvil_url.is_ok() {
        "0x5FbDB2315678afecb367f032d93F642f64180aa3"
    } else {
        env.peer_registry_address.as_str()
    };
    info!("Peer registry address :{:?}", address);
    let address = address.parse::<H160>().unwrap();
    let contract = PeerRegistry::new(address, Arc::new(rpc_for_peer_registry.clone()));
    let peers = {
        match contract.get_peers().call().await {
            Ok(peers) => peers,
            Err(e) => {
                error!("Failed to get peers from contract: {:?}", e);
                vec![]
            }
        }
    };
    peers
}
async fn get_prover_stake(prover: ProverInfoWithoutStake) -> Result<ProverInfo, ContractError<Provider<Http>>> {
    // Only include this block if local_test_net feature is enabled
    //    #[cfg(feature = "local_test_net")]
    // {
    //     let mut rng = rand::thread_rng();
    //     return Ok(ProverInfo {
    //         evm_address: prover.evm_address,
    //         peer_id: prover.peer_id,
    //         address: prover.address,
    //         rpcaddr: prover.rpcaddr,
    //         human_pub_key: prover.human_pub_key,
    //         rsa_pub_key: prover.rsa_pub_key,
    //         voting_power: U256::from(rng.gen_range(10..50)),
    //         idx: prover.idx,
    //     });
    // }
    // // Only include this block if local_test_net feature is NOT enabled
    //  #[cfg(not(feature = "local_test_net"))]
    {
        let rpc_for_stake = Provider::<Http>::try_from(env::var("L1_RPC").expect("L1_RPC note set")).expect("Failed to instatiate ETH provider");
        let contract = AvsGovernance::new(
            env::var("AVS_GOVERNANCE_ADDRESS").expect("AVS_GOVERNANCE_ADDRESS note set").parse::<H160>().unwrap(),
            Arc::new(rpc_for_stake.clone()),
        );
        let voting_power = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let voting_power = rt.block_on(async { contract.voting_power(prover.evm_address).call().await.unwrap() });
            voting_power
        };

        info!("Fetched voting power for prover: {:?} with voting power: {:?}", prover.evm_address, voting_power);
        Ok(ProverInfo {
            evm_address: prover.evm_address,
            peer_id: prover.peer_id,
            address: prover.address,
            rpcaddr: prover.rpcaddr,
            human_pub_key: prover.human_pub_key,
            rsa_pub_key: prover.rsa_pub_key,
            voting_power,
            idx: prover.idx,
        })
    }
}
pub fn generate_random_block_hash() -> [u8; 32] {
    // Generate a random 32-byte array
    let mut rng = rand::thread_rng();
    let mut random_bytes = [0u8; 32];
    rng.fill_bytes(&mut random_bytes);
    // Hash the random bytes using SHA256
    let mut hasher = Sha256::new();
    hasher.update(random_bytes);
    let hash_result = hasher.finalize();
    // Return the 32-byte hash result
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&hash_result);
    hash
}
pub fn validate_port(port_str: &str) -> u16 {
    match port_str.parse::<u16>() {
        Ok(port) => {
            // Check if the port is available
            if !is_port_available(port) {
                eprintln!("Error: Port {} is not available.", port);
                std::process::exit(1); // Exit the program with an error code
            }
            port
        }
        Err(err) => {
            eprintln!("Error parsing port string: {}", err);
            std::process::exit(1); // Exit the program with an error code
        }
    }
}
pub fn send_response<T: std::fmt::Debug>(tx: Sender<T>, response: T, error_message: &str) {
    if let Err(e) = tx.send(response) {
        error!("{:?} Failed Response: {:?}", error_message, e);
    }
}
pub fn is_dialable_address(addr: &Multiaddr) -> bool {
    for p in addr.iter() {
        if let Protocol::Ip4(ip) = p {
            if ip.is_loopback() || ip.is_unspecified() {
                return false;
            }
        }
    }
    true
}
