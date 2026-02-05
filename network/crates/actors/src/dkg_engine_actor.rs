//! This actor defines the `DKGEngineActor`, which manages all the process related to DKG and Resharing of the nodes.

/* trunk-ignore-all(rustfmt) */
// Standard library imports
use anyhow::anyhow;
use ark_ed_on_bn254::EdwardsAffine;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{self, Debug},
};
// Third-party library imports
use async_trait::async_trait;
use k256::AffinePoint;
use libp2p::PeerId;
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef, SupervisionEvent};
use rsa::{pkcs1::DecodeRsaPublicKey, RsaPrivateKey, RsaPublicKey};
use thiserror::*;
use tracing::{debug, error, info, trace, warn};
// Project-specific imports
use messages::{
    actor_type::ActorType,
    message::{
        AppStateChangeMessage, DkgEngineMessage, ElectionInfo, GossipEngineMessage, Message, PrivkeyShares, ProverInfo, ResharingRound1Output, ResharingRound1Outputs, ResharingRound2Output,
        ResharingRound2Outputs, Round1Output, Round1Outputs, Round2Output, Round2Outputs,
    },
    NETWORK_TOPIC,
};
use human_crypto::{
    dkg::{DKGError, Network, Node},
    BabyJubJub, Curve, PointTrait, ScalarTrait, Secp256k1,
};
// Local crate imports
use crate::app_state_actor::{get_state, API_TOKEN};
use crate::{app_state_actor::AppStateEngineError, cast_message, debug_key_shares, gossip_engine_actor::GossipEngineError, group_changed, quorum_context::ResharingInitContext};
#[derive(Debug, Clone, Error)]
pub enum DKGEngineError {
    #[error("Error occurred in election engine: {0}")]
    Custom(String),
}

impl Default for DKGEngineError {
    fn default() -> Self { DKGEngineError::Custom("DKGEngine unable to acquire actor".to_string()) }
}

#[derive(Clone, Debug)]
pub struct DKGEngineActor(RsaPrivateKey);
impl DKGEngineActor {
    pub fn new(rsa_private_key: RsaPrivateKey) -> Self { Self(rsa_private_key) }
}

/// A wrapper for holding the state of the DKG engine.
#[derive(Debug, Default)]
pub struct DkgStateWrapper(Option<DKGEngineState>);

/// A struct representing node data in the DKG engine.
///
/// This structure contains various inputs and public shares related to the node.
#[derive(Eq, PartialEq)]
pub struct NodeData<const N: usize, C: Curve<N>> {
    /// A map from identifiers to vectors of bytes for input Y values.
    pub input_y: HashMap<u128, Vec<u8>>,

    /// A map from identifiers to vectors of curve points for input V values.
    pub input_v: HashMap<u128, Vec<C::Point>>,

    /// A map from identifiers to public share points from other nodes.
    pub other_pubshares: HashMap<u128, C::Point>,

    /// The node itself.
    pub node: Node<N, C>,
}

/// Macro for creating a new `NodeData` instance with the given parameters.
///
/// # Arguments
///
/// * `$n` - The number of elements in the curve.
/// * `$c` - The curve type.
/// * `$rsa_private_key` - The RSA private key used to initialize the node.
/// * `$idx` - The index for the node.
///
/// # Returns
///
/// An instance of `NodeData` initialized with the specified parameters.
macro_rules! new_node {
    ($n:expr, $c: ty, $rsa_private_key:expr, $idx: expr) => {{
        let node = Node::<$n, $c>::with_rsa_key($idx, $rsa_private_key.clone()).map_err(|e| DKGEngineError::Custom(format!("Error initializing node for DKG: {:?}", e)))?;
        NodeData {
            node,
            input_y: HashMap::new(),
            input_v: HashMap::new(),
            other_pubshares: HashMap::new(),
        }
    }};
}

macro_rules! restore_node_with_share {
    ($n:expr, $c: ty, $rsa_private_key:expr, $idx: expr,$keyshare:expr,$keyshare_type:expr,$network_pubkey:expr) => {{
        let node = Node::<$n, $c>::with_rsa_key_and_keyshare($idx, $rsa_private_key.clone(), $keyshare, $network_pubkey)
            .map_err(|e| DKGEngineError::Custom(format!("Error initializing node for resharing: {:?}", e)))?;
        NodeData {
            node,
            input_y: HashMap::new(),
            input_v: HashMap::new(),
            other_pubshares: HashMap::new(),
        }
    }};
}
/// Enum representing different types of node data in the DKG engine.
///
/// This enum can encapsulate different curve types for nodes.
#[derive(Eq, PartialEq)]
pub enum NodeDataVariant {
    /// Node data for the Secp256k1 curve type.
    Secp256k1(NodeData<32, Secp256k1>),

    /// Node data for the BabyJubJub curve type.
    BabyJubJub(NodeData<32, BabyJubJub>),
}

impl NodeDataVariant {
    /// Encodes a vector of curve points into a vector of byte vectors.
    pub fn encode_v_values<C: Curve<32>>(v_values: Vec<C::Point>) -> Vec<Vec<u8>> { v_values.into_iter().map(|v| v.encode()).collect() }

    /// Converts a scalar value to its byte representation.
    pub fn own_y_value_bytes<C: Curve<32>>(own_y_value: C::Scalar) -> Vec<u8> { C::Scalar::to_bytes(&own_y_value) }

    /// Performs the first round of the DKG
    /// Stores the results and outputs them in a format that is independent of the curve type.
    ///
    /// # Parameters
    ///
    /// - `network`: A reference to the `Network` object used during the DKG process.
    ///
    /// # Returns
    ///
    /// A `Result` that contains either:
    /// - `Round1Output`: The results of the DKG Round 1 process, including encrypted values and partial shares.
    /// - `DKGError`: An error if the DKG process fails for any reason.
    pub fn dkg_round_1(&mut self, network: &Network) -> Result<Round1Output, DKGError> {
        fn process_dkg_round_1<C: Curve<32>>(node: &mut Node<32, C>, network: &Network) -> Result<Round1Output, DKGError> {
            trace!("Starting DKG Round 1 processing for curve {}", std::any::type_name::<C>());

            let (encrypted_y_values, v_values, own_y_value) = match node.dkg_round_1(network) {
                Ok(result) => result,
                Err(e) => {
                    error!("Failed to execute DKG Round 1: {:?}", e);
                    return Err(e);
                }
            };

            debug!("DKG Round 1 completed - Encrypted y_values: {}, V values: {}", encrypted_y_values.len(), v_values.len());

            // Update the node's partial share and initialization public share
            node.own_partial_share = own_y_value;
            node.init_pub_share = v_values[0].clone();

            trace!("Updated node's partial share and initialization public share");

            info!(
                "Successfully processed DKG Round 1 - Curve: {}, Encrypted Shares: {}, V Values: {}",
                std::any::type_name::<C>(),
                encrypted_y_values.len(),
                v_values.len()
            );

            Ok((
                encrypted_y_values,
                NodeDataVariant::encode_v_values::<C>(v_values),
                NodeDataVariant::own_y_value_bytes::<C>(own_y_value.into()),
            ))
        }

        debug!("Dispatching DKG Round 1 based on curve type");
        match self {
            Self::Secp256k1(node) => {
                debug!("Processing DKG Round 1 for Secp256k1 curve");
                process_dkg_round_1::<Secp256k1>(&mut node.node, network)
            }
            Self::BabyJubJub(node) => {
                debug!("Processing DKG Round 1 for BabyJubJub curve");
                process_dkg_round_1::<BabyJubJub>(&mut node.node, network)
            }
        }
    }

    /// Performs the first round of the resharing process.
    ///
    /// # Parameters
    ///
    /// - `threshold`: The new threshold value for the resharing process. It defines the minimum number of shares required to reconstruct the key.
    /// - `network`: A reference to the `Network` object used for the current resharing process. This contains information for interacting with other nodes.
    /// - `new_provers`: A `HashMap` mapping `PeerId` to a tuple of `ProverInfo` and an index. This provides information about new provers joining the resharing process.
    ///
    /// # Returns
    ///
    /// A `Result` that contains either:
    /// - `ResharingRound1Output`: The results of the resharing Round 1 process, including encrypted values and other data.
    /// - `DKGError`: An error if the resharing process fails for any reason.
    pub fn resharing_round_1(&mut self, threshold: u32, network: &Network, new_provers: &HashMap<PeerId, (ProverInfo, usize)>) -> Result<ResharingRound1Output, DKGError> {
        debug!("Starting resharing round 1 with threshold: {}", threshold);

        let new_network = match quorum_network(threshold, new_provers, false) {
            Ok(net) => {
                debug!("Successfully created new network with {} provers", new_provers.len());
                net
            }
            Err(e) => {
                error!("Failed to create quorum network: {:?}", e);
                return Err(e.into());
            }
        };

        fn process_resharing_round_1<C: Curve<32>>(node: &mut Node<32, C>, network: &Network, new_network: &Network) -> Result<ResharingRound1Output, DKGError> {
            trace!("Processing resharing round 1 for curve {}", std::any::type_name::<C>());

            let (encrypted_y_values, v_values) = match node.resharing_round_1(network, new_network) {
                Ok(result) => result,
                Err(e) => {
                    error!("Resharing round 1 failed: {:?}", e);
                    return Err(e.into());
                }
            };

            info!(
                "Resharing Round 1 completed - Encrypted y_values for peers: {:?}, V values count: {}",
                encrypted_y_values.keys().collect::<Vec<_>>(),
                v_values.len()
            );

            debug!(
                "Resharing Round 1 results - Curve: {}, Encrypted shares: {}, V values: {}",
                std::any::type_name::<C>(),
                encrypted_y_values.len(),
                v_values.len()
            );

            Ok((encrypted_y_values, NodeDataVariant::encode_v_values::<C>(v_values)))
        }

        debug!("Dispatching resharing round 1 based on curve type");
        match self {
            Self::Secp256k1(node) => {
                debug!("Processing resharing for Secp256k1 curve");
                process_resharing_round_1(&mut node.node, network, &new_network)
            }
            Self::BabyJubJub(node) => {
                debug!("Processing resharing for BabyJubJub curve");
                process_resharing_round_1(&mut node.node, network, &new_network)
            }
        }
    }

    /// Registers the round 1 output received from another node in the DKG protocol.
    /// This function updates the current node's state with the received output.
    ///
    /// # Parameters
    ///
    /// - `my_node_id`: The identifier of the current node.
    /// - `other_node_idx`: The identifier of the other node from which the round 1 output is received.
    /// - `round_1_output`: The round 1 output data received from the other node.
    ///
    pub fn register_round_1_result_from_other_node(&mut self, my_node_id: u128, other_node_idx: u128, round_1_output: Round1Output) -> Result<(), Box<dyn Error>> {
        debug!("Registering Round 1 results from node {} for our node {}", other_node_idx, my_node_id);

        fn process_round_1<C: Curve<32>>(
            input_y: &mut HashMap<u128, Vec<u8>>,
            input_v: &mut HashMap<u128, Vec<C::Point>>,
            my_node_id: u128,
            other_node_idx: u128,
            round_1_output: &Round1Output,
        ) -> Result<(), Box<dyn Error>> {
            trace!("Processing Round 1 results for curve {}", std::any::type_name::<C>());

            // Decode the `v` values from the received round 1 output
            let v_values: Result<Vec<C::Point>, _> = round_1_output
                .1
                .iter()
                .map(|x| {
                    C::Point::from_encoded(x).map_err(|e| {
                        error!("Failed to decode point from node {}: {:?}", other_node_idx, e);
                        Box::<dyn Error>::from("Error decoding points")
                    })
                })
                .collect();

            let v_values = match v_values {
                Ok(values) => {
                    trace!("Successfully decoded {} V values from node {}", values.len(), other_node_idx);
                    values
                }
                Err(e) => {
                    error!("Failed to decode V values from node {}: {:?}", other_node_idx, e);
                    return Err(e);
                }
            };

            let y_value = match round_1_output.0.get(&my_node_id) {
                Some(y) => {
                    trace!("Found Y value for our node {} from node {}", my_node_id, other_node_idx);
                    y.to_vec()
                }
                None => {
                    error!("No Y value found for our node {} in output from node {}", my_node_id, other_node_idx);
                    return Err("Error finding round 1 output for node".into());
                }
            };

            input_y.insert(other_node_idx, y_value);
            input_v.insert(other_node_idx, v_values.clone());

            debug!("Successfully registered Round 1 results from node {} - Y values: 1, V values: {}", other_node_idx, v_values.len());

            Ok(())
        }

        match self {
            Self::Secp256k1(node) => {
                debug!("Processing with Secp256k1 curve");
                process_round_1::<Secp256k1>(&mut node.input_y, &mut node.input_v, my_node_id, other_node_idx, &round_1_output)
            }
            Self::BabyJubJub(node) => {
                debug!("Processing with BabyJubJub curve");
                process_round_1::<BabyJubJub>(&mut node.input_y, &mut node.input_v, my_node_id, other_node_idx, &round_1_output)
            }
        }
    }

    /// Registers the resharing round 1 output received from another node .
    /// This function updates the current node's state with the received resharing output.
    ///
    /// # Parameters
    ///
    /// - `my_node_id`: The identifier of the current node.
    /// - `other_node_idx`: The identifier of the other node from which the resharing round 1 output is received.
    /// - `resharing_round_1_output`: The resharing round 1 output data received from the other node.
    pub fn register_resharing_round_1_result_from_other_node(&mut self, my_node_id: u128, other_node_idx: u128, resharing_round_1_output: ResharingRound1Output) -> Result<(), Box<dyn Error>> {
        debug!("Registering resharing Round 1 results from node {} for our node {}", other_node_idx, my_node_id);

        fn process_resharing_round_1<C: Curve<32>>(
            input_y: &mut HashMap<u128, Vec<u8>>,
            input_v: &mut HashMap<u128, Vec<C::Point>>,
            my_node_id: u128,
            other_node_idx: u128,
            resharing_round_1_output: &ResharingRound1Output,
        ) -> Result<(), Box<dyn Error>> {
            trace!("Processing resharing Round 1 results for curve {}", std::any::type_name::<C>());

            // Decode the `v` values from the received round 1 output
            let v_values: Result<Vec<C::Point>, _> = resharing_round_1_output
                .1
                .iter()
                .map(|x| {
                    C::Point::from_encoded(x).map_err(|e| {
                        error!("Failed to decode resharing point from node {}: {:?}", other_node_idx, e);
                        Box::<dyn Error>::from("Error decoding points")
                    })
                })
                .collect();

            let v_values = match v_values {
                Ok(values) => {
                    debug!("Successfully decoded {} V values from node {}", values.len(), other_node_idx);
                    values
                }
                Err(e) => {
                    error!("Failed to decode resharing V values from node {}: {:?}", other_node_idx, e);
                    return Err(e);
                }
            };

            let y_value = match resharing_round_1_output.0.get(&my_node_id) {
                Some(y) => {
                    trace!("Found resharing Y value for our node {} from node {}", my_node_id, other_node_idx);
                    y.clone()
                }
                None => {
                    error!("No resharing Y value found for our node {} in output from node {}", my_node_id, other_node_idx);
                    return Err(anyhow!("Error finding resharing round 1 output for node from node {}, my node id {}", other_node_idx, my_node_id).into());
                }
            };

            input_y.insert(other_node_idx, y_value);
            input_v.insert(other_node_idx, v_values.clone());

            info!(
                "Successfully registered resharing Round 1 results from node {} - Y values: 1, V values: {}",
                other_node_idx,
                v_values.len()
            );

            Ok(())
        }

        match self {
            Self::Secp256k1(node) => {
                debug!("Processing resharing with Secp256k1 curve");
                process_resharing_round_1::<Secp256k1>(&mut node.input_y, &mut node.input_v, my_node_id, other_node_idx, &resharing_round_1_output)
            }
            Self::BabyJubJub(node) => {
                debug!("Processing resharing with BabyJubJub curve");
                process_resharing_round_1::<BabyJubJub>(&mut node.input_y, &mut node.input_v, my_node_id, other_node_idx, &resharing_round_1_output)
            }
        }
    }
    /// Executes the second round of the DKG.
    /// This function handles the DKG round 2 process, generating private and public shares for the current node.
    ///
    /// # Parameters
    ///
    /// - `network`: The network context in which the DKG is being executed (e.g., Bitcoin, Ethereum).
    ///
    /// # Returns
    ///
    /// A `Result` that is:
    /// - `Ok(Round2Output)`: If the round 2 process is successful, returning the encoded public share.
    /// - `Err(DKGError)`: If an error occurs during the DKG round 2 process.
    pub fn dkg_round_2(&mut self, network: Network) -> Result<Round2Output, DKGError> {
        debug!("Starting DKG Round 2 processing");

        match self {
            Self::Secp256k1(node) => {
                debug!("Processing DKG Round 2 with Secp256k1 curve");
                let (priv_share, pub_share, network_pubkey) = match node.node.dkg_round_2(&network, node.input_y.clone(), node.input_v.clone()) {
                    Ok(result) => {
                        trace!("Successfully executed DKG Round 2 core operation");
                        result
                    }
                    Err(e) => {
                        error!("DKG Round 2 failed for Secp256k1: {:?}", e);
                        return Err(e);
                    }
                };

                // Store the private share and network public key in the node's state.
                node.node.private_share = priv_share;
                node.node.network_pubkey = network_pubkey;
                node.other_pubshares.insert(node.node.id, pub_share.clone());

                debug!("Stored Round 2 results - Node ID: {}, Private share: [redacted], Network pubkey: {:?}", node.node.id, network_pubkey);

                Ok(pub_share.encode())
            }
            Self::BabyJubJub(node) => {
                debug!("Processing DKG Round 2 with BabyJubJub curve");
                let (priv_share, pub_share, network_pubkey) = match node.node.dkg_round_2(&network, node.input_y.clone(), node.input_v.clone()) {
                    Ok(result) => {
                        trace!("Successfully executed DKG Round 2 core operation");
                        result
                    }
                    Err(e) => {
                        error!("DKG Round 2 failed for BabyJubJub: {:?}", e);
                        return Err(e);
                    }
                };

                // Store the private share and network public key in the node's state.
                node.node.private_share = priv_share;
                node.node.network_pubkey = network_pubkey;
                node.other_pubshares.insert(node.node.id, pub_share.clone());

                debug!("Stored Round 2 results - Node ID: {}, Private share: [redacted], Network pubkey: {:?}", node.node.id, network_pubkey);
                Ok(pub_share.encode())
            }
        }
    }

    /// Executes the second round of the resharing process .
    /// This function handles the resharing round 2 process, generating private and public shares for the current node
    /// in the context of a new network.
    ///
    /// # Parameters
    ///
    /// - `tstar_network`: The original network context in which the resharing is being executed.
    /// - `new_network`: The new network context for the resharing process.
    /// - `k_dkg`: A vector of bytes representing the encoded group public key.
    ///
    /// # Returns
    ///
    /// A `Result` that is:
    /// - `Ok(ResharingRound2Output)`: If the resharing round 2 process is successful, returning the encoded public share.
    /// - `Err(DKGError)`: If an error occurs during the resharing round 2 process.
    ///
    pub fn resharing_round_2(&mut self, tstar_network: Network, new_network: Network, k_dkg: Vec<u8>) -> Result<ResharingRound2Output, DKGError> {
        debug!("Starting resharing Round 2 processing");

        match self {
            Self::Secp256k1(node) => {
                debug!("Processing resharing Round 2 with Secp256k1 curve");

                // Convert k_dkg to affine point
                let k_dkg = match AffinePoint::from_encoded(&k_dkg) {
                    Ok(point) => {
                        trace!("Successfully decoded Secp256k1 group public key");
                        point
                    }
                    Err(e) => {
                        error!("Failed to decode Secp256k1 group public key: {}", e);
                        return Err(DKGError::Other(format!("Error encoding group public key to affine point: {}", e)));
                    }
                };

                // Execute resharing round 2
                let (priv_share, pub_share, network_pubkey) = match node.node.resharing_round_2(&tstar_network, &new_network, node.input_y.clone(), node.input_v.clone(), k_dkg) {
                    Ok(result) => {
                        debug!("Successfully executed Secp256k1 resharing Round 2 core operation");
                        result
                    }
                    Err(e) => {
                        error!("Secp256k1 resharing Round 2 failed: {:?}", e);
                        return Err(e.into());
                    }
                };

                // Update node state
                node.node.private_share = priv_share;
                node.node.network_pubkey = network_pubkey;
                node.other_pubshares.insert(node.node.id, pub_share.clone());

                trace!("Stored Secp256k1 resharing results - Node ID: {}, Network pubkey: {:?}", node.node.id, network_pubkey);

                Ok(pub_share.encode())
            }
            Self::BabyJubJub(node) => {
                debug!("Processing resharing Round 2 with BabyJubJub curve");

                // Convert k_dkg to affine point
                let k_dkg = match EdwardsAffine::from_encoded(&k_dkg) {
                    Ok(point) => {
                        trace!("Successfully decoded BabyJubJub group public key");
                        point
                    }
                    Err(e) => {
                        error!("Failed to decode BabyJubJub group public key: {}", e);
                        return Err(DKGError::Other(format!("Error encoding group public key to edwards affine point: {}", e)));
                    }
                };

                // Execute resharing round 2
                let (priv_share, pub_share, network_pubkey) = match node.node.resharing_round_2(&tstar_network, &new_network, node.input_y.clone(), node.input_v.clone(), k_dkg) {
                    Ok(result) => {
                        debug!("Successfully executed BabyJubJub resharing Round 2 core operation");
                        result
                    }
                    Err(e) => {
                        error!("BabyJubJub resharing Round 2 failed: {:?}", e);
                        return Err(e.into());
                    }
                };

                // Update node state
                node.node.private_share = priv_share;
                node.node.network_pubkey = network_pubkey;
                node.other_pubshares.insert(node.node.id, pub_share.clone());

                trace!("Stored BabyJubJub resharing results - Node ID: {}, Network pubkey: {:?}", node.node.id, network_pubkey);

                Ok(pub_share.encode())
            }
        }
    }

    /// Processes the third round of the DKG or resharing.
    /// This function determines whether to execute the third round of DKG or resharing based on the provided flags.
    pub fn process_round_3<C>(node: &Node<32, C>, network: &Network, other_pubshares: &HashMap<u128, C::Point>, is_resharing: bool, is_elected: bool) -> Result<(), DKGError>
    where
        C: Curve<32>,
    {
        debug!("Starting Round 3 processing (resharing: {}, elected: {})", is_resharing, is_elected);

        let result = if is_resharing && is_elected {
            trace!("Executing resharing Round 3 for elected node");
            Ok(node.resharing_round_3(network, other_pubshares)?)
        } else {
            trace!("Executing standard DKG Round 3");
            node.dkg_round_3(network, other_pubshares)
        };

        match result {
            Ok(_) => {
                info!("Successfully completed {} Round 3", if is_resharing { "resharing" } else { "DKG" });
                Ok(())
            }
            Err(e) => {
                error!("Failed to complete {} Round 3: {:?}", if is_resharing { "resharing" } else { "DKG" }, e);
                Err(e.into())
            }
        }
    }

    /// Note: this is intended only to be called with the other nodes', not my own node's, public shares
    pub fn store_pubshare_for_node(&mut self, idx: u128, pubshare: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("Storing public share for node {}", idx);

        match self {
            Self::Secp256k1(node) => {
                let point = match AffinePoint::from_encoded(pubshare) {
                    Ok(p) => {
                        trace!("Successfully decoded Secp256k1 public share for node {}", idx);
                        p
                    }
                    Err(e) => {
                        error!("Failed to decode Secp256k1 public share for node {}: {}", idx, e);
                        return Err(e.into());
                    }
                };

                node.other_pubshares.insert(idx, point);

                if node.other_pubshares.len() > 4 {
                    info!("Collected {} Secp256k1 public shares (Node {} added)", node.other_pubshares.len(), idx);
                } else {
                    debug!("Stored Secp256k1 public share for node {} (Total: {})", idx, node.other_pubshares.len());
                }

                Ok(())
            }
            Self::BabyJubJub(node) => {
                let point = match EdwardsAffine::from_encoded(pubshare) {
                    Ok(p) => {
                        trace!("Successfully decoded BabyJubJub public share for node {}", idx);
                        p
                    }
                    Err(e) => {
                        error!("Failed to decode BabyJubJub public share for node {}: {}", idx, e);
                        return Err(e.into());
                    }
                };

                node.other_pubshares.insert(idx, point);

                if node.other_pubshares.len() > 4 {
                    info!("Collected {} BabyJubJub public shares (Node {} added)", node.other_pubshares.len(), idx);
                } else {
                    debug!("Stored BabyJubJub public share for node {} (Total: {})", idx, node.other_pubshares.len());
                }

                Ok(())
            }
        }
    }

    pub fn get_priv_share(&self) -> Vec<u8> {
        trace!("Retrieving private share");
        match self {
            Self::Secp256k1(node) => {
                trace!("Converting Secp256k1 private share to bytes");
                ScalarTrait::to_bytes(&<Secp256k1 as Curve<32>>::Scalar::from(node.node.private_share))
            }
            Self::BabyJubJub(node) => {
                trace!("Converting BabyJubJub private share to bytes");
                <BabyJubJub as Curve<32>>::Scalar::from(node.node.private_share).to_bytes()
            }
        }
    }

    pub fn get_pub_share(&self) -> Vec<u8> {
        trace!("Calculating public share from private share");
        match self {
            Self::Secp256k1(node) => {
                trace!("Computing Secp256k1 public share");
                Secp256k1::base_point_or_generator().scalar_mul(&node.node.private_share.into()).encode()
            }
            Self::BabyJubJub(node) => {
                trace!("Computing BabyJubJub public share");
                BabyJubJub::base_point_or_generator().scalar_mul(&node.node.private_share.into()).encode()
            }
        }
    }

    pub fn get_no_of_inputs(&self) -> (usize, usize) {
        debug!("Getting number of stored inputs");
        let counts = match self {
            Self::Secp256k1(node) => (node.input_v.len(), node.input_y.len()),
            Self::BabyJubJub(node) => (node.input_v.len(), node.input_y.len()),
        };
        debug!("Input counts - V: {}, Y: {}", counts.0, counts.1);
        counts
    }

    pub fn clear_inputs(&mut self) {
        info!("Clearing all stored inputs");
        match self {
            Self::Secp256k1(node) => {
                let v_count = node.input_v.len();
                let y_count = node.input_y.len();
                let shares_count = node.other_pubshares.len();

                node.input_y.clear();
                node.input_v.clear();
                node.other_pubshares.clear();

                debug!("Cleared Secp256k1 inputs - V: {}, Y: {}, PubShares: {}", v_count, y_count, shares_count);
            }
            Self::BabyJubJub(node) => {
                let v_count = node.input_v.len();
                let y_count = node.input_y.len();
                let shares_count = node.other_pubshares.len();

                node.input_y.clear();
                node.input_v.clear();
                node.other_pubshares.clear();

                debug!("Cleared BabyJubJub inputs - V: {}, Y: {}, PubShares: {}", v_count, y_count, shares_count);
            }
        }
    }

    pub fn get_received_y_v_from(&self) -> HashSet<u128> {
        debug!("Getting set of node IDs we've received Y/V inputs from");
        let mut received_y_idxes = HashSet::new();

        match self {
            Self::Secp256k1(node) => {
                let v_keys: Vec<u128> = node.input_v.keys().cloned().collect();
                let y_keys: Vec<u128> = node.input_y.keys().cloned().collect();
                received_y_idxes.extend(v_keys);
                received_y_idxes.extend(y_keys);

                trace!("Secp256k1 - Found {} V inputs and {} Y inputs", node.input_v.len(), node.input_y.len());
            }
            Self::BabyJubJub(node) => {
                let v_keys: Vec<u128> = node.input_v.keys().cloned().collect();
                let y_keys: Vec<u128> = node.input_y.keys().cloned().collect();
                received_y_idxes.extend(v_keys);
                received_y_idxes.extend(y_keys);

                trace!("BabyJubJub - Found {} V inputs and {} Y inputs", node.input_v.len(), node.input_y.len());
            }
        }

        debug!("Total unique nodes we've received from: {}", received_y_idxes.len());
        received_y_idxes
    }

    pub fn get_no_of_other_pub_shares(&self) -> usize {
        trace!("Getting number of other public shares");
        let count = match self {
            Self::Secp256k1(node) => node.other_pubshares.len(),
            Self::BabyJubJub(node) => node.other_pubshares.len(),
        };
        debug!("Found {} other public shares", count);
        count
    }

    pub fn pub_shares_received_from(&self) -> HashSet<u128> {
        debug!("Collecting indices of received public shares");
        let mut received_shares_indices = HashSet::new();

        match self {
            Self::Secp256k1(node) => {
                trace!("Processing Secp256k1 public shares");
                let keys: Vec<u128> = node.other_pubshares.keys().cloned().collect();
                received_shares_indices.extend(keys.clone());
                debug!("Added {} Secp256k1 public share indices", keys.len());
            }
            Self::BabyJubJub(node) => {
                trace!("Processing BabyJubJub public shares");
                let keys: Vec<u128> = node.other_pubshares.keys().cloned().collect();
                received_shares_indices.extend(keys.clone());
                debug!("Added {} BabyJubJub public share indices", keys.len());
            }
        }

        if received_shares_indices.is_empty() {
            warn!("No public shares received from any participant");
        } else {
            debug!("Total unique public share indices collected: {}", received_shares_indices.len());
        }

        received_shares_indices
    }
    pub fn get_node_type(&self) -> String {
        match self {
            Self::Secp256k1(_) => "Secp256k1".to_string(),
            Self::BabyJubJub(_) => "BabyJubJub".to_string(),
        }
    }
}

fn quorum_network(threshold: u32, provers: &HashMap<PeerId, (ProverInfo, usize)>, is_tstar: bool) -> Result<Network, DKGError> {
    debug!("Initializing new quorum network with threshold: {}, {} provers, is_tstar: {}", threshold, provers.len(), is_tstar);

    // Initialize network
    let mut new_network = Network::new(threshold as usize, provers.len(), is_tstar).map_err(|e| {
        error!("Failed to initialize network for resharing: {:?}", e);
        DKGError::Other(format!("Error initializing network for Resharing: {:?}", e))
    })?;

    trace!("Network structure initialized successfully");

    // Parse RSA keys
    let rsa_keys_map = DKGEngineState::parse_rsa_keys(provers).map_err(|e| {
        error!("Failed to parse RSA public keys for resharing: {}", e);
        DKGError::Other(format!("Error occurred while parsing pub keys for resharing, details: {}", e))
    })?;

    debug!("Successfully parsed {} RSA public keys", rsa_keys_map.len());

    new_network.public_keys = rsa_keys_map;
    trace!("Assigned public keys to network");

    info!("Successfully created quorum network with {} participants and threshold {}", provers.len(), threshold);

    Ok(new_network)
}

/// The `DKGEngineState` struct represents the state of a distributed key generation engine State.
#[derive(Eq, PartialEq)]
pub struct DKGEngineState {
    pub idx: u128,
    pub peer_id: PeerId,
    pub network: Network,
    pub dkg_nodes: HashMap<String, NodeDataVariant>,
    pub tstar_provers: HashMap<PeerId, (ProverInfo, usize)>,
    pub new_provers: HashMap<PeerId, (ProverInfo, usize)>,
    pub election_info: ElectionInfo,
    pub finalized_group_public_keys: HashMap<String, Vec<u8>>,
    pub resharing_round2_triggered: bool,
    pub resharing_round1_context: Option<ResharingInitContext>,
}

impl fmt::Debug for DKGEngineState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DKGEngineState")
            .field("threshold", &self.network.threshold)
            .field("size", &self.network.total_nodes)
            .finish()
    }
}

impl DKGEngineState {
    #[tracing::instrument(name = "Init DKG", skip(rsa_private_key, elected_peers))]
    pub async fn dkg_init(peer_id: PeerId, idx: u128, rsa_private_key: RsaPrivateKey, elected_peers: HashMap<PeerId, (ProverInfo, usize)>, threshold: u32) -> Result<Self, DKGEngineError> {
        info!("Starting DKG initialization for peer {} with index {}", peer_id, idx);
        debug!("Creating DKG nodes with threshold {} and {} elected peers", threshold, elected_peers.len());

        // Initialize DKG nodes
        let mut dkg_nodes = HashMap::new();
        trace!("Creating Secp256k1 OPRF node");
        dkg_nodes.insert("OPRFSecp256k1".to_string(), NodeDataVariant::Secp256k1(new_node!(32, Secp256k1, rsa_private_key, idx)));

        trace!("Creating BabyJubJub OPRF node");
        dkg_nodes.insert("OPRFBabyJubJub".to_string(), NodeDataVariant::BabyJubJub(new_node!(32, BabyJubJub, rsa_private_key, idx)));

        trace!("Creating Secp256k1 JWT-PRF node");
        dkg_nodes.insert("JWTPRFSecp256k1".to_string(), NodeDataVariant::Secp256k1(new_node!(32, Secp256k1, rsa_private_key, idx)));

        trace!("Creating BabyJubJub Decrypt node");
        dkg_nodes.insert("DecryptBabyJubjub".to_string(), NodeDataVariant::BabyJubJub(new_node!(32, BabyJubJub, rsa_private_key, idx)));

        debug!("Successfully created {} DKG node variants", dkg_nodes.len());

        // Initialize network
        debug!("Creating network with threshold {} and {} peers", threshold, elected_peers.len());
        let mut network = Network::new(threshold as usize, elected_peers.len(), false).map_err(|e| {
            error!("Failed to initialize network for DKG: {:?}", e);
            DKGEngineError::Custom(format!("Error initializing network for DKG: {:?}", e))
        })?;

        info!("Network successfully instantiated with threshold {} and {} peers", threshold, elected_peers.len());
        trace!("Network details: {:#?}", network);

        // Parse and set RSA keys
        debug!("Parsing RSA keys for {} peers", elected_peers.len());
        let rsa_keys_map = Self::parse_rsa_keys(&elected_peers).map_err(|e| {
            error!("Failed to parse RSA keys: {:?}", e);
            e
        })?;

        trace!("Successfully parsed {} RSA public keys", rsa_keys_map.len());
        network.public_keys = rsa_keys_map;
        debug!("Assigned RSA public keys to network");

        // Create and return DKG engine instance
        let engine = Self {
            idx,
            peer_id,
            network,
            dkg_nodes,
            tstar_provers: HashMap::new(),
            new_provers: elected_peers,
            election_info: ElectionInfo::new(0, 0, 0),
            finalized_group_public_keys: HashMap::new(),
            resharing_round2_triggered: false,
            resharing_round1_context: None,
        };

        info!("DKG initialization completed successfully for peer {}", peer_id);
        debug!("DKG engine initialized with {} nodes and threshold {}", engine.dkg_nodes.len(), threshold);

        Ok(engine)
    }

    pub async fn restore_old_resharing_state(
        peer_id: PeerId,
        idx: u128,
        rsa_private_key: RsaPrivateKey,
        elected_peers: HashMap<PeerId, (ProverInfo, usize)>,
        threshold: u32,
        private_keyshares: HashMap<String, Vec<u8>>,
        tstar_provers: HashMap<PeerId, (ProverInfo, usize)>,
        group_keys: HashMap<String, Vec<u8>>,
    ) -> Result<Self, DKGEngineError> {
        debug!("Restoring old resharing state for peer {} with index {}", peer_id, idx);

        let mut dkg_nodes = HashMap::new();

        trace!("Retrieving private key share for OPRFSecp256k1");
        let oprf_secp256k1_share = private_keyshares
            .get("OPRFSecp256k1")
            .ok_or_else(|| {
                error!("Missing private key share for OPRFSecp256k1");
                DKGEngineError::Custom("Missing private key share for OPRFSecp256k1".to_string())
            })?
            .clone();

        trace!("Retrieving private key share for OPRFBabyJubJub");
        let oprf_babyjubjub_share = private_keyshares
            .get("OPRFBabyJubJub")
            .ok_or_else(|| {
                error!("Missing private key share for OPRFBabyJubJub");
                DKGEngineError::Custom("Missing private key share for OPRFBabyJubJub".to_string())
            })?
            .clone();

        trace!("Retrieving private key share for JWTPRFSecp256k1");
        let jwtprf_secp256k1_share = private_keyshares
            .get("JWTPRFSecp256k1")
            .ok_or_else(|| {
                error!("Missing private key share for JWTPRFSecp256k1");
                DKGEngineError::Custom("Missing private key share for JWTPRFSecp256k1".to_string())
            })?
            .clone();

        trace!("Retrieving private key share for DecryptBabyJubjub");
        let decrypt_babyjubjub_share = private_keyshares
            .get("DecryptBabyJubjub")
            .ok_or_else(|| {
                error!("Missing private key share for DecryptBabyJubjub");
                DKGEngineError::Custom("Missing private key share for DecryptBabyJubjub".to_string())
            })?
            .clone();

        trace!("Retrieving group key for OPRFSecp256k1");
        let oprf_secp256k1_network_pubkey = group_keys
            .get("OPRFSecp256k1")
            .ok_or_else(|| {
                error!("Missing group key for OPRFSecp256k1");
                DKGEngineError::Custom("Missing group key for OPRFSecp256k1".to_string())
            })?
            .clone();

        trace!("Retrieving group key for OPRFBabyJubJub");
        let oprf_babyjubjub_network_pubkey = group_keys
            .get("OPRFBabyJubJub")
            .ok_or_else(|| {
                error!("Missing group key for OPRFBabyJubJub");
                DKGEngineError::Custom("Missing group key for OPRFBabyJubJub".to_string())
            })?
            .clone();

        trace!("Retrieving group key for JWTPRFSecp256k1");
        let jwtprf_secp256k1_network_pubkey = group_keys
            .get("JWTPRFSecp256k1")
            .ok_or_else(|| {
                error!("Missing group key for JWTPRFSecp256k1");
                DKGEngineError::Custom("Missing group key for JWTPRFSecp256k1".to_string())
            })?
            .clone();

        trace!("Retrieving group key for DecryptBabyJubjub");
        let decrypt_babyjubjub_network_pubkey = group_keys
            .get("DecryptBabyJubjub")
            .ok_or_else(|| {
                error!("Missing group key for DecryptBabyJubjub");
                DKGEngineError::Custom("Missing group key for DecryptBabyJubjub".to_string())
            })?
            .clone();

        trace!("Inserting DKG node for OPRFSecp256k1");
        dkg_nodes.insert(
            "OPRFSecp256k1".to_string(),
            NodeDataVariant::Secp256k1(restore_node_with_share!(
                32,
                Secp256k1,
                rsa_private_key,
                idx,
                oprf_secp256k1_share,
                "Secp256k1",
                oprf_secp256k1_network_pubkey
            )),
        );

        trace!("Inserting DKG node for OPRFBabyJubJub");
        dkg_nodes.insert(
            "OPRFBabyJubJub".to_string(),
            NodeDataVariant::BabyJubJub(restore_node_with_share!(
                32,
                BabyJubJub,
                rsa_private_key,
                idx,
                oprf_babyjubjub_share,
                "BabyJubJub",
                oprf_babyjubjub_network_pubkey
            )),
        );

        trace!("Inserting DKG node for JWTPRFSecp256k1");
        dkg_nodes.insert(
            "JWTPRFSecp256k1".to_string(),
            NodeDataVariant::Secp256k1(restore_node_with_share!(
                32,
                Secp256k1,
                rsa_private_key,
                idx,
                jwtprf_secp256k1_share,
                "Secp256k1",
                jwtprf_secp256k1_network_pubkey
            )),
        );

        trace!("Inserting DKG node for DecryptBabyJubjub");
        dkg_nodes.insert(
            "DecryptBabyJubjub".to_string(),
            NodeDataVariant::BabyJubJub(restore_node_with_share!(
                32,
                BabyJubJub,
                rsa_private_key,
                idx,
                decrypt_babyjubjub_share,
                "BabyJubJub",
                decrypt_babyjubjub_network_pubkey
            )),
        );

        debug!("Initializing network with threshold {} and {} elected peers", threshold, elected_peers.len());
        let mut network = Network::new(threshold as usize, elected_peers.len(), false).map_err(|e| {
            error!("Failed to initialize network for DKG: {:?}", e);
            DKGEngineError::Custom(format!("Failed to initialize network for DKG: {:?}", e))
        })?;

        debug!("Network initialized for DKG with threshold {} and {} peers", network.threshold, network.total_nodes);

        trace!("Parsing RSA public keys for elected peers");
        let rsa_keys_map = Self::parse_rsa_keys(&elected_peers)?;
        network.public_keys = rsa_keys_map;

        debug!("Restored resharing state with {} DKG nodes", dkg_nodes.len());

        Ok(Self {
            idx,
            peer_id,
            network,
            dkg_nodes,
            tstar_provers,
            new_provers: elected_peers.clone(),
            election_info: ElectionInfo::new(threshold, elected_peers.len() as u32, 1),
            finalized_group_public_keys: group_keys,
            resharing_round2_triggered: false,
            resharing_round1_context: None,
        })
    }

    fn parse_rsa_keys(elected_peers: &HashMap<PeerId, (ProverInfo, usize)>) -> Result<HashMap<u128, RsaPublicKey>, DKGEngineError> {
        elected_peers
            .iter()
            .map(|(peer_id, (prover, idx))| {
                trace!("Parsing RSA public key for peer {} with index {}", peer_id, idx);
                let clean_rsa_pub_key = prover.rsa_pub_key.replace("\\r", "\n");
                RsaPublicKey::from_pkcs1_pem(&clean_rsa_pub_key).map(|key| (*idx as u128, key)).map_err(|e| {
                    error!("Failed to parse RSA public key for peer {}: {:?}", peer_id, e);
                    DKGEngineError::Custom(format!("Failed to parse RSA public key for peer {}: {:?}", peer_id, e))
                })
            })
            .collect()
    }

    fn run_round<F, T>(&mut self, output_fn: F, election_info: Option<ElectionInfo>) -> Result<HashMap<String, T>, Box<dyn Error>>
    where
        F: Fn(&mut NodeDataVariant, &Network, Option<&HashMap<PeerId, (ProverInfo, usize)>>) -> Result<T, DKGError>,
    {
        let mut round_results = HashMap::new();
        for (key, node) in self.dkg_nodes.iter_mut() {
            let round_type = if election_info.is_some() { "resharing" } else { "DKG Round 1" };
            debug!("Running {} for node {}", round_type, key);
            let output = if election_info.is_some() {
                output_fn(node, &self.network, Some(&self.new_provers.clone()))?
            } else {
                output_fn(node, &self.network, None)?
            };
            trace!("Completed {} for node {} with output", round_type, key);
            round_results.insert(key.clone(), output);
        }
        let round_type = if election_info.is_some() { "resharing" } else { "DKG Round 1" };
        debug!("Completed {} round for {} nodes", round_type, round_results.len());
        Ok(round_results)
    }

    fn round1(&mut self) -> Result<HashMap<String, Round1Output>, Box<dyn Error>> {
        debug!("Starting DKG Round 1 for {} nodes", self.dkg_nodes.len());
        let result = self.run_round(
            |node, network, _| {
                trace!("Executing DKG Round 1 for node {:?}", node.get_node_type());
                let output = node.dkg_round_1(network)?;
                trace!("Completed DKG Round 1 for node {:?}", node.get_node_type());
                Ok(output)
            },
            None,
        );
        match &result {
            Ok(outputs) => debug!("Completed DKG Round 1 with {} node outputs", outputs.len()),
            Err(e) => error!("Failed DKG Round 1: {:?}", e),
        }
        result
    }

    fn resharing_round1(&mut self, election_info: ElectionInfo) -> Result<HashMap<String, ResharingRound1Output>, Box<dyn Error>> {
        debug!("Starting resharing Round 1 with threshold {} and {} tstar provers", election_info.threshold, self.tstar_provers.len());

        trace!("Initializing tstar network for resharing");
        let tstar_network = quorum_network(election_info.threshold, &self.tstar_provers.clone(), true).map_err(|e| {
            error!("Failed to initialize tstar network for resharing: {:?}", e);
            Box::<dyn Error>::from(e)
        })?;

        let result = self.run_round(
            |node, _, new_provers| {
                trace!("Executing resharing Round 1 for node {:?}", node.get_node_type());
                match new_provers {
                    Some(provers) => {
                        let output = node.resharing_round_1(election_info.threshold, &tstar_network, provers)?;
                        trace!("Completed resharing Round 1 for node {:?}", node.get_node_type());
                        Ok(output)
                    }
                    None => {
                        error!("Re-elected provers set is empty for node {:?}", node.get_node_type());
                        Err(DKGError::Other(String::from("Re-elected provers set is empty")))
                    }
                }
            },
            Some(election_info.clone()),
        );

        match &result {
            Ok(outputs) => debug!("Completed resharing Round 1 with {} node outputs", outputs.len()),
            Err(e) => error!("Failed resharing Round 1: {:?}", e),
        }
        result
    }
}
#[async_trait]
impl Actor for DKGEngineActor {
    type Msg = DkgEngineMessage;
    type State = DkgStateWrapper;
    type Arguments = ();
    async fn pre_start(&self, _myself: ActorRef<Self::Msg>, _args: Self::Arguments) -> Result<Self::State, ActorProcessingErr> { Ok(DkgStateWrapper::default()) }
    //::instrument(name = "Handle DKG Event", skip(self, myself, state, message))]
    async fn handle(&self, myself: ActorRef<Self::Msg>, message: Self::Msg, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match message {
            DkgEngineMessage::Init(idx, peer_id, peers, threshold) => handle_dkg_init(self, state, idx, peer_id, peers, threshold).await,
            DkgEngineMessage::Round1 => handle_round1(myself, state).await,
            DkgEngineMessage::Round1Out(node_id, output) => handle_round1_output(state, node_id, output),
            DkgEngineMessage::Round2 => handle_round2(myself, state).await,
            DkgEngineMessage::StoreReceivedPublicKey(idx, pubshares) => handle_store_received_pubshare(state, idx, pubshares),
            DkgEngineMessage::StoreResharedReceivedPublicKey(idx, pubshares) => handle_store_reshared_received_pubshare(state, idx, pubshares),
            DkgEngineMessage::Round3 => handle_round3(myself, state, false).await,
            DkgEngineMessage::ResharingInit(idx, tstar_provers, new_provers, election_info, peer_id, group_keys) => {
                let resharing_context = ResharingInitContext {
                    rsa_private_key: self.0.clone(),
                    election_info,
                    node_peer_id: peer_id,
                    group_keys,
                };
                handle_resharing_init(resharing_context, state, idx, tstar_provers, new_provers).await
            }
            DkgEngineMessage::ResharingRound1(election_info, is_relected) => handle_resharing_round1(myself, state, election_info, is_relected).await,
            DkgEngineMessage::ResharingRound1Out(node_id, output) => handle_resharing_round1_output(state, node_id, output),
            DkgEngineMessage::ResharingRound2 => handle_resharing_round2(state).await,
            DkgEngineMessage::ResharingRound3 => handle_round3(myself, state, true).await,

            DkgEngineMessage::ClearShares => {
                if let Some(state) = &mut state.0 {
                    for (_, node) in state.dkg_nodes.iter_mut() {
                        node.clear_inputs();
                    }
                }
                Ok(())
            }
            DkgEngineMessage::StartDKGRound1 => {
                handle_start_dkg_round_1(state).await?;
                Ok(())
            }
            DkgEngineMessage::StartResharingRound1 => {
                handle_start_resharing_round_1(state).await?;
                Ok(())
            }
            DkgEngineMessage::StartDKGRound2 => {
                handle_start_dkg_round_2(state).await?;
                Ok(())
            }
            DkgEngineMessage::StartResharingRound2 => {
                handle_start_resharing_round_2(state).await?;
                Ok(())
            }
            DkgEngineMessage::StartDKGRound3 => {
                handle_start_dkg_round_3(state).await?;
                Ok(())
            }
            DkgEngineMessage::StartResharingRound3 => {
                handle_start_resharing_round_3(state).await?;
                Ok(())
            }
        }
    }
}
//All Handlers

/// Initializes the DKG engine with the provided parameters and starts the first round of the DKG.
///
/// This function sets up the Distributed Key Generation (DKG) engine by initializing its state
/// with the provided peer information, threshold, and other parameters. Once the initialization
/// is complete, it triggers the first round of the DKG protocol by sending a `Round1` message.
#[tracing::instrument(name = "handle_init", skip(actor, state))]
async fn handle_dkg_init(
    actor: &DKGEngineActor,
    state: &mut DkgStateWrapper,
    idx: usize,
    peer_id: PeerId,
    peers: HashMap<PeerId, (ProverInfo, usize)>,
    threshold: u32,
) -> Result<(), ActorProcessingErr> {
    debug!("Initializing DKG engine for peer {} with index {} and threshold {}", peer_id, idx, threshold);

    trace!("Setting DKG engine state for peer {}", peer_id);
    state.0 = Some(DKGEngineState::dkg_init(peer_id, idx as u128, actor.0.clone(), peers, threshold).await.map_err(|e| {
        error!("Failed to initialize DKG engine state for peer {}: {:?}", peer_id, e);
        ActorProcessingErr::from(e)
    })?);

    trace!("Casting DKGInitCompleted gossip message for peer {}", peer_id);
    cast_message!(
        ActorType::GossipEngine,
        GossipEngineMessage::Gossip(Message::DKGInitCompleted(peer_id), NETWORK_TOPIC.to_string(),),
        GossipEngineError
    );

    debug!("DKG engine initialization completed for peer {}", peer_id);
    Ok(())
}

/// Handles the initialization for the resharing process .
/// This function sets up the state and network for resharing based on the provided context. It checks
/// if the node is part of the tstar network and proceeds with resharing initialization if applicable.
/// The function clears inputs for DKG nodes and prepares the state for the next round of resharing.
//#[tracing::instrument(name = "handle_resharing_init", skip(context))]
async fn handle_resharing_init(
    context: ResharingInitContext,
    resharing_state: &mut DkgStateWrapper,
    idx: usize,
    tstar_provers: HashMap<PeerId, (ProverInfo, usize)>,
    new_provers: HashMap<PeerId, (ProverInfo, usize)>,
) -> Result<(), ActorProcessingErr> {
    let election_info = context.election_info.clone();
    let node_peer_id = context.node_peer_id;
    let group_keys = context.group_keys.clone();
    debug!(
        "Starting resharing initialization for peer {} with index {} and threshold {}",
        node_peer_id, idx, election_info.threshold
    );

    trace!("Initializing network for resharing with {} tstar provers", tstar_provers.len());
    let mut network = Network::new(election_info.threshold as usize, tstar_provers.len(), true).map_err(|e| {
        error!("Failed to initialize network for resharing: {:?}", e);
        DKGEngineError::Custom(format!("Failed to initialize network for resharing: {:?}", e))
    })?;

    if tstar_provers.contains_key(&node_peer_id) && resharing_state.0.is_none() {
        debug!("Initializing resharing state for tstar node {}", node_peer_id);
        trace!("Retrieving private key shares for peer {}", node_peer_id);
        let private_keyshares: HashMap<String, Vec<u8>> = get_state(&node_peer_id.to_string(), "private_keyshares").unwrap_or_default();
        debug!("Retrieved {} private key shares for peer {}", private_keyshares.len(), node_peer_id);

        trace!("Restoring resharing state for peer {}", node_peer_id);
        resharing_state.0 = Some(
            DKGEngineState::restore_old_resharing_state(
                node_peer_id,
                idx as u128,
                context.rsa_private_key.clone(),
                new_provers.clone(),
                election_info.threshold,
                private_keyshares,
                tstar_provers.clone(),
                group_keys.clone(),
            )
            .await
            .map_err(|e| {
                error!("Failed to restore resharing state for peer {}: {:?}", node_peer_id, e);
                ActorProcessingErr::from(e)
            })?,
        );
    }

    if let Some(state) = &mut resharing_state.0 {
        trace!("Resetting resharing state for peer {}: clearing round 2 flag and round 1 context", state.peer_id);
        state.resharing_round2_triggered = false;
        state.resharing_round1_context = None;

        trace!("Clearing input data for {} DKG nodes", state.dkg_nodes.len());
        for (key, node) in state.dkg_nodes.iter_mut() {
            trace!("Clearing inputs for node {}", key);
            node.clear_inputs();
        }

        if tstar_provers.contains_key(&state.peer_id) {
            trace!("Parsing RSA public keys for tstar provers");
            let rsa_keys_map = DKGEngineState::parse_rsa_keys(&tstar_provers).map_err(|e| {
                error!("Failed to parse RSA public keys for tstar provers: {:?}", e);
                ActorProcessingErr::from(e)
            })?;
            network.public_keys = rsa_keys_map;
            state.network = network;

            trace!("Storing resharing round 1 context for peer {}", state.peer_id);
            state.resharing_round1_context = Some(context);

            trace!("Casting ResharingInitCompleted gossip message for peer {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(Message::ResharingInitCompleted(state.peer_id), NETWORK_TOPIC.to_string(),),
                GossipEngineError
            );

            debug!("Proceeding to resharing Round 1 for tstar node {}", state.peer_id);
        } else if new_provers.contains_key(&state.peer_id) {
            trace!("Casting ResharingInitCompleted gossip message for new prover {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(Message::ResharingInitCompleted(state.peer_id), NETWORK_TOPIC.to_string(),),
                GossipEngineError
            );
            debug!("Initialized resharing for new prover {}", state.peer_id);
        } else {
            warn!("Cannot proceed with resharing for peer {}: not part of tstar or new provers", state.peer_id);
        }

        trace!("Updating resharing state for peer {}: setting provers and election info", state.peer_id);
        state.tstar_provers = tstar_provers.clone();
        state.new_provers = new_provers.clone();
        state.election_info = election_info.clone();
        state.finalized_group_public_keys = group_keys;
    } else if resharing_state.0.is_none() && new_provers.contains_key(&node_peer_id) {
        debug!("Initializing DKG state for new prover {} with no prior state", node_peer_id);
        trace!("Creating DKG state for peer {}", node_peer_id);
        resharing_state.0 = Some(
            DKGEngineState::dkg_init(node_peer_id, idx as u128, context.rsa_private_key.clone(), new_provers.clone(), election_info.threshold)
                .await
                .map_err(|e| {
                    error!("Failed to initialize DKG state for peer {}: {:?}", node_peer_id, e);
                    ActorProcessingErr::from(e)
                })?,
        );

        if let Some(state) = &mut resharing_state.0 {
            trace!("Initializing network for new prover {} with {} provers", node_peer_id, new_provers.len());
            let network = Network::new(election_info.threshold as usize, new_provers.len(), false).map_err(|e| {
                error!("Failed to initialize network for new prover {}: {:?}", node_peer_id, e);
                DKGEngineError::Custom(format!("Failed to initialize network for resharing: {:?}", e))
            })?;

            trace!("Updating DKG state for peer {}: setting provers, election info, and group keys", node_peer_id);
            state.tstar_provers = tstar_provers;
            state.new_provers = new_provers.clone();
            state.election_info = election_info.clone();
            state.network = network;
            state.finalized_group_public_keys = group_keys;

            trace!("Clearing input data for {} DKG nodes", state.dkg_nodes.len());
            for (key, node) in state.dkg_nodes.iter_mut() {
                trace!("Clearing inputs for node {}", key);
                node.clear_inputs();
            }

            trace!("Casting ResharingInitCompleted gossip message for peer {}", node_peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(Message::ResharingInitCompleted(state.peer_id), NETWORK_TOPIC.to_string(),),
                GossipEngineError
            );

            debug!("DKG state initialized for new prover {}", node_peer_id);
        }
    }

    debug!("Completed resharing initialization for peer {}", node_peer_id);
    Ok(())
}

//#[tracing::instrument(name = "handle_start_dkg_round_1", skip(context))]
async fn handle_start_dkg_round_1(dkg_state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Checking DKG Round 1 initiation");

    if let Some(state) = &mut dkg_state.0 {
        trace!("Verifying quorum membership for peer {}", state.peer_id);
        if state.new_provers.contains_key(&state.peer_id) {
            trace!("Casting DKG Round 1 message for peer {}", state.peer_id);
            cast_message!(ActorType::DkgEngine, DkgEngineMessage::Round1, DKGEngineError);
            debug!("Triggered DKG Round 1 for peer {}", state.peer_id);
        } else {
            warn!("Cannot proceed with DKG Round 1 for peer {}: not part of quorum", state.peer_id);
        }
    } else {
        warn!("No DKG state found; cannot proceed with DKG Round 1");
    }

    Ok(())
}

//#[tracing::instrument(name = "handle_start_resharing_round_1", skip(context))]
async fn handle_start_resharing_round_1(resharing_state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Checking resharing Round 1 initiation");

    if let Some(state) = &mut resharing_state.0 {
        trace!("Verifying tstar membership for peer {}", state.peer_id);
        if state.tstar_provers.contains_key(&state.peer_id) {
            if let Some(context) = state.resharing_round1_context.as_ref() {
                trace!("Casting resharing Round 1 message for peer {}", state.peer_id);
                cast_message!(
                    ActorType::DkgEngine,
                    DkgEngineMessage::ResharingRound1(context.election_info.clone(), state.new_provers.contains_key(&context.node_peer_id)),
                    DKGEngineError
                );
                debug!("Triggered resharing Round 1 for tstar peer {}", state.peer_id);
            } else {
                warn!("No resharing context found for peer {}; cannot trigger resharing Round 1", state.peer_id);
            }
        } else {
            warn!("Cannot proceed with resharing Round 1 for peer {}: not part of tstar provers", state.peer_id);
        }
    } else {
        warn!("No resharing state found; cannot proceed with resharing Round 1");
    }
    Ok(())
}

async fn handle_start_dkg_round_2(dkg_state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Checking DKG Round 2 initiation");

    if let Some(state) = &mut dkg_state.0 {
        trace!("Verifying quorum membership for peer {}", state.peer_id);
        if state.new_provers.contains_key(&state.peer_id) {
            trace!("Casting DKG Round 2 message for peer {}", state.peer_id);
            cast_message!(ActorType::DkgEngine, DkgEngineMessage::Round2, DKGEngineError);
            debug!("Triggered DKG Round 2 for peer {}", state.peer_id);
        } else {
            warn!("Cannot proceed with DKG Round 2 for peer {}: not part of quorum", state.peer_id);
        }
    } else {
        warn!("No DKG state found; cannot proceed with DKG Round 2");
    }

    Ok(())
}

async fn handle_start_resharing_round_2(resharing_state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Checking resharing Round 2 initiation");

    if let Some(state) = &mut resharing_state.0 {
        trace!("Verifying new quorum membership for peer {}", state.peer_id);
        if state.new_provers.contains_key(&state.peer_id) {
            trace!("Casting resharing Round 2 message for peer {}", state.peer_id);
            cast_message!(ActorType::DkgEngine, DkgEngineMessage::ResharingRound2, DKGEngineError);
            debug!("Triggered resharing Round 2 for peer {}", state.peer_id);
        } else {
            warn!("Cannot proceed with resharing Round 2 for peer {}: not part of new quorum", state.peer_id);
        }
    } else {
        warn!("No resharing state found; cannot proceed with resharing Round 2");
    }

    Ok(())
}

async fn handle_start_dkg_round_3(dkg_state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Checking DKG Round 3 initiation");

    if let Some(state) = &mut dkg_state.0 {
        trace!("Verifying quorum membership for peer {}", state.peer_id);
        if state.new_provers.contains_key(&state.peer_id) {
            trace!("Casting DKG Round 3 message for peer {}", state.peer_id);
            cast_message!(ActorType::DkgEngine, DkgEngineMessage::Round3, DKGEngineError);
            debug!("Triggered DKG Round 3 for peer {}", state.peer_id);
        } else {
            warn!("Cannot proceed with DKG Round 3 for peer {}: not part of quorum", state.peer_id);
        }
    } else {
        warn!("No DKG state found; cannot proceed with DKG Round 3");
    }

    Ok(())
}

async fn handle_start_resharing_round_3(resharing_state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Checking resharing Round 3 initiation");

    if let Some(state) = &mut resharing_state.0 {
        trace!("Verifying new quorum membership for peer {}", state.peer_id);
        if state.new_provers.contains_key(&state.peer_id) {
            trace!("Casting resharing Round 3 message for peer {}", state.peer_id);
            cast_message!(ActorType::DkgEngine, DkgEngineMessage::ResharingRound3, DKGEngineError);
            debug!("Triggered resharing Round 3 for peer {}", state.peer_id);
        } else {
            warn!("Cannot proceed with resharing Round 3 for peer {}: not part of new quorum", state.peer_id);
        }
    } else {
        warn!("No resharing state found; cannot proceed with resharing Round 3");
    }

    Ok(())
}

/// Handles the execution of the first round of the DKG.
/// This function triggers Round 1 of the DKG process and gossips the results via the `GossipEngine`
#[tracing::instrument(name = "handle_round1", skip(_myself, state))]
async fn handle_round1(_myself: ActorRef<DkgEngineMessage>, state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Handling DKG Round 1 for peer {}", state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default());

    if let Some(state) = &mut state.0 {
        trace!("Executing DKG Round 1 for peer {}", state.peer_id);
        match state.round1() {
            Ok(round1_output) => {
                trace!("Casting Round 1 output gossip message for peer {} with index {}", state.peer_id, state.idx);
                cast_message!(
                    ActorType::GossipEngine,
                    GossipEngineMessage::Gossip(Message::Round1Out(state.idx, round1_output), NETWORK_TOPIC.to_string(),),
                    GossipEngineError
                );

                trace!("Checking DKG Round 1 completion for {} nodes", state.dkg_nodes.len());
                let is_ready = state.dkg_nodes.iter().all(|(key, dkg_node)| {
                    let (i, j) = dkg_node.get_no_of_inputs();
                    debug!("Node {} received {} y-values and {} v-values in Round 1", key, i, j);
                    i == state.network.total_nodes - 1 && j == state.network.total_nodes - 1
                });

                debug!("DKG Round 1 readiness: {}", is_ready);
                if is_ready {
                    debug!("Proceeding to DKG Round 2 for peer {}", state.peer_id);
                    trace!("Casting DKGRound1Completed gossip message for peer {}", state.peer_id);
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::DKGRound1Completed(state.peer_id), NETWORK_TOPIC.to_string(),),
                        GossipEngineError
                    );
                }
            }
            Err(e) => {
                error!("Failed DKG Round 1 for peer {}: {:?}", state.peer_id, e);
            }
        }
    } else {
        warn!("No DKG state found for peer; cannot proceed with DKG Round 1");
    }

    Ok(())
}

/// Handles the execution of the first round of the resharing process.
///
/// This function performs the resharing operations for Round 1 and updates the state of DKG nodes with the results.
/// It also broadcasts the output of this round to other nodes via the `GossipEngine`.
//#[tracing::instrument(name = "handle_resharing_round1", skip(myself, state))]
#[tracing::instrument(name = "handle_resharing_round1", skip(_myself, state))]
async fn handle_resharing_round1(_myself: ActorRef<DkgEngineMessage>, state: &mut DkgStateWrapper, election_info: ElectionInfo, is_relected: bool) -> Result<(), ActorProcessingErr> {
    debug!(
        "Handling resharing Round 1 for peer {} with index {}",
        state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default(),
        state.0.as_ref().map(|s| s.idx).unwrap_or_default()
    );

    if let Some(state) = &mut state.0 {
        trace!("Verifying tstar membership for peer {} with index {}", state.peer_id, state.idx);
        if state.tstar_provers.contains_key(&state.peer_id) {
            trace!("Executing resharing Round 1 for peer {}", state.peer_id);
            match state.resharing_round1(election_info) {
                Ok(resharing_round1_output) => {
                    debug!("Broadcasting resharing Round 1 output for peer {} with index {}", state.peer_id, state.idx);
                    trace!("Casting resharing Round 1 output gossip message for peer {}", state.peer_id);
                    cast_message!(
                        ActorType::GossipEngine,
                        GossipEngineMessage::Gossip(Message::ResharingRound1Out(state.idx, resharing_round1_output.clone()), NETWORK_TOPIC.to_string(),),
                        GossipEngineError
                    );

                    if is_relected {
                        trace!("Processing resharing Round 1 outputs for {} nodes", state.dkg_nodes.len());
                        for (key, outputs) in resharing_round1_output.iter() {
                            if let Some(dkg_node) = state.dkg_nodes.get_mut(key) {
                                match dkg_node {
                                    NodeDataVariant::Secp256k1(node) => {
                                        trace!("Processing Secp256k1 outputs for node {} for peer {}", key, state.peer_id);
                                        let resharing_output = outputs
                                            .0
                                            .get(&state.idx)
                                            .ok_or_else(|| {
                                                error!("Missing resharing Round 1 output for peer {} (index {}) for node {}", state.peer_id, state.idx, key);
                                                anyhow!("Missing resharing Round 1 output for index {}", state.idx)
                                            })?
                                            .clone();
                                        let v_values: Result<Vec<AffinePoint>, _> = outputs
                                            .1
                                            .iter()
                                            .map(|x| {
                                                AffinePoint::from_encoded(x).map_err(|_| {
                                                    error!("Failed to decode Secp256k1 points for node {} for peer {}", key, state.peer_id);
                                                    "Error decoding Secp256k1 points"
                                                })
                                            })
                                            .collect();
                                        let v_values = v_values?;
                                        node.input_y.insert(state.idx, resharing_output.clone());
                                        node.input_v.insert(state.idx, v_values);
                                        trace!("Stored Secp256k1 inputs for node {} for peer {}", key, state.peer_id);
                                    }
                                    NodeDataVariant::BabyJubJub(node) => {
                                        trace!("Processing BabyJubJub outputs for node {} for peer {}", key, state.peer_id);
                                        let resharing_output = outputs
                                            .0
                                            .get(&state.idx)
                                            .ok_or_else(|| {
                                                error!("Missing resharing Round 1 output for peer {} (index {}) for node {}", state.peer_id, state.idx, key);
                                                anyhow!("Missing resharing Round 1 output for index {}", state.idx)
                                            })?
                                            .clone();
                                        let v_values: Result<Vec<EdwardsAffine>, _> = outputs
                                            .1
                                            .iter()
                                            .map(|x| {
                                                EdwardsAffine::from_encoded(x).map_err(|_| {
                                                    error!("Failed to decode BabyJubJub points for node {} for peer {}", key, state.peer_id);
                                                    "Error decoding BabyJubJub points"
                                                })
                                            })
                                            .collect();
                                        let v_values = v_values?;
                                        node.input_y.insert(state.idx, resharing_output.clone());
                                        node.input_v.insert(state.idx, v_values);
                                        trace!("Stored BabyJubJub inputs for node {} for peer {}", key, state.peer_id);
                                    }
                                }
                            }
                        }
                    }

                    debug!("Completed resharing Round 1 for tstar peer {}", state.peer_id);

                    trace!("Checking resharing Round 1 completion for {} nodes", state.dkg_nodes.len());
                    let is_ready = state.dkg_nodes.iter().all(|(key, dkg_node)| {
                        let (i, j) = dkg_node.get_no_of_inputs();
                        debug!("Node {} received {} y-values and {} v-values for peer {}", key, i, j, state.peer_id);
                        i == state.tstar_provers.len() && j == state.tstar_provers.len() && state.new_provers.contains_key(&state.peer_id)
                    });

                    debug!("Resharing Round 1 readiness for peer {}: {}", state.peer_id, is_ready);
                    if is_ready && state.new_provers.contains_key(&state.peer_id) {
                        debug!("Proceeding to resharing Round 2 for peer {}", state.peer_id);
                        trace!("Casting ResharingRound1Completed gossip message for peer {}", state.peer_id);
                        cast_message!(
                            ActorType::GossipEngine,
                            GossipEngineMessage::Gossip(Message::ResharingRound1Completed(state.peer_id), NETWORK_TOPIC.to_string(),),
                            GossipEngineError
                        );
                    } else {
                        if state.new_provers.contains_key(&state.peer_id) {
                            trace!("Logging missing inputs for peer {}", state.peer_id);
                            state.dkg_nodes.iter().for_each(|(key, dkg_node)| {
                                let values = dkg_node.get_received_y_v_from();
                                warn!(
                                    "Missing inputs for node {} for peer {}: received from {:?}, required {} inputs",
                                    key,
                                    state.peer_id,
                                    values,
                                    state.tstar_provers.len()
                                );
                            });
                        } else {
                            warn!("Cannot proceed to resharing Round 2 for peer {}: not part of new quorum", state.peer_id);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed resharing Round 1 for peer {} (index {}): {:?}", state.peer_id, state.idx, e);
                }
            }
        } else {
            warn!("Peer {} (index {}) not eligible for resharing Round 1: not in tstar provers", state.peer_id, state.idx);
        }
    } else {
        warn!("No DKG state found for peer; cannot proceed with resharing Round 1");
    }

    Ok(())
}
// Handles the output of Round 1 in the DKG.
///
/// This function processes the results received from other nodes during Round 1, updates the state of DKG nodes
/// with these results, and determines if the protocol can proceed to Round 2. It also logs the progress and status
/// of the protocol at various stages.
#[tracing::instrument(name = "handle_round1_output", skip(state, round_1_outputs))]
fn handle_round1_output(state: &mut DkgStateWrapper, idx: u128, round_1_outputs: Round1Outputs) -> Result<(), ActorProcessingErr> {
    debug!(
        "Handling DKG Round 1 output from index {} for peer {}",
        idx,
        state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default()
    );

    if let Some(state) = state.0.as_mut() {
        trace!("Processing Round 1 outputs from index {} for peer {}", idx, state.peer_id);

        for (key, outputs) in round_1_outputs.iter() {
            if let Some(dkg_node) = state.dkg_nodes.get_mut(key) {
                trace!("Registering Round 1 output for node {} from index {} for peer {}", key, idx, state.peer_id);
                match dkg_node.register_round_1_result_from_other_node(state.idx, idx, outputs.clone()) {
                    Ok(_) => {
                        debug!("Registered Round 1 output for node {} from index {} for peer {}", key, idx, state.peer_id);
                    }
                    Err(e) => {
                        error!("Failed to register Round 1 output for node {} from index {} for peer {}: {:?}", key, idx, state.peer_id, e);
                    }
                }
            } else {
                warn!("No DKG node found for key {} for peer {}", key, state.peer_id);
            }
        }

        trace!("Checking DKG Round 1 completion for {} nodes", state.dkg_nodes.len());
        let is_ready = state.dkg_nodes.iter().all(|(key, dkg_node)| {
            let (i, j) = dkg_node.get_no_of_inputs();
            debug!("Node {} received {} y-values and {} v-values for peer {}", key, i, j, state.peer_id);
            i == state.network.total_nodes - 1 && j == state.network.total_nodes - 1
        });

        debug!("DKG Round 1 readiness for peer {}: {}", state.peer_id, is_ready);
        if is_ready {
            debug!("Proceeding to DKG Round 2 for peer {}", state.peer_id);
            trace!("Casting DKGRound1Completed gossip message for peer {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(Message::DKGRound1Completed(state.peer_id), NETWORK_TOPIC.to_string(),),
                GossipEngineError
            );
        }
    } else {
        warn!("No DKG state found for peer; not part of elected quorum");
    }

    Ok(())
}

/// Handles the output of Resharing Round 1..
///
/// This function processes the resharing results received from other nodes during Resharing Round 1,
/// updates the state of DKG nodes with these results, and determines if the protocol can proceed to Round 2.
/// It also logs the progress and status of the resharing process at various stages.
///
//#[tracing::instrument(name = "handle_resharing_round1_output", skip(myself, state, round_1_outputs))]
fn handle_resharing_round1_output(state: &mut DkgStateWrapper, idx: u128, round_1_outputs: ResharingRound1Outputs) -> Result<(), ActorProcessingErr> {
    debug!(
        "Handling resharing Round 1 output from index {} for peer {}",
        idx,
        state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default()
    );

    if let Some(state) = state.0.as_mut() {
        trace!("Verifying new prover membership for peer {}", state.peer_id);
        if state.new_provers.contains_key(&state.peer_id) {
            debug!("Processing resharing Round 1 output from index {} for peer {}", idx, state.peer_id);

            trace!("Verifying t-star node status for index {}", idx);
            let idx_match = state.tstar_provers.values().any(|&(_, tstar_idx)| tstar_idx == idx as usize);
            if !idx_match {
                warn!("Received output from index {} for peer {}, but it is not a t-star node", idx, state.peer_id);
                return Ok(());
            }

            trace!("Processing resharing Round 1 outputs for {} nodes", round_1_outputs.len());
            for (key, outputs) in round_1_outputs.iter() {
                let received_indices = outputs.0.keys().cloned().collect::<Vec<u128>>();
                debug!("Received resharing Round 1 outputs for node {} from indices {:?}", key, received_indices);

                if let Some(dkg_node) = state.dkg_nodes.get_mut(key) {
                    trace!("Registering resharing Round 1 output for node {} from index {} for peer {}", key, idx, state.peer_id);
                    match dkg_node.register_resharing_round_1_result_from_other_node(state.idx, idx, outputs.clone()) {
                        Ok(_) => {
                            debug!("Registered resharing Round 1 output for node {} from index {} for peer {}", key, idx, state.peer_id);
                        }
                        Err(e) => {
                            error!("Failed to register resharing Round 1 output for node {} from index {} for peer {}: {:?}", key, idx, state.peer_id, e);
                        }
                    }
                } else {
                    warn!("No DKG node found for key {} for peer {}", key, state.peer_id);
                }
            }

            trace!("Checking resharing Round 1 completion for {} nodes", state.dkg_nodes.len());
            let is_ready = state.dkg_nodes.iter().all(|(key, dkg_node)| {
                let (i, j) = dkg_node.get_no_of_inputs();
                debug!("Node {} received {} y-values and {} v-values for peer {}", key, i, j, state.peer_id);
                i == state.tstar_provers.len() && j == state.tstar_provers.len() && state.new_provers.contains_key(&state.peer_id)
            });

            debug!("Resharing Round 1 readiness for peer {}: {}", state.peer_id, is_ready);
            if is_ready {
                debug!("Proceeding to resharing Round 2 for peer {}", state.peer_id);
                trace!("Casting ResharingRound1Completed gossip message for peer {}", state.peer_id);
                cast_message!(
                    ActorType::GossipEngine,
                    GossipEngineMessage::Gossip(Message::ResharingRound1Completed(state.peer_id), NETWORK_TOPIC.to_string(),),
                    GossipEngineError
                );
            } else {
                trace!("Logging missing inputs for peer {}", state.peer_id);
                state.dkg_nodes.iter().for_each(|(key, dkg_node)| {
                    let received_values = dkg_node.get_received_y_v_from();
                    warn!(
                        "Missing inputs for node {} for peer {}: received from {:?}, required {} inputs",
                        key,
                        state.peer_id,
                        received_values,
                        state.tstar_provers.len()
                    );
                });
            }
        } else {
            warn!("Peer {} not eligible for resharing Round 1: not part of new quorum", state.peer_id);
        }
    } else {
        warn!("No DKG state found for peer; cannot process resharing Round 1 output");
    }

    Ok(())
}

/// Handles the processing of Round 2 in the DKG.
///
/// This function performs the second round of the DKG protocol for each node, collects the results,
/// and sends them to the GossipEngine actor. It also logs the status and completion of the process.
#[tracing::instrument(name = "handle_round2", skip(_myself, state))]
async fn handle_round2(_myself: ActorRef<DkgEngineMessage>, state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Handling DKG Round 2 for peer {}", state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default());

    if let Some(state) = &mut state.0 {
        trace!("Verifying new prover membership for peer {}", state.peer_id);
        if !state.new_provers.contains_key(&state.peer_id) {
            warn!("Peer {} not eligible for DKG Round 2: not part of new quorum", state.peer_id);
            return Ok(());
        }

        trace!("Processing DKG Round 2 for {} nodes", state.dkg_nodes.len());
        let mut pubkeys_for_node: Round2Outputs = HashMap::new();
        for (key, value) in state.dkg_nodes.iter_mut() {
            trace!("Executing DKG Round 2 for node {} for peer {}", key, state.peer_id);
            match value.dkg_round_2(state.network.clone()) {
                Ok(output) => {
                    pubkeys_for_node.insert(key.clone(), output);
                    debug!("Completed DKG Round 2 for node {} for peer {}", key, state.peer_id);
                }
                Err(e) => {
                    error!("Failed DKG Round 2 for node {} for peer {}: {:?}", key, state.peer_id, e);
                    return Err(ActorProcessingErr::from(e));
                }
            }
        }

        debug!("Completed DKG Round 2 processing for peer {}; broadcasting outputs", state.peer_id);
        trace!("Casting Round 2 output gossip message for peer {} with index {}", state.peer_id, state.idx);
        cast_message!(
            ActorType::GossipEngine,
            GossipEngineMessage::Gossip(Message::Round2Out(state.idx, pubkeys_for_node), NETWORK_TOPIC.to_string(),),
            GossipEngineError
        );

        trace!("Checking DKG Round 2 completion for {} nodes", state.dkg_nodes.len());
        let is_ready = state.dkg_nodes.iter().all(|(key, dkg_node)| {
            let cnt = dkg_node.get_no_of_other_pub_shares();
            debug!("Node {} received {} public shares for peer {}", key, cnt, state.peer_id);
            cnt == state.new_provers.len()
        });

        debug!("DKG Round 2 readiness for peer {}: {}", state.peer_id, is_ready);
        if is_ready {
            debug!("Proceeding to DKG Round 3 for peer {}", state.peer_id);
            trace!("Casting DKGRound2Completed gossip message for peer {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(Message::DKGRound2Completed(state.peer_id), NETWORK_TOPIC.to_string(),),
                GossipEngineError
            );
        }
    } else {
        warn!("No DKG state found for peer; cannot proceed with DKG Round 2");
    }

    Ok(())
}

/// Handles the execution of the second round of the resharing process.
///
/// This function performs the resharing operations for Round 2, updates the state of DKG nodes with the results, and
/// broadcasts the resharing output to other nodes via the `GossipEngine`.
#[tracing::instrument(name = "handle_resharing_round2", skip(state))]
async fn handle_resharing_round2(state: &mut DkgStateWrapper) -> Result<(), ActorProcessingErr> {
    debug!("Handling resharing Round 2 for peer {}", state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default());

    if let Some(state) = &mut state.0 {
        trace!("Verifying new prover membership for peer {}", state.peer_id);
        if !state.new_provers.contains_key(&state.peer_id) {
            warn!("Peer {} (index {}) not eligible for resharing Round 2: not part of new quorum", state.peer_id, state.idx);
            return Ok(());
        }

        trace!("Initializing quorum networks for peer {}", state.peer_id);
        let new_network = quorum_network(state.election_info.threshold, &state.new_provers, false).map_err(|e| {
            error!("Failed to initialize new quorum network for peer {}: {:?}", state.peer_id, e);
            ActorProcessingErr::from(e)
        })?;
        let tstar_network = quorum_network(state.election_info.threshold, &state.tstar_provers, true).map_err(|e| {
            error!("Failed to initialize t-star quorum network for peer {}: {:?}", state.peer_id, e);
            ActorProcessingErr::from(e)
        })?;

        trace!("Processing resharing Round 2 for {} nodes", state.dkg_nodes.len());
        let mut pubkeys_for_node: ResharingRound2Outputs = HashMap::new();
        for (key, value) in state.dkg_nodes.iter_mut() {
            trace!("Retrieving finalized group public key for node {} for peer {}", key, state.peer_id);
            let k_dkg = state.finalized_group_public_keys.get(key).ok_or_else(|| {
                error!("Missing finalized group public key for node {} for peer {}", key, state.peer_id);
                anyhow!("Missing finalized group public key for node {}", key)
            })?;

            trace!("Executing resharing Round 2 for node {} for peer {}", key, state.peer_id);
            let output = value.resharing_round_2(tstar_network.clone(), new_network.clone(), k_dkg.clone()).map_err(|e| {
                error!("Failed resharing Round 2 for node {} for peer {}: {:?}", key, state.peer_id, e);
                ActorProcessingErr::from(e)
            })?;
            pubkeys_for_node.insert(key.clone(), output);
            debug!("Completed resharing Round 2 for node {} for peer {}", key, state.peer_id);
        }

        debug!("Completed resharing Round 2 processing for peer {}; broadcasting outputs", state.peer_id);
        trace!("Casting resharing Round 2 output gossip message for peer {} with index {}", state.peer_id, state.idx);
        cast_message!(
            ActorType::GossipEngine,
            GossipEngineMessage::Gossip(Message::ResharingRound2Out(state.idx, pubkeys_for_node), NETWORK_TOPIC.to_string(),),
            GossipEngineError
        );

        trace!("Checking resharing Round 2 completion for {} nodes", state.dkg_nodes.len());
        let is_ready = state.dkg_nodes.iter().all(|(key, dkg_node)| {
            let cnt = dkg_node.get_no_of_other_pub_shares();
            debug!("Node {} received {} public shares for peer {}", key, cnt, state.peer_id);
            cnt == state.new_provers.len()
        });

        debug!("Resharing Round 2 readiness for peer {}: {}", state.peer_id, is_ready);
        if is_ready {
            debug!("Proceeding to resharing Round 3 for peer {}", state.peer_id);
            trace!("Casting ResharingRound2Completed gossip message for peer {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(Message::ResharingRound2Completed(state.peer_id), NETWORK_TOPIC.to_string(),),
                GossipEngineError
            );
        }
    } else {
        warn!("No DKG state found for peer; cannot proceed with resharing Round 2");
    }

    Ok(())
}

/// Handles storing received public key shares from another node during the DKG.
///
/// This function processes and stores the public key shares received from a node for the DKG
/// round 3. It checks if the current node is not the sender, and if all required public key
/// shares are collected. If all public shares are received, the function triggers the next
/// round of the DKG.
#[tracing::instrument(name = "handle_store_received_pubshare", skip(state), fields(pubshares = ?debug_key_shares(&pubshares)))]
fn handle_store_received_pubshare(state: &mut DkgStateWrapper, idx: u128, pubshares: HashMap<String, Vec<u8>>) -> Result<(), ActorProcessingErr> {
    debug!(
        "Handling public share storage from index {} for peer {}",
        idx,
        state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default()
    );

    if let Some(state) = &mut state.0 {
        trace!("Checking if public shares are from self for peer {} (index {})", state.peer_id, state.idx);
        if state.idx == idx {
            debug!("Skipping storage of own public shares for peer {}", state.peer_id);
            return Ok(());
        }

        trace!("Validating node index {} against new provers for peer {}", idx, state.peer_id);
        let valid_node = state.new_provers.iter().any(|(_, &(_, node_idx))| node_idx as u128 == idx);
        if !valid_node {
            warn!("Index {} not part of new quorum for peer {}; skipping public share storage", idx, state.peer_id);
            return Ok(());
        }

        debug!("Storing public shares from index {} for peer {}", idx, state.peer_id);
        for (key, value) in pubshares.iter() {
            if let Some(dkg_node) = state.dkg_nodes.get_mut(key) {
                trace!("Storing public share for node {} from index {} for peer {}", key, idx, state.peer_id);
                dkg_node.store_pubshare_for_node(idx, value).map_err(|e| {
                    error!("Failed to store public share for node {} from index {} for peer {}: {:?}", key, idx, state.peer_id, e);
                    ActorProcessingErr::from(e)
                })?;
                debug!("Stored public share for node {} from index {} for peer {}", key, idx, state.peer_id);
            } else {
                warn!("No DKG node found for key {} for peer {}", key, state.peer_id);
            }
        }

        trace!("Checking DKG Round 2 completion for {} nodes", state.dkg_nodes.len());
        let is_ready = state.dkg_nodes.iter().all(|(key, dkg_node)| {
            let cnt = dkg_node.get_no_of_other_pub_shares();
            debug!("Node {} received {} public shares for peer {}", key, cnt, state.peer_id);
            cnt == state.network.total_nodes
        });

        debug!("DKG Round 2 readiness for peer {}: {}", state.peer_id, is_ready);
        if is_ready {
            debug!("Proceeding to DKG Round 3 for peer {}", state.peer_id);
            trace!("Casting DKGRound2Completed gossip message for peer {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(Message::DKGRound2Completed(state.peer_id), NETWORK_TOPIC.to_string(),),
                GossipEngineError
            );
        } else {
            trace!("Logging missing public shares for peer {}", state.peer_id);
            state.dkg_nodes.iter().for_each(|(key, dkg_node)| {
                let received_from = dkg_node.pub_shares_received_from();
                warn!(
                    "Node {} for peer {} not ready for DKG Round 3: received {} public shares from {:?}, required {}",
                    key,
                    state.peer_id,
                    dkg_node.get_no_of_other_pub_shares(),
                    received_from,
                    state.network.total_nodes
                );
            });
        }
    } else {
        warn!("No DKG state found for peer; cannot store public shares");
    }

    Ok(())
}

/// Handles storing reshared public key shares received from another node.
///
/// This function checks if the received public shares are valid and belong to the current
/// quorum of the resharing process. If valid, the public shares are stored in the state.
/// Once all public shares are received from all nodes, the function triggers the next
/// resharing round.
//#[tracing::instrument(name = "handle_store_reshared_received_pubshare", skip(myself,state), fields(pubshares = ?debug_key_shares(&pubshares)))]
fn handle_store_reshared_received_pubshare(state: &mut DkgStateWrapper, idx: u128, pubshares: HashMap<String, Vec<u8>>) -> Result<(), ActorProcessingErr> {
    debug!(
        "Handling reshared public share storage from index {} for peer {}",
        idx,
        state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default()
    );

    if let Some(state) = &mut state.0 {
        trace!("Checking if public shares are from self for peer {} (index {})", state.peer_id, state.idx);
        if state.idx == idx {
            debug!("Skipping storage of own public shares for peer {}", state.peer_id);
            return Ok(());
        }

        trace!("Verifying new prover membership for peer {}", state.peer_id);
        if !state.new_provers.contains_key(&state.peer_id) {
            warn!("Peer {} (index {}) not eligible for resharing Round 2: not part of new quorum", state.peer_id, state.idx);
            return Ok(());
        }

        trace!("Validating node index {} against new provers for peer {}", idx, state.peer_id);
        let valid_node = state.new_provers.iter().any(|(_, &(_, node_idx))| node_idx as u128 == idx);
        if !valid_node {
            warn!("Index {} not part of new quorum for peer {}; skipping public share storage", idx, state.peer_id);
            return Ok(());
        }

        debug!("Storing reshared public shares from index {} for peer {}", idx, state.peer_id);
        for (key, value) in pubshares.iter() {
            if let Some(dkg_node) = state.dkg_nodes.get_mut(key) {
                trace!("Storing reshared public share for node {} from index {} for peer {}", key, idx, state.peer_id);
                dkg_node.store_pubshare_for_node(idx, value).map_err(|e| {
                    error!("Failed to store reshared public share for node {} from index {} for peer {}: {:?}", key, idx, state.peer_id, e);
                    ActorProcessingErr::from(e)
                })?;
                debug!("Stored reshared public share for node {} from index {} for peer {}", key, idx, state.peer_id);
            } else {
                warn!("No DKG node found for key {} for peer {}", key, state.peer_id);
            }
        }

        trace!("Checking resharing Round 2 completion for {} nodes", state.dkg_nodes.len());
        let is_ready = state.dkg_nodes.iter().all(|(key, dkg_node)| {
            let cnt = dkg_node.get_no_of_other_pub_shares();
            debug!("Node {} received {} public shares for peer {}", key, cnt, state.peer_id);
            cnt == state.new_provers.len()
        });

        debug!("Resharing Round 2 readiness for peer {}: {}", state.peer_id, is_ready);
        if is_ready {
            debug!("Proceeding to resharing Round 3 for peer {}", state.peer_id);
            trace!("Casting ResharingRound2Completed gossip message for peer {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(Message::ResharingRound2Completed(state.peer_id), NETWORK_TOPIC.to_string(),),
                GossipEngineError
            );
        } else {
            trace!("Logging missing public shares for peer {}", state.peer_id);
            state.dkg_nodes.iter().for_each(|(key, dkg_node)| {
                let received_from = dkg_node.pub_shares_received_from();
                warn!(
                    "Node {} for peer {} not ready for resharing Round 3: received {} public shares from {:?}, required {}",
                    key,
                    state.peer_id,
                    dkg_node.get_no_of_other_pub_shares(),
                    received_from,
                    state.new_provers.len()
                );
            });
        }
    } else {
        warn!("No DKG state found for peer; cannot store reshared public shares");
    }

    Ok(())
}

/// Handles the third round of the DKG or resharing protocol asynchronously.
/// This function processes the third round for all nodes within the state, depending on whether resharing is enabled.
#[tracing::instrument(name = "handle_round3", skip(_myself, state))]
async fn handle_round3(_myself: ActorRef<DkgEngineMessage>, state: &mut DkgStateWrapper, is_resharing: bool) -> Result<(), ActorProcessingErr> {
    debug!(
        "Handling {} Round 3 for peer {}",
        if is_resharing { "resharing" } else { "DKG" },
        state.0.as_ref().map(|s| s.peer_id.to_string()).unwrap_or_default()
    );

    if let Some(state) = &mut state.0 {
        trace!("Selecting network for peer {} (resharing: {})", state.peer_id, is_resharing);
        let network = if !is_resharing {
            state.network.clone()
        } else {
            quorum_network(state.election_info.threshold, &state.new_provers, false).map_err(|e| {
                error!("Failed to initialize new quorum network for peer {}: {:?}", state.peer_id, e);
                ActorProcessingErr::from(e)
            })?
        };

        trace!("Processing Round 3 for {} nodes", state.dkg_nodes.len());
        for (key, value) in state.dkg_nodes.iter_mut() {
            trace!(
                "Executing Round 3 for node {} for peer {} (curve: {})",
                key,
                state.peer_id,
                match value {
                    NodeDataVariant::Secp256k1(_) => "Secp256k1",
                    NodeDataVariant::BabyJubJub(_) => "BabyJubJub",
                }
            );
            match value {
                NodeDataVariant::Secp256k1(node) => {
                    NodeDataVariant::process_round_3::<Secp256k1>(&node.node, &network, &node.other_pubshares, is_resharing, state.new_provers.contains_key(&state.peer_id)).map_err(|e| {
                        error!("Failed Round 3 for node {} (Secp256k1) for peer {}: {:?}", key, state.peer_id, e);
                        ActorProcessingErr::from(e)
                    })?;
                }
                NodeDataVariant::BabyJubJub(node) => {
                    NodeDataVariant::process_round_3::<BabyJubJub>(&node.node, &network, &node.other_pubshares, is_resharing, state.new_provers.contains_key(&state.peer_id)).map_err(|e| {
                        error!("Failed Round 3 for node {} (BabyJubJub) for peer {}: {:?}", key, state.peer_id, e);
                        ActorProcessingErr::from(e)
                    })?;
                }
            }
            debug!("Completed Round 3 for node {} for peer {}", key, state.peer_id);
        }

        trace!("Collecting private key shares for peer {}", state.peer_id);
        let partial_shares: PrivkeyShares = state.dkg_nodes.iter().map(|(key, value)| (key.clone(), value.get_priv_share())).collect();

        if is_resharing {
            trace!("Updating network state for peer {}", state.peer_id);
            state.network = network;
        }

        if is_resharing && state.new_provers.contains_key(&state.peer_id) {
            debug!("Storing key shares and gossiping resharing preparation for peer {}", state.peer_id);
            trace!("Casting StoreKeyShares message for peer {}", state.peer_id);
            cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::StoreKeyShares(partial_shares.clone()), AppStateEngineError);

            trace!("Casting ResharingPrepared gossip message for peer {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(
                    Message::ResharingPrepared(state.idx, state.peer_id, state.dkg_nodes.iter().map(|(key, value)| (key.clone(), value.get_pub_share())).collect(),),
                    NETWORK_TOPIC.to_string(),
                ),
                GossipEngineError
            );

            trace!("Resetting API token usage for peer {}", state.peer_id);
            API_TOKEN.lock().await.used = false;
        }

        if !is_resharing && state.tstar_provers.is_empty() {
            debug!("Storing key shares and gossiping DKG completion for peer {}", state.peer_id);
            trace!("Casting StoreKeyShares message for peer {}", state.peer_id);
            cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::StoreKeyShares(partial_shares.clone()), AppStateEngineError);

            let mut keys = HashMap::new();
            for (key, value) in state.dkg_nodes.iter_mut() {
                match value {
                    NodeDataVariant::Secp256k1(node) => {
                        keys.insert(key.clone(), node.node.network_pubkey.encode());
                        debug!("Stored DKG group public key for node {} (Secp256k1) for peer {}", key, state.peer_id);
                    }
                    NodeDataVariant::BabyJubJub(node) => {
                        keys.insert(key.clone(), node.node.network_pubkey.encode());
                        debug!("Stored DKG group public key for node {} (BabyJubJub) for peer {}", key, state.peer_id);
                    }
                }
            }

            trace!("Casting Ready gossip message for peer {}", state.peer_id);
            cast_message!(
                ActorType::GossipEngine,
                GossipEngineMessage::Gossip(
                    Message::Ready(
                        state.idx,
                        state.peer_id,
                        state.dkg_nodes.iter().map(|(key, value)| (key.clone(), value.get_pub_share())).collect(),
                        keys
                    ),
                    NETWORK_TOPIC.to_string(),
                ),
                GossipEngineError
            );
        }
    } else {
        warn!("No DKG state found for peer; cannot proceed with {} Round 3", if is_resharing { "resharing" } else { "DKG" });
    }

    Ok(())
}
pub struct DkgEngineSupervisor {
    panic_tx: tokio::sync::mpsc::Sender<ActorCell>,
}
impl DkgEngineSupervisor {
    pub fn new(panic_tx: tokio::sync::mpsc::Sender<ActorCell>) -> Self { Self { panic_tx } }
}

#[derive(Debug, Error, Default)]
pub enum DkgEngineSupervisorError {
    #[default]
    #[error("failed to acquire DkgEngineSupervisorError from registry")]
    RactorRegistryError,
}

#[async_trait]
impl Actor for DkgEngineSupervisor {
    type Msg = DkgEngineMessage;
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

// unit tests
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_rsa_key() {
        let st = String::from(
            "-----BEGIN RSA PUBLIC KEY-----\rMIIBCgKCAQEAzp9oIa1PJOGlisDpfTG5hKsToL1zJJEV9oM2zT4B6iUt//SN0GkK\rWHZb/84Svss4c6zaBFCAq+R/DJPnDS3iduxa3u7UGUB+OXcS3m8fcUrYeX4Q6t30\rkm0eBqz5N4Da2UH/nxPK0E74VDlEHwOS/KiSkkf+EwbbOisn2+6t00uT/J49s1t5\rveGR7q0p4txU0ChUtFNJH+PViPNgWpifvn/nUwWAllT4SgiPRAL0vksQzqlcqmL+\rcx/pl1w1vd7El0zXpmJEzHCg4yBqbTE+KWm/NAeDnDcJ+AXHA5snX+0pCmobV3ZB\rKT62oy6yJZ8Wzo/cxqkiRy+q+FnYqfQc5QIDAQAB\r-----END RSA PUBLIC KEY-----\r"
            );
        let st = st.replace("\\r", "\n");
        RsaPublicKey::from_pkcs1_pem(&st).unwrap();
    }
}
