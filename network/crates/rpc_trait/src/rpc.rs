use std::collections::HashMap;

use jsonrpsee::proc_macros::rpc;
use jsonrpsee::types::ErrorObjectOwned;
use messages::message::ElectionParams;
use messages::network_utils::RequestToNetwork;
use messages::types::{ElectionResponse, NodeResponse, Response, StateRequest, StateResponse};
/// This trait defines the RPC methods for the Human Network service.
/// The RPC methods are defined under the `mishti` namespace, and the trait can be used to implement both client and server functionality.
#[rpc(client, server, namespace = "mishti")]
#[async_trait::async_trait]
pub trait HumanRpc {
    /// Fetches the state based on the provided `StateRequest`.
    ///
    /// # Arguments
    ///
    /// * `_request` - A request object containing the state parameters.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `StateResponse` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "fetch_state")]
    async fn fetch_state(&self, _request: StateRequest) -> Result<StateResponse, ErrorObjectOwned>;
    /// Executes the threshold_mul operation.
    ///
    /// # Arguments
    ///
    /// * `request` - A `RequestToNetwork` object representing the OPRF request.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `NodeResponse` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "threshold_mul")]
    async fn threshold_mul(&self, request: RequestToNetwork) -> Result<NodeResponse, ErrorObjectOwned>;
    /// Fetches the result of the threshold multiplication operation.
    ///
    /// # Arguments
    ///
    /// * `_request` - A string representing the request to fetch the result.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `NodeResponse` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "fetch_threshold_mul_result")]
    async fn fetch_threshold_mul_result(&self, _request: String) -> Result<NodeResponse, ErrorObjectOwned>;
    /// Pings the service to check its availability.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "ping")]
    async fn ping(&self) -> Result<Response, ErrorObjectOwned>;

    /// Fetches key share for current node for current epoch
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "fetch_key_share")]
    async fn fetch_key_share(&self, api_token: String) -> Result<NodeResponse, ErrorObjectOwned>;

    /// Takes backup of shares and all relevant data
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "backup")]
    async fn backup(&self) -> Result<bool, ErrorObjectOwned>;

    /// Restores key shares for the current node from a backup
    ///
    /// # Arguments
    ///
    /// * `keyshares` - A HashMap containing the keyshares to restore
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "restore_key_share")]
    async fn restore_key_share(&self, keyshares: HashMap<String, String>) -> Result<NodeResponse, ErrorObjectOwned>;

    /// fetches election state of the prover node from the relay node
    ///
    /// # Arguments
    ///
    /// * `peerId` - A String containing the peerId of the prover node
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "fetch_election_state")]
    async fn fetch_election_state(&self, peer_id: String) -> Result<NodeResponse, ErrorObjectOwned>;

    /// Restores election state for the prover node
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "restore_election_state")]
    async fn restore_election_state(&self) -> Result<NodeResponse, ErrorObjectOwned>;

    /// pings the service via quic to check its availability
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "quic_ping")]
    async fn quic_ping(&self, peer_id: String) -> Result<Response, ErrorObjectOwned>;

    /// Syncs the peer data for the relay node
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "sync_peer_data")]
    async fn sync_peer_data(&self) -> Result<Response, ErrorObjectOwned>;

    /// fetchs the election info for the prover node from relay node
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "fetch_election_info")]
    async fn fetch_election_info(&self, peer_id: String) -> Result<ElectionResponse, ErrorObjectOwned>;

    /// fetchs the voting power of the node
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "fetch_voting_power")]
    async fn fetch_voting_power(&self, peer_id: String) -> Result<NodeResponse, ErrorObjectOwned>;

    
    /// sets the resharing enabled flag for the network
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "set_resharing_enabled")]
    async fn set_resharing_enabled(&self, api_token: String, is_resharing_enabled: bool) -> Result<NodeResponse, ErrorObjectOwned>;

    
    /// fetches the current running version of the node
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "get_current_version")]
    async fn get_current_version(&self) -> Result<NodeResponse, ErrorObjectOwned>;
        
    /// add the node's peer id to be excluded from resharing
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "add_to_exclusion_list")]
    async fn add_to_exclusion_list(&self, api_token: String, peer_id: String) -> Result<NodeResponse, ErrorObjectOwned>;

    /// remove the node's peer id from the exclusion list
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "remove_from_exclusion_list")]
    async fn remove_from_exclusion_list(&self, api_token: String, peer_id: String) -> Result<NodeResponse, ErrorObjectOwned>;
        
    /// fetches the current  excluded peers from resharing
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "get_current_excluded_peers")]
    async fn get_current_excluded_peers(&self) -> Result<NodeResponse, ErrorObjectOwned>;
    
    /// fetches connected peers via gossip protocol
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "get_connected_peers")]
    async fn get_connected_peers(&self) -> Result<NodeResponse, ErrorObjectOwned>;

    /// fetches peer scores via gossip protocol
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "get_peer_scores")]
    async fn get_peer_scores(&self) -> Result<NodeResponse, ErrorObjectOwned>;
    
    /// fetches mesh peers via gossip protocol
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "get_mesh_peers")]
    async fn get_mesh_peers(&self) -> Result<NodeResponse, ErrorObjectOwned>;

    /// update election params for the network
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "update_election_params")]
    async fn update_election_params(&self, api_token: String, params: ElectionParams) -> Result<NodeResponse, ErrorObjectOwned>;
        
    /// fetches the election params for the network
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    #[method(name = "get_election_params")]
    async fn get_election_params(&self) -> Result<NodeResponse, ErrorObjectOwned>;

    /// Fetches the current finalized group public keys (one per method) from the relay node.
    /// Keys are hex-encoded encoded points, keyed by method name (e.g. "DecryptBabyJubjub").
    /// These change after every DKG or resharing round.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing `NodeResponse::Keyshare(HashMap<String, String>)` on success.
    #[method(name = "get_pubkey")]
    async fn get_pubkey(&self) -> Result<NodeResponse, ErrorObjectOwned>;
}
