use actors::app_state_actor::{AppStateEngineError, API_TOKEN};
use actors::election_actor::ElectionEngineError;
use actors::gossip_engine_actor::GossipEngineError;
use actors::{cast_message, get_actor_ref};
use async_trait::async_trait;
use jsonrpsee::types::ErrorObjectOwned;
use libp2p::PeerId;
use messages::message::{ElectionEngineMessage, ElectionParams, GossipEngineMessage};
use messages::network_utils::RequestToNetwork;
use messages::types::ElectionResponse;
use messages::{
    actor_type::ActorType,
    message::AppStateChangeMessage,
    types::{NodeResponse, Response, StateRequest, StateResponse},
};
use pubsub_rs::Pubsub;
use ractor::{ActorRef, RpcReplyPort};
use rpc_trait::rpc::HumanRpcServer;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{error, info};

// Define a struct to hold the API token and its usage flag
//pub struct ApiToken {
//    pub token: String,
//    pub used: bool,
//}

const HUMAN_NETWORK_VERSION: &str = "mainnetalphaV3.2.0";

/// Implementation of the RPC server for handling various RPC requests.
#[derive(Debug)]
pub struct RpcServerImpl;

impl RpcServerImpl {
    /// Creates a new instance of the `RpcServerImpl`.
    pub fn new() -> Self { Self {} }
}

#[async_trait]
impl HumanRpcServer for RpcServerImpl {
    /// Handles the `fetch_state` RPC request asynchronously to fetch the current application state.
    ///
    /// # Arguments
    ///
    /// * `request` - A `StateRequest` containing the user's state request data.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either a `StateResponse` with the requested state information
    /// or an `ErrorObjectOwned` if an error occurs.
    async fn fetch_state(&self, request: StateRequest) -> Result<StateResponse, ErrorObjectOwned> {
        info!("Received RPC `fetch_state` from: {:?}", request.user);
        let app_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        // Send the state request to the AppStateEngine actor with a 5-second timeout
        let _ = app_engine_status_actor
            .call(|_: RpcReplyPort<Response>| AppStateChangeMessage::StateRequest(request, tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("".to_string())))?;
        // Handle the response received from the AppStateEngine actor
        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to app state engine actor for fetching state: {:?}", e);
                Ok(StateResponse::Error {
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// The `threshold_mul` function sends an elliptic curve x scalar multiplication request to an app state engine actor
    /// Arguments:
    /// * `request`: The `request` parameter in the `threshold_mul` function represents a `RequestToNetwork` struct
    /// or object that contains the data being sent to the network.
    async fn threshold_mul(&self, request: RequestToNetwork) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received RPC `oprf` method");
        // Create a oneshot channel for receiving the response
        let pubsub = Pubsub::new();
        let uuid = uuid::Uuid::new_v4();
        let subscriber = pubsub.subscribe(vec![uuid.to_string()]).await;
        // Send the OPRF request to the AppStateEngine actor
        cast_message!(
            ActorType::AppStateEngine,
            AppStateChangeMessage::MulRequest(request.clone(), pubsub, uuid.to_string()),
            AppStateEngineError
        );
        // Handle the response received from the AppStateEngine actor
        let timeout = tokio::time::timeout(Duration::from_secs(60), async {
            return subscriber.recv().await;
        });
        match timeout.await {
            Ok(result) => match result {
                Ok((_, node_response)) => Ok(node_response),
                Err(e) => {
                    error!("Failed to fetch response for {}, error :{},", uuid.to_string(), e);
                    Ok(NodeResponse::Submitted { request_id: uuid.to_string() })
                }
            },
            Err(_) => {
                error!("Timeout: 15 seconds elapsed");
                Ok(NodeResponse::Submitted { request_id: uuid.to_string() })
            }
        }
    }
    /// Handles the `fetch_threshold_mul_result` RPC request asynchronously to fetch the result of a threshold multiplication operation.
    ///
    /// # Arguments
    ///
    /// * `request_id` - The ID of the request for which the threshold multiplication result is being fetched.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either a `NodeResponse` with the multiplication result or an `ErrorObjectOwned` if an error occurs.
    async fn fetch_threshold_mul_result(&self, request_id: String) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received RPC `fetch_threshold_mul_result` method");
        let (tx, rx) = oneshot::channel();
        // Send the request to fetch the reconstructed point to the AppStateEngine actor
        cast_message!(ActorType::AppStateEngine, AppStateChangeMessage::FetchReconstructedPoint(request_id.clone(), tx), AppStateEngineError);
        // Handle the response received from the AppStateEngine actor
        match rx.await {
            Ok(r) => {
                info!("Received node response:{:?}", r);
                Ok(r)
            }
            Err(e) => {
                error!(
                    "Error occurred while sending RPC request to app state engine actor for fetching threshold multiplication result: {:?}",
                    e
                );
                Ok(NodeResponse::Error {
                    request_id,
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }
    /// Handles the `ping` RPC request to check the server's availability.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Response` with a "pong" message.
    async fn ping(&self) -> Result<Response, ErrorObjectOwned> {
        info!("Received `ping` method");
        Ok(Response::new(None, true, "pong".to_string()))
    }

    /// Handles the `fetch_key_share` Fetches current key share for the epoch
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Response` with a "keyshares" message.
    async fn fetch_key_share(&self, api_token: String) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `fetch_key_share` method");
        if API_TOKEN.lock().await.used {
            return Ok(NodeResponse::Error {
                request_id: "".to_string(),
                message: "API token has already been used".to_string(),
            });
        }
        if API_TOKEN.lock().await.token != api_token {
            return Ok(NodeResponse::Error {
                request_id: "".to_string(),
                message: "Invalid API token".to_string(),
            });
        }
        let app_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = app_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::FetchKeyShare(tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("".to_string())))?;

        match rx.await {
            Ok(r) => {
                API_TOKEN.lock().await.used = true;
                Ok(r)
            }
            Err(e) => {
                error!("Error occurred while sending RPC request to app state engine actor for fetching keyshare: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `backup` Backup shares and all relevant data
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Response` with a "bool" message.
    async fn backup(&self) -> Result<bool, ErrorObjectOwned> {
        info!("Received `backup` method");
        let app_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let _ = app_engine_status_actor.cast(AppStateChangeMessage::BackupState);
        Ok(true)
    }

    /// Handles the `restore_key_share` restores current key share for the epoch
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn restore_key_share(&self, keyshares: HashMap<String, String>) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `restore_key_share` method");
        let app_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = app_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::RestoreKeyShare(keyshares, tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to app state engine actor for restoring keyshare: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `fetch_election_state` fetches the election state
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn fetch_election_state(&self, peer_id_str: String) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `fetch_election_state` method");
        let app_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let peer_id = match PeerId::from_str(&peer_id_str) {
            Ok(peer_id) => peer_id,
            Err(_) => {
                return Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "invalid PeerId provided".to_string(),
                })
            }
        };
        let _ = app_engine_status_actor
            .call(
                |_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::FetchElectionState(peer_id, tx),
                Some(Duration::from_millis(5000)),
            )
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to app state engine actor for fetching election state: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `restore_election_state` restores election state
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn restore_election_state(&self) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `restore_election_state` method");
        let app_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = app_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::RestoreElectionState(tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to app state engine actor for restoring election state: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `quic_ping` RPC request to check the server's availability.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Response` with a "pong" message.
    async fn quic_ping(&self, peer_id_str: String) -> Result<Response, ErrorObjectOwned> {
        info!("Received `quic_ping` method");
        let app_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let peer_id = match PeerId::from_str(&peer_id_str) {
            Ok(peer_id) => peer_id,
            Err(_) => {
                return Ok(Response::new(None, false, "invalid PeerId provided".to_string()));
            }
        };
        let _ = app_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::QuicPing(peer_id, tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to ping peer_id".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to ping the peers: {:?}", e);
                Ok(Response::new(None, false, format!("Failed to ping the peer_id {} error {}", peer_id_str, e)))
            }
        }
    }

    /// Handles the `sync_peer_data` RPC request to sync the peer data for the relay node (fetch provers and update their info).
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn sync_peer_data(&self) -> Result<Response, ErrorObjectOwned> {
        info!("Received `sync_peer_data` method");
        let app_state_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = app_state_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::SyncPeerData(tx), Some(Duration::from_millis(10000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to sync peer data".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to sync peer data : {:?}", e.to_string());
                Ok(Response::new(None, false, format!("Failed to sync peer dataerror {}", e)))
            }
        }
    }

    /// Handles the `fetch_election_info` RPC request to fetch election info about a prover node (related to quorum election).
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn fetch_election_info(&self, peer_id_str: String) -> Result<ElectionResponse, ErrorObjectOwned> {
        info!("Received `fetch_election_info` method");
        let app_state_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let peer_id = match PeerId::from_str(&peer_id_str) {
            Ok(peer_id) => peer_id,
            Err(_) => {
                return Ok(ElectionResponse::err_response("invalid PeerId provided".to_string()));
            }
        };
        let _ = app_state_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::FetchElectionInfo(peer_id, tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to fetch election info".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to fetch election info: {:?}", e);
                Ok(ElectionResponse::err_response(format!("Failed to fetch election info : {}", e.to_string())))
            }
        }
    }

    /// Handles the `fetch_voting_power` RPC request to fetch voting power of a prover node.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn fetch_voting_power(&self, peer_id_str: String) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `fetch_voting_power` method");
        let app_state_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let peer_id = match PeerId::from_str(&peer_id_str) {
            Ok(peer_id) => peer_id,
            Err(_) => {
                return Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "invalid PeerId provided".to_string(),
                });
            }
        };
        let _ = app_state_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::FetchVotingPower(peer_id, tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to fetch voting power".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to fetch voting power: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: format!("Failed to fetch voting power : {}", e),
                })
            }
        }
    }

    /// Handles the `set_resharing_enabled` RPC request to set the resharing enabled flag for the network.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn set_resharing_enabled(&self, api_token: String, is_resharing_enabled: bool) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `set_resharing_enabled` method");
        if API_TOKEN.lock().await.token != api_token {
            return Ok(NodeResponse::Error {
                request_id: "".to_string(),
                message: "Invalid API token".to_string(),
            });
        }
        let app_state_engine_status_actor: ActorRef<AppStateChangeMessage> = get_actor_ref::<AppStateChangeMessage, AppStateEngineError>(ActorType::AppStateEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = app_state_engine_status_actor
            .call(
                |_: RpcReplyPort<NodeResponse>| AppStateChangeMessage::SetResharingEnabled(is_resharing_enabled, tx),
                Some(Duration::from_millis(5000)),
            )
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to set resharing enabled flag".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to set resharing enabled: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: format!("Failed to set resharing enabled flag : {}", e),
                })
            }
        }
    }

    /// Handles the `get_current_version` RPC request to fetch the current running version of the node.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn get_current_version(&self) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `get_current_version` method");
        Ok(NodeResponse::Version {
            version: format!("Human Node Version: {}", HUMAN_NETWORK_VERSION),
        })
    }

    /// Handles the `add_to_exclusion_list` RPC request to add the node's peer id to the exclusion list.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn add_to_exclusion_list(&self, api_token: String, peer_id_str: String) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `add_to_exclusion_list` method");
        if API_TOKEN.lock().await.token != api_token {
            return Ok(NodeResponse::Error {
                request_id: "".to_string(),
                message: "Invalid API token".to_string(),
            });
        }
        let election_engine_status_actor: ActorRef<ElectionEngineMessage> = get_actor_ref::<ElectionEngineMessage, ElectionEngineError>(ActorType::ElectionEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let peer_id = match PeerId::from_str(&peer_id_str) {
            Ok(peer_id) => peer_id,
            Err(_) => {
                return Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "invalid PeerId provided".to_string(),
                })
            }
        };
        let _ = election_engine_status_actor
            .call(
                |_: RpcReplyPort<NodeResponse>| ElectionEngineMessage::AddToExclusionList(peer_id, tx),
                Some(Duration::from_millis(5000)),
            )
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to add to exclusion list".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to election engine actor for adding to exclusion list: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `remove_from_exclusion_list` RPC request to remove the node's peer id from the exclusion list.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn remove_from_exclusion_list(&self, api_token: String, peer_id_str: String) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `remove_from_exclusion_list` method");
        if API_TOKEN.lock().await.token != api_token {
            return Ok(NodeResponse::Error {
                request_id: "".to_string(),
                message: "Invalid API token".to_string(),
            });
        }
        let election_engine_status_actor: ActorRef<ElectionEngineMessage> = get_actor_ref::<ElectionEngineMessage, ElectionEngineError>(ActorType::ElectionEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let peer_id = match PeerId::from_str(&peer_id_str) {
            Ok(peer_id) => peer_id,
            Err(_) => {
                return Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "invalid PeerId provided".to_string(),
                })
            }
        };
        let _ = election_engine_status_actor
            .call(
                |_: RpcReplyPort<NodeResponse>| ElectionEngineMessage::RemoveFromExclusionList(peer_id, tx),
                Some(Duration::from_millis(5000)),
            )
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to remove from exclusion list".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to election engine actor for removing from exclusion list: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `get_current_excluded_peers` RPC request to retrieve the current excluded peers.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn get_current_excluded_peers(&self) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `get_current_excluded_peers` method");
        let election_engine_status_actor: ActorRef<ElectionEngineMessage> = get_actor_ref::<ElectionEngineMessage, ElectionEngineError>(ActorType::ElectionEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = election_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| ElectionEngineMessage::GetExclusionList(tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to get current excluded peers".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to election engine actor for getting current excluded peers: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `get_connected_peers` RPC request to retrieve the connected peers.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn get_connected_peers(&self) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `get_connected_peers` method");
        let gossip_engine_status_actor: ActorRef<GossipEngineMessage> = get_actor_ref::<GossipEngineMessage, GossipEngineError>(ActorType::GossipEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = gossip_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| GossipEngineMessage::GetConnectedPeers(tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to get connected peers".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to gossip engine actor for getting connected peers: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `get_peer_scores` RPC request to retrieve the peer scores.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn get_peer_scores(&self) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `get_peer_scores` method");
        let gossip_engine_status_actor: ActorRef<GossipEngineMessage> = get_actor_ref::<GossipEngineMessage, GossipEngineError>(ActorType::GossipEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = gossip_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| GossipEngineMessage::GetPeerScores(tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to get peer scores".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to gossip engine actor for getting peer scores: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `get_mesh_peers` RPC request to retrieve the mesh peers.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn get_mesh_peers(&self) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `get_mesh_peers` method");
        let gossip_engine_status_actor: ActorRef<GossipEngineMessage> = get_actor_ref::<GossipEngineMessage, GossipEngineError>(ActorType::GossipEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = gossip_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| GossipEngineMessage::GetMeshPeers(tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to get mesh peers".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to gossip engine actor for getting mesh peers: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `update_election_params` RPC request to update the election parameters.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn update_election_params(&self, api_token: String, params: ElectionParams) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `update_election_params` method");
        if API_TOKEN.lock().await.token != api_token {
            return Ok(NodeResponse::Error {
                request_id: "".to_string(),
                message: "Invalid API token".to_string(),
            });
        }
        let election_engine_status_actor: ActorRef<ElectionEngineMessage> = get_actor_ref::<ElectionEngineMessage, ElectionEngineError>(ActorType::ElectionEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = election_engine_status_actor
            .call(
                |_: RpcReplyPort<NodeResponse>| ElectionEngineMessage::UpdateElectionParams(params, tx),
                Some(Duration::from_millis(5000)),
            )
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to update election params".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to election engine actor for updating election params: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }

    /// Handles the `get_election_params` RPC request to retrieve the current election parameters.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either the `Response` on success or an `ErrorObjectOwned` on failure.
    async fn get_election_params(&self) -> Result<NodeResponse, ErrorObjectOwned> {
        info!("Received `get_election_params` method");
        let election_engine_status_actor: ActorRef<ElectionEngineMessage> = get_actor_ref::<ElectionEngineMessage, ElectionEngineError>(ActorType::ElectionEngine).unwrap();
        let (tx, rx) = oneshot::channel();
        let _ = election_engine_status_actor
            .call(|_: RpcReplyPort<NodeResponse>| ElectionEngineMessage::GetElectionParams(tx), Some(Duration::from_millis(5000)))
            .await
            .map_err(|e| ErrorObjectOwned::owned(1, e.to_string(), Some("Failed to get current election params".to_string())))?;

        match rx.await {
            Ok(r) => Ok(r),
            Err(e) => {
                error!("Error occurred while sending RPC request to election engine actor for getting current election params: {:?}", e);
                Ok(NodeResponse::Error {
                    request_id: "".to_string(),
                    message: "Something went wrong while processing the request".to_string(),
                })
            }
        }
    }
}
