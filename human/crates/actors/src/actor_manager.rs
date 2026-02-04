use crate::app_state_actor::AppStateSupervisorError;
use crate::dkg_engine_actor::{DKGEngineActor, DkgEngineSupervisorError};
use crate::election_actor::{ElectionEngineActor, ElectionEngineSupervisorError};
use crate::gossip_engine_actor::{GossipEngineActor, GossipEngineSupervisorError};
use crate::{app_state_actor::AppStateEngineActor, get_actor_ref};
use libp2p::{Multiaddr, PeerId};
use messages::actor_type::{ActorType, SupervisorType};
use messages::kafka::KafkaProducer;
use messages::message::{AppStateChangeMessage, DkgEngineMessage, ElectionEngineMessage, GossipEngineMessage, Message};
use network::utils::NodeType;
use ractor::{concurrency::JoinHandle, Actor, ActorRef};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
type GossipEngineArgs = (
    UnboundedSender<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>,
    UnboundedReceiver<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>,
);
/// Represents an actor spawn result, containing an actor reference and its join handle.
pub struct SpawnedActor<M: Sized> {
    _actor_ref: ActorRef<M>,      // The reference to the spawned actor
    _join_handle: JoinHandle<()>, // The join handle for the spawned actor's task
}
impl<M: Sized> SpawnedActor<M> {
    /// Creates a new `SpawnedActor` instance with the given actor reference and join handle.
    pub fn new(actor_ref: ActorRef<M>, join_handle: JoinHandle<()>) -> Self {
        Self {
            _actor_ref: actor_ref,
            _join_handle: join_handle,
        }
    }
}
/// Container for managing actor supervisors and their children.
pub struct ActorManager {
    app_state: SpawnedActor<AppStateChangeMessage>,
    election_engine: SpawnedActor<ElectionEngineMessage>,
    dkg_engine: SpawnedActor<DkgEngineMessage>,
    gossip_engine: SpawnedActor<GossipEngineMessage>,
}
#[derive(Debug, Error)]
pub enum ActorManagerError {
    #[error("A {0} actor that experienced a panic was successfully respawned, but the supervisor actor could not be retrieved from the registry.")]
    ActorRestartError(ractor::ActorName),

    #[error("{0}")]
    Custom(String),
}
impl Default for ActorManagerError {
    fn default() -> Self { ActorManagerError::Custom("AppStateEngine unable to acquire actor".to_string()) }
}
impl ActorManager {
    /// Reboots the App State Engine actor with the given parameters and updates the actor manager.
    pub async fn reboot_app_state_engine(
        actor_manager: Arc<Mutex<ActorManager>>,
        actor_name: ractor::ActorName,
        args: (PeerId, PeerId, NodeType, String, Multiaddr, Option<Arc<KafkaProducer>>),
        handler: AppStateEngineActor,
    ) -> Result<(), ActorManagerError> {
        // Get the supervisor actor reference for App State Engine
        let supervisor_ref =
            get_actor_ref::<AppStateChangeMessage, AppStateSupervisorError>(SupervisorType::AppEngineStateSuperVisor).ok_or_else(|| ActorManagerError::ActorRestartError(actor_name.clone()))?;
        // Spawn the App State Engine actor and get its actor reference and join handle
        let (actor_ref, join_handle) = Actor::spawn_linked(Some(actor_name.clone()), handler, args, supervisor_ref.get_cell())
            .await
            .map_err(|e| ActorManagerError::Custom(e.to_string()))?;
        // Update the App State Engine actor in the actor manager
        actor_manager.lock().await.app_state = SpawnedActor::new(actor_ref, join_handle);
        Ok(())
    }
    /// Reboots the Election Engine actor with the given parameters and updates the actor manager.
    pub async fn reboot_election_engine(
        actor_manager: Arc<Mutex<ActorManager>>,
        actor_name: ractor::ActorName,
        args: (NodeType, PeerId, PeerId, Multiaddr, Option<Arc<KafkaProducer>>),
        handler: ElectionEngineActor,
    ) -> Result<(), ActorManagerError> {
        // Get the supervisor actor reference for Election Engine
        let supervisor_ref =
            get_actor_ref::<ElectionEngineMessage, ElectionEngineSupervisorError>(SupervisorType::ElectionEngineSuperVisor).ok_or_else(|| ActorManagerError::ActorRestartError(actor_name.clone()))?;
        // Spawn the Election Engine actor and get its actor reference and join handle
        let (actor_ref, join_handle) = Actor::spawn_linked(Some(actor_name.clone()), handler, args, supervisor_ref.get_cell())
            .await
            .map_err(|e| ActorManagerError::Custom(e.to_string()))?;
        // Update the Election Engine actor in the actor manager
        actor_manager.lock().await.election_engine = SpawnedActor::new(actor_ref, join_handle);
        Ok(())
    }
    /// Reboots the DKG Engine actor with the given parameters and updates the actor manager.
    pub async fn reboot_dkg_engine(actor_manager: Arc<Mutex<ActorManager>>, actor_name: ractor::ActorName, args: (), handler: DKGEngineActor) -> Result<(), ActorManagerError> {
        // Get the supervisor actor reference for DKG Engine
        let supervisor_ref =
            get_actor_ref::<DkgEngineMessage, DkgEngineSupervisorError>(SupervisorType::DkgEngineSuperVisor).ok_or_else(|| ActorManagerError::ActorRestartError(actor_name.clone()))?;
        // Spawn the DKG Engine actor and get its actor reference and join handle
        let (actor_ref, join_handle) = Actor::spawn_linked(Some(actor_name.clone()), handler, args, supervisor_ref.get_cell())
            .await
            .map_err(|e| ActorManagerError::Custom(e.to_string()))?;
        // Update the DKG Engine actor in the actor manager
        actor_manager.lock().await.dkg_engine = SpawnedActor::new(actor_ref, join_handle);
        Ok(())
    }
    /// Reboots the Gossip Engine actor with the given parameters and updates the actor manager.
    pub async fn reboot_gossip_engine(actor_manager: Arc<Mutex<ActorManager>>, actor_name: ractor::ActorName, args: GossipEngineArgs, handler: GossipEngineActor) -> Result<(), ActorManagerError> {
        // Get the supervisor actor reference for Gossip Engine
        let supervisor_ref =
            get_actor_ref::<GossipEngineMessage, GossipEngineSupervisorError>(SupervisorType::GossipEngineSuperVisor).ok_or_else(|| ActorManagerError::ActorRestartError(actor_name.clone()))?;
        // Spawn the Gossip Engine actor and get its actor reference and join handle
        let (actor_ref, join_handle) = Actor::spawn_linked(Some(actor_name.clone()), handler, args, supervisor_ref.get_cell())
            .await
            .map_err(|e| ActorManagerError::Custom(e.to_string()))?;
        // Update the Gossip Engine actor in the actor manager
        actor_manager.lock().await.gossip_engine = SpawnedActor::new(actor_ref, join_handle);
        Ok(())
    }
}
/// Custom async builder for constructing an [`ActorManager`].
#[derive(Default)]
pub struct ActorManagerBuilder {
    app_state_engine: Option<SpawnedActor<AppStateChangeMessage>>,
    election_engine: Option<SpawnedActor<ElectionEngineMessage>>,
    dkg_engine: Option<SpawnedActor<DkgEngineMessage>>,
    gossip_engine: Option<SpawnedActor<GossipEngineMessage>>,
}
impl ActorManagerBuilder {
    /// Spawns the App State Engine actor and updates the state with the spawned actor information.
    pub async fn app_state_engine(
        mut self,
        app_state_engine_actor: AppStateEngineActor,
        app_state_engine_supervisor: ActorRef<AppStateChangeMessage>,
        args: (PeerId, PeerId, NodeType, String, Multiaddr, Option<Arc<KafkaProducer>>),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Spawn the App State Engine actor and get its actor reference and join handle
        let (actor_ref, join_handle) = Actor::spawn_linked(Some(ActorType::AppStateEngine.to_string()), app_state_engine_actor, args, app_state_engine_supervisor.get_cell())
            .await
            .map_err(Box::new)?;
        // Update the state with the spawned actor information
        self.app_state_engine = Some(SpawnedActor::new(actor_ref, join_handle));
        Ok(self)
    }
    /// Spawns the Election Engine actor and updates the state with the spawned actor information.
    pub async fn election_engine(
        mut self,
        election_engine_actor: ElectionEngineActor,
        election_engine_supervisor: ActorRef<ElectionEngineMessage>,
        args: (NodeType, PeerId, PeerId, Multiaddr, Option<Arc<KafkaProducer>>),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Spawn the Election Engine actor and get its actor reference and join handle
        let (actor_ref, join_handle) = Actor::spawn_linked(Some(ActorType::ElectionEngine.to_string()), election_engine_actor, args, election_engine_supervisor.get_cell())
            .await
            .map_err(Box::new)?;
        // Update the state with the spawned actor information
        self.election_engine = Some(SpawnedActor::new(actor_ref, join_handle));
        Ok(self)
    }
    /// Spawns the DKG Engine actor and updates the state with the spawned actor information.
    pub async fn dkg_engine(mut self, dkg_engine_actor: DKGEngineActor, dkg_engine_supervisor: ActorRef<DkgEngineMessage>, args: ()) -> Result<Self, Box<dyn std::error::Error>> {
        // Spawn the DKG Engine actor and get its actor reference and join handle
        let (actor_ref, join_handle) = Actor::spawn_linked(Some(ActorType::DkgEngine.to_string()), dkg_engine_actor, args, dkg_engine_supervisor.get_cell())
            .await
            .map_err(Box::new)?;
        // Update the state with the spawned actor information
        self.dkg_engine = Some(SpawnedActor::new(actor_ref, join_handle));
        Ok(self)
    }
    /// Spawns the Gossip Engine actor and updates the state with the spawned actor information.
    pub async fn gossip_engine(
        mut self,
        gossip_engine_actor: GossipEngineActor,
        gossip_engine_supervisor: ActorRef<GossipEngineMessage>,
        args: GossipEngineArgs,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Spawn the Gossip Engine actor and get its actor reference and join handle
        let (actor_ref, join_handle) = Actor::spawn_linked(Some(ActorType::GossipEngine.to_string()), gossip_engine_actor, args, gossip_engine_supervisor.get_cell())
            .await
            .map_err(Box::new)?;
        // Update the state with the spawned actor information
        self.gossip_engine = Some(SpawnedActor::new(actor_ref, join_handle));
        Ok(self)
    }
    pub fn build(self) -> ActorManager {
        ActorManager {
            app_state: self.app_state_engine.expect("app engine actor failed to start"),
            election_engine: self.election_engine.expect("election engine actor failed to start"),
            dkg_engine: self.dkg_engine.expect("dkg engine actor failed to start"),
            gossip_engine: self.gossip_engine.expect("gossip engine actor failed to start"),
        }
    }
}
