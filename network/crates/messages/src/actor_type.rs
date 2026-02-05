use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::time::Duration;
pub const TIMEOUT_DURATION: Duration = tokio::time::Duration::from_millis(200);
#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub enum ActorType {
    GossipEngine,
    ElectionEngine,
    Relay,
    DkgEngine,
    AppStateEngine,
    Rpc,
}
impl fmt::Display for ActorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            ActorType::GossipEngine => "gossip_engine",
            ActorType::ElectionEngine => "election_engine",
            ActorType::Relay => "relay",
            ActorType::DkgEngine => "dkg_engine",
            ActorType::Rpc => "rpc",
            ActorType::AppStateEngine => "app_state_engine",
        };
        write!(f, "{}", s)
    }
}
pub trait IntoActorType {
    fn resolve_actor_type(&self) -> ActorType;
}
impl IntoActorType for ractor::ActorName {
    fn resolve_actor_type(&self) -> ActorType {
        match self.as_str() {
            "gossip_engine" => ActorType::GossipEngine,
            "election_engine" => ActorType::ElectionEngine,
            "relay" => ActorType::Relay,
            "dkg_engine" => ActorType::DkgEngine,
            "rpc" => ActorType::Rpc,
            "app_state_engine" => ActorType::AppStateEngine,
            _ => unreachable!("Actor name is not an ActorType variant"),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub enum SupervisorType {
    AppEngineStateSuperVisor,
    ElectionEngineSuperVisor,
    DkgEngineSuperVisor,
    GossipEngineSuperVisor,
}
impl fmt::Display for SupervisorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            SupervisorType::AppEngineStateSuperVisor => "app_engine_state_supervisor",
            SupervisorType::ElectionEngineSuperVisor => "election_engine_supervisor",
            SupervisorType::DkgEngineSuperVisor => "dkg_engine_supervisor",
            SupervisorType::GossipEngineSuperVisor => "gossip_engine_supervisor",
        };
        write!(f, "{}", s)
    }
}
