use dkg_engine_actor::DKGEngineError;
use election_actor::ElectionEngineError;
use ractor::{pg::GroupChangeMessage, ActorRef};
use std::collections::HashMap;
pub mod actor_manager;
pub mod app_state_actor;
pub mod cancellation_context;
pub mod dkg_engine_actor;
pub mod election_actor;
pub mod election_context;
pub mod gossip_engine_actor;
pub mod quorum_context;

pub const TRIGGER_RESHARING_DURATION: u64 = 86400;
pub const TOTAL_BOOTSTRAP_RELAY_NODES: usize = 2;
pub const DKG_RESHARING_WAIT_TIME: u64 = 120;
pub const MAX_RESHARING_ATTEMPTS_AT_A_TIME: u64 = 3;
pub const RESHARING_REATTEMPT_WAIT_TIME: u64 = 60*60;
pub const PEER_DISCOVERY_WAIT_TIME: u64 = 10;
pub const MUL_REPONSE_WAIT_TIME: u64 = 10;
pub const SLEEP_PING: u64 = 20;
pub const STATE_DIR: &str = "state";

#[derive(Debug)]
pub enum ProcessMessageError {
    ElectionEngineError(ElectionEngineError),
    DKGEngineError(DKGEngineError),
    Custom(String),
}
impl From<ElectionEngineError> for ProcessMessageError {
    fn from(err: ElectionEngineError) -> Self { ProcessMessageError::ElectionEngineError(err) }
}
impl From<DKGEngineError> for ProcessMessageError {
    fn from(err: DKGEngineError) -> Self { ProcessMessageError::DKGEngineError(err) }
}
/// The function `get_actor_ref` retrieves an actor reference based on the actor type provided.
///
/// # Arguments
///
/// * `actor_type`: The `actor_type` parameter is a type that implements the `ToString` trait, which
///   allows it to be converted into a `String`.
/// * `error_msg`: The `error_msg` parameter is a static string slice (`&'static str`) that contains an
///   \error message to be used if the actor reference cannot be found in the registry.
///
/// # Returns
///
/// This function returns a `Result` containing either an `ActorRef<A>` if the actor type is found in
/// the registry, or an `ActorProcessingErr` if the actor type is not found.
pub fn get_actor_ref<A, E>(actor_type: impl ToString) -> Option<ActorRef<A>>
where
    E: Default + std::error::Error + std::fmt::Debug,
{
    ractor::registry::where_is(actor_type.to_string())
        .ok_or_else(E::default)
        .map_err(|e| {
            tracing::error!("Error caught{:?}", e);
            e
        })
        .ok()
        .map(|actor_ref| actor_ref.into())
}
#[macro_export]
macro_rules! cast_message {
    ($actor_type:expr, $message:expr, $error_type:ty) => {{
        let actor_ref_option = $crate::get_actor_ref::<_, $error_type>($actor_type);
        if let Some(actor_ref) = actor_ref_option {
            if let Err(err) = actor_ref.cast($message) {
                tracing::info!("Failed to cast message: {:?}", err);
            }
        } else {
            tracing::info!("Failed to get actor reference for {:?}", $actor_type);
        };
    }};
}

pub fn group_changed(group_change_message: GroupChangeMessage) {
    match group_change_message {
        GroupChangeMessage::Join(scope, group, actors) => {
            tracing::warn!("Actor(s) {actors:?} have joined group {group:?} with scope {scope:?}")
        }
        GroupChangeMessage::Leave(scope, group, actors) => {
            tracing::warn!("Actor(s) {actors:?} have left group {group:?} with scope {scope:?}")
        }
    }
}
pub fn debug_key_shares(data: &HashMap<String, Vec<u8>>) -> HashMap<String, String> { data.iter().map(|(k, v)| (k.clone(), hex::encode(v))).collect() }
