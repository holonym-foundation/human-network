use libp2p::PeerId;
use messages::message::{ElectionInfo, ProverInfo};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ReElectionCheckContext {
    pub tstar_provers: HashMap<PeerId, (ProverInfo, usize)>,
    pub new_provers: HashMap<PeerId, (ProverInfo, usize)>,
    pub group_keys: HashMap<String, Vec<u8>>,
    pub election_info: ElectionInfo,
}
