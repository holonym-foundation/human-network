use std::fmt;

use hex::{decode, encode};
use libp2p::identity::PublicKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
/// Human Public Key Wrapper
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HumanPublicKey(pub PublicKey);
impl Serialize for HumanPublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex_str = encode(self.0.encode_protobuf());
        serializer.serialize_str(&hex_str)
    }
}
impl<'de> Deserialize<'de> for HumanPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_str: String = Deserialize::deserialize(deserializer)?;
        let bytes = decode(hex_str).map_err(serde::de::Error::custom)?;
        let public_key = PublicKey::try_decode_protobuf(&bytes).map_err(serde::de::Error::custom)?;
        Ok(HumanPublicKey(public_key))
    }
}
impl HumanPublicKey {
    pub fn to_string(&self) -> String {
        encode(self.0.encode_protobuf())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MonitorEvent {
    DKG,
    Resharing,
    MultiplicationResult(String),
}

impl fmt::Display for MonitorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MonitorEvent::DKG => write!(f, "DKG"),
            MonitorEvent::Resharing => write!(f, "Resharing"),
            MonitorEvent::MultiplicationResult(uuid) => write!(f, "MultiplicationResult {}", uuid),
        }
    }
}
