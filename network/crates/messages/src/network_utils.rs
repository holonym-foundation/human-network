/* trunk-ignore-all(rustfmt) */
// Standard library imports
use std::env;
use std::sync::Arc;
use std::{collections::HashMap, u128};
// Third-party library imports
use anyhow::{anyhow, Error};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use ethers::{
    abi::Address,
    contract::abigen,
    core::types::Signature,
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
    types::H160,
};
use lazy_static::lazy_static;
use redis::{Connection, FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{error, info};
// Project-specific imports
use crate::jwt::verify_jwt;
use crate::kafka::{KafkaProducer, KafkaTopic, UserCreditsByMethod};
use human_crypto::{
    encryption::ElGamalCiphertextWithSignedConditions,
    node_selection::{calc_nodes_to_send_to, should_be_sent_to_node},
    zkinjmask::FrontendData,
    BabyJubJub, Curve, PointTrait, Secp256k1,
};
// const HUMAN_TOKEN_TESTNET: &str = "0x757Ab92207D730DF72A4B8Fb2c540F206ac647A9";
pub const HUMAN_CREDITS: &str = "0x18494fecf61d2282c45b8bf481403c1fcb5d94e6";
lazy_static! {
    pub static ref CONDITIONS_CONTRACT: H160 = "0x3a0d4A524Aa53A29959Aaef1Cff899F35Cc7F766".parse().unwrap();
}
// Generate the ABI of the Human Credits RobustNet contract (ETH Mainnet)
abigen!(
    HumanCreditsRobustNet,
    r#"[
    function creditsFor(address user) public view returns (uint256)
    ]"#
);
// // Generate the ABI of the HumanToken contract and include it in the struct HumanToken
// abigen!(
//     HumanToken,
//     r#"[
//     function balanceOf(address account) external view returns (uint256)
//     ]"#
// );
// A decrypt contract says how many credits a decryptor has for decrypting a type of point.
// This is somewhat unusal as one would expect the function to return a boolean value of whether it can decrypt this particular point.
// A uint is more flexible as it says "look, i can't tell on-chain how many times you have decrypted from my contact's rules so here is the maximum amount you are allowed to have decrypted by now"
abigen!(
    DecryptContract,
    r#"[
    function canDecrypt(address decryptor, bytes32 c1Hash) external view returns (bool)
    ]"#
);
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Hash)]
pub enum Method {
    OPRFSecp256k1,
    OPRFBabyJubJub,
    DecryptBabyJubJub,
    JWTPRFSecp256k1,
}
/// Calculates the "standard" credits, i.e. how many they have purchased on mainnnet
async fn standard_credits(user: Address, rpc: &Provider<Http>) -> Result<u128, Error> {
    // return Ok(u128::MAX);
    #[cfg(any(all(test, not(feature = "enable_credits_for_tests")), feature = "mock_credits"))]
    {
        Ok(env::var("HUMAN_MOCK_CREDITS").unwrap_or("3".to_string()).parse()?)
    }
    #[cfg(not(any(all(test, not(feature = "enable_credits_for_tests")), feature = "mock_credits")))]
    {
        let address: Address = HUMAN_CREDITS.parse()?;
        let rpc_clone = Arc::new(rpc);
        let contract = Arc::new(Mutex::new(HumanCreditsRobustNet::new(address, Arc::new(rpc_clone))));
        #[cfg(not(feature = "v0nodes"))]
        {
            // Execute blocking code and handle the Result correctly
            let rt = tokio::runtime::Runtime::new().unwrap();
            let balance_result = rt.block_on(async {
                let contract = contract.lock().await;
                contract.credits_for(user).call().await
            });
            match balance_result {
                Ok(balance) => Ok(balance.as_u128()),
                Err(e) => Err(anyhow!("Failed to get user's credits: {}", e)),
            }
        }
        #[cfg(feature = "v0nodes")]
        {
            let balance_result = contract.lock().await.credits_for(user).call().await;
            match balance_result {
                Ok(balance) => Ok(balance.as_u128()),
                Err(e) => Err(anyhow!("Failed to get user's credits: {}", e).into()),
            }
        }
    }
}
/// It's important that method includes the curve name if there are more than one curves supported for one method. Otherwise,
/// A user could replay a request from one curve to another curve. This does not seem like an impactful attack vector at the moment,\
/// but it is a good practice to include the curve name in the method name. The verifier also does not see which curve the request is from
/// so it would have difficulty verifying requests for different curves; it will think they are the same
/// Alternatively, we could just add the curve name as a different field in signed request, but this is simpler.
impl Method {
    pub fn as_str(&self) -> &'static str {
        match self {
            Method::OPRFSecp256k1 => "OPRFSecp256k1",
            Method::OPRFBabyJubJub => "OPRFBabyJubJub",
            Method::DecryptBabyJubJub => "DecryptBabyJubJub",
            Method::JWTPRFSecp256k1 => "JWTPRFSecp256k1",
        }
    }
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "OPRFSecp256k1" => Some(Method::OPRFSecp256k1),
            "OPRFBabyJubJub" => Some(Method::OPRFBabyJubJub),
            "DecryptBabyJubJub" => Some(Method::DecryptBabyJubJub),
            "JWTPRFSecp256k1" => Some(Method::JWTPRFSecp256k1),
            _ => None,
        }
    }
    /// Gets the number of credits a user has. Credits are treated exactly the same by the network: 1 credit = 1 response.
    /// This function makes it so it's flexible how each method decides to give out credits, based on their own rules on-chain
    pub async fn get_user_credits(&self, user: Address, rpc: &Provider<Http>, point: Vec<u8>, extra_data: Option<Vec<u8>>, conn: &mut MutexGuard<'_, Connection>) -> Result<u128, Error> {
        match self {
            Method::OPRFSecp256k1 => standard_credits(user, rpc).await,
            Method::OPRFBabyJubJub => standard_credits(user, rpc).await,
            Method::DecryptBabyJubJub => {
                // The maximum amount of decryptions they can do is their standard credits, but they can also be limited by the custom decryption conditions to a smaller number.
                // Therefore we check the minimum of the two
                let standard_credits = standard_credits(user, rpc).await?;
                // Check the extra data are access conditions signed by the point we will multiply
                if let Some(extra_data) = extra_data {
                    let data: ElGamalCiphertextWithSignedConditions = bincode::deserialize(&extra_data)?;
                    let (contract, pubkey) = data.get_contract_and_pubkey_if_valid()?;
                    let point = <BabyJubJub as Curve<32>>::Point::from_encoded(&point)?;
                    if point != pubkey {
                        return Err(anyhow!("Point provided does not match the public key in the signature"));
                    }
                    let contract = DecryptContract::new(contract, Arc::new(rpc.clone()));
                    // TODO: Maybe don't encode using encode()? It would be easier in javascript, for
                    // example, if we just hash the concatenation of c1.x and c1.y.
                    let c1 = data.ciphertext.ephemeral_dh_pubkey;
                    let c1_hash = ethers::core::utils::keccak256(c1.encode());
                    #[cfg(not(feature = "v0nodes"))]
                    let can_decrypt = { contract.can_decrypt(user, c1_hash).call().await };
                    #[cfg(feature = "v0nodes")]
                    let can_decrypt = contract.can_decrypt(user, c1_hash).call().await;
                    match can_decrypt {
                        Ok(can_decrypt) => {
                            if can_decrypt {
                                Ok(standard_credits)
                            } else {
                                Ok(0)
                            }
                        }
                        Err(e) => Err(anyhow!("Failed to get user's credits: {}", e)),
                    }
                } else {
                    Err(anyhow!("No extra data provided for DecryptBabyJubJub method"))
                }
            }
            Method::JWTPRFSecp256k1 => {
                let standard_credits = standard_credits(user, rpc).await?;
                match extra_data {
                    Some(raw_data) => {
                        let fd: FrontendData<32, Secp256k1> = bincode::deserialize(&raw_data)?;
                        let result = match verify_jwt(&fd.jwt, fd.key_idx, Some(conn)).await {
                            Ok(result) => result,
                            // if it fails, try again without the cache (for edge case key has recently rotated)
                            Err(_) => verify_jwt(&fd.jwt, fd.key_idx, None).await?,
                        };
                        let (unique_str, public_mask) = result;
                        let _ = fd
                            .mask_proof
                            .verify_and_get_output(&<Secp256k1 as Curve<32>>::Point::from_encoded(&hex::decode(&public_mask)?)?, &unique_str)?;
                        Ok(standard_credits)
                    }

                    None => Err(anyhow!("No extra data provided for JWTPRFSecp256k1 method")),
                }
            }
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
// NOTE: If you modify this struct, please ensure you modify the validation logic for requests accordingly
pub struct RequestToNetwork /*<const N: usize, C: Curve<N>>*/ {
    /// The method of the request
    pub method: Method,
    /// Encoded point to be multiplied
    pub point: Vec<u8>, // C::Point,
    /// Which epoch this request is from
    pub epoch: u32,
    /// Which request number this is from this user. Starts at 1 not 0 to ensure the first request is paid for.
    pub request_per_user: u128,
    /// Signature of the request. If valid, this can be used to recover the address of the signer
    pub signature: Option<Signature>,
    /// Extra optional data
    pub extra_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestToNetworkWithProofs {
    /// Base request fields
    pub base: RequestToNetwork,
    /// Proofs associated with the request
    pub proofs: Vec<(String, String, String, String)>,
}

impl ToRedisArgs for RequestToNetwork {
    fn write_redis_args<W: redis::RedisWrite + ?Sized>(&self, out: &mut W) { out.write_arg(&bincode::serialize(self).unwrap()); }
}
impl FromRedisValue for RequestToNetwork {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> { Ok(bincode::deserialize(&redis::from_redis_value::<Vec<u8>>(v)?).unwrap()) }
}
impl ToRedisArgs for RequestToNetworkWithProofs {
    fn write_redis_args<W: redis::RedisWrite + ?Sized>(&self, out: &mut W) { out.write_arg(&bincode::serialize(self).unwrap()); }
}
impl FromRedisValue for RequestToNetworkWithProofs {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> { Ok(bincode::deserialize(&redis::from_redis_value::<Vec<u8>>(v)?).unwrap()) }
}
/// n is the number of nodes, t is the threshold, and epsilon is the number of extra nodes to query
impl RequestToNetwork {
    /// Determinstically maps a request to a set of nodes
    pub fn is_for_my_node(&self, t_plus_epsilon: u32, my_node_idx: u32, new_provers_idxes: Vec<usize>) -> bool {
        info!("Checking for indexes :{:#?}", new_provers_idxes);
        should_be_sent_to_node(self.epoch, self.request_per_user, t_plus_epsilon, my_node_idx, &new_provers_idxes)
    }
    /// Determinstically maps a request to a set of nodes
    pub fn get_nodes(&self, t_plus_epsilon: u32, nodes_idxes: Vec<usize>) -> Vec<u32> { calc_nodes_to_send_to(self.epoch, self.request_per_user, t_plus_epsilon, nodes_idxes) }
    /// Signs the request
    async fn _sign(&self, wallet: LocalWallet) -> Result<Signature, Error> { Ok(wallet.sign_message(&bincode::serialize(&self)?).await?) }
    /// Signs the request if it has an empty signature and returns a new request with the signature
    pub async fn signed(&self, wallet: LocalWallet) -> Result<Self, Error> {
        if self.signature.is_some() {
            return Err(anyhow!("Request already signed"));
        };
        let sig = self._sign(wallet).await?;
        let mut new = self.clone();
        new.signature = Some(sig);
        Ok(new)
    }
    /// Recovers the signer from the signed request
    pub fn recover(&self) -> Result<Address, Error> {
        match self.signature {
            Some(s) => Ok(s.recover(bincode::serialize(&{
                let mut new = self.clone();
                new.signature = None;
                new
            })?)?),
            None => Err(anyhow!("Request not signed")),
        }
    }

    /// Checks the following:
    ///
    /// * The epoch number is correct.
    /// * The request number is higher than the last recorded request number for this (user, method) tuple.
    /// * The user who signed it has enough credits to warrant a response.
    ///
    /// If all checks pass, updates the user's most recent request number and returns the amount it has increased by
    /// (current request number - last recorded request number for this user, epoch, method tuple).
    ///
    /// If any check fails, returns an error.
    pub async fn account_request_from_signer(&self, conn: &mut MutexGuard<'_, Connection>, rpc: &Provider<Http>, kafka: &KafkaProducer, record: bool) -> Result<u32, Error> {
        let user: H160 = self.recover()?;
        let credits: u128 = self.method.get_user_credits(user, rpc, self.point.clone(), self.extra_data.clone(), conn).await?;
        let method: &str = self.method.as_str();
        // Fetch stored requests per user from Redis
        let stored_requests_per_user: u32 = redis::cmd("GET")
            .arg(format!("total_count:{}:0x{}", method, hex::encode(user.0)))
            .query::<Option<u32>>(conn)?
            .unwrap_or(0);
        info!(
            "Stored credits per user {:?}:{:?}",
            format!("total_count:{}:0x{}", method, hex::encode(user.0)),
            stored_requests_per_user
        );
        let new_credits_per_user = stored_requests_per_user + 1;
        if new_credits_per_user > credits as u32 {
            return Err(anyhow!("No credits for a request: acquired {} credits but used {}", credits, stored_requests_per_user));
        }
        if record {
            info!("Updating total count :{} :{}", format!("total_count:{}:0x{}", method, hex::encode(user.0)), new_credits_per_user);
            // Update Redis with the new request count
            redis::cmd("SET")
                .arg(format!("total_count:{}:0x{}", method, hex::encode(user.0)))
                .arg(TryInto::<i64>::try_into(new_credits_per_user)?) // NOTE: this could cause an overflow if the user has sent a request number greater than 2^63-1.
                .query::<()>(conn)?;

            kafka
                .send(
                    KafkaTopic::Credits,
                    format!("0x{}", hex::encode(user.0)),
                    UserCreditsByMethod {
                        method: method.to_string(),
                        exhausted_credits: self.request_per_user,
                    },
                )
                .await;
        }
        Ok(1)
    }

    pub async fn verify_account_request_from_signer(&self, conn: &mut MutexGuard<'_, Connection>, rpc: &Provider<Http>) -> Result<(), Error> {
        let user: H160 = self.recover()?;
        let credits: u128 = self.method.get_user_credits(user, rpc, self.point.clone(), self.extra_data.clone(), conn).await?;
        let method: &str = self.method.as_str();
        // Fetch stored requests per user from Redis
        let stored_requests_per_user: u32 = redis::cmd("GET")
            .arg(format!("total_count:{}:0x{}", method, hex::encode(user.0)))
            .query::<Option<u32>>(conn)?
            .unwrap_or(0);
        info!(
            "Stored credits per user {:?}:{:?}",
            format!("total_count:{}:0x{}", method, hex::encode(user.0)),
            stored_requests_per_user
        );
        info!(
            "User Request No :{} Stored Request Per user :{}, self.request_per_user > stored_requests_per_user :{}",
            self.request_per_user,
            stored_requests_per_user,
            self.request_per_user as i64 > stored_requests_per_user as i64
        );
        if self.request_per_user as i64 > stored_requests_per_user as i64 {
            if self.request_per_user > credits {
                return Err(anyhow!(
                    "Request # provided: {} must be higher than the last recorded request number: {} for this user but it is higher than credits for user {}",
                    self.request_per_user,
                    stored_requests_per_user,
                    credits
                ));
            } else {
                info!("Updating total count :{} :{}", format!("total_count:{}:0x{}", method, hex::encode(user.0)), self.request_per_user);
                // Update Redis with the new request count
                redis::cmd("SET")
                    .arg(format!("total_count:{}:0x{}", method, hex::encode(user.0)))
                    .arg(TryInto::<i64>::try_into(self.request_per_user)?) // NOTE: this could cause an overflow if the user has sent a request number greater than 2^63-1.
                    .query::<()>(conn)?;
            }
        }

        if self.request_per_user > credits {
            return Err(anyhow!("No credits for a request: acquired {} credits but used {}", credits, self.request_per_user));
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
/// The request from teh user toget  state from the relayer
pub struct StateRequest {
    pub user: Address,
    pub method: Method,
}
#[derive(Serialize, Deserialize)]
/// For the relay node to let the user know their state and the current epoch number in case the user has lost track
pub struct StateResponse {
    pub epoch: u32,
    pub method: Method,
    pub requests_from_user: u128,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct NodeData {
    pub idx: u32,
    pub uri: String,
    /// They key is the curve name (e.g., Secp256k1)
    pub pubkeys: HashMap<String, Pubkeys>,
}
#[derive(Serialize, Deserialize)]
pub struct NodeDataWithoutIP {
    pub idx: u32,
    /// The key is the curve name, e.g. Secp256k1. The value is a hex string representing the public key
    pub pubkeys: HashMap<String, Pubkeys>,
}
/// The key is the method name, e.g. OPRF, Decrypt, etc. the value is a hex string representing the public key
pub type Pubkeys = HashMap<String, Vec<u8>>;
pub struct AVSError(anyhow::Error);
impl IntoResponse for AVSError {
    fn into_response(self) -> Response {
        let s = self.0.to_string();
        error!(name: "error", "Error: {}", s);
        (StatusCode::INTERNAL_SERVER_ERROR, s).into_response()
    }
}
impl<E> From<E> for AVSError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self { Self(err.into()) }
}
/// Sends test data to the test telemetry node
pub async fn testnet_logging<T: Serialize>(endpoint: &'static str, data: T) {
    let testing = std::env::var("HUMAN_NODE_TEST_MODE").unwrap_or("false".to_string()) == "true";
    if !testing {
        return;
    }
    let url = env::var("TESTNET_LOGGING_URL").expect("TESTNET_LOGGING_URL not set");
    let res = surf::post(format!("{}{}/", url, endpoint)).body_json(&data).unwrap().await;
    info!("Response {:#?}", res);
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::task_proofs;
    use ark_ec::{twisted_edwards::Affine, AffineRepr, CurveGroup};
    use ark_ed_on_bn254::EdwardsConfig;
    use ark_ff::UniformRand;
    use human_crypto::{
        encryption::{encrypt_with_conditions, DecryptionContractSignature, ElGamalCiphertext, ElGamalCiphertextWithSignedConditions},
        schnorrsig::SchnorrSig,
        BabyJubJub, Curve, PointTrait, ScalarTrait, Secp256k1,
    };
    use num_bigint::BigUint;
    use rand::{rngs::ThreadRng, thread_rng};
    use std::str::FromStr;
    use task_proofs::TaskProof;
    use tokio::sync::Mutex;
    async fn mock_request(request_number: u128, wallet: LocalWallet) -> RequestToNetworkWithProofs {
        let base = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            epoch: 1,
            request_per_user: request_number,
            point: <Secp256k1 as Curve<32>>::base_point_or_generator()
                .scalar_mul(&<Secp256k1 as Curve<32>>::Scalar::rand_vartime())
                .encode(),
            signature: None,
            extra_data: None,
        }
        .signed(wallet)
        .await
        .unwrap();
        RequestToNetworkWithProofs { base, proofs: vec![] }
    }
    async fn mock_state() -> (Arc<Mutex<Connection>>, Provider<Http>, KafkaProducer) {
        // Instead of giving a simple connection, we give a mutexed connection to accurately simulate the connection in the Axum State struct
        let conn = Arc::new(Mutex::new(redis::Client::open("redis://localhost").unwrap().get_connection().unwrap()));
        let rpc = Provider::<Http>::try_from("https://ethereum-holesky-rpc.publicnode.com").unwrap();
        let kafka = KafkaProducer::new("localhost:9092").await.unwrap();
        (conn, rpc, kafka)
    }
    fn get_redis_conn() -> Connection { redis::Client::open("redis://localhost").unwrap().get_connection().unwrap() }
    #[tokio::test]
    async fn test_signing_works() {
        let wallet = LocalWallet::new(&mut rand::thread_rng());
        let request = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 1,
            signature: None,
            extra_data: None,
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        assert_eq!(request.recover().unwrap(), wallet.address());
    }
    #[tokio::test]
    async fn test_recovering_fails_if_not_signed() {
        let request = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 1,
            signature: None,
            extra_data: None,
        };
        assert!(request.recover().is_err());
    }
    #[tokio::test]
    // This is pretty obvious test, copilot wrote it. But might as well keep it
    async fn test_recovering_not_different_address() {
        let wallet = LocalWallet::new(&mut rand::thread_rng());
        let wallet2 = LocalWallet::new(&mut rand::thread_rng());
        let request = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 1,
            signature: Some(wallet2.sign_message(b"hello").await.unwrap()),
            extra_data: None,
        };
        assert_ne!(request.recover().unwrap(), wallet.address());
    }
    #[tokio::test]
    async fn test_request_number_checking() {
        let (conn, rpc, kafka) = mock_state().await;
        let wallet = LocalWallet::new(&mut rand::thread_rng());
        let other_wallet = LocalWallet::new(&mut rand::thread_rng());
        let request = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 1,
            signature: None,
            extra_data: None,
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        // Check the first request results in an increment of one
        assert_eq!(request.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.unwrap(), 1);
        // Check that it cannot be replayed, even with a different signature or point
        assert!(request.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.is_err());
        let mut bad_request = request.clone();
        bad_request.point = vec![9u8; 32];
        bad_request.signature = None;
        let already_accounted = bad_request.signed(wallet.clone());
        assert!(already_accounted.await.unwrap().account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.is_err());
        // Check the second request results in an increment of one
        let request2 = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 2,
            signature: None,
            extra_data: None,
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        assert_eq!(request2.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.unwrap(), 1);
        // Check that more requets than credits fails
        let request3 = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 3,
            signature: None,
            extra_data: None,
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        request3.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.unwrap();
        let request4 = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 4,
            signature: None,
            extra_data: None,
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        assert!(request4.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.is_err());
        // Check that older request numbers are rejected
        let request5 = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 3,
            signature: None,
            extra_data: None,
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        assert!(request5.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.is_err());
        // ------   new tests from a fresh wallet:   -------- //
        // Check that if the user increments to a high number, it will count by more than one
        let request6 = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 2,
            signature: None,
            extra_data: None,
        }
        .signed(other_wallet.clone())
        .await
        .unwrap();
        assert_eq!(request6.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.unwrap(), 2);
        let request7 = RequestToNetwork {
            method: Method::OPRFSecp256k1,
            point: vec![123u8; 32], // fake point but doens't matter
            epoch: 123,
            request_per_user: 2,
            signature: None,
            extra_data: None,
        }
        .signed(other_wallet.clone())
        .await
        .unwrap();
        assert!(request7.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.is_err());
    }
    #[ignore]
    #[tokio::test]
    async fn wallet_credit_check() {
        #[cfg(any(not(feature = "enable_credits_for_tests"), feature = "mock_credits"))]
        panic!("This test is only valid when credits are enabled. To run it, please enable the feature 'enable_credits_for_tests, e.g.\ncargo test wallet_credit_check --features \"enable_credits_for_tests\" -- --ignored");
        let rpc = Provider::<Http>::try_from("https://ethereum-holesky-rpc.publicnode.com").unwrap();
        let wallet = LocalWallet::new(&mut rand::thread_rng());
        let credits = Method::OPRFSecp256k1
            .get_user_credits(wallet.address(), &rpc, vec![0; 32], None, &mut Mutex::new(get_redis_conn()).lock().await)
            .await
            .unwrap();
        assert!(credits == 0);
    }
    #[tokio::test]
    async fn test_verify_fails_if_one_proof_bad() {
        let (conn, rpc, _) = mock_state().await;
        let mut unsigned = mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await;
        unsigned.base.signature = None;
        // let mut invalid_sig = good_req.clone();
        let p = TaskProof {
            data: vec![
                mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await,
                mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await,
                unsigned,
            ],
        };
        assert!(p.verify().await.is_err());
        // only run this when bad signatures won't recover to an address with 3 credits, i.e. a more realistic credit system is used
        #[cfg(feature = "enable_credits_for_tests")]
        {
            let mut bad_sig = mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await;
            bad_sig.signature = Some(LocalWallet::new(&mut ThreadRng::default()).sign_message(";ascdjna;skjcna;skjn").await.unwrap());
            // let mut invalid_sig = good_req.clone();
            let p = TaskProof {
                data: vec![
                    mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await,
                    mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await,
                    bad_sig,
                ],
            };
            assert!(p.verify(&mut conn.lock().await, &rpc).await.is_err());
        }
    }
    #[ignore]
    #[tokio::test]
    async fn wallet_decryption_credit_check() {
        #[cfg(any(not(feature = "enable_credits_for_tests"), feature = "mock_credits"))]
        panic!("This test is only valid when credits are enabled. To run it, please enable the feature 'enable_credits_for_tests, e.g.\ncargo test wallet_credit_check --features \"enable_credits_for_tests\" -- --ignored");
        // This test won't pass unless this address actually has decryption permissions from the contract (which
        // expire after a certain period of time)
        let wallet_address = Address::from_str("0xb18399Ce7899278C1E2dcDe1dbd00FBAb0C970DF").unwrap();
        let ephemeral_dh_pubkey = Affine::<EdwardsConfig> {
            x: BigUint::from_str("19482562063003194920446873693165398845092736141517500498870809323679649973325")
                .expect("invalid x coordinate")
                .into(),
            y: BigUint::from_str("8153097391443425470691699743893360196493609532247535190899532191328773412149")
                .expect("invalid y coordinate")
                .into(),
        };
        let encrypted_msg = Affine::<EdwardsConfig> {
            x: BigUint::from_str("3977082762446862803754859714588464348618991913718763908125201855655984602966")
                .expect("invalid x coordinate")
                .into(),
            y: BigUint::from_str("9558900191324869715182848595185115538662725274536228335399954336167614187375")
                .expect("invalid x coordinate")
                .into(),
        };
        let ciphertext = ElGamalCiphertext::<32, BabyJubJub> { encrypted_msg, ephemeral_dh_pubkey };
        let contract = "0xe6bab4228ad23d59a1f1d69f1cb14c2ba29d91e9".parse::<Address>().unwrap();
        let sig = serde_json::from_str::<SchnorrSig<32, BabyJubJub>>(
            "{\"R\":\"01200000000000000065b9b2f1d3d7b6118da91e82911c8ae2b0b99b52e7722cb308f81a795d1b5f0a\",\"s\":\"8a90ccae0cfcdb7215cefe558cfb35e5d80fe8fedc12635ec49e054d576de405\"}",
        )
        .unwrap();
        let ciphertext_with_signed_conditions = ElGamalCiphertextWithSignedConditions {
            ciphertext,
            signed_conditions: DecryptionContractSignature { contract, sig },
        };
        let extra_data = bincode::serialize(&ciphertext_with_signed_conditions).ok();
        let rpc_url = env::var("L1_RPC").expect("L1_RPC not set");
        let rpc = Provider::<Http>::try_from(rpc_url).unwrap();
        let credits = Method::DecryptBabyJubJub
            .get_user_credits(wallet_address, &rpc, ephemeral_dh_pubkey.encode(), extra_data, &mut Mutex::new(get_redis_conn()).lock().await)
            .await
            .unwrap();
        assert!(credits == 3);
    }
    #[tokio::test]
    async fn test_verify_fails_if_too_few_requests() {
        let (conn, rpc, _) = mock_state().await;
        let p = TaskProof {
            data: vec![
                mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await,
                mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await,
            ],
        };
        assert!(p.verify().await.is_err());
        let p = TaskProof {
            data: vec![mock_request(2, LocalWallet::new(&mut rand::thread_rng())).await],
        };
        assert!(p.verify().await.is_err());
    }
    #[tokio::test]
    async fn test_verify_succeeds_one_user() {
        let (conn, rpc, _) = mock_state().await;
        let wallet = LocalWallet::new(&mut ThreadRng::default());
        // let p: TaskProof = serde_json::from_str(&r#"{"data":[{"method":"OPRFSecp256k1","masked_point":[3,97,125,233,14,179,13,243,53,30,87,107,163,76,133,16,108,136,172,203,150,12,148,151,63,73,174,123,243,119,233,193,55],"epoch":0,"request_per_user":1,"signature":{"r":"0xbe00d6367521b45a9727a3f5643f33dfad61e06c72ed33632af21409a70ceba","s":"0x173ed46d2a18cbeb6aa9348f9f5142dd2782eb69d068888a9628522ef4fb0b9f","v":28}},{"method":"OPRFSecp256k1","masked_point":[2,237,234,69,179,58,30,252,142,74,15,212,4,129,118,214,38,141,247,190,251,184,171,63,172,13,4,14,138,157,147,248,64],"epoch":0,"request_per_user":2,"signature":{"r":"0x199139fe2dcb7d583d32ab53a328df3f6a13452b167c0f3282b5fcd761365234","s":"0x396fc254c7a3fe30b4d4a13d0b887af0cc0f3b599fa15d599c4af2b81a4b26c1","v":27}},{"method":"OPRFSecp256k1","masked_point":[2,208,15,105,156,147,25,69,28,25,31,88,254,61,238,107,114,81,167,4,98,144,5,121,243,163,157,52,206,152,4,254,205],"epoch":0,"request_per_user":3,"signature":{"r":"0xef7067074c354b43d6839ccdf6ee67f5b80bef279ded1e764d932e72592f41e1","s":"0x389b9ef41fdf47c20f02dcc09df0f4255d697a543a6860001b8ad6882cc14ae","v":27}}]}"#).unwrap();
        let p = TaskProof {
            data: vec![mock_request(1, wallet.clone()).await, mock_request(2, wallet.clone()).await, mock_request(3, wallet.clone()).await],
        };
        assert!(p.verify().await.is_ok());
        let wallet2 = LocalWallet::new(&mut ThreadRng::default());
        let p = TaskProof {
            data: vec![mock_request(3, wallet2).await],
        };
        assert!(p.verify().await.is_ok());
    }
    #[tokio::test]
    async fn test_verify_succeeds_multiple_users() {
        let (conn, rpc, _) = mock_state().await;
        let p = TaskProof {
            data: vec![
                mock_request(2, LocalWallet::new(&mut rand::thread_rng())).await,
                mock_request(1, LocalWallet::new(&mut rand::thread_rng())).await,
            ],
        };
        assert!(p.verify().await.is_ok());
        let p = TaskProof {
            data: vec![
                mock_request(2, LocalWallet::new(&mut rand::thread_rng())).await,
                mock_request(2, LocalWallet::new(&mut rand::thread_rng())).await,
            ],
        };
        assert!(p.verify().await.is_ok());
        let p = TaskProof {
            data: vec![mock_request(3, LocalWallet::new(&mut rand::thread_rng())).await],
        };
        assert!(p.verify().await.is_ok());
    }
    #[tokio::test]
    /// Tests that the user who encrypted it signed the proper conditions contract with the public key
    async fn test_babyjubjub_sig_check() {
        let (conn, rpc, kafka) = mock_state().await;
        let wallet = LocalWallet::new(&mut ThreadRng::default());
        let msg = &[69u8; 24];
        // This contract allows a whitelisted address to decrypt up to 10000 messages
        let conditions_contract = "0x3a0d4A524Aa53A29959Aaef1Cff899F35Cc7F766".parse().unwrap();
        let ciphertext_with_signed_conditions = encrypt_with_conditions(msg, &<BabyJubJub as Curve<32>>::base_point_or_generator().mul_bigint(&[69]).into_affine(), conditions_contract).unwrap();
        let _right_point = ciphertext_with_signed_conditions.ciphertext.ephemeral_dh_pubkey.clone();
        let ciphertext_with_right_pubkey = ciphertext_with_signed_conditions;
        let wrong_point = Affine::<EdwardsConfig>::rand(&mut thread_rng());
        let mut ciphertext_with_wrong_pubkey = ciphertext_with_right_pubkey.clone();
        ciphertext_with_wrong_pubkey.ciphertext.ephemeral_dh_pubkey = wrong_point.clone();
        let fail_wrong_point = RequestToNetwork {
            method: Method::DecryptBabyJubJub,
            point: wrong_point.encode(),
            epoch: 123,
            request_per_user: 2,
            signature: None,
            extra_data: bincode::serialize(&ciphertext_with_right_pubkey).ok(),
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        assert_eq!(
            fail_wrong_point
                .account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true)
                .await
                .err()
                .unwrap()
                .to_string(),
            "Point provided does not match the public key in the signature"
        );
        let fail_wrong_pubkey = RequestToNetwork {
            method: Method::DecryptBabyJubJub,
            point: wrong_point.encode(),
            epoch: 123,
            request_per_user: 2,
            signature: None,
            extra_data: bincode::serialize(&ciphertext_with_wrong_pubkey).ok(),
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        assert_eq!(
            fail_wrong_pubkey
                .account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true)
                .await
                .err()
                .unwrap()
                .to_string(),
            "Invalid signature"
        );
    }
    #[tokio::test]
    async fn test_babyjubjub_decryption_accounting() {
        let (conn, rpc, kafka) = mock_state().await;
        let privkey = env::var("SUPER_SECRET_TEST_PRIVATE_KEY").expect("SUPER_SECRET_TEST_PRIVATE_KEY not set. Please get the test value from the Holonym team");
        let wallet = LocalWallet::from_str(&privkey).expect("Failed to parse private key");
        let other_wallet = LocalWallet::new(&mut ThreadRng::default());
        // clear the test data for the current wallet
        let _: () = redis::cmd("DEL")
            .arg(format!("total_count:{}:0x{}", Method::DecryptBabyJubJub.as_str(), hex::encode(wallet.address().0)).to_string())
            .query(&mut conn.lock().await)
            .unwrap();
        // // set the amount of standard credits to 2
        // env::set_var("HUMAN_MOCK_CREDITS", "2");
        let msg = &[69u8; 24];
        // This contract allows a whitelisted address to decrypt up to 10000 messages
        let conditions_contract = "0x3a0d4A524Aa53A29959Aaef1Cff899F35Cc7F766".parse().unwrap();
        let ciphertext_with_signed_conditions = encrypt_with_conditions(msg, &<BabyJubJub as Curve<32>>::base_point_or_generator().mul_bigint(&[69]).into_affine(), conditions_contract).unwrap();
        let should_succ = RequestToNetwork {
            method: Method::DecryptBabyJubJub,
            point: ciphertext_with_signed_conditions.ciphertext.ephemeral_dh_pubkey.encode(),
            epoch: 123,
            request_per_user: 1,
            signature: None,
            extra_data: bincode::serialize(&ciphertext_with_signed_conditions).ok(),
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        // This should fail because the user has enough custom credits but not enough standard credits
        let should_fail_insufficient_standard_credits = RequestToNetwork {
            method: Method::DecryptBabyJubJub,
            point: ciphertext_with_signed_conditions.ciphertext.ephemeral_dh_pubkey.encode(),
            epoch: 123,
            request_per_user: 4,
            signature: None,
            extra_data: bincode::serialize(&ciphertext_with_signed_conditions).ok(),
        }
        .signed(wallet.clone())
        .await
        .unwrap();
        // This should fail because the user has standard credits but not enough custom credits
        let should_fail_insufficient_custom_credits = RequestToNetwork {
            method: Method::DecryptBabyJubJub,
            point: ciphertext_with_signed_conditions.ciphertext.ephemeral_dh_pubkey.encode(),
            epoch: 123,
            request_per_user: 2,
            signature: None,
            extra_data: bincode::serialize(&ciphertext_with_signed_conditions).ok(),
        }
        .signed(other_wallet.clone())
        .await
        .unwrap();
        assert_eq!(should_succ.account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true).await.unwrap(), 1);
        assert_eq!(
            should_fail_insufficient_standard_credits
                .account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true)
                .await
                .err()
                .unwrap()
                .to_string(),
            "No credits for a request: acquired 3 credits but used 4"
        );
        assert_eq!(
            should_fail_insufficient_custom_credits
                .account_request_from_signer(&mut conn.lock().await, &rpc, &kafka, true)
                .await
                .err()
                .unwrap()
                .to_string(),
            "No credits for a request: acquired 0 credits but used 2"
        );
    }
}
