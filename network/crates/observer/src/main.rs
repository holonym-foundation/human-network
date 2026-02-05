use ark_ec::twisted_edwards::Affine;
use ark_ed_on_bn254::EdwardsConfig;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Query, State},
    http::{
        header::{CONTENT_TYPE, COOKIE, SET_COOKIE},
        HeaderValue, Method,
    },
    routing::{get, post},
    Json, Router,
};
use ethers::{
    abi::Address,
    core::{k256::elliptic_curve::PrimeField, types::H160},
    signers::{LocalWallet, Signer},
};
use futures::stream::TryStreamExt;
use human_crypto::{
    encryption::DecryptionContractSignature,
    schnorrsig::{SchnorrSig, SchnorrSignable},
    BabyJubJub, Curve,
};
use mongodb::bson;
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use sha2::Sha512;
use sha3::{Digest, Sha3_256};
use std::{collections::HashMap, env, net::SocketAddr, str::FromStr};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use volonym::{
    actors::actors::{CommitAndProof, Verifier},
    Fr,
};
mod db;
mod circuits;
mod constants;
mod custom_errors;
mod orders;
mod sui_relayer;
mod sign_protocol;
mod verax;
use custom_errors::Error;
#[derive(Clone)]
struct AppState {
    mongodb_collections: db::ObserverMongoDBCollections,
    wallet: LocalWallet,
}
#[tokio::main]
async fn main() {
    // Make sure env vars are set
    env::var("CLEAN_HANDS_ISSUER_ADDRESS").expect("CLEAN_HANDS_ISSUER_ADDRESS must be set");
    env::var("MONGODB_URI").expect("MONGODB_URI must be set");
    env::var("NODEJS_SCRIPTS_PATH").expect("NODEJS_SCRIPTS_PATH must be set");
    env::var("CIRCUIT_DIR").expect("CIRCUIT_DIR must be set");
    env::var("OP_RPC_URL").expect("OP_RPC_URL must be set");
    env::var("ID_SERVER_URL").expect("ID_SERVER_URL must be set");
    env::var("ID_SERVER_API_KEY").expect("ID_SERVER_API_KEY must be set");
    sui_relayer::init_sui().await;
    let private_key = env::var("ATTESTOR_PRIVATE_KEY").expect("ATTESTOR_PRIVATE_KEY must be set");
    let wallet = LocalWallet::from_str(&private_key).expect("Failed to parse private key");
    println!("Attestor EVM wallet address: {:?}", wallet.address());
    let mongodb_collections = db::ObserverMongoDBCollections::init().await.unwrap();
    let state = AppState { mongodb_collections, wallet };
    let cors_layer = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::PUT])
        .allow_headers([COOKIE, CONTENT_TYPE])
        .expose_headers([SET_COOKIE])
        .allow_credentials(true)
        .allow_origin([
            "https://silksecure.net".parse::<HeaderValue>().unwrap(),
            "https://staging.silksecure.net".parse::<HeaderValue>().unwrap(),
            "http://localhost:3000".parse::<HeaderValue>().unwrap(),
        ]);
    let app = Router::new()
        .route("/observations", post(post_observation))
        .route("/observations/v2", post(post_observation_v2))
        // We don't have a v2 endpoint for Sui because we only really care about charging for EVM
        // attestations right now.
        .route("/observations/sui", post(post_observation_sui))
        .route("/observations", get(get_observations))
        .layer(TraceLayer::new_for_http())
        .layer(cors_layer)
        .layer(DefaultBodyLimit::max(20 * 1024 * 1024)) // 20 MB
        .with_state(state);
    tracing_subscriber::fmt().with_max_level(get_tracing_level()).init();
    // run it
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
#[derive(Serialize, Deserialize)]
struct Observation {
    pub commitment_and_proof: Bytes,
    /// User's signature of the access condition contract
    pub contract_and_sig: DecryptionContractSignature,
    /// For Sui, we also accept the user's address so that we know who to send the SBT to
    pub recipient: Option<String>
}
#[derive(Debug, Serialize, Deserialize)]
pub struct EVMRelayerResult {
    tx_hash: Option<String>,
    error: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct PostObservationEVMResp {
    sign_protocol_result: EVMRelayerResult,
    verax_result: EVMRelayerResult,
}
#[derive(Serialize, Deserialize)]
struct TxDigestSimple {
    tx_digest: String,
}
fn get_tracing_level() -> tracing::Level {
    let tracing_level = env::var("TRACING_LEVEL").unwrap_or("debug".to_string());
    if tracing_level == "debug" {
        return tracing::Level::DEBUG;
    } else if tracing_level == "info" {
        return tracing::Level::INFO;
    } else if tracing_level == "warn" {
        return tracing::Level::WARN;
    } else {
        panic!("Unsupported TRACING_LEVEL '{}'", tracing_level);
    }
}
fn calculate_sui_recipient_commitment(recipient: &[u8; 32]) -> BigUint {
    let mut hasher = Sha512::new();
    hasher.update(recipient);
    let hash_result = hasher.finalize();
    let hash_biguint = BigUint::from_bytes_be(&hash_result);
    let modulus = BigUint::from_str(constants::BABY_JUB_JUB_MODULUS).unwrap();
    hash_biguint % modulus
}
pub enum AddressType {
    EVM,
    Sui
}
/// If addr_type is Sui, then recipient_in_proof is a commitment to a recipient, and we
/// need to validate it against the raw recipient.
fn parse_recipient(
    recipient_in_proof: &[u8; 32],
    addr_type: AddressType,
    recipient: &Option<String>
) -> Result<String, Error> {
    let res = match addr_type {
        AddressType::EVM => {
            let parsed = BigUint::from_bytes_be(recipient_in_proof);
            let recipient = format!("0x{:0>40x}", recipient);
            let parsed = H160::from_str(&parsed).map_err(|_e| Error::CustomBadRequest("Could not parse recipient into H160"))?;
            // For some reason to_string() on H160 abbreviates the address, so we convert it to string this way
            format!("0x{}", hex::encode(&parsed.as_bytes().to_vec()))
        },
        AddressType::Sui => {
            let purported_recipient = recipient.as_ref().ok_or(Error::CustomBadRequest("Recipient must be provided for Sui"))?;
            if purported_recipient.len() != 66 {
                return Err(Error::CustomBadRequest("Invalid Sui recipient address length"));
            }
            let purported_recipient_hex = hex::decode(purported_recipient.trim_start_matches("0x"))
                .map_err(|_e| Error::CustomBadRequest("Could not decode recipient hex string"))?;
            let purported_recipient_bytes =  purported_recipient_hex.as_slice().try_into()
                .map_err(|_| Error::CustomBadRequest("Recipient hex string is not 32 bytes long"))?;
            let expected_commitment = calculate_sui_recipient_commitment(purported_recipient_bytes); // Skip "0x" prefix
            let actual_commitment = BigUint::from_bytes_be(recipient_in_proof);
            if expected_commitment != actual_commitment {
                return Err(Error::CustomBadRequest("Recipient commitment in proof does not match the expected recipient commitment"));
            }
            purported_recipient.clone()
        }
    };
    Ok(res)    
}
struct ParsedZKP {
    pubkey1: [[u8; 32]; 2],
    pubkey2: [[u8; 32]; 2],
    pubkey3: [[u8; 32]; 2],
    recipient: String,
    public_values_array: Vec<Fr>,
    action_id: String,
    action_nullifier: String,
    expiry: chrono::DateTime<chrono::Utc>,
}
fn validate_zkp(
    commitment_and_proof: &Bytes,
    addr_type: AddressType,
    recipient: &Option<String>
) -> Result<ParsedZKP, Error> {
    // -------- Verify zkp --------
    let circuits::Circuit { circuit, inlabels, outlabels } = circuits::CIRCUITS.get("V3CleanHands").ok_or(Error::CustomBadRequest("Encountered unknown error while verifying ZKP"))?;
    let verifier = Verifier::from_circuit(circuit.clone());
    let cnp: CommitAndProof<Fr> = bincode::deserialize(commitment_and_proof.as_ref()).map_err(|_e| Error::CustomBadRequest("Could not deserialize commitment and proof"))?;
    let result = verifier.verify(&cnp).map_err(|_e| Error::CustomBadRequest("Could not verify commitment and proof"))?;
    // -------- Validate public signals --------
    // Get labels for public inputs and outputs
    let mut public_inputs = HashMap::new();
    let mut public_outputs = HashMap::new();
    for (i, label) in inlabels.iter().enumerate() {
        public_inputs.insert(label.clone(), result.public_inputs[i].to_repr().0);
    }
    for (i, label) in outlabels.iter().enumerate() {
        public_outputs.insert(label.clone(), result.public_outputs[i].to_repr().0);
    }
    // println!("public_inputs: {:?}", public_inputs);
    // println!("public_outputs: {:?}", public_outputs);
    let mut public_values_array = result.public_inputs.clone();
    public_values_array.append(&mut result.public_outputs.clone());
    // const actionNullifier = publicSignals[0]
    // const issuerAddress = publicSignals[1]
    // const pubkey1 = [publicSignals[2], publicSignals[3]]
    // const ciphertext1 = [publicSignals[4], publicSignals[5]]
    // const pubkey2 = [publicSignals[6], publicSignals[7]]
    // const ciphertext2 = [publicSignals[8], publicSignals[9]]
    // const pubkey3 = [publicSignals[10], publicSignals[11]]
    // const ciphertext3 = [publicSignals[12], publicSignals[13]]
    // const encryptedTo = [publicSignals[14], publicSignals[15]]
    // const expiry = publicSignals[16]
    // const recipient = publicSignals[17]
    // const actionId = publicSignals[18]
    let issuer_address = public_outputs.get("issuerAddress").ok_or(Error::CustomBadRequest("Could not parse issuerAddress"))?;
    let issuer_address = BigUint::from_bytes_be(issuer_address).to_str_radix(10);
    if issuer_address != *constants::CLEAN_HANDS_ISSUER_ADDRESS {
        return Err(Error::CustomBadRequest("Issuer address does not match clean hands issuer"));
    }
    let encrypted_to = [
        public_inputs.get("encryptedTox").ok_or(Error::CustomBadRequest("Could not parse encryptedTox"))?,
        public_inputs.get("encryptedToy").ok_or(Error::CustomBadRequest("Could not parse encryptedToy"))?,
    ];
    if encrypted_to[0].to_owned() != constants::HUMAN_PUBKEY[0] || encrypted_to[1].to_owned() != constants::HUMAN_PUBKEY[1] {
        return Err(Error::CustomBadRequest("Public key does not match expected public key"));
    }
    let expiry = public_inputs.get("expiry").ok_or(Error::CustomBadRequest("Could not parse expiry"))?;
    let expiry: i64 = BigUint::from_bytes_be(expiry).try_into().map_err(|_e| Error::CustomBadRequest("Could not convert expiry into i64"))?;
    let expiry = chrono::DateTime::from_timestamp(expiry, 0).ok_or(Error::CustomBadRequest("Could not parse expiry"))?;
    if expiry < chrono::Utc::now() {
        return Err(Error::CustomBadRequest("Credentials have expired"));
    }
    let recipient_in_proof: &[u8; 32] = public_inputs.get("recipient").ok_or(Error::CustomBadRequest("Could not parse recipient"))?;
    let recipient = parse_recipient(recipient_in_proof, addr_type, recipient)?;
    let action_id = public_inputs.get("actionId").ok_or(Error::CustomBadRequest("Could not parse actionId"))?;
    let action_id = BigUint::from_bytes_be(action_id).to_str_radix(10);
    let pubkey1 = [
        public_outputs.get("pubkey1x").ok_or(Error::CustomBadRequest("Could not parse pubkey1x"))?,
        public_outputs.get("pubkey1y").ok_or(Error::CustomBadRequest("Could not parse pubkey1y"))?,
    ];
    let pubkey2 = [
        public_outputs.get("pubkey2x").ok_or(Error::CustomBadRequest("Could not parse pubkey2x"))?,
        public_outputs.get("pubkey2y").ok_or(Error::CustomBadRequest("Could not parse pubkey2y"))?,
    ];
    let pubkey3 = [
        public_outputs.get("pubkey3x").ok_or(Error::CustomBadRequest("Could not parse pubkey3x"))?,
        public_outputs.get("pubkey3y").ok_or(Error::CustomBadRequest("Could not parse pubkey3y"))?,
    ];
    if pubkey1 != pubkey2 || pubkey2 != pubkey3 {
        return Err(Error::CustomBadRequest("Each c1 must be the same as the rest"));
    }
    let action_nullifier = public_outputs.get("actionNullifier").ok_or(Error::CustomBadRequest("Could not parse actionId"))?;
    let action_nullifier = format!("0x{}", hex::encode(action_nullifier));
    Ok(ParsedZKP {
        pubkey1: [pubkey1[0].clone(), pubkey1[1].clone()],
        pubkey2: [pubkey2[0].clone(), pubkey2[1].clone()],
        pubkey3: [pubkey3[0].clone(), pubkey3[1].clone()],
        recipient,
        public_values_array,
        action_id,
        action_nullifier,
        expiry,
    })
}
fn verify_access_contract_signature(pubkey1: [[u8; 32]; 2], body: &Observation) -> Result<SchnorrSig<32, BabyJubJub>, Error> {
    // -------- Verify signature of access contract --------
    let pubkey = Affine::<EdwardsConfig> {
        x: BigUint::from_bytes_be(&pubkey1[0]).into(),
        y: BigUint::from_bytes_be(&pubkey1[1]).into(),
    };
    let signature: SchnorrSig<32, BabyJubJub> = body.contract_and_sig.sig.clone();
    let contract = body.contract_and_sig.contract.as_bytes();
    <BabyJubJub as Curve<32>>::Scalar::verify(contract, &pubkey, &signature).map_err(|_e| Error::CustomBadRequest("Could not verify signature"))?;
    Ok(signature)
}
fn get_observation_id(contract: &H160, recipient: &String, action_nullifier: &String) -> Result<String, Error> {
    // -------- id is a hash of recipient, action_nullifier, access_contract --------
    let access_contract = serde_json::to_string(contract).map_err(|_e| Error::CustomBadRequest("Could not serialize access contract"))?;
    let mut hasher = Sha3_256::new();
    hasher.update(format!("{}{}{}", recipient, action_nullifier, access_contract).as_bytes());
    let id = hasher.finalize().as_slice().to_vec();
    Ok(format!("0x{}", hex::encode(&id)))
}
pub fn parse_relay_output(output: Vec<u8>) -> Option<String> {
    let output = String::from_utf8(output).ok()?;
    if output.is_empty() {
        None
    } else {
        Some(output)
    }
}
/// Contains logic common to v1 and v2 POST /observation endpoints.
async fn _post_observation(state: &AppState, body: &Observation, validate_order: bool) -> Result<Json<PostObservationEVMResp>, Error> {
    let ParsedZKP {
        pubkey1,
        pubkey2: _pubkey2,
        pubkey3: _pubkey3,
        recipient,
        public_values_array,
        action_id,
        action_nullifier,
        expiry,
    } = validate_zkp(&body.commitment_and_proof, AddressType::EVM, &None)?;
    // -------- Validate access contract --------
    if !constants::ACCESS_CONDITION_CONTRACTS.contains(&body.contract_and_sig.contract) {
        return Err(Error::CustomBadRequest("Access contract not allowed"));
    }
    let signature = verify_access_contract_signature(pubkey1, &body)?;
    // -------- Validate payment --------
    let mut order_id: Option<String> = None;
    if validate_order {
        order_id = Some(crate::orders::validate_order(&action_nullifier).await?);
    }
    // -------- Get observation id --------
    let id = get_observation_id(&body.contract_and_sig.contract, &recipient, &action_nullifier)?;
    // println!("recipient: {:?}", recipient);
    // println!("expiry: {:?}", expiry);
    // println!("ciphertext1: {:?}", ciphertext1);
    // println!("ciphertext2: {:?}", ciphertext2);
    // println!("ciphertext3: {:?}", ciphertext3);
    // println!("issuer_address: {:?}", issuer_address);
    // TODO: Maybe: Check blacklist of addresses to make sure we aren't creating an attestation for a known bad actor
    let attester_addr = format!("0x{}", hex::encode(&state.wallet.address().as_bytes().to_vec()));
    // -------- Make sure user doesn't already have an attestation --------
    let attestations_resp = sign_protocol::get_attestations(attester_addr, &action_nullifier).await?;
    if attestations_resp.data.rows.len() > 0 {
        return Err(Error::CustomBadRequest("User already has a Clean Hands attestation on Sign Protocol"));
    }
    // -------- Upsert Observation into DB --------
    // (Upsert, not insert, in case the user already has an observation from calling the Sui endpoint)
    let observation_doc = db::ObservationSchema {
        id: id.clone(),
        user_address: Some(recipient.clone().to_lowercase()),
        user_sui_address: None,
        signature: serde_json::to_string(&signature).map_err(|_e| Error::CustomBadRequest("Could not serialize signature"))?,
        access_contract: format!("0x{}", hex::encode(&body.contract_and_sig.contract.as_bytes().to_vec())),
        zkp_public_values: public_values_array.iter().map(|v| hex::encode(v.to_repr().0.to_vec())).collect::<Vec<String>>(),
        order_id: order_id.clone()
    };
    let _result = &state
        .mongodb_collections
        .observations
        .update_one(
            bson::doc! { "id": id.clone() },
            bson::doc! {
                "$set": bson::to_bson(&observation_doc).map_err(|_e| Error::CustomBadRequest("Could not serialize observation to bson"))?
            },
            mongodb::options::UpdateOptions::builder().upsert(true).build()
        )
        .await
        .map_err(|_e| Error::CustomBadRequest("Failed to insert observation into MongoDB"))?;
    // ----- Issue attestation on Sign Protocol -----
    let sign_protocol_output = sign_protocol::issue_attestation(&recipient, &action_id, &action_nullifier, expiry.timestamp()).await?;
    let sign_protocol_result = EVMRelayerResult {
        tx_hash: parse_relay_output(sign_protocol_output.stdout),
        error: parse_relay_output(sign_protocol_output.stderr),
    };
    // ----- Issue attestation on Verax -----
    let verax_output = verax::issue_attestation(&recipient, &action_id, expiry.timestamp()).await?;
    let verax_result = EVMRelayerResult {
        tx_hash: parse_relay_output(verax_output.stdout),
        error: parse_relay_output(verax_output.stderr),
    };
    // ----- Mark order fulfilled if the user received their Sign Protocol attestation -----
    if let Some(order_id) = order_id {
        if sign_protocol_result.tx_hash.is_some() {
            crate::orders::mark_order_fulfilled(order_id).await?;
        }
    }
    // ----- Return -----
    Ok(Json(PostObservationEVMResp {
        sign_protocol_result,
        verax_result
    }))
}
async fn post_observation(State(state): State<AppState>, Json(body): Json<Observation>) -> Result<Json<PostObservationEVMResp>, Error> {
    _post_observation(&state, &body, false).await
}
/// This is the same as the v1 endpoint, except it requires payment via the zeronym orders service.
async fn post_observation_v2(State(state): State<AppState>, Json(body): Json<Observation>) -> Result<Json<PostObservationEVMResp>, Error> {
    _post_observation(&state, &body, true).await
}
async fn post_observation_sui(
    State(state): State<AppState>,
    Json(body): Json<Observation>
) -> Result<Json<TxDigestSimple>, Error> {
    let ParsedZKP {
        pubkey1,
        pubkey2: _pubkey2,
        pubkey3: _pubkey3,
        recipient,
        public_values_array,
        action_id,
        action_nullifier,
        expiry,
    } = validate_zkp(&body.commitment_and_proof, AddressType::Sui, &body.recipient)?;
    // -------- Validate access contract --------
    if !constants::ACCESS_CONDITION_CONTRACTS.contains(&body.contract_and_sig.contract) {
        return Err(Error::CustomBadRequest("Access contract not allowed"));
    }
    let signature = verify_access_contract_signature(pubkey1, &body)?;
    // -------- Get observation id --------
    let id = get_observation_id(&body.contract_and_sig.contract, &recipient, &action_nullifier)?;
    // println!("recipient: {:?}", recipient);
    // println!("expiry: {:?}", expiry);
    // println!("ciphertext1: {:?}", ciphertext1);
    // println!("ciphertext2: {:?}", ciphertext2);
    // println!("ciphertext3: {:?}", ciphertext3);
    // println!("issuer_address: {:?}", issuer_address);
    // TODO: Maybe: Check blacklist of addresses to make sure we aren't creating an attestation for a known bad actor
    // -------- Upsert Observation into DB --------
    let observation_doc = db::ObservationSchema {
        id: id.clone(),
        user_address: None,
        user_sui_address: Some(recipient.clone().to_lowercase()),
        signature: serde_json::to_string(&signature).map_err(|_e| Error::CustomBadRequest("Could not serialize signature"))?,
        access_contract: format!("0x{}", hex::encode(&body.contract_and_sig.contract.as_bytes().to_vec())),
        zkp_public_values: public_values_array.iter().map(|v| hex::encode(v.to_repr().0.to_vec())).collect::<Vec<String>>(),
        order_id: None
    };
    let _result = &state
        .mongodb_collections
        .observations
        .update_one(
            bson::doc! { "id": id.clone() },
            bson::doc! {
                "$set": bson::to_bson(&observation_doc).map_err(|_e| Error::CustomBadRequest("Could not serialize observation to bson"))?
            },
            mongodb::options::UpdateOptions::builder().upsert(true).build()
        )
        .await
        .map_err(|_e| Error::CustomBadRequest("Failed to upsert observation into MongoDB"))?;
    // // ----- Mint Sui SBT -----
    let tx_digest = sui_relayer::mint_sui_sbt(
        recipient,
        constants::CLEAN_HANDS_CIRCUIT_ID.to_string(),
        action_id,
        action_nullifier,
        expiry.timestamp()
    ).await?;
    Ok(Json(TxDigestSimple { tx_digest }))
}
#[derive(Serialize, Deserialize)]
struct GetObservationsQuery {
    user_address: Option<String>,
    user_sui_address: Option<String>
}
#[derive(Serialize, Deserialize, Debug)]
struct ObservationRespItem {
    zkp_public_values: Vec<String>,
    access_contract: String,
    signature: String,
    user_address: Option<String>,
    user_sui_address: Option<String>,
}
#[axum::debug_handler]
async fn get_observations(State(state): State<AppState>, Query(query): Query<GetObservationsQuery>) -> Result<Json<Vec<ObservationRespItem>>, Error> {
    let user_address = if let Some(addr) = query.user_address {
        let _ = Address::from_str(&addr).map_err(|_| Error::CustomBadRequest("Invalid user address"))?;
        Some(addr.to_lowercase())
    } else {
        None
    };
    let user_sui_address = if let Some(addr) = query.user_sui_address {
        let _ = Address::from_str(&addr).map_err(|_| Error::CustomBadRequest("Invalid user address"))?;
        Some(addr.to_lowercase())
    } else {
        None
    };
    let cursor = state
        .mongodb_collections
        .observations
        .find(
            bson::doc! {
                "user_address": user_address,
                "user_sui_address": user_sui_address
            },
            None,
        )
        .await
        .map_err(|_e| Error::CustomBadRequest("Failed to query MongoDB"))?;
    let observations: Vec<db::ObservationSchema> = cursor.try_collect().await.map_err(|_e| Error::CustomBadRequest("Failed to collect observations"))?;
    let observations = observations
        .into_iter()
        .map(|doc| ObservationRespItem {
            zkp_public_values: doc.zkp_public_values,
            access_contract: doc.access_contract,
            signature: doc.signature,
            user_address: doc.user_address,
            user_sui_address: doc.user_sui_address,
        })
        .collect();
    Ok(Json(observations))
}
#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn parse_recipient_succeeds() {
        let recipient_in_proof = hex::decode("1c99bf8b37fd7b7a52ec60ad7fb4f945c8819caaa32316b9fa0c58088bda95").unwrap();
        let mut recipient_in_proof_bytes = [0u8; 32];
        let copy_len = recipient_in_proof.len().min(recipient_in_proof_bytes.len());
        let start_idx = recipient_in_proof_bytes.len() - copy_len;
        recipient_in_proof_bytes[start_idx..].copy_from_slice(&recipient_in_proof[..copy_len]);
        let recipient = "0x643b7f742bb118e732b280c8ae03c537d23d0b34aef4e9e249bac024087cc1d0".to_string();
        let parsed = parse_recipient(
            &recipient_in_proof_bytes,
            AddressType::Sui,
            &Some(recipient.clone())
        );
        match parsed {
            Ok(addr) => {
                println!("Parsed recipient: {}", addr);
                assert_eq!(addr, recipient);
            }
            Err(e) => {
                println!("Error parsing recipient: {:?}", e);
                panic!("Failed to parse recipient: {:?}", e);
            }
        }
    }

    // TODO: Uncomment
    // #[tokio::test]
    // async fn test_post_and_get_observation() {
    //     env::set_var("MONGODB_URI", "mongodb://localhost:27017");
    //     let state = AppState {
    //         mongodb_collections: ObserverMongoDBCollections::init().await.unwrap(),
    //     };

    //     let _result = state.mongodb_collections.observations
    //         .delete_one(mongodb::bson::doc! {
    //             "id": "0xad6f4d0e5fca2a16be19e1ad30bc5d0a4e674c5af121cb23609f9e2fa40a2b45"
    //         }, None)
    //         .await.unwrap();

    //     let expiry = "1747627070".to_string();
    //     let access_contract = Address::from_str("0x69e3373c6165045c3c59a11645415eff8fd15cac").unwrap(),
    //
    //     let body = Observation {
    //         zkp: Groth16ProofResult {
    //             proof: Groth16Proof {
    //                 pi_a: vec![
    //                     "3598117036270896762472915174892584489924076490705604042788219362066437493463".to_string(),
    //                     "2121611527018126103545783444075642341076500978251405114966109815439258569760".to_string(),
    //                     "1".to_string(),
    //                 ],
    //                 pi_b: vec![
    //                     vec![
    //                         "16077857283895985712943414649406175969576975920835353867256253086592525757822".to_string(),
    //                         "19598254911271645867621274105296055871045830766568505189375943698013892936980".to_string(),
    //                     ],
    //                     vec![
    //                         "14898526257376807998890843891703925947953056477045642790402041628168976451240".to_string(),
    //                         "18371071430441672703756777723534816655264825530785297320801315594273003145416".to_string(),
    //                     ],
    //                     vec![
    //                         "1".to_string(),
    //                         "0".to_string(),
    //                     ],
    //                 ],
    //                 pi_c: vec![
    //                     "21470920691113413627391105724607034783102331228639969156318063505874360814160".to_string(),
    //                     "15842145510430253376921940385777415732611939971329000965791004672644626948137".to_string(),
    //                     "1".to_string(),
    //                 ],
    //                 protocol: "groth16".to_string(),
    //                 curve: "bn128".to_string(),
    //             },
    //             public_signals: vec![
    //                 "5385733246434316323272813865927015212139262996967409164502674354450565082216".to_string(),
    //                 "3953516660401541564649985379958697237340496801951929947163239598560489169274".to_string(),
    //                 "7298940672059965768641845964270773272795981288081284939122224850165910347243".to_string(),
    //                 "7926082103045993969094841716375912351275414485995177415265999219539763466679".to_string(),
    //                 "14440256215838718387954991175858749361368934584034933881354100501137534214861".to_string(),
    //                 "7515372452966299157851033460487112678305947476655671376282536166047296797064".to_string(),
    //                 "7298940672059965768641845964270773272795981288081284939122224850165910347243".to_string(),
    //                 "7926082103045993969094841716375912351275414485995177415265999219539763466679".to_string(),
    //                 "17041274316254614346954076311177997760534338228080636326040571383319246980244".to_string(),
    //                 "17516839086985873123996955627189648750143235623145965183359433475285566444699".to_string(),
    //                 "7298940672059965768641845964270773272795981288081284939122224850165910347243".to_string(),
    //                 "7926082103045993969094841716375912351275414485995177415265999219539763466679".to_string(),
    //                 "4336400771443682401090448669306731079272970162404888523321032689570302685630".to_string(),
    //                 "13099544837885240857829652566818460201515352386475574935246802208503035688581".to_string(),
    //                 expiry.clone(),
    //                 "1255056909650833413315090512811037118861410631801".to_string(),
    //                 "123456789".to_string()
    //             ],
    //         },
    //         access_contract: access_contract.clone(),
    //         signature: Signature::from_str("0x8bd929f960179d616a4d2f1f7793c0ea3da3d40603d6cbba676a025a4c13205e1fa27836f48c4b0d8a62fefe615da67e9ca83e70957ff684114da399d28423c81c").unwrap(),
    //     };

    //     let result = post_observation(State(state.clone()), Json(body)).await;

    //     assert!(result.is_ok());

    //     let query = GetObservationsQuery {
    //         user_address: "0xdbd6b2c02338919edaa192f5b60f5e5840a50079".to_string(),
    //     };

    //     let result: Result<Json<Vec<ObservationRespItem>>, Error> = get_observations(State(state), Query(query)).await;

    //     let result = result.unwrap();
    //     assert_eq!(result.0[0].user_address, "0xdbd6b2c02338919edaa192f5b60f5e5840a50079");
    //     assert_eq!(result.0[0].access_contract[0], format!("0x{}", hex::encode(&access_contract[0].as_bytes().to_vec())));
    //     assert_eq!(result.0[0].zkp.public_signals[14], expiry);
    // }
}
