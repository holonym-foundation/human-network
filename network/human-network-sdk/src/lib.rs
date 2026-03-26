#![feature(generic_const_exprs)]
use ark_ec::twisted_edwards::Affine;
use ark_ed_on_bn254::EdwardsConfig;
use ark_ff::PrimeField;
use ethers::{
    abi::encode,
    signers::{LocalWallet, Signer},
    types::{transaction::request, Address, Signature, H160},
};
use lazy_static::lazy_static;
use libp2p::PeerId;
use human_crypto::{
    curve::EncodedBabyJubJubPoint,
    encryption::{decrypt_elgamal_from_shared_secret, encrypt_with_conditions, DecryptionContractSignature, ElGamalCiphertext, ElGamalCiphertextWithSignedConditions, MapCurve},
    oprf_client_1,
    schnorrsig::{SchnorrSig, SchnorrSignable},
    twisted_conversion::TwistedEdwardsConversion,
    zkinjmask::{invert_mask_and_obtain_prf, FrontendData, JWTClaims, Mask, MaskProof},
    BabyJubJub, Curve, DLEQProof, PointTrait, ScalarTrait, Secp256k1,
};
use num_bigint::BigUint;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
// use wasm_bindgen_test::console_log;
use std::{collections::HashMap, fmt::Display, str::FromStr};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
extern crate console_error_panic_hook;
use std::panic;
mod utils;
use web_sys::console;

// Strictly for development. Set to true to enable logs
const ENABLE_LOGS: bool = false;
macro_rules! log {
    ($($arg:tt)*) => {
        if ENABLE_LOGS {
            console::log_1(&format!($($arg)*).into());
        }
    };
}

#[wasm_bindgen]
pub fn enable_errors() { panic::set_hook(Box::new(console_error_panic_hook::hook)); }
// const RELAY_URL: &'static str = "http://159.65.246.91:3031"; // http://localhost:3031
// const RELAY_URL: &'static str = "http://0.0.0.0:8081";
// Mainnet
const RELAY_URL: &'static str = "http://44.217.242.218:8081";
const POLL_INTERVAL_MS: i32 = 2000;
const MAX_POLL_ATTEMPTS: u32 = 30;

async fn async_sleep(ms: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let global = js_sys::global();
        let set_timeout = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
            .expect("setTimeout not found")
            .dyn_into::<js_sys::Function>()
            .expect("setTimeout is not a function");
        set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from(ms)).unwrap();
    });
    JsFuture::from(promise).await.unwrap();
}

fn to_js_err<E: Display>(e: E) -> JsError { JsError::new(&e.to_string()) }
// const TO_JS_ERR: Fn(dyn Error) -> JsError = |e| JsError::new(&e.to_string());
// fn map_jserr<T, E: Display>(e: Result<T, E>) -> Result<T, JsError> {
//     e.map_err(|e| JsError::new(&e.to_string()))
// }
/**
 * ---------------------------------------------------------------------------------------
 * Copied from messages::network_utils because the messages crate doesn't compile in wasm
 * ---------------------------------------------------------------------------------------
 */

/// Latest version.
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub enum StateResponse {
//     Success {
//         epoch: u32,
//         method: Method,
//         requests_from_user: u32,
//     },
//     Error {
//         message: String,
//     },
// }

/// Matches network/crates/messages/src/network_utils.rs StateResponse
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateResponse {
    epoch: u32,
    method: Method,
    requests_from_user: u128,
}
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Method {
    OPRFSecp256k1,
    OPRFBabyJubJub,
    DecryptBabyJubJub,
    JWTPRFSecp256k1,
}
impl Method {
    pub fn as_str(&self) -> &'static str {
        match self {
            Method::OPRFSecp256k1 => "OPRFSecp256k1",
            Method::OPRFBabyJubJub => "OPRFBabyJubJub",
            Method::DecryptBabyJubJub => "DecryptBabyJubJub",
            Method::JWTPRFSecp256k1 => "JWTPRFSecp256k1",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateRequest {
    pub user: Address,
    pub method: Method,
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
    /// Which request number this is from this user in this epoch. Starts at 1 not 0 to ensure the first request is paid for.
    pub request_per_user: u128,
    /// Signature of the request. If valid, this can be used to recover the address of the signer
    pub signature: Option<Signature>,
    /// Extra optional data
    pub extra_data: Option<Vec<u8>>,
}
/// These functions are modified to use unwrap instead of returning a Result
impl RequestToNetwork {
    /// Signs the request
    // async fn _sign(&self, wallet: LocalWallet) -> Result<Signature, Error> {
    async fn _sign(&self, wallet: LocalWallet) -> Signature { wallet.sign_message(&bincode::serialize(&self).unwrap()).await.unwrap() }
    /// Signs the request if it has an empty signature and returns a new request with the signature
    // pub async fn signed(&self, wallet: LocalWallet) -> Result<Self, Error> {
    pub async fn signed(&self, wallet: LocalWallet) -> Self {
        if self.signature.is_some() {
            // return Err(anyhow!("Request already signed"));
            panic!("Request already signed");
        };
        // let sig = self._sign(wallet).await?;
        let sig = self._sign(wallet).await;
        let mut new = self.clone();
        new.signature = Some(sig);
        // Ok(new)
        new
    }
}

// generate oprf specific structs and consts

lazy_static! {
    pub static ref NETWORK_BABYJUB_PUBKEY: <BabyJubJub as Curve<32>>::Point = <BabyJubJub as Curve<32>>::Point::from_encoded(
        &hex::decode("012000000000000000153e6e97edfbd87c5c559b6ef4941e4f89bea681575b1252afccdfce6bbe682f").unwrap()
        // &hex::decode("012000000000000000469835755a0b099d9bb4d2d3da120c91bee97617ae71a18312a5c75550fad60a").unwrap()
    ).unwrap();
    pub static ref CONDITIONS_CONTRACT: H160 = "0x248002ce5220b12d87bdbe148e04ee4bf29682f4".parse().unwrap();
    pub static ref HTTP_CLIENT: Client = reqwest::ClientBuilder::new()
        .build()
        .expect("Failed to build HTTP client");
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageStateResponse {
    Success { epoch: u32, method: Method, requests_from_user: u128 },
    Error { message: String },
}
#[derive(Deserialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: u64,
    result: MessageStateResponse,
}

#[derive(Deserialize)]
struct JsonRpcStateResponse {
    result: Option<MessageStateResponse>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize, Debug)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct JsonRpcNodeResponse {
    jsonrpc: String,
    id: u64,
    result: NodeResponse,
}

/// OPRF Response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeResponse {
    Submitted {
        request_id: String,
    },
    ConstructedProofSecp256k1 {
        node_idx: u32,
        peer_id: PeerId,
        request_id: String,
        proof: DLEQProof<32, Secp256k1>,
        method: Method,
    },
    ConstructedProofBabyJubJub {
        node_idx: u32,
        peer_id: PeerId,
        request_id: String,
        proof: DLEQProof<32, BabyJubJub>,
        method: Method,
    },
    VerifiedProofSecp256k1 {
        request_id: String,
        reconstructed_point: <Secp256k1 as Curve<32>>::Point,
    },
    VerifiedProofBabyJubJub {
        request_id: String,
        reconstructed_point: EncodedBabyJubJubPoint,
    },
    Participated,
    Error {
        message: String,
    },
    Keyshare(HashMap<String, String>),
}

/**
 * ---------------------------------------------------------------------------------------
 */

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EphemeralPrivateKeyWithSig {
    // #[serde(with = "serialization::scalar_hex")]
    // pub private_key: <BabyJubJub as Curve<32>>::Scalar,
    pub private_key: String,
    pub signature: SchnorrSig<32, BabyJubJub>,
}
#[wasm_bindgen]
/// Returns a tuple of two strings: the ephemeral private key and the signature of the conditions contract
pub fn generate_ephemeral_key_and_sign_conditions_with_it(conditions_contract: String) -> String {
    let secret = <BabyJubJub as Curve<32>>::Scalar::rand_vartime();
    let parsed_contract = conditions_contract.parse::<Address>().expect("invalid conditions contract address");
    let contract_bytes = parsed_contract.as_bytes();
    let signed: SchnorrSig<32, BabyJubJub> = secret.sign(contract_bytes);
    let big_uint_secret: BigUint = secret.into_bigint().into();
    serde_json::to_string_pretty(&EphemeralPrivateKeyWithSig {
        private_key: big_uint_secret.to_string(),
        signature: signed,
    })
    .unwrap()
}
#[wasm_bindgen]
pub fn msg_to_point(msg: &[u8]) -> String {
    if msg.len() > 24 {
        return "Message must be less than or equal to 24 bytes".into();
    }
    // Pad the leftmost bytes
    let mut padded_msg = [0u8; 24];
    padded_msg[24 - msg.len()..].copy_from_slice(msg);
    let point = BabyJubJub::koblitz_map(&padded_msg).unwrap();
    // Return in standard Edwards form (a=1). The V3CleanHands circuit applies
    // UntwistedToTwisted() itself before passing to EncryptElGamal/BabyCheck.
    point.to_string()
}

/// Converts an encoded BabyJubJub public key to (x, y) decimal strings in twisted Edwards form,
/// suitable for passing to circom circuits.
/// `encoded_pubkey_hex` is the hex-encoded serialized point (e.g. from finalized_group_public_keys).
#[wasm_bindgen]
pub fn pubkey_to_circom_format(encoded_pubkey_hex: String) -> String {
    let pubkey = <BabyJubJub as Curve<32>>::Point::from_encoded(
        &hex::decode(&encoded_pubkey_hex).expect("invalid hex")
    ).expect("invalid pubkey encoding");
    let twisted = pubkey.edwards_to_twisted();
    let x: BigUint = twisted.x.into_bigint().into();
    let y: BigUint = twisted.y.into_bigint().into();
    serde_json::to_string(&serde_json::json!([x.to_string(), y.to_string()])).unwrap()
}
pub async fn request_user_state(method: Method, address: Address) -> StateResponse {
    let state_req = StateRequest { method, user: address };
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "mishti_fetch_state",
        "params": [state_req],
        "id": 1
    });
    let response_text = HTTP_CLIENT
        .post(RELAY_URL)
        .json(&payload)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    log!("[request_user_state] raw response: {}", response_text);
    let rpc_response: JsonRpcStateResponse = serde_json::from_str(&response_text)
        .unwrap_or_else(|e| panic!("Failed to parse JSON-RPC response: {}. Raw: {}", e, response_text));
    if let Some(err) = rpc_response.error {
        panic!("JSON-RPC error (code {}): {}. Raw: {}", err.code, err.message, response_text);
    }
    match rpc_response.result.expect("JSON-RPC response has neither result nor error") {
        MessageStateResponse::Success { epoch, method, requests_from_user } => {
            StateResponse { epoch, method, requests_from_user }
        }
        MessageStateResponse::Error { message } => {
            panic!("Error getting user state: {}", message);
        }
    }
}
/// * `method` - Either OPRFSecp256k1 or DecryptBabyJubJub
/// * `address` - Address of the user
#[wasm_bindgen]
pub async fn get_user_state(method: String, address: String) -> String {
    let parsed_method = match method.as_str() {
        "OPRFSecp256k1" => Method::OPRFSecp256k1,
        "DecryptBabyJubJub" => Method::DecryptBabyJubJub,
        _ => panic!("invalid method"),
    };
    let parsed_address: Address = address.parse().expect("invalid address");
    let state = request_user_state(parsed_method, parsed_address).await;
    serde_json::to_string(&state).unwrap()
}
/// Utility function to serialize and sign a request. Helpful because
/// there is no js implementation of bincode.
/// * `request` - Must be JSON stringified RequestToNetwork
#[wasm_bindgen]
pub async fn sign_request_to_network(request: String, private_key: String) -> String {
    let req: RequestToNetwork = serde_json::from_str(&request).unwrap();
    let wallet = LocalWallet::from_str(&private_key).unwrap();
    let signed_req = req.signed(wallet).await;
    serde_json::to_string(&signed_req).unwrap()
}
fn parse_decrypt_args(human_user_privkey: String, ciphertext: String, conditions_contract: String, sig: String) -> (LocalWallet, ElGamalCiphertextWithSignedConditions) {
    let wallet = LocalWallet::from_str(&human_user_privkey).expect("Failed to parse private key");
    let ciphertext = serde_json::from_str::<CiphertextInput>(&ciphertext).expect("invalid ciphertext");
    // Ciphertext points from the circuit are already in standard Edwards form (a=1),
    // because V3CleanHands applies TwistedToUntwisted() before outputting encryptions.
    let ephemeral_dh_pubkey = Affine::<EdwardsConfig> {
        x: BigUint::from_str(ciphertext.c1.x.as_str()).expect("invalid x coordinate").into(),
        y: BigUint::from_str(ciphertext.c1.y.as_str()).expect("invalid y coordinate").into(),
    };
    assert!(ephemeral_dh_pubkey.is_on_curve(), "ephemeral_dh_pubkey is not on the curve");
    let encrypted_msg = Affine::<EdwardsConfig> {
        x: BigUint::from_str(ciphertext.c2.x.as_str()).expect("invalid x coordinate").into(),
        y: BigUint::from_str(ciphertext.c2.y.as_str()).expect("invalid y coordinate").into(),
    };
    assert!(encrypted_msg.is_on_curve(), "encrypted_msg is not on the curve");
    let ciphertext = ElGamalCiphertext::<32, BabyJubJub> { encrypted_msg, ephemeral_dh_pubkey };
    let parsed_contract = conditions_contract.parse::<Address>().expect("invalid conditions contract address");
    let parsed_sig = serde_json::from_str::<SchnorrSig<32, BabyJubJub>>(&sig).expect("invalid signature");
    let ciphertext_with_signed_conditions = ElGamalCiphertextWithSignedConditions {
        ciphertext,
        signed_conditions: DecryptionContractSignature {
            contract: parsed_contract,
            sig: parsed_sig,
        },
    };
    (wallet, ciphertext_with_signed_conditions)
}
// #[wasm_bindgen(getter_with_clone)]
#[derive(Serialize, Deserialize)]
pub struct PointInput {
    x: String,
    y: String,
}
// #[wasm_bindgen(getter_with_clone)]
#[derive(Serialize, Deserialize)]
pub struct CiphertextInput {
    c1: PointInput,
    c2: PointInput,
}
/// The x and y values in ciphertext must be decimal strings.
///
/// * `human_user_privkey` - Private key of the account with Human decryption credits
/// * `ciphertext` - ElGamal ciphertext. JSON string with the following structure:
///                 { c1: { x: string, y: string }, c2: { x: string, y: string } }
/// * `conditions_contract` - Address of the smart contract signed by the private
///                           key corresponding to c1 in ciphertext
/// * `sig` - Schnorr signature. JSON string with the following structure:
///           { R: string, s: string }
#[wasm_bindgen]
pub async fn decrypt(human_user_privkey: String, ciphertext: String, conditions_contract: String, sig: String) -> String {
    log!("[decrypt] parsing args...");
    let (wallet, ciphertext_with_signed_conditions) = parse_decrypt_args(human_user_privkey, ciphertext, conditions_contract, sig);
    log!("[decrypt] args parsed. wallet address: {:?}", wallet.address());
    // Get the state of the network, how many requests we can send
    log!("[decrypt] requesting user state...");
    let state = request_user_state(Method::DecryptBabyJubJub, wallet.address()).await;
    log!("[decrypt] user state received. epoch: {}, requests_from_user: {}", state.epoch, state.requests_from_user);
    // if let StateResponse::Error { message } = state {
    //     panic!("Error getting user state: {}", message);
    // }
    // if let StateResponse::Success {
    //     epoch,
    //     method,
    //     requests_from_user,
    // } = state {
    // Prepare the request
    log!("[decrypt] preparing request to network...");
    let req = RequestToNetwork {
        method: state.method,
        epoch: state.epoch,
        request_per_user: state.requests_from_user + 1,
        point: ciphertext_with_signed_conditions.ciphertext.ephemeral_dh_pubkey.encode(),
        signature: None,
        extra_data: bincode::serialize(&ciphertext_with_signed_conditions).ok(),
    }
    .signed(wallet.clone())
    .await;
    // Send the request to the network via JSON-RPC
    log!("[decrypt] sending request to {}...", RELAY_URL);
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "mishti_threshold_mul",
        "params": [req],
        "id": 1
    });
    let res = HTTP_CLIENT
        .post(RELAY_URL)
        .json(&payload)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    log!("[decrypt] response received ({} bytes): {}", res.len(), res);
    let rpc_response: JsonRpcNodeResponse = serde_json::from_str(&res)
        .unwrap_or_else(|e| panic!("Failed to parse JSON-RPC node response: {}. Raw: {}", e, res));
    if let NodeResponse::VerifiedProofBabyJubJub { reconstructed_point, .. } = rpc_response.result {
        log!("[decrypt] got reconstructed point, decoding shared secret...");
        let shared_sec_bytes = bincode::serialize(&reconstructed_point).unwrap();
        let shared_sec = <BabyJubJub as Curve<32>>::Point::from_encoded(&shared_sec_bytes).unwrap();
        log!("[decrypt] shared secret decoded. decrypting ciphertext...");
        let output = decrypt_elgamal_from_shared_secret(&ciphertext_with_signed_conditions.ciphertext, &shared_sec).unwrap();
        log!("[decrypt] decryption successful!");
        BigUint::from_bytes_be(output.as_ref()).to_string()
    } else if let NodeResponse::Submitted { request_id } = rpc_response.result {
        log!("[decrypt] request submitted, polling for result. request_id: {}", request_id);
        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > MAX_POLL_ATTEMPTS {
                panic!("Polling timed out after {} attempts for request_id: {}", MAX_POLL_ATTEMPTS, request_id);
            }
            async_sleep(POLL_INTERVAL_MS).await;
            log!("[decrypt] polling attempt {}/{}...", attempts, MAX_POLL_ATTEMPTS);
            let poll_payload = json!({
                "jsonrpc": "2.0",
                "method": "mishti_fetch_threshold_mul_result",
                "params": [request_id],
                "id": 1
            });
            let poll_res = HTTP_CLIENT
                .post(RELAY_URL)
                .json(&poll_payload)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            log!("[decrypt] poll response: {}", poll_res);
            let poll_response: JsonRpcNodeResponse = serde_json::from_str(&poll_res)
                .unwrap_or_else(|e| panic!("Failed to parse poll response: {}. Raw: {}", e, poll_res));
            match poll_response.result {
                NodeResponse::VerifiedProofBabyJubJub { reconstructed_point, .. } => {
                    log!("[decrypt] got reconstructed point from polling, decoding shared secret...");
                    let shared_sec_bytes = bincode::serialize(&reconstructed_point).unwrap();
                    let shared_sec = <BabyJubJub as Curve<32>>::Point::from_encoded(&shared_sec_bytes).unwrap();
                    log!("[decrypt] shared secret decoded. decrypting ciphertext...");
                    let output = decrypt_elgamal_from_shared_secret(&ciphertext_with_signed_conditions.ciphertext, &shared_sec).unwrap();
                    log!("[decrypt] decryption successful!");
                    return BigUint::from_bytes_be(output.as_ref()).to_string();
                }
                NodeResponse::Submitted { .. } => {
                    log!("[decrypt] result not ready yet, will retry...");
                    continue;
                }
                NodeResponse::Error { message } if message == "Not yet verified" => {
                    log!("[decrypt] not yet verified, will retry...");
                    continue;
                }
                NodeResponse::Error { message } => {
                    panic!("Node error while polling: {}", message);
                }
                other => {
                    panic!("Unexpected poll response: {:?}", other);
                }
            }
        }
    } else if let NodeResponse::Error { message } = rpc_response.result {
        panic!("Node error: {}", message);
    } else {
        panic!("Unexpected response: {:?}", rpc_response.result);
    }
}
#[wasm_bindgen]
pub fn new_mask() -> JsValue { serde_wasm_bindgen::to_value(&Mask::<32, Secp256k1>::new()).unwrap() }
// #[derive(Serialize, Deserialize)]
// pub struct JWTPRFStep1Results {
//     pub mask: Mask<32, Secp256k1>,
//     pub maskproof: MaskProof<32, Secp256k1>,
// }
// / Proves that a mask was created correctly
// #[wasm_bindgen]
// pub async fn mask_proof(secret_mask: String, jwt: &str) -> Result<JsValue, JsError> { // (Mask<32, BabyJubJub>, MaskProof<32, BabyJubJub>) {
//     let secret_mask = <Secp256k1 as Curve<32>>::Scalar::from_bytes(&hex::decode(secret_mask).map_err(to_js_err)?).map_err(to_js_err)?;
//     let unique_str = JWTClaims::from_raw_token_unchecked(jwt).map_err(to_js_err)?.unique_string().map_err(to_js_err)?;
//     serde_wasm_bindgen::to_value(&MaskProof::<32, Secp256k1>::create(secret_mask, &unique_str).unwrap()).map_err(to_js_err)
// }
#[wasm_bindgen]
/// Computes proof of mask and a request to the network with the JWT and its proof of the mask
/// The request will not be complete because the epoch, request number, and signature are empty or zero.
/// Depending on who is signing the request, these need to be filled in or corrected before sending it to the network.
pub async fn make_jwt_request(secret_mask: String, jwt: &str) -> Result<JsValue, JsError> {
    let secret_mask = <Secp256k1 as Curve<32>>::Scalar::from_bytes(&hex::decode(secret_mask).map_err(to_js_err)?).map_err(to_js_err)?;
    let unique_str = JWTClaims::from_raw_token_unchecked(jwt).map_err(to_js_err)?.unique_string().map_err(to_js_err)?;
    let mask_proof = MaskProof::<32, Secp256k1>::create(secret_mask, &unique_str).unwrap();
    serde_wasm_bindgen::to_value(&RequestToNetwork {
        method: Method::JWTPRFSecp256k1,
        point: mask_proof.0.output_point.encode(),
        epoch: 0,
        request_per_user: 0,
        signature: None,
        extra_data: Some(bincode::serialize(&FrontendData {
            jwt: jwt.to_string(),
            key_idx: 0, // there is only one key in the JWK endpoint for Clerk so index is 0
            mask_proof,
        })?),
    })
    .map_err(to_js_err)
}

/// Unmasks a point then hashes it to the curve's scalar field. Outputs the decimal representation of the point as a string
pub fn unmask_point_and_hash_to_field<C: Curve<32>>(mask: String, point: String) -> Result<String, JsError> {
    let mask = <C as Curve<32>>::Scalar::from_bytes(&hex::decode(mask).map_err(to_js_err)?).map_err(to_js_err)?;
    let network_output = <C as Curve<32>>::Point::from_encoded(&hex::decode(point).map_err(to_js_err)?).map_err(to_js_err)?;
    let inv = mask.mul_inv();
    if inv.is_none().unwrap_u8() == 1 {
        return Err(JsError::new("Mask is zero"));
    }
    let unmasked = network_output.scalar_mul(&inv.unwrap());
    Ok(C::hash_from_curve(unmasked).to_string())
}
#[wasm_bindgen]
pub fn unmask_and_hash_to_field_secp256k1(mask: String, point: String) -> Result<String, JsError> {
    // let mask = "9365bb6344dabfbd14f840c08b71b277cf36d04dbdde6f6b8540ea4500b94852".to_string();
    // let point = "0252ED9D6C6BC33BAEE299FFC7CB4E63821AC13D330E855C1DCAAB5744F2106115".to_string();
    unmask_point_and_hash_to_field::<Secp256k1>(mask, point)
}

#[wasm_bindgen]
pub async fn request_from_signer(input: String, method: String, signer_url: String) -> Result<String, JsError> {
    match method.as_str() {
        "OPRFSecp256k1" => handle_oprf_request_from_signer(input, Method::OPRFSecp256k1, signer_url).await,
        "OPRFBabyJubJub" => handle_oprf_request_from_signer(input, Method::OPRFBabyJubJub, signer_url).await,
        "DecryptBabyJubJub" => handle_oprf_request_from_signer(input, Method::DecryptBabyJubJub, signer_url).await,
        "JWTPRFSecp256k1" => handle_oprf_request_from_signer(input, Method::JWTPRFSecp256k1, signer_url).await,
        _ => panic!("Invalid method"),
    }
}

async fn handle_oprf_request_from_signer(input: String, method: Method, signer_url: String) -> Result<String, JsError> {
    let (mask, unsigned_request_to_network) = match method {
        Method::OPRFSecp256k1 => {
            let (point, mask) = compute_oprf::<32, Secp256k1>(&input).await;
            let unsigned_request_to_network = RequestToNetwork {
                point,
                method: Method::OPRFSecp256k1,
                epoch: 0,
                request_per_user: 0,
                signature: None,
                extra_data: None,
            };
            (ScalarTrait::to_bytes(&mask), unsigned_request_to_network)
        }
        Method::OPRFBabyJubJub => {
            let (point, mask) = compute_oprf::<32, BabyJubJub>(&input).await;
            let unsigned_request_to_network = RequestToNetwork {
                point,
                method: Method::OPRFBabyJubJub,
                epoch: 0,
                request_per_user: 0,
                signature: None,
                extra_data: None,
            };
            (ScalarTrait::to_bytes(&mask), unsigned_request_to_network)
        }
        Method::DecryptBabyJubJub => {
            // console_log!("DecryptBabyJubJub not implemented in handle_oprf_request_from_signer");
            return Err(JsError::new("DecryptBabyJubJub not implemented in handle_oprf_request_from_signer"));
        }
        Method::JWTPRFSecp256k1 => {
            // console_log!("JWTPRFSecp256k1 not implemented in handle_oprf_request_from_signer");
            return Err(JsError::new("JWTPRFSecp256k1 not implemented in handle_oprf_request_from_signer"));
        }
    };
    let response = HTTP_CLIENT.post(&signer_url).json(&unsigned_request_to_network).send().await.map_err(to_js_err)?;

    match method {
        Method::OPRFSecp256k1 => {
            if response.status().is_success() {
                let response_text = response.text().await.map_err(to_js_err)?;
                let reconstructed_point = <Secp256k1 as Curve<32>>::Point::from_encoded(&hex::decode(response_text.replace("\"", "")).map_err(to_js_err)?).map_err(to_js_err)?;
                let mask = <Secp256k1 as Curve<32>>::Scalar::from_bytes(&mask).unwrap();
                let output = <Secp256k1 as Curve<32>>::hash_from_curve(reconstructed_point.scalar_mul(&mask.mul_inv().unwrap()));
                Ok(output.to_string())
            } else {
                let error_text = response.text().await.map_err(to_js_err)?;
                Err(JsError::new(&error_text))
            }
        }
        Method::OPRFBabyJubJub => {
            if response.status().is_success() {
                let response_text = response.text().await.map_err(to_js_err)?;
                let encoded_point = serde_json::from_str::<EncodedBabyJubJubPoint>(&response_text).map_err(to_js_err)?;
                let reconstructed_point = <BabyJubJub as Curve<32>>::Point::from_encoded(&bincode::serialize(&encoded_point).map_err(to_js_err)?).map_err(to_js_err)?;
                let mask = <BabyJubJub as Curve<32>>::Scalar::from_bytes(&mask).unwrap();
                let output = <BabyJubJub as Curve<32>>::hash_from_curve(reconstructed_point.scalar_mul(&mask.mul_inv().unwrap()));
                Ok(output.to_string())
            } else {
                let error_text = response.text().await.map_err(to_js_err)?;
                Err(JsError::new(&error_text))
            }
        }
        Method::DecryptBabyJubJub => {
            // console_log!("DecryptBabyJubJub not implemented in handle_oprf_request_from_signer");
            Err(JsError::new("DecryptBabyJubJub not implemented in handle_oprf_request_from_signer"))
        }
        Method::JWTPRFSecp256k1 => {
            // console_log!("JWTPRFSecp256k1 not implemented in handle_oprf_request_from_signer");
            Err(JsError::new("JWTPRFSecp256k1 not implemented in handle_oprf_request_from_signer"))
        }
    }
}

#[wasm_bindgen]
pub async fn generate_oprf(private_key: Option<String>, input: Option<String>, method: String, rpc_url: String) -> Result<String, JsError> {
    match method.as_str() {
        "OPRFSecp256k1" => handle_oprf_request(private_key, input, rpc_url, Method::OPRFSecp256k1).await,
        "OPRFBabyJubJub" => handle_oprf_request(private_key, input, rpc_url, Method::OPRFBabyJubJub).await,
        "DecryptBabyJubJub" => handle_oprf_request(private_key, input, rpc_url, Method::DecryptBabyJubJub).await,
        "JWTPRFSecp256k1" => handle_oprf_request(private_key, input, rpc_url, Method::JWTPRFSecp256k1).await,
        _ => Err(JsError::new(&format!("Invalid method: {}", method))),
    }
}
pub async fn handle_oprf_request(private_key: Option<String>, input: Option<String>, rpc_url: String, method: Method) -> Result<String, JsError> {
    let wallet = LocalWallet::from_str(&private_key.unwrap()).map_err(|e| JsError::new(&format!("Failed to parse private key: {}", e)))?;

    let (point, extra_data, client_secrets) = match method {
        Method::OPRFSecp256k1 => {
            let (point, mask) = compute_oprf::<32, Secp256k1>(&input.unwrap()).await;
            (point, None, ScalarTrait::to_bytes(&mask))
        }
        Method::OPRFBabyJubJub => {
            let (point, mask) = compute_oprf::<32, BabyJubJub>(&input.unwrap()).await;
            (point, None, ScalarTrait::to_bytes(&mask))
        }
        Method::DecryptBabyJubJub => {
            let msg = &[123u8; 24];
            let ciphertext_with_signed_conditions = encrypt_with_conditions(msg, &*NETWORK_BABYJUB_PUBKEY, *CONDITIONS_CONTRACT).unwrap();
            (
                ciphertext_with_signed_conditions.ciphertext.ephemeral_dh_pubkey.encode(),
                Some(bincode::serialize(&ciphertext_with_signed_conditions).unwrap()),
                ciphertext_with_signed_conditions.ciphertext.encrypted_msg.encode(),
            )
        }
        Method::JWTPRFSecp256k1 => {
            let input = input.unwrap();
            let mut split = input.split("--SILK-JWT-SEPARATOR--");
            let secret_mask = <Secp256k1 as Curve<32>>::Scalar::from_bytes(&hex::decode(split.next().unwrap()).map_err(|e| JsError::new(&format!("Invalid hex mask: {}", e)))?)
                .map_err(|e| JsError::new(&format!("Invalid scalar mask: {:?}", e)))?;
            let unfilled_req: RequestToNetwork = serde_json::from_str(split.next().unwrap()).map_err(|e| JsError::new(&format!("Invalid request JSON: {}", e)))?;
            let point = unfilled_req.point;
            let extra_data = unfilled_req.extra_data;
            let client_secrets = ScalarTrait::to_bytes(&secret_mask);

            (point, extra_data, client_secrets)
        }
        _ => panic!("Invalid method"),
    };

    let state_req = StateRequest {
        user: wallet.address(),
        method: method.clone(),
    };
    let state = fetch_state(&*HTTP_CLIENT, &rpc_url, state_req)
        .await
        .map_err(|e| JsError::new(&format!("Failed to fetch state: {}", e)))?;

    match state {
        MessageStateResponse::Success { epoch, method, requests_from_user } => {
            let req = create_request(wallet.clone(), point, method.clone(), epoch, requests_from_user, extra_data.clone()).await;
            let response = send_request(&*HTTP_CLIENT, &rpc_url, req.clone()).await;
            println!("Response: {:?}", response);
            match method {
                Method::OPRFSecp256k1 => match response {
                    Ok(NodeResponse::VerifiedProofSecp256k1 { .. }) => process_finished_response(response.expect("error in response"), &client_secrets),
                    Ok(NodeResponse::Submitted { request_id }) => process_response(&*HTTP_CLIENT, &rpc_url, &request_id, &client_secrets).await,
                    _ => {
                        let err_msg = match response {
                            Ok(NodeResponse::Error { message }) => format!("Node error: {}", message),
                            Err(e) => format!("Request error: {}", e),
                            Ok(resp) => format!("Unexpected response type: {:?}", resp),
                        };
                        console::log_1(&err_msg.clone().into());
                        return Err(JsError::new(&err_msg));
                    }
                },
                Method::OPRFBabyJubJub => match response {
                    Ok(NodeResponse::VerifiedProofBabyJubJub { .. }) => process_finished_response(response.expect("error in response"), &client_secrets),
                    Ok(NodeResponse::Submitted { request_id }) => process_response(&*HTTP_CLIENT, &rpc_url, &request_id, &client_secrets).await,
                    _ => panic!("Invalid response for OPRFBabyJubJub"),
                },
                Method::DecryptBabyJubJub => {
                    if let Ok(NodeResponse::VerifiedProofBabyJubJub { ref reconstructed_point, .. }) = response {
                        let c = bincode::serialize(reconstructed_point).unwrap();
                        // Get the Diffie-Hellman shared secret. This will be the network's public key if you encrypted with a secret key of 1.
                        let shared_sec = <BabyJubJub as Curve<32>>::Point::from_encoded(&c).unwrap();
                        // Decrypt the ciphertext using the shared secret.
                        if let Some(ref data) = extra_data {
                            let ciphertext_with_signed_conditions: ElGamalCiphertextWithSignedConditions = bincode::deserialize(data).unwrap();
                            let output = decrypt_elgamal_from_shared_secret(&ciphertext_with_signed_conditions.ciphertext, &shared_sec).unwrap();
                            println!("{:?}", hex::encode(output));
                            Ok(hex::encode(output))
                        } else {
                            Err(JsError::new("DecryptBabyJubJub while decrypting"))
                        }
                    } else {
                        Err(JsError::new("DecryptBabyJubJub while decrypting"))
                    }
                }
                Method::JWTPRFSecp256k1 => match response {
                    Ok(NodeResponse::VerifiedProofSecp256k1 { .. }) => process_finished_response(response.expect("error in response"), &client_secrets),
                    Ok(NodeResponse::Submitted { request_id }) => process_response(&*HTTP_CLIENT, &rpc_url, &request_id, &client_secrets).await,
                    _ => panic!("Invalid response for JWTPRFSecp256k1"),
                },
            }
        }
        MessageStateResponse::Error { message } => Err(JsError::new(&format!("State error: {}", message))),
    }
}

async fn compute_oprf<const N: usize, C: Curve<N>>(input: &str) -> (Vec<u8>, C::Scalar) {
    let salt = b"test-salt";
    oprf_client_1::<N, C, &[u8]>(salt, input.as_bytes()).expect("Failed to compute OPRF step 1")
}

async fn fetch_state(client: &Client, rpc_url: &str, state_req: StateRequest) -> Result<MessageStateResponse, Box<dyn std::error::Error>> {
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "human_fetch_state",
        "params": [state_req],
        "id": 1
    });
    let response = client.post(rpc_url).json(&payload).send().await?;

    let response_text = response.text().await?;
    // console_log!("Full response body: {}", response_text);

    let message_state: JsonRpcResponse = serde_json::from_str(&response_text)?;
    Ok(message_state.result)
}

async fn send_request(client: &Client, rpc_url: &str, req: RequestToNetwork) -> Result<NodeResponse, Box<dyn std::error::Error>> {
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "human_threshold_mul",
        "params": [req],
        "id": 1
    });
    let response = client.post(rpc_url).json(&payload).send().await?;
    let response_text = response.text().await?;
    // console_log!("Full response body: {}", response_text);

    let message_state: JsonRpcNodeResponse = serde_json::from_str(&response_text)?;
    Ok(message_state.result)
    // client.threshold_mul(req).await.expect("Failed to get OPRF response")
}
async fn query_threshold_multiplication(client: &Client, rpc_url: &str, request_id: &str) -> Result<NodeResponse, Box<dyn std::error::Error>> {
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "human_threshold_mul",
        "params": [request_id],
        "id": 1
    });
    let response = client.post(rpc_url).json(&payload).send().await?;
    let response_text = response.text().await?;
    // console_log!("Full response body: {}", response_text);

    let message_state: JsonRpcNodeResponse = serde_json::from_str(&response_text)?;
    Ok(message_state.result)
    // client
    //     .fetch_threshold_mul_result(request_id.to_string())
    //     .await
    //     .expect("Failed to fetch threshold multiplication result")
}
async fn create_request(wallet: LocalWallet, point: Vec<u8>, method: Method, epoch: u32, requests_from_user: u128, extra_data: Option<Vec<u8>>) -> RequestToNetwork {
    RequestToNetwork {
        point,
        method,
        epoch,
        request_per_user: requests_from_user + 1,
        signature: None,
        extra_data,
    }
    .signed(wallet)
    .await
}

async fn process_response(client: &Client, rpc_url: &str, request_id: &str, mask: &Vec<u8>) -> Result<String, JsError> {
    let response = query_threshold_multiplication(client, rpc_url, request_id).await;
    println!("Processing response: {:?}", response);
    match response {
        Ok(NodeResponse::VerifiedProofBabyJubJub { .. }) => process_finished_response(response.expect("threshold mul res error"), mask),
        Ok(NodeResponse::VerifiedProofSecp256k1 { .. }) => process_finished_response(response.expect("threshold mul res error"), mask),
        _ => Err(JsError::new(&format!("Unexpected response for process_response"))),
    }
}

fn process_finished_response(response: NodeResponse, mask: &Vec<u8>) -> Result<String, JsError> {
    let output = match response {
        NodeResponse::VerifiedProofSecp256k1 { reconstructed_point, .. } => {
            let mask = <Secp256k1 as Curve<32>>::Scalar::from_bytes(mask).unwrap();
            <Secp256k1 as Curve<32>>::hash_from_curve(reconstructed_point.scalar_mul(&mask.mul_inv().unwrap()))
        }
        NodeResponse::VerifiedProofBabyJubJub { reconstructed_point, .. } => {
            // BabyJubJub has a strange adaptor for serialization right now
            let reconstructed_point = <BabyJubJub as Curve<32>>::Point::from_encoded(&bincode::serialize(&reconstructed_point).unwrap()).unwrap();
            let mask = <BabyJubJub as Curve<32>>::Scalar::from_bytes(mask).unwrap();
            <BabyJubJub as Curve<32>>::hash_from_curve(reconstructed_point.scalar_mul(&mask.mul_inv().unwrap()))
        }
        _ => panic!("Invalid response"),
    };
    println!("Final Multiplication Result: {}", output);
    Ok(output.to_string())
}
// /// Temporary function for testing, not for production
// #[wasm_bindgen]
// pub async fn jwtprf_testfn(secret_mask: String, jwt: &str, requestor_privkey: String) -> String {
//     let secret_mask = <Secp256k1 as Curve<32>>::Scalar::from_bytes(&hex::decode(secret_mask).unwrap()).unwrap();
//     let unique_str = JWTClaims::from_raw_token_unchecked(jwt).unwrap().unique_string().unwrap();
//     let mask_proof = MaskProof::<32, Secp256k1>::create(secret_mask, &unique_str).unwrap();
//     let wallet = LocalWallet::from_str(&requestor_privkey).unwrap();
//     let state = request_user_state(Method::JWTPRFSecp256k1, wallet.address()).await;
//     let req = RequestToNetwork {
//         method: Method::JWTPRFSecp256k1,
//         point: mask_proof.0.output_point.encode(),
//         epoch: state.epoch,
//         request_per_user: state.requests_from_user + 1,
//         signature: None,
//         extra_data: Some(bincode::serialize(&FrontendData {
//             jwt: jwt.to_string(),
//             key_idx: 0, // there is only one key in the JWK endpoint for Clerk so index is 0
//             mask_proof,
//         }).unwrap())
//     }.signed(wallet).await;
//     let client = reqwest::Client::new();
//     let res = client.post(format!("{}/relay-JWTPRFSecp256k1", RELAY_URL))
//         .header("Content-Type", "application/json")
//         .body(serde_json::to_string(&req).unwrap())
//         .send()
//         .await
//         .unwrap()
//         .text()
//         .await
//         .unwrap();
//     res
// }
// type SecpPoint = <Secp256k1 as Curve<32>>::Point;
// type SecpScalar = <Secp256k1 as Curve<32>>::Scalar;
// #[wasm_bindgen]
// pub async fn prf_step2(secret_mask: SecpScalar, network_output: SecpPoint) -> Vec<u8> {
//     invert_mask_and_obtain_prf(&secret_mask, &network_output).unwrap()
// }
