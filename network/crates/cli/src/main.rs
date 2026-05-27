///! This is a simple test CLI tool to interact with the Human Network RPC server. It is not to be used for production!
///
/* trunk-ignore-all(rustfmt) */
use clap::Parser;
use ethers::signers::{LocalWallet, Signer};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use messages::network_utils::{Method, RequestToNetwork, CONDITIONS_CONTRACT};
use messages::types::{NodeResponse, StateRequest, StateResponse};
use human_crypto::encryption::{decrypt_elgamal_from_shared_secret, encrypt_with_conditions, ElGamalCiphertextWithSignedConditions};

use human_crypto::{oprf_client_1, BabyJubJub, Curve, Secp256k1};
use human_crypto::{PointTrait, ScalarTrait};
use rpc_trait::rpc::HumanRpcClient;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::{str::FromStr, time::Duration};
use tokio::time::sleep;

#[derive(Debug, Clone)]
struct KeyShareMap(HashMap<String, String>);

impl FromStr for KeyShareMap {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expected format: "key1:value1,key2:value2"
        let mut map = HashMap::new();

        if s.is_empty() {
            return Ok(KeyShareMap(map));
        }

        for pair in s.split(',') {
            let parts: Vec<&str> = pair.split(':').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid key-value pair: {}", pair));
            }
            map.insert(parts[0].to_string(), parts[1].to_string());
        }

        Ok(KeyShareMap(map))
    }
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// The private key as a string
    #[arg(short, long)]
    private_key: Option<String>,

    /// The input as a string
    #[arg(short, long)]
    input: Option<String>,

    /// The method to use: OPRFSecp256k1, OPRFBabyJubJub, DecryptBabyJubJub, etc.
    #[arg(short, long)]
    method: String,

    #[arg(short, long)]
    api_token: Option<String>,

    /// Key shares in format "key1:value1,key2:value2"
    #[arg(short, long)]
    keyshares: Option<KeyShareMap>,
    /// The Human Network RPC URL as a string
    #[arg(short, long)]
    rpc_url: String,

    #[arg(short, long)]
    peer_id: Option<String>,
    
    #[arg(short, long)]
    is_resharing_enabled: Option<bool>,
}

#[tokio::main]
async fn main() {
    // let contract_address: Address = HUMAN_CREDITS_ROBUSTNET.parse().unwrap();
    // let rpc = Arc::new(Provider::<Http>::try_from( env::var("L1_RPC").expect("L1_RPC not set") ).unwrap() );
    // let contract = HumanCreditsRobustNet::new(contract_address, rpc);
    // let credits = contract.credits_for(contract_address).call().await.expect("Failed to get credits");
    // println!("Credits: {}", credits);
    let args = Args::parse();
    match args.method.as_str() {
        "OPRFSecp256k1" => handle_method(args, Method::OPRFSecp256k1).await,
        "OPRFBabyJubJub" => handle_method(args, Method::OPRFBabyJubJub).await,
        "DecryptBabyJubJub" => handle_method(args, Method::DecryptBabyJubJub).await,
        "JWTPRFSecp256k1" => handle_method(args, Method::JWTPRFSecp256k1).await,
        "backup" => {
            let client = build_http_client(&args.rpc_url);
            let resp = client.backup().await.expect("Failed to take backup response");
            println!("Backup response {:?}", resp);
        }
        "fetch_keyshare" => {
            if args.api_token.is_none() {
                panic!("API token is required for fetch_keyshare method");
            }
            let client = build_http_client(&args.rpc_url);
            let resp = client.fetch_key_share(args.api_token.unwrap()).await.expect("Failed to get key share response");
            println!("Key share response {:?}", resp);
        }
        "restore_keyshare" => {
            if let Some(keyshare_map) = args.keyshares {
                let client = build_http_client(&args.rpc_url);
                let resp = client.restore_key_share(keyshare_map.0).await.expect("Failed to restore keyshares");
                println!("restore Key share response {:?}", resp);
            } else {
                eprintln!("No keyshares provided");
            }
        }
        "restore_election_state" => {
            let client = build_http_client(&args.rpc_url);
            let resp = client.restore_election_state().await.expect("Failed to restore election state");
            println!("Restore election state response {:?}", resp);
        }
        "quic_ping" => {
            if let Some(peer_id) = args.peer_id {
                let client = build_http_client(&args.rpc_url);
                let resp = client.quic_ping(peer_id).await.expect("Failed to quic ping");
                println!("{:?}", resp);
            } else {
                eprintln!("No peer id provided");
            }
        }
        "sync_peer_data" => {
            let client = build_http_client(&args.rpc_url);
            let resp = client.sync_peer_data().await.expect("Failed to sync peer data");
            println!("{:?}", resp);
        }
        "fetch_election_info" => {
            if let Some(peer_id) = args.peer_id {
                let client = build_http_client(&args.rpc_url);
                let resp = client.fetch_election_info(peer_id).await.expect("Failed to fetch election info ");
                println!("{:?}", resp);
            } else {
                eprintln!("No peer id provided");
            }
        }
        "fetch_voting_power" => {
            if let Some(peer_id) = args.peer_id {
                let client = build_http_client(&args.rpc_url);
                let resp = client.fetch_voting_power(peer_id).await.expect("Failed to fetch voting power");
                println!("{:?}", resp);
            } else {
                eprintln!("No peer id provided");
            }
        }
        "set_resharing_enabled" => {
            if args.api_token.is_none() {
                panic!("API token is required for set_resharing_enabled method");
            }
            if let Some(is_enabled) = args.is_resharing_enabled {
                let client = build_http_client(&args.rpc_url);
                let resp = client.set_resharing_enabled(args.api_token.unwrap(), is_enabled).await.expect("Failed to set resharing enabled status");
                println!("set resharing response {:?}", resp);
            } else {
                eprintln!("No is resharing enabled flag provided");
            }
        }
        _ => panic!("Invalid method"),
    }
}
async fn compute_oprf<const N: usize, C: Curve<N>>(input: &str) -> (Vec<u8>, C::Scalar) {
    let salt = b"test-salt";
    oprf_client_1::<N, C, &[u8]>(salt, input.as_bytes()).expect("Failed to compute OPRF step 1")
}
fn build_http_client(rpc_url: &str) -> HttpClient { HttpClientBuilder::default().build(rpc_url).expect("Failed to build client") }

lazy_static! {
    /// Caches the network's finalized BabyJubJub group public key after the first
    /// `get_pubkey` fetch so we don't re-query the relay on every decrypt request.
    /// It's a `OnceCell` rather than a plain value because the key is fetched asynchronously.
    static ref NETWORK_BABYJUB_PUBKEY: tokio::sync::OnceCell<<BabyJubJub as Curve<32>>::Point> = tokio::sync::OnceCell::new();
}

/// Fetches the network's finalized BabyJubJub group public key from the relay via the
/// `get_pubkey` RPC method and caches it in [`NETWORK_BABYJUB_PUBKEY`].
async fn get_network_babyjub_pubkey(client: &HttpClient) -> <BabyJubJub as Curve<32>>::Point {
    *NETWORK_BABYJUB_PUBKEY
        .get_or_init(|| async {
            let response = client.get_pubkey().await.expect("Failed to fetch network pubkey via get_pubkey");
            match response {
                NodeResponse::Keyshare(pubkeys) => {
                    let encoded = pubkeys.get("DecryptBabyJubjub").expect("DecryptBabyJubjub pubkey not found in get_pubkey response");
                    let bytes = hex::decode(encoded).expect("Failed to hex-decode network BabyJubJub pubkey");
                    <BabyJubJub as Curve<32>>::Point::from_encoded(&bytes).expect("Failed to decode network BabyJubJub pubkey point")
                }
                NodeResponse::Error { message, .. } => panic!("Network error fetching pubkey: {}", message),
                other => panic!("Unexpected response from get_pubkey: {:?}", other),
            }
        })
        .await
}
async fn fetch_state(client: &HttpClient, state_req: StateRequest) -> StateResponse { client.fetch_state(state_req).await.expect("Failed to fetch state") }
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
    .expect("Failed to sign request")
}
async fn send_request(client: &HttpClient, address: String, req: RequestToNetwork) -> NodeResponse { client.threshold_mul(req).await.expect("Failed to get OPRF response") }

async fn process_response(client: &HttpClient, request_id: &str, mask: &Vec<u8>) {
    query_threshold_multiplication(client, request_id).await;
    sleep(Duration::from_secs(3)).await;
    let response = query_threshold_multiplication(client, request_id).await;
    println!("Response: {:?}", response);
    match response {
        NodeResponse::VerifiedProofBabyJubJub { .. } => process_finished_response(response, mask),
        NodeResponse::VerifiedProofSecp256k1 { .. } => process_finished_response(response, mask),
        _ => panic!("Invalid response"),
    };
}
fn process_finished_response(response: NodeResponse, mask: &Vec<u8>) {
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
}
async fn query_threshold_multiplication(client: &HttpClient, request_id: &str) -> NodeResponse {
    client
        .fetch_threshold_mul_result(request_id.to_string())
        .await
        .expect("Failed to fetch threshold multiplication result")
}
async fn handle_method(args: Args, method: Method) {
    let wallet = LocalWallet::from_str(&args.private_key.unwrap()).expect("Failed to parse private key");

    let (point, extra_data, client_secrets) = match method {
        Method::OPRFSecp256k1 => {
            let (point, mask) = compute_oprf::<32, Secp256k1>(&args.input.unwrap()).await;
            (point, None, ScalarTrait::to_bytes(&mask))
        }
        Method::OPRFBabyJubJub => {
            let (point, mask) = compute_oprf::<32, BabyJubJub>(&args.input.unwrap()).await;
            (point, None, ScalarTrait::to_bytes(&mask))
        }
        Method::DecryptBabyJubJub => {
            let msg = &[123u8; 24];
            let network_pubkey = get_network_babyjub_pubkey(&build_http_client(&args.rpc_url)).await;
            let ciphertext_with_signed_conditions = encrypt_with_conditions(msg, &network_pubkey, *CONDITIONS_CONTRACT).unwrap();
            (
                ciphertext_with_signed_conditions.ciphertext.ephemeral_dh_pubkey.encode(),
                Some(bincode::serialize(&ciphertext_with_signed_conditions).unwrap()),
                ciphertext_with_signed_conditions.ciphertext.encrypted_msg.encode(),
            )
        }
        Method::JWTPRFSecp256k1 => {
            let input = args.input.unwrap();
            let mut split = input.split("--SILK-JWT-SEPARATOR--");
            let secret_mask = <Secp256k1 as Curve<32>>::Scalar::from_bytes(&hex::decode(split.next().unwrap()).unwrap()).unwrap();
            let unfilled_req: RequestToNetwork = serde_json::from_str(split.next().unwrap()).unwrap();
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
    let client = build_http_client(&args.rpc_url);
    let state = fetch_state(&client, state_req).await;

    if let StateResponse::Success { epoch, method, requests_from_user } = state {
        let req = create_request(wallet.clone(), point, method.clone(), epoch, requests_from_user, extra_data.clone()).await;
        let response = send_request(&client, wallet.address().to_string(), req.clone()).await;
        println!("Response: {:?}", response);
        match method {
            Method::OPRFSecp256k1 => match response {
                NodeResponse::VerifiedProofSecp256k1 { .. } => {
                    process_finished_response(response, &client_secrets);
                }
                NodeResponse::Submitted { request_id } => {
                    process_response(&client, &request_id, &client_secrets).await;
                }
                _ => panic!("Invalid response for OPRFSecp256k1"),
            },
            Method::OPRFBabyJubJub => match response {
                NodeResponse::VerifiedProofBabyJubJub { .. } => {
                    process_finished_response(response, &client_secrets);
                }
                NodeResponse::Submitted { request_id } => {
                    process_response(&client, &request_id, &client_secrets).await;
                }
                _ => panic!("Invalid response for OPRFBabyJubJub"),
            },
            Method::DecryptBabyJubJub => {
                if let NodeResponse::VerifiedProofBabyJubJub { ref reconstructed_point, .. } = response {
                    let c = bincode::serialize(reconstructed_point).unwrap();
                    // Get the Diffie-Hellman shared secret. This will be the network's public key if you encrypted with a secret key of 1.
                    let shared_sec = <BabyJubJub as Curve<32>>::Point::from_encoded(&c).unwrap();
                    // Decrypt the ciphertext using the shared secret.
                    if let Some(ref data) = extra_data {
                        let ciphertext_with_signed_conditions: ElGamalCiphertextWithSignedConditions = bincode::deserialize(data).unwrap();
                        let output = decrypt_elgamal_from_shared_secret(&ciphertext_with_signed_conditions.ciphertext, &shared_sec).unwrap();
                        println!("{:?}", hex::encode(output));
                    }
                }
            }
            Method::JWTPRFSecp256k1 => match response {
                NodeResponse::VerifiedProofSecp256k1 { .. } => {
                    process_finished_response(response, &client_secrets);
                }
                NodeResponse::Submitted { request_id } => {
                    process_response(&client, &request_id, &client_secrets).await;
                }
                _ => panic!("Invalid response for JWTPRFSecp256k1"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[tokio::test]
    // async fn test_babyjub_point() {
    //     let (point, mask) = oprf_client_1::<32, BabyJubJub, Vec<u8>>(vec![1,2,3], vec![1,2,3]).unwrap();
    //     println!("Point {:?}", point);
    //     println!("Pont {:?}", <BabyJubJub as Curve<32>>::Point::from_encoded(&point).unwrap());
    //     // An example of a point that failed decoding
    //     let failed_point: Vec<u8> = vec![0, 32, 0, 0, 0, 0, 0, 0, 0, 215, 79, 198, 116, 98, 128, 248, 139, 2, 204, 170, 170, 119, 86, 130, 121, 44, 253, 82, 252, 118, 31, 209, 232, 65, 54, 110, 91, 147, 133, 225, 0];
    //     println!("Failed Point {:?}", <BabyJubJub as Curve<32>>::Point::from_encoded(&failed_point).unwrap());
    // }
}
