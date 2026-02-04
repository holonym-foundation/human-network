use ark_ec::twisted_edwards::Affine;
use ark_ed_on_bn254::{EdwardsConfig, Fr};
use ethers::{
    signers::{LocalWallet, Signer},
    types::Address,
};
use k256::AffinePoint;
use lazy_static::lazy_static;
use messages::network_utils::{Method, RequestToNetwork, StateRequest, StateResponse};
use human_crypto::{
    encryption::{decrypt_elgamal_from_shared_secret, encrypt_with_conditions, testfn, DecryptionContractSignature, ElGamalCiphertext, ElGamalCiphertextWithSignedConditions},
    oprf_client_1,
    schnorrsig::{EphemeralPrivateKeyWithSig, SchnorrSignable},
    twisted_conversion::TwistedEdwardsConversion,
    zkinjmask::{FrontendData, JWTClaims, MaskProof},
    BabyJubJub, Curve, PointTrait, ScalarTrait, Secp256k1,
};
use num_bigint::BigUint;
use std::{env, str::FromStr};
// const RELAY_URL: &'static str = "http://159.65.246.91:3031"; http://localhost:3031
lazy_static! {
    pub static ref NETWORK_BABYJUB_PUBKEY: <BabyJubJub as Curve<32>>::Point = <BabyJubJub as Curve<32>>::Point::from_encoded(
        &hex::decode("012000000000000000153e6e97edfbd87c5c559b6ef4941e4f89bea681575b1252afccdfce6bbe682f").unwrap()
        // &hex::decode("012000000000000000469835755a0b099d9bb4d2d3da120c91bee97617ae71a18312a5c75550fad60a").unwrap()
    ).unwrap();
}
#[tokio::main]
pub async fn main() {
    let mut args = env::args().skip(1);
    let method = args.next().expect("No method provided");
    let input = args.next().expect("No input provided");
    let relay_url = env::var("RELAY_URL").expect("RELAY_URL not set");
    let privkey = env::var("HUMAN_NETWORK_CLIENT_PRIVATE_KEY").expect("HUMAN_NETWORK_CLIENT_PRIVATE_KEY not set");
    let wallet = LocalWallet::from_str(&privkey).expect("Failed to parse private key");
    match method.as_str() {
        "oprf" => oprf_test_client(&input, wallet, &relay_url).await,
        "decrypt" => decrypt_test_client(&input, wallet, &relay_url).await,
        "jwtprf" => jwt_test_client(&input, wallet, &relay_url).await,
        _ => panic!("Invalid method"),
    }
}
async fn get_state(method: Method, wallet: LocalWallet, relay_url: &str) -> StateResponse {
    let state_req = StateRequest { method, user: wallet.address() };
    surf::post(format!("{}/user-state/", relay_url))
        .body_json(&state_req)
        .unwrap()
        .await
        .unwrap()
        .body_json()
        .await
        .unwrap()
}
async fn oprf_test_client(input: &String, wallet: LocalWallet, relay_url: &str) {
    let salt = "salt".as_bytes();
    let (point, mask) = oprf_client_1::<32, Secp256k1, &[u8]>(salt, input.as_bytes()).expect("Failed to compute OPRF step 1");
    let state = get_state(Method::OPRFSecp256k1, wallet.clone(), relay_url).await;
    let req = RequestToNetwork {
        method: state.method,
        epoch: state.epoch,
        request_per_user: state.requests_from_user + 1,
        point,
        signature: None,
        extra_data: None,
    }
    .signed(wallet)
    .await
    .unwrap();
    let res = surf::post(format!("{}/relay-OPRFSecp256k1", relay_url))
        .body_json(&req)
        .unwrap()
        .await
        .unwrap()
        .body_string()
        .await
        .unwrap();
    // point encoding and hex decoding are unrelated -- don't let the names trick you
    let res = <Secp256k1 as Curve<32>>::Point::from_encoded(&hex::decode(res).unwrap()).unwrap();
    let output = <Secp256k1 as Curve<32>>::hash_from_curve((res * mask.invert().unwrap()).to_affine());
    println!("{}", output);
}
async fn decrypt_test_client(input: &String, wallet: LocalWallet, relay_url: &str) {
    // a message to encrypt to the network
    let msg = &[123u8; 24];
    // This contract allows a whitelisted address to decrypt up to 10000 messages
    let conditions_contract = "0x248002ce5220b12d87bdbe148e04ee4bf29682f4".parse().unwrap();
    // If you want to get the public key of the network, you can uncomment this:
    // use the private key of 1 to get the network's public key as a response
    // let ciphertext_with_signed_conditions = testfn(msg, &BabyJubJub::base_point_or_generator(), conditions_contract, Fr::from_biguint_vartime(BigUint::from_str("1").unwrap()).unwrap()).unwrap();
    // Otherwise you can encrypt with a random ephemeral private key:
    let ciphertext_with_signed_conditions = encrypt_with_conditions(msg, &*NETWORK_BABYJUB_PUBKEY, conditions_contract).unwrap();
    // get the state of the network, how many requests we can send
    let state = get_state(Method::DecryptBabyJubJub, wallet.clone(), relay_url).await;
    // Prepare the request
    let req = RequestToNetwork {
        method: state.method,
        epoch: state.epoch,
        request_per_user: state.requests_from_user + 1,
        point: ciphertext_with_signed_conditions.ciphertext.ephemeral_dh_pubkey.encode(),
        signature: None,
        extra_data: bincode::serialize(&ciphertext_with_signed_conditions).ok(),
    }
    .signed(wallet.clone())
    .await
    .unwrap();
    // Send the request to the relay node
    let res = surf::post(format!("{}/relay-DecryptBabyJubJub", relay_url))
        .body_json(&req)
        .unwrap()
        .await
        .unwrap()
        .body_string()
        .await
        .unwrap();
    // Get the Diffie Hellman Shared secret. This will be the network's public key if you encrypted with a secret key of 1
    let shared_sec = <BabyJubJub as Curve<32>>::Point::from_encoded(&hex::decode(res).unwrap()).unwrap();
    // Decrypt the ciphertext using the shared secret
    let output = decrypt_elgamal_from_shared_secret(&ciphertext_with_signed_conditions.ciphertext, &shared_sec).unwrap();
    println!("{:?}", output);

    // This was just to check that some circuit outputs were correct. Commenting out for now, then will delete in a future commit.
    // let pubkey_should_be = <BabyJubJub as Curve<32>>::base_point_or_generator().scalar_mul(
    //     &BigUint::from_str("15219242633815656831485775100299900419930232172338789909295807669388532445331").unwrap().into()
    // );
    // println!("Public key should be {:?} untwisted; {:?} twisted; {:?} wtf", pubkey_should_be, pubkey_should_be.twisted_to_edwards(), pubkey_should_be.edwards_to_twisted());
    // //e2e test with a point generated from the circuit (can delete if it works)
    // let ephemeral_pubkey = Affine::<EdwardsConfig> {
    //     // x: BigUint::from_str("15219242633815656831485775100299900419930232172338789909295807669388532445331").unwrap().into(),
    //     // y: BigUint::from_str("20512527499210776757013871327841196413167202786789993707049529906788609286687").unwrap().into()
    //     x: BigUint::from_str("7298940672059965768641845964270773272795981288081284939122224850165910347243").unwrap().into(),
    //     y: BigUint::from_str("7926082103045993969094841716375912351275414485995177415265999219539763466679").unwrap().into()
    // };

    // let ephemeral_priv_key: <BabyJubJub as Curve<32>>::Scalar = BigUint::from_str(&"15219242633815656831485775100299900419930232172338789909295807669388532445331").unwrap().into();
    // println!("ephemeral pubkey: {:?}. {:?}", ephemeral_pubkey, BabyJubJub::base_point_or_generator().scalar_mul(&ephemeral_priv_key));
    // let ephemeral_sig = SchnorrSignable::<32, BabyJubJub>::sign(&ephemeral_priv_key, conditions_contract.as_bytes());
    // let _test_sig_is_verified = <BabyJubJub as Curve<32>>::Scalar::verify(
    //     &conditions_contract.as_bytes(),
    //     &ephemeral_pubkey,
    //     &ephemeral_sig).unwrap();
    // // let ephemeral_private_key_with_sig: EphemeralPrivateKeyWithSig = serde_json::from_str(r#"{
    // //     "privateKey": "17329715bef6c2d7f1d14770a3f7e5bf94c101e31e6d2b3dafbd5f0f3ebf2802",
    // //     "signature": {
    // //       "R": "012000000000000000f420e90cc96cf65aa8e831457fc70fa3618ee08958345f68ae557aaf08232927",
    // //       "s": "ab6e6973e7ca9738f84dd4bd97392cfdeaca53d339dd465d263a129a128b8203"
    // //     }
    // //   }
    // // "#).unwrap();
    // let ciphertext_with_signed_conditions_from_circuit_output = ElGamalCiphertextWithSignedConditions {
    //     ciphertext: ElGamalCiphertext {
    //         encrypted_msg: Affine::<EdwardsConfig> {
    //             x: BigUint::from_str("14440256215838718387954991175858749361368934584034933881354100501137534214861").unwrap().into(),
    //             y: BigUint::from_str("7515372452966299157851033460487112678305947476655671376282536166047296797064").unwrap().into()
    //         },
    //         ephemeral_dh_pubkey: ephemeral_pubkey.clone()
    //     },
    //     signed_conditions: DecryptionContractSignature {
    //         contract: conditions_contract,
    //         sig: ephemeral_sig.clone()
    //     }
    // };

    // let req = RequestToNetwork {
    //     method: Method::DecryptBabyJubJub,
    //     epoch: state.epoch,
    //     request_per_user: state.requests_from_user + 2,
    //     point: ephemeral_pubkey.encode(),
    //     signature: None,
    //     extra_data: bincode::serialize(&ciphertext_with_signed_conditions_from_circuit_output).ok()
    // }.signed(wallet).await.unwrap();
    // let shared_secret = surf::post(format!("{}/relay-DecryptBabyJubJub", relay_url))
    //     .body_json(&req)
    //     .unwrap().await.unwrap().body_string().await.unwrap();
    // println!("shared secret from circuit output: {:?}", hex::decode(&shared_secret).unwrap());
    // println!("shared secret should be {:?}", NETWORK_BABYJUB_PUBKEY.scalar_mul(&ephemeral_priv_key).encode());
    // let shared_secret = <BabyJubJub as Curve<32>>::Point::from_encoded(&hex::decode(shared_secret).unwrap()).unwrap();
    // println!("decrypted from test output signals: {:?}, " , hex::encode(decrypt_elgamal_from_shared_secret(&ciphertext_with_signed_conditions_from_circuit_output.ciphertext, &shared_secret).unwrap()));
}
// This is just a hacky function for semi-manual end-to-end testing, using the results the testing frontend after logging in.
async fn jwt_test_client(input: &String, wallet: LocalWallet, relay_url: &str) {
    let mut split = input.split("-SILKJWTSEPARATOR-");
    let secret_mask = <Secp256k1 as Curve<32>>::Scalar::from_bytes(&hex::decode(split.next().unwrap()).unwrap()).unwrap();
    let unfilled_req: RequestToNetwork = serde_json::from_str(split.next().unwrap()).unwrap();
    // let fedata = bincode::deserialize::<FrontendData<32, Secp256k1>>(&unfilled_req.clone().extra_data.unwrap()).unwrap();
    // let claims = JWTClaims::from_raw_token_unchecked(&fedata.jwt).unwrap();
    // assert_eq!(Secp256k1::base_point_or_generator().scalar_mul(&secret_mask), AffinePoint::from_encoded(&hex::decode(&claims.pubmask).unwrap()).unwrap());
    let mut req = unfilled_req.clone();
    let state = get_state(Method::JWTPRFSecp256k1, wallet.clone(), relay_url).await;
    req.epoch = state.epoch;
    req.request_per_user = state.requests_from_user + 1;
    let res = surf::post(format!("{}/relay-JWTPRFSecp256k1", relay_url))
        .body_json(&req.signed(wallet).await.unwrap())
        .unwrap()
        .await
        .unwrap()
        .body_string()
        .await
        .unwrap();
    // point encoding and hex decoding are unrelated -- don't let the names trick you
    let res = <Secp256k1 as Curve<32>>::Point::from_encoded(&hex::decode(res).unwrap()).unwrap();
    let output = <Secp256k1 as Curve<32>>::hash_from_curve((res * secret_mask.invert().unwrap()).to_affine());
    println!("{}", output);
}
