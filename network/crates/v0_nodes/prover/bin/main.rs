use anyhow::anyhow;
use axum::{
    extract::{ConnectInfo, State},
    routing::post,
    Json, Router,
};
use ethers::providers::{Http, Provider};
use messages::network_utils::{testnet_logging, AVSError, NodeDataWithoutIP, Pubkeys, RequestToNetwork};
use human_crypto::{BabyJubJub, Curve, DLEQProof, PointTrait, ScalarTrait, Secp256k1};
use num_bigint::BigUint;
use redis::Connection;
use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tower_http::trace::TraceLayer;
/// Checks all aspects of the request are valid except the curve point check which require generics
async fn _validate_request_without_generics(socketaddr: SocketAddr, state: AppState, request: RequestToNetwork) -> Result<(), AVSError> {
    if !(socketaddr.ip() == state.relayer_ip) {
        return Err(anyhow!(format!("Request from {} not from relayer, {}", socketaddr.ip(), state.relayer_ip)).into());
    }
    if !request.is_for_my_node(state.n, state.t + state.epsilon, state.my_node_idx) {
        return Err(anyhow!("Request not for this node").into());
    }
    if request.epoch != *state.current_epoch.lock().await {
        return Err(anyhow!("Invalid epoch").into());
    }
    let _ = request.account_request_from_signer(&mut state.redis.lock().await, &state.eth).await?;
    Ok(())
}
macro_rules! validate_request {
    ( $curve_type: ty, $socketaddr: expr, $state: expr, $request: expr ) => {
        match _validate_request_without_generics($socketaddr, $state, $request).await {
            Ok(_) => match <$curve_type as Curve<32>>::Point::from_encoded(&$request.point) {
                Ok(pt) => {
                    if pt.is_on_curve() && pt.is_in_subgroup_assuming_on_curve() {
                        Ok(())
                    } else {
                        Err(anyhow!("Invalid point").into())
                    }
                }
                Err(_) => Err(anyhow!("Invalid point").into()),
            },
            Err(e) => Err(e),
        }
    };
}
macro_rules! endpoint_for_curve {
    ( $module_name:ident, $curve_type:ty, $point:ty, $keyshares: ident ) => {
        mod $module_name {
            use super::*;
            pub async fn handle(
                ConnectInfo(socketaddr): ConnectInfo<SocketAddr>,
                State(state): State<AppState>,
                Json(request): Json<RequestToNetwork>,
            ) -> Result<Json<DLEQProof<32, $curve_type>>, AVSError> {
                validate_request!($curve_type, socketaddr, state.clone(), request.clone())?;
                let point = <$point>::from_encoded(&request.point)?;
                let keyshares = state.$keyshares.lock().await;
                let secret_key = keyshares.get(request.method.as_str()).ok_or_else(|| anyhow!("Method not implemented for curve"))?;
                Ok(Json(DLEQProof::new(point, *secret_key)?))
            }
        }
    };
}
macro_rules! keyshares_for_curve_method {
    ( $method_name:expr, $curve_type:ty, $all_pubkeys: ident ) => {{
        let method = $method_name.to_string();
        let mut keyshares = HashMap::new();
        let env_var_name = format!("{}_KEYSHARE", $method_name);
        keyshares.insert(
            method.clone(),
            <$curve_type as Curve<32>>::Scalar::from_biguint_vartime(BigUint::from_bytes_be(
                &hex::decode(&env::var(&env_var_name).expect(&format!("{} not set", &env_var_name))).expect(&format!("{}: Invalid private key hex", &env_var_name)),
            ))
            .expect(&format!("{}: Invalid private key value", &env_var_name)),
        );
        // Add the public key to the list of public keys
        let mut pubkeys = $all_pubkeys.get(&<$curve_type>::NAME.to_string()).unwrap_or(&HashMap::new()).clone();
        let generator = <$curve_type as Curve<32>>::base_point_or_generator();
        let pubkey = generator.scalar_mul(keyshares.get(&method).unwrap());
        pubkeys.insert(method, pubkey.encode());
        // Insdert/Update the list of public keys (insert updates if key is already present)
        $all_pubkeys.insert(<$curve_type>::NAME.to_string(), pubkeys);
        keyshares
    }};
}
#[derive(Clone)]
struct AppState {
    current_epoch: Arc<Mutex<u32>>, // Always 0 for the first testnet
    my_node_idx: u32,
    /// Number of nodes with keyshares
    n: u32,
    /// Threshold of nodes
    t: u32,
    /// Extra nodes that should be queried in addition to t for redundancy
    epsilon: u32,
    /// Maps (Curve::Name, Method enum variant) to a (potential) keyshare. Not all methods are gauranteed ot be implemented for all curves.
    redis: Arc<Mutex<Connection>>,
    eth: Provider<Http>,
    relayer_ip: std::net::IpAddr,
    secp256k1_jwt_keyshares: Arc<Mutex<HashMap<String, <Secp256k1 as Curve<32>>::Scalar>>>,
    secp256k1_oprf_keyshares: Arc<Mutex<HashMap<String, <Secp256k1 as Curve<32>>::Scalar>>>,
    babyjubjub_keyshares: Arc<Mutex<HashMap<String, <BabyJubJub as Curve<32>>::Scalar>>>,
}
#[tokio::main]
async fn main() {
    println!("version 1");
    let my_node_idx = env::var("MY_NODE_IDX").expect("MY_NODE_IDX not set").parse().expect("Failed to parse MY_NODE_IDX");
    // let mut babyjubjub_keyshares = HashMap::new();
    // babyjubjub_keyshares.insert(
    //     "DecryptBabyJubJub".to_string(),
    //     <BabyJubJub as Curve<32>>::Scalar::from_biguint_vartime(
    //         BigUint::from_bytes_be(&hex::decode(&env::var("BABYJUBJUB_DECRYPT_KEYSHARE").expect("BABYJUBJUB_DECRYPT_KEYSHARE not set")).expect("Invalid private key hex"))
    //     ).expect("Invalid private key value")
    // );
    let mut pubkeys_for_all_curves: HashMap<String, Pubkeys> = HashMap::new();
    let secp256k1_jwt_keyshares = keyshares_for_curve_method!("JWTPRFSecp256k1", Secp256k1, pubkeys_for_all_curves);
    let secp256k1_oprf_keyshares = keyshares_for_curve_method!("OPRFSecp256k1", Secp256k1, pubkeys_for_all_curves);
    let babyjubjub_keyshares = keyshares_for_curve_method!("DecryptBabyJubJub", BabyJubJub, pubkeys_for_all_curves);
    #[cfg(feature = "testnet_logging")]
    testnet_logging(
        "register-prover",
        &NodeDataWithoutIP {
            idx: my_node_idx,
            pubkeys: pubkeys_for_all_curves,
        },
    )
    .await;
    let state = AppState {
        my_node_idx,
        secp256k1_jwt_keyshares: Arc::new(Mutex::new(secp256k1_jwt_keyshares)),
        secp256k1_oprf_keyshares: Arc::new(Mutex::new(secp256k1_oprf_keyshares)),
        babyjubjub_keyshares: Arc::new(Mutex::new(babyjubjub_keyshares)),
        redis: Arc::new(Mutex::new(
            redis::Client::open(env::var("REDIS_URL").expect("REDIS_URL not set"))
                .expect("Failed to open Redis connection")
                .get_connection()
                .expect("Failed to get Redis connection"),
        )),
        eth: Provider::<Http>::try_from(env::var("L1_RPC").expect("L1_RPC note set")).expect("Failed to instatiate ETH provider"),
        current_epoch: Arc::new(Mutex::new(0)),
        n: env::var("THRESHOLD_N").expect("THRESHOLD_N not set").parse().expect("Failed to parse THRESHOLD_N"),
        t: env::var("THRESHOLD_T").expect("THRESHOLD_T not set").parse().expect("Failed to parse THRESHOLD_T"),
        epsilon: env::var("THRESHOLD_EPSILON").expect("THRESHOLD_EPSILON not set").parse().expect("Failed to parse THRESHOLD_EPSILON"),
        relayer_ip: env::var("RELAYER_IP").expect("RELAYER_IP not set").parse().expect("Failed to parse RELAYER_IP"),
    };
    println!("My node idx: {}", &state.my_node_idx);
    let app = Router::new()
        .route("/proof/JWTPRFSecp256k1", post(secp256k1_prf_for_jwts::handle))
        .route("/proof/OPRFSecp256k1", post(secp256k1_oprf::handle))
        .route("/proof/DecryptBabyJubJub", post(babyjubjub::handle))
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();
    // run it
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
endpoint_for_curve!(secp256k1_oprf, Secp256k1, <Secp256k1 as Curve<32>>::Point, secp256k1_oprf_keyshares);
endpoint_for_curve!(secp256k1_prf_for_jwts, Secp256k1, <Secp256k1 as Curve<32>>::Point, secp256k1_jwt_keyshares);
endpoint_for_curve!(babyjubjub, BabyJubJub, <BabyJubJub as Curve<32>>::Point, babyjubjub_keyshares);
