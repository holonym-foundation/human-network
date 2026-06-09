use ethers::abi::Address;
use lazy_static::lazy_static;
use std::{str::FromStr, env};
lazy_static! {
    pub static ref CLEAN_HANDS_ISSUER_ADDRESS: String = env::var("CLEAN_HANDS_ISSUER_ADDRESS").expect("CLEAN_HANDS_ISSUER_ADDRESS not set");
    pub static ref NODEJS_SCRIPT_PATH: String = env::var("NODEJS_SCRIPTS_PATH").expect("NODEJS_SCRIPTS_PATH not set");
    pub static ref ATTESTOR_PRIVATE_KEY: String = env::var("ATTESTOR_PRIVATE_KEY").expect("ATTESTOR_PRIVATE_KEY not set");
    pub static ref OP_RPC_URL: String = env::var("OP_RPC_URL").expect("OP_RPC_URL not set");
    pub static ref SIGN_PROTOCOL_API_KEY: String = env::var("SIGN_PROTOCOL_API_KEY").expect("SIGN_PROTOCOL_API_KEY not set");
    pub static ref ACCESS_CONDITION_CONTRACTS: Vec<Address> = {
        let contracts_str = env::var("ACCESS_CONDITION_CONTRACTS")
            .unwrap_or_else(|_| "0x69e3373c6165045c3c59a11645415eff8fd15cac,0x3a0d4A524Aa53A29959Aaef1Cff899F35Cc7F766,0xE6BaB4228Ad23D59A1F1D69f1Cb14C2Ba29D91e9".to_string());
        contracts_str
            .split(',')
            .map(|addr| Address::from_str(addr.trim()).expect(&format!("Invalid address: {}", addr)))
            .collect()
    };
}
pub const CLEAN_HANDS_CIRCUIT_ID: &str = "0x2af184333d99b600000000000000000000000000000000000000000000000000";
pub const BABY_JUB_JUB_MODULUS: &str = "21888242871839275222246405745257275088548364400416034343698204186575808495617";
// const HUMAN_PUBKEY: [&str; 2] = [
//     "7298940672059965768641845964270773272795981288081284939122224850165910347243",
//     "7926082103045993969094841716375912351275414485995177415265999219539763466679"
// ];
// const HUMAN_PUBKEY: [[u8; 32]; 2] = [
//     [16,35,13,212,87,211,179,160,124,193,120,141,167,113,139,225,136,204,137,101,188,182,78,205,62,152,204,220,57,78,29,235],
//     [17,134,0,228,223,237,114,219,143,0,231,36,49,2,182,93,76,120,240,204,143,222,219,94,239,151,240,74,13,31,17,183]
// ];
pub const HUMAN_PUBKEY: [[u8; 32]; 2] = [
    [
        24, 182, 202, 82, 8, 234, 40, 13, 243, 164, 125, 228, 75, 173, 167, 3, 157, 72, 107, 0, 178, 160, 103, 241, 124, 31, 81, 105, 201, 239, 121, 27,
    ],
    [
        6, 125, 71, 71, 213, 123, 133, 202, 42, 28, 23, 10, 99, 40, 11, 47, 144, 149, 211, 19, 184, 62, 162, 115, 41, 121, 251, 198, 66, 156, 28, 249,
    ],
];