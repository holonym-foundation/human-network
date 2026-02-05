use lazy_static::lazy_static;
use std::env;
/// Represents the application's environment configuration.
#[derive(Debug, Clone)]
pub struct Environment {
    pub rsa_seed: String,
    pub secp256k1_seed: String,
    pub node_type: String,
    pub node_multiaddr: String,
    pub l1_rpc: String,
    pub l2_rpc: String,
    pub redis_url: String,
    pub othentic_rpc_url: String,
    pub avs_governance_address: String,
    pub attestation_center_address: String,
    pub othentic_bootstrap_id: String,
    pub othentic_bootstrap_seed: String,
    pub peer_registry_address: String,
    pub l1_chain: u64,
    pub l2_chain: u64,
}
/// Retrieves the environment variable or returns a default value with error logging.
fn get_var_or_err(var: &str, err_buf: &mut String) -> String {
    match env::var(var) {
        Ok(val) => val.trim().to_string(),
        Err(_) => {
            err_buf.push_str(var);
            err_buf.push_str(", ");
            String::new()
        }
    }
}
impl Environment {
    /// Creates a new `Environment` instance by loading values from environment variables.
    /* trunk-ignore(clippy/new_without_default) */
    pub fn new() -> Self {
        let mut err_buf = String::new();
        let rsa_seed = get_var_or_err("RSA_SEED", &mut err_buf);
        let secp256k1_seed = get_var_or_err("SECP256K1_SEED", &mut err_buf);
        let node_type = get_var_or_err("NODE_TYPE", &mut err_buf);
        let node_multiaddr = get_var_or_err("NODE_MULTIADDR", &mut err_buf);
        let l1_rpc = get_var_or_err("L1_RPC", &mut err_buf);
        let l2_rpc = get_var_or_err("L2_RPC", &mut err_buf);
        let redis_url = get_var_or_err("REDIS_URL", &mut err_buf);
        let othentic_rpc_url = get_var_or_err("OTHENTIC_RPC_URL", &mut err_buf);
        let avs_governance_address = get_var_or_err("AVS_GOVERNANCE_ADDRESS", &mut err_buf);
        let attestation_center_address = get_var_or_err("ATTESTATION_CENTER_ADDRESS", &mut err_buf);
        let othentic_bootstrap_id = get_var_or_err("OTHENTIC_BOOTSTRAP_ID", &mut err_buf);
        let othentic_bootstrap_seed = get_var_or_err("OTHENTIC_BOOTSTRAP_SEED", &mut err_buf);
        let peer_registry_address = get_var_or_err("PEER_REGISTRY_ADDRESS", &mut err_buf);
        let l1_chain = get_var_or_err("L1_CHAIN", &mut err_buf).trim().parse().unwrap();
        let l2_chain = get_var_or_err("L2_CHAIN", &mut err_buf).trim().parse().unwrap();

        if !err_buf.is_empty() {
            panic!("Environment variables missing: {}", err_buf);
        }
        Self {
            rsa_seed,
            secp256k1_seed,
            node_type,
            node_multiaddr,
            l1_rpc,
            l2_rpc,
            redis_url,
            othentic_rpc_url,
            avs_governance_address,
            attestation_center_address,
            othentic_bootstrap_id,
            othentic_bootstrap_seed,
            peer_registry_address,
            l1_chain,
            l2_chain,
        }
    }
}
lazy_static! {
    /// Global singleton of `Environment` initialized with environment variables.
    pub static ref ENVIRONMENT: Environment = Environment::new();
}
impl Default for Environment {
    fn default() -> Self { Self::new() }
}
