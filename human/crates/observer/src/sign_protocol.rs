use serde::{Deserialize, Serialize};
use tokio::process::Command;
use crate::custom_errors::Error;
use crate::constants;
#[derive(Serialize, Deserialize)]
pub struct EthSignAttestationIndexResponseData {
    pub page: i64,
    pub rows: Vec<serde_json::Value>,
}
#[derive(Serialize, Deserialize)]
pub struct EthSignAttestationIndexResponse {
    pub message: String,
    #[serde(rename = "statusCode")]
    pub status_code: i64,
    pub success: bool,
    pub data: EthSignAttestationIndexResponseData,
}
/// Get Clean Hands attestations
pub async fn get_attestations(
    attester_addr: String,
    action_nullifier: &String,
) -> Result<EthSignAttestationIndexResponse, Error> {
    let attestation_query_data = serde_json::json!({
        "schemaId": "onchain_evm_10_0x8",
        // Zeronym relayer address = 0xB1f50c6C34C72346b1229e5C80587D0D659556Fd
        "attestor": attester_addr,
        "indexingValue": action_nullifier,
    });
    let url = "https://mainnet-rpc.sign.global/api/index/attestations";
    let mut res: surf::Response = surf::get(url)
        .header("x-sign-api-key", constants::SIGN_PROTOCOL_API_KEY)
        .query(&attestation_query_data)
        .unwrap()
        .await
        .map_err(|_e| Error::CustomBadRequest("Encountered error while trying to query attestation"))?;
    if res.status() != 200 {
        return Err(Error::CustomBadRequest("Received non 200 HTTP status when querying for attestation"));
    }
    let res_body: String = res
        .body_string()
        .await
        .map_err(|_e| Error::CustomBadRequest("Could not parse response to attestation query into string"))?;
    let res_body: EthSignAttestationIndexResponse = serde_json::from_str(&res_body).map_err(|_e| Error::CustomBadRequest("Could not parse response to attestation query into struct"))?;
    Ok(res_body)
}
/// Issue a Clean Hands attestation
pub async fn issue_attestation(
    recipient: &String,
    action_id: &String,
    action_nullifier: &String,
    expiry: i64
) -> Result<std::process::Output, Error> {
    let inputs = serde_json::json!({
        "recipient": recipient,
        "actionId": action_id,
        "actionNullifier": action_nullifier,
        "expiry": expiry,
    });
    // let res = Command::new("node")
    return Command::new("node")
        .env("ATTESTOR_PRIVATE_KEY", constants::ATTESTOR_PRIVATE_KEY.as_str())
        .env("OP_RPC_URL", constants::OP_RPC_URL.as_str())
        .arg(format!("{}/ethsign-relayer.js", *constants::NODEJS_SCRIPT_PATH))
        .arg(serde_json::to_string(&inputs).unwrap())
        .output()
        .await
        .map_err(|_e| Error::CustomBadRequest("Encountered an error while trying to issue attestation"));
}
