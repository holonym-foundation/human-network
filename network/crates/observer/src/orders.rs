use std::str::FromStr;
use ethers::{
    types::U256,
    utils::keccak256,
};
use std::env;
use crate::custom_errors::Error;

pub const PAYMENT_SALT_STR: &str =
    "22557218514837509687577551223814030923306036243544724208542958861224904505416";

fn u256_to_32bytes(u: U256) -> Vec<u8> {
    let mut bytes = vec![0u8; 32];
    u.to_big_endian(&mut bytes);
    bytes
}

pub async fn validate_order(action_nullifier: &String) -> Result<String, Error> {
    let action_nullifier_u256 = U256::from_str(action_nullifier.clone().as_str()).map_err(|_e| Error::CustomBadRequest("Failed to parse action nullifier into U256"))?;
    let payment_salt_u256 = U256::from_dec_str(PAYMENT_SALT_STR).unwrap();

    // add action_nullifier_u256 and salt_u256
    let preimage = action_nullifier_u256.overflowing_add(payment_salt_u256).0;

    // get keccak256 of preimage
    let digest = format!("0x{}", hex::encode(keccak256(u256_to_32bytes(preimage))));

    // digest is externalOrderId
    let mut order_status_res: surf::Response = surf::get(format!("{}/orders/{}/transaction/status", env::var("ID_SERVER_URL").unwrap(), digest))
        .send()
        .await
        .map_err(|_e| Error::CustomBadRequest("Encountered error while trying to query attestation"))?;

    // check if order_status is 200
    if order_status_res.status() != 200 {
        return Err(Error::CustomBadRequest("Order status is not 200"));
    }
    // parse order_status body as json
    let order_status = order_status_res.body_string().await.map_err(|_e| Error::CustomBadRequest("Could not parse order response into string"))?;
    let order_status_json: serde_json::Value = serde_json::from_str(order_status.as_str()).map_err(|_| Error::CustomBadRequest("Failed to parse order as JSON"))?;
    // println!("order_status_json: {:?}", order_status_json);

    // check txReceipt.confirmations > 0
    let confirmations = order_status_json
        .get("txReceipt")
        .and_then(|receipt| receipt.get("confirmations"))
        .and_then(|conf| conf.as_u64())
        .unwrap_or(0);

    // if confirmations is 0, return error
    if confirmations == 0 {
        return Err(Error::CustomBadRequest("Order is not confirmed yet"));
    }

    // check if order is fulfilled
    let is_fulfilled = order_status_json
        .get("order")
        .and_then(|order| order.get("fulfilled"))
        .and_then(|fulfilled| fulfilled.as_bool())
        .unwrap_or(false);

    // if order is fulfilled, return error
    if is_fulfilled {
        return Err(Error::CustomBadRequest("Order is already fulfilled"));
    }

    Ok(digest)
}

pub async fn mark_order_fulfilled(order_id: String) -> Result<(), Error> {
    // get id-server api key from environment variable
    let api_key = env::var("ID_SERVER_API_KEY")
        .map_err(|_| Error::CustomBadRequest("ID_SERVER_API_KEY environment variable not set"))?;
    
    let fulfilled_order_res: surf::Response = surf::get(format!("{}/orders/{}/fulfilled", env::var("ID_SERVER_URL").unwrap(), order_id))
        .header("x-api-key", api_key)
        .send()
        .await
        .map_err(|_e| Error::CustomBadRequest("Encountered error while trying to query attestation"))?;

    // log fulfilled_order status
    // println!("fulfilled_order status: {:?}", fulfilled_order_res.status());

    // check if fulfilled_order is 200
    if fulfilled_order_res.status() != 200 {
        return Err(Error::CustomBadRequest("Failed to set order status to fulfilled"));
    }

    Ok(())
}
