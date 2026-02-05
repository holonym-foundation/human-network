use crate::constants;
use crate::custom_errors::Error;
use tokio::process::Command;
pub async fn issue_attestation(
    recipient: &String,
    action_id: &String,
    expiry: i64
) -> Result<std::process::Output, Error> {
    let inputs = serde_json::json!({
        "recipient": recipient,
        "actionId": action_id,
        // "actionNullifier": action_nullifier,
        "expiry": expiry,
    });
    return Command::new("node")
        .env("ATTESTOR_PRIVATE_KEY", constants::ATTESTOR_PRIVATE_KEY.as_str())
        .arg(format!("{}/verax-relayer.js", *constants::NODEJS_SCRIPT_PATH))
        .arg(serde_json::to_string(&inputs).unwrap())
        .output()
        .await
        .map_err(|e| {
            println!("{:?}", e);
            Error::CustomBadRequest("Encountered an error while trying to issue attestation")
        });
}
