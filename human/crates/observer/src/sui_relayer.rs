use std::env;
use tokio::process::Command;
use lazy_static::lazy_static;
use crate::custom_errors::Error;

lazy_static! {
    static ref SUI_SBT_PACKAGE_ID: String = env::var("SUI_SBT_PACKAGE_ID")
        .unwrap_or_else(|_| "0x53ddebd997f0e57dc899d598f12001930e228dddadf268a41d4c9a7c1df47a97".to_string());
    static ref SUI_SBT_PACKAGE_MINTER_CAP: String = env::var("SUI_SBT_PACKAGE_MINTER_CAP")
        .unwrap_or_else(|_| "0x938f1d700b292340201a701f25bdf37d3cf53da03d8a77a8cc5253c189f214b9".to_string());
}
pub async fn init_sui() {
    // -------- Create config --------
    let res = Command::new("sui")
        .arg("client")
        .arg("-y")
        .output()
        .await
        .unwrap();
    if res.stderr.len() > 0 {
        panic!("Error creating Sui config: {}", String::from_utf8(res.stderr).unwrap());
    }
    let separator = "------------------------------------------------------------";
    println!(
        "{}\nOutput from `sui client -y`...\n{}{}",
        separator,
        String::from_utf8(res.stdout).unwrap(),
        separator
    );
    // -------- Create mainnet env --------
    let rpc_url = "https://fullnode.mainnet.sui.io:443";
    let res = Command::new("sui")
        .arg("client")
        .arg("new-env")
        .arg("--alias")
        .arg("mainnet")
        .arg("--rpc")
        .arg(rpc_url)
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8(res.stdout).unwrap();
    if res.stderr.len() > 0 && !stdout.contains("Environment config with name [mainnet] already exists") {
        let stderr = String::from_utf8(res.stderr).unwrap();
        panic!("Error configuring mainnet env for Sui client:\nstderr...\n{}\nstdout...\n{}", stderr, stdout);
    }
    println!("{}\nOutput from `sui client new-env --alias mainnet --rpc {}`...\n{}{}", separator, rpc_url, stdout, separator);
    // -------- Switch to mainnet --------
    let res = Command::new("sui")
        .arg("client")
        .arg("switch")
        .arg("--env")
        .arg("mainnet")
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8(res.stdout).unwrap();
    if res.stderr.len() > 0 && !stdout.contains("Active environment switched to [mainnet]") {
        let stderr = String::from_utf8(res.stderr).unwrap();
        panic!("Error switching Sui client to mainnet:\nstderr...\n{}\nstdout...\n{}", stderr, stdout);
    }
    println!("{}\nOutput from `sui client switch --env mainnet`...\n{}{}", separator, stdout, separator);
    // -------- Import key --------
    let private_key = env::var("SUI_PRIVATE_KEY").expect("SUI_PRIVATE_KEY must be set");
    let key_scheme = env::var("SUI_PRIVATE_KEY_SCHEME").expect("SUI_PRIVATE_KEY_SCHEME must be one of ed25519, secp256k1, secp256r1");
    let res = Command::new("sui")
        .arg("keytool")
        .arg("import")
        .arg(private_key)
        .arg(key_scheme)
        .output()
        .await
        .unwrap();
    if res.stderr.len() > 0 {
        panic!("Error importing Sui key: {}", String::from_utf8(res.stderr).unwrap());
    }
    let stdout = String::from_utf8(res.stdout).unwrap();
    println!("{}\nOutput from `sui keytool import <private-key> <key-scheme>`...\n{}{}", separator, stdout, separator);
    // -------- Tell Sui client to use imported key --------
    let key_alias = stdout.lines().find(|line| line.contains("alias")).unwrap().split("│").nth(2).unwrap().trim().to_string();
    let res = Command::new("sui")
        .arg("client")
        .arg("switch")
        .arg("--address")
        .arg(&key_alias)
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8(res.stdout).unwrap();
    println!("{}\nOutput from `sui client switch --address {}`...\n{}{}", separator, key_alias, stdout, separator);
}
pub async fn mint_sui_sbt(
    user_sui_address: String,
    circuit_id: String,
    action_id: String,
    action_nullifier: String,
    expiry: i64,
) -> Result<String, Error> {
    let res = Command::new("sui")
        .arg("client")
        .arg("call")
        .arg("--package")
        .arg(SUI_SBT_PACKAGE_ID.as_str())
        .arg("--module")
        .arg("sbt")
        .arg("--function")
        .arg("mint")
        .arg("--args")
        .arg(SUI_SBT_PACKAGE_MINTER_CAP.as_str())
        .arg(user_sui_address)
        .arg(circuit_id)
        .arg(action_id)
        .arg(action_nullifier)
        .arg(expiry.to_string())
        .arg("--gas-budget")
        .arg("10000000")
        .output()
        .await
        .map_err(|err| {
            println!("Error: {:?}", err);
            Error::CustomBadRequest("Could not mint SUI SBT")
        })?;
    let output = String::from_utf8(res.stdout).map_err(|_e| Error::CustomBadRequest("Could not parse tx hash"))?;
    if res.stderr.len() > 0 {
        println!("mint_sui_sbt: Error: {:?}", String::from_utf8(res.stderr).unwrap());
        if output.len() == 0 {
            return Err(Error::CustomBadRequest("Could not mint SUI SBT"));
        }
    }
    println!("mint_sui_sbt: stdout:\n{}", output);
    let tx_hash = output.split("Transaction Digest: ").collect::<Vec<&str>>()[1].split("\n").collect::<Vec<&str>>()[0].trim();
    Ok(tx_hash.to_string())
}
