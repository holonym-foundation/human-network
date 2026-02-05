use drand_verify::{G1Pubkey, Pubkey};
use hex_literal::hex;
use reqwest::Error;
use serde::{Deserialize, Serialize};
use tracing::error;

// Public key League of Entropy Mainnet
pub const PK_LEO_MAINNET: [u8; 48] = hex!("868f005eb8e6e4ca0a47c8a77ceaa5309a47978a7c71bc5cce96366b5d7a569937c529eeda66c7293784a9402801af31");

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DrandResponse {
    pub round: u64,
    pub randomness: String,
    pub signature: String,
    pub previous_signature: String,
}

pub async fn fetch_drand_data() -> Result<DrandResponse, Error> {
    let url = "https://api.drand.sh/public/0";

    // Send a GET request to the Drand API
    let response = reqwest::get(url).await?;

    // Deserialize the JSON response into the DrandResponse struct
    let drand_data: DrandResponse = response.json().await?;

    // Return the fetched data
    Ok(drand_data)
}

pub fn verify_drand(round: u64, previous_signature: &[u8], signature: &[u8]) -> bool {
    let pk = G1Pubkey::from_fixed(PK_LEO_MAINNET).unwrap();
    match pk.verify(round, previous_signature, signature) {
        Ok(valid) => valid,
        Err(e) => {
            error!("Verification failed for round: {}", e);
            false
        }
    }
}
