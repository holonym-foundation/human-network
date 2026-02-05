use anyhow::anyhow;
use base64::{prelude::BASE64_STANDARD_NO_PAD, Engine};
use ethers::types::{Address, Signature};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use human_crypto::zkinjmask::JWTClaims;
use redis::{Commands, Connection};
use serde_json::Value;
use tokio::sync::MutexGuard;
/// Allows caching the jwk endpoint to reduce request latency and prevent rate limiting
/// It is important JWK_CACHE_SEC is short for security and UX at the time of key rotation
const JWK_CACHE_SEC: u64 = 600;
/// Verifies the jwt against the key from the jwk endpoint (issuer/.well-known/jwks.json) and checks that the jwk endpoint is correct
/// Returns the unique string representing the important claims, i.e. those unique and invariant for this (user, website) tuple,
/// followed by the public mask value as per the zkInjectedMask protocol
/// Returns an Error if the jwt or its relevant fail verification.
///
/// If the JWT is signed by the enclave, bypasses the typical checks and returns the unique string and public mask
///
/// `jwt`: The JWT in string format i.e. base64.base64.base64
/// `key_idx`: The index of the correct key in the JWK endpoint. For the Clerk setup this is 0 since there aren't >1 key
pub async fn verify_jwt(jwt: &str, key_idx: usize, jwk_cache: Option<&mut MutexGuard<'_, Connection>>) -> Result<(String, String), anyhow::Error> {
    let unvalidated = JWTClaims::from_raw_token_unchecked(jwt)?;

    if unvalidated.from_enclave.unwrap_or(false) {
        // verify it and if successful, return the unique string and public mask
        verify_enclave_jwt(jwt)?;
        return Ok((unvalidated.unique_string()?, unvalidated.pubmask));
    }

    let jwk = get_jwk(&unvalidated.iss, jwk_cache).await?;
    let key: &Value = jwk
        .get("keys")
        .ok_or(anyhow!("JWK lacks 'keys' field"))?
        .as_array()
        .ok_or(anyhow!("JWK 'keys' is not an array"))?
        .get(key_idx)
        .ok_or(anyhow!("JWK key index is out of boudns"))?;
    let (modulus, exponent) = (
        key.get("n")
            .ok_or(anyhow!("JWK key (assuming RSA) is missing modulus"))?
            .as_str()
            .ok_or(anyhow!("key has invalid format"))?,
        key.get("e")
            .ok_or(anyhow!("JWK key (assuming RSA) is missing exponent"))?
            .as_str()
            .ok_or(anyhow!("key has invalid format"))?,
    );
    let key = DecodingKey::from_rsa_components(modulus, exponent)?;
    let validation = Validation::new(Algorithm::RS256);
    let token = decode::<JWTClaims>(jwt, &key, &validation)?;
    if token.claims.iss != unvalidated.iss {
        return Err(anyhow!("Issuer in the JWT does not match the issuer whose key was fetched"));
    };
    Ok((token.claims.unique_string()?, token.claims.pubmask))
}

// Test address for enclave signer. Can be overridden by the ENCLAVE_SIGNER_ADDRESS environment variable e.g. in production.
const ENCLAVE_SIGNER_ADDRESS: &str = "0x9C26A4a9dc213511Af416fE36571213a389C3d40"; // The testing environment address is "0x558D6eC349225971c16Af134e8CB81e91B3289dA";
pub fn verify_enclave_jwt(jwt: &str) -> Result<(), anyhow::Error> {
    let enclave_signer_address = std::env::var("ENCLAVE_SIGNER_ADDRESS").unwrap_or(ENCLAVE_SIGNER_ADDRESS.to_string())
                                    .parse::<Address>()?;
    let unvalidated = JWTClaims::from_raw_token_unchecked(jwt)?;
    if !unvalidated.from_enclave.unwrap_or(false) { return Err(anyhow!("Must be from enclave")) }

    let sig = jwt.split(".").nth(2).ok_or(anyhow!("JWT lacks signature"))?;
    let sig = BASE64_STANDARD_NO_PAD.decode(sig)?;
    let sig = Signature::try_from(sig.as_slice())?;
    let recovered = sig.recover(unvalidated.unique_string()?.as_bytes())?;
    
    if recovered == enclave_signer_address {
        Ok(())
    } else {
        Err(anyhow!("JWT signature from {} does not match enclave signer address: {}", recovered, enclave_signer_address))
    }
}
/// Retrieves the jwk endpoint, caching the result for JWK_CACHE_SEC seconds
/// If no redis::Conection is given, the cache is ignored and the jwk endpoint is fetched
/// It is important for security and UX around key rotation that JWK_CACHE_SEC is short
pub async fn get_jwk(issuer: &str, connection: Option<&mut MutexGuard<'_, Connection>>) -> Result<Value, anyhow::Error> {
    match connection {
        Some(c) => {
            let cached: String = c.get(format!("jwk:issuer:{}", issuer))?;
            Ok(serde_json::from_str(&cached)?)
        }
        None => {
            let jwk_endpoint = format!("{}/.well-known/jwks.json", issuer);
            let jwk: Value = reqwest::Client::new().get(&jwk_endpoint).send().await?.json().await?;
            // If Connection was supplied, cache the result
            if let Some(conn) = connection {
                let _: Option<()> = conn.set_ex(format!("jwk:issuer:{}", issuer), serde_json::to_string(&jwk).unwrap(), JWK_CACHE_SEC).ok();
            };
            Ok(jwk)
        }
    }
}
#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn invalid_sig_fail() {
        let jwt = "eyJhbGciOiJSUzI1NiIsImNhdCI6ImNsX0I3ZDRQRDExMUFBQSIsImtpZCI6Imluc18yaG8yUUc5TGM4WjRFTGY3UkhzM3EyWWYxYzMiLCJ0eXAiOiJKV1QifQ.eyJhenAiOiJodHRwOi8vZWMyLTU0LTI0Ni0yNDktMTEwLmV1LXdlc3QtMS5jb21wdXRlLmFtYXpvbmF3cy5jb206MzAwMCIsImV4cCI6MTcyMTE2MTg2OSwiaWF0IjoxNzIxMTYxODA5LCJpc3MiOiJodHRwczovL2hhbmR5LXF1YWlsLTg0LmNsZXJrLmFjY291bnRzLmRldiIsImp0aSI6ImY4N2E3ZGI0MTMxOGRkZDFlODc2IiwibmJmIjoxNzIxMTYxNzk5LCJwdWJtYXNrIjoiMDJkMWI5N2ViN2I3MGM3ZjlhMTIwZGFjNTkxOTdmNWFhODQzOWFlMjM4MWY3ZTFkNDU5YWQ2M2Y2YmY1M2M2YWUyIiwic2lkIjoic2Vzc18yakZKeDhxNWxhZ3phcFR5Q0VSQzRCemZOa1kiLCJzdWIiOiJ1c2VyXzJobzNpVWVCVXZveFdkTWRPYTdGTmpid0pueSJ9.Wd6DmUnwfaq0V1FDkb8FYM-bBT247fy3PeABi-fNvjKrGOnkFLSw25CFNXzCl1pQP3aKKsWAZVKhm-24P8KJSjxg5Cq1uXt8mwotFrGB3rNshWXYBY5sComoeHfe8sG_UxZJmi6-t-vrPT7Jp_yqlcz1h-kCc3r_BTPskU0O0ibTzVJa0YOBypC2UPC695nOTXsP-lWmlfXBpykhQfwDsnnH3DEy7x3cRxEOeHEIi6xRdTDSmZPDszthhInN_BOryFxA5cy-qrpUhMCl75iCOTYmNaw5lMSNG0Xt8c-CmUShwaGaNLInem3vongf3YosgjMIvRi81W9S5YtT_ABU1g";
        let res = verify_jwt(jwt, 0, None).await;
        assert_eq!(res.unwrap_err().to_string(), "InvalidSignature");
    }
}
