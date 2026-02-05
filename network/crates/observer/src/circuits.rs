use lazy_static::lazy_static;
use std::{collections::HashMap, fs::File, io::BufReader,env};
use volonym::{
    circom::r1cs::R1CSFile,
    zkp::R1CSWithMetadata,
    Fr,
};
pub type PublicValueLabels = Vec<String>;
pub struct Circuit {
    /// The constraints along with indices of public inputs, public outputs, etc.
    pub circuit: R1CSWithMetadata<Fr>,
    /// Names of public inputs
    pub inlabels: PublicValueLabels,
    /// Names of public outputs
    pub outlabels: PublicValueLabels,
}
lazy_static! {
    pub static ref CIRCUITS: HashMap<String, Circuit> = {
        let mut h = HashMap::new();
        let circuit_dir = env::var("CIRCUIT_DIR").expect("CIRCUIT_DIR env var not set");
        h.insert(
            "V3CleanHands".into(),
            Circuit {
                circuit: R1CSFile::from_reader(
                    BufReader::new(
                        File::open(format!("{}/circom/V3CleanHands.r1cs", circuit_dir)).expect("Could not find R1CS file")
                    )
                )
                .expect("Could not parse R1CS file")
                .to_crate_format(),

                inlabels: vec![
                    "encryptedTox".into(),
                    "encryptedToy".into(),
                    "expiry".into(),
                    "recipient".into(),
                    "actionId".into(),
                ],

                // Names of public outputs
                outlabels: vec![
                    "actionNullifier".into(),
                    "issuerAddress".into(),
                    "pubkey1x".into(),
                    "pubkey1y".into(),
                    "ciphertext1x".into(),
                    "ciphertext1y".into(),
                    "pubkey2x".into(),
                    "pubkey2y".into(),
                    "ciphertext2x".into(),
                    "ciphertext2y".into(),
                    "pubkey3x".into(),
                    "pubkey3y".into(),
                    "ciphertext3x".into(),
                    "ciphertext3y".into(),
                ]
            }
        );

        h
    };
}
