use serde::{Deserialize, Serialize};
use std::env;
use crate::custom_errors::Error;
#[derive(Serialize, Deserialize, Debug)]
pub struct ObservationSchema {
    /// Distinct from _id. This is a hash of the fields of the observation. Allows for more efficient lookups.
    pub id: String,
    pub user_address: Option<String>,
    pub user_sui_address: Option<String>,
    pub signature: String,
    pub access_contract: String,
    pub zkp_public_values: Vec<String>,
    /// On April 22, 2025, we enabled payments via the zeronym orders service. This is
    /// the order ID of the order used to pay for the attestation.
    pub order_id: Option<String>
}
#[derive(Clone)]
pub struct ObserverMongoDBCollections {
    pub observations: mongodb::Collection<ObservationSchema>,
}
impl ObserverMongoDBCollections {
    pub async fn get_db() -> mongodb::Database {
        let mdb_uri = env::var("MONGODB_URI").expect("MONGODB_URI must be set");
        let mdb_clientoptions = mongodb::options::ClientOptions::parse(mdb_uri).await.unwrap();
        let mdb_client = mongodb::Client::with_options(mdb_clientoptions).unwrap();
        mdb_client.database("userdb")
    }
    pub async fn init() -> Result<ObserverMongoDBCollections, Error> {
        let db = Self::get_db().await;
        Ok(ObserverMongoDBCollections {
            observations: db.collection("observations"),
        })
    }
}
