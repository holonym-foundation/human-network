use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use diesel::insert_into;
use diesel::PgConnection;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use messages::kafka::PeerReachabilityQuicStatus;
use messages::kafka::PeerReachabilityTCPStatus;
use messages::kafka::QuorumResharingInfo;
use messages::kafka::RequestInfo;
use messages::kafka::UserCreditsByMethod;
use tracing::info;

use crate::models::NewMultiplierInfo;
use crate::models::NewMultiplierServedRequest;
use crate::models::NewPeerReachabilityQuic;
use crate::models::NewPeerReachabilityTcp;
use crate::models::NewQuorumResharingInfo;
use crate::models::NewRequest;
use crate::models::NewUserCredit;
use crate::models::QuorumResharingInfoDb;
use crate::schema::multiplier_info;
use crate::schema::multiplier_served_requests;
use crate::schema::peer_reachability_quic;
use crate::schema::peer_reachability_tcp;
use crate::schema::quorum_resharing_info;
use crate::schema::requests;
use crate::schema::user_credits;

/// Trait for handling database ingestion for different Kafka message types.
#[async_trait]
pub trait KafkaMessageIngestor {
    async fn ingest(
        &self,
        conn: &mut PgConnection,
        kafka_timestamp: Option<DateTime<Utc>>,
        key: &str,
    ) -> Result<(), DieselError>;
}

#[async_trait]
impl KafkaMessageIngestor for RequestInfo {
    async fn ingest(
        &self,
        conn: &mut PgConnection,
        kafka_timestamp: Option<DateTime<Utc>>,
        key: &str, // key is request_id
    ) -> Result<(), DieselError> {
        let new_request = NewRequest {
            kafka_timestamp,
            request_id: key.to_string(),
            user_address: self.address.to_string(),
            method: self.method.as_str().to_string(),
        };

        info!("Attempting to insert new request: {:?}", new_request);
        insert_into(requests::table)
            .values(&new_request)
            .execute(conn)?;    
        info!("Successfully inserted request with request_id: {}", key);

        for multiplier_peer_id in &self.served_by_multipliers {
            let new_served_request = NewMultiplierServedRequest {
                kafka_timestamp,
                multiplier_peer_id: multiplier_peer_id.to_string(), // Convert PeerId to String
                request_id: key.to_string(),
            };
            info!(
                "Attempting to insert multiplier served request: {:?}",
                new_served_request
            );
            insert_into(multiplier_served_requests::table)
                .values(&new_served_request)
                .execute(conn)?;
            info!(
                "Successfully inserted served request for request_id: {} by multiplier: {}",
                key,
                multiplier_peer_id.to_string()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl KafkaMessageIngestor for UserCreditsByMethod {
    async fn ingest(
        &self,
        conn: &mut PgConnection,
        kafka_timestamp: Option<DateTime<Utc>>,
        key: &str,
    ) -> Result<(), DieselError> {
        // Diesel's `insert_into().values().on_conflict().do_update_set()` is used for upsert.
        // The `AsChangeset` derive on `NewUserCredit` makes this possible.
        let new_user_credit = NewUserCredit {
            kafka_timestamp,
            user_address: key.to_string(),
            method: self.method.as_str().to_string(),
            exhausted_credits: BigDecimal::from(self.exhausted_credits), // Convert u128 to BigDecimal
        };

        info!("Attempting to upsert user credits: {:?}", new_user_credit);
        diesel::insert_into(user_credits::table)
            .values(&new_user_credit)
            .on_conflict((user_credits::user_address, user_credits::method))
            .do_update()
            .set(&new_user_credit) // Update all fields from new_user_credit
            .execute(conn)?;
        info!(
            "Successfully upserted credits for user: {} method: {}",
            key, self.method
        );
        Ok(())
    }
}

#[async_trait]
impl KafkaMessageIngestor for PeerReachabilityQuicStatus {
    async fn ingest(
        &self,
        conn: &mut PgConnection,
        kafka_timestamp: Option<DateTime<Utc>>,
        key: &str, // key is peer_id
    ) -> Result<(), DieselError> {
        let new_quic_status = NewPeerReachabilityQuic {
            kafka_timestamp,
            multiplier_peer_id: key.to_string(), // Assuming key is peer_id
            success: self.success,
            duration_micros: self.duration.map(|d| d.as_micros() as i64), // Convert Duration to i64 microseconds
        };

        info!(
            "Attempting to upsert QUIC reachability status: {:?}",
            new_quic_status
        );
        // Use on_conflict for upsert, assuming UNIQUE(peer_id) constraint exists
        diesel::insert_into(peer_reachability_quic::table)
            .values(&new_quic_status)
            .on_conflict(peer_reachability_quic::multiplier_peer_id)
            .do_update()
            .set(&new_quic_status)
            .execute(conn)?;
        info!("Successfully upserted QUIC reachability for peer: {}", key);
        Ok(())
    }
}

#[async_trait]
impl KafkaMessageIngestor for PeerReachabilityTCPStatus {
    async fn ingest(
        &self,
        conn: &mut PgConnection,
        kafka_timestamp: Option<DateTime<Utc>>,
        key: &str, // key is peer_id
    ) -> Result<(), DieselError> {
        let new_tcp_status = NewPeerReachabilityTcp {
            kafka_timestamp,
            multiplier_peer_id: key.to_string(), // Assuming key is peer_id
            success: self.success,
            rpc_url: self.rpc_url.clone(),
        };

        info!(
            "Attempting to upsert TCP reachability status: {:?}",
            new_tcp_status
        );
        // Use on_conflict for upsert, assuming UNIQUE(peer_id) constraint exists
        diesel::insert_into(peer_reachability_tcp::table)
            .values(&new_tcp_status)
            .on_conflict(peer_reachability_tcp::multiplier_peer_id)
            .do_update()
            .set(&new_tcp_status)
            .execute(conn)?;
        info!("Successfully upserted TCP reachability for peer: {}", key);
        Ok(())
    }
}

#[async_trait]
impl KafkaMessageIngestor for QuorumResharingInfo {
    async fn ingest(
        &self,
        conn: &mut PgConnection,
        kafka_timestamp: Option<DateTime<Utc>>,
        _key: &str,
    ) -> Result<(), DieselError> {
        // 1. Insert into quorum_resharing_info table
        let new_quorum_info = NewQuorumResharingInfo {
            kafka_timestamp,
            success: self.success,
        };

        info!(
            "Attempting to insert new quorum resharing info: {:?}",
            new_quorum_info
        );
        let inserted_quorum_info: QuorumResharingInfoDb = insert_into(quorum_resharing_info::table)
            .values(&new_quorum_info)
            .get_result(conn)?; // Get the inserted row, including its generated ID
        info!(
            "Successfully inserted quorum resharing info with ID: {}",
            inserted_quorum_info.id
        );

        // 2. Insert each multiplier into multiplier_info table, linking to the new quorum_info_id
        for (idx, multiplier) in self.multipliers.iter().enumerate() {
            let new_multiplier_info = NewMultiplierInfo {
                quorum_info_id: inserted_quorum_info.id,
                multiplier_evm_address: multiplier.evm_address.to_string(),
                multiplier_peer_id: multiplier.peer_id.to_string(),
                multi_address: multiplier.address.to_string(),
                rpc_address: multiplier.rpcaddr.clone(),
                human_pub_key: multiplier.human_pub_key.to_string(), // Convert to String
                rsa_pub_key: multiplier.rsa_pub_key.to_string(),       // Convert to String
                voting_power: multiplier.voting_power.to_string(),     // Convert U256 to String
                node_idx: idx as i32, // Use the loop index for `idx`
            };
            info!("Attempting to insert multiplier info: {:?}", new_multiplier_info);
            insert_into(multiplier_info::table)
                .values(&new_multiplier_info)
                .execute(conn)?;
            info!(
                "Successfully inserted multiplier for quorum_info_id: {} idx: {}",
                inserted_quorum_info.id, idx
            );
        }
        Ok(())
    }
}