use crate::{
    models::{NewMonitorTaskState, NewOperatorPointsLedger, NewTask, NewTaskAttestor, NewTaskPerformer},
    schema::{monitor_task_state, operator_points_ledger, task_attestors, task_performers, tasks},
    PgPool,
};
use alloy::{
    primitives::{Address, Bytes, B256, U256},
    providers::{
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
        Identity, Provider, ProviderBuilder, RootProvider, WsConnect,
    },
    rpc::types::BlockNumberOrTag,
    sol,
};
use alloy_sol_types::{SolEvent, SolType};
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, instrument, warn};

pub enum OperatorRole {
    Performer,
    Attestor,
}

impl OperatorRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Performer => "performer",
            Self::Attestor => "attestor",
        }
    }
}

sol!(
    #[sol(rpc)]
    contract AttestationCenter {
        event TaskSubmitted(
            address indexed operator,
            uint32 taskNumber,
            string proofOfTask,
            bytes data,
            uint16 indexed taskDefinitionId,
            uint256[] attestersIds
        );
        struct TaskData {
            uint256 task_size;
            address[] performers;
        }
        // Define the function from the smart contract that returns operator details
        function getActiveOperatorsDetails() external view returns (OperatorDetails[] memory _operators);
        // Lookup any operator (including inactive) by ID
        function getOperatorPaymentDetail(uint256 _operatorId) external view returns (PaymentDetails memory);
    }

    // Define the struct that the smart contract function returns
    struct OperatorDetails {
        address operator;
        uint256 operatorId;
        uint256 votingPower;
        uint256 feeToClaim;
    }

    struct PaymentDetails {
        address operator;
        uint256 lastPaidTaskNumber;
        uint256 feeToClaim;
        uint8 paymentStatus;
    }
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSubmittedEvent {
    pub operator: Address,
    pub task_number: u32,
    pub proof_of_task: String,
    pub data: Bytes,
    pub task_size: usize,
    pub performers: Vec<Address>, // Populated from the `performers` field in the JSON data
    pub task_definition_id: u16,
    pub attesters_ids: Vec<U256>,         // The raw IDs from the event
    pub attester_addresses: Vec<Address>, // The addresses converted from attesters_ids
    pub block_number: u64,
    pub transaction_hash: B256,
    pub log_index: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct TaskData {
    task_size: usize,
    performers: Vec<Address>,
}

impl TaskSubmittedEvent {
    pub fn from_log(log: &alloy::rpc::types::Log) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let prim_log: alloy::primitives::Log = log.clone().into();
        let decoded = AttestationCenter::TaskSubmitted::decode_log(&prim_log, true)?;
        Ok(Self {
            operator: decoded.operator,
            task_number: decoded.taskNumber,
            proof_of_task: decoded.proofOfTask.clone(),
            data: decoded.data.data.clone(),
            // These fields will be populated in `process_log` after async calls
            task_size: 0,
            performers: Vec::new(),
            task_definition_id: decoded.taskDefinitionId,
            attesters_ids: decoded.attestersIds.clone(),
            attester_addresses: Vec::new(),
            block_number: log.block_number.unwrap_or_default(),
            transaction_hash: log.transaction_hash.unwrap_or_default(),
            log_index: log.log_index.unwrap_or_default(),
        })
    }
    pub fn ingest(&self, pool: &PgPool) -> Result<(), DieselError> {
        let mut conn = match pool.get() {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to acquire database connection from pool: {}", e);
                return Err(DieselError::NotFound); // Skip processing this message and try next
            }
        };
        conn.build_transaction().run::<_, DieselError, _>(|conn| {
            let new_task = NewTask {
                task_number: self.task_number as i32,
                task_definition_id: self.task_definition_id as i32,
                task_size: self.task_size as i64,
                proof_of_task: self.proof_of_task.clone(),
                operator: self.operator.as_slice().to_vec(),
                block_number: self.block_number as i64,
                transaction_hash: self.transaction_hash.as_slice().to_vec(),
                log_index: self.log_index as i64,
                timestamp: Utc::now(),
            };

            let task_id: i64 = diesel::insert_into(tasks::table).values(&new_task).returning(tasks::id).get_result(conn)?;

            // Prepare and insert task performers
            let new_performers: Vec<NewTaskPerformer> = self
                .performers
                .iter()
                .map(|performer_addr| NewTaskPerformer {
                    task_id,
                    performer: performer_addr.as_slice().to_vec(),
                })
                .collect();

            if !new_performers.is_empty() {
                diesel::insert_into(task_performers::table).values(&new_performers).execute(conn)?;
            }

            // Prepare and insert task attestors
            let new_attestors: Vec<NewTaskAttestor> = self
                .attester_addresses
                .iter()
                .map(|attestor_addr| NewTaskAttestor {
                    task_id,
                    attestor: attestor_addr.as_slice().to_vec(),
                })
                .collect();

            if !new_attestors.is_empty() {
                diesel::insert_into(task_attestors::table).values(&new_attestors).execute(conn)?;
            }

            let performer_points: Vec<NewOperatorPointsLedger> = self
                .performers
                .iter()
                .map(|addr| NewOperatorPointsLedger {
                    operator: addr.as_slice().to_vec(),
                    task_id,
                    role: OperatorRole::Performer.as_str().into(),
                    points: self.task_size as f64,
                    created_at: new_task.timestamp,
                })
                .collect();

            if !performer_points.is_empty() {
                diesel::insert_into(operator_points_ledger::table).values(&performer_points).on_conflict_do_nothing().execute(conn)?;
            }

            let attestor_points: Vec<NewOperatorPointsLedger> = self
                .attester_addresses
                .iter()
                .map(|addr| NewOperatorPointsLedger {
                    operator: addr.as_slice().to_vec(),
                    task_id,
                    role: OperatorRole::Attestor.as_str().into(),
                    points: 0.1 * self.task_size as f64,
                    created_at: new_task.timestamp,
                })
                .collect();

            if !attestor_points.is_empty() {
                diesel::insert_into(operator_points_ledger::table).values(&attestor_points).on_conflict_do_nothing().execute(conn)?;
            }

            // Persist the last processed block number
            diesel::insert_into(monitor_task_state::table)
                .values(&NewMonitorTaskState {
                    id: 1, // We only need a single row for the state
                    last_processed_block: self.block_number as i64,
                })
                .on_conflict(monitor_task_state::id)
                .do_update()
                .set(monitor_task_state::last_processed_block.eq(self.block_number as i64))
                .execute(conn)?;
            Ok(())
        })
    }
}

// Define a type alias for the provider to improve readability and type correctness.
type WsProvider = FillProvider<JoinFill<Identity, JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>>, RootProvider>;

// The core struct now holds the provider and is ready to use.
#[derive(Debug, Clone)]
pub struct ContractEventMonitor {
    database_pool: PgPool,
    contract_address: Address,
    provider: Arc<WsProvider>,
    max_retries: u32,
    retry_delay: Duration,
    // Using an Arc<Mutex<HashMap>> to allow the cache to be shared and mutated across tasks.
    operator_address_cache: Arc<Mutex<HashMap<U256, Address>>>,
}

impl ContractEventMonitor {
    pub async fn new(pool: PgPool) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let contract_address = env::var("ATTESTATION_CENTER_ADDRESS")
            .expect("ATTESTATION_CENTER_ADDRESS not set")
            .parse::<Address>()
            .expect("Invalid ATTESTATION_CENTER_ADDRESS");

        let provider_url = env::var("PROVIDER_RPC_URL").expect("PROVIDER_RPC_URL not set");

        // Initialize cryptographic provider
        rustls::crypto::aws_lc_rs::default_provider().install_default().expect("Failed to install cryptographic provider");

        // Create WebSocket connection with timeout
        let ws = WsConnect::new(&provider_url);
        let provider = Arc::new(
            timeout(Duration::from_secs(30), ProviderBuilder::new().on_ws(ws))
                .await
                .map_err(|e| format!("Connection timeout: {}", e))?
                .map_err(|e| format!("Failed to create provider: {}", e))?,
        );

        info!("Connected to provider: {}", provider_url);

        Ok(Self {
            database_pool: pool,
            contract_address,
            provider,
            max_retries: 5,
            retry_delay: Duration::from_secs(5),
            operator_address_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    #[instrument(skip(self))]
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting contract event monitor for address: {}", self.contract_address);

        let mut retry_count = 0;

        loop {
            match self.monitor_events().await {
                Ok(_) => {
                    warn!("Event monitoring stream ended unexpectedly, reconnecting...");
                    retry_count = 0;
                }
                Err(e) => {
                    retry_count += 1;
                    error!("Event monitoring failed (attempt {}/{}): {:?}", retry_count, self.max_retries, e);

                    if retry_count >= self.max_retries {
                        error!("Max retries exceeded, exiting");
                        return Err(e);
                    }
                }
            }

            info!("Retrying in {:?}...", self.retry_delay);
            sleep(self.retry_delay).await;
        }
    }

    // Helper to retrieve the last processed block from the database
    async fn get_last_processed_block(&self) -> Result<Option<u64>, DieselError> {
        let mut conn = match self.database_pool.get() {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to acquire database connection from pool: {}", e);
                return Err(DieselError::NotFound); // Skip processing this message and try next
            }
        };
        let block_number: Option<i64> = monitor_task_state::table.select(monitor_task_state::last_processed_block).find(1).get_result(&mut *conn).optional()?;

        Ok(block_number.map(|b| b as u64))
    }

    #[instrument(skip(self))]
    async fn monitor_events(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let contract = AttestationCenter::new(self.contract_address, self.provider.clone());

        // Get the starting block - use latest if no previous events
        let from_block = self.get_last_processed_block().await.map_err(|e| format!("Failed to get last processed block: {}", e))?;
        let block_tag = match from_block {
            Some(block) => BlockNumberOrTag::Number(block + 1), // Start from next block after last processed
            None => BlockNumberOrTag::Latest,                   // If no blocks have been processed, start from the latest
        };

        info!("Starting from block: {:?}", block_tag);

        let task_filter = contract.TaskSubmitted_filter().from_block(block_tag).filter;

        let sub = self.provider.subscribe_logs(&task_filter).await.map_err(|e| format!("Failed to subscribe to logs: {}", e))?;

        let mut stream = sub.into_stream();
        info!("Successfully subscribed to TaskSubmitted events");

        while let Some(log) = stream.next().await {
            if let Err(e) = self.process_log(log).await {
                error!("Failed to process log: {:?}", e);
            }
        }

        Ok(())
    }

    // New helper method to fetch and cache operator IDs and addresses
    #[instrument(skip(self))]
    async fn get_operator_address_map(&self, force_refresh: bool) -> Result<HashMap<U256, Address>, Box<dyn std::error::Error + Send + Sync>> {
        // Acquire a lock on the cache to check if it's already populated
        let mut cache = self.operator_address_cache.lock().await;

        if !force_refresh && !cache.is_empty() {
            debug!("Using cached operator addresses");
            return Ok(cache.clone());
        }

        info!("Fetching active operator details from contract...");

        let contract = AttestationCenter::new(self.contract_address, self.provider.clone());
        let operators_call = contract.getActiveOperatorsDetails();
        let operators = operators_call.call().await.map_err(|e| format!("Failed to fetch operator details: {}", e))?;

        // Merge new entries into cache (never remove old entries, as inactive
        // operators still need to be resolved for historical attestations)
        for details in operators._operators {
            cache.insert(details.operatorId, details.operator);
        }

        info!("Successfully fetched and cached {} active operators.", cache.len());
        Ok(cache.clone())
    }

    #[instrument(skip(self))]
    async fn process_log(&self, log: alloy::rpc::types::Log) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Parse the log into our event structure
        let mut event = TaskSubmittedEvent::from_log(&log).map_err(|e| format!("Failed to parse TaskSubmitted event: {}", e))?;
        info!("decoded event: {:?}", event);

        // Skip internal tasks
        if event.task_definition_id == 10001 || event.task_definition_id == 10002 {
            info!("Skipping internal task with definition ID: {}", event.task_definition_id);
            return Ok(());
        }
        // 1. Decode the JSON data from the `data` field and populate `task_size` and `performers`
        match AttestationCenter::TaskData::abi_decode(&event.data, true) {
            Ok(task_data) => {
                event.task_size = task_data.task_size.to::<usize>();
                event.performers = task_data.performers;
                info!("Decoded task size: {} and performers from JSON.", event.task_size);
            }
            Err(e) => {
                error!("Failed to decode `data` field from JSON: {:?}. Raw data: {}", e, event.data);
            }
        }

        // 2. Get the operator address map (using the new helper function with caching)
        let mut operator_map = self.get_operator_address_map(false).await?;

        // 3. Convert `attesters_ids` to `attester_addresses`
        let mut attester_addresses = Vec::new();
        for attester_id in &event.attesters_ids {
            if let Some(address) = operator_map.get(attester_id) {
                attester_addresses.push(*address);
            } else {
                warn!("Could not find address for attester ID: {:?} in active operators, refreshing cache", attester_id);
                // Force a cache refresh and try again
                operator_map = self.get_operator_address_map(true).await?;
                if let Some(address) = operator_map.get(attester_id) {
                    attester_addresses.push(*address);
                    info!("Successfully found attester ID {:?} after cache refresh.", attester_id);
                } else {
                    // Fallback: look up via getOperatorPaymentDetail (works for inactive operators)
                    warn!("Attester ID {:?} not in active set, trying getOperatorPaymentDetail", attester_id);
                    let contract = AttestationCenter::new(self.contract_address, self.provider.clone());
                    match contract.getOperatorPaymentDetail(*attester_id).call().await {
                        Ok(detail) if detail._0.operator != Address::ZERO => {
                            let addr = detail._0.operator;
                            info!("Resolved inactive attester ID {:?} to address {}", attester_id, addr);
                            attester_addresses.push(addr);
                            // Cache it so we don't need to look it up again
                            let mut cache = self.operator_address_cache.lock().await;
                            cache.insert(*attester_id, addr);
                        }
                        Ok(_) => {
                            error!("Attester ID {:?} resolved to zero address, skipping", attester_id);
                        }
                        Err(e) => {
                            error!("Failed to look up attester ID {:?} via getOperatorPaymentDetail: {:?}", attester_id, e);
                        }
                    }
                }
            }
        }
        event.attester_addresses = attester_addresses;

        info!(
            "Parsed TaskSubmitted event: operator={}, task_number={}, task_definition_id={}, block={}, performers={:?}, attester_addresses={:?}",
            event.operator, event.task_number, event.task_definition_id, event.block_number, event.performers, event.attester_addresses
        );

        if let Err(e) = event.ingest(&self.database_pool) {
            error!("Failed to ingest event into database: {:?}", e);
        } else {
            info!("Successfully processed TaskSubmitted event: operator={}, task_number={}", event.operator, event.task_number);
        }
        Ok(())
    }
}
