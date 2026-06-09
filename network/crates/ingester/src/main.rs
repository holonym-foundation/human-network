use alloy::sol;
use axum::routing::get;
use axum::Router;
use chrono::DateTime;
use diesel::{
    prelude::*,
    r2d2::{self, ConnectionManager},
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use dotenv::dotenv;
use futures::StreamExt;
use lru_time_cache::LruCache;
use messages::kafka::{KafkaConsumer, KafkaTopic, PeerReachabilityQuicStatus, PeerReachabilityTCPStatus, QuorumResharingInfo, RequestInfo, UserCreditsByMethod};
use rdkafka::{message::Message, Timestamp};
use std::{env, net::SocketAddr, time::Duration};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{error, info, level_filters::LevelFilter};

use crate::message_ingester::KafkaMessageIngestor;
use crate::task_monitor::ContractEventMonitor;

pub mod api_models;
pub mod daily_points_updater;
pub mod handlers;
pub mod message_ingester;
pub mod models;
pub mod schema;
pub mod task_monitor;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

pub type PgPool = r2d2::Pool<ConnectionManager<PgConnection>>;

/// Establishes a connection pool to the PostgreSQL database.
/// Panics if the DATABASE_URL environment variable is not set or connection pool creation fails.
pub fn establish_connection_pool() -> PgPool {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    r2d2::Pool::builder()
        .max_size(10) // Maximum number of connections in the pool
        .min_idle(Some(2)) // Minimum number of idle connections
        .connection_timeout(Duration::from_secs(5)) // Timeout for acquiring a connection
        .build(manager)
        .expect("Failed to create PostgreSQL connection pool")
}

/// Ingests Kafka messages into the PostgreSQL database.
/// This function runs indefinitely, consuming messages from the subscribed topics.
async fn ingest_kafka_messages(kafka_consumer: KafkaConsumer, pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    kafka_consumer
        .subscribe(&[
            KafkaTopic::PeerReachabilityQUIC,
            KafkaTopic::PeerReachabilityTCP,
            KafkaTopic::QuorumResharingStatus,
            KafkaTopic::Requests,
            KafkaTopic::Credits,
        ])
        .await
        .expect("Failed to subscribe to topics");

    info!("Subscribed to Kafka topics. Starting message consumption loop.");

    let mut message_stream = kafka_consumer.consumer.stream();

    while let Some(msg) = message_stream.next().await {
        match msg {
            Ok(msg) => {
                let msg = msg.detach(); // Detach message for processing
                let message_topic = msg.topic();
                let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string()).unwrap_or_else(|| "no key".to_string());
                let payload_bytes = msg.payload();
                let kafka_timestamp = match msg.timestamp() {
                    Timestamp::CreateTime(millis) | Timestamp::LogAppendTime(millis) => {
                        let seconds = millis / 1000;
                        let nanoseconds = ((millis % 1000) * 1_000_000) as u32;
                        DateTime::from_timestamp(seconds, nanoseconds).map(|dt| dt.to_utc())
                    }
                    Timestamp::NotAvailable => None,
                };

                // Acquire a connection from the pool for each message
                let mut conn = match pool.get() {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to acquire database connection from pool: {}", e);
                        continue; // Skip processing this message and try next
                    }
                };

                // Process message based on topic
                match KafkaTopic::from_str(message_topic) {
                    Some(KafkaTopic::PeerReachabilityQUIC) => {
                        if let Some(bytes) = payload_bytes {
                            match serde_json::from_slice::<PeerReachabilityQuicStatus>(bytes) {
                                Ok(payload) => {
                                    if let Err(e) = payload.ingest(&mut conn, kafka_timestamp, &key).await {
                                        error!("Failed to ingest QUIC reachability status: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to deserialize PeerReachabilityQuicStatus: {}", e)
                                }
                            }
                        } else {
                            error!("Received QUIC reachability message with no payload.");
                        }
                    }
                    Some(KafkaTopic::PeerReachabilityTCP) => {
                        if let Some(bytes) = payload_bytes {
                            match serde_json::from_slice::<PeerReachabilityTCPStatus>(bytes) {
                                Ok(payload) => {
                                    if let Err(e) = payload.ingest(&mut conn, kafka_timestamp, &key).await {
                                        error!("Failed to ingest TCP reachability status: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to deserialize PeerReachabilityTCPStatus: {}", e)
                                }
                            }
                        } else {
                            error!("Received TCP reachability message with no payload.");
                        }
                    }
                    Some(KafkaTopic::QuorumResharingStatus) => {
                        if let Some(bytes) = payload_bytes {
                            match serde_json::from_slice::<QuorumResharingInfo>(bytes) {
                                Ok(payload) => {
                                    if let Err(e) = payload.ingest(&mut conn, kafka_timestamp, &key).await {
                                        error!("Failed to ingest QuorumResharingInfo: {}", e);
                                    }
                                }
                                Err(e) => error!("Failed to deserialize QuorumResharingInfo: {}", e),
                            }
                        } else {
                            error!("Received QuorumResharingStatus message with no payload.");
                        }
                    }
                    Some(KafkaTopic::Requests) => {
                        if let Some(bytes) = payload_bytes {
                            match serde_json::from_slice::<RequestInfo>(bytes) {
                                Ok(payload) => {
                                    if let Err(e) = payload.ingest(&mut conn, kafka_timestamp, &key).await {
                                        error!("Failed to ingest RequestInfo: {}", e);
                                    }
                                }
                                Err(e) => error!("Failed to deserialize RequestInfo: {}", e),
                            }
                        } else {
                            error!("Received Requests message with no payload.");
                        }
                    }
                    Some(KafkaTopic::Credits) => {
                        if let Some(bytes) = payload_bytes {
                            match serde_json::from_slice::<UserCreditsByMethod>(bytes) {
                                Ok(payload) => {
                                    // Assuming Kafka key for Credits topic is user_address
                                    if let Err(e) = payload.ingest(&mut conn, kafka_timestamp, &key).await {
                                        error!("Failed to ingest UserCreditsByMethod: {}", e);
                                    }
                                }
                                Err(e) => error!("Failed to deserialize UserCreditsByMethod: {}", e),
                            }
                        } else {
                            error!("Received Credits message with no payload.");
                        }
                    }
                    None => {
                        error!("Received message with unknown topic: {}", message_topic);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving message from Kafka stream: {}", e);
            }
        }
    }
    Ok(()) // Return Ok(()) if the stream ends gracefully (though it's an infinite loop)
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
    }
);

async fn task_contract_txs(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let monitor = ContractEventMonitor::new(pool.clone()).await.map_err(|e| <Box<dyn std::error::Error + Send + Sync>>::from(e))?;

    monitor.run().await.map_err(|e| <Box<dyn std::error::Error + Send + Sync>>::from(e))?;
    Ok(())
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    tvl_cache: LruCache<(), f64>,
}
impl Default for AppState {
    fn default() -> Self {
        let pool = establish_connection_pool();
        AppState {
            pool,
            tvl_cache: LruCache::<(), f64>::with_expiry_duration(Duration::from_secs(60 * 60 * 24)),
        }
    }
}
#[tokio::main]
async fn main() {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .expect("Failed to parse RUST_LOG environment variable"),
        )
        .init();

    info!("Starting ingester...");

    // Establish database connection pool
    let pool = establish_connection_pool();

    // Run database migrations
    info!("Running database migrations...");
    let mut conn = pool.get().expect("Failed to get connection for migrations");
    match conn.run_pending_migrations(MIGRATIONS) {
        Ok(_) => info!("Database migrations applied successfully."),
        Err(e) => {
            error!("Failed to run database migrations: {}", e);
            std::process::exit(1);
        }
    }

    // Create Kafka consumer
    let kafka_brokers = env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS not set");
    let kafka_consumer = KafkaConsumer::new(&kafka_brokers, "mainnetalpha").await.expect("Failed to create Kafka consumer");

    // Build the Axum application
    let api_router = Router::new()
        .route("/healthcheck", get(hello_world))
        .route("/current_quorum", get(handlers::get_current_quorum))
        .route("/quorum_elections", get(handlers::get_quorum_elections))
        .route("/requests", get(handlers::get_requests))
        .route("/total_keys_generated", get(handlers::get_total_keys_generated))
        .route("/keys_generated_chart", get(handlers::get_keys_generated_chart))
        .route("/user_credits", get(handlers::get_user_credits))
        .route("/operator_computed_requests", get(handlers::get_multiplier_computed_requests))
        .route("/operator_elected_quorums", get(handlers::get_multiplier_elected_quorums))
        .route("/peers_reachability_status/tcp", get(handlers::get_peers_reachability_status_tcp))
        .route("/peers_reachability_status/quic", get(handlers::get_peers_reachability_status_quic))
        .route("/node_versions", get(handlers::get_node_versions))
        .route("/tasks", get(handlers::get_tasks))
        .route("/operator_points", get(handlers::get_operator_points))
        .route("/operator_points_v2", get(handlers::get_operator_points_v2))
        .route("/operator_points_v3", get(handlers::get_operator_points_v3))
        .route("/total_network_tvl", get(handlers::get_network_tvl));

    let symbiotic_router = Router::new()
        .route("/health", get(handlers::symbiotic_health))
        .route("/synced-to", get(handlers::symbiotic_synced_to))
        .route("/stats", get(handlers::symbiotic_stats))
        .route("/points", get(handlers::symbiotic_points));

    let app_state = AppState {
        pool: pool.clone(),
        tvl_cache: LruCache::<(), f64>::with_expiry_duration(Duration::from_secs(60 * 60 * 24)), // Cache TVL for 24 hours
    };
    let app = Router::new()
        .nest("/api/v1", api_router)
        .nest("/api/v1/symbiotic", symbiotic_router)
        .with_state(app_state)
        // Add CORS layer for production readiness
        .layer(
            CorsLayer::new()
                .allow_origin(Any) // Be more restrictive in production, e.g., .allow_origin("http://your-frontend.com".parse().unwrap())
                .allow_methods(Any)
                .allow_headers(Any),
        )
        // Add tracing layer for request logging
        .layer(TraceLayer::new_for_http());

    let port = env::var("PORT").unwrap_or_else(|_| "3030".to_string()).parse::<u16>().expect("PORT must be a valid u16");

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
    // Spawn both tasks concurrently
    let api_server_task = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>());

    let kafka_ingestion_task = tokio::spawn(ingest_kafka_messages(kafka_consumer, pool.clone()));

    let task_ingestion_task = tokio::spawn(task_contract_txs(pool.clone()));

    let daily_points_task = tokio::spawn(daily_points_updater::run_daily_points_updater(pool.clone()));

    // Await both tasks. If one fails, the other might continue, but we'll log the error.
    tokio::select! {
        kafka_ingestion_res = kafka_ingestion_task => {
            if let Err(e) = kafka_ingestion_res {
                error!("Kafka ingestion task failed: {:?}", e);
            }
            info!("Kafka ingester stopped.");
        }
        task_ingestion_res = task_ingestion_task => {
            if let Err(e) = task_ingestion_res {
                error!("task ingestion task failed: {:?}", e);
            }
            info!("task ingester stopped.");
        }
        daily_points_res = daily_points_task => {
            if let Err(e) = daily_points_res {
                error!("Daily updater task failed: {:?}", e);
            }
            info!("daily points updater task stopped.");
        }
        api_res = api_server_task => {
            if let Err(e) = api_res {
                error!("API server task failed: {:?}", e);
            }
            info!("API server stopped.");
        }
    }
}

async fn hello_world() -> &'static str { "Hello world" }
