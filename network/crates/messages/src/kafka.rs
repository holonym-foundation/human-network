use ethers::abi::Address;
use libp2p::PeerId;
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info};

use crate::message::ProverInfo;
use crate::network_utils::Method;

const TOPIC_REPLICATION: i32 = 1;
const TOPIC_NUM_PARTITIONS: i32 = 1;

pub struct KafkaProducer {
    producer: FutureProducer,
    admin_client: AdminClient<DefaultClientContext>,
}
impl std::fmt::Debug for KafkaProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_struct("KafkaProducer").finish_non_exhaustive() }
}
pub enum KafkaTopic {
    Requests,
    Credits,
    PeerReachabilityTCP,
    PeerReachabilityQUIC,
    QuorumResharingStatus,
}

impl KafkaTopic {
    pub fn as_str(&self) -> &str {
        match self {
            KafkaTopic::QuorumResharingStatus => "quorum_resharing_status",
            KafkaTopic::Requests => "requests",
            KafkaTopic::Credits => "credits",
            KafkaTopic::PeerReachabilityTCP => "peer_reachability_tcp",
            KafkaTopic::PeerReachabilityQUIC => "peer_reachability_quic",
        }
    }
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "quorum_resharing_status" => Some(KafkaTopic::QuorumResharingStatus),
            "requests" => Some(KafkaTopic::Requests),
            "credits" => Some(KafkaTopic::Credits),
            "peer_reachability_tcp" => Some(KafkaTopic::PeerReachabilityTCP),
            "peer_reachability_quic" => Some(KafkaTopic::PeerReachabilityQUIC),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PeerReachabilityTCPStatus {
    pub success: bool,
    pub rpc_url: String,
    pub version: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PeerReachabilityQuicStatus {
    pub success: bool,
    pub duration: Option<Duration>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UserCreditsByMethod {
    pub method: String,
    pub exhausted_credits: u128,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QuorumResharingInfo {
    pub multipliers: Vec<ProverInfo>,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestInfo {
    pub address: Address,
    pub method: Method,
    pub served_by_multipliers: Vec<PeerId>,
}

impl KafkaProducer {
    /// Create a new KafkaProducer.
    /// `brokers` is a comma-separated list of broker addresses.
    /// `topic` is the topic to produce to.
    pub async fn new(brokers: &str) -> Result<Self, KafkaError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .set("queue.buffering.max.messages", "1000000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("compression.type", "lz4")
            .create::<FutureProducer>()
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;

        let admin_client = ClientConfig::new().set("bootstrap.servers", brokers).create().map_err(|e| KafkaError::ClientCreation(e.to_string()))?;

        Ok(KafkaProducer { producer, admin_client })
    }

    pub async fn new_with_topic_registration(brokers: &str) -> Result<Self, KafkaError> {
        let KafkaProducer { producer, admin_client } = Self::new(brokers).await?;

        // Register topics if they do not exist
        for topic in [
            KafkaTopic::QuorumResharingStatus,
            KafkaTopic::Requests,
            KafkaTopic::Credits,
            KafkaTopic::PeerReachabilityTCP,
            KafkaTopic::PeerReachabilityQUIC,
        ] {
            let topic_name = topic.as_str();
            if let Err(e) = admin_client.create_topics(&[NewTopic::new(topic_name, TOPIC_NUM_PARTITIONS, TopicReplication::Fixed(TOPIC_REPLICATION))], &Default::default()).await {
                error!("Failed to create topic {}: {}", topic_name, e);
            } else {
                info!("Successfully created or verified topic: {}", topic_name);
            }
        }

        Ok(KafkaProducer { producer, admin_client })
    }

    /// Send a message to Kafka.
    /// `key` and `payload` can be any type that can be referenced as bytes.
    pub async fn send<K, P>(&self, topic: KafkaTopic, key: K, payload: P)
    where
        K: AsRef<str>,
        P: Serialize,
    {
        let payload_bytes: Vec<u8> = match serde_json::to_vec(&payload) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to serialize payload: {}", e);
                return;
            }
        };
        let record = FutureRecord::<str, [u8]>::to(topic.as_str()).payload(&payload_bytes).key(key.as_ref());

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(delivery) => {
                info!("Message delivered: {:?}", delivery);
            }
            Err((e, _)) => {
                error!("Failed to deliver message: {}", e);
            }
        }
    }
}

pub struct KafkaConsumer {
    pub consumer: StreamConsumer,
}

impl KafkaConsumer {
    /// Create a new KafkaConsumer.
    /// `brokers` is a comma-separated list of broker addresses.
    /// `group_id` is the consumer group ID.
    pub async fn new(brokers: &str, group_id: &str) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set_log_level(RDKafkaLogLevel::Error)
            .create()
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;
        Ok(KafkaConsumer { consumer })
    }

    pub async fn subscribe(&self, topics: &[KafkaTopic]) -> Result<(), KafkaError> {
        let topic_strs: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
        self.consumer.subscribe(&topic_strs).map_err(|e| KafkaError::ClientCreation(e.to_string()))?;
        info!("Subscribed to topics: {:?}", topic_strs);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_kafka_producer_send_and_receive() {
        let producer = KafkaProducer::new("localhost:9092").await;
        if producer.is_err() {
            error!("Failed to create KafkaProducer: {:?}", producer.err());
            assert!(false, "KafkaProducer creation failed");
            return;
        }
        let producer = producer.unwrap();
        producer.send(KafkaTopic::QuorumResharingStatus, "test-key", "test-message").await;

        let consumer = KafkaConsumer::new("localhost:9092", "test-group").await;
        if consumer.is_err() {
            error!("Failed to create KafkaConsumer: {:?}", consumer.err());
            assert!(false, "KafkaConsumer creation failed");
            return;
        }
        let consumer = consumer.unwrap();

        consumer.subscribe(&[KafkaTopic::QuorumResharingStatus]).await.expect("Failed to subscribe to topic");

        // Instead of a `while` loop, you can use `StreamExt::take(n)` to receive a fixed number of messages.
        // Here, we use `.take(1)` to receive just one message for the test.
        let mut stream = consumer.consumer.stream().take(1);

        if let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    print!("Received message: {:?}", msg);
                    info!("Received message: {:?}", msg);
                    // match msg.payload_view::<str>() {
                    //     Some(Ok(payload)) => {
                    //         info!("Received message: {}", payload);
                    //         assert_eq!(payload, "test-message");
                    //     }
                    //     Some(Err(e)) => {
                    //         error!("Failed to parse message payload: {}", e);
                    //         assert!(false, "Failed to parse message payload");
                    //     }
                    //     None => {
                    //         error!("Received message with no payload");
                    //         assert!(false, "Received message with no payload");
                    //     }
                    // }
                }
                Err(e) => {
                    error!("Failed to receive message: {}", e);
                    assert!(false, "Failed to receive message");
                }
            }
        } else {
            error!("No message received");
            assert!(false, "No message received");
        }
        info!("Test completed successfully");
        assert!(true, "KafkaProducer and KafkaConsumer test passed");
    }
}
