use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::io::Write;
use tracing::{error, info, warn, debug};

/// Valid DataDog log levels
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Alert,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info", 
            LogLevel::Warning => "warning",
            LogLevel::Error => "error",
            LogLevel::Alert => "alert",
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct DatadogLogEvent {
    pub message: String,
    pub ddtags: String,
    pub service: String,
    pub level: String,
    #[serde(flatten)]
    pub attributes: HashMap<String, serde_json::Value>,
}

#[derive(Clone)]
pub struct DatadogClient {
    client: reqwest::Client,
    api_key: String,
    service: String,
    environment: String,
}

impl DatadogClient {
    pub fn new(api_key: String, service: String, environment: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
            service,
            environment,
        }
    }

    fn get_logs_endpoint(&self) -> String {
        let site = std::env::var("DD_SITE").unwrap_or_else(|_| "datadoghq.com".to_string());
        format!("https://http-intake.logs.{}/v1/input", site)
    }

    pub async fn send_log(&self, message: String, level: String, attributes: HashMap<String, serde_json::Value>) -> Result<(), anyhow::Error> {
        let ddtags = format!("env:{},service:{}", self.environment, self.service);
        
        let log_event = DatadogLogEvent {
            message,
            ddtags,
            service: self.service.clone(),
            level,
            attributes,
        };

        let logs = vec![log_event];
        let json_payload = serde_json::to_vec(&logs)?;

        // Compress the payload
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&json_payload)?;
        let compressed_payload = encoder.finish()?;

        let response = self.client
            .post(&self.get_logs_endpoint())
            .header("DD-API-KEY", &self.api_key)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(compressed_payload)
            .send()
            .await?;

        if !response.status().is_success() {
            warn!(
                status = response.status().as_u16(),
                "Failed to send log to Datadog"
            );
        }

        Ok(())
    }

    pub async fn send_logs(&self, log_events: Vec<DatadogLogEvent>) -> Result<(), anyhow::Error> {
        if log_events.is_empty() {
            return Ok(());
        }

        let json_payload = serde_json::to_vec(&log_events)?;

        // Compress the payload
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&json_payload)?;
        let compressed_payload = encoder.finish()?;

        let response = self.client
            .post(&self.get_logs_endpoint())
            .header("DD-API-KEY", &self.api_key)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(compressed_payload)
            .send()
            .await?;

        if !response.status().is_success() {
            warn!(
                status = response.status().as_u16(),
                "Failed to send logs to Datadog"
            );
        }

        Ok(())
    }
}

/// Helper function to send logs to Datadog
pub async fn send_to_datadog(datadog_client: &Option<DatadogClient>, message: String, level: String, attributes: HashMap<String, serde_json::Value>) {
    if let Some(client) = datadog_client {
        if let Err(e) = client.send_log(message, level, attributes).await {
            // Log error but don't fail the operation
            warn!(error = ?e, "Failed to send log to Datadog");
        }
    }
}

pub fn init_datadog_client() -> Option<DatadogClient> {
    if let Ok(api_key) = env::var("DD_API_KEY") {
        let service = env::var("DD_SERVICE").expect("DD_SERVICE must be set if DD_API_KEY is set");
        let environment = env::var("DD_ENV").expect("DD_ENV must be set if DD_API_KEY is set");
        info!(service = %service, environment = %environment, "Datadog logging enabled");
        Some(DatadogClient::new(api_key, service, environment))
    } else {
        info!("Datadog logging disabled - DD_API_KEY not set");
        None
    }
}

/// Macro for sending logs to Datadog with key-value pairs.
/// Supports Display (%) and Debug (?) formatters like tracing.
/// Usage: 
/// - dd_log!(datadog_client, LogLevel::Info, key1 = value1, key2 = %value2, key3 = ?value3; "message");
/// - dd_log!(datadog_client, LogLevel::Warning, key1 = value1; "message");
/// - dd_log!(datadog_client, LogLevel::Error; "message");
#[macro_export]
macro_rules! dd_log {
    // Pattern with mixed formatters: key = value, key = %value, key = ?value
    ($datadog_client:expr, $level:expr, $($key:ident = $fmt:tt $value:expr),* $(,)?; $message:expr) => {
        {
            // Type check for datadog_client and level
            let _: &Option<$crate::datadog_client::DatadogClient> = $datadog_client;
            let _: $crate::datadog_client::LogLevel = $level;

            let mut attributes = std::collections::HashMap::new();
            $(
                $crate::dd_log_insert_field!(attributes, $key, $fmt $value);
            )*
            $crate::datadog_client::send_to_datadog($datadog_client, $message.to_string(), $level.to_string(), attributes).await;
        }
    };
    
    // Pattern with just message (no fields)
    ($datadog_client:expr, $level:expr; $message:expr) => {
        {
            // Type check for datadog_client and level
            let _: &Option<$crate::datadog_client::DatadogClient> = $datadog_client;
            let _: $crate::datadog_client::LogLevel = $level;

            let attributes = std::collections::HashMap::new();
            $crate::datadog_client::send_to_datadog($datadog_client, $message.to_string(), $level.to_string(), attributes).await;
        }
    };
}

/// Helper macro to handle different field formatting
#[macro_export]
macro_rules! dd_log_insert_field {
    // Debug formatter: key = ?value
    ($map:expr, $key:ident, ? $value:expr) => {
        {
            let _: &dyn std::fmt::Debug = &$value;
            $map.insert(stringify!($key).to_string(), serde_json::Value::String(format!("{:?}", $value)));
        }
    };
    
    // Display formatter: key = %value  
    ($map:expr, $key:ident, % $value:expr) => {
        {
            let _: &dyn std::fmt::Display = &$value;
            $map.insert(stringify!($key).to_string(), serde_json::Value::String(format!("{}", $value)));
        }
    };
    
    // Catch invalid formatters and provide helpful error message
    ($map:expr, $key:ident, $invalid:tt $value:expr) => {
        compile_error!(concat!(
            "Invalid formatter '", 
            stringify!($invalid), 
            "' for field '", 
            stringify!($key), 
            "'. Use % for Display formatting or ? for Debug formatting."
        ));
    };
}
