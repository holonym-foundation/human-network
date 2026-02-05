use tracing::subscriber::set_global_default;
use tracing::Subscriber;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};
/// Creates a `tracing` subscriber with multiple layers for structured logging.
///
/// # Arguments
///
/// * `name` - The name for the `BunyanFormattingLayer`, typically used as the service name or application name.
/// * `env_filter` - A filter string for controlling which logs are emitted, typically set via the environment.
/// * `sink` - A sink that implements `MakeWriter` trait used to write logs.
///
/// # Returns
///
/// Returns a subscriber that combines:
/// - An environment filter layer (`EnvFilter`), allowing filtering based on environment variables or provided filter string.
/// - A JSON storage layer (`JsonStorageLayer`), for structured JSON log storage.
/// - A formatting layer (`BunyanFormattingLayer`), for formatting logs in the Bunyan style.
///
/// # Type Parameters
///
/// * `Sink` - The type of the sink used for writing logs. It must implement `MakeWriter`, `Send`, `Sync`, and `'static`.
pub fn get_subscriber<Sink>(name: String, env_filter: String, sink: Sink) -> impl Subscriber + Sync + Send
where
    Sink: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    // Create an environment filter from default environment variable or use the provided filter string
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));
    // Create a formatting layer with the specified name and sink
    let formatting_layer = BunyanFormattingLayer::new(name, sink);
    // Combine all layers into a single subscriber
    Registry::default()
        .with(env_filter) // Add environment filter layer
        .with(JsonStorageLayer) // Add JSON storage layer
        .with(formatting_layer) // Add Bunyan formatting layer
}
/// Initializes and sets the provided subscriber as the global default for processing span data.
///
/// # Arguments
///
/// * `subscriber` - An instance of a `tracing` subscriber to be set as the global default.
///
/// # Panics
///
/// Panics if initializing the logger or setting the global default subscriber fails.
///
/// # Usage
///
/// This function is used to set up the global tracing subscriber, which will handle all log events
/// and span data in the application.
pub fn init_subscriber(subscriber: impl Subscriber + Sync + Send) {
    // Initialize the log tracer to capture log events
    LogTracer::init().expect("Failed to set logger");
    // Set the provided subscriber as the global default subscriber
    set_global_default(subscriber).expect("Failed to set subscriber");
}
