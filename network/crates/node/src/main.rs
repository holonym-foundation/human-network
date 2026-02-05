/* trunk-ignore-all(rustfmt) */
use actors::actor_manager::ActorManagerBuilder;
use actors::dkg_engine_actor::DkgEngineSupervisor;
use actors::election_actor::ElectionEngineSupervisor;
use actors::gossip_engine_actor::GossipEngineSupervisor;
use actors::{
    actor_manager::ActorManager,
    app_state_actor::{AppStateEngineActor, AppStateSupervisor},
    dkg_engine_actor::DKGEngineActor,
    election_actor::ElectionEngineActor,
    gossip_engine_actor::{GossipEngineActor, GossipEngineListener},
};
use clap::Parser;
use dotenv::dotenv;
use hyper::Method;
use jsonrpsee::server::{ServerBuilder, ServerHandle};
use libp2p::identity::Keypair;
use libp2p::{Multiaddr, PeerId};
use log::info;
use messages::actor_type::{IntoActorType, SupervisorType};
use messages::kafka::KafkaProducer;
use messages::{actor_type::ActorType, message::Message};
use network::utils::{fetch_node_details, validate_port, NodeType};
use ractor::{actor::Actor, ActorCell, ActorStatus};
use rpc::RpcServerImpl;
use rpc_trait::rpc::HumanRpcServer;
use std::env;
use std::{error::Error, sync::Arc};
use telemetry::{get_subscriber, init_subscriber};
use tokio::sync::{mpsc, Mutex};
use tower_http::cors::{Any, CorsLayer};
use tracing::error;
/// Arguments for the program
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Idx of the relay node from the keypairs.json file
    // #[arg(long)]
    // relay_idx: Option<u8>,

    #[arg(long)]
    bootstrap_peer_id: Option<String>,

    #[arg(long)]
    bootstrap_multiaddr: Option<String>,

    #[arg(long)]
    relay_peer_id: Option<String>,

    #[arg(long)]
    relay_multiaddr: Option<String>,

    /// Port
    #[arg(short, long)]
    port: Option<String>,

    /// redis-url
    #[arg(short, long)]
    redis: Option<String>,
}
pub struct ActorConfig {
    pub actor_manager: Arc<Mutex<ActorManager>>,
    pub panic_rx: tokio::sync::mpsc::Receiver<ActorCell>,
}
pub struct NetworkConfig {
    pub keypair: Keypair,
    pub port: String,
    pub multi_addr: Multiaddr,
    pub local_peer_id: PeerId,
    pub node_type: NodeType,
    pub redis_url: String,
    pub bootstrap_peer_id: PeerId,
    pub bootstrap_multi_addr: Multiaddr,
    pub relay_peer_id: PeerId,
    pub relay_multi_addr: Multiaddr,
    pub kafka: Option<Arc<KafkaProducer>>,
}
pub struct ActorEngineGroup {
    pub app_engine_actor: AppStateEngineActor,
    pub election_engine_actor: ElectionEngineActor,
    pub dkg_engine_actor: DKGEngineActor,
    pub gossip_engine_actor: GossipEngineActor,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let args = Args::parse();
    tracing::info!("Current Working Directory: {:?}", std::env::current_dir());
    let (keypair, rsa_secret_key, node_type, multi_addr) = fetch_node_details();
    let local_peer_id = keypair.public().to_peer_id();
    let subscriber = get_subscriber(local_peer_id.to_string(), env::var("LEVEL").unwrap_or_else(|_| "info".to_string()), std::io::stdout);
    init_subscriber(subscriber);
    let (panic_tx, panic_rx): (tokio::sync::mpsc::Sender<ActorCell>, tokio::sync::mpsc::Receiver<ActorCell>) = tokio::sync::mpsc::channel(500);
    if args.relay_multiaddr.is_none() && (node_type == NodeType::Prover || node_type == NodeType::Bootstrap) {
        log::error!("Provide multiaddr of relay node");
        std::process::exit(0);
    }
    if args.port.is_none() {
        log::error!("Provide port");
        std::process::exit(0);
    }
    let port = args.port.unwrap();
    validate_port(port.as_str());
    let bootstrap_peer_id = args
        .bootstrap_peer_id
        .expect("Provide bootstrap node peer id")
        .parse::<PeerId>()
        .expect("Bootstrap node peer id is invalid");
    let bootstrap_multi_addr = args
        .bootstrap_multiaddr
        .expect("Provide Bootstrap node multiaddr")
        .parse::<Multiaddr>()
        .expect("bootstrap node multiaddr is invalid");
    let relay_peer_id = args.relay_peer_id.expect("Provide relay node peer id").parse::<PeerId>().expect("Relay node peer id is invalid");
    let relay_multi_addr = args
        .relay_multiaddr
        .expect("Provide relay node multiaddr")
        .parse::<Multiaddr>()
        .expect("Relay node multiaddr is invalid");
    let (gossip_actor_sender_tx, engine_receiver_tx) = mpsc::unbounded_channel::<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>();
    let (engine_sender_tx, gossip_actor_receiver_tx) = mpsc::unbounded_channel::<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>();
    let one_time_token = uuid::Uuid::new_v4().to_string().replace("-", "");
    actors::app_state_actor::API_TOKEN.lock().await.token = one_time_token.clone();
    log::info!(
        "\n\
        🚀 Human Node Info\n\
        - Node Type: {}\n\
        - MultiAddr: {}\n\
        - Port: {}\n\
        - Ed25519 ID: {}\n\
        - Peer ID: {}\n\
        - One-Time Token: {}",
        node_type,
        multi_addr,
        port,
        hex::encode(keypair.public().encode_protobuf()),
        keypair.public().to_peer_id(),
        one_time_token
    );

    let app_state_supervisor = AppStateSupervisor::new(panic_tx.clone());
    let election_engine_supervisor = ElectionEngineSupervisor::new(panic_tx.clone());
    let dkg_engine_supervisor = DkgEngineSupervisor::new(panic_tx.clone());
    let gossip_engine_supervisor = GossipEngineSupervisor::new(panic_tx.clone());
    let app_engine_actor = AppStateEngineActor::new();
    let election_engine_actor = ElectionEngineActor::new();
    let dkg_engine_actor = DKGEngineActor::new(rsa_secret_key);
    let gossip_engine_actor: GossipEngineActor = GossipEngineActor::new();
    let (app_engine_supervisor, _) = Actor::spawn(Some(SupervisorType::AppEngineStateSuperVisor.to_string()), app_state_supervisor, ())
        .await
        .map_err(Box::new)?;
    let (election_engine_supervisor, _) = Actor::spawn(Some(SupervisorType::ElectionEngineSuperVisor.to_string()), election_engine_supervisor, ())
        .await
        .map_err(Box::new)?;
    let (dkg_engine_supervisor, _) = Actor::spawn(Some(SupervisorType::DkgEngineSuperVisor.to_string()), dkg_engine_supervisor, ()).await.map_err(Box::new)?;
    let (gossip_engine_supervisor, _) = Actor::spawn(Some(SupervisorType::GossipEngineSuperVisor.to_string()), gossip_engine_supervisor, ())
        .await
        .map_err(Box::new)?;
    info!("Local Peer ID  :{:?} Relay Peer ID :{:?}", local_peer_id, relay_peer_id);
    let rpc_server = start_rpc_server(port.clone(), local_peer_id == relay_peer_id).await?;
    tokio::spawn(rpc_server.stopped());

    let kafka = match local_peer_id == relay_peer_id {
        true => Some(Arc::new(
            KafkaProducer::new_with_topic_registration(&env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS not set"))
                .await
                .expect("Failed to create Kafka producer"),
        )),
        false => None,
    };

    let mut gossip_engine_listener = initialize_gossip_listener(
        keypair.clone(),
        multi_addr.clone(),
        engine_sender_tx,
        engine_receiver_tx,
        bootstrap_peer_id,
        bootstrap_multi_addr.clone(),
        relay_peer_id,
        node_type,
        kafka.clone(),
    )
    .await?;
    let redis_url = args.redis.unwrap();
    let actor_manager_inner: ActorManager = ActorManagerBuilder::default()
        .app_state_engine(
            app_engine_actor.clone(),
            app_engine_supervisor,
            (local_peer_id, relay_peer_id, node_type, redis_url.clone(), relay_multi_addr.clone(), kafka.clone()),
        )
        .await?
        .election_engine(
            election_engine_actor.clone(),
            election_engine_supervisor,
            (node_type, local_peer_id, relay_peer_id, relay_multi_addr.clone(), kafka.clone()),
        )
        .await?
        .dkg_engine(dkg_engine_actor.clone(), dkg_engine_supervisor, ())
        .await?
        .gossip_engine(gossip_engine_actor.clone(), gossip_engine_supervisor, (gossip_actor_sender_tx, gossip_actor_receiver_tx))
        .await?
        .build();
    let actor_manager = Arc::new(Mutex::new(actor_manager_inner));
    tokio::spawn(handle_actor_panic(
        ActorConfig { actor_manager, panic_rx },
        NetworkConfig {
            keypair,
            port,
            multi_addr,
            local_peer_id,
            node_type,
            redis_url,
            bootstrap_peer_id,
            bootstrap_multi_addr,
            relay_peer_id,
            relay_multi_addr,
            kafka,
        },
        ActorEngineGroup {
            app_engine_actor,
            election_engine_actor,
            dkg_engine_actor,
            gossip_engine_actor,
        },
    ));
    let _ = tokio::spawn(async move {
        gossip_engine_listener.start().await;
    })
    .await;
    Ok(())
}

async fn start_rpc_server(port: String, is_relay_node: bool) -> Result<ServerHandle, Box<dyn Error>> {
    let rpc = RpcServerImpl::new();

    //let host_filter = HostFilterLayer::new(["localhost", "127.0.0.1", "localhost:8081", "127.0.0.1:8081"]).unwrap();

    // Add a CORS middleware for handling HTTP requests.
    // This middleware does affect the response, including appropriate
    // headers to satisfy CORS. Because any origins are allowed, the
    // "Access-Control-Allow-Origin: *" header is appended to the response.
    let cors = CorsLayer::new()
        // Allow `POST` when accessing the resource
        .allow_methods([Method::POST])
        .allow_origin(Any)
        .allow_headers([hyper::header::CONTENT_TYPE]);
    let http_middleware = tower::ServiceBuilder::new().layer(cors);
    if is_relay_node {
        let server = ServerBuilder::default()
            .set_http_middleware(http_middleware)
            .build(format!("0.0.0.0:{}", port))
            .await
            .map_err(Box::new)?;
        let addr = server.local_addr().unwrap();
        info!("Starting Relay node with RPC at {:?}", addr);
        Ok(server.start(rpc.into_rpc()))
    } else {
        //    let server = ServerBuilder::default().set_http_middleware(middleware).build(format!("0.0.0.0:{}", port)).await.map_err(Box::new)?;
        let server = ServerBuilder::default()
            .set_http_middleware(http_middleware)
            .build(format!("0.0.0.0:{}", port))
            .await
            .map_err(Box::new)?;
        let addr = server.local_addr().unwrap();
        info!("Starting local RPC server at {:?}", addr);
        Ok(server.start(rpc.into_rpc()))
    }
}
async fn initialize_gossip_listener(
    keypair: libp2p::identity::Keypair,
    multi_addr: Multiaddr,
    engine_sender_tx: mpsc::UnboundedSender<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>,
    engine_receiver_tx: mpsc::UnboundedReceiver<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>,
    bootstrap_peer_id: PeerId,
    bootstrap_multi_addr: Multiaddr,
    relay_peer_id: PeerId,
    node_type: NodeType,
    kafka: Option<Arc<KafkaProducer>>,
) -> Result<GossipEngineListener, Box<dyn Error>> {
    let gossip_engine_listener = if node_type == NodeType::Bootstrap {
        GossipEngineListener::new(keypair, None, multi_addr, engine_sender_tx, engine_receiver_tx, node_type, relay_peer_id, kafka).await
    } else {
        GossipEngineListener::new(
            keypair,
            Some(vec![(bootstrap_peer_id, bootstrap_multi_addr)]),
            multi_addr,
            engine_sender_tx,
            engine_receiver_tx,
            node_type,
            relay_peer_id,
            kafka,
        )
        .await
    };
    gossip_engine_listener.map_err(|e| Box::new(e) as Box<dyn Error>)
}
pub async fn handle_actor_panic(actor_config: ActorConfig, network_config: NetworkConfig, actor_engines: ActorEngineGroup) {
    let ActorConfig { actor_manager, mut panic_rx } = actor_config;
    let NetworkConfig {
        keypair,
        port: _,
        multi_addr,
        local_peer_id,
        node_type,
        redis_url,
        bootstrap_peer_id,
        bootstrap_multi_addr,
        relay_peer_id,
        relay_multi_addr,
        kafka,
    } = network_config;
    let ActorEngineGroup {
        app_engine_actor,
        election_engine_actor,
        dkg_engine_actor,
        gossip_engine_actor,
    } = actor_engines;
    while let Some(actor) = panic_rx.recv().await {
        if actor.get_status() == ActorStatus::Stopped {
            if let Some(actor_name) = actor.get_name() {
                let manager_ptr = Arc::clone(&actor_manager);
                match actor_name.resolve_actor_type() {
                    ActorType::AppStateEngine => {
                        if let Err(e) = ActorManager::reboot_app_state_engine(
                            manager_ptr,
                            actor_name.clone(),
                            (local_peer_id, relay_peer_id, node_type, redis_url.clone(), relay_multi_addr.clone(), kafka.clone()),
                            app_engine_actor.clone(),
                        )
                        .await
                        {
                            error!(error = %e, "Failed to respawn app_state_engine_actor");
                        }
                    }
                    ActorType::ElectionEngine => {
                        if let Err(e) = ActorManager::reboot_election_engine(
                            manager_ptr,
                            actor_name.clone(),
                            (node_type, local_peer_id, relay_peer_id, relay_multi_addr.clone(), kafka.clone()),
                            election_engine_actor.clone(),
                        )
                        .await
                        {
                            error!(error = %e, "Failed to respawn election_engine_actor");
                        }
                    }
                    ActorType::DkgEngine => {
                        if let Err(e) = ActorManager::reboot_dkg_engine(manager_ptr, actor_name.clone(), (), dkg_engine_actor.clone()).await {
                            error!(error = %e, "Failed to respawn dkg_engine_actor");
                        }
                    }
                    ActorType::GossipEngine => {
                        let (gossip_actor_sender_tx, gossip_actor_receiver_tx) = mpsc::unbounded_channel::<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>();
                        let (engine_sender_tx, engine_receiver_tx) = mpsc::unbounded_channel::<(Message, String, Option<Vec<(PeerId, Multiaddr)>>)>();
                        if let Err(e) = ActorManager::reboot_gossip_engine(manager_ptr, actor_name.clone(), (gossip_actor_sender_tx, gossip_actor_receiver_tx), gossip_engine_actor.clone()).await {
                            error!(error = %e, "Failed to respawn gossip_engine_actor");
                        } else {
                            match initialize_gossip_listener(
                                keypair.clone(),
                                multi_addr.clone(),
                                engine_sender_tx,
                                engine_receiver_tx,
                                bootstrap_peer_id,
                                bootstrap_multi_addr.clone(),
                                relay_peer_id,
                                node_type,
                                kafka.clone(),
                            )
                            .await
                            {
                                Ok(mut gossip_engine_listener) => {
                                    tokio::spawn(async move {
                                        gossip_engine_listener.start().await;
                                    });
                                }
                                Err(e) => {
                                    error!(error = %e, "Failed to start gossip_engine_listener");
                                }
                            }
                        }
                    }
                    _ => unreachable!("Actor is not a child of a supervisor"),
                }
            } else {
                error!("Actor name is None for stopped actor");
            }
        } else {
            error!("Received actor with non-stopped status: {:?}", actor.get_status());
        }
    }
}
