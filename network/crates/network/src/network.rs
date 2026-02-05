// network.rs
use crate::behaviour::MyBehaviour;
use libp2p::gossipsub::IdentTopic;
use libp2p::identity::Keypair;
use libp2p::multiaddr::Protocol;
use libp2p::{noise, tcp, yamux, Multiaddr, PeerId, Swarm, SwarmBuilder};
use messages::NETWORK_TOPIC;
use std::error::Error;
use std::time::Duration;
use tracing::info;
/// The `init_swarm` function in Rust initializes a libp2p swarm with specified configurations.
///
/// Arguments:
///
/// * `keypair`: The `keypair` parameter is an optional `Keypair` that can be provided to the
/// `init_swarm` function. If a `Keypair` is provided, it will be used for identity within the libp2p
/// swarm. If no `Keypair` is provided
/// * `bootstrap_addresses`: The `bootstrap_addresses` parameter is an optional vector of tuples
/// containing the PeerId and Multiaddr of bootstrap nodes. These nodes are used to initially connect to
/// the network and discover other peers. If provided, the swarm will attempt to connect to these
/// bootstrap nodes during initialization.
/// * `port`: The `port` parameter in the `init_swarm` function is a String that represents the port
/// number on which the libp2p swarm will listen for incoming connections. It specifies the network port
/// that the swarm will use for communication with other peers on the network.
///
/// Returns:
///
/// The function `init_swarm` returns a `Result` containing a `Swarm<MyBehaviour>` or a `Box<dyn
/// Error>`.

#[tracing::instrument(skip(keypair))]
pub async fn init_swarm(keypair: Option<Keypair>, bootstrap_addresses: Option<Vec<(PeerId, Multiaddr)>>, multi_addr: Multiaddr) -> Result<Swarm<MyBehaviour>, Box<dyn Error>> {
    // If keypair is provided, use it for identity
    let builder = if let Some(keypair) = keypair.clone() {
        SwarmBuilder::with_existing_identity(keypair)
    } else {
        SwarmBuilder::with_new_identity()
    };
    let config = libp2p::quic::Config::new(&keypair.unwrap().clone());
    // Create a libp2p swarm with configuration
    let mut swarm = builder
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_quic_config(|_| config)
        //.with_quic()
    .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|keypair, relay_behaviour| {
            if bootstrap_addresses.is_none() {
                info!("Bootstrap Peer ID :{}", keypair.public().to_peer_id());
            }
            MyBehaviour::new(keypair.clone(), relay_behaviour).unwrap()
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
    let port = multi_addr.iter().find_map(|p| if let Protocol::Udp(p) = p { Some(p) } else { None }).expect("UDP port not found in multiaddr");
    let listen_address = format!("/ip4/0.0.0.0/udp/{}/quic-v1", port);
    info!("Listen Address :{:?}", listen_address);
    // Add bootstrap nodes if provided
    if let Some(ref bootstrap_addresses) = bootstrap_addresses {
        for (peer_id, multi_addr) in bootstrap_addresses {
            swarm.behaviour_mut().kademlia.add_address(peer_id, multi_addr.clone());
            swarm.dial(multi_addr.clone())?;
        }
        swarm.behaviour_mut().kademlia.bootstrap()?;
    }

    // Subscribe to the topic (can be done after swarm creation)
    swarm.behaviour_mut().gossipsub.subscribe(&IdentTopic::new(NETWORK_TOPIC))?;
    swarm.listen_on(listen_address.parse()?)?;
    info!("External Address: {:?}", multi_addr);
    swarm.add_external_address(multi_addr);
    Ok(swarm)
}
#[tracing::instrument(skip(swarm))]
pub async fn subscribe(swarm: &mut Swarm<MyBehaviour>, topic: IdentTopic) -> Result<(), Box<dyn Error>> {
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
    Ok(())
}
#[tracing::instrument(skip(swarm))]
pub async fn unsubscribe(swarm: &mut Swarm<MyBehaviour>, topic: IdentTopic) -> Result<(), Box<dyn Error>> {
    swarm.behaviour_mut().gossipsub.unsubscribe(&topic)?;
    Ok(())
}
