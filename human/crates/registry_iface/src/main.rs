/* trunk-ignore-all(rustfmt) */
use clap::{Parser, Subcommand};
use dotenv::dotenv;
use env::environment::ENVIRONMENT;
use ethers::{
    contract::abigen,
    middleware::SignerMiddleware,
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
    types::{TransactionReceipt, H160},
};
use libp2p::Multiaddr;
use network::utils::{fetch_rsa_key, fetch_secp256k1_keypair};
use rsa::pkcs1::EncodeRsaPublicKey;
use std::str::FromStr;
use std::{error::Error, sync::Arc};
use url::Url;
abigen!(
    PeerRegistry,
    r#"[
        {
            "inputs": [],
            "name": "getPeers",
            "outputs": [{
                "components": [
                    {
                        "internalType": "address",
                        "name": "addr",
                        "type": "address"
                    },
                    {
                        "internalType": "string",
                        "name": "peerID",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "multiaddr",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "rpc_addr",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "libp2pPubkey",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "rsaPubkey",
                        "type": "string"
                    }
                ],
                "internalType": "struct Peer[]",
                "name": "",
                "type": "tuple[]"
            }],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {
                    "internalType": "string",
                    "name": "peerID",
                    "type": "string"
                },
                {
                    "internalType": "string",
                    "name": "multiaddr",
                    "type": "string"
                },
                {
                    "internalType": "string",
                    "name": "rpc_addr",
                    "type": "string"
                },
                {
                    "internalType": "string",
                    "name": "libp2pPubkey",
                    "type": "string"
                },
                {
                    "internalType": "string",
                    "name": "rsaPubkey",
                    "type": "string"
                }
            ],
            "name": "register",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {
                    "internalType": "string",
                    "name": "peerID",
                    "type": "string"
                },
                {
                    "internalType": "string",
                    "name": "multiaddr",
                    "type": "string"
                },
                {
                    "internalType": "string",
                    "name": "rpc_addr",
                    "type": "string"
                },
                {
                    "internalType": "string",
                    "name": "libp2pPubkey",
                    "type": "string"
                },
                {
                    "internalType": "string",
                    "name": "rsaPubkey",
                    "type": "string"
                }
            ],
            "name": "updatePeer",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        }
    ]"#
);
/// Arguments for the program
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Parser, Debug)]
#[command(version, about)]
struct RegisterArgs {
    #[arg(long)]
    rpc_url: String,

    #[arg(long)]
    private_key: String,

    #[arg(long)]
    multiaddr: String,

    #[arg(long)]
    rpcaddr: String,

    #[arg(long)]
    test: Option<bool>,
}
#[derive(Subcommand, Debug)]
#[command(version, about)]
enum Command {
    Register(RegisterArgs),
    Update(RegisterArgs),
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let args = Args::parse();
    // Right now, main just calls register().
    // TODO: Allow user to call getPeers(), updatePeer(), and removePeer().
    match args.command {
        Command::Register(args) => register(args).await,
        Command::Update(args) => update(args).await,
    }
    Ok(())
}
async fn register(args: RegisterArgs) { register_or_update(args, false).await }
async fn update(args: RegisterArgs) { register_or_update(args, true).await }

async fn register_or_update(args: RegisterArgs, update: bool) {
    let env=&*ENVIRONMENT;
    let using_anvil = args.test.unwrap_or(false);
    let chain_id: u64 = if using_anvil { 31337 } else { env.l1_chain };
    println!("Registration to use RPC URL: {}\n", args.rpc_url);
    let provider = Provider::<Http>::try_from(args.rpc_url).expect("Failed to instatiate ETH provider");
    let private_key = args.private_key;
    let wallet = LocalWallet::from_str(&private_key).expect("Failed to parse private key");
    println!("Wallet address: {}\n", wallet.address());
    let signer_client = SignerMiddleware::new(provider, wallet.with_chain_id(chain_id));
    let address = if using_anvil {
        "0x5FbDB2315678afecb367f032d93F642f64180aa3"
    } else {
        env.peer_registry_address.as_str()
    };
    println!("Peer Registry Address: {}\n", address);
    let address = address.parse::<H160>().unwrap();
    println!("Peer registry address: {}\n", address);
    let contract = PeerRegistry::new(address, Arc::new(signer_client.clone()));
    let multiaddr = args.multiaddr;
    let _parsed_multiaddr = multiaddr.parse::<Multiaddr>().expect("Multiaddr is invalid");
    let rpcaddr = args.rpcaddr;
    let _parsed_rpcaddr = validate_url(rpcaddr.as_str());
    let libp2p_keypair = fetch_secp256k1_keypair();
    let libp2p_pubkey = hex::encode(libp2p_keypair.public().encode_protobuf());
    let peer_id = libp2p_keypair.public().to_peer_id().to_string();
    let rsa_private_key = fetch_rsa_key();
    let rsa_pubkey = rsa_private_key.to_public_key().to_pkcs1_pem(rsa::pkcs8::LineEnding::CR).unwrap();
    // let _result = {
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //     let result = rt.block_on(async {
    //         // let peers = contract.get_peers().call().await.unwrap();
    //         // peers
    //         let function_call = contract.register(peer_id, multiaddr, rpcaddr, libp2p_pubkey, rsa_pubkey);
    //         let result = function_call.clone().send().await.expect("Failed to register");
    //         result
    //         //.send().await.expect("Failed to register")
    //     });
    //     result
    // };
    println!("Peer ID: {}\n", peer_id);
    println!("Multiaddr: {}\n", multiaddr);
    println!("RPC Address: {}\n", rpcaddr);
    println!("Libp2p Public Key: {}\n", libp2p_pubkey);
    println!("RSA Public Key: {:?}\n", rsa_pubkey);
    let function_call = if update { 
        contract.update_peer(peer_id, multiaddr, rpcaddr, libp2p_pubkey, rsa_pubkey)
    } else {
        contract.register(peer_id, multiaddr, rpcaddr, libp2p_pubkey, rsa_pubkey)
    };
    let tx_receipt: TransactionReceipt = function_call
        .send()
        .await
        .expect("Contract call failed")
        .log_msg("Transaction is pending...")
        .await
        .expect("Contract call failed (transaction failed or reverted)")
        .ok_or("Transaction receipt not found")
        .expect("Transaction receipt not found");
    println!("Transaction receipt: {:?}", tx_receipt);
}
fn validate_url(url: &str) -> bool {
    match Url::parse(url) {
        Ok(parsed_url) => parsed_url.scheme() == "http" || parsed_url.scheme() == "https",
        Err(_) => false,
    }
}
