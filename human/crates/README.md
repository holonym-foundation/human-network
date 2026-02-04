## Human Threshold Network

The Human Threshold Network is a decentralized network designed to generate and manage threshold cryptographic keys.

### Setup Instructions

#### 1. Build the Project

To begin, build the project by running:

```bash
./human_network.sh build
```

This command will compile the `human_node` binary. If the build process fails, the script will display an error message and stop execution.

#### 2. Generate Key Pairs

To create key pairs for the nodes in the network, run:

```bash
./human_network.sh generate 10
```

This command generates 10 key pairs for nodes. You can adjust the number by replacing `10` with your desired count.

#### 3. Run the Network

Start the network with the following commands based on the type of node you want to initiate:

- **Bootstrap Node:**

  ```bash
  ./human_network.sh run -bootstrap
  ```

  This command launches the bootstrap node, which is crucial for initializing the network.

- **Relay Node:**

  ```bash
  ./human_network.sh run -relay
  ```

  This starts the relay node, which facilitates communication between nodes.

- **Prover Nodes:**

  ```bash
  ./human_network.sh run -prover <number_of_nodes>
  ```

  This command initiates the specified number of prover nodes. Replace `<number_of_nodes>` with the actual count of prover nodes you want to start.

- **Specific Prover Node:**

  ```bash
  ./human_network.sh run -prover -idx <index>
  ```

  This command starts a specific prover node based on the provided index. Replace `<index>` with the prover node's index.

### Sample Program

To test an example of an OPRFSecp256k1 (Oblivious Pseudorandom Function), use:

```bash
RUST_LOG=INFO ./target/debug/cli --input "usr:123" --private-key 5b500f5d7493316ea4d50f232bee2f170cfb85353d766cdce27ca50b27ef9b99 --method OPRFSecp256k1 --rpc-url "http://localhost:9091"
```

This command demonstrates OPRF usage with the specified input, private key, and RPC URL.

#### Triggering Resharing

If the `local_test_net` feature is enabled, you can trigger resharing by sending a SIGHUP signal to the relay node.

- **Without Docker Compose:**

  If running without Docker Compose, execute the following:

  ```bash
  kill -SIGHUP $(ps aux | grep '6379/1' | grep -v grep | awk '{print $2}')
  ```

- **With Docker Compose:**

  If using Docker Compose, enter the relay node container first:

  ```bash
  docker compose --file ./avs/docker-compose.new.yml exec relay sh
  ```

  Then run:

  ```bash
  apt-get update && apt-get install procps -y
  kill -HUP $(ps aux | grep '6379/1' | grep -v grep | awk '{print $2}')
  ```

### Additional Commands

- **Fetch Key Share:**

  To fetch a key share, run:

  ```bash
  target/release/cli --method fetch_keyshare --api-token 75853e3ceb8947478bce1dfc6670e1fc --rpc-url "http://127.0.0.1:8082"
  ```

- **Backup Data:**
  To back up network data, use:

  ```bash
  target/release/cli --method backup  --rpc-url "http://127.0.0.1:8082"
  ```
- **Quic Ping :**
  To quic ping node from relay node , use:

  ```bash
  target/release/cli --method quic_ping  --peer-id 16Uiu2HAmEJYJA8Q6Jg388Kf4mpp84YF8Pty3TYjg2NTpjdE1sYRz --addr "/ip4/127.0.0.1/udp/8082/quic-v1" --rpc-url "http://127.0.0.1:8081"
  ```

- **Sync Peer data :**
  To sync peer data from relay node , use:

  ```bash
  target/release/cli --method sync_peer_data   - --rpc-url "http://127.0.0.1:8082"
  ```