# How to Run
run `docker compose up --build`. for fast builds, ask Nanak Nihal for invite to docker cloud build.

# How it works
## Custom nodes
The Prover, Verifier, Relayer, and Test Telemetry are types of nodes built from the rust binaries in this project. The Prover nodes do multiplications and their corresponding DLEQ proofs and the Relayer relays requests from users to a set of Provers. If the Relayer reaches a certain number of requests, currently set to 1000, it sends a task proof to the verifier nodes (via the Othentic RPC and AVS WebAPI nodes, described later on). This task proof just proves there have been a certain number of signed requests sent to the network. It doesn't prove the network has responded -- there are future incentive mechanisms to respond which provide cryptoeconomic trust that the network will respond.

The Test Telemetry node is used for tests, so logs across all nodes are shared in a centralized place.

## Othentic nodes
The AVSWebAPI, Attestor and Aggregator are nodes provided by Othentic. Attestors attest to validity of task proofs with BLS signatures and Aggregators combine these signatures to one signature, posting the one signature on-chain. Attestor images do not directly validate the signatures. Rather they query AVSWebAPI images, which in our case are just images of Verifier nodes. AVSWebAPI is more an interface, specifying a certain endpoint it has to respond to in a certain way, and the Verifier node implements this interface.

Essentially Attestor, AVSWebAPI, and Verifiers are all the same -- each Attestor should run an AVSWebAPI container which is a Verifier.

## Network
The relayer is mapped to the container's host's port 3031. This allows you to locally interact with the network like a user would. The rest of the network is largely private, within a docker network called p2p (similar to Othentic oracle example docker-compose.yaml)

## Setup
Since this is a permissioned testnet, no resharing is done. No epochs exist either! DKG is done beforehand, and private keyshares are put in the environment variables for the containers. The public keys are hardcoded into the relayer. If you change the private keys, run the `docker-compose up` and check the telemetry node's output for what should be the new value to replace the relayer's ACTIVE_PROVERS env var! 