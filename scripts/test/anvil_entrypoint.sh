#!/bin/bash

# This script:
# - Starts the Anvil server in the background
# - Deploys the smart contracts
# - Registers peer metadata with the PeerRegistry contract
# - Stops the Anvil server if the deployment fails or if
#   a termination signal is received

# Get current working directory
WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# These functions for peer registration use hardcoded data. The docker
# compose file needs to be in sync with this data for tests to work.

# TODO: Once we integrate new key storage code (i.e., the code that
# doesn't rely on test_keypairs.json), update this code.

cleanup() {
    echo "Stopping Anvil node..."
    kill "$bg_pid"
    wait "$bg_pid"
    exit 0
}

trap cleanup SIGINT SIGTERM

# Start the local blockchain node in the background
anvil --port 8540 --host 0.0.0.0 --accounts 25 &
bg_pid=$!

# Sleep for a bit to allow the node to start
sleep 0.5

# Deploy
bash $WORKDIR/deploy_test_contracts.sh || cleanup

# Register peers
if [[ "$1" == "register-peers" ]]; then
    sleep 1;
    echo "Registering peers..."
    bash $WORKDIR/register_peers.sh || cleanup
fi

# Wait for the background process to finish
wait "$bg_pid"
