#!/bin/bash

# Assumes local Anvil node is running and the default accounts are funded

# Get current working directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# We make an assumption about where the smart contracts are
cd $DIR/../../human-smart-contracts

# Use private key of first default anvil account
forge create --rpc-url http://localhost:8540 --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 ./src/PeerRegistry.sol:PeerRegistry