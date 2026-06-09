#!/bin/bash

# Assumes local Anvil node is running and the default accounts are funded

# Get current working directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# We make an assumption about where the smart contracts are
cd $DIR/../../mishti-smart-contracts

# Use private key of first default anvil account
forge create --rpc-url http://localhost:8540 --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 ./src/PeerRegistry.sol:PeerRegistry --broadcast

# Deploy AuthorityWithDailyRateLimitV2 to a temp address, then plant it at the mainnet
# address (0x3a0d4A524Aa53A29959Aaef1Cff899F35Cc7F766) using Anvil cheatcodes so the
# SDK's hardcoded CONDITIONS_CONTRACT works without modification.
ANVIL_DEPLOYER=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
ANVIL_OWNER=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
CONDITIONS_CONTRACT=0x3a0d4A524Aa53A29959Aaef1Cff899F35Cc7F766

DEPLOY_OUTPUT=$(forge create \
    --rpc-url http://localhost:8540 \
    --private-key $ANVIL_DEPLOYER \
    ./src/decryption/AuthorityWithDailyRateLimitV2.sol:AuthorityWithDailyRateLimitV2 \
    --broadcast 2>&1)
echo "$DEPLOY_OUTPUT"
TEMP_ADDR=$(echo "$DEPLOY_OUTPUT" | grep "Deployed to:" | awk '{print $3}')
echo "Temp AuthorityWithDailyRateLimitV2 address: $TEMP_ADDR"

# Copy runtime bytecode to the mainnet address
RUNTIME_CODE=$(cast code $TEMP_ADDR --rpc-url http://localhost:8540)
cast rpc anvil_setCode "$CONDITIONS_CONTRACT" "$RUNTIME_CODE" --rpc-url http://localhost:8540

# Set slot 0 (_owner) to the first Anvil account so it can call requestDecryption.
# Value is the 20-byte address left-padded to 32 bytes.
OWNER_SLOT=0x0000000000000000000000000000000000000000000000000000000000000000
OWNER_VALUE=0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266
cast rpc anvil_setStorageAt "$CONDITIONS_CONTRACT" "$OWNER_SLOT" "$OWNER_VALUE" --rpc-url http://localhost:8540

echo "AuthorityWithDailyRateLimitV2 planted at $CONDITIONS_CONTRACT (owner: $ANVIL_OWNER)"