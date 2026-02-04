####################################################################
# Dockerfile used for testing. It runs a local EVM node and deploys
# the Human Network contracts so that Human Network nodes can interact with them.
####################################################################

# First stage: Build the application using the foundry image
FROM ghcr.io/foundry-rs/foundry AS builder

WORKDIR /app

# Note that when we copy files into our image, we preserve the
# directory structure. This is because the scripts rely on this
# structure.
COPY ./human-smart-contracts ./human-smart-contracts

RUN cd human-smart-contracts; forge install; forge build

# Second stage: Build Human Network PeerRegistry interface (which is a
# potential dependency of anvil_entrypoint.sh)
FROM rust:slim-bullseye AS human_builder

RUN rustup default nightly
RUN apt update && apt install cmake pkg-config libssl-dev build-essential -y

COPY ./vole-zk-prover ./vole-zk-prover
COPY ./human /human

WORKDIR /human

RUN cargo build --package registry_iface --bin registry_iface

# Thrid stage: Create the final runtime environment
FROM node:20-bullseye

WORKDIR /app

# Copy the built application and scripts from the builder stage
COPY --from=builder /app /app
COPY --from=builder /usr/local/bin/anvil /usr/local/bin/anvil
COPY --from=builder /usr/local/bin/cast /usr/local/bin/cast
COPY --from=builder /usr/local/bin/forge /usr/local/bin/forge

RUN apt update && apt install bash -y
RUN apt update && apt install -y libssl-dev pkg-config

COPY ./scripts/test/ /app/scripts/test/

# Because we want the register_peers script to work both with and without docker, we 
# want to preserve the folder structure here.
COPY --from=human_builder /human/target/debug/registry_iface /app/human/target/debug/registry_iface

RUN chmod +x /app/scripts/test/anvil_entrypoint.sh

ENTRYPOINT ["/app/scripts/test/anvil_entrypoint.sh"]
