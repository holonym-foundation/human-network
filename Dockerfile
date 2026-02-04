# # FROM --platform=arm64 rust:latest
# FROM rust:slim-bullseye AS builder
# RUN rustup default nightly
# # CMD ["sleep", "infinity"]
# # RUN apk update && apk add musl-dev alpine-sdk openssl-dev
# RUN apt update && apt install pkg-config libssl-dev build-essential -y
# WORKDIR /project
# # RUN mkdir src
# # COPY ./scalar-mul-core/Cargo.toml.cache-for-docker ./Cargo.toml
# # COPY ./scalar-mul-core/src/empty.rs ./src/empty.rs
# # RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build
# # RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --release
# # RUN rm -f ./Cargo.toml
# # RUN rm -f ./Cargo.lock
# # COPY ./scalar-mul-core/. .

# COPY ./vole-zk-prover ./vole-zk-prover
# COPY ./human ./human
# WORKDIR ./human
# # # For faster compiles and debugging but slower running:
# # # RUN #--release
# # RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin verifier --features "testnet_logging mock_credits" #--release
# # RUN CARGO_REGISTRIES_CRAT CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin prover --features "testnet_logging mock_credits"ES_IO_PROTOCOL=sparse cargo build --bin relayer --features "testnet_logging mock_credits" #--release
# # RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin testnet_logging --features "testnet_logging mock_credits" #--release
# # For production:
# RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin prover --release --features "testnet_logging"
# RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin verifier --release --features "testnet_logging"
# RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin relayer --release --features "testnet_logging"
# RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin testnet_logging --release --features "testnet_logging"


# FROM node:18-bullseye-slim
# #FROM node:18-alpine3.19
# # FROM redis:bullseye

# RUN apt update && apt install curl libcurl4 -y

# RUN npm install -g npm@10.5.0
# ARG NPM_TOKEN=YOUR_NPM_TOKEN_HERE

# RUN echo "//registry.npmjs.org/:_authToken=${NPM_TOKEN}" > $HOME/.npmrc
# RUN npm i -g @othentic/othentic-cli

# # RUN apt install lsb-release curl gpg -y
# # RUN curl -fsSL https://packages.redis.io/gpg | gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
# # RUN echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/redis.list
# RUN apt install redis-server -y
# # RUN apk add redis


# # For faster compiles and debugging but slower running:
# # COPY --from=builder /project/target/debug/prover ./prover
# # COPY --from=builder /project/target/debug/verifier ./verifier
# # COPY --from=builder /project/target/debug/relayer ./relayer
# # COPY --from=builder /project/target/debug/testnet_logging ./testnet_logging
# # For production
# COPY --from=builder /project/human/target/release/prover ./prover
# COPY --from=builder /project/human/target/release/verifier ./verifier
# COPY --from=builder /project/human/target/release/relayer ./relayer
# COPY --from=builder /project/human/target/release/testnet_logging ./testnet_logging




# ^^^^^ OLD DOCKERFILE ^^^^^
# vvvvv NEW DOCKERFILE vvvvv




# FROM --platform=arm64 rust:latest
FROM rust:slim-bullseye AS builder
RUN rustup toolchain install nightly-2024-09-07
RUN rustup default nightly-2024-09-07
RUN apt update && apt install pkg-config libssl-dev build-essential cmake -y
WORKDIR /project

# Installing bunyan before copying projects to avoid recompiling
RUN cargo install bunyan

COPY ./vole-zk-prover ./vole-zk-prover
COPY ./human ./human
WORKDIR ./human

# Define the build argument
ARG LOCAL_TEST_NET=false

# # For faster compiles and debugging but slower running:
# RUN cargo build --package node --bin human_node
# RUN cargo build --package registry_iface --bin registry_iface
# RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin verifier --features "v0nodes"
# For production:
# RUN cargo build --package node --bin human_node --release --features "testnet_logging"
RUN if [ "$LOCAL_TEST_NET" = "true" ]; then \
        cargo build --package node --bin human_node --release -p actors --features "local_test_net" -p network --features "local_test_net"; \
    else \
        cargo build --package node --bin human_node --release; \
    fi
RUN cargo build --package registry_iface --bin registry_iface --release
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin signer --release
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin verifier --release --features "v0nodes"
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin cli --release

FROM node:22.6.0-bullseye-slim
RUN apt update && apt install curl libcurl4 -y

RUN npm install -g npm@10.5.0
RUN npm i -g @othentic/othentic-cli
RUN npm i -g @libp2p/autonat

# TODO: Maybe we should let the user specify their own redis node?
RUN apt install redis-server -y

# For faster compiles and debugging but slower running:
# COPY --from=builder /project/human/target/debug/human_node ./human_node
# COPY --from=builder /project/human/target/debug/registry_iface ./registry_iface
# COPY --from=builder /project/human/target/debug/verifier ./verifier
# For production
COPY --from=builder /project/human/target/release/human_node ./human_node
COPY --from=builder /project/human/target/release/registry_iface ./registry_iface
COPY --from=builder /project/human/target/release/signer ./signer
COPY --from=builder /project/human/target/release/verifier ./verifier
COPY --from=builder /project/human/target/release/cli ./cli

COPY --from=builder /usr/local/cargo/bin/bunyan /usr/bin/bunyan

# # Create a new user to run the node, so the node lacks root privilege:
# RUN groupadd -g 999 humanuser && \
# useradd -r -u 999 -g humanuser humanuser
# USER humanuser