FROM rust:slim-bullseye AS builder
RUN rustup default nightly
RUN apt update && apt install pkg-config libssl-dev build-essential cmake gcc libclang-dev libpq-dev git -y
RUN cargo install --locked --git https://github.com/MystenLabs/sui.git --branch testnet sui --features tracing
WORKDIR /project

COPY ./human ./human
COPY ./vole-zk-prover ./vole-zk-prover
# For faster compiles and debugging but slower running:
# RUN cd ./human && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse RUSTFLAGS="-Znext-solver=globally" cargo build --bin observer
# RUN cd ./human && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin observer
# For production:
RUN cd ./human && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin observer --release

FROM node:18-bullseye-slim

RUN apt update && apt install curl libcurl4 -y

# For faster compiles and debugging but slower running:
# COPY --from=builder /project/human/target/debug/observer ./observer
# For production
COPY --from=builder /project/human/target/release/observer ./observer
    
COPY --from=builder /project/human/crates/observer/src/ethsign-relayer.js ./ethsign-relayer.js
RUN mkdir circuits
COPY --from=builder /project/human/crates/observer/circuits ./circuits

COPY --from=builder /project/human/crates/observer/package.json ./package.json
COPY --from=builder /project/human/crates/observer/package-lock.json ./package-lock.json

COPY --from=builder /usr/local/cargo/bin/sui ./usr/local/bin/sui

RUN npm install

# RUN npm install -g npm@10.5.0

ENV NODEJS_SCRIPTS_PATH=./
ENV CIRCUIT_DIR=./circuits

CMD ["./observer"]
EXPOSE 3000
