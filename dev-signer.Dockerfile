FROM rust:slim-bookworm AS builder

RUN rustup toolchain install nightly-2024-09-07
RUN rustup default nightly-2024-09-07
RUN apt update && apt install pkg-config libssl-dev build-essential cmake -y
WORKDIR /project
COPY ./network ./network
WORKDIR /project/network
# Define the build argument
ARG LOCAL_TEST_NET=false
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin signer --release

FROM debian:bookworm-slim

RUN apt update && apt install libssl-dev ca-certificates -y
# required for rate limiter
RUN apt install redis-server -y
COPY --from=builder /project/network/target/release/signer /
CMD ["sh", "-c", "redis-server --daemonize yes; /signer"]

