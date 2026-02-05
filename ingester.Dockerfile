# Stage 1: Builder
FROM rust:slim-bookworm AS builder

# Install the specific nightly toolchain
RUN rustup toolchain install nightly-2024-09-07
RUN rustup default nightly-2024-09-07

# Update apt and install necessary build dependencies
# - pkg-config: Helps Rust crates find C libraries
# - libssl-dev: OpenSSL development headers
# - build-essential: Provides gcc, g++, make (for compiling C/C++ dependencies)
# - cmake: Often required by C/C++ build scripts
# - librdkafka-dev: Development headers for the librdkafka C library
# - libcurl4-openssl-dev: Development headers for libcurl (with OpenSSL support)
# - libsasl2-dev: Development headers for SASL (required by librdkafka for authentication)
# - zlib1g-dev: Development headers for zlib (compression library)
# - libgmp-dev: Development headers for GMP (GNU Multiple Precision Arithmetic Library), often needed by crypto libraries
# - libpq-dev: PostgreSQL client library development files (CRITICAL - this was missing!)
RUN apt update && apt install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    cmake \
    librdkafka-dev \
    libcurl4-openssl-dev \
    libsasl2-dev \
    zlib1g-dev \
    libgmp-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory for the project
WORKDIR /project

# Copy your Rust project source code
COPY ./network ./network

# Change to the project's specific directory
WORKDIR /project/network

# Define the build argument (if needed for your project logic)
ARG LOCAL_TEST_NET=false

# Build the Rust application in release mode
# CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse can speed up dependency resolution
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo build --bin ingester --release

# Stage 2: Runtime
# Use a slim Debian image for the final runtime to keep the image size small
FROM debian:bookworm-slim

# Install runtime dependencies
# - libssl3: Runtime library for OpenSSL (changed from libssl-dev)
# - ca-certificates: For trusted root certificates for HTTPS/TLS connections
# - zlib1g: Runtime library for zlib
# - libpq5: PostgreSQL client library runtime (CRITICAL - this was missing!)
# - librdkafka1: Runtime library for librdkafka
# - libcurl4: Runtime library for libcurl
# - libsasl2-2: Runtime library for SASL
RUN apt update && apt install -y \
    libssl3 \
    ca-certificates \
    zlib1g \
    libpq5 \
    librdkafka1 \
    libcurl4 \
    libsasl2-2 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /project/network/target/release/ingester /

# Set the command to run when the container starts
CMD ["/ingester"]