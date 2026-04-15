# Multi-stage build for the tiktok-gen Rust Kafka generator.
#
# Build args:
#   (none)

FROM rust:1.82-bookworm AS builder

# librdkafka (built via rdkafka's default cmake-build feature) needs cmake, a C/C++
# toolchain, pkg-config and OpenSSL headers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/tiktok-gen /usr/local/bin/tiktok-gen

ENTRYPOINT ["/usr/local/bin/tiktok-gen"]
