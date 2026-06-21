# syntax=docker/dockerfile:1
# Slim release image for the beyond-objects server, published to
# ghcr.io/beyondoss/beyond-objects for local-dev / docker-compose use.
# Built and run on ubuntu:24.04 (noble) to match the production rootfs.
FROM ubuntu:24.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl ca-certificates clang libclang-dev pkg-config \
      libssl-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain 1.92.0 --profile minimal
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --bin beyond-objects \
    && cp /src/target/release/beyond-objects /usr/local/bin/beyond-objects \
    && strip /usr/local/bin/beyond-objects

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl openssl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/beyond-objects /usr/local/bin/beyond-objects
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/beyond-objects", "serve"]
