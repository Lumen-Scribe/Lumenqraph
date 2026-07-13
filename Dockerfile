# Multi-stage build producing one image with all four service binaries.
# Uses rustls throughout (no OpenSSL), so the runtime only needs CA certs.

FROM rust:1-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations
RUN cargo build --release --workspace

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lumenqraph-indexer /usr/local/bin/
COPY --from=builder /app/target/release/lumenqraph-api /usr/local/bin/
COPY --from=builder /app/target/release/lumenqraph-webhooks /usr/local/bin/
COPY --from=builder /app/target/release/lumenqraph-mcp /usr/local/bin/
# Default to the API; override `command:` per service in compose.
CMD ["lumenqraph-api"]
