# Build stage
FROM rust:1.75-slim as builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto/ ./proto/
COPY crates/ ./crates/
COPY src/ ./src/

RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/dblentry /usr/local/bin/dblentry

EXPOSE 3000

ENTRYPOINT ["dblentry"]
