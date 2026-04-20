# syntax=docker/dockerfile:1
FROM rust:1.94 AS builder
WORKDIR /app
COPY . .
# SSH forwarding for private git deps (br-rust-common).
# Build with: docker build --ssh default .
RUN --mount=type=ssh cargo build --release --bin svc-notifier

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/svc-notifier /usr/local/bin/
COPY --from=builder /app/migrations /app/migrations
ENTRYPOINT ["svc-notifier"]
