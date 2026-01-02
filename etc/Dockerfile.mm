FROM rust:1.90.0-trixie AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
COPY --from=planner /app/sqlx-sqlite-shim ./sqlx-sqlite-shim
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin market-maker --exclude integration-tests --workspace

FROM debian:trixie
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/market-maker /app/
CMD ["./market-maker"]

