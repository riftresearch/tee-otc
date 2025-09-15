FROM rust:1.88 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin otc-server
RUN mkdir -p /app/config

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/otc-server /app/
EXPOSE 4422 
CMD ["./otc-server", "--host", "0.0.0.0", "--port", "4422"]
