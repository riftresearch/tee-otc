# Default database URL for development
database_url := "postgres://postgres:password@localhost:5433/otc_dev"

# Docker compose files for main deployment
dc_files := "docker compose -f etc/compose.devnet.yml -f etc/compose.override.yml"

# Docker compose for test database
test_db := "docker compose -f etc/compose.test-db.yml"

# Show this help message
help:
    @echo 'Usage: just [recipe]'
    @echo ''
    @echo 'Recipes:'
    @just --list

# Start development database
start-db:
    @echo "Starting PostgreSQL..."
    @{{test_db}} up -d
    @echo "Waiting for database to be ready..."
    @until docker exec otc_dev pg_isready -U postgres -d otc_dev >/dev/null 2>&1; do sleep 0.1; done
    @echo "Database ready at: {{database_url}}"

# Stop and remove database volumes
clean-db:
    {{test_db}} down -v

# Restart database
redb:
    just clean-db
    just start-db

# Build the project for testing
build-test:
    cargo build --tests

# Cache the devnet
cache-devnet: build-test
    cargo run --bin devnet -- cache
    @echo "Devnet cached"

# Run all tests, assumes devnet has been cached
test: build-test
    just redb
    cargo nextest run
    just clean-db

# Same as test but will clean up resources on success/failure
test-clean: build-test cache-devnet
    #!/usr/bin/env bash
    set -e
    trap "echo 'Cleaning up...'; just clean-db" EXIT
    just start-db
    cargo nextest run

# Build and push the OTC server Docker image
docker-release:
    #!/usr/bin/env bash
    set -e
    GIT_COMMIT=$(git rev-parse --short HEAD)
    GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    VERSION_TAG="${GIT_BRANCH}-${GIT_COMMIT}"
    echo "Building Docker image for version: ${VERSION_TAG}"
    docker build -f etc/Dockerfile.otc -t riftresearch/otc-server:${VERSION_TAG} .
    docker tag riftresearch/otc-server:${VERSION_TAG} riftresearch/otc-server:latest
    docker push riftresearch/otc-server:${VERSION_TAG}
    docker push riftresearch/otc-server:latest

# Deploy to Phala
phala-deploy:
    phala deploy --uuid 1c11019a-3dc1-440d-8a52-2147dcf608da -c etc/compose.phala.yml -e .env.otc

# Docker compose command with all config files - passes through any arguments
# DOCKER_DEFAULT_PLATFORM=linux/amd64  
dc +args:
    PRIMARY_DB_PASSWORD=replica_password REPLICA_DB_PASSWORD=actual_replica_password POSTGRES_REPLICA_PASSWORD=replica_password {{dc_files}} {{args}}

# Run clippy
clippy:
    cargo clippy --fix --allow-dirty --allow-staged