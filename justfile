# Docker compose files for main deployment
dc_files := "COMPOSE_BAKE=true docker compose -f etc/compose.devnet.yml -f etc/compose.override.yml"

dc_mm_files := "COMPOSE_BAKE=true docker compose --env-file .env.mm -f etc/compose.mm.yml"

dc_replica_files := "COMPOSE_BAKE=true docker compose --env-file .env.replica -f etc/compose.replica.yml"

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
    @{{test_db}} up -d --wait
    @echo "PostgreSQL is healthy and ready"

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
    VERSION_TAG=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="otc-server") | .version')
    echo "Checking version: ${VERSION_TAG}"
    
    # Check if the version tag already exists in Docker Hub
    if docker manifest inspect riftresearch/otc-server:${VERSION_TAG} > /dev/null 2>&1; then
        echo "Error: Version ${VERSION_TAG} already exists in Docker Hub"
        echo "Please update the version in Cargo.toml before releasing"
        exit 1
    fi
    
    echo "Building Docker image for version: ${VERSION_TAG}"
    docker build -f etc/Dockerfile.otc -t riftresearch/otc-server:${VERSION_TAG} .
    docker tag riftresearch/otc-server:${VERSION_TAG} riftresearch/otc-server:latest
    docker push riftresearch/otc-server:${VERSION_TAG}
    docker push riftresearch/otc-server:latest

# Deploy to Phala
phala-deploy:
    # stop the app first, otherwise the deploy command will just shutoff the machine and stop the app
    -phala cvms stop app_97c189391e051abc6e372aecad1d54bb34c39fde
    sleep 3
    phala deploy --uuid 1c11019a-3dc1-440d-8a52-2147dcf608da -c etc/compose.phala.yml -e .env.otc

# Docker compose prefix command for local integration testing 
# DOCKER_DEFAULT_PLATFORM=linux/amd64  
dc +args:
   HELIOS_BASE_EXECUTION_RPC="mock" HELIOS_ETHEREUM_EXECUTION_RPC="mock" PHALA_DB_SNI=mock PRIMARY_DB_PASSWORD=replica_password REPLICA_DB_PASSWORD=actual_replica_password POSTGRES_REPLICA_PASSWORD=replica_password {{dc_files}} {{args}}

# Docker compose prefix command for market maker with all config files - passes through any arguments
mm +args:
    #!/usr/bin/env bash
    set -e
    # Check if running 'down' with '-v' flag to prevent accidental volume deletion
    if [[ " {{args}} " =~ " down " ]] && [[ " {{args}} " =~ " -v " ]]; then
        echo "⚠️  WARNING: This will delete all volumes for the market maker!"
        echo "Are you sure you want to continue? (yes/no)"
        read -r response
        if [[ "$response" != "yes" ]]; then
            echo "Aborted."
            exit 1
        fi
    fi
    {{dc_mm_files}} {{args}}

# Docker compose prefix command for read replica
replica +args:
    {{dc_replica_files}} {{args}}

# Run clippy
clippy:
    cargo clippy --fix --allow-dirty --allow-staged
