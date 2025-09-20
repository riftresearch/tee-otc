.PHONY: start-db stop-db clean-db test build run help test-clean test-isolated ci-test test-robust docker-build-and-push phala-deploy

.ONESHELL:

# Default database URL for development
DATABASE_URL := postgres://postgres:password@localhost:5432/otc_dev

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

start-db: ## Start development database
	@echo "Starting PostgreSQL..."
	@docker compose -f compose.test-db.yml up -d
	@echo "Waiting for database to be ready..."
	@until docker exec otc_dev pg_isready -U postgres -d otc_dev >/dev/null 2>&1; do \
		sleep 0.1; \
	done
	@echo "Database ready at: $(DATABASE_URL)"

stop-db: ## Stop development database
	@docker compose -f compose.test-db.yml down

clean-db: ## Stop and remove database volumes
	@docker compose -f compose.test-db.yml down -v

test-clean: build-test | cache-devnet ## Same as test but will clean up resources on success/failure
	@bash -c 'set -e; \
	trap "echo \"Cleaning up...\"; $(MAKE) clean-db" EXIT; \
	$(MAKE) start-db; \
	cargo nextest run'

test: build-test | ## Run all tests, assumes devnet has been cached and database is running
	@$(MAKE) stop-db 
	@$(MAKE) start-db
	@cargo nextest run
	@$(MAKE) stop-db

build-test: ## Build the project for testing
	@cargo build --tests

cache-devnet: build-test ## Cache the devnet
	cargo run --bin devnet -- cache
	@echo "Devnet cached"

docker-release: ## Build and push the OTC server Docker image
	@GIT_COMMIT=$$(git rev-parse --short HEAD); \
	GIT_BRANCH=$$(git rev-parse --abbrev-ref HEAD); \
	VERSION_TAG="$${GIT_BRANCH}-$${GIT_COMMIT}"; \
	echo "Building Docker image for version: $${VERSION_TAG}"; \
	docker build -f Dockerfile.otc -t riftresearch/otc-server:$${VERSION_TAG} .; \
	docker tag riftresearch/otc-server:$${VERSION_TAG} riftresearch/otc-server:latest; \
	docker push riftresearch/otc-server:$${VERSION_TAG}; \
	docker push riftresearch/otc-server:latest

phala-deploy:
	phala deploy --uuid 408d8b85-c215-4dda-a71b-a1b601f1d6d4 -c compose.phala.yml -e .env.otc
