.PHONY: dev-db stop-db clean-db test build run help test-clean test-isolated ci-test test-robust

.ONESHELL:

# Default database URL for development
DATABASE_URL := postgres://postgres:password@localhost:5432/otc_dev

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev-db: ## Start development database
	@echo "Starting PostgreSQL..."
	@docker compose -f compose.test-db.yml up -d
	@echo "Waiting for database to be ready..."
	@until docker exec test_db pg_isready -U postgres -d otc_dev >/dev/null 2>&1; do \
		sleep 0.1; \
	done
	@echo "Database ready at: $(DATABASE_URL)"

stop-db: ## Stop development database
	@docker compose -f compose.test-db.yml down

clean-db: ## Stop and remove database volumes
	@docker compose -f compose.test-db.yml down -v

test-clean: build-test ## Same as test but will clean up resources on success/failure
	@bash -c 'set -e; \
	trap "echo \"Cleaning up...\"; $(MAKE) clean-db" EXIT; \
	$(MAKE) dev-db; \
	cargo nextest run'

test: build-test | dev-db ## Run all tests
	@cargo nextest run

build-test: ## Build the project for testing
	@cargo build --tests


build: ## Build the project
	@cargo build

run: ## Run the OTC server
	@cargo run --bin otc-server

migrate: ## Run database migrations (requires running database)
	@sqlx migrate run --database-url $(DATABASE_URL)

migrate-revert: ## Revert last migration
	@sqlx migrate revert --database-url $(DATABASE_URL)

migrate-info: ## Show migration status
	@sqlx migrate info --database-url $(DATABASE_URL)