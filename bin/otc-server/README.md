# OTC Server

The main server for the TEE-OTC cross-chain swap system.

## Development

### Prerequisites

- Rust 1.70+
- PostgreSQL 14+
- Docker (optional, for running PostgreSQL)

### Database Setup

Start the development database with migrations:

```bash
make dev-db
```

This will:
- Start PostgreSQL on port 5432
- Create the `otc_dev` database
- Wait for the database to be ready
- Run all migrations automatically

Other database commands:
```bash
make stop-db     # Stop the database
make clean-db    # Stop and remove all data
make migrate     # Run migrations manually
```

### Database Migrations

The project uses SQLx migrations for schema management:

- Migrations are in `./migrations/`
- Applied automatically on server startup
- Embedded in the binary at compile time

To create a new migration:
```bash
# Install sqlx-cli if you haven't already
cargo install sqlx-cli --no-default-features --features rustls,postgres

# Create a new migration
sqlx migrate add <migration_name>

# Edit the migration file in ./migrations/
# The server will apply it on next startup
```

### Database Configuration

The project uses SQLx with runtime queries and automatic migrations. Database migrations are run automatically when the server starts.

To run the test database:
```bash
docker compose -f ../../compose.test-db.yml up -d
```

The database connection URL can be configured via:
- `DATABASE_URL` environment variable
- `--database-url` command line argument
- Default: `postgres://otc_user:otc_password@localhost:5432/otc_db`

### Running Tests

The database tests use `#[sqlx::test]` which automatically:
- Creates a temporary test database for each test
- Runs the schema setup
- Executes the test in isolation
- Drops the database after the test

To run tests:
```bash
# From the otc-server directory
cargo test

# Run a specific test
cargo test test_quote_round_trip

# Run with output
cargo test -- --nocapture
```

The `sqlx::test` macro automatically creates isolated test databases for each test using the DATABASE_URL environment variable.

### Running the Server

```bash
# Development mode
cargo run

# With custom settings
cargo run -- --host 0.0.0.0 --port 8080 --database-url postgres://user:pass@localhost/db
```

## Architecture

- `db/` - Database layer with repositories
  - `quote_repo.rs` - Quote CRUD operations
  - `swap_repo.rs` - Swap CRUD with public/private separation
  - `conversions.rs` - Type conversions for database storage
  - `row_mappers.rs` - Row to domain type mapping
- `migrations/` - SQLx database migrations
- `api/` - API request/response DTOs
- `services/` - Business logic services
- `server.rs` - HTTP/WebSocket server setup
- `main.rs` - CLI entry point

## Testing

Tests are embedded in the repository files using `#[cfg(test)]` modules. This keeps tests close to the code they test and ensures they're updated together.

Key test scenarios:
- Round-trip serialization of all types
- U256 large number handling
- Public/Private data separation for swaps
- Enum serialization (ChainType, TokenIdentifier, SwapStatus)
- Timestamp precision handling