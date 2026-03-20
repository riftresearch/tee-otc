use snafu::ResultExt;
use sqlx::{migrate::Migrator, PgPool};
use tracing::info;

use crate::error::{ReplicaMigrationSnafu, Result};

// These migrations target the logical subscriber / replica endpoint that Sauron
// reads from. Keep replica-only objects, such as NOTIFY trigger wiring, here.
//
// The replica database also carries the primary OTC migrations in `_sqlx_migrations`,
// so this migrator must ignore applied versions it does not own.
static REPLICA_MIGRATOR: Migrator = Migrator {
    ignore_missing: true,
    ..sqlx::migrate!("./migrations-replica")
};

pub async fn migrate_replica(pool: &PgPool) -> Result<()> {
    info!("Running Sauron replica migrations...");
    REPLICA_MIGRATOR
        .run(pool)
        .await
        .context(ReplicaMigrationSnafu)?;
    info!("Sauron replica migrations complete");
    Ok(())
}
