use std::{sync::Arc, time::Duration};

use snafu::ResultExt;
use sqlx::{
    postgres::{PgListener, PgPoolOptions},
    PgPool,
};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::{
    config::SauronArgs,
    discovery::{
        bitcoin::BitcoinDiscoveryBackend, evm_erc20::EvmErc20DiscoveryBackend, run_backends,
        DiscoveryBackend, DiscoveryContext,
    },
    error::{
        ReplicaDatabaseConnectionSnafu, ReplicaListenSnafu, ReplicaListenerConnectionSnafu,
        ReplicaNotificationReceiveSnafu, Result,
    },
    otc_client::OtcClient,
    replica_db::migrate_replica,
    watch::{full_reconcile, WatchChangeNotification, WatchRepository, WatchStore},
};

pub async fn run(args: SauronArgs) -> Result<()> {
    let replica_pool = connect_replica_pool(&args).await?;
    migrate_replica(&replica_pool).await?;
    let repository = WatchRepository::new(replica_pool.clone());

    let mut listener = PgListener::connect(&args.otc_replica_database_url)
        .await
        .context(ReplicaListenerConnectionSnafu)?;
    listener
        .listen(&args.otc_replica_notification_channel)
        .await
        .context(ReplicaListenSnafu {
            channel: args.otc_replica_notification_channel.clone(),
        })?;

    let otc_client = OtcClient::new(&args)?;
    let store = WatchStore::default();
    let backends = build_backends(&args).await?;

    full_reconcile(&store, &repository).await?;

    let initial_watch_count = store.len().await;
    info!(
        replica_database = %args.otc_replica_database_name,
        notification_channel = %args.otc_replica_notification_channel,
        reconcile_interval_seconds = args.sauron_reconcile_interval_seconds,
        initial_watch_count,
        "Sauron startup completed"
    );

    let notification_task = run_notification_loop(
        listener,
        repository.clone(),
        store.clone(),
        args.otc_replica_notification_channel.clone(),
    );
    let reconcile_task = run_reconcile_loop(
        store.clone(),
        repository.clone(),
        Duration::from_secs(args.sauron_reconcile_interval_seconds),
    );
    let expiration_prune_task = run_expiration_prune_loop(store.clone(), Duration::from_secs(60));
    let discovery_task = run_backends(
        backends,
        DiscoveryContext {
            watches: store.clone(),
            otc_client,
        },
    );

    tokio::try_join!(
        notification_task,
        reconcile_task,
        expiration_prune_task,
        discovery_task
    )?;
    Ok(())
}

async fn connect_replica_pool(args: &SauronArgs) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(8)
        .connect(&args.otc_replica_database_url)
        .await
        .context(ReplicaDatabaseConnectionSnafu)
}

async fn build_backends(args: &SauronArgs) -> Result<Vec<Arc<dyn DiscoveryBackend>>> {
    let bitcoin = BitcoinDiscoveryBackend::new(args).await?;
    let ethereum = EvmErc20DiscoveryBackend::new_ethereum(args).await?;
    let base = EvmErc20DiscoveryBackend::new_base(args).await?;

    Ok(vec![Arc::new(bitcoin), Arc::new(ethereum), Arc::new(base)])
}

async fn run_notification_loop(
    mut listener: PgListener,
    repository: WatchRepository,
    store: WatchStore,
    channel: String,
) -> Result<()> {
    loop {
        let notification = listener
            .recv()
            .await
            .context(ReplicaNotificationReceiveSnafu)?;

        match WatchChangeNotification::parse(&notification) {
            Ok(change) => match repository.load_swap(change.swap_id).await {
                Ok(Some(watch)) => {
                    store.upsert(watch).await;
                    info!(swap_id = %change.swap_id, "Refreshed watch after Postgres notification");
                }
                Ok(None) => {
                    store.remove(change.swap_id).await;
                    info!(swap_id = %change.swap_id, "Removed watch after Postgres notification");
                }
                Err(error) => {
                    warn!(
                        swap_id = %change.swap_id,
                        %error,
                        "Targeted watch refresh failed; falling back to full reconcile"
                    );
                    full_reconcile(&store, &repository).await?;
                }
            },
            Err(error) => {
                warn!(
                    channel = %channel,
                    payload = notification.payload(),
                    %error,
                    "Replica notification payload was invalid; falling back to full reconcile"
                );
                full_reconcile(&store, &repository).await?;
            }
        }
    }
}

async fn run_reconcile_loop(
    store: WatchStore,
    repository: WatchRepository,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        full_reconcile(&store, &repository).await?;
    }
}

async fn run_expiration_prune_loop(store: WatchStore, interval: Duration) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let pruned = store.prune_expired().await;
        if pruned > 0 {
            info!(pruned, "Pruned expired Sauron watches from memory");
        }
    }
}
