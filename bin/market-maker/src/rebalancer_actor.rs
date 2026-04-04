use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use coinbase_exchange_client::CoinbaseClient;
use otc_models::{ChainType, TokenIdentifier, CB_BTC_CONTRACT_ADDRESS};
use snafu::{location, Location, OptionExt, ResultExt, Snafu};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};
use uuid::Uuid;

use crate::{
    db::{RebalanceJobState, RebalanceRepository},
    planner::{PlannerAsset, Rebalance},
    rebalancer::{
        broadcast_btc_to_cbbtc_source_transfer, broadcast_cbbtc_to_btc_source_transfer,
        wait_for_withdrawal_completion, RebalancerError,
    },
    wallet::{Wallet, WalletError},
};

#[derive(Debug, Snafu)]
pub enum RebalanceActorError {
    #[snafu(display("Rebalance repository error: {source}"))]
    RebalanceRepository {
        source: crate::db::RebalanceRepositoryError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Rebalance execution failed: {source}"))]
    Rebalancer {
        source: RebalancerError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Planner callback failed: {reason} at {loc}"))]
    PlannerCallback {
        reason: String,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Missing field {field} for rebalance job {job_id} at {loc}"))]
    MissingField {
        job_id: Uuid,
        field: &'static str,
        #[snafu(implicit)]
        loc: Location,
    },
}

pub type Result<T, E = RebalanceActorError> = std::result::Result<T, E>;

#[async_trait]
pub trait RebalanceDispatcher: Send + Sync {
    async fn dispatch_rebalance(&self, rebalance: Rebalance) -> Result<()>;
}

#[async_trait]
pub trait RebalanceStateSink: Send + Sync {
    async fn restore_active_rebalance(&self, rebalance: Rebalance) -> Result<()>;
    async fn rebalance_succeeded(&self) -> Result<()>;
    async fn rebalance_failed(&self) -> Result<()>;
}

#[async_trait]
trait RebalanceExecution: Send + Sync {
    fn recipient_address(&self, dst_asset: PlannerAsset) -> String;
    fn source_confirmations_required(&self, rebalance: Rebalance) -> u32;
    async fn broadcast_source_transfer(&self, rebalance: Rebalance) -> Result<String>;
    async fn wait_source_confirmations(
        &self,
        rebalance: Rebalance,
        tx_hash: &str,
        required_confirmations: u32,
    ) -> Result<()>;
    async fn submit_withdrawal(
        &self,
        rebalance: Rebalance,
        recipient_address: &str,
    ) -> Result<String>;
    async fn wait_withdrawal_completion(&self, withdrawal_id: &str) -> Result<String>;
}

#[derive(Clone)]
pub struct RebalanceActor {
    repository: Arc<RebalanceRepository>,
    execution: Arc<dyn RebalanceExecution>,
    sink: Arc<dyn RebalanceStateSink>,
    evm_chain: ChainType,
    active_jobs: Arc<Mutex<BTreeSet<Uuid>>>,
    cancellation_token: CancellationToken,
}

impl RebalanceActor {
    fn new(
        repository: Arc<RebalanceRepository>,
        execution: Arc<dyn RebalanceExecution>,
        sink: Arc<dyn RebalanceStateSink>,
        evm_chain: ChainType,
        cancellation_token: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            repository,
            execution,
            sink,
            evm_chain,
            active_jobs: Arc::new(Mutex::new(BTreeSet::new())),
            cancellation_token,
        })
    }

    pub async fn restore_and_resume(&self) -> Result<()> {
        let active = self
            .repository
            .list_active_rebalances()
            .await
            .context(RebalanceRepositorySnafu)?;

        for stored in active {
            self.sink.restore_active_rebalance(stored.rebalance).await?;
            self.spawn_job_runner(stored.id);
        }

        Ok(())
    }

    fn spawn_job_runner(&self, job_id: Uuid) {
        {
            let mut guard = self
                .active_jobs
                .lock()
                .expect("rebalance actor mutex poisoned");
            if !guard.insert(job_id) {
                return;
            }
        }

        let actor = self.clone();
        tokio::spawn(async move {
            let result = actor.run_job(job_id).await;
            if let Err(error) = &result {
                error!(job_id = %job_id, error = %error, "rebalance actor job failed");
            }
            let mut guard = actor
                .active_jobs
                .lock()
                .expect("rebalance actor mutex poisoned");
            guard.remove(&job_id);
        });
    }

    async fn run_job(&self, job_id: Uuid) -> Result<()> {
        loop {
            if self.cancellation_token.is_cancelled() {
                return Ok(());
            }

            let stored = match self
                .repository
                .get_rebalance(job_id)
                .await
                .context(RebalanceRepositorySnafu)?
            {
                Some(stored) => stored,
                None => return Ok(()),
            };

            match stored.state {
                RebalanceJobState::PendingSourceTransfer => {
                    let tx_hash = match self
                        .execution
                        .broadcast_source_transfer(stored.rebalance)
                        .await
                    {
                        Ok(tx_hash) => tx_hash,
                        Err(error) => {
                            return self.fail_job(job_id, error).await;
                        }
                    };

                    self.repository
                        .mark_waiting_source_confirmations(job_id, &tx_hash)
                        .await
                        .context(RebalanceRepositorySnafu)?;
                }
                RebalanceJobState::WaitingSourceConfirmations => {
                    let tx_hash = stored
                        .source_tx_hash
                        .as_deref()
                        .context(MissingFieldSnafu {
                            job_id,
                            field: "source_tx_hash",
                        })?;

                    if let Err(error) = self
                        .execution
                        .wait_source_confirmations(
                            stored.rebalance,
                            tx_hash,
                            stored.source_confirmations_required,
                        )
                        .await
                    {
                        return self.fail_job(job_id, error).await;
                    }

                    self.repository
                        .mark_pending_withdrawal_submission(job_id)
                        .await
                        .context(RebalanceRepositorySnafu)?;
                }
                RebalanceJobState::PendingWithdrawalSubmission => {
                    let withdrawal_id = match self
                        .execution
                        .submit_withdrawal(stored.rebalance, &stored.recipient_address)
                        .await
                    {
                        Ok(withdrawal_id) => withdrawal_id,
                        Err(error) => {
                            warn!(
                                job_id = %job_id,
                                error = %error,
                                "rebalance withdrawal submission failed, will retry"
                            );
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            continue;
                        }
                    };

                    self.repository
                        .mark_waiting_withdrawal_completion(job_id, &withdrawal_id)
                        .await
                        .context(RebalanceRepositorySnafu)?;
                }
                RebalanceJobState::WaitingWithdrawalCompletion => {
                    let withdrawal_id =
                        stored.withdrawal_id.as_deref().context(MissingFieldSnafu {
                            job_id,
                            field: "withdrawal_id",
                        })?;

                    let completion_tx_hash = match self
                        .execution
                        .wait_withdrawal_completion(withdrawal_id)
                        .await
                    {
                        Ok(tx_hash) => tx_hash,
                        Err(error) => {
                            return self.fail_job(job_id, error).await;
                        }
                    };

                    self.repository
                        .mark_completed(job_id, &completion_tx_hash)
                        .await
                        .context(RebalanceRepositorySnafu)?;
                    self.sink.rebalance_succeeded().await?;
                    return Ok(());
                }
                RebalanceJobState::Completed | RebalanceJobState::Failed => return Ok(()),
            }
        }
    }

    async fn fail_job(&self, job_id: Uuid, error: RebalanceActorError) -> Result<()> {
        self.repository
            .mark_failed(job_id, &error.to_string())
            .await
            .context(RebalanceRepositorySnafu)?;
        self.sink.rebalance_failed().await?;
        Err(error)
    }
}

#[async_trait]
impl RebalanceDispatcher for RebalanceActor {
    async fn dispatch_rebalance(&self, rebalance: Rebalance) -> Result<()> {
        let recipient_address = self.execution.recipient_address(rebalance.dst_asset);
        let source_confirmations_required = self.execution.source_confirmations_required(rebalance);

        let stored = self
            .repository
            .create_rebalance(
                rebalance,
                self.evm_chain,
                &recipient_address,
                source_confirmations_required,
            )
            .await
            .context(RebalanceRepositorySnafu)?;

        self.spawn_job_runner(stored.id);
        Ok(())
    }
}

struct CoinbaseRebalanceExecution {
    evm_chain: ChainType,
    coinbase_client: CoinbaseClient,
    bitcoin_wallet: Arc<dyn Wallet>,
    evm_wallet: Arc<dyn Wallet>,
    confirmation_poll_interval: Duration,
    btc_coinbase_confirmations: u32,
    cbbtc_coinbase_confirmations: u32,
}

impl CoinbaseRebalanceExecution {
    fn new(
        evm_chain: ChainType,
        coinbase_client: CoinbaseClient,
        bitcoin_wallet: Arc<dyn Wallet>,
        evm_wallet: Arc<dyn Wallet>,
        confirmation_poll_interval: Duration,
        btc_coinbase_confirmations: u32,
        cbbtc_coinbase_confirmations: u32,
    ) -> Self {
        Self {
            evm_chain,
            coinbase_client,
            bitcoin_wallet,
            evm_wallet,
            confirmation_poll_interval,
            btc_coinbase_confirmations,
            cbbtc_coinbase_confirmations,
        }
    }
}

#[async_trait]
impl RebalanceExecution for CoinbaseRebalanceExecution {
    fn recipient_address(&self, dst_asset: PlannerAsset) -> String {
        match dst_asset {
            PlannerAsset::AssetA => self
                .bitcoin_wallet
                .receive_address(&TokenIdentifier::Native),
            PlannerAsset::AssetB => self.evm_wallet.receive_address(&TokenIdentifier::address(
                CB_BTC_CONTRACT_ADDRESS.to_string(),
            )),
        }
    }

    fn source_confirmations_required(&self, rebalance: Rebalance) -> u32 {
        match rebalance.src_asset {
            PlannerAsset::AssetA => self.btc_coinbase_confirmations,
            PlannerAsset::AssetB => self.cbbtc_coinbase_confirmations,
        }
    }

    async fn broadcast_source_transfer(&self, rebalance: Rebalance) -> Result<String> {
        match rebalance.src_asset {
            PlannerAsset::AssetA => broadcast_btc_to_cbbtc_source_transfer(
                self.bitcoin_wallet.as_ref(),
                &self.coinbase_client,
                rebalance.amount,
                self.evm_chain,
            )
            .await
            .map_err(rebalancer_actor_error),
            PlannerAsset::AssetB => broadcast_cbbtc_to_btc_source_transfer(
                self.evm_wallet.as_ref(),
                &self.coinbase_client,
                rebalance.amount,
                self.evm_chain,
            )
            .await
            .map_err(rebalancer_actor_error),
        }
    }

    async fn wait_source_confirmations(
        &self,
        rebalance: Rebalance,
        tx_hash: &str,
        required_confirmations: u32,
    ) -> Result<()> {
        match rebalance.src_asset {
            PlannerAsset::AssetA => self
                .bitcoin_wallet
                .guarantee_confirmations(
                    tx_hash,
                    required_confirmations as u64,
                    self.confirmation_poll_interval,
                )
                .await
                .map_err(wallet_rebalancer_actor_error),
            PlannerAsset::AssetB => self
                .evm_wallet
                .guarantee_confirmations(
                    tx_hash,
                    required_confirmations as u64,
                    self.confirmation_poll_interval,
                )
                .await
                .map_err(wallet_rebalancer_actor_error),
        }?;

        Ok(())
    }

    async fn submit_withdrawal(
        &self,
        rebalance: Rebalance,
        recipient_address: &str,
    ) -> Result<String> {
        self.coinbase_client
            .withdraw_bitcoin(
                recipient_address,
                &rebalance.amount,
                match rebalance.dst_asset {
                    PlannerAsset::AssetA => ChainType::Bitcoin,
                    PlannerAsset::AssetB => self.evm_chain,
                },
            )
            .await
            .map_err(coinbase_rebalancer_actor_error)
    }

    async fn wait_withdrawal_completion(&self, withdrawal_id: &str) -> Result<String> {
        wait_for_withdrawal_completion(&self.coinbase_client, withdrawal_id)
            .await
            .map_err(rebalancer_actor_error)
    }
}

pub fn build_rebalance_actor(
    repository: Arc<RebalanceRepository>,
    sink: Arc<dyn RebalanceStateSink>,
    evm_chain: ChainType,
    coinbase_client: CoinbaseClient,
    bitcoin_wallet: Arc<dyn Wallet>,
    evm_wallet: Arc<dyn Wallet>,
    confirmation_poll_interval: Duration,
    btc_coinbase_confirmations: u32,
    cbbtc_coinbase_confirmations: u32,
    cancellation_token: CancellationToken,
) -> Arc<RebalanceActor> {
    let execution = Arc::new(CoinbaseRebalanceExecution::new(
        evm_chain,
        coinbase_client,
        bitcoin_wallet,
        evm_wallet,
        confirmation_poll_interval,
        btc_coinbase_confirmations,
        cbbtc_coinbase_confirmations,
    ));

    RebalanceActor::new(repository, execution, sink, evm_chain, cancellation_token)
}

fn rebalancer_actor_error(source: RebalancerError) -> RebalanceActorError {
    RebalanceActorError::Rebalancer {
        source,
        loc: location!(),
    }
}

fn wallet_rebalancer_actor_error(source: WalletError) -> RebalanceActorError {
    rebalancer_actor_error(RebalancerError::Wallet {
        source,
        loc: location!(),
    })
}

fn coinbase_rebalancer_actor_error(
    source: coinbase_exchange_client::CoinbaseExchangeClientError,
) -> RebalanceActorError {
    rebalancer_actor_error(RebalancerError::CoinbaseExchangeClientError {
        source,
        loc: location!(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Mutex as AsyncMutex;
    use tokio::time::{sleep, timeout};

    #[derive(Default)]
    struct FakeSink {
        restored: AsyncMutex<Vec<Rebalance>>,
        succeeded: AsyncMutex<u32>,
        failed: AsyncMutex<u32>,
    }

    #[async_trait]
    impl RebalanceStateSink for FakeSink {
        async fn restore_active_rebalance(&self, rebalance: Rebalance) -> Result<()> {
            self.restored.lock().await.push(rebalance);
            Ok(())
        }

        async fn rebalance_succeeded(&self) -> Result<()> {
            *self.succeeded.lock().await += 1;
            Ok(())
        }

        async fn rebalance_failed(&self) -> Result<()> {
            *self.failed.lock().await += 1;
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeExecution {
        calls: AsyncMutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl RebalanceExecution for FakeExecution {
        fn recipient_address(&self, dst_asset: PlannerAsset) -> String {
            match dst_asset {
                PlannerAsset::AssetA => "btc-dest".to_string(),
                PlannerAsset::AssetB => "cbbtc-dest".to_string(),
            }
        }

        fn source_confirmations_required(&self, _rebalance: Rebalance) -> u32 {
            3
        }

        async fn broadcast_source_transfer(&self, _rebalance: Rebalance) -> Result<String> {
            self.calls.lock().await.push("broadcast_source_transfer");
            Ok("source-tx".to_string())
        }

        async fn wait_source_confirmations(
            &self,
            _rebalance: Rebalance,
            _tx_hash: &str,
            _required_confirmations: u32,
        ) -> Result<()> {
            self.calls.lock().await.push("wait_source_confirmations");
            Ok(())
        }

        async fn submit_withdrawal(
            &self,
            _rebalance: Rebalance,
            _recipient_address: &str,
        ) -> Result<String> {
            self.calls.lock().await.push("submit_withdrawal");
            Ok("withdrawal-id".to_string())
        }

        async fn wait_withdrawal_completion(&self, _withdrawal_id: &str) -> Result<String> {
            self.calls.lock().await.push("wait_withdrawal_completion");
            Ok("completion-tx".to_string())
        }
    }

    async fn wait_for_terminal_state(
        pool: &sqlx::PgPool,
        expected_state: &str,
    ) -> sqlx::Result<()> {
        timeout(Duration::from_secs(5), async {
            loop {
                let state: Option<String> = sqlx::query_scalar(
                    "SELECT state FROM mm_rebalances ORDER BY created_at DESC LIMIT 1",
                )
                .fetch_optional(pool)
                .await?;
                if state.as_deref() == Some(expected_state) {
                    return Ok::<(), sqlx::Error>(());
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("timed out waiting for terminal rebalance state")
    }

    #[sqlx::test]
    async fn dispatch_rebalance_persists_and_completes(pool: sqlx::PgPool) -> sqlx::Result<()> {
        let db = crate::db::Database::from_pool(pool.clone()).await.unwrap();
        let repository = Arc::new(db.rebalances());
        let sink = Arc::new(FakeSink::default());
        let execution = Arc::new(FakeExecution::default());
        let actor = RebalanceActor::new(
            repository,
            execution.clone(),
            sink.clone(),
            ChainType::Base,
            CancellationToken::new(),
        );

        actor
            .dispatch_rebalance(Rebalance {
                src_asset: PlannerAsset::AssetA,
                dst_asset: PlannerAsset::AssetB,
                amount: 42,
            })
            .await
            .unwrap();

        wait_for_terminal_state(&pool, "completed").await?;

        let succeeded = *sink.succeeded.lock().await;
        let failed = *sink.failed.lock().await;
        let calls = execution.calls.lock().await.clone();

        assert_eq!(succeeded, 1);
        assert_eq!(failed, 0);
        assert_eq!(
            calls,
            vec![
                "broadcast_source_transfer",
                "wait_source_confirmations",
                "submit_withdrawal",
                "wait_withdrawal_completion",
            ]
        );

        Ok(())
    }

    #[sqlx::test]
    async fn restore_and_resume_waiting_withdrawal_completion(
        pool: sqlx::PgPool,
    ) -> sqlx::Result<()> {
        let db = crate::db::Database::from_pool(pool.clone()).await.unwrap();
        let repository = Arc::new(db.rebalances());
        let stored = repository
            .create_rebalance(
                Rebalance {
                    src_asset: PlannerAsset::AssetB,
                    dst_asset: PlannerAsset::AssetA,
                    amount: 99,
                },
                ChainType::Base,
                "btc-dest",
                2,
            )
            .await
            .unwrap();
        repository
            .mark_waiting_source_confirmations(stored.id, "source-tx")
            .await
            .unwrap();
        repository
            .mark_pending_withdrawal_submission(stored.id)
            .await
            .unwrap();
        repository
            .mark_waiting_withdrawal_completion(stored.id, "withdrawal-id")
            .await
            .unwrap();

        let sink = Arc::new(FakeSink::default());
        let execution = Arc::new(FakeExecution::default());
        let actor = RebalanceActor::new(
            repository,
            execution.clone(),
            sink.clone(),
            ChainType::Base,
            CancellationToken::new(),
        );

        actor.restore_and_resume().await.unwrap();
        wait_for_terminal_state(&pool, "completed").await?;

        let restored = sink.restored.lock().await.clone();
        let succeeded = *sink.succeeded.lock().await;
        let calls = execution.calls.lock().await.clone();

        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].amount, 99);
        assert_eq!(succeeded, 1);
        assert_eq!(calls, vec!["wait_withdrawal_completion"]);

        Ok(())
    }
}
