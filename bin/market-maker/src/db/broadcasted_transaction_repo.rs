use otc_models::ChainType;
use snafu::prelude::*;
use sqlx::PgPool;

use crate::bitcoin_wallet::transaction_broadcaster::ForeignUtxo;

#[derive(Debug, Snafu)]
pub enum BroadcastedTransactionRepositoryError {
    #[snafu(display("Database error: {source}"))]
    Database { source: sqlx::Error },

    #[snafu(display("Invalid batch nonce digest length: {len}"))]
    InvalidDigest { len: usize },

    #[snafu(display("Unknown chain value: {value}"))]
    UnknownChain { value: String },

    #[snafu(display("Serialization error: {source}"))]
    Serialization { source: serde_json::Error },
}

pub type BroadcastedTransactionRepositoryResult<T, E = BroadcastedTransactionRepositoryError> =
    std::result::Result<T, E>;

#[derive(Clone)]
pub struct BroadcastedTransactionRepository {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct BroadcastedTransaction {
    pub txid: String,
    pub chain: ChainType,
    pub txdata: Vec<u8>,
    /// Only present for bitcoin transactions
    pub bitcoin_tx_foreign_utxos: Option<Vec<ForeignUtxo>>,
    pub absolute_fee: u64,
}

impl BroadcastedTransactionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_broadcasted_transaction(
        &self,
        txid: &str,
    ) -> BroadcastedTransactionRepositoryResult<Option<BroadcastedTransaction>> {
        let row: Option<(String, String, Vec<u8>, Option<serde_json::Value>, i64)> =
            sqlx::query_as(
                r#"
            SELECT txid, chain, txdata, bitcoin_tx_foreign_utxos, absolute_fee
            FROM mm_broadcasted_transactions
            WHERE txid = $1
            "#,
            )
            .bind(txid)
            .fetch_optional(&self.pool)
            .await
            .context(DatabaseSnafu)?;

        if let Some((txid, chain_str, txdata, utxos_json, absolute_fee)) = row {
            let chain = chain_from_db(&chain_str)?;
            let bitcoin_tx_foreign_utxos: Option<Vec<ForeignUtxo>> = utxos_json
                .map(serde_json::from_value)
                .transpose()
                .context(SerializationSnafu)?;

            Ok(Some(BroadcastedTransaction {
                txid,
                chain,
                txdata,
                bitcoin_tx_foreign_utxos,
                absolute_fee: absolute_fee as u64,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn set_broadcasted_transaction(
        &self,
        txid: &str,
        chain: ChainType,
        txdata: Vec<u8>,
        bitcoin_tx_foreign_utxos: Option<Vec<ForeignUtxo>>,
        absolute_fee: u64,
    ) -> BroadcastedTransactionRepositoryResult<()> {
        // Serialize optional ForeignUtxo list to JSON using serde derives
        let foreign_utxos_json: Option<serde_json::Value> = bitcoin_tx_foreign_utxos
            .map(serde_json::to_value)
            .transpose()
            .context(SerializationSnafu)?;

        sqlx::query(
            r#"
            INSERT INTO mm_broadcasted_transactions (txid, chain, txdata, bitcoin_tx_foreign_utxos, absolute_fee)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (txid)
            DO UPDATE SET
                chain = EXCLUDED.chain,
                txdata = EXCLUDED.txdata,
                bitcoin_tx_foreign_utxos = EXCLUDED.bitcoin_tx_foreign_utxos,
                absolute_fee = EXCLUDED.absolute_fee
            "#,
        )
        .bind(txid)
        .bind(chain_to_db(&chain))
        .bind(txdata)
        .bind(foreign_utxos_json)
        .bind(absolute_fee as i64)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }
}

fn chain_to_db(chain: &ChainType) -> &'static str {
    match chain {
        ChainType::Bitcoin => "bitcoin",
        ChainType::Ethereum => "ethereum",
    }
}

fn chain_from_db(value: &str) -> BroadcastedTransactionRepositoryResult<ChainType> {
    match value {
        "bitcoin" => Ok(ChainType::Bitcoin),
        "ethereum" => Ok(ChainType::Ethereum),
        other => Err(BroadcastedTransactionRepositoryError::UnknownChain {
            value: other.to_string(),
        }),
    }
}
