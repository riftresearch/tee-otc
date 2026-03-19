use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to connect to replica database"))]
    ReplicaDatabaseConnection { source: sqlx::Error },

    #[snafu(display("Replica database query failed"))]
    ReplicaDatabaseQuery { source: sqlx::Error },

    #[snafu(display("Failed to initialize Postgres notification listener"))]
    ReplicaListenerConnection { source: sqlx::Error },

    #[snafu(display("Failed to subscribe to Postgres notification channel {channel}"))]
    ReplicaListen {
        source: sqlx::Error,
        channel: String,
    },

    #[snafu(display("Failed to receive Postgres notification"))]
    ReplicaNotificationReceive { source: sqlx::Error },

    #[snafu(display("Replica watch row was invalid: {message}"))]
    InvalidWatchRow { message: String },

    #[snafu(display("Failed to parse replica notification payload: {source}"))]
    NotificationPayload { source: serde_json::Error },

    #[snafu(display("Failed to initialize chain {chain}: {message}"))]
    ChainInit { chain: String, message: String },

    #[snafu(display("Failed to initialize discovery backend {backend}: {message}"))]
    DiscoveryBackendInit { backend: String, message: String },

    #[snafu(display("Bitcoin RPC request failed"))]
    BitcoinRpc {
        source: bitcoincore_rpc_async::Error,
    },

    #[snafu(display("Bitcoin esplora request failed"))]
    BitcoinEsplora { source: esplora_client::Error },

    #[snafu(display("EVM RPC request failed"))]
    EvmRpc {
        source: alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
    },

    #[snafu(display("EVM token indexer request failed"))]
    EvmTokenIndexer {
        source: evm_token_indexer_client::Error,
    },

    #[snafu(display("OTC request failed"))]
    OtcRequest { source: reqwest::Error },

    #[snafu(display("OTC rejected deposit observation with status {status}: {body}"))]
    OtcRejected {
        status: reqwest::StatusCode,
        body: String,
    },

    #[snafu(display("Failed to build OTC URL from base {base_url}: {source}"))]
    OtcUrl {
        source: url::ParseError,
        base_url: String,
    },

    #[snafu(display("A discovery backend task terminated unexpectedly"))]
    DiscoveryTaskJoin { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
