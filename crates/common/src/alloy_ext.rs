use std::str::FromStr;

use alloy::{network::EthereumWallet, providers::{fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller}, Identity, ProviderBuilder, RootProvider, WsConnect}, pubsub::{ConnectionHandle, PubSubConnect}, rpc::client::ClientBuilder, signers::local::LocalSigner, transports::{impl_future, TransportResult}};
use backoff::exponential::ExponentialBackoff;
use snafu::{ResultExt, Whatever};

#[derive(Clone, Debug)]
pub struct RetryWsConnect(WsConnect);

impl PubSubConnect for RetryWsConnect {
    fn is_local(&self) -> bool {
        self.0.is_local()
    }

    fn connect(&self) -> impl_future!(<Output = TransportResult<ConnectionHandle>>) {
        self.0.connect()
    }

    async fn try_reconnect(&self) -> TransportResult<ConnectionHandle> {
        backoff::future::retry(
            ExponentialBackoff::<backoff::SystemClock>::default(),
            || async { Ok(self.0.try_reconnect().await?) },
        )
        .await
    }
}

pub type WebsocketWalletProvider = FillProvider<
    JoinFill<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        WalletFiller<EthereumWallet>,
    >,
    RootProvider,
>;


/// Creates a provider that is both a websocket provider and a wallet provider.
/// note NOT type erased so we can access the wallet methods of the provider
pub async fn create_websocket_wallet_provider(
    evm_rpc_websocket_url: &str,
    private_key: [u8; 32],
) -> Result<WebsocketWalletProvider, Whatever> {
    let ws = RetryWsConnect(WsConnect::new(evm_rpc_websocket_url));
    let client = ClientBuilder::default()
        .pubsub(ws)
        .await.whatever_context("Failed to create client")?;

    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(
            LocalSigner::from_str(&alloy::hex::encode(private_key)).whatever_context("Failed to create local signer")?
        ))
        .connect_client(client);

    Ok(provider)
}
