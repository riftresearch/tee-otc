use std::time::Duration;

use alloy::{
    hex,
    primitives::U256,
    providers::ext::AnvilApi,
    signers::{local::PrivateKeySigner, SignerSync},
    sol,
    sol_types::{eip712_domain, SolStruct},
};
use bitcoin::consensus::deserialize;
use bitcoincore_rpc_async::RpcApi;
use devnet::{bitcoin_devnet::MiningMode, MultichainAccount, RiftDevnet};
use market_maker::{
    bitcoin_wallet::BitcoinWallet, evm_wallet::EVMWallet, run_market_maker, wallet::Wallet,
};
use otc_chains::traits::Payment;
use otc_models::{
    ChainType, Currency, Lot, QuoteRequest, Swap, SwapMode, SwapStatus, TokenIdentifier,
};
use otc_protocols::rfq::RFQResult;
use otc_server::{
    api::{
        CreateSwapRequest, DepositObservationAcceptedResponse, DepositObservationRequest,
        ParticipantAuth, ParticipantAuthKind,
    },
    server::run_server,
};
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use tokio::task::JoinSet;
use tracing::info;
use uuid::Uuid;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_mm_test_args_for_base,
    build_otc_server_test_args, build_rfq_server_test_args, build_test_user_base_wallet,
    build_test_user_ethereum_wallet, build_tmp_bitcoin_wallet_db_file,
    disable_auto_sauron_for_port, get_free_port, wait_for_market_maker_to_connect_to_rfq_server,
    wait_for_otc_server_to_be_ready, wait_for_rfq_server_to_be_ready, wait_for_swap_status,
    wait_for_swap_statuses, PgConnectOptionsExt, TEST_DETECTOR_API_ID, TEST_DETECTOR_API_SECRET,
};

sol! {
    #[derive(Debug)]
    struct ParticipantDepositDetectionAuthPayload {
        string swapId;
        string sourceChain;
        string sourceToken;
        string depositAddress;
        string txHash;
        uint64 transferIndex;
        uint256 amount;
        uint64 signedAt;
    }
}

struct ExternalDetectionFixture {
    devnet: RiftDevnet,
    service_join_set: JoinSet<()>,
    wallet_join_set: JoinSet<market_maker::Result<()>>,
    user_bitcoin_wallet: BitcoinWallet,
    user_account: MultichainAccount,
    otc_port: u16,
    rfq_port: u16,
}

impl ExternalDetectionFixture {
    async fn shutdown(mut self) {
        drop(self.devnet);
        tokio::join!(
            self.wallet_join_set.shutdown(),
            self.service_join_set.shutdown()
        );
    }
}

#[sqlx::test]
async fn trusted_detector_route_accepts_bitcoin_deposit(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let mut fixture =
        setup_external_detection_fixture(connect_options, false, ChainType::Ethereum).await;
    let client = reqwest::Client::new();

    let swap = request_and_create_btc_to_eth_swap(
        &client,
        fixture.otc_port,
        fixture.rfq_port,
        &fixture.devnet,
        &fixture.user_account,
    )
    .await;

    let tx_hash = fixture
        .user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: swap.quote.from.currency.clone(),
                    amount: swap.quote.min_input,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    let (transfer_index, amount) =
        find_bitcoin_output_for_address(&fixture.devnet, &tx_hash, &swap.deposit_vault_address)
            .await;

    let request = DepositObservationRequest {
        source_chain: swap.quote.from.currency.chain,
        source_token: swap.quote.from.currency.token.clone(),
        tx_hash: tx_hash.clone(),
        amount,
        transfer_index,
        address: swap.deposit_vault_address.clone(),
        observed_at: utc::now(),
        participant_auth: None,
    };

    let response = client
        .post(deposit_observation_url(fixture.otc_port, swap.id))
        .header("X-API-ID", TEST_DETECTOR_API_ID)
        .header("X-API-SECRET", TEST_DETECTOR_API_SECRET)
        .json(&request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let accepted: DepositObservationAcceptedResponse = response.json().await.unwrap();
    assert_eq!(accepted.result, "accepted");
    assert_eq!(accepted.swap_id, swap.id);
    assert_eq!(accepted.status, "waiting_user_deposit_confirmed");

    let swap = wait_for_swap_statuses(
        &client,
        fixture.otc_port,
        swap.id,
        &[
            "WaitingUserDepositConfirmed",
            "WaitingMMDepositInitiated",
            "WaitingMMDepositConfirmed",
            "Settled",
        ],
    )
    .await;
    assert!(matches!(
        swap.status,
        SwapStatus::WaitingUserDepositConfirmed
            | SwapStatus::WaitingMMDepositInitiated
            | SwapStatus::WaitingMMDepositConfirmed
            | SwapStatus::Settled
    ));
    assert_eq!(swap.user_deposit_status.as_ref().unwrap().tx_hash, tx_hash);
    if matches!(
        swap.status,
        SwapStatus::WaitingMMDepositInitiated
            | SwapStatus::WaitingMMDepositConfirmed
            | SwapStatus::Settled
    ) {
        assert!(swap.mm_notified_at.is_some());
    }

    fixture.shutdown().await;
}

#[sqlx::test]
async fn participant_signed_route_accepts_bitcoin_deposit(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let mut fixture =
        setup_external_detection_fixture(connect_options, false, ChainType::Ethereum).await;
    let client = reqwest::Client::new();

    let swap = request_and_create_btc_to_eth_swap(
        &client,
        fixture.otc_port,
        fixture.rfq_port,
        &fixture.devnet,
        &fixture.user_account,
    )
    .await;

    let tx_hash = fixture
        .user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: swap.quote.from.currency.clone(),
                    amount: swap.quote.min_input,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    let (transfer_index, amount) =
        find_bitcoin_output_for_address(&fixture.devnet, &tx_hash, &swap.deposit_vault_address)
            .await;

    let observed_at = utc::now();
    let signed_at = utc::now();
    let signer = PrivateKeySigner::from_slice(&fixture.user_account.secret_bytes).unwrap();
    let signature = sign_participant_detection_payload(
        &signer,
        &swap,
        swap.quote.from.currency.chain,
        &swap.quote.from.currency.token,
        &swap.deposit_vault_address,
        &tx_hash,
        transfer_index,
        amount,
        signed_at,
        1,
    );

    let request = DepositObservationRequest {
        source_chain: swap.quote.from.currency.chain,
        source_token: swap.quote.from.currency.token.clone(),
        tx_hash: tx_hash.clone(),
        amount,
        transfer_index,
        address: swap.deposit_vault_address.clone(),
        observed_at,
        participant_auth: Some(ParticipantAuth {
            kind: ParticipantAuthKind::ParticipantEip712,
            signer: fixture.user_account.ethereum_address.to_string(),
            signer_chain: ChainType::Ethereum,
            signature,
            signed_at,
        }),
    };

    let response = client
        .post(deposit_observation_url(fixture.otc_port, swap.id))
        .json(&request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let accepted: DepositObservationAcceptedResponse = response.json().await.unwrap();
    assert_eq!(accepted.result, "accepted");
    assert_eq!(accepted.swap_id, swap.id);
    assert_eq!(accepted.status, "waiting_user_deposit_confirmed");

    let swap = wait_for_swap_statuses(
        &client,
        fixture.otc_port,
        swap.id,
        &[
            "WaitingUserDepositConfirmed",
            "WaitingMMDepositInitiated",
            "WaitingMMDepositConfirmed",
            "Settled",
        ],
    )
    .await;
    assert!(matches!(
        swap.status,
        SwapStatus::WaitingUserDepositConfirmed
            | SwapStatus::WaitingMMDepositInitiated
            | SwapStatus::WaitingMMDepositConfirmed
            | SwapStatus::Settled
    ));
    assert_eq!(swap.user_deposit_status.as_ref().unwrap().tx_hash, tx_hash);
    if matches!(
        swap.status,
        SwapStatus::WaitingMMDepositInitiated
            | SwapStatus::WaitingMMDepositConfirmed
            | SwapStatus::Settled
    ) {
        assert!(swap.mm_notified_at.is_some());
    }

    fixture.shutdown().await;
}

#[sqlx::test]
async fn participant_signed_route_rejects_mismatched_transfer_binding(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let mut fixture =
        setup_external_detection_fixture(connect_options, false, ChainType::Ethereum).await;
    let client = reqwest::Client::new();

    let swap = request_and_create_btc_to_eth_swap(
        &client,
        fixture.otc_port,
        fixture.rfq_port,
        &fixture.devnet,
        &fixture.user_account,
    )
    .await;

    let tx_hash = fixture
        .user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: swap.quote.from.currency.clone(),
                    amount: swap.quote.min_input,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    let (transfer_index, amount) =
        find_bitcoin_output_for_address(&fixture.devnet, &tx_hash, &swap.deposit_vault_address)
            .await;

    let signed_at = utc::now();
    let signer = PrivateKeySigner::from_slice(&fixture.user_account.secret_bytes).unwrap();
    let signature = sign_participant_detection_payload(
        &signer,
        &swap,
        swap.quote.from.currency.chain,
        &swap.quote.from.currency.token,
        &swap.deposit_vault_address,
        &tx_hash,
        transfer_index.saturating_add(1),
        amount,
        signed_at,
        1,
    );

    let request = DepositObservationRequest {
        source_chain: swap.quote.from.currency.chain,
        source_token: swap.quote.from.currency.token.clone(),
        tx_hash,
        amount,
        transfer_index,
        address: swap.deposit_vault_address.clone(),
        observed_at: utc::now(),
        participant_auth: Some(ParticipantAuth {
            kind: ParticipantAuthKind::ParticipantEip712,
            signer: fixture.user_account.ethereum_address.to_string(),
            signer_chain: ChainType::Ethereum,
            signature,
            signed_at,
        }),
    };

    let response = client
        .post(deposit_observation_url(fixture.otc_port, swap.id))
        .json(&request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let error: otc_server::api::DepositObservationErrorResponse = response.json().await.unwrap();
    assert_eq!(error.error.code, "participant_signature_invalid");

    fixture.shutdown().await;
}

#[sqlx::test]
async fn sauron_detects_bitcoin_deposit_end_to_end(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let mut fixture =
        setup_external_detection_fixture(connect_options, true, ChainType::Ethereum).await;
    let client = reqwest::Client::new();

    let swap = request_and_create_btc_to_eth_swap(
        &client,
        fixture.otc_port,
        fixture.rfq_port,
        &fixture.devnet,
        &fixture.user_account,
    )
    .await;

    let tx_hash = fixture
        .user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: swap.quote.from.currency.clone(),
                    amount: swap.quote.min_input,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    fixture.devnet.bitcoin.mine_blocks(1).await.unwrap();
    fixture
        .devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let swap = wait_for_swap_statuses(
        &client,
        fixture.otc_port,
        swap.id,
        &[
            "WaitingUserDepositConfirmed",
            "WaitingMMDepositInitiated",
            "WaitingMMDepositConfirmed",
            "Settled",
        ],
    )
    .await;
    assert!(matches!(
        swap.status,
        SwapStatus::WaitingUserDepositConfirmed
            | SwapStatus::WaitingMMDepositInitiated
            | SwapStatus::WaitingMMDepositConfirmed
            | SwapStatus::Settled
    ));
    assert_eq!(swap.user_deposit_status.as_ref().unwrap().tx_hash, tx_hash);

    fixture.shutdown().await;
}

#[sqlx::test]
async fn sauron_detects_ethereum_deposit_end_to_end(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let mut fixture =
        setup_external_detection_fixture(connect_options, true, ChainType::Ethereum).await;
    let client = reqwest::Client::new();
    let (mut user_wallet_join_set, user_ethereum_wallet) =
        build_test_user_ethereum_wallet(&fixture.devnet, &fixture.user_account).await;

    fixture
        .devnet
        .ethereum
        .fund_eth_address(
            fixture.user_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000u128),
        )
        .await
        .unwrap();
    fixture
        .devnet
        .ethereum
        .mint_cbbtc(
            fixture.user_account.ethereum_address,
            U256::from(9_000_000_000u64),
        )
        .await
        .unwrap();
    user_ethereum_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&fixture.user_account.secret_bytes).unwrap(),
        )
        .await
        .unwrap();

    let swap = request_and_create_evm_to_btc_swap(
        &client,
        fixture.otc_port,
        fixture.rfq_port,
        &fixture.devnet,
        &fixture.user_account,
        ChainType::Ethereum,
    )
    .await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let tx_hash = send_evm_deposit(
        &user_ethereum_wallet,
        ChainType::Ethereum,
        &swap,
        fixture.devnet.ethereum.cbbtc_contract.address().to_string(),
    )
    .await;

    fixture
        .devnet
        .ethereum
        .funded_provider
        .anvil_mine(Some(1), None)
        .await
        .unwrap();

    let swap = wait_for_swap_statuses(
        &client,
        fixture.otc_port,
        swap.id,
        &[
            "WaitingUserDepositConfirmed",
            "WaitingMMDepositInitiated",
            "WaitingMMDepositConfirmed",
            "Settled",
        ],
    )
    .await;
    assert!(matches!(
        swap.status,
        SwapStatus::WaitingUserDepositConfirmed
            | SwapStatus::WaitingMMDepositInitiated
            | SwapStatus::WaitingMMDepositConfirmed
            | SwapStatus::Settled
    ));
    assert_eq!(swap.user_deposit_status.as_ref().unwrap().tx_hash, tx_hash);

    user_wallet_join_set.shutdown().await;
    fixture.shutdown().await;
}

#[sqlx::test]
async fn sauron_detects_base_deposit_end_to_end(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let mut fixture =
        setup_external_detection_fixture(connect_options, true, ChainType::Base).await;
    let client = reqwest::Client::new();
    let (mut user_wallet_join_set, user_base_wallet) =
        build_test_user_base_wallet(&fixture.devnet, &fixture.user_account).await;

    fixture
        .devnet
        .base
        .fund_eth_address(
            fixture.user_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000u128),
        )
        .await
        .unwrap();
    fixture
        .devnet
        .base
        .mint_cbbtc(
            fixture.user_account.ethereum_address,
            U256::from(9_000_000_000u64),
        )
        .await
        .unwrap();
    user_base_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&fixture.user_account.secret_bytes).unwrap(),
        )
        .await
        .unwrap();

    let swap = request_and_create_evm_to_btc_swap(
        &client,
        fixture.otc_port,
        fixture.rfq_port,
        &fixture.devnet,
        &fixture.user_account,
        ChainType::Base,
    )
    .await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let tx_hash = send_evm_deposit(
        &user_base_wallet,
        ChainType::Base,
        &swap,
        fixture.devnet.base.cbbtc_contract.address().to_string(),
    )
    .await;

    fixture
        .devnet
        .base
        .funded_provider
        .anvil_mine(Some(1), None)
        .await
        .unwrap();

    let swap = wait_for_swap_statuses(
        &client,
        fixture.otc_port,
        swap.id,
        &[
            "WaitingUserDepositConfirmed",
            "WaitingMMDepositInitiated",
            "WaitingMMDepositConfirmed",
            "Settled",
        ],
    )
    .await;
    assert!(matches!(
        swap.status,
        SwapStatus::WaitingUserDepositConfirmed
            | SwapStatus::WaitingMMDepositInitiated
            | SwapStatus::WaitingMMDepositConfirmed
            | SwapStatus::Settled
    ));
    assert_eq!(swap.user_deposit_status.as_ref().unwrap().tx_hash, tx_hash);

    user_wallet_join_set.shutdown().await;
    fixture.shutdown().await;
}

async fn setup_external_detection_fixture(
    connect_options: PgConnectOptions,
    auto_start_sauron: bool,
    mm_evm_chain: ChainType,
) -> ExternalDetectionFixture {
    let market_maker_account = MultichainAccount::new(101);
    let user_account = MultichainAccount::new(102);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .bitcoin_mining_mode(MiningMode::Manual)
        .build()
        .await
        .unwrap()
        .0;

    let mut wallet_join_set = JoinSet::new();
    let user_bitcoin_wallet = BitcoinWallet::new(
        &build_tmp_bitcoin_wallet_db_file(),
        &build_bitcoin_wallet_descriptor(&user_account.bitcoin_wallet.private_key),
        bitcoin::Network::Regtest,
        &devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        None,
        None,
        100,
        &mut wallet_join_set,
    )
    .await
    .unwrap();

    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000),
        )
        .await
        .unwrap();
    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000),
        )
        .await
        .unwrap();
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000u128),
        )
        .await
        .unwrap();
    devnet
        .ethereum
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(9_000_000_000u64),
        )
        .await
        .unwrap();
    devnet
        .base
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000u128),
        )
        .await
        .unwrap();
    devnet
        .base
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(9_000_000_000u64),
        )
        .await
        .unwrap();
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let otc_port = get_free_port().await;
    let mut otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;
    if !auto_start_sauron {
        disable_auto_sauron_for_port(otc_port);
    }

    let mut service_join_set = JoinSet::new();
    service_join_set.spawn(async move {
        run_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });
    wait_for_otc_server_to_be_ready(otc_port).await;

    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    let mm_args = match mm_evm_chain {
        ChainType::Base => {
            build_mm_test_args_for_base(
                otc_port,
                rfq_port,
                &market_maker_account,
                &devnet,
                &connect_options,
            )
            .await
        }
        ChainType::Ethereum => {
            build_mm_test_args(
                otc_port,
                rfq_port,
                &market_maker_account,
                &devnet,
                &connect_options,
            )
            .await
        }
        other => panic!("unsupported MM EVM chain for fixture: {other:?}"),
    };
    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });
    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    ExternalDetectionFixture {
        devnet,
        service_join_set,
        wallet_join_set,
        user_bitcoin_wallet,
        user_account,
        otc_port,
        rfq_port,
    }
}

async fn request_and_create_btc_to_eth_swap(
    client: &reqwest::Client,
    otc_port: u16,
    rfq_port: u16,
    devnet: &RiftDevnet,
    user_account: &MultichainAccount,
) -> Swap {
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(10_000_000),
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::address(devnet.ethereum.cbbtc_contract.address().to_string()),
            decimals: 8,
        },
        affiliate: None,
    };

    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v2/quote"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();
    assert_eq!(quote_response.status(), StatusCode::OK);
    let quote_response: rfq_server::server::QuoteResponse = quote_response.json().await.unwrap();
    let quote = match quote_response.quote.unwrap() {
        RFQResult::Success(quote) => quote,
        other => panic!("expected successful quote, got {other:?}"),
    };

    let swap_request = CreateSwapRequest {
        quote,
        user_destination_address: user_account.ethereum_address.to_string(),
        refund_address: user_account.bitcoin_wallet.address.to_string(),
        metadata: None,
    };
    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let swap: Swap = response.json().await.unwrap();
    info!("Created swap {}", swap.id);
    swap
}

async fn request_and_create_evm_to_btc_swap(
    client: &reqwest::Client,
    otc_port: u16,
    rfq_port: u16,
    devnet: &RiftDevnet,
    user_account: &MultichainAccount,
    source_chain: ChainType,
) -> Swap {
    let source_token = match source_chain {
        ChainType::Ethereum => devnet.ethereum.cbbtc_contract.address().to_string(),
        ChainType::Base => devnet.base.cbbtc_contract.address().to_string(),
        other => panic!("unsupported source chain for EVM deposit test: {other:?}"),
    };

    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(100_000_000),
        from: Currency {
            chain: source_chain,
            token: TokenIdentifier::address(source_token),
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        affiliate: None,
    };

    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v2/quote"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();
    assert_eq!(quote_response.status(), StatusCode::OK);
    let quote_response: rfq_server::server::QuoteResponse = quote_response.json().await.unwrap();
    let quote = match quote_response.quote.unwrap() {
        RFQResult::Success(quote) => quote,
        other => panic!("expected successful quote, got {other:?}"),
    };

    let swap_request = CreateSwapRequest {
        quote,
        user_destination_address: user_account.bitcoin_wallet.address.to_string(),
        refund_address: user_account.ethereum_address.to_string(),
        metadata: None,
    };
    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let swap: Swap = response.json().await.unwrap();
    info!("Created swap {}", swap.id);
    swap
}

async fn find_bitcoin_output_for_address(
    devnet: &RiftDevnet,
    tx_hash: &str,
    deposit_address: &str,
) -> (u64, U256) {
    let txid = tx_hash.parse().unwrap();
    let verbose = devnet
        .bitcoin
        .rpc_client
        .get_raw_transaction_verbose(&txid)
        .await
        .unwrap();
    let raw = hex::decode(verbose.hex).unwrap();
    let tx: bitcoin::Transaction = deserialize(&raw).unwrap();

    for (index, output) in tx.output.iter().enumerate() {
        let output_address =
            bitcoin::Address::from_script(&output.script_pubkey, bitcoin::Network::Regtest)
                .unwrap()
                .to_string();
        if output_address == deposit_address {
            return (index as u64, U256::from(output.value.to_sat()));
        }
    }

    panic!("failed to find deposit output for address {deposit_address}");
}

fn deposit_observation_url(port: u16, swap_id: Uuid) -> String {
    format!("http://localhost:{port}/api/v1/swaps/{swap_id}/deposit-observation")
}

async fn send_evm_deposit(
    wallet: &EVMWallet,
    source_chain: ChainType,
    swap: &Swap,
    token_address: String,
) -> String {
    wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: source_chain,
                        token: TokenIdentifier::address(token_address),
                        decimals: swap.quote.from.currency.decimals,
                    },
                    amount: swap.quote.min_input,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap()
}

fn sign_participant_detection_payload(
    signer: &PrivateKeySigner,
    swap: &Swap,
    source_chain: ChainType,
    source_token: &TokenIdentifier,
    deposit_address: &str,
    tx_hash: &str,
    transfer_index: u64,
    amount: U256,
    signed_at: chrono::DateTime<chrono::Utc>,
    signer_chain_id: u64,
) -> String {
    let payload = ParticipantDepositDetectionAuthPayload {
        swapId: swap.id.to_string(),
        sourceChain: canonical_chain(source_chain).to_string(),
        sourceToken: canonical_token_identifier(source_token),
        depositAddress: canonical_address(source_chain, deposit_address),
        txHash: tx_hash.to_lowercase(),
        transferIndex: transfer_index,
        amount,
        signedAt: signed_at.timestamp() as u64,
    };
    let domain = eip712_domain! {
        name: "Rift OTC Deposit Detection",
        version: "1",
        chain_id: signer_chain_id,
    };
    signer
        .sign_typed_data_sync(&payload, &domain)
        .unwrap()
        .to_string()
}

fn canonical_chain(chain: ChainType) -> &'static str {
    match chain {
        ChainType::Bitcoin => "bitcoin",
        ChainType::Ethereum => "ethereum",
        ChainType::Base => "base",
    }
}

fn canonical_token_identifier(token: &TokenIdentifier) -> String {
    match token.normalize() {
        TokenIdentifier::Native => "native".to_string(),
        TokenIdentifier::Address(address) => address,
    }
}

fn canonical_address(chain: ChainType, address: &str) -> String {
    match chain {
        ChainType::Bitcoin => address.to_string(),
        ChainType::Ethereum | ChainType::Base => address.to_lowercase(),
    }
}
