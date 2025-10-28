//! Integration tests package lib
#![allow(dead_code)]
#![allow(warnings)]
#![allow(clippy::all)]

mod utils;

#[cfg(test)]
mod market_maker_otc_auth_test;

#[cfg(test)]
mod simple_swap_test;

#[cfg(test)]
mod indexer_client_test;

#[cfg(test)]
mod evm_wallet_test;

#[cfg(test)]
mod bitcoin_wallet_test;

#[cfg(test)]
mod rfq_flow_test;

#[cfg(test)]
mod price_oracle_test;

#[cfg(test)]
mod quote_storage_test;

#[cfg(test)]
mod deposit_key_vault_test;

#[cfg(test)]
mod refund_test;

#[cfg(test)]
mod metrics_export_test;

#[cfg(test)]
mod timeout_triggers_cancel_test;

#[cfg(test)]
mod liquidity_endpoint_test;

#[cfg(test)]
mod rfq_reconnection_test;

#[cfg(test)]
mod connection_reset_test;
