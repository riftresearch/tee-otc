use crate::traits::MarketMakerPaymentValidation;
use crate::{key_derivation, ChainOperations, Result};
use alloy::hex;
use alloy::primitives::U256;
use async_trait::async_trait;
use bdk_wallet::{signer::SignOptions, CreateParams, Wallet as BdkWallet};
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::{Address, Amount, CompressedPublicKey, Network, OutPoint, PrivateKey, Transaction};
use bitcoincore_rpc_async::{Auth, Client, RpcApi};
use otc_models::{ChainType, Lot, TokenIdentifier, TransferInfo, TxStatus, Wallet};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info};

pub struct BitcoinChain {
    rpc_client: Client,
    esplora_client: esplora_client::AsyncClient,
    network: Network,
}

impl BitcoinChain {
    /// Auth (if necessary) should be embedded in the bitcoin_core_rpc_url
    pub async fn new(
        bitcoin_core_rpc_url: &str,
        bitcoin_core_rpc_auth: Auth,
        esplora_url: &str,
        network: Network,
    ) -> Result<Self> {
        let rpc_client = Client::new(bitcoin_core_rpc_url.to_string(), bitcoin_core_rpc_auth)
            .await
            .map_err(|_| crate::Error::Rpc {
                message: "Failed to create Bitcoin RPC client".to_string(),
            })?;

        let esplora_client = esplora_client::Builder::new(esplora_url)
            .build_async()
            .map_err(|_| crate::Error::Rpc {
                message: "Failed to create Esplora client".to_string(),
            })?;

        Ok(Self {
            rpc_client,
            esplora_client,
            network,
        })
    }
}

#[async_trait]
impl ChainOperations for BitcoinChain {
    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])> {
        // Generate a random salt
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt).map_err(|_| crate::Error::Serialization {
            message: "Failed to generate random salt".to_string(),
        })?;

        // Generate a new private key
        let secp = Secp256k1::new();
        let secret_key = bitcoin::secp256k1::SecretKey::from_slice(&salt).unwrap();
        let private_key = PrivateKey::new(secret_key, self.network);

        // Derive public key and address
        let compressed_pk = CompressedPublicKey::from_private_key(&secp, &private_key).unwrap();
        let address = Address::p2wpkh(&compressed_pk, self.network);

        info!("Created new Bitcoin wallet: {}", address);

        let wallet = Wallet::new(address.to_string(), private_key.to_wif());
        Ok((wallet, salt))
    }

    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet> {
        // Derive private key using HKDF
        let private_key_bytes =
            key_derivation::derive_private_key(master_key, salt, b"bitcoin-wallet")?;

        // Create secp256k1 secret key
        let secret_key =
            SecretKey::from_slice(&private_key_bytes).map_err(|_| crate::Error::Serialization {
                message: "Failed to create secret key from derived bytes".to_string(),
            })?;

        let private_key = PrivateKey::new(secret_key, self.network);

        // Derive public key and address
        let secp = Secp256k1::new();
        let compressed_pk = CompressedPublicKey::from_private_key(&secp, &private_key).unwrap();
        let address = Address::p2wpkh(&compressed_pk, self.network);

        debug!("Derived Bitcoin wallet: {}", address);

        Ok(Wallet::new(address.to_string(), private_key.to_wif()))
    }

    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let tx = self
            .rpc_client
            .get_raw_transaction_verbose(&bitcoin::Txid::from_str(tx_hash).unwrap())
            .await?;
        if tx.confirmations.unwrap_or(0) > 0 {
            Ok(TxStatus::Confirmed(tx.confirmations.unwrap_or(0)))
        } else {
            Ok(TxStatus::NotFound)
        }
    }

    async fn dump_to_address(
        &self,
        token: &TokenIdentifier,
        private_key: &str,
        recipient_address: &str,
        fee: U256,
    ) -> Result<String> {
        if token != &TokenIdentifier::Native {
            return Err(crate::Error::DumpToAddress {
                message: "Native token not supported".to_string(),
            });
        }
        let private_key =
            PrivateKey::from_wif(private_key).map_err(|e| crate::Error::DumpToAddress {
                message: "Invalid signer private key".to_string(),
            })?;
        let recipient_address = Address::from_str(recipient_address)?.assume_checked();

        // Determine the sender address from the provided private key and collect its UTXOs
        let secp = Secp256k1::new();
        let sender_address = Address::p2wpkh(
            &CompressedPublicKey::from_private_key(&secp, &private_key).unwrap(),
            self.network,
        );

        let utxos = self
            .esplora_client
            .get_address_utxo(&sender_address)
            .await?;
        if utxos.is_empty() {
            return Err(crate::Error::DumpToAddress {
                message: "No UTXOs found".to_string(),
            });
        }
        if utxos.iter().map(|utxo| utxo.value).sum::<u64>() < fee.to::<u64>() {
            return Err(crate::Error::DumpToAddress {
                message: "Insufficient balance to cover fee".to_string(),
            });
        }
        // Calculate totals
        let total_in: u64 = utxos.iter().map(|u| u.value).sum();
        let fee_sats: u64 = fee.to::<u64>();
        if total_in <= fee_sats {
            return Err(crate::Error::DumpToAddress {
                message: format!(
                    "Insufficient balance: inputs {total_in} sats, fee {fee_sats} sats"
                ),
            });
        }

        let send_amount = total_in - fee_sats;

        // Build a descriptor from the provided WIF — our addresses are P2WPKH
        let descriptor = format!("wpkh({})", private_key.to_wif());

        // Create a temporary in-memory BDK wallet for signing and building
        let mut temp_wallet = BdkWallet::create_with_params(
            CreateParams::new_single(descriptor).network(self.network),
        )
        .map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to create temp wallet: {e}"),
        })?;

        let mut tx_builder = temp_wallet.build_tx();
        tx_builder.manually_selected_only();
        tx_builder.add_recipient(
            recipient_address.script_pubkey(),
            Amount::from_sat(send_amount),
        );
        tx_builder.fee_absolute(Amount::from_sat(fee_sats));

        // Add inputs as foreign UTXOs with full PSBT metadata for reliability
        for utxo in utxos {
            let tx_hex = self
                .rpc_client
                .get_raw_transaction_hex(&utxo.txid, None)
                .await
                .map_err(|e| crate::Error::DumpToAddress {
                    message: format!(
                        "Failed to fetch raw transaction for {}: {e}",
                        utxo.txid
                    ),
                })?;

            let tx_bytes = alloy::hex::decode(&tx_hex).map_err(|e| crate::Error::DumpToAddress {
                message: format!(
                    "Failed to decode raw transaction hex for {}: {e}",
                    utxo.txid
                ),
            })?;

            let full_tx = bitcoin::consensus::deserialize::<Transaction>(&tx_bytes).map_err(
                |e| crate::Error::DumpToAddress {
                    message: format!(
                        "Failed to deserialize raw transaction for {}: {e}",
                        utxo.txid
                    ),
                },
            )?;

            let output = full_tx
                .output
                .get(utxo.vout as usize)
                .cloned()
                .ok_or_else(|| crate::Error::DumpToAddress {
                    message: format!(
                        "Transaction {} missing vout {} for foreign UTXO",
                        utxo.txid, utxo.vout
                    ),
                })?;

            let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                witness_utxo: Some(output),
                non_witness_utxo: Some(full_tx.clone()),
                ..Default::default()
            };

            let satisfaction_weight = bdk_wallet::bitcoin::Weight::from_wu(108);

            tx_builder
                .add_foreign_utxo(
                    OutPoint::new(utxo.txid, utxo.vout),
                    psbt_input,
                    satisfaction_weight,
                )
                .map_err(|e| crate::Error::DumpToAddress {
                    message: format!(
                        "Failed to add foreign UTXO {}:{}: {e}",
                        utxo.txid, utxo.vout
                    ),
                })?;
        }

        let mut psbt = tx_builder
            .finish()
            .map_err(|e| crate::Error::DumpToAddress {
                message: format!("Failed to build transaction: {e}"),
            })?;

        // Sign with the temporary wallet
        let finalized = temp_wallet
            .sign(&mut psbt, SignOptions::default())
            .map_err(|e| crate::Error::DumpToAddress {
                message: format!("Failed to sign PSBT: {e}"),
            })?;

        if !finalized {
            return Err(crate::Error::DumpToAddress {
                message: "PSBT not fully finalized after signing".to_string(),
            });
        }

        let tx = psbt.extract_tx().map_err(|e| crate::Error::DumpToAddress {
            message: format!("Failed to extract transaction: {e}"),
        })?;

        // Return raw signed transaction hex
        let raw = bitcoin::consensus::serialize(&tx);
        Ok(hex::encode(raw))
    }

    async fn search_for_transfer(
        &self,
        address: &str,
        lot: &Lot,
        mm_payment: Option<MarketMakerPaymentValidation>,
        _from_block_height: Option<u64>,
    ) -> Result<Option<TransferInfo>> {
        info!("Searching for transfer");
        let span = tracing::span!(
            tracing::Level::DEBUG,
            "search_for_transfer",
            address = address,
            lot = format!("{:?}", lot),
            mm_payment = format!("{:?}", mm_payment)
        );
        let _enter = span.enter();

        if !matches!(lot.currency.chain, ChainType::Bitcoin)
            || !matches!(lot.currency.token, otc_models::TokenIdentifier::Native)
        {
            return Err(crate::Error::InvalidCurrency {
                lot: lot.clone(),
                network: ChainType::Bitcoin,
            });
        }
        let address = bitcoin::Address::from_str(address)?.assume_checked();
        let transfer_opt = self
            .get_transfer_hint(address.to_string().as_str(), &lot.amount, mm_payment)
            .await?;
        debug!("Potential transfer: {:?}", transfer_opt);
        Ok(transfer_opt)
    }

    fn validate_address(&self, address: &str) -> bool {
        match Address::from_str(address) {
            Ok(addr) => addr.is_valid_for_network(self.network),
            Err(_) => false,
        }
    }

    fn minimum_block_confirmations(&self) -> u32 {
        2
    }

    fn estimated_block_time(&self) -> Duration {
        Duration::from_secs(600) // 10 minutes
    }
}

impl BitcoinChain {
    // The output of this function can be trusted as we validate the transfer hint against the rpc client
    async fn get_transfer_hint(
        &self,
        address: &str,
        amount: &U256,
        mm_payment: Option<MarketMakerPaymentValidation>,
    ) -> Result<Option<TransferInfo>> {
        let address = bitcoin::Address::from_str(address)?.assume_checked();

        // Called a hint b/c the esplora client CANNOT be trusted to return non-fradulent data (b/c it not intended to run locally)
        // Note that if there are more than 50 utxos available to the address, this could ignore a valid transfer (TODO: how to handle this?)
        let utxos = self.esplora_client.get_address_utxo(&address).await?;
        debug!("UTXOs: {:?}", utxos);
        let current_block_height = self.rpc_client.get_block_count().await? as u32;
        let mut most_confirmed_transfer: Option<TransferInfo> = None;
        for utxo in utxos {
            if utxo.value < amount.to::<u64>() {
                continue;
            }
            // TODO: the height of the utxo should be validated against the rpc client
            let cur_utxo_confirmations =
                current_block_height - utxo.status.block_height.unwrap_or(current_block_height);
            if most_confirmed_transfer.is_some()
                && (most_confirmed_transfer.as_ref().unwrap().confirmations
                    > cur_utxo_confirmations as u64)
            {
                // if we already have a candidate let's do the cheap check to see if it's better confirmations wise before we fully validate it
                // before we download the full tx
                continue;
            }
            // At this point, we either have a new candidate that's more confirmed than the current candidate
            // as let's finally validate that it's the correct transfer
            if let Some(mm_payment) = &mm_payment {
                // we only need to do this check if the embedded nonce is a requirement
                let embedded_nonce = mm_payment.embedded_nonce;
                // TODO: Use rpc client instead of esplora so we dont have to implement validate logic twice
                let tx_hex = self
                    .rpc_client
                    .get_raw_transaction_hex(&utxo.txid, None)
                    .await;

                if tx_hex.is_err() {
                    info!(
                        message = "Failed to get raw transaction, skipping",
                        tx_hash = utxo.txid.to_string()
                    );
                    continue;
                }
                let tx_hex = tx_hex.unwrap();
                let tx_bytes = hex::decode(&tx_hex);
                if tx_bytes.is_err() {
                    info!(
                        message = "Failed to decode raw transaction, skipping",
                        tx_hash = utxo.txid.to_string()
                    );
                    continue;
                }
                let tx_bytes = tx_bytes.unwrap();
                let tx = bitcoin::consensus::deserialize::<Transaction>(&tx_bytes).unwrap();

                // Each tx can only have one of the following prefixed script pubkeys
                // [OP_RETURN (0x6a) + OP_PUSHBYTES_16 (0x10)]
                if tx
                    .output
                    .iter()
                    .filter(|output| output.script_pubkey.to_bytes().starts_with(&[0x6a, 0x10]))
                    .count()
                    != 1
                {
                    // Either not a mm payment OR invalid payment that has multiple OP_RETURN outputs
                    info!(
                        message = "Invalid mm payment, either not a mm payment or invalid payment that has multiple OP_RETURN outputs",
                        tx_hash = utxo.txid.to_string()
                    );
                    continue;
                }

                let mut needle = vec![0x6a, 0x10];
                needle.extend_from_slice(&embedded_nonce);

                if !tx
                    .output
                    .iter()
                    .any(|output| output.script_pubkey.to_bytes() == needle)
                {
                    // The embedded nonce is not in the OP_RETURN output
                    info!(
                        message =
                            "Invalid mm payment, embedded nonce is not in the OP_RETURN output",
                        tx_hash = utxo.txid.to_string()
                    );
                    continue;
                }
                // finally validate fee
                let fee = mm_payment.fee_amount;
                let fee_address =
                    Address::from_str(&otc_models::FEE_ADDRESSES_BY_CHAIN[&ChainType::Bitcoin])?
                        .assume_checked();
                if !tx.output.iter().any(|output| {
                    output.script_pubkey == fee_address.script_pubkey()
                        && output.value >= Amount::from_sat(fee.to::<u64>())
                }) {
                    // The fee is not in the OP_RETURN output
                    info!(
                        message = "Invalid mm payment, invalid fee amount or fee address",
                        tx_hash = utxo.txid.to_string()
                    );
                    continue;
                }
            }
            // At this point, our new candidate is valid and the most confirmed transfer we've seen
            // so let's return it
            most_confirmed_transfer = Some(TransferInfo {
                tx_hash: utxo.txid.to_string(),
                amount: U256::from(utxo.value),
                detected_at: utc::now(),
                confirmations: cur_utxo_confirmations as u64,
            });
        }
        Ok(most_confirmed_transfer)
    }
}
