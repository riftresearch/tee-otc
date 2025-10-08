use alloy::{
    dyn_abi::{DynSolType, DynSolValue},
    primitives::{keccak256, Address, U256},
    signers::Signature,
};
use chrono::{DateTime, Utc};
use otc_models::{ChainType, Metadata, Quote, RefundSwapReason};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHashResponse {
    pub block_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundPayload {
    pub swap_id: Uuid,
    pub refund_recipient: String,
    pub refund_transaction_fee: U256,
}

/// Request to refund a swap
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RefundSwapRequest {
    /// The "message" part of the EIP712 typed data
    pub payload: RefundPayload,

    /// The signature of the full EIP712 typed data
    pub signature: Vec<u8>,
}

pub static RIFT_DOMAIN_TYPE: LazyLock<DynSolType> = LazyLock::new(|| {
    DynSolType::Tuple(vec![
        DynSolType::String,    // name
        DynSolType::String,    // version
        DynSolType::Uint(256), // chainId
        DynSolType::Address,   // verifyingContract
    ])
});

pub static RIFT_DOMAIN_VALUE: LazyLock<DynSolValue> = LazyLock::new(|| {
    DynSolValue::Tuple(vec![
        DynSolValue::String("Rift OTC".to_string()),
        DynSolValue::String("1.0.0".to_string()),
        DynSolValue::Uint(U256::from(1), 256),
        DynSolValue::Address(Address::from([0x42; 20])),
    ])
});

impl RefundPayload {
    pub fn get_signer_address_from_signature(&self, signature: &[u8]) -> Result<Address, String> {
        let message_value = DynSolValue::Tuple(vec![
            DynSolValue::String(self.swap_id.to_string()),
            DynSolValue::String(self.refund_recipient.to_string()),
            DynSolValue::Uint(self.refund_transaction_fee, 256),
        ]);

        let encoded_domain = RIFT_DOMAIN_VALUE.abi_encode();
        let encoded_message = message_value.abi_encode();

        let domain_separator = keccak256(&encoded_domain);
        let message_hash = keccak256(&encoded_message);
        let eip712_hash =
            keccak256([&[0x19, 0x01], &domain_separator[..], &message_hash[..]].concat());
        let recovered_address = Signature::from_raw(signature)
            .map_err(|e| format!("Passed signature could not be parsed: {}", e))?
            .recover_address_from_prehash(&eip712_hash)
            .map_err(|e| format!("Bad signature for prehash: {}", e))?;

        Ok(recovered_address)
    }
}

/// Response after refunding a swap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundSwapResponse {
    /// The refunded swap ID
    pub swap_id: Uuid,
    /// The reason the refund was allowed
    pub reason: RefundSwapReason,
    /// The "ready to be broadcast" signed transaction data as a hex string
    pub tx_data: String,
    /// The chain the transaction needs to be broadcast on
    pub tx_chain: ChainType,
}

/// Request to create a new swap from a quote
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateSwapRequest {
    /// The quote ID to create a swap from
    pub quote: Quote,

    /// User's destination address for receiving funds
    pub user_destination_address: String,

    /// User's EVM account that is authorized to control the swap
    pub user_evm_account_address: Address,

    /// Optional metadata describing the swap source
    #[serde(default)]
    pub metadata: Option<Metadata>,
}

/// Response after successfully creating a swap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSwapResponse {
    /// The newly created swap ID
    pub swap_id: Uuid,

    /// Deposit address for the user to send funds to
    pub deposit_address: String,

    /// Chain type for the deposit (Bitcoin/Ethereum)
    pub deposit_chain: String,

    /// Expected amount to deposit (matches quote.from.amount)
    pub expected_amount: U256,

    /// Number of decimals for the amount
    pub decimals: u8,

    /// Token type (Native or token address)
    pub token: String,

    /// When the swap expires (based on quote expiry)
    pub expires_at: DateTime<Utc>,

    /// Current swap status
    pub status: String,
}

/// Response for GET /swaps/:id
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapResponse {
    pub id: Uuid,
    pub quote_id: Uuid,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    /// User's deposit information
    pub user_deposit: DepositInfoResponse,

    /// Market maker's deposit information  
    pub mm_deposit: DepositInfoResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositInfoResponse {
    pub address: String,
    pub chain: String,
    pub expected_amount: U256,
    pub decimals: u8,
    pub token: String,

    /// Actual deposit info if detected
    pub deposit_tx: Option<String>,
    pub deposit_amount: Option<U256>,
    pub deposit_detected_at: Option<DateTime<Utc>>,
}
