use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use otc_models::ChainType;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum UiEvent {
    Swap(SwapUpdate),
    Log(String),
}

#[derive(Clone, Debug)]
pub struct SwapUpdate {
    pub index: usize,
    pub stage: SwapStage,
    pub amount: Option<U256>,
    pub deposit_chain: Option<ChainType>,
    pub sender_address: Option<String>,
    pub timestamp: DateTime<Utc>,
}

impl SwapUpdate {
    pub fn new(index: usize, stage: SwapStage) -> Self {
        Self {
            index,
            stage,
            amount: None,
            deposit_chain: None,
            sender_address: None,
            timestamp: utc::now(),
        }
    }

    pub fn with_amount_and_chain(
        index: usize,
        stage: SwapStage,
        amount: U256,
        deposit_chain: ChainType,
    ) -> Self {
        Self {
            index,
            stage,
            amount: Some(amount),
            deposit_chain: Some(deposit_chain),
            sender_address: None,
            timestamp: utc::now(),
        }
    }

    pub fn with_sender(
        index: usize,
        stage: SwapStage,
        amount: U256,
        deposit_chain: ChainType,
        sender_address: String,
    ) -> Self {
        Self {
            index,
            stage,
            amount: Some(amount),
            deposit_chain: Some(deposit_chain),
            sender_address: Some(sender_address),
            timestamp: utc::now(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum SwapStage {
    QuoteRequested,
    QuoteFailed {
        reason: String,
    },
    QuoteReceived {
        quote_id: Uuid,
    },
    SwapSubmitted {
        swap_id: Uuid,
    },
    PaymentBroadcast {
        swap_id: Uuid,
        tx_hash: String,
    },
    PaymentFailed {
        swap_id: Option<Uuid>,
        reason: String,
    },
    StatusUpdated {
        swap_id: Uuid,
        status: String,
    },
    Settled {
        swap_id: Uuid,
    },
    FinishedWithError {
        swap_id: Option<Uuid>,
        reason: String,
    },
    Shutdown,
}
