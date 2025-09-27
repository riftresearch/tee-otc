use chrono::{DateTime, Utc};
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
    pub timestamp: DateTime<Utc>,
}

impl SwapUpdate {
    pub fn new(index: usize, stage: SwapStage) -> Self {
        Self {
            index,
            stage,
            timestamp: Utc::now(),
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
