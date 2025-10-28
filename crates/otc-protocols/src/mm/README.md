# OTC Market Maker Protocol

This crate defines the protocol for communication between the OTC settlement server and market makers.

## Overview

The protocol is transport-agnostic - this crate only defines message types and serialization. Implementations are responsible for their own networking layer (WebSocket, TCP, QUIC, etc.).

## Message Flow

1. **Quote Validation**: Server asks MM if they'll fill a quote
2. **User Deposit Notification**: Server notifies MM when user has deposited
3. **Swap Completion**: Server provides user's private key after settlement

## Usage

```rust
use otc_mm_protocol::{MMRequest, MMResponse, ProtocolMessage};

// Create a quote validation request
let request = MMRequest::ValidateQuote {
    request_id: Uuid::new_v4(),
    quote_id: quote.id,
    user_id: user.id,
    timestamp: utc::now(),
};

// Wrap in protocol message
let message = ProtocolMessage {
    version: PROTOCOL_VERSION.to_string(),
    sequence: 1,
    payload: request,
};

// Serialize and send over your transport
let json = serde_json::to_string(&message)?;
```

## Message Types

### Requests (Server → MM)

- `ValidateQuote`: Check if MM will fill a quote
- `UserDeposited`: Notify MM of user deposit
- `SwapComplete`: Provide user's private key

### Responses (MM → Server)

- `QuoteValidated`: Accept/reject quote
- `DepositInitiated`: MM has sent funds
- `SwapCompleteAck`: Acknowledge completion
- `Error`: Error response

## Versioning

The protocol uses semantic versioning. Current version: 1.0.0

## Transport Implementation

This crate does not provide networking. For examples:

- WebSocket: Use `tokio-tungstenite` or `tungstenite`
- TCP: Use `tokio::net::TcpStream`
- QUIC: Use `quinn`
