//! WebSocket server infrastructure for market maker connections
//!
//! This crate provides reusable WebSocket connection management for servers
//! that need to communicate with market makers over WebSocket connections.
//!
//! # Architecture
//!
//! - `MessageHandler` trait: Implement this to handle incoming messages
//! - `AuthValidator` trait: Implement this to validate API credentials
//! - `handle_mm_connection`: Core connection lifecycle management
//!
//! The crate handles:
//! - WebSocket connection lifecycle
//! - Background task management (message forwarding)
//! - Graceful cleanup on disconnect
//! - Connection hooks (on_connect, on_disconnect)

pub mod auth;
pub mod connection;
pub mod handler;

pub use auth::{extract_auth_headers, AuthError, AuthValidator};
pub use connection::handle_mm_connection;
pub use handler::{MessageHandler, MessageSender};

/// Re-export common types
pub use axum::extract::ws::{Message, WebSocket};
pub use uuid::Uuid;

