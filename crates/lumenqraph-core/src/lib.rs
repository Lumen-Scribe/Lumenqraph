//! Shared types and logic used by every Lumenqraph service. Defining the event
//! schema and decoding once here means services can never drift.

pub mod error;
pub mod models;
pub mod xdr;

pub use error::{Error, Result};
pub use models::{
    ApiKey, Contract, EventRow, IndexerStatus, NewEvent, TokenTransfer, WebhookSubscription,
};
