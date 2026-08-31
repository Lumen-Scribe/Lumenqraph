//! Re-export URL validation from lumenqraph-core for use in webhook registration.
pub use lumenqraph_core::url_validation::{
    validate_webhook_url,
    validate_webhook_url_at_delivery,
};
