//! Library surface of the indexer, used by the integration tests in `tests/`.
//! The binary (`main.rs`) owns the process orchestration and declares the same
//! modules itself; this file only exposes the pieces tests need to drive.

pub mod store;
