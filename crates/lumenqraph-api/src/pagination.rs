//! Keyset (cursor) pagination utilities for efficient paging through large result sets.
//!
//! Implements Relay-style cursor pagination using (ledger, event_id) tuples.
//! This avoids the O(n) cost of OFFSET pagination on large tables.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;

/// Encode a cursor from (ledger, event_id) position.
pub fn encode_cursor(ledger: i64, event_id: &str) -> String {
    B64.encode(format!("{ledger}|{event_id}"))
}

/// Decode a cursor to (ledger, event_id).
///
/// - `Ok(None)` — no cursor supplied; callers should start from the newest page.
/// - `Ok(Some((ledger, id)))` — valid cursor, decoded successfully.
/// - `Err` — cursor was present but malformed; callers must surface an error to
///   the client rather than silently restarting from page 1.
pub fn decode_cursor(cursor: Option<&str>) -> Result<Option<(i64, String)>, &'static str> {
    let Some(raw) = cursor else { return Ok(None) };
    let bytes = B64.decode(raw).map_err(|_| "invalid cursor")?;
    let s = String::from_utf8(bytes).map_err(|_| "invalid cursor")?;
    let (ledger, id) = s.split_once('|').ok_or("invalid cursor")?;
    let ledger = ledger.parse::<i64>().map_err(|_| "invalid cursor")?;
    Ok(Some((ledger, id.to_string())))
}

/// Configuration for keyset (cursor) pagination requests.
pub struct PaginationConfig {
    pub after_ledger: Option<i64>,
    pub after_event_id: Option<String>,
}

impl PaginationConfig {
    /// Create pagination config from a limit and optional cursor.
    pub fn new(_limit: i64, after: Option<&str>) -> Result<Self, &'static str> {
        let (after_ledger, after_event_id) = match decode_cursor(after) {
            Ok(Some((l, id))) => (Some(l), Some(id)),
            Ok(None) => (None, None),
            Err(e) => return Err(e),
        };
        Ok(Self {
            after_ledger,
            after_event_id,
        })
    }
}
