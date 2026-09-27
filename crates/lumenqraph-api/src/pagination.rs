//! Keyset (cursor) pagination utilities shared by every list endpoint.
//!
//! List endpoints page newest-first with an opaque `after` cursor. For the
//! materialized event views the cursor is base64 of `(ledger, event_id)`; for
//! webhook deliveries it is base64 of the row `id`. Keyset paging costs the
//! same on page 1 and page 10,000, unlike `OFFSET`, which scans and discards
//! every skipped row.
//!
//! `offset` is still accepted for backward compatibility, but it is:
//!
//! - capped at [`MAX_OFFSET`] rows (400 beyond that), and
//! - deprecated: responses to offset requests carry a `Deprecation: true`
//!   header and a `Warning` header pointing at the `after` cursor.
//!
//! Every list endpoint answers with the same envelope ([`Page`]):
//!
//! ```json
//! { "data": [...], "has_more": true, "next_cursor": "MTIzfGFiYw==" }
//! ```

use axum::http::{HeaderName, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::Serialize;

use crate::error::ApiError;

/// Deepest `offset` still served. Deeper pages must use the `after` cursor.
pub const MAX_OFFSET: i64 = 10_000;

/// Value of the `Warning` header sent with offset-paginated responses.
const OFFSET_DEPRECATION_WARNING: &str =
    "299 - \"offset pagination is deprecated; use the 'after' cursor from next_cursor\"";

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

/// An opaque keyset position that can round-trip through a query string.
pub trait Cursor: Sized {
    fn encode(&self) -> String;
    fn decode(raw: &str) -> Result<Self, &'static str>;
}

/// `(ledger, event_id)` position, used by all event-derived views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerCursor {
    pub ledger: i64,
    pub event_id: String,
}

impl LedgerCursor {
    pub fn new(ledger: i64, event_id: &str) -> Self {
        Self {
            ledger,
            event_id: event_id.to_string(),
        }
    }
}

impl Cursor for LedgerCursor {
    fn encode(&self) -> String {
        encode_cursor(self.ledger, &self.event_id)
    }

    fn decode(raw: &str) -> Result<Self, &'static str> {
        let (ledger, event_id) = decode_cursor(Some(raw))?.ok_or("invalid cursor")?;
        Ok(Self { ledger, event_id })
    }
}

/// Monotonic row-id position, used for tables keyed by a `BIGSERIAL` id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdCursor(pub i64);

impl Cursor for IdCursor {
    fn encode(&self) -> String {
        B64.encode(self.0.to_string())
    }

    fn decode(raw: &str) -> Result<Self, &'static str> {
        let bytes = B64.decode(raw).map_err(|_| "invalid cursor")?;
        let s = String::from_utf8(bytes).map_err(|_| "invalid cursor")?;
        s.parse::<i64>().map(IdCursor).map_err(|_| "invalid cursor")
    }
}

/// A validated page request: clamped `limit`, capped `offset`, decoded cursor.
///
/// When a cursor is supplied `offset` is ignored (forced to 0).
#[derive(Debug)]
pub struct PageRequest<C> {
    pub limit: i64,
    pub offset: i64,
    pub after: Option<C>,
}

impl<C: Cursor> PageRequest<C> {
    /// Validate raw query parameters.
    ///
    /// `limit` is clamped to `1..=max_limit`. An `offset` above [`MAX_OFFSET`]
    /// without a cursor is rejected; a malformed cursor is rejected.
    pub fn parse(
        limit: i64,
        max_limit: i64,
        offset: i64,
        after: Option<&str>,
    ) -> Result<Self, ApiError> {
        let limit = limit.clamp(1, max_limit);
        let after = after
            .map(C::decode)
            .transpose()
            .map_err(|e| ApiError::bad_request(format!("invalid cursor: {e}")))?;
        let offset = if after.is_some() { 0 } else { offset.max(0) };
        if offset > MAX_OFFSET {
            return Err(ApiError::bad_request(format!(
                "offset pagination is limited to {MAX_OFFSET} rows. For deeper pages, use \
                 cursor pagination with the 'after' parameter (see API documentation)."
            )));
        }
        Ok(Self {
            limit,
            offset,
            after,
        })
    }

    /// Row count to request from the database: one extra sentinel row tells us
    /// whether another page exists without a `COUNT(*)`.
    pub fn fetch_limit(&self) -> i64 {
        self.limit + 1
    }

    /// Whether this request uses deprecated offset pagination.
    pub fn uses_offset(&self) -> bool {
        self.after.is_none() && self.offset > 0
    }

    /// Trim the sentinel row and build the response envelope. `cursor_of`
    /// extracts the keyset position of a row.
    pub fn finish<T>(&self, mut rows: Vec<T>, cursor_of: impl Fn(&T) -> C) -> Page<T> {
        let has_more = rows.len() as i64 > self.limit;
        if has_more {
            rows.truncate(self.limit as usize);
        }
        let next_cursor = if has_more {
            rows.last().map(|r| cursor_of(r).encode())
        } else {
            None
        };
        Page {
            data: rows,
            has_more,
            next_cursor,
            offset_deprecated: self.uses_offset(),
        }
    }
}

/// Uniform list response envelope.
#[derive(Debug, Serialize)]
pub struct Page<T> {
    /// The rows in this page.
    pub data: Vec<T>,
    /// Whether more rows are available after this page.
    pub has_more: bool,
    /// Opaque cursor to pass as `after` for the next page. Null on the last page.
    pub next_cursor: Option<String>,
    /// Whether the request used deprecated offset pagination (drives headers).
    #[serde(skip)]
    pub offset_deprecated: bool,
}

impl<T: Serialize> IntoResponse for Page<T> {
    fn into_response(self) -> Response {
        let deprecated = self.offset_deprecated;
        respond(self, deprecated)
    }
}

/// Serialize `body` as JSON, adding the offset deprecation headers when
/// `offset_deprecated` is set. Used by endpoints that wrap [`Page`] in a larger
/// body.
pub fn respond<B: Serialize>(body: B, offset_deprecated: bool) -> Response {
    let mut response = Json(body).into_response();
    if offset_deprecated {
        let headers = response.headers_mut();
        headers.insert(
            HeaderName::from_static("deprecation"),
            HeaderValue::from_static("true"),
        );
        headers.insert(
            axum::http::header::WARNING,
            HeaderValue::from_static(OFFSET_DEPRECATION_WARNING),
        );
    }
    response
}
