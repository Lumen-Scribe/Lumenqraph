//! Bounding and normalising error messages that originate from upstream RPC
//! simulation (`simulateTransaction`) before they are surfaced to clients.
//!
//! Upstream simulation errors can be large and may echo internal detail
//! (endpoint specifics, XDR dumps, resource internals) back to unauthenticated
//! callers. They are also an unbounded-error-body DoS / logging-noise vector.
//!
//! The functions here keep the client-facing copy **concise and free of
//! internal leakage**, while the full, raw detail is logged server-side by the
//! caller (the same way [`crate::error::Error`] logs 500s).

/// Maximum number of *characters* we will ever send to a client as a
/// simulation-error message. Large enough to stay useful (most contract traps
/// carry a short, helpful message), small enough to bound response bodies and
/// log noise.
pub const MAX_SIMULATION_ERROR_LEN: usize = 256;

/// Normalise and bound an upstream simulation error message for client display.
///
/// The transformation is deliberately lossy and conservative:
/// - leading/trailing whitespace is trimmed,
/// - control characters (incl. embedded newlines, tabs, NUL, ESC) are dropped,
/// - runs of whitespace are collapsed to a single space,
/// - **internal-leakage redaction**: endpoint URLs (`http(s)://…`) and
///   long base64/XDR-style blobs (alphanumeric `+/=`-only runs of 16+ chars,
///   e.g. encoded ledger entries, resource internals) are replaced with a
///   `[redacted:…]` placeholder so they never reach an unauthenticated caller,
/// - the result is truncated to [`MAX_SIMULATION_ERROR_LEN`] characters,
///   with a `…` ellipsis appended when text was cut.
///
/// The caller is responsible for logging the *full* `raw` message server-side
/// so that no diagnostic detail is actually lost.
pub fn sanitize_simulation_error(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        // Drop control chars but keep the normal space; everything else
        // printable passes through unchanged.
        .filter(|c| !c.is_control())
        .collect();

    let collapsed = cleaned
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let redacted = redact_internal_detail(&collapsed);

    let trimmed = redacted.trim();

    if trimmed.chars().count() <= MAX_SIMULATION_ERROR_LEN {
        return trimmed.to_string();
    }

    let mut out: String = trimmed
        .chars()
        .take(MAX_SIMULATION_ERROR_LEN.saturating_sub(1))
        .collect();
    out.push('…');
    out
}

/// Replace endpoint URLs and long base64/XDR-style blobs with redaction
/// placeholders. Operates token-by-token so normal prose (e.g. "contract
/// trapped: arithmetic overflow") is left intact.
fn redact_internal_detail(s: &str) -> String {
    s.split_whitespace()
        .map(|tok| {
            if tok.starts_with("http://") || tok.starts_with("https://") {
                "[redacted:url]".to_string()
            } else if tok.chars().count() >= 16
                && tok
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '=')
            {
                "[redacted:blob]".to_string()
            } else {
                tok.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_message_passes_through() {
        let msg = "contract trapped: overflow";
        assert_eq!(sanitize_simulation_error(msg), msg);
    }

    #[test]
    fn control_chars_are_stripped() {
        let msg = "trap\x00with\x1b[31mcontrol\x07chars\nand\ttabs";
        let out = sanitize_simulation_error(msg);
        assert!(!out.contains('\x00'));
        assert!(!out.contains('\x1b'));
        assert!(!out.contains('\n'));
        assert!(!out.contains('\t'));
        assert_eq!(out, "trap with control chars and tabs");
    }

    #[test]
    fn long_message_is_truncated_with_ellipsis() {
        let long = "x".repeat(2000);
        let out = sanitize_simulation_error(&long);
        assert!(out.ends_with('…'));
        assert_eq!(out.chars().count(), MAX_SIMULATION_ERROR_LEN);
    }

    #[test]
    fn internal_detail_is_redacted() {
        // Endpoint URL + a long base64/XDR blob must be scrubbed, while the
        // useful prose ("contract trapped") survives.
        let leaked = format!(
            "call failed at https://internal.rpc/simulate XDR={} contract trapped",
            "A".repeat(2000)
        );
        let out = sanitize_simulation_error(&leaked);
        assert!(!out.contains("https://internal.rpc"), "url leaked: {out}");
        assert!(!out.contains("AAAAAAAA"), "blob leaked: {out}");
        assert!(out.contains("contract trapped"), "prose lost: {out}");
        assert!(out.contains("[redacted:url]"));
        assert!(out.contains("[redacted:blob]"));
    }

    #[test]
    fn short_blob_is_not_redacted() {
        // A short token (e.g. a 4-char XDR snippet) is below the threshold and
        // left alone; only long base64 runs are treated as leaks.
        let msg = "saw XDR=AAAA but ok";
        assert_eq!(sanitize_simulation_error(msg), msg);
    }

    #[test]
    fn whitespace_runs_collapse() {
        let msg = "  too    many     spaces  ";
        assert_eq!(sanitize_simulation_error(msg), "too many spaces");
    }
}
