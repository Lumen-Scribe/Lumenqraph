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

/// Parse a contract error code out of a Soroban simulation error string.
///
/// Soroban reports contract errors in the form `Error(Contract, #N)` inside
/// the simulation error text. This extracts the numeric code `N` so the API
/// can look it up in the contract's error-enum spec and return a named error
/// rather than a bare number.
///
/// Returns `Some(code)` when the error text contains a recognisable contract
/// error pattern, `None` otherwise (host-level or other errors).
pub fn parse_contract_error_code(error_text: &str) -> Option<u32> {
    // The canonical form emitted by Soroban is `Error(Contract, #N)`.
    // We look for that pattern with some flexibility for whitespace.
    let text = error_text;

    // Find "Error(Contract" (case-sensitive, as emitted by the host).
    let start = text.find("Error(Contract")?;
    let after = &text[start + "Error(Contract".len()..];

    // Consume optional whitespace and a comma, then "#".
    let after = after.trim_start_matches(|c: char| c.is_whitespace() || c == ',');
    let after = after.strip_prefix('#')?;

    // Parse the decimal number up to the first non-digit.
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<u32>().ok()
}

/// Normalise and bound an upstream simulation error message for client display.
///
/// The transformation is deliberately lossy and conservative:
/// - leading/trailing whitespace is trimmed,
/// - control characters (incl. embedded newlines, tabs, NUL, ESC) are dropped,
/// - runs of whitespace are collapsed to a single space,
/// - **internal-leakage redaction**: endpoint URLs (`http(s)://…`) and
///   base64/XDR-style blobs are replaced with a `[redacted:…]` placeholder so
///   they never reach an unauthenticated caller. Public on-chain identifiers
///   are preserved: valid strkeys (`C…`/`G…`, validated with
///   `stellar_strkey`) and identifier-like tokens (`^[A-Za-z][A-Za-z0-9_]*$`,
///   up to 64 chars, e.g. `InsufficientAllowance`) survive intact. Only tokens
///   that look like base64 (contain `+`, `/` or `=`, or mix upper/lower case
///   with digits above an entropy threshold) or exceed 64 chars are redacted,
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

/// Replace endpoint URLs and base64/XDR-style blobs with redaction
/// placeholders. Operates token-by-token so normal prose (e.g. "contract
/// trapped: arithmetic overflow") is left intact.
///
/// Public identifiers are deliberately preserved: strkeys (validated with
/// `stellar_strkey`) and identifier-like tokens up to 64 chars are the most
/// useful part of an auth or trap error and are not internal detail.
fn redact_internal_detail(s: &str) -> String {
    s.split_whitespace()
        .map(|tok| {
            if tok.starts_with("http://") || tok.starts_with("https://") {
                "[redacted:url]".to_string()
            } else if is_public_identifier(tok) {
                tok.to_string()
            } else if looks_like_base64(tok) || tok.chars().count() > 64 {
                "[redacted:blob]".to_string()
            } else {
                tok.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `tok` is a public on-chain identifier that must survive
/// sanitisation: a valid strkey (`C…`/`G…`/etc.) or an identifier-like token
/// (`^[A-Za-z][A-Za-z0-9_]*$`) of at most 64 characters.
fn is_public_identifier(tok: &str) -> bool {
    if is_strkey(tok) {
        return true;
    }
    let len = tok.chars().count();
    if len == 0 || len > 64 {
        return false;
    }
    let mut chars = tok.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Validate `tok` as a Stellar strkey (account, contract, muxed account, …).
fn is_strkey(tok: &str) -> bool {
    stellar_strkey::Strkey::from_string(tok).is_ok()
}

/// Heuristic for base64/XDR-style blobs: tokens containing base64-only
/// punctuation (`+`, `/`, `=`) are treated as blobs, as are long tokens that
/// mix upper and lower case with digits (high entropy, unlike identifiers).
fn looks_like_base64(tok: &str) -> bool {
    if tok.contains('+') || tok.contains('/') || tok.contains('=') {
        return true;
    }
    let len = tok.chars().count();
    if len < 16 {
        return false;
    }
    let has_upper = tok.chars().any(|c| c.is_ascii_uppercase());
    let has_lower = tok.chars().any(|c| c.is_ascii_lowercase());
    let has_digit = tok.chars().any(|c| c.is_ascii_digit());
    has_upper && has_lower && has_digit
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

    #[test]
    fn strkeys_survive_sanitisation() {
        // Real testnet contract and account strkeys captured from RPC errors.
        let contract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let account = "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF";
        let msg = format!("Error(Auth, InvalidAction) address {account} contract {contract}");
        let out = sanitize_simulation_error(&msg);
        assert!(out.contains(contract), "contract strkey lost: {out}");
        assert!(out.contains(account), "account strkey lost: {out}");
        assert!(!out.contains("[redacted:blob]"), "strkey redacted: {out}");
    }

    #[test]
    fn long_error_names_survive_sanitisation() {
        let msg = "HostError: Error(Contract, #1) InsufficientAllowance ExceededMaxSupplyLimit";
        let out = sanitize_simulation_error(msg);
        assert!(out.contains("InsufficientAllowance"), "name lost: {out}");
        assert!(out.contains("ExceededMaxSupplyLimit"), "name lost: {out}");
        assert!(!out.contains("[redacted:blob]"), "name redacted: {out}");
    }

    #[test]
    fn real_testnet_rpc_error_keeps_identifiers() {
        // Shape of a real testnet simulateTransaction error string.
        let msg = "HostError: Error(Auth, InvalidAction) \
                   contract: CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM \
                   address: GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF \
                   event: InsufficientAllowance";
        let out = sanitize_simulation_error(msg);
        assert!(out.contains("CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM"));
        assert!(out.contains("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"));
        assert!(out.contains("InsufficientAllowance"));
        assert!(out.contains("Error(Auth, InvalidAction)"));
    }

    #[test]
    fn base64_blobs_and_urls_are_redacted() {
        let blob = "SGVsbG8gV29ybGQhIFRoaXMgaXMgYSBiYXNlNjQgYmxvYg==";
        let msg = format!("call failed at https://internal.rpc/simulate XDR={blob} trapped");
        let out = sanitize_simulation_error(&msg);
        assert!(!out.contains("https://internal.rpc"), "url leaked: {out}");
        assert!(!out.contains(blob), "blob leaked: {out}");
        assert!(out.contains("[redacted:url]"));
        assert!(out.contains("[redacted:blob]"));
        assert!(out.contains("trapped"), "prose lost: {out}");
    }

    #[test]
    fn overlong_token_is_redacted() {
        let long = "A".repeat(65);
        let out = sanitize_simulation_error(&long);
        assert_eq!(out, "[redacted:blob]");
    }
}
