//! Issue #712: `reenrich --contract` and `--force` must reach the re-enrichment
//! pass, and a malformed `--contract` must fail before any query runs.
//!
//! These tests drive the real binary (via `CARGO_BIN_EXE_*`) instead of
//! re-implementing its SQL. The argument-to-options mapping and the validation
//! rules are covered by unit tests in `src/main.rs`.

use std::process::Command;

/// The indexer binary, pointed at unreachable services. If the binary tries to
/// reach the database or RPC, it fails with a connection error, which the
/// assertions below would catch.
fn indexer() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_lumenqraph-indexer"));
    cmd.env("DATABASE_URL", "postgres://user:pass@127.0.0.1:1/none")
        .env("RPC_URL", "http://127.0.0.1:1")
        .env_remove("RPC_URLS")
        .env_remove("SKIP_MIGRATIONS")
        .current_dir(env!("CARGO_MANIFEST_DIR"));
    cmd
}

#[test]
fn invalid_contract_fails_before_touching_database_or_rpc() {
    let out = indexer()
        .args(["reenrich", "--contract", "CNOT-A-STRKEY"])
        .output()
        .expect("run indexer");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "malformed --contract must exit non-zero");
    assert!(stderr.contains("invalid --contract"), "stderr: {stderr}");
    assert!(
        !stderr.contains("connect") && !stderr.contains("getNetwork"),
        "validation must run before any connection attempt: {stderr}"
    );
}

#[test]
fn reenrich_help_documents_contract_and_force() {
    let out = indexer()
        .args(["reenrich", "--help"])
        .output()
        .expect("run indexer");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("--contract"), "help: {stdout}");
    assert!(stdout.contains("--force"), "help: {stdout}");
}
