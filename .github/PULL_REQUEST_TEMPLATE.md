## Description

<!-- Briefly describe what this PR does and why -->

Fixes #<!-- issue number -->

## Changes

<!-- List the main changes in this PR -->

- 
- 

## Test proving the fix

<!--
Required for any PR that closes a bug or feature issue (see "Definition of done"
in CONTRIBUTING.md). Name the test(s) that exercise the behaviour the issue
describes, through the route, CLI command, job or migration a user would hit,
and that fail without this change. A unit test of a helper alone is not enough.

Docs-only or CI-only change? Write "N/A" and explain how you checked it.
-->

- Test: <!-- e.g. crates/lumenqraph-api/tests/webhooks.rs::rotate_secret_signs_with_previous_secret -->
- Fails without this change: <!-- yes / explain -->

## Pre-submit checklist

<!-- Check all that apply -->

- [ ] I have run `make fmt` (or `cargo fmt --all`)
- [ ] I have run `make lint` (or `cargo clippy --workspace --all-targets -- -D warnings`)
- [ ] I have run `make test` (or `cargo test --workspace`)
- [ ] I have run `cargo audit` and `cargo deny check` (if dependencies changed)
- [ ] I have updated the documentation (if user-facing changes)
- [ ] I have added a migration in `migrations/` (if schema changed), and it passes `make migrations-lint` or justifies each `squawk-ignore`
- [ ] I have updated the CHANGELOG (if notable feature or fix)
- [ ] I have tested the Docker build (if Dockerfile or dependencies changed)
- [ ] CI is green, and every issue this PR closes meets the [definition of done](../CONTRIBUTING.md#definition-of-done) (if it only partly fixes an issue, use "Refs #N", not "Fixes #N")

## Testing

<!-- How was this tested? -->

- [ ] Unit tests
- [ ] Integration tests
- [ ] Manual testing: <!-- describe what you tested -->

## Additional context

<!-- Any additional context, screenshots, or notes for reviewers -->
