# Known gaps: closed issues that aren't fixed on `main`

Several issues were closed without a test proving the behaviour, while CI was red and couldn't catch the gap (#476). This page records each one, what `main` actually does, and the **open** issue that now tracks the work, so nobody plans around a feature that doesn't work yet.

If you fix one of these, update this table in the same PR, and include the behaviour-level test described in the [definition of done](../CONTRIBUTING.md#definition-of-done).

| Closed issue | What's on `main` | Open tracking issue |
| --- | --- | --- |
| #86, #87, #88: swap, NFT and liquidity verticals | Tables and endpoints exist; nothing in the indexer writes to them, so they always return empty results | [#599](https://github.com/Lumen-Scribe/Lumenqraph/issues/599) |
| #91: admin API for API keys | No key-management routes exist; keys are still minted with `scripts/gen_api_key.sh` | [#431](https://github.com/Lumen-Scribe/Lumenqraph/issues/431) |
| #116: backfill a new webhook subscription | `since` is stored but never read by the enqueuer | None yet: [#116](https://github.com/Lumen-Scribe/Lumenqraph/issues/116) should be reopened |
| #117: prune `webhook_deliveries` | The prune function exists but is never spawned | [#567](https://github.com/Lumen-Scribe/Lumenqraph/issues/567), [#568](https://github.com/Lumen-Scribe/Lumenqraph/issues/568) |
| #195: no hardcoded encryption key | The API still falls back to `default-key-for-testing` when `WEBHOOK_ENCRYPTION_KEY` is unset | [#422](https://github.com/Lumen-Scribe/Lumenqraph/issues/422), [#644](https://github.com/Lumen-Scribe/Lumenqraph/issues/644) |
| #199: duplicate migration versions | New duplicates at `0017`, `0021`, `0022`, `0023` and `0024` | [#535](https://github.com/Lumen-Scribe/Lumenqraph/issues/535) |
| #198: GIN index for param filters | Migration `0019` is a no-op: `idx_events_enriched` already exists from `0004` | None yet: [#198](https://github.com/Lumen-Scribe/Lumenqraph/issues/198) should be reopened (see also #364) |
| #100: proxy integration test | The test file contains only comments | None yet: [#100](https://github.com/Lumen-Scribe/Lumenqraph/issues/100) should be reopened |
| #106, #233: re-enrichment and its progress | `reenrich` panics on JSONB decoding, skips rows (OFFSET paging over a predicate it mutates), and ignores `--force` and `--contract` | [#597](https://github.com/Lumen-Scribe/Lumenqraph/issues/597) |
| #264: webhook secret rotation | `rotate-secret` writes columns that don't exist, and the dispatcher never signs with the previous secret | [#564](https://github.com/Lumen-Scribe/Lumenqraph/issues/564) |
| #73: MSRV | The workspace doesn't compile on the declared MSRV (1.84); `Cargo.lock` needs 1.85 | [#532](https://github.com/Lumen-Scribe/Lumenqraph/issues/532) |

The broader "`main` doesn't compile and CI hasn't passed" situation is tracked in [#522](https://github.com/Lumen-Scribe/Lumenqraph/issues/522).

_Audited against `main` at `3945118`._
