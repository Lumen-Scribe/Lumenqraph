# Security Model & Threat Analysis

This document outlines Lumenqraph's security posture, including trust boundaries, threat model, known residual risks, and hardening strategies for operators.

## Trust Boundaries

```
┌─────────────────────────────────────────────────────────────┐
│                    UNTRUSTED (Internet)                     │
│  Soroban RPC, Webhook subscribers, HTTP API consumers       │
└────────────────────────────┬────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │ HTTPS / TLS 1.2+ │
                    │ (Required in prod)│
                    └────────┬────────┘
                             │
      ┌──────────────────────┴────────────────────────────┐
      │                                                   │
      ▼                                                   ▼
┌─────────────────────┐                         ┌──────────────────┐
│  Indexer Service    │                         │  API Service     │
│ (reads RPC only)    │                         │ (HTTP gateway)   │
│  - No auth on RPC   │                         │ - Rate limits    │
│  - Validates XDR    │                         │ - API keys       │
│  - Immutable DB     │                         │ - GraphQL limits │
└─────────┬───────────┘                         └────────┬─────────┘
          │                                             │
          └─────────────┬──────────────────────────────┘
                        │
                    ┌───▼───────────────┐
                    │  PostgreSQL DB    │
                    │  (trusted compute)│
                    │  - Network only   │
                    │  - Password auth  │
                    │  - At-rest crypto │
                    └───────┬───────────┘
                            │
      ┌─────────────────────┴────────────────────────┐
      │                                              │
      ▼                                              ▼
┌──────────────────────┐                  ┌──────────────────────┐
│ Webhook Service      │                  │ Self-Hosters' Apps   │
│ (outbound HTTP only) │                  │ (consumer trust)     │
│ - Validates URLs     │                  │                      │
│ - Signs payloads     │                  │ - Must verify sigs   │
│ - Retries safely     │                  │ - Rate-limit input   │
└──────────────────────┘                  └──────────────────────┘
```

## What Lumenqraph Protects Against

### ✓ INTEGRITY & AUTHENTICITY

1. **Webhook Payload Integrity**
   - All webhooks are signed with HMAC-SHA256 over the raw body
   - Verification uses constant-time comparison (timing-attack resistant)
   - See [WEBHOOKS.md](WEBHOOKS.md) for verification examples

2. **RPC Data Validation**
   - All XDR from Soroban RPC is cryptographically validated by the Stellar SDK
   - Invalid events are rejected at parse time
   - Decoded JSON is validated against on-chain contract specs

3. **API Key Security**
   - Only SHA-256 hashes of keys are stored in the database
   - Raw keys are never logged or exposed
   - Keys can be revoked without database resets

4. **Webhook Secret Encryption**
   - Secrets can be encrypted at rest using pgcrypto (AES-128-CBC)
   - Set `WEBHOOK_ENCRYPTION_KEY` to enable encryption
   - Decryption happens only during delivery

### ✓ RATE LIMITING

1. **Unauthenticated Requests**
   ```
   Default: 60 req/min per IP
   Configurable via ANON_RATE_LIMIT_PER_MIN
   Trust X-Forwarded-For via RATE_LIMIT_TRUST_XFF (proxy only)
   ```

2. **API Key Tiers**
   - Custom per-key rate limits (stored in `api_keys.rate_limit_per_min`)
   - Sliding-window bucket per key

3. **Expensive RPC Routes** (`/contracts/:id/call`, `/contracts/:id/simulate`)
   - Separate, tighter rate limit (default: 10 req/min vs 60)
   - Can require separate API key authentication
   - Prevents RPC quota exhaustion

4. **GraphQL Limits**
   - Query depth limit: 12 (prevent nested DoS)
   - Complexity limit: 1000 (prevent expensive aggregations)
   - Configurable via `GRAPHQL_MAX_DEPTH` / `GRAPHQL_MAX_COMPLEXITY`

### ✓ NETWORK ISOLATION

1. **Webhook SSRF Prevention**
   - URLs are validated to prevent targeting private IPs
   - Blocked ranges: `127.0.0.1`, `::1`, `169.254.*` (link-local), `10.*`, `172.16-31.*`, `192.168.*`, private Docker networks
   - Localhost webhooks require explicit `ALLOW_LOCALHOST_WEBHOOKS=true`

2. **RPC Endpoint Control**
   - Operators choose the Soroban RPC they trust
   - Default: Stellar Development Foundation public (untrusted, read-only)
   - Paid/private RPC endpoints supported for higher reliability

3. **Database Network Isolation**
   - PostgreSQL password authentication required
   - TLS connections to database recommended in production

### ✓ DATA PUBLIC-BY-DESIGN

Lumenqraph assumes **all indexed data is public** (by nature of blockchain). However, secrets are protected:

**Public by default** (no auth required):
- `/health` — indexer status
- `/metrics` — Prometheus metrics
- `/contracts` — indexed contracts & event counts
- `/contracts/:id/events` — contract events
- `/contracts/:id/interface` — on-chain contract spec
- GraphQL query endpoints (read-only)

**Protected by API key** (when `REQUIRE_API_KEY=true`):
- `/contracts/:id/call` — view call simulation (RPC-backed)
- `/contracts/:id/simulate` — transaction simulation
- Management endpoints (e.g., webhooks CRUD — not yet exposed)

## What Lumenqraph Does NOT Protect Against

### ✗ SOROBAN RPC COMPROMISE

If your configured RPC endpoint is compromised or malicious:

- **Attack:** Fake events or false state data
- **Impact:** Corrupted index; webhooks deliver false data
- **Mitigation:** Use trusted RPC endpoints (SDF public, your own Stellar node, paid providers)
- **Detection:** Compare event counts / checksums with trusted sources

Example:
```bash
# Use a trusted private RPC
RPC_URL=https://your-private-soroban-rpc.example.com
```

### ✗ DATABASE COMPROMISE

If the PostgreSQL server is compromised:

- **Attack:** Direct table modification, plaintext secret access (if unencrypted)
- **Impact:** Incorrect index state, webhook secret disclosure
- **Mitigation:**
  - Strong database password (min 32 chars, random)
  - Enable at-rest encryption: `WEBHOOK_ENCRYPTION_KEY` (pgcrypto)
  - Network isolation: database inside private VPC, no public IP
  - Regular backups, tested restoration

```bash
# Enable webhook secret encryption
WEBHOOK_ENCRYPTION_KEY="$(openssl rand -hex 32)"

# Rotate regularly
ALTER SYSTEM SET pgp_symmetric_cipher='aes128';
```

### ✗ API KEY LEAKAGE

If an API key is compromised:

- **Attack:** Attacker makes requests as your app
- **Impact:** Hit rate limits, potentially consume expensive RPC quota
- **Mitigation:**
  - Rotate keys frequently (monthly recommended)
  - Use separate keys per environment (dev, staging, prod)
  - Log key usage, monitor for anomalies
  - Revoke immediately if leaked

```sql
-- Revoke a leaked key
UPDATE api_keys SET revoked = true WHERE key_hash = '...';

-- Monitor usage
SELECT key_hash, count(*) as requests FROM api_audit 
WHERE created_at > now() - interval '1 hour'
GROUP BY key_hash;
```

### ✗ WEBHOOK RECEIVER COMPROMISE

If your webhook endpoint is compromised:

- **Attack:** Attacker can see webhook payloads (if HTTPS is not enforced)
- **Impact:** Leak event data over HTTP
- **Mitigation:**
  - Always use HTTPS for webhook URLs
  - Verify signatures (required)
  - Monitor webhook failure logs
  - Disable suspicious subscriptions

```bash
# Only allow HTTPS webhooks (recommended)
# (Validation happens in API, requires HTTPS: scheme)
```

### ✗ TIMING ATTACKS ON SIGNATURES

If signature verification doesn't use constant-time comparison:

- **Attack:** Measure response time to learn the signature byte-by-byte
- **Impact:** Attacker can forge signatures
- **Mitigation:** Always use `crypto.timingSafeEqual()` (Node.js) or `verify_hmac_signature()` (Rust)

This is why Lumenqraph's webhook service uses constant-time comparison; consumers **must also do this**.

## Known Residual Risks

### 1. RPC Rate Limiting (Shared Quota)

**Risk:** Heavy consumers exhaust shared RPC quota, starving other users.

**Mitigation:**
- Set `RPC_ROUTE_RATE_LIMIT_PER_MIN` aggressively (default: 10 req/min)
- Optionally require API key just for `/call` routes: `RPC_REQUIRE_API_KEY=true`
- Use paid/private RPC endpoints with guaranteed quota

**Example:**
```bash
# Stricter limits for expensive routes
RPC_ROUTE_RATE_LIMIT_PER_MIN=5
RPC_REQUIRE_API_KEY=true  # Force auth on /call and /simulate
```

### 2. Shallow Reorg Gaps

**Risk:** If RPC returns different events for shallow reorgs, old events may be missed.

**Mitigation:**
- Enable `REORG_OVERLAP_LEDGERS` (default: 0) to re-scan trailing ledgers
- Set to 10–100 for conservative protection; higher values cost more RPC calls
- Monitor `last_error` in `webhook_deliveries` for skipped events

```bash
REORG_OVERLAP_LEDGERS=50  # Re-scan last 50 ledgers each cycle
```

### 3. GraphQL Introspection in Production

**Risk:** Exposing schema via introspection gives attackers a roadmap.

**Mitigation:** Disable GraphQL introspection in production
```bash
GRAPHQL_INTROSPECTION_ENABLED=false  # Default
```

### 4. Unencrypted Webhook Secrets in Transit

**Risk:** HTTP webhook URLs transmit secrets in plaintext.

**Mitigation:**
- Enforce HTTPS for webhook URLs (done via URL validation)
- Encrypt secrets at rest: `WEBHOOK_ENCRYPTION_KEY`
- Use separate secrets per webhook (default)

### 5. CORS Misconfiguration

**Risk:** If `API_CORS_ALLOWED_ORIGINS` is set to `*`, browsers leak session cookies.

**Mitigation:** Use specific origins or `same_origin`
```bash
# Default (safe)
API_CORS_ALLOWED_ORIGINS="same_origin"

# Specific list
API_CORS_ALLOWED_ORIGINS="https://app1.example.com,https://app2.example.com"
```

### 6. Unencrypted Database Connection

**Risk:** PostgreSQL password sent in plaintext over unencrypted connection.

**Mitigation:** Use SSL/TLS for database connections
```bash
# Connection string with SSL
DATABASE_URL="postgres://user:pass@host:5432/db?sslmode=require"
```

## Hardening Checklist

### For Self-Hosted Deployments

- [ ] **HTTPS Everywhere**
  - [ ] API endpoints behind HTTPS reverse proxy (nginx, Caddy)
  - [ ] Webhook URLs must be HTTPS
  - [ ] Database connection uses SSL/TLS

- [ ] **Authentication & Secrets**
  - [ ] Strong database password (min 32 chars)
  - [ ] Enable webhook secret encryption: `WEBHOOK_ENCRYPTION_KEY=<random-hex>`
  - [ ] Require API keys: `REQUIRE_API_KEY=true`
  - [ ] Disable GraphQL introspection: `GRAPHQL_INTROSPECTION_ENABLED=false`

- [ ] **Rate Limiting**
  - [ ] Review `ANON_RATE_LIMIT_PER_MIN` (default: 60, consider lower)
  - [ ] Set tight RPC route limit: `RPC_ROUTE_RATE_LIMIT_PER_MIN=5`
  - [ ] Require API key for expensive routes: `RPC_REQUIRE_API_KEY=true`
  - [ ] Set GraphQL limits: `GRAPHQL_MAX_DEPTH=10`, `GRAPHQL_MAX_COMPLEXITY=500`

- [ ] **Network Isolation**
  - [ ] Database in private VPC, no public IP
  - [ ] Indexer / API services not internet-facing (behind proxy)
  - [ ] Webhook service restricted to outbound HTTPS only
  - [ ] Ingress firewall rules (allow only trusted IPs for admin endpoints)

- [ ] **RPC Endpoint**
  - [ ] Use trusted RPC (private node, reputable provider)
  - [ ] Monitor RPC for anomalies
  - [ ] Consider regional fallback endpoints

- [ ] **Logging & Monitoring**
  - [ ] Enable logging: `RUST_LOG=info`
  - [ ] Log auth failures and rate-limit hits
  - [ ] Monitor `/metrics` endpoint for anomalies
  - [ ] Alert on webhook delivery failures

- [ ] **Webhook Management**
  - [ ] Verify webhook signatures on your end (required)
  - [ ] Monitor auto-disabled subscriptions
  - [ ] Rotate secrets periodically
  - [ ] Delete inactive subscriptions

- [ ] **Operational Security**
  - [ ] Regular database backups (tested restoration)
  - [ ] Keep dependencies updated: `cargo update`
  - [ ] Review access logs for unauthorized API key usage
  - [ ] Run security scans: `cargo audit`, `cargo deny check`

### Production Deployment Example

```bash
# .env.production

# ---- HTTPS & TLS ----
API_BIND_ADDR=0.0.0.0:8080  # Behind nginx reverse proxy with HTTPS
DATABASE_URL=postgres://user:$(openssl rand -hex 16)@db-private:5432/lumenqraph?sslmode=require

# ---- RPC Trust ----
RPC_URL=https://your-trusted-rpc.example.com
RPC_TIMEOUT_SECS=30

# ---- Authentication ----
REQUIRE_API_KEY=true
API_CORS_ALLOWED_ORIGINS="https://app.example.com"

# ---- Rate Limiting ----
ANON_RATE_LIMIT_PER_MIN=20
RPC_ROUTE_RATE_LIMIT_PER_MIN=5
RPC_REQUIRE_API_KEY=true
GRAPHQL_MAX_DEPTH=10
GRAPHQL_MAX_COMPLEXITY=500
GRAPHQL_INTROSPECTION_ENABLED=false

# ---- Webhooks ----
WEBHOOK_ENCRYPTION_KEY="$(openssl rand -hex 32)"
WEBHOOK_TICK_SECS=3
WEBHOOK_MAX_ATTEMPTS=6
WEBHOOK_FAILURE_THRESHOLD=10

# ---- Indexing ----
CONTRACT_IDS=CADQZ...  # Bounded set (cheaper)
UPGRADE_WATCH=true
STATE_INDEXING=false  # Enable only if needed
KEY_INDEXING=false    # Enable only if needed
RETENTION_LEDGERS=120960  # 7 days

# ---- Logging ----
RUST_LOG=info,lumenqraph_indexer=warn,lumenqraph_api=warn,lumenqraph_webhooks=warn
```

## Dependency & Supply Chain Security

Lumenqraph uses automated scanning to prevent vulnerable dependencies:

1. **Cargo Audit** — Checks against [RustSec Advisory Database](https://rustsec.org/)
   ```bash
   cargo audit  # Fails CI if advisories found
   ```

2. **Cargo Deny** — Comprehensive supply-chain scanning
   ```bash
   cargo deny check  # Checks advisories, licenses, duplicates, sources
   ```

3. **GitHub Dependabot** — Automated PRs for dependency updates

### Running Locally

```bash
# Check for security advisories
cargo audit

# Run full supply-chain check
cargo deny check
```

## Compliance & Standards

- **OWASP Top 10**:
  - Injection (XDR validation, parameterized queries)
  - Broken Auth (API key rate limiting, constant-time comparison)
  - SSRF (webhook URL validation)
  - Sensitive Data Exposure (encryption at rest, TLS in transit)

- **NIST Recommendations**:
  - Key rotation (API keys, webhook secrets)
  - Minimal privilege (separate rate limits per key)
  - Audit logging (webhook deliveries, auth failures)

- **CWE/CVE Coverage**:
  - CWE-208: Observable Timing Discrepancy (constant-time comparison)
  - CWE-613: Insufficient Session Expiration (not applicable — stateless)
  - CWE-776: Improper Restriction of Recursive Entity (XDR parsing limits)

## Incident Response

If a vulnerability is discovered:

1. **Reporting:** Use [GitHub Security Advisory](https://github.com/Lumen-Scribe/Lumenqraph/security/advisories)
2. **Timeline:** Critical fixes within 7 days; high within 14 days
3. **Disclosure:** Published advisories after patches are available
4. **Credits:** Security researchers acknowledged (unless requesting anonymity)

See [SECURITY.md](../SECURITY.md) for full vulnerability reporting policy.

## See Also

- [SECURITY.md](../SECURITY.md) — Vulnerability reporting and cryptographic details
- [WEBHOOKS.md](WEBHOOKS.md) — Webhook signature verification
- [CONFIGURATION.md](CONFIGURATION.md) — All security-related configuration variables
- [DEPLOYMENT.md](DEPLOYMENT.md) — Production deployment guide
