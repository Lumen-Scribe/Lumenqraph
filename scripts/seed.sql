-- Demo dataset / seed script for local exploration without a live RPC.
-- Inserts realistic decoded events, interfaces, state, and data so the
-- explorer and API show meaningful content immediately after seeding.
--
-- Usage:
--   psql $DATABASE_URL -f scripts/seed.sql
--
-- By default the script picks a SEED_LEDGER close to the current Stellar
-- mainnet tip so that /health reports a small lag_ledgers value.  You can
-- override it explicitly:
--
--   psql $DATABASE_URL -v SEED_LEDGER=55000000 -f scripts/seed.sql
--
-- NOTE ON LAG: seed.sh sets indexer_cursor.last_processed_ledger to the
-- same SEED_LEDGER value, so the reported lag after seeding will be ≈ 0.
-- If you seed without running seed.sh the lag shown by /health reflects
-- the real distance between SEED_LEDGER and the live chain tip, which is
-- expected and not an error.

BEGIN;

-- ---------------------------------------------------------------------------
-- Resolve SEED_LEDGER.
--
-- When this file is executed directly with psql (not via seed.sh) the
-- :'SEED_LEDGER' variable will be undefined.  We use a temporary table as a
-- portable way to carry a computed default into later statements without
-- requiring server-side procedural code.
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE _seed_cfg (ledger bigint) ON COMMIT DROP;

DO $$
DECLARE
  v_ledger bigint;
BEGIN
  -- Try to read the psql variable injected by seed.sh / the caller.
  -- current_setting returns '' when the variable is absent.
  v_ledger := nullif(current_setting('seed.ledger', true), '')::bigint;

  IF v_ledger IS NULL THEN
    -- Stellar mainnet produces ~1 ledger every 5 seconds.
    -- Approximate the tip as: genesis_ledger + seconds_since_genesis / 5
    -- Genesis (ledger 1) closed at 2015-09-30 18:00:00 UTC.
    v_ledger := 2 + EXTRACT(EPOCH FROM (NOW() - TIMESTAMPTZ '2015-09-30 18:00:00 UTC'))::bigint / 5;
  END IF;

  INSERT INTO _seed_cfg VALUES (v_ledger);
END;
$$;

-- ---------------------------------------------------------------------------
-- Ensure cursor exists (required for indexer operation).
-- seed.sh will update this to the same ledger after psql exits, but we
-- set it here too so a bare `psql … -f seed.sql` run is self-contained.
-- ---------------------------------------------------------------------------
INSERT INTO indexer_cursor (id, last_processed_ledger, updated_at)
SELECT 1, ledger, NOW() FROM _seed_cfg
ON CONFLICT (id) DO UPDATE
    SET last_processed_ledger = EXCLUDED.last_processed_ledger,
        updated_at             = NOW();

-- ---------------------------------------------------------------------------
-- Demo contract: SEP-41 token (fictional USDC-like stablecoin).
-- ---------------------------------------------------------------------------
INSERT INTO events (
    event_id, contract_id, ledger, ledger_closed_at, event_type,
    topics, event_name, value, tx_hash, in_successful_call, paging_token, created_at
)
SELECT
    -- Transfer event
    LPAD((ledger)::text, 7, '0') || '-0000000001',
    'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
    ledger,
    NOW() - INTERVAL '1 hour',
    'contract',
    '["AAAADwAAAAh0cmFuc2Zlcg=="]',
    'transfer',
    'AAAAEQAAAAEAAAACAAAAEgAAAAAAAAAAfn0jtXvOQK5+p8H7mZYyxmjQKDZd1v1dw1DqPBRPzDAAAAASAAAAAAAAAABXVaE6fY9F8v7DOC1+v2cF6wZXpKTNRjKDZPrJpLj4AA==',
    'abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
    true,
    ledger::text || '-1',
    NOW() - INTERVAL '1 hour'
FROM _seed_cfg

UNION ALL

-- Mint event (ledger + 1)
SELECT
    LPAD((ledger + 1)::text, 7, '0') || '-0000000002',
    'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
    ledger + 1,
    NOW() - INTERVAL '50 minutes',
    'contract',
    '["AAAADwAAAARtaW50"]',
    'mint',
    'AAAAEQAAAAEAAAACAAAAEgAAAAAAAAAAV1WhOn2PRfL+wzgtfr9nBesGV6SkzUYyg2T6yaS4+AAAAA4AAAAQMTAwMDAwMDAwMDAwMDAwMA==',
    'fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321',
    true,
    (ledger + 1)::text || '-1',
    NOW() - INTERVAL '50 minutes'
FROM _seed_cfg

UNION ALL

-- Burn event (ledger + 2)
SELECT
    LPAD((ledger + 2)::text, 7, '0') || '-0000000003',
    'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
    ledger + 2,
    NOW() - INTERVAL '40 minutes',
    'contract',
    '["AAAADwAAAARidXJu"]',
    'burn',
    'AAAAEQAAAAEAAAACAAAAEgAAAAAAAAAAfn0jtXvOQK5+p8H7mZYyxmjQKDZd1v1dw1DqPBRPzDAAAAAOAAAADzUwMDAwMDAwMDAwMDAwMA==',
    'aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899',
    true,
    (ledger + 2)::text || '-1',
    NOW() - INTERVAL '40 minutes'
FROM _seed_cfg

UNION ALL

-- Approval event (ledger + 3)
SELECT
    LPAD((ledger + 3)::text, 7, '0') || '-0000000004',
    'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
    ledger + 3,
    NOW() - INTERVAL '30 minutes',
    'contract',
    '["AAAADwAAAAhhcHByb3Zl"]',
    'approve',
    'AAAAEQAAAAEAAAADAAAAEgAAAAAAAAAAfn0jtXvOQK5+p8H7mZYyxmjQKDZd1v1dw1DqPBRPzDAAAAAOAAAADzEwMDAwMDAwMDAwMDAwMDAAAAAKAAAAAAAAAAAAAAARmN6/ag==',
    '1122334455667788991122334455667788991122334455667788991122334455',
    true,
    (ledger + 3)::text || '-1',
    NOW() - INTERVAL '30 minutes'
FROM _seed_cfg;

-- ---------------------------------------------------------------------------
-- Demo contract interface: SEP-41 token spec.
-- ---------------------------------------------------------------------------
INSERT INTO contract_specs (
    contract_id, wasm_hash, interface, spec_section, has_events, fetched_at
) VALUES
('CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2',
 '{
   "functions": [
     {"name": "transfer", "inputs": [{"name": "from", "type": "Address"}, {"name": "to", "type": "Address"}, {"name": "amount", "type": "i128"}], "outputs": []},
     {"name": "mint", "inputs": [{"name": "to", "type": "Address"}, {"name": "amount", "type": "i128"}], "outputs": []},
     {"name": "burn", "inputs": [{"name": "from", "type": "Address"}, {"name": "amount", "type": "i128"}], "outputs": []},
     {"name": "approve", "inputs": [{"name": "from", "type": "Address"}, {"name": "spender", "type": "Address"}, {"name": "amount", "type": "i128"}, {"name": "expiration_ledger", "type": "u32"}], "outputs": []},
     {"name": "balance", "inputs": [{"name": "id", "type": "Address"}], "outputs": [{"type": "i128"}]}
   ],
   "events": [
     {"name": "transfer", "fields": [{"name": "from", "type": "Address"}, {"name": "to", "type": "Address"}, {"name": "amount", "type": "i128"}]},
     {"name": "mint", "fields": [{"name": "to", "type": "Address"}, {"name": "amount", "type": "i128"}]},
     {"name": "burn", "fields": [{"name": "from", "type": "Address"}, {"name": "amount", "type": "i128"}]},
     {"name": "approve", "fields": [{"name": "from", "type": "Address"}, {"name": "spender", "type": "Address"}, {"name": "amount", "type": "i128"}, {"name": "expiration_ledger", "type": "u32"}]}
   ]
 }',
 '', true, NOW())
ON CONFLICT (contract_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Demo contract state: token metadata.
-- ---------------------------------------------------------------------------
INSERT INTO contract_state (
    contract_id, ledger, storage, captured_at
)
SELECT
    'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
    ledger,
    '{
      "name": "Demo USDC",
      "symbol": "USDC",
      "decimals": 7,
      "admin": "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"
    }',
    NOW() - INTERVAL '1 hour'
FROM _seed_cfg
ON CONFLICT (contract_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Demo contract data: per-holder balances.
-- ---------------------------------------------------------------------------
INSERT INTO contract_data (
    contract_id, key_hash, key, key_xdr, durability, ledger, value, label, captured_at
)
SELECT
    -- Balance for holder 1
    'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    '["Balance", "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"]',
    'AAAADwAAAAdCYWxhbmNlAAAAABIAAAAAAAAAAH59I7V7zkCufqfB+5mWMsZo0Cg2Xdb9XcNQ6jwUT8ww',
    'persistent',
    ledger + 3,
    '"5000000000000000"',
    'balance',
    NOW() - INTERVAL '30 minutes'
FROM _seed_cfg

UNION ALL

SELECT
    -- Balance for holder 2
    'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM',
    'a1b2c3d4e5f6708192a3b4c5d6e7f8091a2b3c4d5e6f7081920a3b4c5d6e7f80',
    '["Balance", "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC3I7"]',
    'AAAADwAAAAdCYWxhbmNlAAAAABIAAAAAAAAAAFdVoTp9j0Xy/sM4LX6/ZwXrBlekpM1GMoNk+smkuPgA',
    'persistent',
    ledger + 3,
    '"10000000000000000"',
    'balance',
    NOW() - INTERVAL '30 minutes'
FROM _seed_cfg
ON CONFLICT (contract_id, key_hash) DO NOTHING;

COMMIT;

SELECT 'Seed data inserted successfully!' AS status,
       (SELECT ledger FROM _seed_cfg)      AS seed_ledger,
       (SELECT COUNT(*) FROM events)        AS events_count,
       (SELECT COUNT(*) FROM contract_specs) AS specs_count,
       (SELECT COUNT(*) FROM contract_state) AS state_count,
       (SELECT COUNT(*) FROM contract_data)  AS data_count;
