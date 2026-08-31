#!/usr/bin/env python3
"""Import a list of Soroban contract ids from Stellar.Expert and optionally import into Lumenqraph.

Stellar.Expert already tracks every deployed contract; this pulls a batch of
them so Lumenqraph can index them. Prints a comma-separated list suitable for
the indexer's CONTRACT_IDS env var.

Note on ordering: the `/explorer/<net>/contract` endpoint only orders by
contract id (ascending or descending) — it exposes no "by rating/activity"
sort. So the useful lever for a *populated* demo is `--active-only` (the
default), which keeps only contracts that have actually emitted events (the
data an event indexer can show). Pages are scanned in `--order` id-order until
`--limit` such contracts are collected.

Usage:
  python3 scripts/import_contracts.py [--network public|testnet]
                                      [--order desc|asc]
                                      [--limit N]
                                      [--include-empty]
                                      [--api-url <url>]

Examples:
  # 20 mainnet contracts that have emitted events (good for a demo):
  python3 scripts/import_contracts.py --network public --limit 20
  # include contracts with zero events too:
  python3 scripts/import_contracts.py --include-empty --limit 20
  # import into local Lumenqraph instance:
  python3 scripts/import_contracts.py --api-url http://localhost:8080 --limit 20
"""
import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from typing import Optional

API = "https://api.stellar.expert/explorer"

# Stellar.Expert's WAF 403s the default python-urllib User-Agent, so send a
# browser-like one.
HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Lumenqraph contract importer)",
}

# The endpoint caps a page at this size.
PAGE_SIZE = 200
# Guard against scanning the whole ledger when few contracts are active.
MAX_PAGES = 25
# Retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF_SECS = 1
REQUEST_TIMEOUT_SECS = 30

# Contract ID format validation: Stellar contract IDs start with 'C' followed by alphanumeric
CONTRACT_ID_PATTERN = re.compile(r"^C[A-Z2-7]{55}$")


def validate_contract_id(contract_id: str) -> bool:
    """Validate that a contract ID has the correct format (C-strkey)."""
    return CONTRACT_ID_PATTERN.match(contract_id) is not None


def fetch_with_retry(url: str, max_retries: int = MAX_RETRIES) -> dict:
    """Fetch a URL with exponential backoff retry logic."""
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECS) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code >= 500 and attempt < max_retries - 1:
                backoff = INITIAL_BACKOFF_SECS * (2**attempt)
                print(f"HTTP {e.code} on {url}, retrying in {backoff}s...", file=sys.stderr)
                time.sleep(backoff)
                continue
            raise
        except urllib.error.URLError as e:
            if attempt < max_retries - 1:
                backoff = INITIAL_BACKOFF_SECS * (2**attempt)
                print(f"Network error: {e.reason}, retrying in {backoff}s...", file=sys.stderr)
                time.sleep(backoff)
                continue
            raise
    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts")


def fetch(network: str, order: str, limit: int, active_only: bool) -> list[dict]:
    """Fetch contracts from Stellar.Expert with error handling."""
    out: list[dict] = []
    seen: set[str] = set()
    cursor = None
    for _ in range(MAX_PAGES):
        if len(out) >= limit:
            break
        url = f"{API}/{network}/contract?order={order}&limit={PAGE_SIZE}"
        if cursor:
            url += f"&cursor={cursor}"
        try:
            data = fetch_with_retry(url)
        except Exception as e:
            print(f"error fetching contracts: {e}", file=sys.stderr)
            raise
        records = data.get("_embedded", {}).get("records", [])
        if not records:
            break
        for r in records:
            cid = r.get("contract")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            # `events` is an integer count of events the contract has emitted;
            # 0/None means nothing for an event indexer to show.
            if active_only and not r.get("events"):
                continue
            out.append(r)
            if len(out) >= limit:
                break
        cursor = records[-1].get("paging_token")
        if not cursor:
            break
    return out[:limit]


def import_to_lumenqraph(api_url: str, contract_ids: list[str]) -> tuple[int, list[str]]:
    """Import contract IDs into Lumenqraph API. Returns (success_count, failed_ids)."""
    failed_ids = []
    for contract_id in contract_ids:
        if not validate_contract_id(contract_id):
            print(f"invalid contract ID format: {contract_id}", file=sys.stderr)
            failed_ids.append(contract_id)
            continue

        url = f"{api_url}/contracts/{contract_id}"
        try:
            req = urllib.request.Request(url, method="POST", headers={"content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECS) as resp:
                if resp.status not in (200, 201, 204):
                    print(f"failed to import {contract_id}: HTTP {resp.status}", file=sys.stderr)
                    failed_ids.append(contract_id)
        except Exception as e:
            print(f"failed to import {contract_id}: {e}", file=sys.stderr)
            failed_ids.append(contract_id)

    return len(contract_ids) - len(failed_ids), failed_ids


def main() -> int:
    ap = argparse.ArgumentParser(description="Import Soroban contract ids from Stellar.Expert")
    ap.add_argument("--network", default="public", choices=["public", "testnet"])
    ap.add_argument(
        "--order",
        default="desc",
        choices=["desc", "asc"],
        help="contract-id order to scan (the only ordering the API supports)",
    )
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument(
        "--include-empty",
        action="store_true",
        help="also include contracts with zero events (default: active only)",
    )
    ap.add_argument(
        "--api-url",
        type=str,
        default=None,
        help="if provided, import fetched contracts into this Lumenqraph API URL",
    )
    args = ap.parse_args()

    try:
        records = fetch(args.network, args.order, args.limit, not args.include_empty)
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if not records:
        print("no contracts returned", file=sys.stderr)
        return 1

    ids = [r["contract"] for r in records]
    total_events = sum(r.get("events") or 0 for r in records)
    scope = "all" if args.include_empty else "active"
    print(
        f"fetched {len(ids)} {args.network} contracts ({scope}, "
        f"{total_events} events total)",
        file=sys.stderr,
    )

    # Import to Lumenqraph if API URL provided
    if args.api_url:
        success_count, failed_ids = import_to_lumenqraph(args.api_url, ids)
        print(f"imported {success_count}/{len(ids)} contracts to {args.api_url}", file=sys.stderr)
        if failed_ids:
            print(f"failed imports: {', '.join(failed_ids)}", file=sys.stderr)
            return 1
    else:
        # Output CSV list to stdout for use in env vars
        print(",".join(ids))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
