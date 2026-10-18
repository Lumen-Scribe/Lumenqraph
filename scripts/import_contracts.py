#!/usr/bin/env python3
"""Import a list of Soroban contract ids from Stellar.Expert.

Stellar.Expert already tracks every deployed contract; this pulls a batch of
them so Lumenqraph can index them. Prints a comma-separated list suitable for
the indexer's CONTRACT_IDS env var.

Lumenqraph discovers contracts from the events it indexes, so there is nothing
to POST here: seeding an instance is a matter of putting the printed ids in its
CONTRACT_IDS and starting it.

Note on ordering: the `/explorer/<net>/contract` endpoint only orders by
contract id (ascending or descending) — it exposes no "by rating/activity"
sort. So the useful lever for a *populated* demo is `--active-only` (the
default), which keeps only contracts that have actually emitted events (the
data an event indexer can show). Pages are scanned in `--order` id-order until
`--limit` such contracts are collected.

Note on `CONTRACT_IDS`: `parse_contract_ids` in `lumenqraph-core` accepts at
most 25 ids per instance (getEvents takes 5 filters x 5 ids). When `--limit`
asks for more than that, the ids are printed as several comma-separated lines,
one group of at most 25 per line, so each line can be handed to a different
indexer instance.

Usage:
  python3 scripts/import_contracts.py [--network public|testnet]
                                      [--order desc|asc]
                                      [--limit N]
                                      [--include-empty]

Examples:
  # 20 mainnet contracts that have emitted events (good for a demo):
  python3 scripts/import_contracts.py --network public --limit 20
  # include contracts with zero events too:
  python3 scripts/import_contracts.py --include-empty --limit 20
  # 60 contracts: prints three lines of 25, 25 and 10 ids, one per instance:
  python3 scripts/import_contracts.py --limit 60
"""
import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request

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

# Mirrors `MAX_CONTRACT_IDS` in `lumenqraph-core/src/xdr.rs`: a single instance's
# CONTRACT_IDS may hold at most 25 ids (5 getEvents filters x 5 ids each).
MAX_IDS_PER_GROUP = 25


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


def group_ids(contract_ids: list[str], size: int = MAX_IDS_PER_GROUP) -> list[list[str]]:
    """Split ids into groups that each fit one instance's CONTRACT_IDS."""
    return [contract_ids[i : i + size] for i in range(0, len(contract_ids), size)]


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
    args = ap.parse_args()

    try:
        records = fetch(args.network, args.order, args.limit, not args.include_empty)
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if not records:
        print("no contracts returned", file=sys.stderr)
        return 1

    # Keep only well-formed C-strkeys: `parse_contract_ids` rejects the whole
    # CONTRACT_IDS value over a single bad entry, so never emit one.
    ids = []
    for r in records:
        cid = r["contract"]
        if not validate_contract_id(cid):
            print(f"skipping malformed contract id {cid!r}", file=sys.stderr)
            continue
        ids.append(cid)
    if not ids:
        print("no usable contract ids returned", file=sys.stderr)
        return 1

    total_events = sum(r.get("events") or 0 for r in records)
    scope = "all" if args.include_empty else "active"
    print(
        f"fetched {len(ids)} {args.network} contracts ({scope}, "
        f"{total_events} events total)",
        file=sys.stderr,
    )

    groups = group_ids(ids)
    if len(groups) > 1:
        print(
            f"note: {len(ids)} ids exceed the {MAX_IDS_PER_GROUP}-id CONTRACT_IDS "
            f"cap, so they are printed as {len(groups)} groups of at most "
            f"{MAX_IDS_PER_GROUP}, one per indexer instance",
            file=sys.stderr,
        )

    # One comma-separated line per group, each a valid CONTRACT_IDS value.
    for group in groups:
        print(",".join(group))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
