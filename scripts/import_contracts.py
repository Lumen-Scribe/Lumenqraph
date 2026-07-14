#!/usr/bin/env python3
"""Import a list of Soroban contract ids from Stellar.Expert.

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

Examples:
  # 20 mainnet contracts that have emitted events (good for a demo):
  python3 scripts/import_contracts.py --network public --limit 20
  # include contracts with zero events too:
  python3 scripts/import_contracts.py --include-empty --limit 20
"""
import argparse
import json
import sys
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


def fetch(network: str, order: str, limit: int, active_only: bool) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    cursor = None
    for _ in range(MAX_PAGES):
        if len(out) >= limit:
            break
        url = f"{API}/{network}/contract?order={order}&limit={PAGE_SIZE}"
        if cursor:
            url += f"&cursor={cursor}"
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
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
    except Exception as e:  # noqa: BLE001
        print(f"error: {e}", file=sys.stderr)
        return 1

    if not records:
        print("no contracts returned", file=sys.stderr)
        return 1

    ids = [r["contract"] for r in records]
    total_events = sum(r.get("events") or 0 for r in records)
    # Human-readable summary to stderr; the machine-usable list to stdout.
    scope = "all" if args.include_empty else "active"
    print(
        f"fetched {len(ids)} {args.network} contracts ({scope}, "
        f"{total_events} events total)",
        file=sys.stderr,
    )
    print(",".join(ids))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
