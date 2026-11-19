#!/usr/bin/env python3
"""Check that the Prometheus monitoring config only relies on metrics that a
scraped Lumenqraph service actually exposes.

For monitoring/prometheus.yml and monitoring/prometheus_alerts.yml:

1. Every Lumenqraph scrape job maps to a known service crate (SERVICE_CRATES).
2. Every `lumenqraph_*` metric referenced in a rule `expr` is emitted by at
   least one crate that has a scrape job. Metrics referenced only in
   annotations (summaries/descriptions) are not checked.

Emission is detected by searching each crate's non-test Rust source for the
metric name. promtool syntax checks run separately in CI.

Usage:
  python3 scripts/check_alert_metrics.py [--monitoring-dir PATH]

Exit codes: 0 when every metric is emitted by a scraped service, 1 otherwise.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent

# Scrape job name -> crate whose /metrics it scrapes.
SERVICE_CRATES = {
    "lumenqraph-api": "lumenqraph-api",
    "lumenqraph-indexer": "lumenqraph-indexer",
    "lumenqraph-webhooks": "lumenqraph-webhooks",
}

METRIC = re.compile(r"\blumenqraph_[a-z0-9_]+\b")
TEST_MARKER = "#[cfg(test)]\nmod tests"


def emitted_metrics(crate_dir: Path) -> set[str]:
    """Metric names that appear in the crate's non-test Rust source."""
    found: set[str] = set()
    for path in sorted((crate_dir / "src").rglob("*.rs")):
        text = path.read_text(encoding="utf-8")
        marker = text.find(TEST_MARKER)
        if marker != -1:
            text = text[:marker]
        found.update(METRIC.findall(text))
    return found


def rule_expressions(rules_doc: dict) -> set[str]:
    """Metric names used in rule `expr` fields (not annotations)."""
    used: set[str] = set()
    for group in rules_doc.get("groups", []):
        for rule in group.get("rules", []):
            expr = rule.get("expr")
            if expr:
                used.update(METRIC.findall(str(expr)))
    return used


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--monitoring-dir",
        type=Path,
        default=ROOT / "monitoring",
        help="directory containing prometheus.yml and prometheus_alerts.yml",
    )
    args = parser.parse_args()

    prom = yaml.safe_load((args.monitoring_dir / "prometheus.yml").read_text(encoding="utf-8"))
    rules = yaml.safe_load((args.monitoring_dir / "prometheus_alerts.yml").read_text(encoding="utf-8"))

    errors: list[str] = []

    scraped_crates: set[str] = set()
    for job in prom.get("scrape_configs", []):
        name = job.get("job_name", "")
        if not name.startswith("lumenqraph-"):
            continue
        crate = SERVICE_CRATES.get(name)
        if crate is None:
            errors.append(f"scrape job {name!r} has no entry in SERVICE_CRATES")
            continue
        scraped_crates.add(crate)

    exposed: dict[str, set[str]] = {
        crate: emitted_metrics(ROOT / "crates" / crate) for crate in SERVICE_CRATES.values()
    }

    for metric in sorted(rule_expressions(rules)):
        emitters = sorted(c for c in scraped_crates if metric in exposed[c])
        if not emitters:
            owners = sorted(c for c, names in exposed.items() if metric in names)
            hint = f" (emitted only by {', '.join(owners)}, which is not scraped)" if owners else ""
            errors.append(f"alert metric {metric!r} is not emitted by any scraped service{hint}")

    if errors:
        print("Alert metric check failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print(
        f"OK: every alert expression metric is emitted by a scraped service "
        f"({', '.join(sorted(scraped_crates))})."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
