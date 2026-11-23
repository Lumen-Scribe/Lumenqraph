#!/usr/bin/env python3
"""Validate that Grafana dashboard metrics match those defined in Rust source code.

This script ensures dashboard panels reference only metrics that actually exist
in the codebase, catching drift early.
"""
import json
import re
import sys
from pathlib import Path


def extract_metrics_from_dashboard(dashboard_path: str) -> set[str]:
    """Extract all metric names referenced in Grafana dashboard."""
    with open(dashboard_path) as f:
        dashboard = json.load(f)

    metrics = set()
    for panel in dashboard.get("panels", []):
        for target in panel.get("targets", []):
            expr = target.get("expr", "")
            if expr:
                # Extract metric names from Prometheus expressions
                # Patterns: metric_name, rate(metric_name), histogram_quantile(...metric_name...)
                found = re.findall(r"\b(lumenqraph_\w+)\b", expr)
                metrics.update(found)

    return metrics


def extract_metrics_from_rust(rust_dir: str) -> set[str]:
    """Extract all metric names defined in Rust source code."""
    metrics = set()

    # Scan every Rust source file so counters defined outside `metrics.rs`
    # (e.g. `store.rs`, `retention.rs`, `http.rs`) are also discovered.
    for rust_file in Path(rust_dir).rglob("*.rs"):
        with open(rust_file) as f:
            content = f.read()
            for metric, kind in re.findall(r"# TYPE (lumenqraph_\w+) (\w+)", content):
                metrics.add(metric)
                if kind == "histogram":
                    metrics.update(metric + suffix for suffix in ("_bucket", "_sum", "_count"))

    return metrics


def main(repo_root: Path | None = None) -> int:
    repo_root = repo_root or Path(__file__).resolve().parent.parent
    dashboard_path = repo_root / "monitoring" / "grafana_dashboard.json"
    rust_crates = repo_root / "crates"

    if not dashboard_path.exists():
        print(f"error: dashboard not found at {dashboard_path}", file=sys.stderr)
        return 1

    try:
        dashboard = extract_metrics_from_dashboard(str(dashboard_path))
        alerts = (repo_root / "monitoring" / "prometheus_alerts.yml").read_text()
        # Match inline expressions and indented YAML block or multiline scalars.
        expressions = re.findall(r"(?m)^([ ]*)expr:[ ]*([^\n]*(?:\n\1[ ]+[^\n]*)*)", alerts)
        alert_metrics = set(re.findall(r"\b(lumenqraph_\w+)\b", " ".join(expr for _, expr in expressions)))
        exported = extract_metrics_from_rust(str(rust_crates))
    except (OSError, ValueError) as e:
        print(f"error reading files: {e}", file=sys.stderr)
        return 1

    failed = False
    for source, metrics in (("dashboard", dashboard), ("alerts", alert_metrics)):
        missing = metrics - exported
        if missing:
            print(f"error: {source} references metrics not found in code: {', '.join(sorted(missing))}", file=sys.stderr)
            failed = True
        else:
            print(f"{source} references {len(metrics)} valid metrics")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
