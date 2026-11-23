import contextlib
import io
import json
import re
import tempfile
import unittest
from pathlib import Path

from validate_dashboard_metrics import extract_metrics_from_rust, main


class MetricValidationTest(unittest.TestCase):
    def test_exporters_have_disjoint_service_namespaces(self):
        root = Path(__file__).resolve().parents[1]
        inventory = (root / "monitoring" / "README.md").read_text()
        exported = set()
        for service in ("api", "indexer", "webhooks"):
            metrics = extract_metrics_from_rust(str(root / "crates" / f"lumenqraph-{service}" / "src"))
            self.assertTrue(metrics)
            self.assertFalse(exported & metrics)
            for metric in metrics:
                self.assertTrue(metric.startswith(f"lumenqraph_{service}_"), metric)
                family = metric
                if f"| `{family}` |" not in inventory:
                    family = re.sub(r"_(bucket|sum|count)$", "", metric)
                self.assertIn(f"| `{family}` |", inventory)
            exported.update(metrics)

    def test_dashboard_and_alerts_require_exported_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "monitoring").mkdir()
            (root / "crates" / "indexer").mkdir(parents=True)
            (root / "crates" / "indexer" / "http.rs").write_text(
                'let response = "# TYPE lumenqraph_duration histogram\\n";\n'
                'let unrelated = "lumenqraph_unknown";\n'
            )
            dashboard = root / "monitoring" / "grafana_dashboard.json"
            alerts = root / "monitoring" / "prometheus_alerts.yml"
            for dashboard_expr, alert_expr, expected in (
                ("lumenqraph_duration_bucket", "lumenqraph_duration_sum + lumenqraph_duration_count", 0),
                ("lumenqraph_unknown", "lumenqraph_duration_count", 1),
                ("lumenqraph_duration_bucket", "lumenqraph_unknown", 1),
            ):
                with self.subTest(dashboard=dashboard_expr, alert=alert_expr):
                    dashboard.write_text(json.dumps({"panels": [{"targets": [{"expr": dashboard_expr}]}]}))
                    alerts.write_text(f"rules:\n  - alert: Test\n    expr: |\n      {alert_expr}\n")
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(main(root), expected)
