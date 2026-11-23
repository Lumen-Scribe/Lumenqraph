import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from validate_dashboard_metrics import main


class MetricValidationTest(unittest.TestCase):
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
