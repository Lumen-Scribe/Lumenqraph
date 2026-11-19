#!/usr/bin/env python3
"""Fixture tests for scripts/check_sensitive_logging.py (#645).

Each positive fixture is a logging call the lint must flag; each negative
fixture is a call it must accept. Run with:

    python3 scripts/test_check_sensitive_logging.py
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import check_sensitive_logging as lint  # noqa: E402


def flagged(source: str) -> bool:
    return bool(lint.find_violations(source))


class PositiveFixtures(unittest.TestCase):
    """Calls that must be flagged."""

    def test_bare_macro_with_raw_rpc_url(self):
        # The exact pattern named in #645's acceptance criteria.
        self.assertTrue(flagged('info!(rpc = %rpc_url);'))

    def test_bare_macro_with_config_field_access(self):
        self.assertTrue(flagged('info!(rpc = %config.rpc_url, "starting");'))

    def test_qualified_macro_with_database_url(self):
        self.assertTrue(flagged('tracing::warn!(database_url = %database_url, "db");'))

    def test_multi_line_call_is_checked_as_one_unit(self):
        src = (
            "info!(\n"
            '    "connecting to node",\n'
            "    rpc = %config.rpc_url,\n"
            ");\n"
        )
        self.assertTrue(flagged(src))

    def test_inline_format_capture_of_secret(self):
        self.assertTrue(flagged('info!("connected to {rpc_url}");'))

    def test_whole_config_debug_dump(self):
        self.assertTrue(flagged('info!(config = ?config, "effective configuration");'))

    def test_token_and_secret_values(self):
        self.assertTrue(flagged("debug!(token = ?api_token);"))
        self.assertTrue(flagged("error!(secret = %webhook_secret, \"bad\");"))

    def test_encryption_key(self):
        self.assertTrue(flagged("info!(key = %webhook_encryption_key);"))

    def test_string_with_string_escapes_still_flags_later_arg(self):
        self.assertTrue(flagged(r'info!("a \" quote", rpc = %rpc_url);'))


class NegativeFixtures(unittest.TestCase):
    """Calls that must be accepted."""

    def test_redacted_helper_is_accepted(self):
        self.assertFalse(flagged('info!(rpc = %redact_url(&rpc_url), "ok");'))
        self.assertFalse(flagged('info!(db = %redact_database_url(&database_url));'))

    def test_plain_fields_are_accepted(self):
        self.assertFalse(flagged('info!(rows = backfilled, "encrypted legacy rows");'))

    def test_sensitive_word_only_inside_message_text_is_accepted(self):
        self.assertFalse(flagged('info!("token refresh complete");'))
        self.assertFalse(flagged('warn!(reason, "failed to decrypt secret (NULL or wrong key)");'))

    def test_named_config_fields_are_accepted(self):
        self.assertFalse(flagged('info!(batch_size = config.batch_size, "tick");'))

    def test_non_log_macros_are_ignored(self):
        self.assertFalse(flagged("let x = format!(\"{}\", rpc_url);"))
        self.assertFalse(flagged("assert_eq!(rpc_url, other);"))

    def test_log_error_helper_is_not_a_log_macro(self):
        self.assertFalse(flagged("log_error!(rpc_url);"))


class RepositoryIsClean(unittest.TestCase):
    def test_crates_have_no_violations(self):
        root = Path(__file__).resolve().parent.parent / "crates"
        problems = []
        for rs_file in sorted(root.glob("**/*.rs")):
            for line, msg, snippet in lint.find_violations(rs_file.read_text(encoding="utf-8")):
                problems.append(f"{rs_file}:{line}: {msg}: {snippet}")
        self.assertEqual(problems, [], "\n".join(problems))


if __name__ == "__main__":
    unittest.main()
