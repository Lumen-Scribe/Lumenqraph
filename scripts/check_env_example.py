#!/usr/bin/env python3
"""Fail when a Rust source reads an environment variable that .env.example
does not document.

Extracts every key passed as a string literal to an env-reading call
(`std::env::var("KEY")`, `env_parse("KEY", …)`, `env_bool(…)`, …) under
crates/, and checks that each one appears in .env.example — either set
(`KEY=…`) or commented out (`# KEY=…`). Variables that only matter to tests
or tooling are listed in ALLOWLIST.

Usage:
  python3 scripts/check_env_example.py [--repo-root PATH]

Exit codes:
  0 — every runtime variable is documented
  1 — at least one variable is missing from .env.example
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Calls whose first string-literal argument is an environment variable name.
# Covers std::env::var / var_os and the per-crate config helpers
# (env, env_parse, env_parse_i64, env_bool, env_string, webhooks' `parse`).
ENV_CALL = re.compile(
    r"""\b(?:std::)?(?:env::)?
        (?:var|var_os|env|env_parse\w*|env_bool|env_string|parse)
        (?:::<[^>]*>)?
        \(\s*"([A-Z][A-Z0-9_]+)"
    """,
    re.VERBOSE,
)

# Documented keys: `KEY=` at the start of a line, optionally commented out.
DOC_KEY = re.compile(r"^\s*#?\s*([A-Z][A-Z0-9_]+)=", re.MULTILINE)

# Read by the code but intentionally absent from .env.example.
ALLOWLIST = {
    # Test-only: points Postgres-backed tests at a throwaway database.
    "TEST_DATABASE_URL",
}

# Test code reads/sets variables freely; it is not runtime configuration.
SKIP_DIRS = {"tests", "benches", "target"}


def env_keys_in_sources(crates_dir: Path) -> dict[str, list[str]]:
    """Map each env key read under crates_dir to the files that read it."""
    found: dict[str, list[str]] = {}
    for path in sorted(crates_dir.rglob("*.rs")):
        if SKIP_DIRS.intersection(path.relative_to(crates_dir).parts):
            continue
        text = path.read_text(encoding="utf-8")
        # Drop `#[cfg(test)] mod tests { … }` blocks: by convention they sit
        # at the end of the file, so everything after the marker is test code.
        marker = text.find("#[cfg(test)]\nmod tests")
        if marker != -1:
            text = text[:marker]
        for key in ENV_CALL.findall(text):
            found.setdefault(key, []).append(str(path.relative_to(crates_dir.parent)))
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="repository root (default: parent of this script's directory)",
    )
    args = parser.parse_args()

    env_example = args.repo_root / ".env.example"
    documented = set(DOC_KEY.findall(env_example.read_text(encoding="utf-8")))
    used = env_keys_in_sources(args.repo_root / "crates")

    missing = sorted(k for k in used if k not in documented and k not in ALLOWLIST)
    if missing:
        print(f"{len(missing)} environment variable(s) read by the code are missing from .env.example:")
        for key in missing:
            files = ", ".join(sorted(set(used[key])))
            print(f"  {key}  ({files})")
        print(
            "\nDocument each one in .env.example (with its default and a one-line "
            "explanation), or add it to ALLOWLIST in scripts/check_env_example.py "
            "if it is test-only."
        )
        return 1

    print(f"OK: all {len(used)} environment variables read by the code are documented in .env.example.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
