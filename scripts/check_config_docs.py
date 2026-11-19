#!/usr/bin/env python3
"""Fail when the Rust code reads an environment variable that
docs/CONFIGURATION.md does not document.

Every UPPER_SNAKE string literal in runtime (non-test) Rust source under
crates/ is treated as a candidate environment variable. Scanning literals,
rather than only the arguments of known env helpers, also catches names passed
through helpers (`env_parse_override`, `db_setting`), struct tables and loops
such as `for key in ["METRICS_BEARER_TOKEN", "METRICS_API_KEY"]`.

A candidate counts as documented when docs/CONFIGURATION.md mentions it in
backticks, e.g. a `#### \\`NAME\\`` heading or a table cell.

Test code is excluded: `#[cfg(test)]` items are removed by brace matching, and
tests/ and benches/ directories are skipped. Compile-time names that are never
read at runtime are listed in ALLOWLIST.

Usage:
  python3 scripts/check_config_docs.py [--repo-root PATH]

Exit codes:
  0 - every runtime environment variable is documented
  1 - at least one variable is missing from docs/CONFIGURATION.md
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Any UPPER_SNAKE string literal, e.g. "RATE_LIMIT_BACKEND".
LITERAL = re.compile(r'"([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)"')

# A documented variable is named in backticks: `NAME`.
DOCUMENTED = re.compile(r"`([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)`")

# Names that look like env vars but are never read at runtime.
ALLOWLIST = {
    "CARGO_PKG_VERSION": "compile-time env!() macro, not runtime configuration",
    "LUMENQRAPH_BUILD_TIME": "compile-time option_env!() macro, not runtime configuration",
    "LUMENQRAPH_GIT_SHA": "compile-time option_env!() macro, not runtime configuration",
}

# Test code reads and sets variables freely; it is not runtime configuration.
SKIP_DIRS = {"tests", "benches", "target"}

CFG_TEST = "#[cfg(test)]"


def strip_test_items(text: str) -> str:
    """Remove every `#[cfg(test)]` item (a module, fn, impl or use) from text.

    The item body is removed by matching braces. Items without a body end at
    the first ';'. This keeps production code that follows a test-only helper
    in the same file.
    """
    out: list[str] = []
    pos = 0
    while True:
        start = text.find(CFG_TEST, pos)
        if start == -1:
            out.append(text[pos:])
            break
        out.append(text[pos:start])
        cursor = start + len(CFG_TEST)
        end_match = re.search(r"[{;]", text[cursor:])
        if end_match is None:
            break
        if end_match.group() == ";":
            pos = cursor + end_match.end()
            continue
        depth = 0
        p = cursor + end_match.start()
        while p < len(text):
            if text[p] == "{":
                depth += 1
            elif text[p] == "}":
                depth -= 1
                if depth == 0:
                    p += 1
                    break
            p += 1
        pos = p
    return "".join(out)


def runtime_literals(crates_dir: Path) -> dict[str, list[str]]:
    """Map each UPPER_SNAKE literal in runtime Rust code to the files using it."""
    found: dict[str, list[str]] = {}
    for path in sorted(crates_dir.rglob("*.rs")):
        if SKIP_DIRS.intersection(path.relative_to(crates_dir).parts):
            continue
        text = strip_test_items(path.read_text(encoding="utf-8"))
        for name in set(LITERAL.findall(text)):
            found.setdefault(name, []).append(str(path.relative_to(crates_dir.parent)))
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

    doc = args.repo_root / "docs" / "CONFIGURATION.md"
    documented = set(DOCUMENTED.findall(doc.read_text(encoding="utf-8")))
    used = runtime_literals(args.repo_root / "crates")

    candidates = {k: v for k, v in used.items() if k not in ALLOWLIST}
    missing = sorted(k for k in candidates if k not in documented)
    if missing:
        print(f"{len(missing)} environment variable(s) read by the code are not documented in docs/CONFIGURATION.md:")
        for key in missing:
            files = ", ".join(sorted(set(candidates[key])))
            print(f"  {key}  ({files})")
        print(
            "\nAdd a `#### `NAME`` section for each one to docs/CONFIGURATION.md "
            "(default, valid range, service, and security impact). If a literal is "
            "not an environment variable, add it to ALLOWLIST in scripts/check_config_docs.py "
            "with a reason."
        )
        return 1

    print(f"OK: all {len(candidates)} environment variables read by the code are documented in docs/CONFIGURATION.md.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
