#!/usr/bin/env python3
"""
Lint check script to prevent sensitive credentials and raw config objects
from being logged directly in tracing calls.
"""

import sys
import re
from pathlib import Path

SENSITIVE_PATTERNS = [
    (re.compile(r'tracing::\w+!\([^)]*\bconfig\s*=\s*\?config'), "Do not log unredacted `?config` directly"),
    (re.compile(r'tracing::\w+!\([^)]*\bdatabase_url\s*='), "Do not log raw `database_url` directly"),
    (re.compile(r'tracing::\w+!\([^)]*%database_url'), "Do not log raw `%database_url` directly"),
    (re.compile(r'tracing::\w+!\([^)]*\?database_url'), "Do not log raw `?database_url` directly"),
]

def check_file(path: Path) -> int:
    errors = 0
    text = path.read_text(encoding="utf-8")
    for line_idx, line in enumerate(text.splitlines(), start=1):
        for pattern, msg in SENSITIVE_PATTERNS:
            if pattern.search(line):
                print(f"ERROR: {path}:{line_idx}: {msg}: {line.strip()}", file=sys.stderr)
                errors += 1
    return errors

def main() -> int:
    root = Path(__file__).resolve().parent.parent / "crates"
    total_errors = 0
    for rs_file in root.glob("**/*.rs"):
        total_errors += check_file(rs_file)
    
    if total_errors > 0:
        print(f"\nFound {total_errors} sensitive logging violation(s).", file=sys.stderr)
        return 1
    
    print("No sensitive logging violations found.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
