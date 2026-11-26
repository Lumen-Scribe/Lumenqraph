#!/usr/bin/env python3
"""Reject tracked editor backups and case-insensitive path collisions."""

from collections import defaultdict
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
BACKUP_SUFFIXES = (".bak", ".orig", ".rej", ".swp")


def main() -> int:
    tracked = subprocess.check_output(
        ["git", "ls-files", "-z"], cwd=ROOT
    ).decode("utf-8").split("\0")
    tracked = [path for path in tracked if path]

    backup_paths = [
        path for path in tracked if path.lower().endswith(BACKUP_SUFFIXES)
    ]
    folded_paths: dict[str, list[str]] = defaultdict(list)
    for path in tracked:
        folded_paths[path.casefold()].append(path)
    collisions = [
        paths for paths in folded_paths.values() if len(paths) > 1
    ]

    failed = False
    if backup_paths:
        print("Tracked backup files are not allowed:", file=sys.stderr)
        print("\n".join(f"  {path}" for path in sorted(backup_paths)), file=sys.stderr)
        failed = True
    if collisions:
        print("Case-insensitive tracked path collisions are not allowed:", file=sys.stderr)
        for paths in sorted(collisions):
            print(f"  {', '.join(sorted(paths))}", file=sys.stderr)
        failed = True
    if failed:
        return 1

    print("Tracked paths contain no backup files or case-insensitive collisions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
