#!/usr/bin/env python3
"""Keep the webhook verification examples in docs/WEBHOOKS.md honest.

1. Runs the Node, Python and Go test suites in docs/examples/webhooks/. Each
   one verifies the committed cross-language test vector (accepts it, rejects
   a tampered body, a wrong secret and a stale delivery).
2. Checks that every example snippet shown in docs/WEBHOOKS.md is byte-for-byte
   the code between the `--- begin doc example ---` / `--- end doc example ---`
   markers in its source file, so the docs cannot drift from tested code.

The Rust example is covered by `verifies_committed_cross_language_vector` in
crates/lumenqraph-core/src/crypto.rs, which the build-test job runs with
`cargo test`.

Exit codes: 0 when everything passes, 1 otherwise.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "docs" / "examples" / "webhooks"
DOC = ROOT / "docs" / "WEBHOOKS.md"

MARKER = re.compile(
    r"^.*--- begin doc example ---\n(?P<body>.*?)^.*--- end doc example ---",
    re.MULTILINE | re.DOTALL,
)

# (example file, language tag used in the doc's fenced block)
SNIPPETS = [
    (EXAMPLES / "verify.js", "javascript"),
    (EXAMPLES / "verify.py", "python"),
    (EXAMPLES / "go" / "verify.go", "go"),
]

# (label, working directory, command)
SUITES = [
    ("node", EXAMPLES, ["node", "--test", "verify.test.js"]),
    ("python", EXAMPLES, [sys.executable, "-m", "unittest", "test_verify.py"]),
    ("go", EXAMPLES / "go", ["go", "test", "./..."]),
]


def run_suites() -> list[str]:
    failures = []
    for label, cwd, cmd in SUITES:
        tool = cmd[0]
        if tool != sys.executable and shutil.which(tool) is None:
            failures.append(f"{label}: '{tool}' is not installed")
            continue
        print(f"== {label}: {' '.join(cmd)} (in {cwd.relative_to(ROOT)})")
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        if result.returncode != 0:
            failures.append(f"{label}: test suite failed (exit {result.returncode})")
    return failures


def check_doc_snippets() -> list[str]:
    failures = []
    doc = DOC.read_text(encoding="utf-8")
    for path, lang in SNIPPETS:
        match = MARKER.search(path.read_text(encoding="utf-8"))
        if match is None:
            failures.append(f"{path.relative_to(ROOT)}: doc-example markers not found")
            continue
        code = match.group("body").rstrip("\n")
        fenced = f"```{lang}\n{code}\n```"
        if fenced not in doc:
            failures.append(
                f"docs/WEBHOOKS.md: the {lang} snippet no longer matches "
                f"{path.relative_to(ROOT)}; copy the code between the doc-example markers"
            )
        else:
            print(f"OK doc snippet matches {path.relative_to(ROOT)}")
    return failures


def main() -> int:
    failures = run_suites() + check_doc_snippets()
    if failures:
        print("\nWebhook example checks failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("\nOK: webhook verification examples pass against the test vector and match the docs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
