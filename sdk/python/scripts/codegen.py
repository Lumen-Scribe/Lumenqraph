#!/usr/bin/env python3
"""Generate ``lumenqraph/models.py`` from the repository's ``openapi.yaml``.

The models are produced by `datamodel-code-generator`_ with a pinned set of
options so output is deterministic. CI runs this with ``--check``, which
regenerates into a temporary file and fails if it differs from the committed
module, the Python counterpart of the TypeScript SDK's ``codegen:check``.

Usage (from ``sdk/python``)::

    pip install -e ".[codegen]"
    python scripts/codegen.py          # rewrite lumenqraph/models.py
    python scripts/codegen.py --check  # exit 1 if it is out of date

.. _datamodel-code-generator: https://github.com/koxudaxi/datamodel-code-generator
"""

import argparse
import difflib
import subprocess
import sys
import tempfile
from pathlib import Path

SDK_DIR = Path(__file__).resolve().parent.parent
SPEC = SDK_DIR.parent.parent / "openapi.yaml"
OUTPUT = SDK_DIR / "lumenqraph" / "models.py"

HEADER = (
    "# Generated from openapi.yaml by scripts/codegen.py. DO NOT EDIT.\n"
    "# Regenerate with `python scripts/codegen.py`; CI checks for drift."
)

ARGS = [
    "--input-file-type", "openapi",
    "--output-model-type", "pydantic_v2.BaseModel",
    "--target-python-version", "3.10",
    "--use-standard-collections",
    "--use-schema-description",
    "--use-field-description",
    "--field-constraints",
    "--strict-nullable",
    "--enum-field-as-literal", "all",
    "--disable-timestamp",
    "--formatters", "builtin",
    "--custom-file-header", HEADER,
]


def generate(output: Path) -> None:
    cmd = ["datamodel-codegen", "--input", str(SPEC), "--output", str(output), *ARGS]
    subprocess.run(cmd, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true",
                        help="fail if lumenqraph/models.py is out of date")
    opts = parser.parse_args()

    if not opts.check:
        generate(OUTPUT)
        print(f"wrote {OUTPUT.relative_to(SDK_DIR)}")
        return 0

    with tempfile.TemporaryDirectory() as tmp:
        fresh = Path(tmp) / "models.py"
        generate(fresh)
        expected = fresh.read_text(encoding="utf-8")
    actual = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
    if expected == actual:
        print("lumenqraph/models.py is up to date with openapi.yaml")
        return 0

    sys.stdout.writelines(difflib.unified_diff(
        actual.splitlines(keepends=True), expected.splitlines(keepends=True),
        fromfile="lumenqraph/models.py (committed)",
        tofile="lumenqraph/models.py (from openapi.yaml)",
    ))
    print("\nmodels.py drifted from openapi.yaml; run `python scripts/codegen.py`",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
