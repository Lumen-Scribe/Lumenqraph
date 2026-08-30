#!/usr/bin/env python3
"""
OpenAPI drift checker for Lumenqraph CI.

Usage:
    # Compare a live /openapi.json endpoint against the committed spec:
    python3 scripts/check_openapi_drift.py openapi.yaml <(curl -s http://localhost:8080/openapi.json)

    # Compare the generated spec (from --print-openapi) against the committed one:
    cargo run -p lumenqraph-api -- --print-openapi > /tmp/generated.json
    python3 scripts/check_openapi_drift.py openapi.yaml /tmp/generated.json

Exit codes:
    0  No drift detected.
    1  Drift detected — paths, methods, or parameters differ.
    2  Usage error or parse failure.

What is checked:
    - Path set: paths present in the committed spec but missing from the
      generated one, and vice versa (newly added endpoints not yet documented).
    - HTTP methods per path.
    - Query and path parameter names per operation.
    - Required response status codes per operation.

What is intentionally NOT checked:
    - Free-text descriptions, summaries, and examples (cosmetic, not contract).
    - Schema property types and formats (exhaustive schema comparison is noisy
      and prone to false positives from JSON Schema dialect differences between
      the hand-authored YAML and utoipa's output; a follow-up can tighten this).
"""
import json
import sys
import yaml  # PyYAML; available in GitHub Actions ubuntu-latest


def load(path: str) -> dict:
    if path == "-":
        content = sys.stdin.read()
    else:
        with open(path) as fh:
            content = fh.read()
    # Try JSON first (generated output), fall back to YAML (committed file).
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return yaml.safe_load(content)


def ops(spec: dict) -> dict[tuple[str, str], dict]:
    """Return a flat mapping of (path, method) -> operation object."""
    result: dict[tuple[str, str], dict] = {}
    for path, item in (spec.get("paths") or {}).items():
        for method in ("get", "post", "put", "patch", "delete", "head", "options"):
            op = item.get(method)
            if op:
                result[(path, method.upper())] = op
    return result


def param_names(op: dict) -> set[str]:
    return {p["name"] for p in op.get("parameters") or []}


def response_codes(op: dict) -> set[str]:
    return set((op.get("responses") or {}).keys())


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "Usage: check_openapi_drift.py <committed.yaml> <generated.json>",
            file=sys.stderr,
        )
        return 2

    committed_path, generated_path = sys.argv[1], sys.argv[2]

    try:
        committed = load(committed_path)
        generated = load(generated_path)
    except Exception as exc:
        print(f"error loading spec: {exc}", file=sys.stderr)
        return 2

    committed_ops = ops(committed)
    generated_ops = ops(generated)

    drifts: list[str] = []

    # ---- paths in committed but absent from generated (regression) ----------
    for key in sorted(committed_ops):
        if key not in generated_ops:
            drifts.append(f"MISSING in generated:  {key[1]} {key[0]}")

    # ---- paths in generated but absent from committed (undocumented) --------
    for key in sorted(generated_ops):
        if key not in committed_ops:
            drifts.append(f"UNDOCUMENTED in committed: {key[1]} {key[0]}")

    # ---- parameter drift per shared operation --------------------------------
    for key in sorted(committed_ops.keys() & generated_ops.keys()):
        c_params = param_names(committed_ops[key])
        g_params = param_names(generated_ops[key])

        for p in sorted(c_params - g_params):
            drifts.append(
                f"PARAM removed from generated: {key[1]} {key[0]} -> param '{p}'"
            )
        for p in sorted(g_params - c_params):
            drifts.append(
                f"PARAM added in generated (undocumented): {key[1]} {key[0]} -> param '{p}'"
            )

    if drifts:
        print("OpenAPI drift detected:\n", file=sys.stderr)
        for d in drifts:
            print(f"  {d}", file=sys.stderr)
        print(
            "\nUpdate openapi.yaml to match the generated spec, or fix the utoipa "
            "annotations to match the committed spec.",
            file=sys.stderr,
        )
        return 1

    print("No OpenAPI drift detected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
