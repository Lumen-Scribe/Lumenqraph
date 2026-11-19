#!/usr/bin/env python3
"""
Lint check that stops credentials and raw config objects from being written to
logs by `tracing` macros.

Earlier versions only matched `tracing::<level>!(` followed by one exact
pattern on a single line. The crates import the macros and call them bare
(`info!(...)`), and calls span several lines, so that check passed vacuously.
This version:

* matches bare and `tracing::`-qualified `trace!`/`debug!`/`info!`/`warn!`/
  `error!` invocations,
* extracts each complete invocation by balancing parentheses (so multi-line
  calls are checked as one unit, and string literals are skipped when
  balancing),
* flags sensitive identifiers (database/RPC URLs, keys, tokens, passwords,
  secrets) used as a macro argument unless that argument is passed through a
  redaction helper, and
* flags a whole `config` object formatted with `?`/`%` (its `Debug` output
  must never be the thing that reaches the log).

Run the fixture tests with `python3 scripts/test_check_sensitive_logging.py`.
"""

import re
import sys
from pathlib import Path

LOG_MACRO_RE = re.compile(r"\b(?:tracing::)?(?:trace|debug|info|warn|error)!\s*\(")

# Identifiers whose value is a secret or a URL that can embed credentials.
# Matched as substrings of a word so `rpc_url`, `config.rpc_url`, `api_key_hash`
# and `webhook_encryption_key` are all caught.
SENSITIVE_RE = re.compile(
    r"\w*(?:database_url|rpc_url|rpc_urls|encryption_key|private_key|secret|"
    r"password|passwd|token|api_key|credential)\w*",
    re.IGNORECASE,
)

# A whole config value formatted directly: `?config`, `%config`, `config = ?config`.
RAW_CONFIG_RE = re.compile(r"(?<![\w.])[?%]\s*config\b(?!\s*\.)")

REDACTION_RE = re.compile(r"redact", re.IGNORECASE)

# Reading only the size of a sensitive collection (`rpc_urls.len()`) logs no secret.
SIZE_ONLY_RE = re.compile(r"[\w.]+\.(?:len|is_empty|count)\(\)")

# A reviewed exemption: a call carrying this marker in its arguments is skipped.
# Use it only where the logged value is already redacted by its own `Debug`
# impl (for example the startup "effective configuration" line), and say why.
EXEMPT_MARKER = "sensitive-log-ok"


def _skip_string(text: str, i: int) -> int:
    """Return the index just past the string literal that starts at `text[i]`."""
    j = i + 1
    while j < len(text):
        if text[j] == "\\":
            j += 2
            continue
        if text[j] == '"':
            return j + 1
        j += 1
    return j


def _invocation_args(text: str, open_paren: int) -> tuple[str, int]:
    """Return (argument text, index after the matching `)`) for the `(` at `open_paren`."""
    depth = 0
    i = open_paren
    while i < len(text):
        ch = text[i]
        if ch == '"':
            i = _skip_string(text, i)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[open_paren + 1 : i], i + 1
        i += 1
    return text[open_paren + 1 :], len(text)


def _top_level_args(args: str) -> list[str]:
    """Split an argument list on commas that are not nested in brackets or strings."""
    parts, depth, start, i = [], 0, 0, 0
    while i < len(args):
        ch = args[i]
        if ch == '"':
            i = _skip_string(args, i)
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(args[start:i])
            start = i + 1
        i += 1
    parts.append(args[start:])
    return parts


def _code_only(arg: str) -> str:
    """Drop string-literal contents so words inside messages are not matched.

    Inline format captures such as `"{rpc_url}"` are kept, since they do format
    the variable.
    """
    out, i = [], 0
    while i < len(arg):
        if arg[i] == '"':
            end = _skip_string(arg, i)
            literal = arg[i:end]
            out.append(" ".join(re.findall(r"\{(\w+)[^}]*\}", literal)))
            i = end
            continue
        out.append(arg[i])
        i += 1
    return "".join(out)


def find_violations(text: str) -> list[tuple[int, str, str]]:
    """Return (line number, message, offending snippet) for each violation in `text`."""
    violations = []
    for match in LOG_MACRO_RE.finditer(text):
        open_paren = match.end() - 1
        args, _ = _invocation_args(text, open_paren)
        if EXEMPT_MARKER in args:
            continue
        line = text.count("\n", 0, match.start()) + 1
        snippet = " ".join(text[match.start() : match.start() + 160].split())

        if RAW_CONFIG_RE.search(_code_only(args)):
            violations.append((line, "Do not log the whole `config` object (use its redacted Debug only via named, non-sensitive fields)", snippet))

        for arg in _top_level_args(args):
            code = SIZE_ONLY_RE.sub(" ", _code_only(arg))
            if REDACTION_RE.search(code):
                continue
            hit = SENSITIVE_RE.search(code)
            if hit:
                violations.append((line, f"Do not log sensitive value `{hit.group(0)}` (wrap it in a redaction helper)", snippet))
                break
    return violations


def check_file(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    errors = 0
    for line, msg, snippet in find_violations(text):
        print(f"ERROR: {path}:{line}: {msg}: {snippet}", file=sys.stderr)
        errors += 1
    return errors


def main() -> int:
    root = Path(__file__).resolve().parent.parent / "crates"
    total_errors = 0
    for rs_file in sorted(root.glob("**/*.rs")):
        total_errors += check_file(rs_file)

    if total_errors > 0:
        print(f"\nFound {total_errors} sensitive logging violation(s).", file=sys.stderr)
        return 1

    print("No sensitive logging violations found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
