#!/usr/bin/env bash
# Lint SQL migrations for lock-unsafe DDL with squawk (#475).
#
# Usage:
#   scripts/lint_migrations.sh                 # migrations changed vs origin/main
#   scripts/lint_migrations.sh <base-ref>      # migrations changed vs <base-ref>
#   scripts/lint_migrations.sh --all           # every migration (historical ones fail)
#   scripts/lint_migrations.sh -- <file>...    # explicit files
#
# Only new or changed migrations are linted by default: already-released
# migrations can't be edited (sqlx checksums them), so the known-unsafe ones
# (0010, 0016, 0019, 0022) are documented in docs/MIGRATIONS.md instead.
#
# sqlx runs each migration inside a transaction unless the file starts with
# `-- no-transaction`, so files are linted in two groups: transactional files
# with --assume-in-transaction (which also catches a CONCURRENTLY statement
# that would fail inside the transaction), and `-- no-transaction` files
# without it.
#
# Rules and exclusions live in .squawk.toml. To accept a flagged statement on
# purpose, put `-- squawk-ignore <rule>` on the line above it with a comment
# explaining why it is safe.
#
# Set SQUAWK to the squawk binary if it isn't on PATH.

set -euo pipefail

SQUAWK="${SQUAWK:-squawk}"
cd "$(git rev-parse --show-toplevel)"

files=()
case "${1:-}" in
  --all)
    files=(migrations/*.sql)
    ;;
  --)
    shift
    files=("$@")
    ;;
  *)
    base="${1:-origin/main}"
    while IFS= read -r f; do
      [ -n "$f" ] && files+=("$f")
    done < <(git diff --name-only --diff-filter=ACMR "$base"...HEAD -- 'migrations/*.sql')
    ;;
esac

if [ ${#files[@]} -eq 0 ]; then
  echo "No migrations to lint."
  exit 0
fi

tx_files=()
no_tx_files=()
for f in "${files[@]}"; do
  if head -n 1 "$f" | grep -q '^-- no-transaction'; then
    no_tx_files+=("$f")
  else
    tx_files+=("$f")
  fi
done

status=0
if [ ${#tx_files[@]} -gt 0 ]; then
  echo "Linting transactional migrations: ${tx_files[*]}"
  "$SQUAWK" --assume-in-transaction "${tx_files[@]}" || status=1
fi
if [ ${#no_tx_files[@]} -gt 0 ]; then
  echo "Linting -- no-transaction migrations: ${no_tx_files[*]}"
  "$SQUAWK" --no-assume-in-transaction "${no_tx_files[@]}" || status=1
fi

if [ $status -ne 0 ]; then
  echo
  echo "Unsafe migration DDL found. See docs/MIGRATIONS.md#writing-lock-safe-migrations"
  echo "for safe patterns, or add '-- squawk-ignore <rule>' with a justification."
fi
exit $status
