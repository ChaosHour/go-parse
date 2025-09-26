#!/usr/bin/env zsh
# Scan binlog files in a directory and run go-parse to detect large operations.

set -euo pipefail

BIN_DIR="${1:-tests}"
THRESHOLD="${2:-1000}"
BIN="$(dirname "$0")/../bin/go-parse"
SCHEMA="${3:-schema/sbtest-schema-only.sql}"

if [[ ! -x "$BIN" ]]; then
  echo "Binary $BIN not found or not executable. Run 'make build' first."
  exit 1
fi

echo "Scanning directory: $BIN_DIR for binlog files"
echo "Large-op threshold: $THRESHOLD rows"

shopt -s nullglob
for f in "$BIN_DIR"/*; do
  # Only consider files that look like mysql-bin.* or contain 'mysql-bin'
  if [[ $(basename "$f") == mysql-bin* ]] || [[ "$f" == *mysql-bin* ]]; then
    echo "\n--- Processing $f ---"
    # Run the parser; it will dump any large events to stdout
    # Capture output and also search for user/IP patterns
    out="$($BIN -file "$f" -detectLarge $THRESHOLD -schema "$SCHEMA" 2>&1 || true)"
    echo "$out"

    # Try to extract connection info from output: look for IP addresses and user= or host=
    echo "Potential connection info found:" 
    echo "$out" | egrep -o "([0-9]{1,3}\.){3}[0-9]{1,3}" | sort -u || true
    echo "$out" | egrep -o "user=[^[:space:];,]+|host=[^[:space:];,]+|client=[^[:space:];,]+" | sort -u || true
  fi
done

echo "Done."
