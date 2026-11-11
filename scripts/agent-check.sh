#!/usr/bin/env bash
# Pre-PR gate for agents and humans: reject unwrap/expect on lines this
# branch adds, then run the default test tier. `--no-test` skips the tests.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

base=$(git merge-base HEAD origin/main 2>/dev/null || git merge-base HEAD main)
hits=$( { git diff "$base" -U0 -- '*.rs' | grep -E '^\+[^+]'
  git ls-files --others --exclude-standard -- '*.rs' | xargs -r grep -Hn . ; } \
  | grep -E '\.unwrap\(\)|\.expect\(' || true)
if [[ -n "$hits" ]]; then
  echo "added unwrap/expect (return Result and use ?):" >&2
  echo "$hits" >&2
  exit 1
fi
echo "no added unwrap/expect"

[[ "${1:-}" == "--no-test" ]] && exit 0
cargo test --workspace --locked
