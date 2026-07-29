#!/usr/bin/env bash
# Fails if any third-party GitHub Action is pinned to a mutable ref
# (@master, @main, @latest, or a bare branch/SHA shorter than 40 chars).
# Regression guard for AUDIT ID 5 (gosec was previously pinned to @master).
set -euo pipefail

status=0
while IFS= read -r line; do
  # Match lines like:  uses: owner/repo@ref
  ref="${line#*@}"
  owner_repo="${line#*uses: }"
  owner_repo="${owner_repo%%@*}"
  # Skip first-party local actions (path-based, no @).
  if [[ "$line" != *"@"* ]]; then
    continue
  fi
  if [[ "$ref" == "master" || "$ref" == "main" || "$ref" == "latest" ]]; then
    echo "::error::${owner_repo} is pinned to mutable ref @${ref}; pin to a full 40-char commit SHA" >&2
    status=1
    continue
  fi
  if [[ ${#ref} -lt 40 ]]; then
    echo "::error::${owner_repo} uses short ref @${ref}; pin to a full 40-char commit SHA" >&2
    status=1
  fi
done < <(grep -rnE '^[[:space:]]*-?[[:space:]]*uses:[[:space:]]+[^[:space:]]+@' .github/workflows || true)

exit "$status"
