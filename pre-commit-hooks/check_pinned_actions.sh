#!/bin/bash

# Only run if .github/ files are staged
staged_github_files=$(git diff --cached --name-only --diff-filter=ACM | grep '^\.github/')
if [ -z "$staged_github_files" ]; then
  exit 0
fi

# Check for unpinned external GitHub Actions (not using commit SHA)
offenders=$(echo "$staged_github_files" | grep -E '\.github/(workflows|actions)/' |
  xargs grep -E "uses:[[:space:]]*[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@" |
  grep -v "\.github/actions" |
  grep -v -E "@[0-9a-f]{40}($|[^0-9a-f])")

if [ -n "$offenders" ]; then
  echo "❌ Error: Detected external GitHub Actions that are not pinned to a commit SHA." >&2
  echo "Please update your workflows accordingly to prevent supply chain attacks!" >&2
  echo "Offending lines:" >&2
  echo "$offenders" >&2
  exit 1
fi