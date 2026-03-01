#!/usr/bin/env bash
set -euo pipefail

WARTT_ROOT="${WARTT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# shellcheck disable=SC1090
source "$WARTT_ROOT/libexec/common.sh"

SINCE="$WARTT_PERIODIC_SINCE"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --since)
      [[ $# -ge 2 ]] || die "--since requires a value"
      SINCE="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: rebuild.sh [--since <duration>]
EOF
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

"$WARTT_ROOT/libexec/aggregate.sh" --since "$SINCE"
printf "Rebuilt wartt outputs (since=%s)\n" "$SINCE"

