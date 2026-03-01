#!/usr/bin/env bash
set -euo pipefail

WARTT_ROOT="${WARTT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# shellcheck disable=SC1090
source "$WARTT_ROOT/libexec/common.sh"

SINCE="60m"
FORMAT="csv"
OUTPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --since)
      [[ $# -ge 2 ]] || die "--since requires a value"
      SINCE="$2"
      shift 2
      ;;
    --format)
      [[ $# -ge 2 ]] || die "--format requires a value"
      FORMAT="$2"
      shift 2
      ;;
    --output)
      [[ $# -ge 2 ]] || die "--output requires a value"
      OUTPUT="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: export.sh [--since <duration>] [--format <csv|human-csv|ndjson>] --output <path>
EOF
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -n "$OUTPUT" ]] || die "--output is required"
case "$FORMAT" in
  csv|human-csv|ndjson) ;;
  *) die "--format must be csv, human-csv, or ndjson" ;;
esac

if [[ "$FORMAT" == "csv" ]]; then
  "$WARTT_ROOT/libexec/aggregate.sh" --since "$SINCE"
  cp "$WARTT_SUMMARY_FILE" "$OUTPUT"
  exit 0
fi

if [[ "$FORMAT" == "human-csv" ]]; then
  "$WARTT_ROOT/libexec/aggregate.sh" --since "$SINCE"
  cp "$WARTT_HUMAN_FILE" "$OUTPUT"
  exit 0
fi

require_cmd jq
cutoff_ms=$(( $(now_ms) - $(duration_to_ms "$SINCE") ))

jq -Rr --argjson cutoff "$cutoff_ms" '
  fromjson? |
  select(type == "object") |
  select(((.ts_unix_ms // 0) | tonumber? // 0) >= $cutoff)
' "$WARTT_EVENTS_FILE" > "$OUTPUT"
