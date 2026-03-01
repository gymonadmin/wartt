#!/usr/bin/env bash
set -euo pipefail

WARTT_ROOT="${WARTT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# shellcheck disable=SC1090
source "$WARTT_ROOT/libexec/common.sh"

SINCE="60m"
LIMIT=20
WITH_STAGE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --since)
      [[ $# -ge 2 ]] || die "--since requires a value"
      SINCE="$2"
      shift 2
      ;;
    --limit)
      [[ $# -ge 2 ]] || die "--limit requires a value"
      LIMIT="$2"
      shift 2
      ;;
    --include-stage-breakdown)
      WITH_STAGE=1
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: slowest.sh [--since <duration>] [--limit <n>] [--include-stage-breakdown]
EOF
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

"$WARTT_ROOT/libexec/aggregate.sh" --since "$SINCE"

tmp_sorted="$(mktemp)"
{
  head -n 1 "$WARTT_SUMMARY_FILE"
  tail -n +2 "$WARTT_SUMMARY_FILE" | sort -t, -k5,5nr | head -n "$LIMIT"
} > "$tmp_sorted"

if [[ "$WITH_STAGE" -eq 1 ]]; then
  awk -F, '
NR==1 {
  print "time_eet,trace_id,message_type,total_ms,queue_wait_ms,download_audio_ms,transcribe_ms,llm_first_token_ms,llm_total_ms,overhead_ms,llm_model,status,error_code"
  next
}
{
  printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", $1,$2,$4,$5,$6,$7,$8,$12,$13,$14,$18,$19,$20
}
' "$tmp_sorted"
else
  awk -F, '
NR==1 {
  print "time_eet,trace_id,message_type,total_ms,llm_model,status,error_code"
  next
}
{
  printf "%s,%s,%s,%s,%s,%s,%s\n", $1,$2,$4,$5,$18,$19,$20
}
' "$tmp_sorted"
fi

rm -f "$tmp_sorted"
