#!/usr/bin/env bash
set -euo pipefail

WARTT_ROOT="${WARTT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# shellcheck disable=SC1090
source "$WARTT_ROOT/libexec/common.sh"

LAST="30m"
SINCE=""
MESSAGE_TYPE="all"
STATUS_FILTER="all"
MODEL_FILTER=""
JSON_OUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --last)
      [[ $# -ge 2 ]] || die "--last requires a value"
      LAST="$2"
      shift 2
      ;;
    --since)
      [[ $# -ge 2 ]] || die "--since requires a value"
      SINCE="$2"
      shift 2
      ;;
    --message-type)
      [[ $# -ge 2 ]] || die "--message-type requires a value"
      MESSAGE_TYPE="$2"
      shift 2
      ;;
    --status)
      [[ $# -ge 2 ]] || die "--status requires a value"
      STATUS_FILTER="$2"
      shift 2
      ;;
    --model)
      [[ $# -ge 2 ]] || die "--model requires a value"
      MODEL_FILTER="$2"
      shift 2
      ;;
    --json)
      JSON_OUT=1
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: summary.sh [--last 5m|30m|60m] [--message-type <text|voice|all>] [--status <ok|error|timeout|partial|all>] [--model <glob>] [--json]
       summary.sh --since <duration> [...same filters...]
EOF
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

if [[ -n "$SINCE" && -n "$LAST" ]]; then
  if [[ "$LAST" != "30m" ]]; then
    die "use either --last or --since"
  fi
fi

if [[ -z "$SINCE" ]]; then
  case "$LAST" in
    5m|30m|60m) SINCE="$LAST" ;;
    *) die "--last only supports 5m, 30m, or 60m" ;;
  esac
fi

if [[ "$MESSAGE_TYPE" != "all" && "$MESSAGE_TYPE" != "text" && "$MESSAGE_TYPE" != "voice" ]]; then
  die "--message-type must be one of text|voice|all"
fi

regex_model=""
if [[ -n "$MODEL_FILTER" ]]; then
  regex_model="$(glob_to_regex "$MODEL_FILTER")"
fi

"$WARTT_ROOT/libexec/aggregate.sh" --since "$SINCE"

awk \
  -F, \
  -v since="$SINCE" \
  -v msg_filter="$MESSAGE_TYPE" \
  -v status_filter="$STATUS_FILTER" \
  -v model_regex="$regex_model" \
  -v json_out="$JSON_OUT" '
function p(arr, n, q, i, tmp, idx) {
  if (n < 1) return 0
  delete tmp
  for (i = 1; i <= n; i++) tmp[i] = arr[i]
  asort(tmp)
  idx = int((q * n) + 0.999999)
  if (idx < 1) idx = 1
  if (idx > n) idx = n
  return int(tmp[idx] + 0.5)
}
function max_stage(a, b, c, d, e, f, best_name, best_val) {
  best_name = "queue_wait_ms"
  best_val = a
  if (b > best_val) { best_name = "download_audio_ms"; best_val = b }
  if (c > best_val) { best_name = "transcribe_ms"; best_val = c }
  if (d > best_val) { best_name = "llm_first_token_ms"; best_val = d }
  if (e > best_val) { best_name = "llm_total_ms"; best_val = e }
  if (f > best_val) { best_name = "overhead_ms"; best_val = f }
  return best_name
}
NR == 1 { next }
{
  msg = $4
  model = $18
  status = $19

  if (msg_filter != "all" && msg != msg_filter) next
  if (status_filter != "all" && status != status_filter) next
  if (model_regex != "" && model !~ model_regex) next

  total = $5 + 0
  queue = $6 + 0
  download = $7 + 0
  transcribe = $8 + 0
  llm_first = $12 + 0
  llm_total = $13 + 0
  overhead = $14 + 0

  n_total++
  total_vals[n_total] = total
  queue_vals[n_total] = queue
  download_vals[n_total] = download
  transcribe_vals[n_total] = transcribe
  llm_first_vals[n_total] = llm_first
  llm_total_vals[n_total] = llm_total
  overhead_vals[n_total] = overhead

  if (msg == "text") text_count++
  if (msg == "voice") voice_count++
  if (status != "ok") error_count++
}
END {
  if (n_total < 1) {
    if (json_out == 1) {
      printf "{"
      printf "\"window\":\"last %s\",", since
      printf "\"count\":0,"
      printf "\"text_count\":0,"
      printf "\"voice_count\":0,"
      printf "\"error_count\":0,"
      printf "\"error_rate_pct\":0,"
      printf "\"p50_total_ms\":0,"
      printf "\"p95_total_ms\":0,"
      printf "\"p99_total_ms\":0"
      printf "}\n"
    } else {
      printf "Window: last %s\n", since
      print "Messages: 0"
      print "No traces matched current filters."
    }
    exit 0
  }

  p50_total = p(total_vals, n_total, 0.50)
  p95_total = p(total_vals, n_total, 0.95)
  p99_total = p(total_vals, n_total, 0.99)
  p95_queue = p(queue_vals, n_total, 0.95)
  p95_download = p(download_vals, n_total, 0.95)
  p95_transcribe = p(transcribe_vals, n_total, 0.95)
  p95_llm_first = p(llm_first_vals, n_total, 0.95)
  p95_llm_total = p(llm_total_vals, n_total, 0.95)
  p95_overhead = p(overhead_vals, n_total, 0.95)
  error_rate = (100.0 * error_count) / n_total
  top_stage = max_stage(p95_queue, p95_download, p95_transcribe, p95_llm_first, p95_llm_total, p95_overhead)

  if (json_out == 1) {
    printf "{"
    printf "\"window\":\"last %s\",", since
    printf "\"count\":%d,", n_total
    printf "\"text_count\":%d,", text_count + 0
    printf "\"voice_count\":%d,", voice_count + 0
    printf "\"error_count\":%d,", error_count + 0
    printf "\"error_rate_pct\":%.2f,", error_rate
    printf "\"p50_total_ms\":%d,", p50_total
    printf "\"p95_total_ms\":%d,", p95_total
    printf "\"p99_total_ms\":%d,", p99_total
    printf "\"p95_queue_wait_ms\":%d,", p95_queue
    printf "\"p95_download_audio_ms\":%d,", p95_download
    printf "\"p95_transcribe_ms\":%d,", p95_transcribe
    printf "\"p95_llm_first_token_ms\":%d,", p95_llm_first
    printf "\"p95_llm_total_ms\":%d,", p95_llm_total
    printf "\"p95_overhead_ms\":%d,", p95_overhead
    printf "\"top_bottleneck_stage\":\"%s\"", top_stage
    printf "}\n"
  } else {
    printf "Window: last %s\n", since
    printf "Messages: %d (text %d, voice %d)\n", n_total, text_count + 0, voice_count + 0
    printf "Total latency: p50 %dms | p95 %dms | p99 %dms\n", p50_total, p95_total, p99_total
    printf "Stage p95: queue %dms | download %dms | transcribe %dms | llm_first %dms | llm_total %dms | overhead %dms\n",
      p95_queue, p95_download, p95_transcribe, p95_llm_first, p95_llm_total, p95_overhead
    printf "Errors: %d/%d (%.2f%%)\n", error_count + 0, n_total, error_rate
    printf "Top bottleneck: %s\n", top_stage
  }
}
' "$WARTT_SUMMARY_FILE"

