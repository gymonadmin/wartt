#!/usr/bin/env bash
set -euo pipefail

WARTT_ROOT="${WARTT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# shellcheck disable=SC1090
source "$WARTT_ROOT/libexec/common.sh"

SINCE="60m"
GROUP_BY="none"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --since)
      [[ $# -ge 2 ]] || die "--since requires a value"
      SINCE="$2"
      shift 2
      ;;
    --group-by)
      [[ $# -ge 2 ]] || die "--group-by requires a value"
      GROUP_BY="$2"
      shift 2
      ;;
    --rolling)
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: report.sh [--since <duration>] [--group-by <none|model|message_type>] [--rolling]
EOF
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "$GROUP_BY" in
  none|model|message_type) ;;
  *) die "--group-by must be one of none|model|message_type" ;;
esac

if [[ "$GROUP_BY" == "none" ]]; then
  exec "$WARTT_ROOT/libexec/summary.sh" --since "$SINCE"
fi

"$WARTT_ROOT/libexec/aggregate.sh" --since "$SINCE"

awk \
  -F, \
  -v since="$SINCE" \
  -v group_by="$GROUP_BY" '
function p(group, q, n, i, arr, idx) {
  n = g_count[group]
  if (n < 1) return 0
  delete arr
  for (i = 1; i <= n; i++) arr[i] = g_total[group, i]
  asort(arr)
  idx = int((q * n) + 0.999999)
  if (idx < 1) idx = 1
  if (idx > n) idx = n
  return int(arr[idx] + 0.5)
}
NR == 1 { next }
{
  group = (group_by == "model" ? $18 : $4)
  if (group == "") group = "(unknown)"

  if (!(group in seen_group)) {
    seen_group[group] = 1
    group_list[++group_n] = group
  }

  idx = ++g_count[group]
  g_total[group, idx] = $5 + 0
  if ($19 != "ok") g_error[group]++
}
END {
  printf "Window: last %s\n", since
  if (group_by == "model") {
    print "Grouped by: llm_model"
  } else {
    print "Grouped by: message_type"
  }
  print "group,count,error_count,error_rate_pct,p50_total_ms,p95_total_ms,p99_total_ms"
  for (i = 1; i <= group_n; i++) {
    g = group_list[i]
    c = g_count[g] + 0
    e = g_error[g] + 0
    r = (c > 0) ? (100.0 * e / c) : 0
    printf "%s,%d,%d,%.2f,%d,%d,%d\n", g, c, e, r, p(g, 0.50), p(g, 0.95), p(g, 0.99)
  }
}
' "$WARTT_SUMMARY_FILE"

