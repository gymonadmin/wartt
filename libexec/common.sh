#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${WARTT_ROOT:-}" ]]; then
  WARTT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi

WARTT_ENV_FILE="${WARTT_ENV_FILE:-$WARTT_ROOT/config/wartt.env}"
if [[ -f "$WARTT_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$WARTT_ENV_FILE"
fi

: "${WARTT_LOG_DIR:=/var/log/wa-latency}"
: "${WARTT_EVENTS_FILE:=$WARTT_LOG_DIR/wa_latency_events.ndjson}"
: "${WARTT_SUMMARY_FILE:=$WARTT_LOG_DIR/latency_summary.csv}"
: "${WARTT_HUMAN_FILE:=$WARTT_LOG_DIR/latency_human.csv}"
: "${WARTT_ROLLUP_1M_FILE:=$WARTT_LOG_DIR/latency_rollup_1m.csv}"
: "${WARTT_ROLLUP_5M_FILE:=$WARTT_LOG_DIR/latency_rollup_5m.csv}"
: "${WARTT_TRACE_TTL_MS:=60000}"
: "${WARTT_PERIODIC_SINCE:=24h}"
: "${WARTT_FAST_TEXT_MS:=5000}"
: "${WARTT_SLOW_TEXT_MS:=10000}"
: "${WARTT_FAST_VOICE_MS:=12000}"
: "${WARTT_SLOW_VOICE_MS:=25000}"
: "${WARTT_DISPLAY_TZ:=Europe/Bucharest}"
: "${WARTT_INGEST_LOOKBACK_BYTES:=1048576}"

die() {
  echo "wartt: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

now_ms() {
  date +%s%3N
}

duration_to_seconds() {
  local value="$1"
  if [[ "$value" =~ ^([0-9]+)([smhd])$ ]]; then
    local n="${BASH_REMATCH[1]}"
    local u="${BASH_REMATCH[2]}"
    case "$u" in
      s) echo "$n" ;;
      m) echo $((n * 60)) ;;
      h) echo $((n * 3600)) ;;
      d) echo $((n * 86400)) ;;
      *) die "unsupported duration unit: $u" ;;
    esac
    return 0
  fi
  die "invalid duration '$value' (expected e.g. 5m, 30m, 1h)"
}

duration_to_ms() {
  local seconds
  seconds="$(duration_to_seconds "$1")"
  echo $((seconds * 1000))
}

glob_to_regex() {
  local re="$1"
  re="${re//\\/\\\\}"
  re="${re//./\\.}"
  re="${re//\*/.*}"
  re="${re//\?/.}"
  printf '^%s$' "$re"
}

ensure_log_dir() {
  mkdir -p "$WARTT_LOG_DIR"
}
