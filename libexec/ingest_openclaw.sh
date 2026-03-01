#!/usr/bin/env bash
set -euo pipefail

WARTT_ROOT="${WARTT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# shellcheck disable=SC1090
source "$WARTT_ROOT/libexec/common.sh"

MODE="once"
SOURCE_FILE=""
STATE_FILE="${WARTT_LOG_DIR}/.openclaw_ingest.state"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --once)
      MODE="once"
      shift
      ;;
    --follow)
      MODE="follow"
      shift
      ;;
    --source)
      [[ $# -ge 2 ]] || die "--source requires a value"
      SOURCE_FILE="$2"
      shift 2
      ;;
    --state-file)
      [[ $# -ge 2 ]] || die "--state-file requires a value"
      STATE_FILE="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ingest_openclaw.sh [--once|--follow] [--source <logfile>] [--state-file <path>]

Parses OpenClaw gateway log lines into wartt NDJSON events.
EOF
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

require_cmd jq
require_cmd awk
ensure_log_dir
touch "$WARTT_EVENTS_FILE"

resolve_default_source() {
  # Prefer the most recently written OpenClaw log, regardless of filename date.
  # This avoids missing traffic when the process keeps appending to yesterday's file.
  ls -1t /tmp/openclaw/openclaw-*.log /tmp/openclaw-*/openclaw-*.log 2>/dev/null | head -n 1 || true
}

last_ingested_ts_ms() {
  [[ -s "$WARTT_EVENTS_FILE" ]] || {
    echo 0
    return 0
  }
  tail -n 500 "$WARTT_EVENTS_FILE" | jq -Rr '
    fromjson? |
    select(type == "object") |
    ((.ts_unix_ms // 0) | tonumber? // 0)
  ' | tail -n 1
}

if [[ -z "$SOURCE_FILE" ]]; then
  SOURCE_FILE="$(resolve_default_source)"
fi
[[ -n "$SOURCE_FILE" ]] || die "no OpenClaw log source found"
[[ -f "$SOURCE_FILE" ]] || die "source log does not exist: $SOURCE_FILE"

parse_stream() {
  jq -Rr '
    fromjson? |
    (.time // "") as $t |
    (
      if ($t | type) == "string" and ($t | length) > 0 then
        (
          (($t | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) * 1000)
          +
          (
            (
              ((try ($t | capture("\\.(?<ms>[0-9]+)Z$").ms) catch "0") + "000")
              | .[0:3]
            ) | tonumber
          )
        )
      else
        0
      end
    ) as $ts |
    [
      ($ts | floor),
      ((.["1"] // "") | tostring),
      ((.["0"] // "") | tostring),
      ((.["2"] // "") | tostring)
    ] | @tsv
  ' | awk -F'\t' '
function esc(s) {
  gsub(/\\/,"\\\\",s)
  gsub(/"/,"\\\"",s)
  gsub(/\r/,"",s)
  gsub(/\n/," ",s)
  return s
}
function detect_message_type(message_json, lower, mt) {
  lower = tolower(message_json)
  if (match(lower, /"mediatype":"([^"]+)"/, m)) {
    mt = m[1]
    if (mt ~ /(audio|voice|ptt|opus|ogg|m4a|mp3|wav)/) return "voice"
    return "text"
  }
  if (match(lower, /"mediakind":"([^"]+)"/, k)) {
    mt = k[1]
    if (mt ~ /(audio|voice|ptt|opus|ogg|m4a|mp3|wav)/) return "voice"
    return "text"
  }
  if (lower ~ /<media:[[:space:]]*audio>/) return "voice"
  if (lower ~ /<media:[[:space:]]*voice>/) return "voice"
  if (match(lower, /"mediapath":"[^"]+\.(ogg|opus|mp3|wav|m4a)"/)) return "voice"
  return "text"
}
function extract_client_send_ms(message_json, lower, raw) {
  lower = tolower(message_json)
  if (match(lower, /"timestamp":[[:space:]]*([0-9]{10,13})/, t)) {
    raw = t[1] + 0
    if (raw > 0 && raw < 1000000000000) raw = raw * 1000
    return raw
  }
  return 0
}
function extract_message_preview(message_json, lower, raw) {
  raw = ""
  if (match(message_json, /"body":"([^"]*)"/, b)) {
    raw = b[1]
  } else if (match(message_json, /"text":"([^"]*)"/, t)) {
    raw = t[1]
  }
  if (raw == "") {
    lower = tolower(message_json)
    if (match(lower, /"mediatype":"([^"]+)"/, m)) {
      raw = "<media:" m[1] ">"
    }
  }
  gsub(/\\n/, " ", raw)
  gsub(/\\r/, " ", raw)
  gsub(/\\"/, "\"", raw)
  gsub(/\\\\/, "\\", raw)
  gsub(/[[:space:]]+/, " ", raw)
  sub(/^ /, "", raw)
  sub(/ $/, "", raw)
  gsub(/,/, ";", raw)
  if (length(raw) > 180) raw = substr(raw, 1, 177) "..."
  return raw
}
function emit(trace, msg_type, stage, ts, status, tool, provider, model, client_send_ms, message_preview, line, sep) {
  if (trace == "" || ts <= 0 || stage == "") return
  if (msg_type == "") msg_type = "text"
  line = "{\"trace_id\":\"" esc(trace) "\",\"channel\":\"whatsapp\",\"message_type\":\"" esc(msg_type) "\",\"stage\":\"" stage "\",\"ts_unix_ms\":" ts
  if (status != "") line = line ",\"status\":\"" esc(status) "\""
  if (tool != "" || provider != "" || model != "" || (client_send_ms + 0) > 0 || message_preview != "") {
    line = line ",\"meta\":{"
    sep = ""
    if (provider != "") { line = line "\"llm_provider\":\"" esc(provider) "\""; sep = "," }
    if (model != "") { line = line sep "\"llm_model\":\"" esc(model) "\""; sep = "," }
    if (tool != "") { line = line sep "\"tool_name\":\"" esc(tool) "\""; sep = "," }
    if ((client_send_ms + 0) > 0) { line = line sep "\"client_send_ms\":" int(client_send_ms + 0) }
    if (message_preview != "") { line = line sep "\"message_preview\":\"" esc(message_preview) "\"" }
    line = line "}"
  }
  line = line "}"
  print line
}
{
  ts = $1 + 0
  msg = $2
  desc = $4
  if (ts <= 0 || msg == "") next

  if (desc == "inbound web message") {
    client_ts = extract_client_send_ms(msg)
    preview = extract_message_preview(msg)
    if (client_ts <= 0 && pending_client_ts > 0 && (ts - pending_client_seen_ts) <= 5000) {
      client_ts = pending_client_ts
    }
    if (preview == "" && pending_preview != "" && (ts - pending_preview_seen_ts) <= 5000) {
      preview = pending_preview
    }
    in_n++
    in_ts[in_n] = ts
    in_type[in_n] = detect_message_type(msg)
    in_client_ts[in_n] = client_ts
    in_preview[in_n] = preview
    in_used[in_n] = 0
    next
  }

  if (desc == "inbound message") {
    pending_client_ts = extract_client_send_ms(msg)
    pending_client_seen_ts = ts
    pending_preview = extract_message_preview(msg)
    pending_preview_seen_ts = ts
    next
  }

  if (match(msg, /lane dequeue: .*waitMs=([0-9]+)/, m)) {
    pending_wait_ms = m[1] + 0
    pending_wait_ts = ts
    next
  }

  if (match(msg, /embedded run start: runId=([0-9a-fA-F-]+).*messageChannel=whatsapp/, m)) {
    run = m[1]
    p = ""
    md = ""
    run_type = "text"
    inbound_ts = 0
    inbound_client_ts = 0
    inbound_preview = ""
    best_idx = 0
    for (j = in_n; j >= 1 && j >= in_n - 120; j--) {
      if (in_used[j] == 1) continue
      delta = ts - in_ts[j]
      if (delta < 0) continue
      if (delta <= 30000) {
        best_idx = j
        break
      }
    }
    if (best_idx > 0) {
      in_used[best_idx] = 1
      inbound_ts = in_ts[best_idx]
      run_type = in_type[best_idx]
      inbound_client_ts = in_client_ts[best_idx] + 0
      inbound_preview = in_preview[best_idx]
    }
    if (match(msg, /provider=([^ ]+)/, pm)) p = pm[1]
    if (match(msg, /model=([^ ]+)/, mm)) md = mm[1]
    provider_by_run[run] = p
    model_by_run[run] = md
    msg_type_by_run[run] = run_type
    preview_by_run[run] = inbound_preview

    wait_ms = 0
    if (pending_wait_ms > 0 && (ts - pending_wait_ts) <= 5000) {
      wait_ms = pending_wait_ms
    }
    pending_wait_ms = 0
    pending_wait_ts = 0

    if (inbound_ts <= 0) inbound_ts = ts - wait_ms
    emit(run, run_type, "inbound_event_received", inbound_ts, "", "", p, md, inbound_client_ts, inbound_preview)
    emit(run, run_type, "processing_start", ts, "", "", p, md, "", "")
    next
  }

  if (match(msg, /embedded run prompt start: runId=([0-9a-fA-F-]+)/, m)) {
    run = m[1]
    emit(run, msg_type_by_run[run], "llm_start", ts, "", "", provider_by_run[run], model_by_run[run], "", "")
    next
  }

  if (match(msg, /embedded run prompt end: runId=([0-9a-fA-F-]+)/, m)) {
    run = m[1]
    emit(run, msg_type_by_run[run], "llm_end", ts, "", "", provider_by_run[run], model_by_run[run], "", "")
    next
  }

  if (match(msg, /embedded run tool start: runId=([0-9a-fA-F-]+).*tool=([^ ]+)/, m)) {
    run = m[1]
    tool = m[2]
    emit(run, msg_type_by_run[run], "tool_call_start", ts, "", tool, provider_by_run[run], model_by_run[run], "", "")
    next
  }

  if (match(msg, /embedded run tool end: runId=([0-9a-fA-F-]+).*tool=([^ ]+)/, m)) {
    run = m[1]
    tool = m[2]
    emit(run, msg_type_by_run[run], "tool_call_end", ts, "", tool, provider_by_run[run], model_by_run[run], "", "")
    next
  }

  if (match(msg, /embedded run agent end: runId=([0-9a-fA-F-]+) isError=(true|false)/, m)) {
    run = m[1]
    is_error_by_run[run] = (m[2] == "true" ? 1 : 0)
    next
  }

  if (match(msg, /embedded run done: runId=([0-9a-fA-F-]+).*aborted=(true|false)/, m)) {
    run = m[1]
    status = "ok"
    if (is_error_by_run[run] == 1) {
      status = "error"
    } else if (m[2] == "true") {
      status = "timeout"
    }
    emit(run, msg_type_by_run[run], "response_sent", ts, status, "", provider_by_run[run], model_by_run[run], "", "")
    next
  }
}
'
}

if [[ "$MODE" == "once" ]]; then
  local_inode="$(stat -c '%i' "$SOURCE_FILE")"
  local_size="$(stat -c '%s' "$SOURCE_FILE")"
  last_path=""
  last_inode=""
  last_offset=0
  if [[ -f "$STATE_FILE" ]]; then
    IFS=$'\t' read -r last_path last_inode last_offset _ < "$STATE_FILE" || true
  fi
  same_source=0
  if [[ "$last_path" == "$SOURCE_FILE" && "$last_inode" == "$local_inode" && "$last_offset" -le "$local_size" ]]; then
    same_source=1
  fi

  filter_new_events() {
    local min_ts="$1"
    awk -v min_ts="$min_ts" '
    {
      if (match($0, /"ts_unix_ms":([0-9]+)/, m)) {
        if ((m[1] + 0) > (min_ts + 0)) print
      }
    }'
  }

  if [[ "$same_source" -eq 1 ]]; then
    if [[ "$local_size" -gt "$last_offset" ]]; then
      min_ts="$(last_ingested_ts_ms)"
      [[ -n "$min_ts" ]] || min_ts=0
      start_byte=1
      if [[ "$last_offset" -gt "$WARTT_INGEST_LOOKBACK_BYTES" ]]; then
        start_byte=$((last_offset - WARTT_INGEST_LOOKBACK_BYTES + 1))
      fi
      tail -c "+$start_byte" "$SOURCE_FILE" | parse_stream | filter_new_events "$min_ts" >> "$WARTT_EVENTS_FILE"
    fi
  else
    # Source switched (or rotated). Backfill only events newer than what we already ingested
    # to avoid duplicating old traces when jumping between log files.
    min_ts="$(last_ingested_ts_ms)"
    [[ -n "$min_ts" ]] || min_ts=0
    if [[ "$local_size" -gt 0 ]]; then
      cat "$SOURCE_FILE" | parse_stream | filter_new_events "$min_ts" >> "$WARTT_EVENTS_FILE"
    fi
  fi

  printf "%s\t%s\t%s\n" "$SOURCE_FILE" "$local_inode" "$local_size" > "$STATE_FILE"
  echo "Ingested OpenClaw logs (once) from: $SOURCE_FILE"
  exit 0
fi

tail -n 0 -F "$SOURCE_FILE" | parse_stream >> "$WARTT_EVENTS_FILE"
