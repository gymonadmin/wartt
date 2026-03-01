#!/usr/bin/env bash
set -euo pipefail

WARTT_ROOT="${WARTT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# shellcheck disable=SC1090
source "$WARTT_ROOT/libexec/common.sh"

SINCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --since)
      [[ $# -ge 2 ]] || die "--since requires a value"
      SINCE="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: aggregate.sh [--since <duration>]

Builds:
  - latency_summary.csv
  - latency_human.csv
  - latency_rollup_1m.csv
  - latency_rollup_5m.csv
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
export TZ="${WARTT_DISPLAY_TZ:-Europe/Bucharest}"

cutoff_ms=0
if [[ -n "$SINCE" ]]; then
  cutoff_ms=$(( $(now_ms) - $(duration_to_ms "$SINCE") ))
fi
current_ms="$(now_ms)"

tmp_summary="$(mktemp)"
tmp_human="$(mktemp)"
tmp_roll1="$(mktemp)"
tmp_roll5="$(mktemp)"

jq -Rr '
  fromjson? |
  select(type == "object") |
  [
    (.trace_id // ""),
    (.channel // ""),
    (.message_type // ""),
    (.stage // ""),
    ((.ts_unix_ms // 0) | tonumber? // 0),
    (.status // ""),
    (.error_code // ""),
    (.meta.llm_provider // ""),
    (.meta.llm_model // ""),
    ((.meta.payload_in_bytes // "") | tostring),
    ((.meta.payload_out_bytes // "") | tostring),
    ((.meta.client_send_ms // "") | tostring),
    (.meta.message_preview // "")
  ] | @tsv
' "$WARTT_EVENTS_FILE" | awk \
  -v cutoff_ms="$cutoff_ms" \
  -v now_ms="$current_ms" \
  -v ttl_ms="$WARTT_TRACE_TTL_MS" \
  -v fast_text_ms="$WARTT_FAST_TEXT_MS" \
  -v slow_text_ms="$WARTT_SLOW_TEXT_MS" \
  -v fast_voice_ms="$WARTT_FAST_VOICE_MS" \
  -v slow_voice_ms="$WARTT_SLOW_VOICE_MS" \
  -v summary_out="$tmp_summary" \
  -v human_out="$tmp_human" \
  -v roll1_out="$tmp_roll1" \
  -v roll5_out="$tmp_roll5" '
BEGIN {
  FS=OFS="\t"
  print "time_eet,trace_id,channel,message_type,total_ms,queue_wait_ms,download_audio_ms,transcribe_ms,json_load_ms,search_ms,tool_calls_ms,llm_first_token_ms,llm_total_ms,overhead_ms,payload_in_bytes,payload_out_bytes,llm_provider,llm_model,status,error_code" > summary_out
  print "time_eet,trace_id,message_type,status,total_ms,latency_class,is_delayed,sla_target_ms,bottleneck_stage,bottleneck_ms,queue_wait_ms,download_audio_ms,transcribe_ms,llm_total_ms,overhead_ms,llm_model,message_preview,t1_inbound_gateway_eet,t2_stt_start_eet,t3_stt_end_eet,t4_llm_start_eet,t5_llm_end_eet,t6_outbound_send_eet,upload_ingest_ms,queue_before_stt_ms,whisper_total_ms,llm_latency_ms,download_audio_source,transcribe_source" > human_out
  print "window_start_utc,window_size,count,error_count,error_rate_pct,p50_total_ms,p95_total_ms,p99_total_ms,p95_queue_wait_ms,p95_transcribe_ms,p95_llm_first_token_ms,p95_llm_total_ms" > roll1_out
  print "window_start_utc,window_size,count,error_count,error_rate_pct,p50_total_ms,p95_total_ms,p99_total_ms,p95_queue_wait_ms,p95_transcribe_ms,p95_llm_first_token_ms,p95_llm_total_ms" > roll5_out
}

function max0(v) { return (v < 0 ? 0 : v) }
function span(start, stop) {
  if (start > 0 && stop >= start) return stop - start
  return 0
}
function bottleneck_stage(queue, download, transcribe, jsonload, search, tools, llm_first, llm_total, overhead, best_name, best_val) {
  best_name = "queue_wait_ms"
  best_val = queue
  if (download > best_val) { best_name = "download_audio_ms"; best_val = download }
  if (transcribe > best_val) { best_name = "transcribe_ms"; best_val = transcribe }
  if (jsonload > best_val) { best_name = "json_load_ms"; best_val = jsonload }
  if (search > best_val) { best_name = "search_ms"; best_val = search }
  if (tools > best_val) { best_name = "tool_calls_ms"; best_val = tools }
  if (llm_first > best_val) { best_name = "llm_first_token_ms"; best_val = llm_first }
  if (llm_total > best_val) { best_name = "llm_total_ms"; best_val = llm_total }
  if (overhead > best_val) { best_name = "overhead_ms"; best_val = overhead }
  return best_name
}
function bottleneck_value(queue, download, transcribe, jsonload, search, tools, llm_first, llm_total, overhead, best_val) {
  best_val = queue
  if (download > best_val) best_val = download
  if (transcribe > best_val) best_val = transcribe
  if (jsonload > best_val) best_val = jsonload
  if (search > best_val) best_val = search
  if (tools > best_val) best_val = tools
  if (llm_first > best_val) best_val = llm_first
  if (llm_total > best_val) best_val = llm_total
  if (overhead > best_val) best_val = overhead
  return best_val
}
function latency_class(msg, status, total, quick_limit, delayed_limit) {
  if (status == "error" || status == "timeout") return status
  if (msg == "voice") {
    quick_limit = fast_voice_ms + 0
    delayed_limit = slow_voice_ms + 0
  } else {
    quick_limit = fast_text_ms + 0
    delayed_limit = slow_text_ms + 0
  }
  if (total <= quick_limit) return "quick"
  if (total > delayed_limit) return "delayed"
  return "moderate"
}
function as_ms_or_blank(v) {
  if (v > 0) return int(v)
  return ""
}
function clean_text(v) {
  gsub(/\r/, " ", v)
  gsub(/\n/, " ", v)
  gsub(/,/, ";", v)
  gsub(/[[:space:]]+/, " ", v)
  sub(/^ /, "", v)
  sub(/ $/, "", v)
  if (length(v) > 220) v = substr(v, 1, 217) "..."
  return v
}
function fmt_hms_ms(v, sec, ms, base) {
  if (v < 1000000000000) return ""
  sec = int(v / 1000)
  ms = int(v % 1000)
  base = strftime("%H:%M:%S", sec)
  return sprintf("%s.%03d", base, ms)
}
function add_metric(key, metric, value, idx) {
  if (value < 0) value = 0
  idx = ++mcount[key, metric]
  mvals[key, metric, idx] = value
}
function pval(key, metric, p, n, i, arr, idx) {
  n = mcount[key, metric]
  if (n < 1) return 0
  delete arr
  for (i = 1; i <= n; i++) arr[i] = mvals[key, metric, i]
  asort(arr)
  idx = int((p * n) + 0.999999)
  if (idx < 1) idx = 1
  if (idx > n) idx = n
  return int(arr[idx] + 0.5)
}
function add_rollup(width, inbound_ms, status, total, queue, transcribe, llm_first, llm_total, ws, key) {
  ws = int((inbound_ms / 1000) / width) * width
  key = width "|" ws
  if (!(key in win_seen)) {
    win_seen[key] = 1
    win_width[key] = width
    win_start[key] = ws
    if (width == 60) {
      w60_list[++w60_n] = ws
    } else if (width == 300) {
      w300_list[++w300_n] = ws
    }
  }
  win_count[key]++
  if (status != "ok") win_error[key]++
  add_metric(key, "total", total)
  add_metric(key, "queue", queue)
  add_metric(key, "transcribe", transcribe)
  add_metric(key, "llm_first", llm_first)
  add_metric(key, "llm_total", llm_total)
}

{
  tid = $1
  if (tid == "") next
  stage = $4
  ts = $5 + 0
  if (ts <= 0) next

  if (!(tid in seen_trace)) {
    seen_trace[tid] = 1
    trace_order[++trace_n] = tid
  }

  if (!(tid in channel) && $2 != "") channel[tid] = $2
  if (!(tid in msg_type) && $3 != "") msg_type[tid] = $3
  if ($6 != "") last_status[tid] = $6
  if ($7 != "") last_error[tid] = $7
  if ($8 != "") llm_provider[tid] = $8
  if ($9 != "") llm_model[tid] = $9
  if ($10 != "" && ($10 + 0) > payload_in[tid]) payload_in[tid] = ($10 + 0)
  if ($11 != "" && ($11 + 0) > payload_out[tid]) payload_out[tid] = ($11 + 0)
  if ($12 != "" && ($12 + 0) > 0) client_send_ms[tid] = ($12 + 0)
  if ($13 != "" && !(tid in message_preview)) message_preview[tid] = $13

  if (stage == "inbound_event_received") {
    if (!(tid in inbound_ms) || ts < inbound_ms[tid]) inbound_ms[tid] = ts
  } else if (stage == "processing_start") {
    if (!(tid in proc_start_ms)) proc_start_ms[tid] = ts
  } else if (stage == "audio_download_start") {
    if (!(tid in dl_start_ms)) dl_start_ms[tid] = ts
  } else if (stage == "audio_download_end") {
    dl_end_ms[tid] = ts
  } else if (stage == "transcribe_start") {
    if (!(tid in tr_start_ms)) tr_start_ms[tid] = ts
  } else if (stage == "transcribe_end") {
    tr_end_ms[tid] = ts
  } else if (stage == "json_load_start") {
    if (!(tid in jl_start_ms)) jl_start_ms[tid] = ts
  } else if (stage == "json_load_end") {
    jl_end_ms[tid] = ts
  } else if (stage == "search_start") {
    if (!(tid in sr_start_ms)) sr_start_ms[tid] = ts
  } else if (stage == "search_end") {
    sr_end_ms[tid] = ts
  } else if (stage == "tool_call_start") {
    tool_cur_start_ms[tid] = ts
  } else if (stage == "tool_call_end") {
    if ((tid in tool_cur_start_ms) && ts >= tool_cur_start_ms[tid]) {
      tool_sum_ms[tid] += (ts - tool_cur_start_ms[tid])
    }
    delete tool_cur_start_ms[tid]
  } else if (stage == "llm_start") {
    if (!(tid in llm_start_ms)) llm_start_ms[tid] = ts
  } else if (stage == "llm_first_token") {
    if (!(tid in llm_first_ms)) llm_first_ms[tid] = ts
  } else if (stage == "llm_end") {
    llm_end_ms[tid] = ts
  } else if (stage == "response_sent") {
    response_ms[tid] = ts
  }
}

END {
  for (i = 1; i <= trace_n; i++) {
    tid = trace_order[i]
    if (!(tid in inbound_ms)) continue
    if (cutoff_ms > 0 && inbound_ms[tid] < cutoff_ms) continue

    total = max0(((tid in response_ms) ? response_ms[tid] : now_ms) - inbound_ms[tid])
    raw_queue = max0(((tid in proc_start_ms) ? proc_start_ms[tid] : inbound_ms[tid]) - inbound_ms[tid])
    queue = raw_queue
    download = span(dl_start_ms[tid], dl_end_ms[tid])
    transcribe = span(tr_start_ms[tid], tr_end_ms[tid])
    jsonload = span(jl_start_ms[tid], jl_end_ms[tid])
    search = span(sr_start_ms[tid], sr_end_ms[tid])
    tools = max0(tool_sum_ms[tid] + 0)
    llm_total = span(llm_start_ms[tid], llm_end_ms[tid])
    llm_first = span(llm_start_ms[tid], llm_first_ms[tid])

    status = last_status[tid]
    if (status == "") status = ((tid in response_ms) ? "ok" : "partial")
    if (!(tid in response_ms) && ((now_ms - inbound_ms[tid]) >= ttl_ms) && status != "error") status = "timeout"
    if (last_error[tid] != "" && status == "ok") status = "error"

    msg = ((tid in msg_type) ? msg_type[tid] : "text")
    t0 = ((tid in client_send_ms) ? client_send_ms[tid] + 0 : 0)
    t1 = inbound_ms[tid] + 0
    t2 = ((tid in tr_start_ms) ? tr_start_ms[tid] + 0 : 0)
    t3 = ((tid in tr_end_ms) ? tr_end_ms[tid] + 0 : 0)
    t4 = ((tid in llm_start_ms) ? llm_start_ms[tid] + 0 : 0)
    t5 = ((tid in llm_end_ms) ? llm_end_ms[tid] + 0 : 0)
    t6 = ((tid in response_ms) ? response_ms[tid] + 0 : 0)
    download_source = (download > 0 ? "explicit" : "none")
    transcribe_source = (transcribe > 0 ? "explicit" : "none")

    # In current OpenClaw logs, voice pre-LLM work is often not split into explicit
    # download/transcribe stages. Infer split from pre-run gap so those columns are useful.
    if (msg == "voice" && download == 0 && transcribe == 0 && raw_queue > 0) {
      inferred_download = raw_queue
      if (inferred_download > 600) inferred_download = 600
      download = inferred_download
      transcribe = raw_queue - inferred_download
      queue = 0
      t2 = t1 + download
      t3 = t2 + transcribe
      download_source = "inferred"
      transcribe_source = "inferred"
    }

    queue_before_stt = queue
    whisper_total = transcribe
    llm_latency = llm_total
    upload_ingest = 0
    if (t0 > 0 && t1 >= t0) upload_ingest = t1 - t0

    measured = queue + download + transcribe + jsonload + search + tools + llm_total
    overhead = total - measured
    if (overhead < 0) overhead = 0

    cls = latency_class(msg, status, total)
    delayed = (cls == "delayed" || cls == "error" || cls == "timeout") ? "yes" : "no"
    if (msg == "voice") {
      sla_target = slow_voice_ms + 0
    } else {
      sla_target = slow_text_ms + 0
    }
    b_stage = bottleneck_stage(queue, download, transcribe, jsonload, search, tools, llm_first, llm_total, overhead)
    b_ms = bottleneck_value(queue, download, transcribe, jsonload, search, tools, llm_first, llm_total, overhead)

    time_eet = fmt_hms_ms(inbound_ms[tid] + 0)
    printf "%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s\n",
      time_eet,
      tid,
      ((tid in channel) ? channel[tid] : "whatsapp"),
      msg,
      total, queue, download, transcribe, jsonload, search, tools, llm_first, llm_total, overhead,
      (payload_in[tid] + 0), (payload_out[tid] + 0),
      ((tid in llm_provider) ? llm_provider[tid] : ""),
      ((tid in llm_model) ? llm_model[tid] : ""),
      status,
      ((tid in last_error) ? last_error[tid] : "") >> summary_out
    printf "%s,%s,%s,%s,%d,%s,%s,%d,%s,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%s,%s,%s\n",
      time_eet,
      tid,
      msg,
      status,
      total,
      cls,
      delayed,
      sla_target,
      b_stage,
      b_ms,
      queue,
      download,
      transcribe,
      llm_total,
      overhead,
      ((tid in llm_model) ? llm_model[tid] : ""),
      clean_text((tid in message_preview) ? message_preview[tid] : ""),
      fmt_hms_ms(t1),
      fmt_hms_ms(t2),
      fmt_hms_ms(t3),
      fmt_hms_ms(t4),
      fmt_hms_ms(t5),
      fmt_hms_ms(t6),
      as_ms_or_blank(upload_ingest),
      queue_before_stt,
      whisper_total,
      llm_latency,
      download_source,
      transcribe_source >> human_out

    add_rollup(60, inbound_ms[tid], status, total, queue, transcribe, llm_first, llm_total)
    add_rollup(300, inbound_ms[tid], status, total, queue, transcribe, llm_first, llm_total)
  }

  if (w60_n > 0) {
    for (i = 1; i <= w60_n; i++) tmp60[i] = w60_list[i]
    asort(tmp60)
    for (i = 1; i <= w60_n; i++) {
      ws = tmp60[i]
      key = "60|" ws
      cnt = win_count[key] + 0
      err = win_error[key] + 0
      rate = (cnt > 0) ? (100.0 * err / cnt) : 0
      printf "%s,1m,%d,%d,%.2f,%d,%d,%d,%d,%d,%d,%d\n",
        strftime("%Y-%m-%dT%H:%M:%SZ", ws, 1),
        cnt, err, rate,
        pval(key, "total", 0.50), pval(key, "total", 0.95), pval(key, "total", 0.99),
        pval(key, "queue", 0.95), pval(key, "transcribe", 0.95),
        pval(key, "llm_first", 0.95), pval(key, "llm_total", 0.95) >> roll1_out
    }
  }

  if (w300_n > 0) {
    for (i = 1; i <= w300_n; i++) tmp300[i] = w300_list[i]
    asort(tmp300)
    for (i = 1; i <= w300_n; i++) {
      ws = tmp300[i]
      key = "300|" ws
      cnt = win_count[key] + 0
      err = win_error[key] + 0
      rate = (cnt > 0) ? (100.0 * err / cnt) : 0
      printf "%s,5m,%d,%d,%.2f,%d,%d,%d,%d,%d,%d,%d\n",
        strftime("%Y-%m-%dT%H:%M:%SZ", ws, 1),
        cnt, err, rate,
        pval(key, "total", 0.50), pval(key, "total", 0.95), pval(key, "total", 0.99),
        pval(key, "queue", 0.95), pval(key, "transcribe", 0.95),
        pval(key, "llm_first", 0.95), pval(key, "llm_total", 0.95) >> roll5_out
    }
  }
}
'

mv "$tmp_summary" "$WARTT_SUMMARY_FILE"
mv "$tmp_human" "$WARTT_HUMAN_FILE"
mv "$tmp_roll1" "$WARTT_ROLLUP_1M_FILE"
mv "$tmp_roll5" "$WARTT_ROLLUP_5M_FILE"
