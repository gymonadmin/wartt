# wartt

Standalone WhatsApp reply-time telemetry utility.

## Install layout
- `/opt/wartt/bin/wartt`
- `/opt/wartt/libexec/*`
- `/opt/wartt/config/wartt.env`
- `/opt/wartt/schema/*`
- runtime output in `/var/log/wa-latency`

## Commands
- `wartt summary --last 5m|30m|60m`
- `wartt summary 5m --json`
- `wartt report --since 1h --group-by model`
- `wartt ingest-openclaw --once`
- `wartt ingest-openclaw --follow`
- `wartt tail --since 30m --message-type voice --follow`
- `wartt tail --since 60m --human`
- `wartt slowest --since 60m --limit 20 --include-stage-breakdown`
- `wartt export --since 24h --format csv --output /tmp/wartt-24h.csv`
- `wartt export --since 24h --format human-csv --output /tmp/wartt-24h-human.csv`
- `wartt rebuild --since 24h`

## Event input
`wartt` reads NDJSON events from:
- `/var/log/wa-latency/wa_latency_events.ndjson`

Fallback source bridge:
- `wartt ingest-openclaw --once` parses OpenClaw gateway logs from `/tmp/openclaw/openclaw-YYYY-MM-DD.log` and appends derived events.
- Inbound `mediaType` values like `audio/*` or `ptt` are mapped to `message_type=voice`.

## Bash completion
- Completion script: `/opt/wartt/completions/wartt.bash`
- Installed globally at: `/etc/bash_completion.d/wartt`
- New shell session recommended after install.

Expected fields:
- `trace_id`, `channel`, `message_type`, `stage`, `ts_unix_ms`
- optional: `status`, `error_code`, `meta.llm_provider`, `meta.llm_model`,
  `meta.payload_in_bytes`, `meta.payload_out_bytes`

## Generated outputs
- `latency_summary.csv` (one row per trace)
- `latency_human.csv` (quick-scan per trace with `latency_class`, `is_delayed`, `bottleneck_stage`)
- `latency_rollup_1m.csv`
- `latency_rollup_5m.csv`

Quick scan examples:
- `column -s, -t < /var/log/wa-latency/latency_human.csv | less -S`
- `awk -F, 'NR==1 || $7=="yes"' /var/log/wa-latency/latency_human.csv`

Stage boundary fields in `latency_human.csv` are human-readable EET timestamps:
- `t1_inbound_gateway_eet`, `t2_stt_start_eet`, `t3_stt_end_eet`, `t4_llm_start_eet`, `t5_llm_end_eet`, `t6_outbound_send_eet`
- plus derived KPIs: `upload_ingest_ms`, `queue_before_stt_ms`, `whisper_total_ms`, `llm_latency_ms`
- `message_preview` stores a sanitized inbound message snippet for quick operator triage.

When OpenClaw does not emit explicit audio stage markers, `download_audio_ms` and `transcribe_ms` are inferred from pre-LLM voice gap and flagged via:
- `download_audio_source` / `transcribe_source` = `inferred|explicit|none`

## Notes
- Designed for WhatsApp Web mode (no Meta webhook callback required).
- Incomplete traces are emitted as `partial` or `timeout` based on TTL.
