# wartt

Standalone WhatsApp reply-time telemetry utility.

## Quick install on a new machine
After cloning the repo:

```bash
cd /opt/wartt
./install.sh
```

What this does:
- installs required dependencies (`make`, `go>=1.24.2`)
- builds `wartt`
- installs app files under `/opt/wartt`
- links `/usr/local/bin/wartt` to `/opt/wartt/bin/wartt`

## Install layout
- `/opt/wartt/bin/wartt`
- `/opt/wartt/config/wartt.env`
- runtime output in `/var/log/wa-latency/wartt.db`
- ingest state in `/var/log/wa-latency/.openclaw_ingest.state`

## Commands
- `wartt` (opens the interactive TUI)
- `wartt ingest-openclaw [--source <logfile>]`
- `wartt seed-ndjson [path]` (default: `/var/log/wa-latency/wa_latency_events.ndjson`)

## OpenClaw ingest source discovery
If `--source` is not provided, `wartt` searches:
- `/tmp/openclaw/openclaw-*.log`
- `/tmp/openclaw-*/openclaw-*.log`

## Notes
- Designed for WhatsApp Web mode (no Meta webhook callback required).
