// Package aggregate stitches raw events into Trace records.
package aggregate

import (
	"fmt"
	"time"

	"wartt/internal/config"
	"wartt/internal/model"
)

// Aggregator computes trace metrics from a slice of events.
type Aggregator struct {
	cfg *config.Config
	loc *time.Location
}

// New creates an Aggregator. It loads the display timezone from cfg.
func New(cfg *config.Config) *Aggregator {
	loc, err := time.LoadLocation(cfg.DisplayTZ)
	if err != nil {
		loc = time.UTC
	}
	return &Aggregator{cfg: cfg, loc: loc}
}

// Aggregate builds a Trace from all events for a single trace_id.
// Returns nil if the trace lacks a valid inbound timestamp.
func (a *Aggregator) Aggregate(traceID string, events []model.Event) *model.Trace {
	// ── Collect per-stage timestamps ─────────────────────────────────────
	var (
		inboundMs    int64
		procStartMs  int64
		llmStartMs   int64
		llmEndMs     int64
		responseMs   int64
		toolSumMs    int64
		toolCurStart int64
		msgType      = "text"
		channel      string
		llmModel     string
		status       string
		clientSendMs int64
		preview      string
	)

	for _, e := range events {
		if e.MessageType != "" {
			msgType = e.MessageType
		}
		if e.Channel != "" {
			channel = e.Channel
		}
		if e.Meta.LLMModel != "" {
			llmModel = e.Meta.LLMModel
		}
		if e.Status != "" {
			status = e.Status
		}
		if e.Meta.ClientSendMs > 0 {
			clientSendMs = e.Meta.ClientSendMs
		}
		if e.Meta.MessagePreview != "" {
			preview = e.Meta.MessagePreview
		}

		switch e.Stage {
		case "inbound_event_received":
			if inboundMs == 0 || e.TsUnixMs < inboundMs {
				inboundMs = e.TsUnixMs
			}
		case "processing_start":
			if procStartMs == 0 {
				procStartMs = e.TsUnixMs
			}
		case "llm_start":
			if llmStartMs == 0 {
				llmStartMs = e.TsUnixMs
			}
		case "llm_end":
			llmEndMs = e.TsUnixMs
		case "tool_call_start":
			toolCurStart = e.TsUnixMs
		case "tool_call_end":
			if toolCurStart > 0 && e.TsUnixMs >= toolCurStart {
				toolSumMs += e.TsUnixMs - toolCurStart
				toolCurStart = 0
			}
		case "response_sent":
			responseMs = e.TsUnixMs
		}
	}

	if inboundMs == 0 {
		return nil
	}

	// ── Compute durations ─────────────────────────────────────────────────
	nowMs := time.Now().UnixMilli()

	totalMs := int64(0)
	if responseMs > 0 {
		totalMs = max0(responseMs - inboundMs)
	} else {
		totalMs = max0(nowMs - inboundMs)
	}

	rawQueue := int64(0)
	if procStartMs > 0 {
		rawQueue = max0(procStartMs - inboundMs)
	}

	llmTotal := span(llmStartMs, llmEndMs)
	toolCalls := max0(toolSumMs)

	// For voice without explicit download/transcribe, infer from queue gap
	downloadAudio := int64(0)
	transcribe := int64(0)
	queueWait := rawQueue

	if msgType == "voice" && downloadAudio == 0 && transcribe == 0 && rawQueue > 0 {
		inferred := rawQueue
		if inferred > 600 {
			inferred = 600
		}
		downloadAudio = inferred
		transcribe = rawQueue - inferred
		queueWait = 0
	}

	queueBeforeStt := queueWait
	whisperTotal := transcribe

	uploadIngest := int64(0)
	if clientSendMs > 0 && inboundMs >= clientSendMs {
		uploadIngest = inboundMs - clientSendMs
	}

	measured := queueWait + downloadAudio + transcribe + toolCalls + llmTotal
	overhead := max0(totalMs - measured)

	// ── Status ────────────────────────────────────────────────────────────
	if status == "" {
		if responseMs > 0 {
			status = "ok"
		} else if (nowMs-inboundMs) >= a.cfg.TraceWindowMs {
			status = "timeout"
		} else {
			status = "partial"
		}
	}

	// ── SLA classification ────────────────────────────────────────────────
	latencyClass := classifyLatency(msgType, status, totalMs, a.cfg)

	// ── Bottleneck ────────────────────────────────────────────────────────
	bottleneckStage, bottleneckMs := bottleneck(map[string]int64{
		"queue_wait":    queueWait,
		"download_audio": downloadAudio,
		"transcribe":    transcribe,
		"tool_calls":    toolCalls,
		"llm_total":     llmTotal,
		"overhead":      overhead,
	})

	// ── Timestamps in display TZ ──────────────────────────────────────────
	fmtTs := func(ms int64) string {
		if ms <= 0 {
			return ""
		}
		return time.UnixMilli(ms).In(a.loc).Format("15:04:05.000")
	}

	t1 := inboundMs
	t2 := int64(0)
	t3 := int64(0)
	if msgType == "voice" && downloadAudio > 0 {
		t2 = t1 + downloadAudio
		t3 = t2 + transcribe
	}
	t4 := llmStartMs
	t5 := llmEndMs
	t6 := responseMs

	return &model.Trace{
		TraceID:        traceID,
		TimeEET:        fmtTs(inboundMs),
		TsUnixMs:       inboundMs,
		MessageType:    msgType,
		Channel:        channel,
		Status:         status,
		MessagePreview: preview,

		T1InboundGatewayEET: fmtTs(t1),
		T2SttStartEET:       fmtTs(t2),
		T3SttEndEET:         fmtTs(t3),
		T4LlmStartEET:       fmtTs(t4),
		T5LlmEndEET:         fmtTs(t5),
		T6OutboundSendEET:   fmtTs(t6),

		TotalMs:          totalMs,
		QueueWaitMs:      queueWait,
		UploadIngestMs:   uploadIngest,
		QueueBeforeSttMs: queueBeforeStt,
		DownloadAudioMs:  downloadAudio,
		WhisperTotalMs:   whisperTotal,
		TranscribeMs:     transcribe,
		ToolCallsMs:      toolCalls,
		LLMTotalMs:       llmTotal,
		LLMLatencyMs:     llmTotal, // same as llm_total in current OpenClaw (no first-token events)
		OverheadMs:       overhead,

		LLMModel:        llmModel,
		LatencyClass:    latencyClass,
		BottleneckStage: bottleneckStage,
		BottleneckMs:    bottleneckMs,
	}
}

func classifyLatency(msgType, status string, totalMs int64, cfg *config.Config) string {
	if status == "error" || status == "timeout" {
		return status
	}
	var fast, slow int64
	if msgType == "voice" {
		fast = cfg.FastVoiceMs
		slow = cfg.SlowVoiceMs
	} else {
		fast = cfg.FastTextMs
		slow = cfg.SlowTextMs
	}
	if totalMs <= fast {
		return "quick"
	}
	if totalMs > slow {
		return "delayed"
	}
	return "moderate"
}

func bottleneck(stages map[string]int64) (string, int64) {
	var bestStage string
	var bestMs int64
	order := []string{"queue_wait", "download_audio", "transcribe", "tool_calls", "llm_total", "overhead"}
	for _, s := range order {
		v := stages[s]
		if v > bestMs {
			bestMs = v
			bestStage = s
		}
	}
	return bestStage, bestMs
}

func max0(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func span(start, end int64) int64 {
	if start > 0 && end >= start {
		return end - start
	}
	return 0
}

// FmtDuration formats milliseconds as a human-readable string.
func FmtDuration(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	return fmt.Sprintf("%.1fs", float64(ms)/1000.0)
}
