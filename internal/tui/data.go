package tui

import (
	"database/sql"
	"math"
	"sort"

	tea "github.com/charmbracelet/bubbletea"

	"wartt/internal/model"
)

type tracesLoadedMsg struct {
	traces  []model.Trace
	summary []summaryRow
}

func (a *App) loadData() tea.Cmd {
	return func() tea.Msg {
		traces, err := queryTail(a.db, 200)
		if err != nil {
			return errMsg{err}
		}
		summary, err := querySummary(a.db)
		if err != nil {
			return errMsg{err}
		}
		return tracesLoadedMsg{traces: traces, summary: summary}
	}
}

func queryTail(db *sql.DB, limit int) ([]model.Trace, error) {
	rows, err := db.Query(`
		SELECT trace_id, time_eet, ts_unix_ms, message_type, channel, status, message_preview,
		       t1_inbound_gateway_eet, t2_stt_start_eet, t3_stt_end_eet,
		       t4_llm_start_eet, t5_llm_end_eet, t6_outbound_send_eet,
		       total_ms, queue_wait_ms, upload_ingest_ms, queue_before_stt_ms,
		       download_audio_ms, whisper_total_ms, transcribe_ms, tool_calls_ms,
		       llm_total_ms, llm_latency_ms, overhead_ms,
		       llm_model, latency_class, bottleneck_stage, bottleneck_ms
		FROM traces
		ORDER BY ts_unix_ms DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTraces(rows)
}

func querySummary(db *sql.DB) ([]summaryRow, error) {
	rows, err := db.Query(`
		SELECT COALESCE(channel, 'whatsapp') || '/' || message_type AS label, status, total_ms
		FROM traces
		WHERE ts_unix_ms > (strftime('%s','now') - 86400) * 1000
		ORDER BY ts_unix_ms ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type bucket struct {
		totals []int64
		errors int
	}
	buckets := map[string]*bucket{}
	order := []string{}

	for rows.Next() {
		var label, status string
		var totalMs sql.NullInt64
		if err := rows.Scan(&label, &status, &totalMs); err != nil {
			return nil, err
		}
		if _, ok := buckets[label]; !ok {
			buckets[label] = &bucket{}
			order = append(order, label)
		}
		b := buckets[label]
		if totalMs.Valid && totalMs.Int64 > 0 {
			b.totals = append(b.totals, totalMs.Int64)
		}
		if status == "error" || status == "timeout" {
			b.errors++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var result []summaryRow
	for _, k := range order {
		b := buckets[k]
		result = append(result, summaryRow{
			Label:  k,
			Count:  len(b.totals),
			Errors: b.errors,
			P50:    percentile(b.totals, 0.50),
			P95:    percentile(b.totals, 0.95),
			P99:    percentile(b.totals, 0.99),
		})
	}
	return result, nil
}

func percentile(vals []int64, p float64) int64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]int64, len(vals))
	copy(sorted, vals)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func scanTraces(rows *sql.Rows) ([]model.Trace, error) {
	var traces []model.Trace
	for rows.Next() {
		var t model.Trace
		var (
			totalMs, queueWait, uploadIngest, queueBeforeStt sql.NullInt64
			downloadAudio, whisperTotal, transcribe           sql.NullInt64
			toolCalls, llmTotal, llmLatency, overhead        sql.NullInt64
			bottleneckMs                                      sql.NullInt64
			channel                                           sql.NullString
		)
		err := rows.Scan(
			&t.TraceID, &t.TimeEET, &t.TsUnixMs, &t.MessageType, &channel, &t.Status, &t.MessagePreview,
			&t.T1InboundGatewayEET, &t.T2SttStartEET, &t.T3SttEndEET,
			&t.T4LlmStartEET, &t.T5LlmEndEET, &t.T6OutboundSendEET,
			&totalMs, &queueWait, &uploadIngest, &queueBeforeStt,
			&downloadAudio, &whisperTotal, &transcribe, &toolCalls,
			&llmTotal, &llmLatency, &overhead,
			&t.LLMModel, &t.LatencyClass, &t.BottleneckStage, &bottleneckMs,
		)
		if err != nil {
			return nil, err
		}
		if channel.Valid {
			t.Channel = channel.String
		}
		if totalMs.Valid {
			t.TotalMs = totalMs.Int64
		}
		if queueWait.Valid {
			t.QueueWaitMs = queueWait.Int64
		}
		if uploadIngest.Valid {
			t.UploadIngestMs = uploadIngest.Int64
		}
		if queueBeforeStt.Valid {
			t.QueueBeforeSttMs = queueBeforeStt.Int64
		}
		if downloadAudio.Valid {
			t.DownloadAudioMs = downloadAudio.Int64
		}
		if whisperTotal.Valid {
			t.WhisperTotalMs = whisperTotal.Int64
		}
		if transcribe.Valid {
			t.TranscribeMs = transcribe.Int64
		}
		if toolCalls.Valid {
			t.ToolCallsMs = toolCalls.Int64
		}
		if llmTotal.Valid {
			t.LLMTotalMs = llmTotal.Int64
		}
		if llmLatency.Valid {
			t.LLMLatencyMs = llmLatency.Int64
		}
		if overhead.Valid {
			t.OverheadMs = overhead.Int64
		}
		if bottleneckMs.Valid {
			t.BottleneckMs = bottleneckMs.Int64
		}
		traces = append(traces, t)
	}
	return traces, rows.Err()
}
