package tui

import (
	"sort"

	"github.com/charmbracelet/bubbles/table"

	"wartt/internal/aggregate"
	"wartt/internal/model"
)

// columnDefs defines the table columns.
var columnDefs = []table.Column{
	{Title: "TIME", Width: 10},
	{Title: "TYPE", Width: 5},
	{Title: "CHAN", Width: 9},
	{Title: "STATUS", Width: 7},
	{Title: "CLASS", Width: 8},
	{Title: "TOTAL", Width: 7},
	{Title: "BN_STAGE", Width: 12},
	{Title: "BN_MS", Width: 7},
	{Title: "Q_WAIT", Width: 7},
	{Title: "DL_AUD", Width: 7},
	{Title: "XSCRIB", Width: 7},
	{Title: "LLM", Width: 7},
	{Title: "OVHD", Width: 7},
	{Title: "UPLOAD", Width: 7},
	{Title: "Q_STT", Width: 7},
	{Title: "WHSPR", Width: 7},
	{Title: "LLM_LAT", Width: 7},
}

func tailColumns(_ int) []table.Column { return columnDefs }

func traceRows(traces []model.Trace) []table.Row {
	rows := make([]table.Row, len(traces))
	for i, t := range traces {
		rows[i] = traceToRow(t)
	}
	return rows
}

func traceToRow(t model.Trace) table.Row {
	totalStr := aggregate.FmtDuration(t.TotalMs)
	if t.Status == "error" || t.Status == "timeout" {
		totalStr = "-"
	}
	bn := t.BottleneckStage
	if bn == "" {
		bn = "-"
	}
	return table.Row{
		t.TimeEET,
		t.MessageType,
		t.Channel,
		t.Status,
		t.LatencyClass,
		totalStr,
		bn,
		fmtMs(t.BottleneckMs),
		fmtMs(t.QueueWaitMs),
		fmtMs(t.DownloadAudioMs),
		fmtMs(t.TranscribeMs),
		fmtMs(t.LLMTotalMs),
		fmtMs(t.OverheadMs),
		fmtMs(t.UploadIngestMs),
		fmtMs(t.QueueBeforeSttMs),
		fmtMs(t.WhisperTotalMs),
		fmtMs(t.LLMLatencyMs),
	}
}

// sortedCopy returns a sorted copy of traces without mutating the original.
func sortedCopy(traces []model.Trace, col int, asc bool) []model.Trace {
	cp := make([]model.Trace, len(traces))
	copy(cp, traces)
	sort.SliceStable(cp, func(i, j int) bool {
		less := compareByCol(cp[i], cp[j], col)
		if asc {
			return less
		}
		return !less
	})
	return cp
}

func compareByCol(a, b model.Trace, col int) bool {
	switch col {
	case 0:
		return a.TsUnixMs < b.TsUnixMs
	case 1:
		return a.MessageType < b.MessageType
	case 2:
		return a.Channel < b.Channel
	case 3:
		return a.Status < b.Status
	case 4:
		return a.LatencyClass < b.LatencyClass
	case 5:
		return a.TotalMs < b.TotalMs
	case 6:
		return a.BottleneckStage < b.BottleneckStage
	case 7:
		return a.BottleneckMs < b.BottleneckMs
	case 8:
		return a.QueueWaitMs < b.QueueWaitMs
	case 9:
		return a.DownloadAudioMs < b.DownloadAudioMs
	case 10:
		return a.TranscribeMs < b.TranscribeMs
	case 11:
		return a.LLMTotalMs < b.LLMTotalMs
	case 12:
		return a.OverheadMs < b.OverheadMs
	case 13:
		return a.UploadIngestMs < b.UploadIngestMs
	case 14:
		return a.QueueBeforeSttMs < b.QueueBeforeSttMs
	case 15:
		return a.WhisperTotalMs < b.WhisperTotalMs
	case 16:
		return a.LLMLatencyMs < b.LLMLatencyMs
	default:
		return a.TsUnixMs < b.TsUnixMs
	}
}
