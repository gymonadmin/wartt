package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"wartt/internal/model"
)

var (
	overlayBorder = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("62")).
			Padding(0, 1)
	labelCol = lipgloss.NewStyle().Width(22).Foreground(lipgloss.Color("240"))
	valueCol = lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
	bnMark   = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true).Render(" ◀ bottleneck")
)

func renderDetail(t *model.Trace, width int) string {
	innerWidth := width - 6
	if innerWidth < 40 {
		innerWidth = 40
	}

	title := fmt.Sprintf("Trace: %s", t.TraceID)

	row := func(label, value string) string {
		return lipgloss.JoinHorizontal(lipgloss.Top,
			labelCol.Render(label),
			valueCol.Render(value),
		)
	}

	preview := t.MessagePreview
	if len(preview) > innerWidth-24 {
		preview = preview[:innerWidth-27] + "..."
	}
	if preview == "" {
		preview = "-"
	}

	lines := []string{
		row("Time:", t.T1InboundGatewayEET),
		row("Type:", t.MessageType+"   Status: "+t.Status),
		row("Channel:", valueIfElse(t.Channel, "-")),
		row("Message:", `"`+preview+`"`),
		row("Model:", valueIfElse(t.LLMModel, "-")),
		strings.Repeat("─", innerWidth),
	}

	// Stage breakdown
	type stageEntry struct {
		name string
		ms   int64
	}
	stages := []stageEntry{
		{"queue_wait", t.QueueWaitMs},
		{"download_audio", t.DownloadAudioMs},
		{"transcribe", t.TranscribeMs},
		{"tool_calls", t.ToolCallsMs},
		{"llm_total", t.LLMTotalMs},
		{"overhead", t.OverheadMs},
	}
	stageLabel := lipgloss.NewStyle().Width(22)
	for _, s := range stages {
		if s.ms <= 0 {
			continue
		}
		suffix := ""
		if s.name == t.BottleneckStage {
			suffix = bnMark
		}
		lines = append(lines,
			lipgloss.JoinHorizontal(lipgloss.Top,
				stageLabel.Render(s.name),
				valueCol.Render(fmtMs(s.ms)),
				suffix,
			),
		)
	}

	lines = append(lines, strings.Repeat("─", innerWidth))
	lines = append(lines, row("total", fmtMs(t.TotalMs)))
	lines = append(lines, "")
	lines = append(lines, lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("Esc to close"))

	content := lipgloss.JoinVertical(lipgloss.Left, lines...)

	return overlayBorder.Width(innerWidth).Render(
		lipgloss.JoinVertical(lipgloss.Left,
			lipgloss.NewStyle().Bold(true).Render(title),
			strings.Repeat("─", innerWidth),
			content,
		),
	)
}

func valueIfElse(v, def string) string {
	if v != "" {
		return v
	}
	return def
}
