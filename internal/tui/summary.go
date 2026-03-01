package tui

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

func renderSummary(rows []summaryRow, width int) string {
	if len(rows) == 0 {
		return lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Render("  No data in the last 24h.")
	}

	headerStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("14"))
	labelStyle := lipgloss.NewStyle().Width(10)
	numStyle := lipgloss.NewStyle().Width(8).Align(lipgloss.Right)

	header := lipgloss.JoinHorizontal(lipgloss.Top,
		headerStyle.Render(labelStyle.Render("TYPE")),
		headerStyle.Render(numStyle.Render("COUNT")),
		headerStyle.Render(numStyle.Render("ERRORS")),
		headerStyle.Render(numStyle.Render("ERR%")),
		headerStyle.Render(numStyle.Render("P50")),
		headerStyle.Render(numStyle.Render("P95")),
		headerStyle.Render(numStyle.Render("P99")),
	)

	sep := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(strings.Repeat("─", width))
	lines := []string{header, sep}

	rowStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
	for _, r := range rows {
		errPct := 0.0
		if r.Count > 0 {
			errPct = float64(r.Errors) / float64(r.Count) * 100
		}
		line := lipgloss.JoinHorizontal(lipgloss.Top,
			rowStyle.Render(labelStyle.Render(r.Label)),
			rowStyle.Render(numStyle.Render(itoa(r.Count))),
			rowStyle.Render(numStyle.Render(itoa(r.Errors))),
			rowStyle.Render(numStyle.Render(fmtPct(errPct))),
			rowStyle.Render(numStyle.Render(fmtMs(r.P50))),
			rowStyle.Render(numStyle.Render(fmtMs(r.P95))),
			rowStyle.Render(numStyle.Render(fmtMs(r.P99))),
		)
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

func fmtPct(f float64) string {
	return fmt.Sprintf("%.1f%%", f)
}

func fmtMs(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	return fmt.Sprintf("%.1fs", float64(ms)/1000.0)
}
