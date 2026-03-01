// Package tui implements the bubbletea TUI for WARTT.
package tui

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"wartt/internal/model"
)

// summaryPanelLines is the fixed line budget reserved at the bottom for the
// inline summary panel (separator + header + separator + up to 5 data rows).
const summaryPanelLines = 8

type tickMsg struct{}

type errMsg struct{ err error }

// App is the root bubbletea model.
type App struct {
	db        *sql.DB
	table     table.Model
	rawTraces []model.Trace // as loaded from DB (time DESC)
	traces    []model.Trace // sorted view used by the table and detail overlay
	summary   []summaryRow
	detail    *model.Trace
	sortCol   int  // index into columnDefs; 0 = TIME
	sortAsc   bool // false = descending (default: newest first)
	width     int
	height    int
	err       error
	ready     bool
}

type summaryRow struct {
	Label  string
	Count  int
	Errors int
	P50    int64
	P95    int64
	P99    int64
}

func NewApp(db *sql.DB) *App {
	return &App{db: db}
}

func (a *App) Init() tea.Cmd {
	return tea.Batch(tick(), tea.EnterAltScreen)
}

func tick() tea.Cmd {
	return tea.Tick(10*time.Second, func(t time.Time) tea.Msg {
		return tickMsg{}
	})
}

func (a *App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		a.width = msg.Width
		a.height = msg.Height
		a.rebuildTable()
		a.ready = true
		return a, a.loadData()

	case tickMsg:
		return a, tea.Batch(tick(), a.loadData())

	case tracesLoadedMsg:
		a.rawTraces = msg.traces
		a.summary = msg.summary
		a.rebuildTable()
		return a, nil

	case errMsg:
		a.err = msg.err
		return a, nil

	case tea.KeyMsg:
		if a.detail != nil {
			if msg.String() == "esc" || msg.String() == "q" {
				a.detail = nil
			}
			return a, nil
		}

		switch msg.String() {
		case "q", "ctrl+c":
			return a, tea.Quit

		case "r":
			return a, a.loadData()

		case "<", ",":
			if a.sortCol > 0 {
				a.sortCol--
			} else {
				a.sortCol = len(columnDefs) - 1
			}
			a.rebuildTable()
			return a, nil

		case ">", ".":
			a.sortCol = (a.sortCol + 1) % len(columnDefs)
			a.rebuildTable()
			return a, nil

		case "s":
			a.sortAsc = !a.sortAsc
			a.rebuildTable()
			return a, nil

		case "enter":
			if i := a.table.Cursor(); i >= 0 && i < len(a.traces) {
				t := a.traces[i]
				a.detail = &t
			}
			return a, nil
		}

		var cmd tea.Cmd
		a.table, cmd = a.table.Update(msg)
		return a, cmd
	}

	var cmd tea.Cmd
	a.table, cmd = a.table.Update(msg)
	return a, cmd
}

func (a *App) View() string {
	if !a.ready {
		return "Loading…"
	}

	header := a.renderHeader()
	var body string
	if a.detail != nil {
		body = renderDetail(a.detail, a.width)
	} else {
		body = a.table.View()
	}
	summaryPanel := a.renderSummaryPanel()

	status := ""
	if a.err != nil {
		status = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render("error: " + a.err.Error())
	}

	return lipgloss.JoinVertical(lipgloss.Left, header, body, summaryPanel, status)
}

func (a *App) renderHeader() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("11"))
	sortStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("14"))
	hintStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))

	dir := "▼"
	if a.sortAsc {
		dir = "▲"
	}
	title := titleStyle.Render("WARTT")
	sortInfo := sortStyle.Render(fmt.Sprintf("  sort:%s%s", columnDefs[a.sortCol].Title, dir))
	hints := hintStyle.Render("<> col  s:dir  Enter:detail  r:refresh  q:quit")

	left := lipgloss.JoinHorizontal(lipgloss.Top, title, sortInfo)
	padding := a.width - lipgloss.Width(left) - lipgloss.Width(hints)
	if padding < 1 {
		padding = 1
	}
	spacer := lipgloss.NewStyle().Width(padding).Render("")
	return lipgloss.JoinHorizontal(lipgloss.Top, left, spacer, hints)
}

func (a *App) renderSummaryPanel() string {
	sep := lipgloss.NewStyle().Foreground(lipgloss.Color("240")).
		Render(strings.Repeat("─", a.width))
	return lipgloss.JoinVertical(lipgloss.Left, sep, renderSummary(a.summary, a.width))
}

func (a *App) rebuildTable() {
	// Reserve lines: 1 header + summaryPanelLines + 1 status/padding
	tableHeight := a.height - 1 - summaryPanelLines - 1
	if tableHeight < 5 {
		tableHeight = 5
	}

	a.traces = sortedCopy(a.rawTraces, a.sortCol, a.sortAsc)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true).
		Foreground(lipgloss.Color("15"))
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	s.Cell = s.Cell.Foreground(lipgloss.Color("252"))

	t := table.New(
		table.WithColumns(tailColumns(a.width)),
		table.WithRows(traceRows(a.traces)),
		table.WithFocused(true),
		table.WithHeight(tableHeight),
		table.WithStyles(s),
	)
	a.table = t
}

func Run(db *sql.DB) error {
	app := NewApp(db)
	p := tea.NewProgram(app, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
