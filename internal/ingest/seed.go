// Package ingest: seed.go seeds the DB from the legacy NDJSON events file.
package ingest

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"wartt/internal/aggregate"
	"wartt/internal/config"
	"wartt/internal/model"
)

type ndjsonEvent struct {
	TraceID     string `json:"trace_id"`
	MessageType string `json:"message_type"`
	Stage       string `json:"stage"`
	TsUnixMs    int64  `json:"ts_unix_ms"`
	Status      string `json:"status"`
	Meta        struct {
		LLMProvider    string `json:"llm_provider"`
		LLMModel       string `json:"llm_model"`
		ToolName       string `json:"tool_name"`
		ClientSendMs   int64  `json:"client_send_ms"`
		MessagePreview string `json:"message_preview"`
	} `json:"meta"`
}

// SeedFromNDJSON reads an NDJSON events file and upserts events + traces.
func SeedFromNDJSON(db *sql.DB, cfg *config.Config, ndjsonPath string) error {
	f, err := os.Open(ndjsonPath)
	if err != nil {
		return fmt.Errorf("open ndjson: %w", err)
	}
	defer f.Close()

	var events []model.Event
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 512*1024), 512*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var raw ndjsonEvent
		if err := json.Unmarshal(line, &raw); err != nil {
			continue
		}
		if raw.TraceID == "" || raw.TsUnixMs <= 0 {
			continue
		}
		events = append(events, model.Event{
			TraceID:     raw.TraceID,
			MessageType: raw.MessageType,
			Stage:       raw.Stage,
			TsUnixMs:    raw.TsUnixMs,
			Status:      raw.Status,
			Meta: model.EventMeta{
				LLMProvider:    raw.Meta.LLMProvider,
				LLMModel:       raw.Meta.LLMModel,
				ToolName:       raw.Meta.ToolName,
				ClientSendMs:   raw.Meta.ClientSendMs,
				MessagePreview: raw.Meta.MessagePreview,
			},
		})
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	if len(events) == 0 {
		return fmt.Errorf("no events found in %s", ndjsonPath)
	}
	fmt.Fprintf(os.Stderr, "wartt: seeding %d events from %s\n", len(events), ndjsonPath)

	// Insert events
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO events
			(trace_id, message_type, stage, ts_unix_ms, status, meta)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	traceIDs := map[string]bool{}
	for _, e := range events {
		meta := metaJSON(e.Meta)
		if _, err := stmt.Exec(e.TraceID, e.MessageType, e.Stage, e.TsUnixMs, e.Status, meta); err != nil {
			return err
		}
		traceIDs[e.TraceID] = true
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	// Aggregate all affected traces
	agg := aggregate.New(cfg)
	for tid := range traceIDs {
		traceEvents, err := loadTraceEvents(db, tid)
		if err != nil {
			continue
		}
		trace := agg.Aggregate(tid, traceEvents)
		if trace == nil {
			continue
		}
		if err := upsertTrace(db, trace); err != nil {
			return fmt.Errorf("upsert trace %s: %w", tid, err)
		}
	}

	fmt.Fprintf(os.Stderr, "wartt: seeded %d traces\n", len(traceIDs))
	return nil
}
