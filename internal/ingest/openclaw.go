// Package ingest parses OpenClaw gateway log lines into Event structs.
package ingest

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"wartt/internal/aggregate"
	"wartt/internal/config"
	"wartt/internal/model"
)

// State tracks ingest progress across runs.
type State struct {
	Path   string
	Inode  uint64
	Offset int64
}

// ReadState reads the state file (path\tinode\toffset).
func ReadState(stateFile string) (*State, error) {
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return &State{}, nil
		}
		return nil, err
	}
	parts := strings.Split(strings.TrimSpace(string(data)), "\t")
	if len(parts) < 3 {
		return &State{}, nil
	}
	inode, _ := strconv.ParseUint(parts[1], 10, 64)
	offset, _ := strconv.ParseInt(parts[2], 10, 64)
	return &State{Path: parts[0], Inode: inode, Offset: offset}, nil
}

// WriteState persists state to disk.
func WriteState(stateFile string, path string, inode uint64, size int64) error {
	content := fmt.Sprintf("%s\t%d\t%d\n", path, inode, size)
	return os.WriteFile(stateFile, []byte(content), 0644)
}

// fileInode returns the inode of a file.
func fileInode(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return inodeFromFileInfo(info), nil
}

// ResolveDefaultSource finds the most recently written OpenClaw log file.
func ResolveDefaultSource() (string, error) {
	patterns := []string{
		"/tmp/openclaw/openclaw-*.log",
		"/tmp/openclaw-*/openclaw-*.log",
	}
	var newest string
	var newestTime time.Time
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		for _, m := range matches {
			info, err := os.Stat(m)
			if err != nil {
				continue
			}
			if info.ModTime().After(newestTime) {
				newestTime = info.ModTime()
				newest = m
			}
		}
	}
	if newest == "" {
		return "", fmt.Errorf("no OpenClaw log source found")
	}
	return newest, nil
}

// IngestOnce reads new lines from sourceFile and stores events + traces in db.
func IngestOnce(db *sql.DB, cfg *config.Config, sourceFile string) error {
	info, err := os.Stat(sourceFile)
	if err != nil {
		return fmt.Errorf("stat source: %w", err)
	}
	localInode := inodeFromFileInfo(info)
	localSize := info.Size()

	state, err := ReadState(cfg.StateFile)
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}

	// Determine start offset
	startByte := int64(0)
	if state.Path == sourceFile && state.Inode == localInode && state.Offset <= localSize {
		if localSize <= state.Offset {
			// No new data
			return WriteState(cfg.StateFile, sourceFile, localInode, localSize)
		}
		startByte = state.Offset - cfg.IngestLookbackBytes
		if startByte < 0 {
			startByte = 0
		}
	}

	f, err := os.Open(sourceFile)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer f.Close()

	if startByte > 0 {
		if _, err := f.Seek(startByte, io.SeekStart); err != nil {
			return fmt.Errorf("seek: %w", err)
		}
	}

	events, err := parseStream(f)
	if err != nil {
		return fmt.Errorf("parse stream: %w", err)
	}

	// Filter to only new events if we have state
	if state.Path == sourceFile && state.Inode == localInode {
		minTs := lastIngestedTs(db)
		filtered := events[:0]
		for _, e := range events {
			if e.TsUnixMs > minTs {
				filtered = append(filtered, e)
			}
		}
		events = filtered
	}

	if len(events) > 0 {
		if err := storeEvents(db, cfg, events); err != nil {
			return fmt.Errorf("store events: %w", err)
		}
	}

	return WriteState(cfg.StateFile, sourceFile, localInode, localSize)
}

// lastIngestedTs returns the maximum ts_unix_ms in the events table.
func lastIngestedTs(db *sql.DB) int64 {
	var ts int64
	row := db.QueryRow(`SELECT COALESCE(MAX(ts_unix_ms), 0) FROM events`)
	_ = row.Scan(&ts)
	return ts
}

// storeEvents inserts events into DB and then aggregates new traces.
func storeEvents(db *sql.DB, cfg *config.Config, events []model.Event) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO events
			(trace_id, message_type, stage, ts_unix_ms, status, meta, channel)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Collect affected trace IDs
	traceIDs := map[string]bool{}
	for _, e := range events {
		meta := metaJSON(e.Meta)
		_, err := stmt.Exec(e.TraceID, e.MessageType, e.Stage, e.TsUnixMs, e.Status, meta, e.Channel)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
		traceIDs[e.TraceID] = true
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Aggregate each affected trace
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

	return nil
}

func loadTraceEvents(db *sql.DB, traceID string) ([]model.Event, error) {
	rows, err := db.Query(`
		SELECT trace_id, message_type, stage, ts_unix_ms, status, meta, channel
		FROM events WHERE trace_id = ? ORDER BY ts_unix_ms ASC
	`, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []model.Event
	for rows.Next() {
		var e model.Event
		var metaStr string
		if err := rows.Scan(&e.TraceID, &e.MessageType, &e.Stage, &e.TsUnixMs, &e.Status, &metaStr, &e.Channel); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(metaStr), &e.Meta)
		events = append(events, e)
	}
	return events, rows.Err()
}

func upsertTrace(db *sql.DB, t *model.Trace) error {
	_, err := db.Exec(`
		INSERT INTO traces (
			trace_id, time_eet, ts_unix_ms, message_type, channel, status, message_preview,
			t1_inbound_gateway_eet, t2_stt_start_eet, t3_stt_end_eet,
			t4_llm_start_eet, t5_llm_end_eet, t6_outbound_send_eet,
			total_ms, queue_wait_ms, upload_ingest_ms, queue_before_stt_ms,
			download_audio_ms, whisper_total_ms, transcribe_ms, tool_calls_ms,
			llm_total_ms, llm_latency_ms, overhead_ms,
			llm_model, latency_class, bottleneck_stage, bottleneck_ms
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(trace_id) DO UPDATE SET
			time_eet=excluded.time_eet, ts_unix_ms=excluded.ts_unix_ms,
			message_type=excluded.message_type, channel=excluded.channel,
			status=excluded.status,
			message_preview=excluded.message_preview,
			t1_inbound_gateway_eet=excluded.t1_inbound_gateway_eet,
			t2_stt_start_eet=excluded.t2_stt_start_eet,
			t3_stt_end_eet=excluded.t3_stt_end_eet,
			t4_llm_start_eet=excluded.t4_llm_start_eet,
			t5_llm_end_eet=excluded.t5_llm_end_eet,
			t6_outbound_send_eet=excluded.t6_outbound_send_eet,
			total_ms=excluded.total_ms, queue_wait_ms=excluded.queue_wait_ms,
			upload_ingest_ms=excluded.upload_ingest_ms,
			queue_before_stt_ms=excluded.queue_before_stt_ms,
			download_audio_ms=excluded.download_audio_ms,
			whisper_total_ms=excluded.whisper_total_ms,
			transcribe_ms=excluded.transcribe_ms,
			tool_calls_ms=excluded.tool_calls_ms,
			llm_total_ms=excluded.llm_total_ms, llm_latency_ms=excluded.llm_latency_ms,
			overhead_ms=excluded.overhead_ms, llm_model=excluded.llm_model,
			latency_class=excluded.latency_class,
			bottleneck_stage=excluded.bottleneck_stage,
			bottleneck_ms=excluded.bottleneck_ms
	`,
		t.TraceID, t.TimeEET, t.TsUnixMs, t.MessageType, t.Channel, t.Status, t.MessagePreview,
		t.T1InboundGatewayEET, t.T2SttStartEET, t.T3SttEndEET,
		t.T4LlmStartEET, t.T5LlmEndEET, t.T6OutboundSendEET,
		t.TotalMs, t.QueueWaitMs, t.UploadIngestMs, t.QueueBeforeSttMs,
		t.DownloadAudioMs, t.WhisperTotalMs, t.TranscribeMs, t.ToolCallsMs,
		t.LLMTotalMs, t.LLMLatencyMs, t.OverheadMs,
		t.LLMModel, t.LatencyClass, t.BottleneckStage, t.BottleneckMs,
	)
	return err
}

func metaJSON(m model.EventMeta) string {
	if m.LLMProvider == "" && m.LLMModel == "" && m.ToolName == "" &&
		m.ClientSendMs == 0 && m.MessagePreview == "" {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// ─── Log line parsing ────────────────────────────────────────────────────────

// logLine represents one parsed JSON line from an OpenClaw log.
type logLine struct {
	tsUnixMs int64
	msg      string // "1" field — message JSON or description text
	desc     string // "2" field — log description
}

// rawLogLine is used for JSON unmarshaling.
type rawLogLine struct {
	Time string          `json:"time"`
	F0   json.RawMessage `json:"0"`
	F1   json.RawMessage `json:"1"`
	F2   json.RawMessage `json:"2"`
}

func parseLogLine(raw []byte) (*logLine, bool) {
	var r rawLogLine
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, false
	}
	if r.Time == "" {
		return nil, false
	}
	ts := parseTimeMs(r.Time)
	if ts <= 0 {
		return nil, false
	}
	msg := jsonFieldToString(r.F1)
	desc := jsonFieldToString(r.F2)
	return &logLine{tsUnixMs: ts, msg: msg, desc: desc}, true
}

func jsonFieldToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	// Try as plain string first
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	// Otherwise return raw JSON
	return string(raw)
}

func parseTimeMs(t string) int64 {
	// Try ISO8601 with sub-second: 2006-01-02T15:04:05.999Z
	layouts := []string{
		"2006-01-02T15:04:05.999999999Z",
		"2006-01-02T15:04:05.999Z",
		"2006-01-02T15:04:05Z",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, t); err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

// ─── State machine for parsing ───────────────────────────────────────────────

var (
	reLaneDequeue   = regexp.MustCompile(`lane dequeue:.*waitMs=(\d+)`)
	reRunStart      = regexp.MustCompile(`embedded run start: runId=([0-9a-fA-F-]+).*messageChannel=(\w+)`)
	reRunProvider   = regexp.MustCompile(`provider=(\S+)`)
	reRunModel      = regexp.MustCompile(`model=(\S+)`)
	rePromptStart   = regexp.MustCompile(`embedded run prompt start: runId=([0-9a-fA-F-]+)`)
	rePromptEnd     = regexp.MustCompile(`embedded run prompt end: runId=([0-9a-fA-F-]+)`)
	reToolStart     = regexp.MustCompile(`embedded run tool start: runId=([0-9a-fA-F-]+).*tool=(\S+)`)
	reToolEnd       = regexp.MustCompile(`embedded run tool end: runId=([0-9a-fA-F-]+).*tool=(\S+)`)
	reAgentEnd      = regexp.MustCompile(`embedded run agent end: runId=([0-9a-fA-F-]+) isError=(true|false)`)
	reRunDone       = regexp.MustCompile(`embedded run done: runId=([0-9a-fA-F-]+).*aborted=(true|false)`)

	reMediaType  = regexp.MustCompile(`(?i)"mediatype":"([^"]+)"`)
	reMediaKind  = regexp.MustCompile(`(?i)"mediakind":"([^"]+)"`)
	reMediaPath  = regexp.MustCompile(`(?i)"mediapath":"[^"]+\.(ogg|opus|mp3|wav|m4a)"`)
	reMediaTag   = regexp.MustCompile(`(?i)<media:\s*(audio|voice)>`)
	reTimestamp  = regexp.MustCompile(`"timestamp":\s*(\d{10,13})`)
	reBodyText   = regexp.MustCompile(`"body":"([^"]*)"`)
	reTextField  = regexp.MustCompile(`"text":"([^"]*)"`)
	reMediaTypeG = regexp.MustCompile(`(?i)"mediatype":"([^"]+)"`)
)

type inboundEntry struct {
	ts       int64
	msgType  string
	clientTs int64
	preview  string
	used     bool
}

type runState struct {
	provider string
	model    string
	msgType  string
	channel  string
	preview  string
	isError  bool
}

func detectMessageType(msg string) string {
	lower := strings.ToLower(msg)
	if m := reMediaType.FindStringSubmatch(lower); len(m) > 1 {
		if isAudioType(m[1]) {
			return "voice"
		}
		return "text"
	}
	if m := reMediaKind.FindStringSubmatch(lower); len(m) > 1 {
		if isAudioType(m[1]) {
			return "voice"
		}
		return "text"
	}
	if reMediaTag.MatchString(lower) {
		return "voice"
	}
	if reMediaPath.MatchString(lower) {
		return "voice"
	}
	return "text"
}

func isAudioType(s string) bool {
	s = strings.ToLower(s)
	for _, t := range []string{"audio", "voice", "ptt", "opus", "ogg", "m4a", "mp3", "wav"} {
		if strings.Contains(s, t) {
			return true
		}
	}
	return false
}

func extractClientSendMs(msg string) int64 {
	m := reTimestamp.FindStringSubmatch(msg)
	if len(m) < 2 {
		return 0
	}
	raw, _ := strconv.ParseInt(m[1], 10, 64)
	if raw <= 0 {
		return 0
	}
	// Convert seconds to ms if needed
	if raw < 1_000_000_000_000 {
		raw *= 1000
	}
	return raw
}

func extractMessagePreview(msg string) string {
	var raw string
	if m := reBodyText.FindStringSubmatch(msg); len(m) > 1 {
		raw = m[1]
	} else if m := reTextField.FindStringSubmatch(msg); len(m) > 1 {
		raw = m[1]
	}
	if raw == "" {
		if m := reMediaTypeG.FindStringSubmatch(strings.ToLower(msg)); len(m) > 1 {
			raw = "<media:" + m[1] + ">"
		}
	}
	// Unescape common sequences
	raw = strings.ReplaceAll(raw, `\n`, " ")
	raw = strings.ReplaceAll(raw, `\r`, " ")
	raw = strings.ReplaceAll(raw, `\"`, `"`)
	raw = strings.ReplaceAll(raw, `\\`, `\`)
	// Normalize whitespace
	raw = strings.Join(strings.Fields(raw), " ")
	// Replace commas (CSV safety)
	raw = strings.ReplaceAll(raw, ",", ";")
	if len(raw) > 180 {
		raw = raw[:177] + "..."
	}
	return raw
}

func parseStream(r io.Reader) ([]model.Event, error) {
	var events []model.Event

	// Inbound tracking
	inbounds := make([]inboundEntry, 0, 128)

	// Pending state
	var pendingClientTs int64
	var pendingClientSeenTs int64
	var pendingPreview string
	var pendingPreviewSeenTs int64
	var pendingWaitMs int64
	var pendingWaitTs int64

	// Per-run state
	runs := map[string]*runState{}

	emit := func(traceID, msgType, channel, stage string, ts int64, status, toolName, provider, llmModel string, clientSendMs int64, preview string) {
		if traceID == "" || ts <= 0 || stage == "" {
			return
		}
		if msgType == "" {
			msgType = "text"
		}
		e := model.Event{
			TraceID:     traceID,
			MessageType: msgType,
			Channel:     channel,
			Stage:       stage,
			TsUnixMs:    ts,
			Status:      status,
			Meta: model.EventMeta{
				LLMProvider:    provider,
				LLMModel:       llmModel,
				ToolName:       toolName,
				ClientSendMs:   clientSendMs,
				MessagePreview: preview,
			},
		}
		events = append(events, e)
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		ll, ok := parseLogLine(line)
		if !ok {
			continue
		}
		ts := ll.tsUnixMs
		msg := ll.msg
		desc := ll.desc

		switch desc {
		case "inbound web message":
			clientTs := extractClientSendMs(msg)
			preview := extractMessagePreview(msg)
			// Fall back to pending state if within 5s
			if clientTs <= 0 && pendingClientTs > 0 && (ts-pendingClientSeenTs) <= 5000 {
				clientTs = pendingClientTs
			}
			if preview == "" && pendingPreview != "" && (ts-pendingPreviewSeenTs) <= 5000 {
				preview = pendingPreview
			}
			inbounds = append(inbounds, inboundEntry{
				ts:       ts,
				msgType:  detectMessageType(msg),
				clientTs: clientTs,
				preview:  preview,
			})
			continue

		case "inbound message":
			pendingClientTs = extractClientSendMs(msg)
			pendingClientSeenTs = ts
			pendingPreview = extractMessagePreview(msg)
			pendingPreviewSeenTs = ts
			continue
		}

		// Check for lane dequeue
		if m := reLaneDequeue.FindStringSubmatch(msg); len(m) > 1 {
			pendingWaitMs, _ = strconv.ParseInt(m[1], 10, 64)
			pendingWaitTs = ts
			continue
		}

		// Embedded run start — group 1 = runId, group 2 = channel
		if m := reRunStart.FindStringSubmatch(msg); len(m) > 2 {
			runID := m[1]
			channel := m[2]
			provider := ""
			llmModel := ""
			if pm := reRunProvider.FindStringSubmatch(msg); len(pm) > 1 {
				provider = pm[1]
			}
			if mm := reRunModel.FindStringSubmatch(msg); len(mm) > 1 {
				llmModel = mm[1]
			}

			// Match to most recent unused inbound within 30s
			runType := "text"
			var inboundTs int64
			var inboundClientTs int64
			var inboundPreview string
			for j := len(inbounds) - 1; j >= 0 && j >= len(inbounds)-120; j-- {
				if inbounds[j].used {
					continue
				}
				delta := ts - inbounds[j].ts
				if delta < 0 {
					continue
				}
				if delta <= 30000 {
					inbounds[j].used = true
					inboundTs = inbounds[j].ts
					runType = inbounds[j].msgType
					inboundClientTs = inbounds[j].clientTs
					inboundPreview = inbounds[j].preview
					break
				}
			}

			waitMs := int64(0)
			if pendingWaitMs > 0 && (ts-pendingWaitTs) <= 5000 {
				waitMs = pendingWaitMs
			}
			pendingWaitMs = 0
			pendingWaitTs = 0

			if inboundTs <= 0 {
				inboundTs = ts - waitMs
			}

			runs[runID] = &runState{
				provider: provider,
				model:    llmModel,
				msgType:  runType,
				channel:  channel,
				preview:  inboundPreview,
			}

			emit(runID, runType, channel, "inbound_event_received", inboundTs, "", "", provider, llmModel, inboundClientTs, inboundPreview)
			emit(runID, runType, channel, "processing_start", ts, "", "", provider, llmModel, 0, "")
			continue
		}

		// Embedded run prompt start
		if m := rePromptStart.FindStringSubmatch(msg); len(m) > 1 {
			runID := m[1]
			rs := runs[runID]
			if rs == nil {
				continue
			}
			emit(runID, rs.msgType, rs.channel, "llm_start", ts, "", "", rs.provider, rs.model, 0, "")
			continue
		}

		// Embedded run prompt end
		if m := rePromptEnd.FindStringSubmatch(msg); len(m) > 1 {
			runID := m[1]
			rs := runs[runID]
			if rs == nil {
				continue
			}
			emit(runID, rs.msgType, rs.channel, "llm_end", ts, "", "", rs.provider, rs.model, 0, "")
			continue
		}

		// Embedded run tool start
		if m := reToolStart.FindStringSubmatch(msg); len(m) > 2 {
			runID := m[1]
			tool := m[2]
			rs := runs[runID]
			if rs == nil {
				continue
			}
			emit(runID, rs.msgType, rs.channel, "tool_call_start", ts, "", tool, rs.provider, rs.model, 0, "")
			continue
		}

		// Embedded run tool end
		if m := reToolEnd.FindStringSubmatch(msg); len(m) > 2 {
			runID := m[1]
			tool := m[2]
			rs := runs[runID]
			if rs == nil {
				continue
			}
			emit(runID, rs.msgType, rs.channel, "tool_call_end", ts, "", tool, rs.provider, rs.model, 0, "")
			continue
		}

		// Embedded run agent end
		if m := reAgentEnd.FindStringSubmatch(msg); len(m) > 2 {
			runID := m[1]
			if rs := runs[runID]; rs != nil {
				rs.isError = m[2] == "true"
			}
			continue
		}

		// Embedded run done
		if m := reRunDone.FindStringSubmatch(msg); len(m) > 2 {
			runID := m[1]
			aborted := m[2] == "true"
			rs := runs[runID]
			if rs == nil {
				continue
			}
			status := "ok"
			if rs.isError {
				status = "error"
			} else if aborted {
				status = "timeout"
			}
			emit(runID, rs.msgType, rs.channel, "response_sent", ts, status, "", rs.provider, rs.model, 0, "")
			delete(runs, runID)
			continue
		}
	}

	return events, scanner.Err()
}

// inodeFromFileInfo extracts the inode number from os.FileInfo.
// On Linux this uses the underlying syscall.Stat_t.
func inodeFromFileInfo(info os.FileInfo) uint64 {
	return inodeNum(info)
}
