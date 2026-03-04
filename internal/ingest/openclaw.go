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
	Path          string                     `json:"path"`
	Inode         uint64                     `json:"inode"`
	Offset        int64                      `json:"offset"`
	SessionCursor map[string]sessionProgress `json:"discord_sessions,omitempty"`
}

type sessionProgress struct {
	Inode          uint64 `json:"inode"`
	Offset         int64  `json:"offset"`
	PendingTraceID string `json:"pending_trace_id,omitempty"`
	PendingTs      int64  `json:"pending_ts,omitempty"`
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
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return &State{SessionCursor: map[string]sessionProgress{}}, nil
	}

	if strings.HasPrefix(trimmed, "{") {
		var st State
		if err := json.Unmarshal([]byte(trimmed), &st); err != nil {
			return nil, err
		}
		if st.SessionCursor == nil {
			st.SessionCursor = map[string]sessionProgress{}
		}
		return &st, nil
	}

	parts := strings.Split(trimmed, "\t")
	if len(parts) < 3 {
		return &State{SessionCursor: map[string]sessionProgress{}}, nil
	}
	inode, _ := strconv.ParseUint(parts[1], 10, 64)
	offset, _ := strconv.ParseInt(parts[2], 10, 64)
	return &State{
		Path:          parts[0],
		Inode:         inode,
		Offset:        offset,
		SessionCursor: map[string]sessionProgress{},
	}, nil
}

// WriteState persists state to disk.
func WriteState(stateFile string, st *State) error {
	if st == nil {
		st = &State{}
	}
	if st.SessionCursor == nil {
		st.SessionCursor = map[string]sessionProgress{}
	}
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(stateFile, b, 0644)
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
	if state.SessionCursor == nil {
		state.SessionCursor = map[string]sessionProgress{}
	}
	minTs := int64(0)
	if state.Path == sourceFile && state.Inode == localInode {
		minTs = lastIngestedTs(db)
	}

	// Determine start offset
	startByte := int64(0)
	sourceHasNewData := true
	if state.Path == sourceFile && state.Inode == localInode && state.Offset <= localSize {
		if localSize <= state.Offset {
			sourceHasNewData = false
		}
		if sourceHasNewData {
			startByte = state.Offset - cfg.IngestLookbackBytes
			if startByte < 0 {
				startByte = 0
			}
		}
	}

	events := make([]model.Event, 0, 256)
	if sourceHasNewData {
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

		streamEvents, err := parseStream(f)
		if err != nil {
			return fmt.Errorf("parse stream: %w", err)
		}
		events = append(events, streamEvents...)
	}
	sessionEvents, err := parseDiscordSessionEvents(minTs, state)
	if err == nil && len(sessionEvents) > 0 {
		events = append(events, sessionEvents...)
	}

	// Filter to only new events if we have state
	if state.Path == sourceFile && state.Inode == localInode {
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

	state.Path = sourceFile
	state.Inode = localInode
	state.Offset = localSize
	return WriteState(cfg.StateFile, state)
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
	source   string // "0" field — source JSON/name
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
	source := jsonFieldToString(r.F0)
	msg := jsonFieldToString(r.F1)
	desc := jsonFieldToString(r.F2)
	return &logLine{tsUnixMs: ts, source: source, msg: msg, desc: desc}, true
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
	// Accept both Z and explicit timezone offsets.
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
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
	reLaneDequeue      = regexp.MustCompile(`lane dequeue:.*waitMs=(\d+)`)
	reRunStart         = regexp.MustCompile(`embedded run start: runId=([0-9a-fA-F-]+).*messageChannel=(\w+)`)
	reRunProvider      = regexp.MustCompile(`provider=(\S+)`)
	reRunModel         = regexp.MustCompile(`model=(\S+)`)
	rePromptStart      = regexp.MustCompile(`embedded run prompt start: runId=([0-9a-fA-F-]+)`)
	rePromptEnd        = regexp.MustCompile(`embedded run prompt end: runId=([0-9a-fA-F-]+)`)
	reToolStart        = regexp.MustCompile(`embedded run tool start: runId=([0-9a-fA-F-]+).*tool=(\S+)`)
	reToolEnd          = regexp.MustCompile(`embedded run tool end: runId=([0-9a-fA-F-]+).*tool=(\S+)`)
	reAgentEnd         = regexp.MustCompile(`embedded run agent end: runId=([0-9a-fA-F-]+) isError=(true|false)`)
	reRunDone          = regexp.MustCompile(`embedded run done: runId=([0-9a-fA-F-]+).*aborted=(true|false)`)
	reModuleInSrc      = regexp.MustCompile(`(?i)"module":"([^"]+)"`)
	reSubsystemInSrc   = regexp.MustCompile(`(?i)"subsystem":"([^"]+)"`)
	reRunIDInSrc       = regexp.MustCompile(`(?i)"runid":"([0-9a-fA-F-]+)"`)
	reDiscordChanID    = regexp.MustCompile(`(?i)"channelid":"([^"]+)"`)
	reDiscordInbVb     = regexp.MustCompile(`(?i)discord inbound:.*preview="([^"]*)"`)
	reDiscordDelVb     = regexp.MustCompile(`(?i)discord:\s+delivered\s+\d+\s+repl(?:y|ies)\s+to\s+(\S+)`)
	reDiscordMsgCreate = regexp.MustCompile(`(?i)"event":"message_create"`)

	reMediaType     = regexp.MustCompile(`(?i)"mediatype":"([^"]+)"`)
	reMediaKind     = regexp.MustCompile(`(?i)"mediakind":"([^"]+)"`)
	reMediaPath     = regexp.MustCompile(`(?i)"mediapath":"[^"]+\.(ogg|opus|mp3|wav|m4a)"`)
	reMediaTag      = regexp.MustCompile(`(?i)<media:\s*(audio|voice)>`)
	reTimestamp     = regexp.MustCompile(`"timestamp":\s*(\d{10,13})`)
	reBodyText      = regexp.MustCompile(`"body":"([^"]*)"`)
	reTextField     = regexp.MustCompile(`"text":"([^"]*)"`)
	reMediaTypeG    = regexp.MustCompile(`(?i)"mediatype":"([^"]+)"`)
	reCorrelationID = regexp.MustCompile(`(?i)"correlationid":"([^"]+)"`)
	reDiscordMsgID  = regexp.MustCompile(`(?i)"message_id"\s*:\s*"([^"]+)"`)
)

type inboundEntry struct {
	traceID  string
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

func extractModuleAndRunID(source string) (module, runID string) {
	if m := reModuleInSrc.FindStringSubmatch(source); len(m) > 1 {
		module = strings.ToLower(m[1])
	}
	if m := reRunIDInSrc.FindStringSubmatch(source); len(m) > 1 {
		runID = m[1]
	}
	return module, runID
}

func extractSubsystem(source string) string {
	if m := reSubsystemInSrc.FindStringSubmatch(source); len(m) > 1 {
		return strings.ToLower(m[1])
	}
	return ""
}

func channelFromModule(module string) string {
	switch {
	case strings.HasPrefix(module, "web-"):
		return "whatsapp"
	case strings.HasPrefix(module, "discord-"):
		return "discord"
	default:
		return ""
	}
}

func extractDiscordChannelID(msg string) string {
	if m := reDiscordChanID.FindStringSubmatch(msg); len(m) > 1 {
		return m[1]
	}
	return ""
}

func extractCorrelationID(msg string) string {
	if m := reCorrelationID.FindStringSubmatch(msg); len(m) > 1 {
		return strings.ToLower(m[1])
	}
	return ""
}

func composeTraceID(runID, msg string) string {
	if runID == "" {
		return ""
	}
	if corr := extractCorrelationID(msg); corr != "" {
		return runID + ":" + corr
	}
	return runID
}

func parseStream(r io.Reader) ([]model.Event, error) {
	var events []model.Event

	// Inbound tracking
	inbounds := make([]inboundEntry, 0, 128)
	discordInbounds := make([]inboundEntry, 0, 64)

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
		source := ll.source
		msg := ll.msg
		desc := ll.desc
		module, sourceRunID := extractModuleAndRunID(source)
		subsystem := extractSubsystem(source)
		lowerMsg := strings.ToLower(msg)

		// Newer OpenClaw streams can emit discord preflight skips without embedded traces.
		// Surface them as short traces so channel activity is visible in wartt.
		if strings.HasPrefix(desc, "discord: skipping ") {
			traceID := sourceRunID
			if traceID == "" {
				traceID = fmt.Sprintf("discord-skip-%d-%s", ts, extractDiscordChannelID(msg))
			}
			emit(traceID, "text", "discord", "inbound_event_received", ts, "", "", "", "", 0, desc)
			emit(traceID, "text", "discord", "response_sent", ts, "ok", "", "", "", 0, "")
			continue
		}

		// Fallback for Discord traffic when only monitor events are visible in info logs.
		if subsystem == "discord/monitor" && desc == "Slow listener detected" && reDiscordMsgCreate.MatchString(msg) {
			traceID := fmt.Sprintf("discord-monitor-%d", ts)
			emit(traceID, "text", "discord", "inbound_event_received", ts, "", "", "", "", 0, "discord MESSAGE_CREATE")
			emit(traceID, "text", "discord", "processing_start", ts, "", "", "", "", 0, "")
			continue
		}

		// Successful Discord auto-reply logs are often emitted only in verbose mode and
		// don't include run IDs; synthesize traces from inbound/delivered verbose lines.
		if strings.Contains(lowerMsg, "discord inbound:") {
			traceID := fmt.Sprintf("discord-vb-%d", ts)
			preview := ""
			if m := reDiscordInbVb.FindStringSubmatch(msg); len(m) > 1 {
				preview = strings.ReplaceAll(m[1], ",", ";")
			}
			entry := inboundEntry{
				traceID: traceID,
				ts:      ts,
				msgType: "text",
				preview: preview,
			}
			discordInbounds = append(discordInbounds, entry)
			emit(traceID, "text", "discord", "inbound_event_received", ts, "", "", "", "", 0, preview)
			emit(traceID, "text", "discord", "processing_start", ts, "", "", "", "", 0, "")
			continue
		}
		if strings.Contains(lowerMsg, "discord: delivered ") && reDiscordDelVb.MatchString(msg) {
			matched := false
			for i := len(discordInbounds) - 1; i >= 0 && i >= len(discordInbounds)-60; i-- {
				if discordInbounds[i].used {
					continue
				}
				delta := ts - discordInbounds[i].ts
				if delta < 0 || delta > 120000 {
					continue
				}
				discordInbounds[i].used = true
				emit(discordInbounds[i].traceID, "text", "discord", "response_sent", ts, "ok", "", "", "", 0, "")
				matched = true
				break
			}
			if !matched {
				traceID := fmt.Sprintf("discord-vb-%d", ts)
				emit(traceID, "text", "discord", "inbound_event_received", ts, "", "", "", "", 0, "")
				emit(traceID, "text", "discord", "response_sent", ts, "ok", "", "", "", 0, "")
			}
			continue
		}

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
			if sourceRunID != "" {
				traceID := composeTraceID(sourceRunID, msg)
				if traceID == "" {
					traceID = sourceRunID
				}
				channel := channelFromModule(module)
				if channel == "" {
					channel = "whatsapp"
				}
				msgType := detectMessageType(msg)
				runs[traceID] = &runState{
					msgType: msgType,
					channel: channel,
					preview: preview,
				}
				emit(traceID, msgType, channel, "inbound_event_received", ts, "", "", "", "", clientTs, preview)
				emit(traceID, msgType, channel, "processing_start", ts, "", "", "", "", 0, "")
			}
			continue

		case "inbound message":
			pendingClientTs = extractClientSendMs(msg)
			pendingClientSeenTs = ts
			pendingPreview = extractMessagePreview(msg)
			pendingPreviewSeenTs = ts
			continue

		case "auto-reply sent (text)", "auto-reply sent (media)", "auto-reply sent (voice)":
			if sourceRunID == "" {
				continue
			}
			traceID := composeTraceID(sourceRunID, msg)
			if traceID == "" {
				traceID = sourceRunID
			}
			rs := runs[traceID]
			msgType := detectMessageType(msg)
			channel := channelFromModule(module)
			if rs != nil {
				if rs.msgType != "" {
					msgType = rs.msgType
				}
				if rs.channel != "" {
					channel = rs.channel
				}
			}
			if channel == "" {
				channel = "whatsapp"
			}
			emit(traceID, msgType, channel, "response_sent", ts, "ok", "", "", "", 0, "")
			delete(runs, traceID)
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

type ocSessionIndexEntry struct {
	SessionFile string `json:"sessionFile"`
	LastChannel string `json:"lastChannel"`
}

type ocSessionLine struct {
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	Message   struct {
		Role      string `json:"role"`
		Timestamp int64  `json:"timestamp"`
		Content   []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"message"`
}

func parseDiscordSessionEvents(minTs int64, state *State) ([]model.Event, error) {
	stateDir := resolveOpenClawStateDir()
	if stateDir == "" {
		return nil, fmt.Errorf("openclaw state dir not found")
	}
	if state == nil {
		state = &State{}
	}
	if state.SessionCursor == nil {
		state.SessionCursor = map[string]sessionProgress{}
	}
	indexPaths, err := filepath.Glob(filepath.Join(stateDir, "agents", "*", "sessions", "sessions.json"))
	if err != nil || len(indexPaths) == 0 {
		return nil, fmt.Errorf("no openclaw session indexes found")
	}

	events := make([]model.Event, 0, 128)
	seen := map[string]bool{}
	for _, indexPath := range indexPaths {
		data, err := os.ReadFile(indexPath)
		if err != nil {
			continue
		}
		var idx map[string]json.RawMessage
		if err := json.Unmarshal(data, &idx); err != nil {
			continue
		}
		baseDir := filepath.Dir(indexPath)
		for _, raw := range idx {
			var entry ocSessionIndexEntry
			if err := json.Unmarshal(raw, &entry); err != nil {
				continue
			}
			if entry.LastChannel != "discord" || entry.SessionFile == "" {
				continue
			}
			sessionPath := entry.SessionFile
			if !filepath.IsAbs(sessionPath) {
				sessionPath = filepath.Join(baseDir, sessionPath)
			}
			seen[sessionPath] = true
			cursor := state.SessionCursor[sessionPath]
			ev, nextCursor, err := parseDiscordSessionFile(sessionPath, minTs, cursor)
			if err != nil {
				continue
			}
			state.SessionCursor[sessionPath] = nextCursor
			events = append(events, ev...)
		}
	}
	for path := range state.SessionCursor {
		if !seen[path] {
			delete(state.SessionCursor, path)
		}
	}
	return events, nil
}

func resolveOpenClawStateDir() string {
	if v := strings.TrimSpace(os.Getenv("OPENCLAW_STATE_DIR")); v != "" {
		return v
	}
	if cfgPath := strings.TrimSpace(os.Getenv("OPENCLAW_CONFIG_PATH")); cfgPath != "" {
		return filepath.Dir(cfgPath)
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".openclaw")
	}
	return ""
}

func parseDiscordSessionFile(path string, minTs int64, cursor sessionProgress) ([]model.Event, sessionProgress, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, cursor, err
	}
	inode := inodeFromFileInfo(info)
	size := info.Size()
	startOffset := int64(0)
	sameFile := cursor.Inode == inode
	if sameFile && cursor.Offset >= 0 && cursor.Offset <= size {
		startOffset = cursor.Offset
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, cursor, err
	}
	defer f.Close()
	if startOffset > 0 {
		if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
			return nil, cursor, err
		}
	}

	events := make([]model.Event, 0, 32)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 32*1024*1024)

	type pendingInbound struct {
		traceID string
		ts      int64
	}
	var pending *pendingInbound
	if sameFile && cursor.PendingTraceID != "" && cursor.PendingTs > 0 {
		pending = &pendingInbound{traceID: cursor.PendingTraceID, ts: cursor.PendingTs}
	}

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		var rec ocSessionLine
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if rec.Type != "message" {
			continue
		}

		ts := rec.Message.Timestamp
		if ts <= 0 {
			ts = parseTimeMs(rec.Timestamp)
		}
		if ts <= minTs {
			continue
		}

		text := extractSessionText(rec.Message.Content)
		if text == "" {
			continue
		}

		switch rec.Message.Role {
		case "user":
			preview := extractDiscordUserPreview(text)
			if preview == "" {
				continue
			}
			traceID := "discord-session-" + extractDiscordSessionMessageID(text, ts)
			events = append(events,
				model.Event{
					TraceID:     traceID,
					MessageType: "text",
					Channel:     "discord",
					Stage:       "inbound_event_received",
					TsUnixMs:    ts,
					Meta: model.EventMeta{
						MessagePreview: preview,
					},
				},
				model.Event{
					TraceID:     traceID,
					MessageType: "text",
					Channel:     "discord",
					Stage:       "processing_start",
					TsUnixMs:    ts,
				},
			)
			pending = &pendingInbound{traceID: traceID, ts: ts}
		case "assistant":
			if pending == nil || ts < pending.ts {
				continue
			}
			events = append(events, model.Event{
				TraceID:     pending.traceID,
				MessageType: "text",
				Channel:     "discord",
				Stage:       "response_sent",
				TsUnixMs:    ts,
				Status:      "ok",
			})
			pending = nil
		}
	}

	if err := sc.Err(); err != nil {
		return nil, cursor, err
	}
	nextOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		nextOffset = size
	}
	next := sessionProgress{Inode: inode, Offset: nextOffset}
	if pending != nil {
		next.PendingTraceID = pending.traceID
		next.PendingTs = pending.ts
	}
	return events, next, nil
}

func extractSessionText(content []struct {
	Type string `json:"type"`
	Text string `json:"text"`
}) string {
	for _, c := range content {
		if c.Type == "text" && c.Text != "" {
			return c.Text
		}
	}
	return ""
}

func extractDiscordUserPreview(raw string) string {
	const marker = "```json\n\n"
	if i := strings.LastIndex(raw, marker); i >= 0 {
		tail := strings.TrimSpace(raw[i+len(marker):])
		if tail != "" {
			return strings.ReplaceAll(tail, ",", ";")
		}
	}
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	if len(lines) == 0 {
		return ""
	}
	return strings.ReplaceAll(strings.TrimSpace(lines[len(lines)-1]), ",", ";")
}

func extractDiscordSessionMessageID(raw string, ts int64) string {
	if m := reDiscordMsgID.FindStringSubmatch(raw); len(m) > 1 {
		return m[1]
	}
	return strconv.FormatInt(ts, 10)
}

// inodeFromFileInfo extracts the inode number from os.FileInfo.
// On Linux this uses the underlying syscall.Stat_t.
func inodeFromFileInfo(info os.FileInfo) uint64 {
	return inodeNum(info)
}
