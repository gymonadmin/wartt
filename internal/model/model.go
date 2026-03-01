package model

// Event is a single stage event parsed from an OpenClaw log line.
type Event struct {
	TraceID     string
	MessageType string // text | voice
	Stage       string
	TsUnixMs    int64
	Status      string
	Meta        EventMeta
}

// EventMeta holds optional fields stored in the meta JSON blob.
type EventMeta struct {
	LLMProvider  string
	LLMModel     string
	ToolName     string
	ClientSendMs int64
	MessagePreview string
}

// Trace is a fully aggregated per-trace record.
type Trace struct {
	TraceID         string
	TimeEET         string
	TsUnixMs        int64
	MessageType     string // text | voice
	Status          string // ok | error | timeout | partial
	MessagePreview  string

	// Absolute stage timestamps (EET)
	T1InboundGatewayEET string
	T2SttStartEET       string
	T3SttEndEET         string
	T4LlmStartEET       string
	T5LlmEndEET         string
	T6OutboundSendEET   string

	// Stage durations (ms), 0 = not measured
	TotalMs          int64
	QueueWaitMs      int64
	UploadIngestMs   int64
	QueueBeforeSttMs int64
	DownloadAudioMs  int64
	WhisperTotalMs   int64
	TranscribeMs     int64
	ToolCallsMs      int64
	LLMTotalMs       int64
	LLMLatencyMs     int64
	OverheadMs       int64

	// Classification
	LLMModel        string
	LatencyClass    string // quick | moderate | delayed | error | timeout
	BottleneckStage string
	BottleneckMs    int64
}
