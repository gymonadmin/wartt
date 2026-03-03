CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY,
    trace_id     TEXT    NOT NULL,
    message_type TEXT    NOT NULL,
    stage        TEXT    NOT NULL,
    ts_unix_ms   INTEGER NOT NULL,
    mono_ns      INTEGER,
    status       TEXT,
    error_code   TEXT,
    meta         TEXT,
    channel      TEXT    DEFAULT 'whatsapp'
);
CREATE INDEX IF NOT EXISTS idx_events_trace ON events(trace_id);
CREATE INDEX IF NOT EXISTS idx_events_ts    ON events(ts_unix_ms);

CREATE TABLE IF NOT EXISTS traces (
    trace_id              TEXT    PRIMARY KEY,
    time_eet              TEXT    NOT NULL,
    ts_unix_ms            INTEGER NOT NULL,
    message_type          TEXT    NOT NULL,
    channel               TEXT    DEFAULT 'whatsapp',
    status                TEXT,
    message_preview       TEXT,

    t1_inbound_gateway_eet    TEXT,
    t2_stt_start_eet          TEXT,
    t3_stt_end_eet            TEXT,
    t4_llm_start_eet          TEXT,
    t5_llm_end_eet            TEXT,
    t6_outbound_send_eet      TEXT,

    total_ms              INTEGER,
    queue_wait_ms         INTEGER,
    upload_ingest_ms      INTEGER,
    queue_before_stt_ms   INTEGER,
    download_audio_ms     INTEGER,
    whisper_total_ms      INTEGER,
    transcribe_ms         INTEGER,
    tool_calls_ms         INTEGER,
    llm_total_ms          INTEGER,
    llm_latency_ms        INTEGER,
    overhead_ms           INTEGER,

    llm_model             TEXT,
    latency_class         TEXT,
    bottleneck_stage      TEXT,
    bottleneck_ms         INTEGER
);
CREATE INDEX IF NOT EXISTS idx_traces_ts    ON traces(ts_unix_ms);
CREATE INDEX IF NOT EXISTS idx_traces_type  ON traces(message_type);
CREATE INDEX IF NOT EXISTS idx_traces_model ON traces(llm_model);
