package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	LogDir           string
	DBFile           string
	StateFile        string
	TraceWindowMs    int64
	FastTextMs       int64
	SlowTextMs       int64
	FastVoiceMs      int64
	SlowVoiceMs      int64
	DisplayTZ        string
	IngestLookbackBytes int64
}

func Load(envFile string) (*Config, error) {
	// Load env file if it exists, ignore error (env vars may already be set)
	_ = godotenv.Load(envFile)

	logDir := getenv("WARTT_LOG_DIR", "/var/log/wa-latency")

	return &Config{
		LogDir:              logDir,
		DBFile:              getenv("WARTT_DB_FILE", logDir+"/wartt.db"),
		StateFile:           getenv("WARTT_STATE_FILE", logDir+"/.openclaw_ingest.state"),
		TraceWindowMs:       getenvInt("WARTT_TRACE_TTL_MS", 60000),
		FastTextMs:          getenvInt("WARTT_FAST_TEXT_MS", 5000),
		SlowTextMs:          getenvInt("WARTT_SLOW_TEXT_MS", 10000),
		FastVoiceMs:         getenvInt("WARTT_FAST_VOICE_MS", 12000),
		SlowVoiceMs:         getenvInt("WARTT_SLOW_VOICE_MS", 25000),
		DisplayTZ:           getenv("WARTT_DISPLAY_TZ", "Europe/Bucharest"),
		IngestLookbackBytes: getenvInt("WARTT_INGEST_LOOKBACK_BYTES", 1048576),
	}, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
