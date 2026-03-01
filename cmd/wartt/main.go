package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"wartt/internal/config"
	dbpkg "wartt/internal/db"
	"wartt/internal/ingest"
	"wartt/internal/tui"
)

const envFile = "/opt/wartt/config/wartt.env"

func main() {
	cfg, err := config.Load(envFile)
	if err != nil {
		fatalf("config: %v", err)
	}

	if err := os.MkdirAll(cfg.LogDir, 0755); err != nil {
		fatalf("mkdir %s: %v", cfg.LogDir, err)
	}

	sqlDB, err := dbpkg.Open(cfg.DBFile)
	if err != nil {
		fatalf("db: %v", err)
	}
	defer sqlDB.Close()

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "ingest-openclaw":
			runIngest(sqlDB, cfg)
			return
		case "seed-ndjson":
			ndjsonPath := "/var/log/wa-latency/wa_latency_events.ndjson"
			if len(os.Args) >= 3 {
				ndjsonPath = os.Args[2]
			}
			if err := ingest.SeedFromNDJSON(sqlDB, cfg, ndjsonPath); err != nil {
				fatalf("seed: %v", err)
			}
			return
		}
	}

	if err := tui.Run(sqlDB); err != nil {
		fatalf("tui: %v", err)
	}
}

func runIngest(sqlDB *sql.DB, cfg *config.Config) {
	sourceFile := ""
	for i, arg := range os.Args {
		if arg == "--source" && i+1 < len(os.Args) {
			sourceFile = os.Args[i+1]
		}
	}

	var err error
	if sourceFile == "" {
		sourceFile, err = ingest.ResolveDefaultSource()
		if err != nil {
			fatalf("resolve source: %v", err)
		}
	}

	if _, err := os.Stat(sourceFile); err != nil {
		fatalf("source file: %v", err)
	}

	fmt.Fprintf(os.Stderr, "wartt: ingesting from %s\n", filepath.Base(sourceFile))
	if err := ingest.IngestOnce(sqlDB, cfg, sourceFile); err != nil {
		fatalf("ingest: %v", err)
	}
	fmt.Fprintf(os.Stderr, "wartt: ingest done\n")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "wartt: "+format+"\n", args...)
	os.Exit(1)
}
