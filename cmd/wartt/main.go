package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"wartt/internal/config"
	dbpkg "wartt/internal/db"
	"wartt/internal/ingest"
	"wartt/internal/tui"
)

const envFile = "/opt/wartt/config/wartt.env"

type cliOptions struct {
	sourceFile   string
	autoIngest   bool
	command      string
	commandArg   string
	commandKnown bool
}

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

	opts, err := parseCLI(os.Args[1:])
	if err != nil {
		fatalf("args: %v", err)
	}

	if opts.commandKnown {
		switch opts.command {
		case "ingest-openclaw":
			runIngest(sqlDB, cfg, opts.sourceFile)
			return
		case "seed-ndjson":
			ndjsonPath := "/var/log/wa-latency/wa_latency_events.ndjson"
			if opts.commandArg != "" {
				ndjsonPath = opts.commandArg
			}
			if err := ingest.SeedFromNDJSON(sqlDB, cfg, ndjsonPath); err != nil {
				fatalf("seed: %v", err)
			}
			return
		}
	}

	ingester := makeIngester(sqlDB, cfg, opts.sourceFile)
	if err := tui.Run(sqlDB, ingester, opts.autoIngest); err != nil {
		fatalf("tui: %v", err)
	}
}

func runIngest(sqlDB *sql.DB, cfg *config.Config, sourceFile string) {
	sourceFile, err := resolveSource(sourceFile)
	if err != nil {
		fatalf("%v", err)
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

func parseCLI(args []string) (*cliOptions, error) {
	opts := &cliOptions{autoIngest: true}
	if len(args) > 0 {
		switch args[0] {
		case "ingest-openclaw":
			opts.commandKnown = true
			opts.command = "ingest-openclaw"
			args = args[1:]
		case "seed-ndjson":
			opts.commandKnown = true
			opts.command = "seed-ndjson"
			args = args[1:]
			if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
				opts.commandArg = args[0]
				args = args[1:]
			}
		}
	}

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--no-auto-ingest":
			opts.autoIngest = false
		case "--source":
			if i+1 >= len(args) {
				return nil, errors.New("--source requires a value")
			}
			opts.sourceFile = args[i+1]
			i++
		default:
			return nil, fmt.Errorf("unknown argument: %s", args[i])
		}
	}

	return opts, nil
}

func resolveSource(sourceFile string) (string, error) {
	var err error
	if sourceFile == "" {
		sourceFile, err = ingest.ResolveDefaultSource()
		if err != nil {
			return "", fmt.Errorf("resolve source: %w", err)
		}
	}

	if _, err := os.Stat(sourceFile); err != nil {
		return "", fmt.Errorf("source file: %w", err)
	}
	return sourceFile, nil
}

func makeIngester(sqlDB *sql.DB, cfg *config.Config, sourceHint string) func() error {
	return func() error {
		source, err := resolveSource(sourceHint)
		if err != nil {
			return err
		}
		return ingest.IngestOnce(sqlDB, cfg, source)
	}
}
