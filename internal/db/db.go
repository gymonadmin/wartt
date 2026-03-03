package db

import (
	"database/sql"
	_ "embed"
	"fmt"

	_ "modernc.org/sqlite"
)

//go:embed migrations.sql
var migrationSQL string

// Open opens (or creates) the SQLite database and runs migrations.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite is single-writer
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func migrate(db *sql.DB) error {
	// migrateColumns first: add missing columns to existing tables before
	// running migrations.sql (which may reference those columns in indexes).
	if err := migrateColumns(db); err != nil {
		return err
	}
	_, err := db.Exec(migrationSQL)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	// Create channel index now that the column is guaranteed to exist.
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_traces_channel ON traces(channel)`)
	if err != nil {
		return fmt.Errorf("create channel index: %w", err)
	}
	return nil
}

// migrateColumns adds columns to existing tables that predate the migrations.sql change.
// Safe to call on new (empty) databases — errors from non-existent tables are ignored.
func migrateColumns(db *sql.DB) error {
	for _, table := range []string{"events", "traces"} {
		has, err := hasColumn(db, table, "channel")
		if err != nil {
			// Table may not exist yet on a fresh database; skip.
			continue
		}
		if !has {
			_, err = db.Exec(fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN channel TEXT DEFAULT 'whatsapp'", table,
			))
			if err != nil {
				return fmt.Errorf("alter %s add channel: %w", table, err)
			}
		}
	}
	return nil
}

func hasColumn(db *sql.DB, table, col string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == col {
			return true, nil
		}
	}
	return false, rows.Err()
}
