package db

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// DB holds separate read-write and read-only connections to the SQLite database.
type DB struct {
	rw *sql.DB
	ro *sql.DB
}

// Open opens (or creates) a SQLite database at path, applies the schema, and
// enables WAL mode.
func Open(path string) (*DB, error) {
	rw, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	rw.SetMaxOpenConns(1)

	ro, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		rw.Close()
		return nil, fmt.Errorf("open db read-only: %w", err)
	}
	ro.SetMaxOpenConns(4)

	d := &DB{rw: rw, ro: ro}
	if err := d.init(); err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) init() error {
	if _, err := d.rw.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("enable WAL: %w", err)
	}
	d.recoverReposRename()
	if _, err := d.rw.Exec(schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	d.migrate()
	return nil
}

// recoverReposRename completes an interrupted collation migration.
// Handles two failure states:
//  1. repos missing, repos_new present: crash between DROP and RENAME.
//  2. repos exists (empty), repos_new present: a prior startup ran
//     schemaSQL which recreated an empty repos before recovery could run.
//
// Must run before schemaSQL to handle case 1 on first occurrence.
func (d *DB) recoverReposRename() {
	var newExists bool
	_ = d.rw.QueryRow(
		`SELECT 1 FROM sqlite_master WHERE type='table' AND name='repos_new'`,
	).Scan(&newExists)
	if !newExists {
		return
	}

	var reposExists bool
	_ = d.rw.QueryRow(
		`SELECT 1 FROM sqlite_master WHERE type='table' AND name='repos'`,
	).Scan(&reposExists)

	if !reposExists {
		// Case 1: repos was dropped, repos_new has the data.
		if _, err := d.rw.Exec(`ALTER TABLE repos_new RENAME TO repos`); err != nil {
			slog.Warn("repos collation migration: recover rename", "err", err)
		}
		return
	}

	// Case 2: both exist. repos_new has the real data; repos is empty
	// (recreated by a prior schemaSQL run). Replace repos with repos_new.
	if _, err := d.rw.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		slog.Warn("repos collation recovery: cannot disable FKs", "err", err)
		return
	}
	defer d.rw.Exec(`PRAGMA foreign_keys = ON`) //nolint:errcheck

	stmts := []string{
		`DROP TABLE repos`,
		`ALTER TABLE repos_new RENAME TO repos`,
	}
	for _, s := range stmts {
		if _, err := d.rw.Exec(s); err != nil {
			slog.Warn("repos collation recovery: coexistence fix", "err", err, "stmt", s)
			return
		}
	}
}

// migrate applies idempotent column additions for existing databases.
func (d *DB) migrate() {
	migrations := []string{
		"ALTER TABLE pull_requests ADD COLUMN ci_checks_json TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE repos ADD COLUMN allow_squash_merge INTEGER NOT NULL DEFAULT 1",
		"ALTER TABLE repos ADD COLUMN allow_merge_commit INTEGER NOT NULL DEFAULT 1",
		"ALTER TABLE repos ADD COLUMN allow_rebase_merge INTEGER NOT NULL DEFAULT 1",
		"ALTER TABLE pull_requests ADD COLUMN author_display_name TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE pull_requests ADD COLUMN mergeable_state TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE pull_requests ADD COLUMN github_head_sha TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE pull_requests ADD COLUMN github_base_sha TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE pull_requests ADD COLUMN diff_head_sha TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE pull_requests ADD COLUMN diff_base_sha TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE pull_requests ADD COLUMN merge_base_sha TEXT NOT NULL DEFAULT ''",
	}
	for _, m := range migrations {
		_, _ = d.rw.Exec(m) // Ignore errors — column may already exist
	}
	d.migrateReposCollation()
}

// migrateReposCollation rebuilds the repos table so owner/name use COLLATE
// NOCASE for case-insensitive uniqueness. Idempotent — skips if already done.
func (d *DB) migrateReposCollation() {
	// Check if the owner column already uses NOCASE.
	var tableDDL string
	err := d.ro.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type='table' AND name='repos'`,
	).Scan(&tableDDL)
	if err != nil || strings.Contains(strings.ToUpper(tableDDL), "COLLATE NOCASE") {
		return
	}

	// Disable FK checks for the table rebuild, re-enable after.
	if _, err := d.rw.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		slog.Warn("repos collation migration: cannot disable FKs", "err", err)
		return
	}
	defer d.rw.Exec(`PRAGMA foreign_keys = ON`) //nolint:errcheck

	tx, err := d.rw.Begin()
	if err != nil {
		slog.Warn("repos collation migration: begin tx", "err", err)
		return
	}
	defer tx.Rollback() //nolint:errcheck

	// Remap child rows from duplicate repos to the survivor (lowest id).
	childTables := []string{
		`UPDATE pull_requests SET repo_id = (
			SELECT MIN(r2.id) FROM repos r2
			WHERE LOWER(r2.owner) = (SELECT LOWER(r.owner) FROM repos r WHERE r.id = pull_requests.repo_id)
			  AND LOWER(r2.name) = (SELECT LOWER(r.name) FROM repos r WHERE r.id = pull_requests.repo_id)
		)`,
		`UPDATE issues SET repo_id = (
			SELECT MIN(r2.id) FROM repos r2
			WHERE LOWER(r2.owner) = (SELECT LOWER(r.owner) FROM repos r WHERE r.id = issues.repo_id)
			  AND LOWER(r2.name) = (SELECT LOWER(r.name) FROM repos r WHERE r.id = issues.repo_id)
		)`,
		// Delete starred_items that would conflict after remapping to the
		// survivor repo_id (keep the one from the survivor).
		`DELETE FROM starred_items WHERE rowid NOT IN (
			SELECT MIN(s.rowid) FROM starred_items s
			JOIN repos r ON r.id = s.repo_id
			GROUP BY LOWER(r.owner), LOWER(r.name), s.item_type, s.number
		)`,
		`UPDATE starred_items SET repo_id = (
			SELECT MIN(r2.id) FROM repos r2
			WHERE LOWER(r2.owner) = (SELECT LOWER(r.owner) FROM repos r WHERE r.id = starred_items.repo_id)
			  AND LOWER(r2.name) = (SELECT LOWER(r.name) FROM repos r WHERE r.id = starred_items.repo_id)
		)`,
	}
	for _, s := range childTables {
		if _, err := tx.Exec(s); err != nil {
			slog.Warn("repos collation migration: remap children", "err", err)
			return
		}
	}

	// Delete duplicate repos (keep the lowest id per case-folded name).
	// Drop leftover temp table from any prior failed migration attempt.
	stmts := []string{
		`DROP TABLE IF EXISTS repos_new`,
		`DELETE FROM repos WHERE id NOT IN (
			SELECT MIN(id) FROM repos GROUP BY LOWER(owner), LOWER(name)
		)`,
		`CREATE TABLE repos_new (
			id                     INTEGER PRIMARY KEY AUTOINCREMENT,
			owner                  TEXT NOT NULL COLLATE NOCASE,
			name                   TEXT NOT NULL COLLATE NOCASE,
			last_sync_started_at   DATETIME,
			last_sync_completed_at DATETIME,
			last_sync_error        TEXT DEFAULT '',
			allow_squash_merge     INTEGER NOT NULL DEFAULT 1,
			allow_merge_commit     INTEGER NOT NULL DEFAULT 1,
			allow_rebase_merge     INTEGER NOT NULL DEFAULT 1,
			created_at             DATETIME NOT NULL DEFAULT (datetime('now')),
			UNIQUE(owner, name)
		)`,
		`INSERT INTO repos_new SELECT * FROM repos`,
		`DROP TABLE repos`,
		`ALTER TABLE repos_new RENAME TO repos`,
	}
	for _, s := range stmts {
		if _, err := tx.Exec(s); err != nil {
			slog.Warn("repos collation migration failed", "err", err, "stmt", s)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		slog.Warn("repos collation migration: commit", "err", err)
	}
}

// Close closes both database connections.
func (d *DB) Close() error {
	d.ro.Close()
	return d.rw.Close()
}

// ReadDB returns the read-only connection pool.
func (d *DB) ReadDB() *sql.DB { return d.ro }

// WriteDB returns the read-write connection pool.
func (d *DB) WriteDB() *sql.DB { return d.rw }

// Tx runs fn inside a transaction, rolling back on error.
func (d *DB) Tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := d.rw.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
