package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	d, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return d
}

func TestOpenAndSchema(t *testing.T) {
	d := openTestDB(t)
	tables := []string{"repos", "pull_requests", "pr_events", "kanban_state"}
	for _, tbl := range tables {
		var name string
		err := d.ReadDB().QueryRow(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?", tbl,
		).Scan(&name)
		require.NoErrorf(t, err, "table %s should exist", tbl)
	}
}

func TestOpenCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "new.db")
	d, err := Open(path)
	require.NoError(t, err)
	d.Close()
	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestOpenIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	d1, err := Open(path)
	require.NoError(t, err)
	d1.Close()
	d2, err := Open(path)
	require.NoError(t, err)
	d2.Close()
}

func TestMigrateReposCollation(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create a database with the real schema but patch repos to use the OLD
	// collation (no COLLATE NOCASE) so the migration triggers on reopen.
	raw, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(0)")
	require.NoError(err)

	// Apply the current schema, then rebuild repos without NOCASE.
	_, err = raw.Exec(schemaSQL)
	require.NoError(err)
	for _, s := range []string{
		`CREATE TABLE repos_old (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			owner TEXT NOT NULL,
			name TEXT NOT NULL,
			last_sync_started_at DATETIME,
			last_sync_completed_at DATETIME,
			last_sync_error TEXT DEFAULT '',
			allow_squash_merge INTEGER NOT NULL DEFAULT 1,
			allow_merge_commit INTEGER NOT NULL DEFAULT 1,
			allow_rebase_merge INTEGER NOT NULL DEFAULT 1,
			created_at DATETIME NOT NULL DEFAULT (datetime('now')),
			UNIQUE(owner, name)
		)`,
		`INSERT INTO repos_old SELECT * FROM repos`,
		`DROP TABLE repos`,
		`ALTER TABLE repos_old RENAME TO repos`,
	} {
		_, err = raw.Exec(s)
		require.NoError(err)
	}

	// Insert case-conflicting repos (old schema allows this).
	_, err = raw.Exec(`INSERT INTO repos (id, owner, name) VALUES (1, 'Acme', 'Widget'), (2, 'acme', 'widget')`)
	require.NoError(err)

	// Add child rows: PR on repo 1, issue on repo 2, stars on both (conflicting).
	_, err = raw.Exec(`INSERT INTO pull_requests (repo_id, github_id, number, created_at, updated_at, last_activity_at)
		VALUES (1, 100, 1, datetime('now'), datetime('now'), datetime('now'))`)
	require.NoError(err)
	_, err = raw.Exec(`INSERT INTO issues (repo_id, github_id, number, created_at, updated_at, last_activity_at)
		VALUES (2, 200, 5, datetime('now'), datetime('now'), datetime('now'))`)
	require.NoError(err)
	_, err = raw.Exec(`INSERT INTO starred_items (item_type, repo_id, number) VALUES ('pr', 1, 1), ('pr', 2, 1)`)
	require.NoError(err)
	raw.Close()

	// Reopen through our normal Open path — this triggers the migration.
	d, err := Open(path)
	require.NoError(err)
	defer d.Close()

	// Verify: only one repo remains with the survivor's casing.
	var count int
	require.NoError(d.ro.QueryRow(`SELECT COUNT(*) FROM repos`).Scan(&count))
	require.Equal(1, count)

	// All child rows point to the surviving repo.
	var prRepoID, issueRepoID int64
	require.NoError(d.ro.QueryRow(`SELECT repo_id FROM pull_requests WHERE github_id = 100`).Scan(&prRepoID))
	require.NoError(d.ro.QueryRow(`SELECT repo_id FROM issues WHERE github_id = 200`).Scan(&issueRepoID))
	require.Equal(prRepoID, issueRepoID)

	// Only one star survives (the duplicate was deduplicated).
	require.NoError(d.ro.QueryRow(`SELECT COUNT(*) FROM starred_items`).Scan(&count))
	require.Equal(1, count)

	// The table now uses COLLATE NOCASE.
	var ddl string
	require.NoError(d.ro.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type='table' AND name='repos'`,
	).Scan(&ddl))
	require.Contains(ddl, "COLLATE NOCASE")
}

func TestMigrateReposCollation_InterruptedRename(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Simulate a crash between DROP TABLE repos and RENAME repos_new.
	raw, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	require.NoError(err)

	// Create repos_new (the post-migration table) with data, but no repos table.
	_, err = raw.Exec(`CREATE TABLE repos_new (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		owner TEXT NOT NULL COLLATE NOCASE,
		name TEXT NOT NULL COLLATE NOCASE,
		last_sync_started_at DATETIME,
		last_sync_completed_at DATETIME,
		last_sync_error TEXT DEFAULT '',
		allow_squash_merge INTEGER NOT NULL DEFAULT 1,
		allow_merge_commit INTEGER NOT NULL DEFAULT 1,
		allow_rebase_merge INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		UNIQUE(owner, name)
	)`)
	require.NoError(err)
	_, err = raw.Exec(`INSERT INTO repos_new (owner, name) VALUES ('Acme', 'Widget')`)
	require.NoError(err)
	raw.Close()

	// Reopen through Open — recovery should rename repos_new to repos.
	d, err := Open(path)
	require.NoError(err)
	defer d.Close()

	var count int
	require.NoError(d.ro.QueryRow(`SELECT COUNT(*) FROM repos`).Scan(&count))
	require.Equal(1, count)

	var owner string
	require.NoError(d.ro.QueryRow(`SELECT owner FROM repos WHERE id = 1`).Scan(&owner))
	require.Equal("Acme", owner)
}

func TestMigrateReposCollation_CoexistenceRecovery(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Simulate: prior startup created empty repos (with NOCASE) but repos_new
	// has the real data from a failed migration.
	raw, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	require.NoError(err)
	_, err = raw.Exec(`CREATE TABLE repos (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		owner TEXT NOT NULL COLLATE NOCASE,
		name TEXT NOT NULL COLLATE NOCASE,
		last_sync_started_at DATETIME,
		last_sync_completed_at DATETIME,
		last_sync_error TEXT DEFAULT '',
		allow_squash_merge INTEGER NOT NULL DEFAULT 1,
		allow_merge_commit INTEGER NOT NULL DEFAULT 1,
		allow_rebase_merge INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		UNIQUE(owner, name)
	)`)
	require.NoError(err)
	_, err = raw.Exec(`CREATE TABLE repos_new (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		owner TEXT NOT NULL COLLATE NOCASE,
		name TEXT NOT NULL COLLATE NOCASE,
		last_sync_started_at DATETIME,
		last_sync_completed_at DATETIME,
		last_sync_error TEXT DEFAULT '',
		allow_squash_merge INTEGER NOT NULL DEFAULT 1,
		allow_merge_commit INTEGER NOT NULL DEFAULT 1,
		allow_rebase_merge INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		UNIQUE(owner, name)
	)`)
	require.NoError(err)
	_, err = raw.Exec(`INSERT INTO repos_new (owner, name) VALUES ('Acme', 'Widget')`)
	require.NoError(err)
	raw.Close()

	// Reopen — recovery should replace empty repos with populated repos_new.
	d, err := Open(path)
	require.NoError(err)
	defer d.Close()

	var count int
	require.NoError(d.ro.QueryRow(`SELECT COUNT(*) FROM repos`).Scan(&count))
	require.Equal(1, count)

	var owner string
	require.NoError(d.ro.QueryRow(`SELECT owner FROM repos`).Scan(&owner))
	require.Equal("Acme", owner)

	// repos_new should be gone.
	var leftover bool
	_ = d.ro.QueryRow(
		`SELECT 1 FROM sqlite_master WHERE type='table' AND name='repos_new'`,
	).Scan(&leftover)
	require.False(leftover)
}

func TestMigrateReposCollation_RepopulatedRepos(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Simulate: prior startup created empty repos, sync repopulated it,
	// and repos_new still lingers from the failed migration.
	raw, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	require.NoError(err)
	_, err = raw.Exec(`CREATE TABLE repos (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		owner TEXT NOT NULL COLLATE NOCASE,
		name TEXT NOT NULL COLLATE NOCASE,
		last_sync_started_at DATETIME,
		last_sync_completed_at DATETIME,
		last_sync_error TEXT DEFAULT '',
		allow_squash_merge INTEGER NOT NULL DEFAULT 1,
		allow_merge_commit INTEGER NOT NULL DEFAULT 1,
		allow_rebase_merge INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT (datetime('now')),
		UNIQUE(owner, name)
	)`)
	require.NoError(err)
	// repos has been repopulated by normal sync.
	_, err = raw.Exec(`INSERT INTO repos (owner, name) VALUES ('Acme', 'Widget')`)
	require.NoError(err)
	// Stale repos_new from the old failed migration.
	_, err = raw.Exec(`CREATE TABLE repos_new (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		owner TEXT NOT NULL COLLATE NOCASE,
		name TEXT NOT NULL COLLATE NOCASE,
		UNIQUE(owner, name)
	)`)
	require.NoError(err)
	_, err = raw.Exec(`INSERT INTO repos_new (owner, name) VALUES ('OldOwner', 'OldName')`)
	require.NoError(err)
	raw.Close()

	// Reopen — should keep the live repos data, drop stale repos_new.
	d, err := Open(path)
	require.NoError(err)
	defer d.Close()

	var count int
	require.NoError(d.ro.QueryRow(`SELECT COUNT(*) FROM repos`).Scan(&count))
	require.Equal(1, count)

	var owner string
	require.NoError(d.ro.QueryRow(`SELECT owner FROM repos`).Scan(&owner))
	require.Equal("Acme", owner)

	// repos_new should be cleaned up.
	var leftover bool
	_ = d.ro.QueryRow(
		`SELECT 1 FROM sqlite_master WHERE type='table' AND name='repos_new'`,
	).Scan(&leftover)
	require.False(leftover)
}

func TestMigrateMergeableState(t *testing.T) {
	d := openTestDB(t)
	var val string
	err := d.ReadDB().QueryRow(
		"SELECT mergeable_state FROM pull_requests LIMIT 0",
	).Scan(&val)
	require.ErrorIs(t, err, sql.ErrNoRows)
}
