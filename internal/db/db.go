// Package db manages the SQLite analytics database.
// It runs in WAL mode so background writes do not block the hot read path.
package db

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const schema = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS api_logs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    token                 TEXT NOT NULL,
    appliance_id          TEXT NOT NULL,
    priority_class        TEXT NOT NULL,
    queued_at             TIMESTAMP NOT NULL,
    acquired_at           TIMESTAMP NOT NULL,
    api_sent_at           TIMESTAMP,
    api_recv_at           TIMESTAMP,
    ns_remaining          INTEGER,
    ns_reset              INTEGER,
    status_code           INTEGER,
    estimated_latency_ms  REAL,
    expected_remaining    INTEGER,
    implied_legacy_usage  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_logs_recv  ON api_logs(api_recv_at);
CREATE INDEX IF NOT EXISTS idx_logs_token ON api_logs(token);

CREATE TABLE IF NOT EXISTS window_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    window_start   TIMESTAMP NOT NULL,
    window_end     TIMESTAMP NOT NULL,
    detected_via   TEXT NOT NULL,
    trigger_token  TEXT,
    logged_at      TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS anomaly_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type   TEXT NOT NULL,
    appliance_id TEXT,
    token        TEXT,
    detail       TEXT,
    logged_at    TIMESTAMP NOT NULL
);
`

// DB wraps *sql.DB with prepared statements for hot-path inserts.
type DB struct {
	db           *sql.DB
	insertLog    *sql.Stmt
	insertWindow *sql.Stmt
	insertAnomaly *sql.Stmt
}

// Open opens (or creates) the SQLite database at the given path and runs migrations.
func Open(path string) (*DB, error) {
	raw, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=rwc&_journal_mode=WAL", path))
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	// Single writer to avoid SQLITE_BUSY contention; reads can still be concurrent.
	raw.SetMaxOpenConns(1)

	if _, err := raw.Exec(schema); err != nil {
		return nil, fmt.Errorf("schema migration: %w", err)
	}

	insertLog, err := raw.Prepare(`
		INSERT INTO api_logs (
			token, appliance_id, priority_class,
			queued_at, acquired_at, api_sent_at, api_recv_at,
			ns_remaining, ns_reset, status_code,
			estimated_latency_ms, expected_remaining, implied_legacy_usage
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare insertLog: %w", err)
	}

	insertWindow, err := raw.Prepare(`
		INSERT INTO window_log (window_start, window_end, detected_via, trigger_token, logged_at)
		VALUES (?,?,?,?,?)
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare insertWindow: %w", err)
	}

	insertAnomaly, err := raw.Prepare(`
		INSERT INTO anomaly_log (event_type, appliance_id, token, detail, logged_at)
		VALUES (?,?,?,?,?)
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare insertAnomaly: %w", err)
	}

	log.Printf("[db] opened %s", path)
	return &DB{db: raw, insertLog: insertLog, insertWindow: insertWindow, insertAnomaly: insertAnomaly}, nil
}

// Close closes all prepared statements and the underlying DB.
func (d *DB) Close() {
	d.insertLog.Close()
	d.insertWindow.Close()
	d.insertAnomaly.Close()
	d.db.Close()
}

// LogEntry is the data written for each completed API request.
type LogEntry struct {
	Token               string
	ApplianceID         string
	PriorityClass       string
	QueuedAt            time.Time
	AcquiredAt          time.Time
	APISentAt           time.Time
	APIRecvAt           time.Time
	NSRemaining         int
	NSReset             int
	StatusCode          int
	EstimatedLatencyMs  float64
	ExpectedRemaining   int
	ImpliedLegacyUsage  int
}

// WriteLog persists a completed request record. It is called from a background goroutine.
func (d *DB) WriteLog(e LogEntry) {
	_, err := d.insertLog.Exec(
		e.Token, e.ApplianceID, e.PriorityClass,
		e.QueuedAt.UTC(), e.AcquiredAt.UTC(),
		nullableTime(e.APISentAt), nullableTime(e.APIRecvAt),
		e.NSRemaining, e.NSReset, e.StatusCode,
		e.EstimatedLatencyMs, e.ExpectedRemaining, e.ImpliedLegacyUsage,
	)
	if err != nil {
		log.Printf("[db] WriteLog error: %v", err)
	}
}

// WindowEntry describes a detected rate-limit window boundary.
type WindowEntry struct {
	WindowStart  time.Time
	WindowEnd    time.Time
	DetectedVia  string
	TriggerToken string
}

// WriteWindow persists a window detection event.
func (d *DB) WriteWindow(e WindowEntry) {
	_, err := d.insertWindow.Exec(
		e.WindowStart.UTC(), e.WindowEnd.UTC(),
		e.DetectedVia, e.TriggerToken,
		time.Now().UTC(),
	)
	if err != nil {
		log.Printf("[db] WriteWindow error: %v", err)
	}
}

// WriteAnomaly records an unexpected event (expired ticket, unmatched token, etc.).
func (d *DB) WriteAnomaly(eventType, applianceID, token, detail string) {
	_, err := d.insertAnomaly.Exec(eventType, applianceID, token, detail, time.Now().UTC())
	if err != nil {
		log.Printf("[db] WriteAnomaly error: %v", err)
	}
}

// nullableTime returns nil for zero time values so SQLite stores NULL.
func nullableTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

// ---- Dashboard query types ----

// RecentLog is a row returned by the dashboard's recent-requests query.
type RecentLog struct {
	ID                  int64
	Token               string
	ApplianceID         string
	PriorityClass       string
	QueuedAt            string
	AcquiredAt          string
	APISentAt           string
	APIRecvAt           string
	NSRemaining         *int
	NSReset             *int
	StatusCode          *int
	EstimatedLatencyMs  *float64
	ExpectedRemaining   *int
	ImpliedLegacyUsage  *int
}

// QueryRecentLogs returns the n most recent rows from api_logs.
func (d *DB) QueryRecentLogs(n int) ([]RecentLog, error) {
	rows, err := d.db.Query(`
		SELECT id, token, appliance_id, priority_class,
		       queued_at, acquired_at,
		       COALESCE(api_sent_at,''), COALESCE(api_recv_at,''),
		       ns_remaining, ns_reset, status_code,
		       estimated_latency_ms, expected_remaining, implied_legacy_usage
		FROM api_logs
		ORDER BY id DESC
		LIMIT ?`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RecentLog
	for rows.Next() {
		var r RecentLog
		if err := rows.Scan(
			&r.ID, &r.Token, &r.ApplianceID, &r.PriorityClass,
			&r.QueuedAt, &r.AcquiredAt, &r.APISentAt, &r.APIRecvAt,
			&r.NSRemaining, &r.NSReset, &r.StatusCode,
			&r.EstimatedLatencyMs, &r.ExpectedRemaining, &r.ImpliedLegacyUsage,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// WindowRow is a row from window_log.
type WindowRow struct {
	ID           int64
	WindowStart  string
	WindowEnd    string
	DetectedVia  string
	TriggerToken string
	LoggedAt     string
}

// QueryRecentWindows returns the n most recent window detections.
func (d *DB) QueryRecentWindows(n int) ([]WindowRow, error) {
	rows, err := d.db.Query(`
		SELECT id, window_start, window_end, detected_via,
		       COALESCE(trigger_token,''), logged_at
		FROM window_log
		ORDER BY id DESC
		LIMIT ?`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []WindowRow
	for rows.Next() {
		var r WindowRow
		if err := rows.Scan(&r.ID, &r.WindowStart, &r.WindowEnd,
			&r.DetectedVia, &r.TriggerToken, &r.LoggedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AnomalyRow is a row from anomaly_log.
type AnomalyRow struct {
	ID          int64
	EventType   string
	ApplianceID string
	Token       string
	Detail      string
	LoggedAt    string
}

// QueryRecentAnomalies returns the n most recent anomaly records.
func (d *DB) QueryRecentAnomalies(n int) ([]AnomalyRow, error) {
	rows, err := d.db.Query(`
		SELECT id, event_type, COALESCE(appliance_id,''), COALESCE(token,''),
		       COALESCE(detail,''), logged_at
		FROM anomaly_log
		ORDER BY id DESC
		LIMIT ?`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AnomalyRow
	for rows.Next() {
		var r AnomalyRow
		if err := rows.Scan(&r.ID, &r.EventType, &r.ApplianceID,
			&r.Token, &r.Detail, &r.LoggedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Stats aggregates headline numbers for the dashboard.
type Stats struct {
	TotalRequests    int64
	TotalLegacyUsage int64
	Total429s        int64
	Total4xx         int64
	Total5xx         int64
	AvgLatencyMs     float64
}

// QueryStats computes all-time headline stats.
func (d *DB) QueryStats() (Stats, error) {
	var s Stats
	row := d.db.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(implied_legacy_usage),0),
			COALESCE(SUM(CASE WHEN status_code=429   THEN 1 ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN status_code>=400 AND status_code<500 AND status_code!=429 THEN 1 ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN status_code>=500 AND status_code<600 THEN 1 ELSE 0 END),0),
			COALESCE(AVG(estimated_latency_ms),0)
		FROM api_logs`)
	if err := row.Scan(&s.TotalRequests, &s.TotalLegacyUsage, &s.Total429s, &s.Total4xx, &s.Total5xx, &s.AvgLatencyMs); err != nil {
		return s, err
	}
	return s, nil
}

// PeriodStats are stats scoped to a time window.
type PeriodStats struct {
	TotalRequests    int64
	Total429s        int64
	Total4xx         int64
	Total5xx         int64
	TotalLegacyUsage int64
	AvgLatencyMs     float64
}

// QueryStatsSince computes stats for entries at or after since.
// If appliance is non-empty, results are filtered to that appliance only.
func (d *DB) QueryStatsSince(since time.Time, appliance string) (PeriodStats, error) {
	var s PeriodStats
	query := `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN status_code=429   THEN 1 ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN status_code>=400 AND status_code<500 AND status_code!=429 THEN 1 ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN status_code>=500 AND status_code<600 THEN 1 ELSE 0 END),0),
			COALESCE(SUM(implied_legacy_usage),0),
			COALESCE(AVG(estimated_latency_ms),0)
		FROM api_logs WHERE api_recv_at >= ?`
	args := []interface{}{since.UTC()}
	if appliance != "" {
		query += " AND appliance_id = ?"
		args = append(args, appliance)
	}
	row := d.db.QueryRow(query, args...)
	if err := row.Scan(&s.TotalRequests, &s.Total429s, &s.Total4xx, &s.Total5xx, &s.TotalLegacyUsage, &s.AvgLatencyMs); err != nil {
		return s, err
	}
	return s, nil
}

// QueryDistinctAppliances returns all distinct appliance IDs from api_logs.
func (d *DB) QueryDistinctAppliances() ([]string, error) {
	rows, err := d.db.Query(`SELECT DISTINCT appliance_id FROM api_logs ORDER BY appliance_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
