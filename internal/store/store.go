package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
	_ "modernc.org/sqlite"
)

type Store struct {
	db      *sql.DB
	writeDB *sql.DB
	path    string

	jobs   chan writeJob
	done   chan struct{}
	closed chan struct{}
	once   sync.Once
}

type writeJob struct {
	ctx  context.Context
	fn   func(context.Context, *sql.Tx) error
	done chan error
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("database path is empty")
	}
	if path != ":memory:" {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		path = abs
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	dsn := sqliteDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open read database: %w", err)
	}
	writeDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		db.Close()
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	if err := writeDB.Ping(); err != nil {
		db.Close()
		writeDB.Close()
		return nil, fmt.Errorf("open write database: %w", err)
	}
	if _, err := writeDB.Exec(schemaSQL); err != nil {
		db.Close()
		writeDB.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	s := &Store{
		db:      db,
		writeDB: writeDB,
		path:    path,
		jobs:    make(chan writeJob, 128),
		done:    make(chan struct{}),
		closed:  make(chan struct{}),
	}
	go s.writerLoop()
	return s, nil
}

func sqliteDSN(path string) string {
	if path == ":memory:" {
		return "file:cdnlab-memory?mode=memory&cache=shared&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	}
	slashPath := filepath.ToSlash(path)
	if !strings.HasPrefix(slashPath, "/") {
		// file:///C:/... is the portable URI form for a Windows drive path.
		slashPath = "/" + slashPath
	}
	u := url.URL{Scheme: "file", Path: slashPath}
	q := u.Query()
	q.Set("_pragma", "foreign_keys(1)")
	u.RawQuery = q.Encode()
	return u.String() + "&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
}

func (s *Store) Path() string    { return s.path }
func (s *Store) Reader() *sql.DB { return s.db }

func (s *Store) writerLoop() {
	defer close(s.done)
	for {
		select {
		case <-s.closed:
			return
		case job := <-s.jobs:
			err := s.runTx(job.ctx, job.fn)
			job.done <- err
		}
	}
}

func (s *Store) runTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Do serializes a mutation through the single writer goroutine.
func (s *Store) Do(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if fn == nil {
		return errors.New("nil write function")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	job := writeJob{ctx: ctx, fn: fn, done: make(chan error, 1)}
	select {
	case s.jobs <- job:
	case <-s.closed:
		return errors.New("store is closed")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-job.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return errors.New("store is closed")
	}
}

// DoTxForTest is intentionally not exposed to application code; package tests
// can use Do with this helper identity when simulating crash windows.
func (s *Store) DoDirect(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.Do(ctx, fn)
}

func (s *Store) Close() error {
	var err error
	s.once.Do(func() {
		close(s.closed)
		<-s.done
		if e := s.writeDB.Close(); e != nil {
			err = e
		}
		if e := s.db.Close(); e != nil && err == nil {
			err = e
		}
	})
	return err
}

func (s *Store) Now() time.Time { return time.Now() }

func (s *Store) AddAudit(ctx context.Context, requestID, code, message string, details map[string]any, nowMs int64) error {
	if details == nil {
		details = map[string]any{}
	}
	raw, _ := json.Marshal(details)
	return s.Do(ctx, func(_ context.Context, tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO audit_events(request_id,code,message,details_json,created_at_ms) VALUES(?,?,?,?,?)`,
			requestID, code, message, string(raw), nowMs)
		return err
	})
}

func (s *Store) Meta(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return value, err == nil, err
}

func (s *Store) SetMetaTx(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(`INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (s *Store) GetMetaTx(tx *sql.Tx, key string) (string, bool, error) {
	var value string
	err := tx.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return value, err == nil, err
}

// Entry is the persisted cache entry model.
type Entry struct {
	ID              int64             `json:"id"`
	NodeID          string            `json:"node_id"`
	CacheKey        string            `json:"cache_key"`
	NormPath        string            `json:"normalized_path"`
	NormQuery       string            `json:"normalized_query"`
	Vary            []cache.VaryValue `json:"vary"`
	ETag            string            `json:"etag"`
	Body            []byte            `json:"-"`
	BodyLength      int64             `json:"body_length"`
	ContentType     string            `json:"content_type"`
	Tags            []string          `json:"tags"`
	State           cache.State       `json:"state"`
	SnapshotVersion int64             `json:"snapshot_version"`
	WrittenAtMs     int64             `json:"written_at_ms"`
	FreshUntilMs    int64             `json:"fresh_until_ms"`
	StaleUntilMs    int64             `json:"stale_until_ms"`
	UpdatedAtMs     int64             `json:"updated_at_ms"`
	LastRequestID   string            `json:"last_request_id"`
	LastIntentID    int64             `json:"last_intent_id"`
}

func (e Entry) VaryJSON() string {
	raw, _ := json.Marshal(e.Vary)
	return string(raw)
}

func (e Entry) TagsJSON() string {
	raw, _ := json.Marshal(e.Tags)
	return string(raw)
}

const entryColumns = `id,node_id,cache_key,norm_path,norm_query,vary_json,etag,body,body_length,content_type,tags_json,state,snapshot_version,written_at_ms,fresh_until_ms,stale_until_ms,updated_at_ms,last_request_id,last_intent_id`

func scanEntry(scan interface {
	Scan(dest ...any) error
}) (Entry, error) {
	var e Entry
	var varyJSON, tagsJSON, state string
	err := scan.Scan(&e.ID, &e.NodeID, &e.CacheKey, &e.NormPath, &e.NormQuery, &varyJSON, &e.ETag, &e.Body, &e.BodyLength, &e.ContentType, &tagsJSON, &state,
		&e.SnapshotVersion, &e.WrittenAtMs, &e.FreshUntilMs, &e.StaleUntilMs, &e.UpdatedAtMs, &e.LastRequestID, &e.LastIntentID)
	if err != nil {
		return Entry{}, err
	}
	_ = json.Unmarshal([]byte(varyJSON), &e.Vary)
	_ = json.Unmarshal([]byte(tagsJSON), &e.Tags)
	e.State = cache.State(state)
	return e, nil
}

func (s *Store) GetEntry(ctx context.Context, nodeID, cacheKey string) (*Entry, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+entryColumns+` FROM cache_entries WHERE node_id=? AND cache_key=?`, nodeID, cacheKey)
	e, err := scanEntry(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func GetEntryTx(ctx context.Context, tx *sql.Tx, nodeID, cacheKey string) (*Entry, error) {
	row := tx.QueryRowContext(ctx, `SELECT `+entryColumns+` FROM cache_entries WHERE node_id=? AND cache_key=?`, nodeID, cacheKey)
	e, err := scanEntry(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func GetEntryByIDTx(ctx context.Context, tx *sql.Tx, id int64) (*Entry, error) {
	row := tx.QueryRowContext(ctx, `SELECT `+entryColumns+` FROM cache_entries WHERE id=?`, id)
	e, err := scanEntry(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

type EntryFilter struct {
	NodeID   string
	NormPath string
	CacheKey string
	State    cache.State
	Tag      string
	Limit    int
}

func (s *Store) ListEntries(ctx context.Context, f EntryFilter) ([]Entry, error) {
	q := `SELECT ` + prefixedColumns("e") + ` FROM cache_entries e`
	args := []any{}
	where := []string{}
	if f.Tag != "" {
		q += ` JOIN entry_tags t ON t.entry_id=e.id AND t.active=1`
		where = append(where, `t.tag=?`)
		args = append(args, f.Tag)
	}
	if f.NodeID != "" {
		where = append(where, `e.node_id=?`)
		args = append(args, f.NodeID)
	}
	if f.NormPath != "" {
		where = append(where, `e.norm_path=?`)
		args = append(args, f.NormPath)
	}
	if f.CacheKey != "" {
		where = append(where, `e.cache_key=?`)
		args = append(args, f.CacheKey)
	}
	if f.State != "" {
		where = append(where, `e.state=?`)
		args = append(args, string(f.State))
	}
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	q += ` ORDER BY e.node_id,e.id`
	if f.Limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, f.Limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func prefixedColumns(alias string) string {
	cols := strings.Split(entryColumns, ",")
	for i := range cols {
		cols[i] = alias + "." + cols[i]
	}
	return strings.Join(cols, ",")
}

func ReplaceIndexesTx(ctx context.Context, tx *sql.Tx, e Entry, active bool) error {
	activeInt := 0
	if active {
		activeInt = 1
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM entry_tags WHERE entry_id=?`, e.ID); err != nil {
		return err
	}
	for _, tag := range e.Tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO entry_tags(entry_id,tag,node_id,active) VALUES(?,?,?,?)`, e.ID, tag, e.NodeID, activeInt); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO route_map(entry_id,node_id,norm_path,cache_key,active) VALUES(?,?,?,?,?)
		ON CONFLICT(entry_id) DO UPDATE SET node_id=excluded.node_id,norm_path=excluded.norm_path,cache_key=excluded.cache_key,active=excluded.active,purge_id=''`,
		e.ID, e.NodeID, e.NormPath, e.CacheKey, activeInt)
	return err
}

func InvalidateIndexesTx(ctx context.Context, tx *sql.Tx, entryID int64, purgeID string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE route_map SET active=0,purge_id=? WHERE entry_id=?`, purgeID, entryID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE entry_tags SET active=0,purge_id=? WHERE entry_id=?`, purgeID, entryID)
	return err
}

func InsertTransitionTx(ctx context.Context, tx *sql.Tx, e Entry, from, to cache.State, requestID string, nowMs int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entry_transitions(entry_id,node_id,cache_key,from_state,to_state,request_id,snapshot_version,at_ms) VALUES(?,?,?,?,?,?,?,?)`,
		e.ID, e.NodeID, e.CacheKey, string(from), string(to), requestID, e.SnapshotVersion, nowMs)
	return err
}

// TransitionTx validates the state machine before executing any UPDATE. This
// guarantees an illegal transition cannot mutate the persisted state.
func TransitionTx(ctx context.Context, tx *sql.Tx, e Entry, to cache.State, requestID string, nowMs int64) error {
	if e.State == to {
		return nil
	}
	if err := cache.ValidateTransition(e.State, to); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE cache_entries SET state=?,updated_at_ms=?,last_request_id=? WHERE id=? AND state=?`,
		string(to), nowMs, requestID, e.ID, string(e.State))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return model.NewBiz(model.CodeInvalidTransition, "concurrent state transition rejected")
	}
	return InsertTransitionTx(ctx, tx, e, e.State, to, requestID, nowMs)
}

func UpsertEntryTx(ctx context.Context, tx *sql.Tx, e Entry) (int64, error) {
	res, err := tx.ExecContext(ctx, `INSERT INTO cache_entries(node_id,cache_key,norm_path,norm_query,vary_json,etag,body,body_length,content_type,tags_json,state,snapshot_version,written_at_ms,fresh_until_ms,stale_until_ms,updated_at_ms,last_request_id,last_intent_id)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(node_id,cache_key) DO UPDATE SET norm_path=excluded.norm_path,norm_query=excluded.norm_query,vary_json=excluded.vary_json,etag=excluded.etag,body=excluded.body,body_length=excluded.body_length,content_type=excluded.content_type,tags_json=excluded.tags_json,state=excluded.state,snapshot_version=excluded.snapshot_version,written_at_ms=excluded.written_at_ms,fresh_until_ms=excluded.fresh_until_ms,stale_until_ms=excluded.stale_until_ms,updated_at_ms=excluded.updated_at_ms,last_request_id=excluded.last_request_id,last_intent_id=excluded.last_intent_id`,
		e.NodeID, e.CacheKey, e.NormPath, e.NormQuery, e.VaryJSON(), e.ETag, e.Body, e.BodyLength, e.ContentType, e.TagsJSON(), string(e.State), e.SnapshotVersion,
		e.WrittenAtMs, e.FreshUntilMs, e.StaleUntilMs, e.UpdatedAtMs, e.LastRequestID, e.LastIntentID)
	if err != nil {
		return 0, err
	}
	if e.ID == 0 {
		id, err := res.LastInsertId()
		if err != nil {
			return 0, err
		}
		e.ID = id
	}
	return e.ID, nil
}

type OriginObject struct {
	CacheKey    string   `json:"cache_key"`
	ETag        string   `json:"etag"`
	Body        []byte   `json:"-"`
	BodyLength  int64    `json:"body_length"`
	ContentType string   `json:"content_type"`
	Tags        []string `json:"tags"`
	VersionSeq  int64    `json:"version_seq"`
	UpdatedAtMs int64    `json:"updated_at_ms"`
	FetchCount  int64    `json:"fetch_count"`
}

func RecordOriginSuccessTx(ctx context.Context, tx *sql.Tx, obj OriginObject) error {
	raw, _ := json.Marshal(obj.Tags)
	_, err := tx.ExecContext(ctx, `INSERT INTO origin_objects(cache_key,etag,body,body_length,content_type,tags_json,version_seq,updated_at_ms,fetch_count)
		VALUES(?,?,?,?,?,?,?,?,1)
		ON CONFLICT(cache_key) DO UPDATE SET
		  version_seq=CASE WHEN origin_objects.etag=excluded.etag THEN origin_objects.version_seq ELSE origin_objects.version_seq+1 END,
		  etag=excluded.etag,body=excluded.body,body_length=excluded.body_length,content_type=excluded.content_type,tags_json=excluded.tags_json,updated_at_ms=excluded.updated_at_ms,fetch_count=origin_objects.fetch_count+1`,
		obj.CacheKey, obj.ETag, obj.Body, obj.BodyLength, obj.ContentType, string(raw), obj.VersionSeq, obj.UpdatedAtMs)
	return err
}

func RecordOriginAttemptTx(ctx context.Context, tx *sql.Tx, requestID, nodeID, key, status, code, etag string, durationMs, nowMs int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO origin_attempts(request_id,node_id,cache_key,status,code,etag,duration_ms,at_ms) VALUES(?,?,?,?,?,?,?,?)`,
		requestID, nodeID, key, status, code, etag, durationMs, nowMs)
	return err
}

type RequestRecord struct {
	RequestID       string            `json:"request_id"`
	Method          string            `json:"method"`
	NodeID          string            `json:"node_id"`
	SnapshotVersion int64             `json:"snapshot_version"`
	Status          string            `json:"status"`
	ResponseStatus  int               `json:"response_status"`
	ResponseBody    []byte            `json:"response_body_base64"`
	ResponseHeaders map[string]string `json:"response_headers"`
	BusinessCode    string            `json:"business_code,omitempty"`
	TraceJSON       string            `json:"trace_json"`
	CreatedAtMs     int64             `json:"created_at_ms"`
	CompletedAtMs   int64             `json:"completed_at_ms"`
}

func (s *Store) GetRequestRecord(ctx context.Context, requestID string) (*RequestRecord, error) {
	var r RequestRecord
	var headersJSON string
	err := s.db.QueryRowContext(ctx, `SELECT request_id,method,node_id,snapshot_version,status,response_status,response_body,response_headers_json,business_code,trace_json,created_at_ms,completed_at_ms FROM request_records WHERE request_id=?`, requestID).
		Scan(&r.RequestID, &r.Method, &r.NodeID, &r.SnapshotVersion, &r.Status, &r.ResponseStatus, &r.ResponseBody, &headersJSON, &r.BusinessCode, &r.TraceJSON, &r.CreatedAtMs, &r.CompletedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.ResponseHeaders = map[string]string{}
	_ = json.Unmarshal([]byte(headersJSON), &r.ResponseHeaders)
	return &r, nil
}

func InsertRequestClaimTx(ctx context.Context, tx *sql.Tx, r RequestRecord) (bool, error) {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO request_records(request_id,method,node_id,snapshot_version,status,created_at_ms) VALUES(?,?,?,?,?,?)`,
		r.RequestID, r.Method, r.NodeID, r.SnapshotVersion, "inflight", r.CreatedAtMs)
	if err != nil {
		return false, err
	}
	// LastInsertId is not meaningful for INSERT OR IGNORE. changes() is
	// per-connection and the writer holds the only write connection.
	var changes int
	if err := tx.QueryRowContext(ctx, `SELECT changes()`).Scan(&changes); err != nil {
		return false, err
	}
	return changes == 1, nil
}

func CompleteRequestTx(ctx context.Context, tx *sql.Tx, r RequestRecord) error {
	headers, _ := json.Marshal(r.ResponseHeaders)
	_, err := tx.ExecContext(ctx, `UPDATE request_records SET status=?,response_status=?,response_body=?,response_headers_json=?,business_code=?,trace_json=?,completed_at_ms=? WHERE request_id=?`,
		r.Status, r.ResponseStatus, r.ResponseBody, string(headers), r.BusinessCode, r.TraceJSON, r.CompletedAtMs, r.RequestID)
	return err
}

func ListTraces(ctx context.Context, db *sql.DB, limit int) ([]RequestRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := db.QueryContext(ctx, `SELECT request_id,method,node_id,snapshot_version,status,response_status,response_body,response_headers_json,business_code,trace_json,created_at_ms,completed_at_ms FROM request_records ORDER BY created_at_ms DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RequestRecord
	for rows.Next() {
		var r RequestRecord
		var headersJSON string
		if err := rows.Scan(&r.RequestID, &r.Method, &r.NodeID, &r.SnapshotVersion, &r.Status, &r.ResponseStatus, &r.ResponseBody, &headersJSON, &r.BusinessCode, &r.TraceJSON, &r.CreatedAtMs, &r.CompletedAtMs); err != nil {
			return nil, err
		}
		r.ResponseHeaders = map[string]string{}
		_ = json.Unmarshal([]byte(headersJSON), &r.ResponseHeaders)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ListTraces(ctx context.Context, limit int) ([]RequestRecord, error) {
	return ListTraces(ctx, s.db, limit)
}

func (s *Store) Metrics(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	queries := map[string]string{
		"cache_entries":   `SELECT COUNT(*) FROM cache_entries`,
		"fresh_entries":   `SELECT COUNT(*) FROM cache_entries WHERE state='FRESH'`,
		"stale_entries":   `SELECT COUNT(*) FROM cache_entries WHERE state='STALE'`,
		"purged_entries":  `SELECT COUNT(*) FROM cache_entries WHERE state='PURGED'`,
		"origin_fetches":  `SELECT COALESCE(SUM(fetch_count),0) FROM origin_objects`,
		"origin_attempts": `SELECT COUNT(*) FROM origin_attempts`,
		"traces":          `SELECT COUNT(*) FROM request_records`,
		"policies":        `SELECT COUNT(*) FROM policy_snapshots`,
	}
	for name, q := range queries {
		var n int64
		if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			return nil, err
		}
		out[name] = n
	}
	return out, nil
}

func (s *Store) TableExists(ctx context.Context, name string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&n)
	return n > 0, err
}
