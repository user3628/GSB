package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "registrylab/internal/sqlite"
)

// DB owns the SQLite connection pool and convenience transactions.
type DB struct {
	sql *sql.DB
}

// OpenDB opens a local SQLite database with WAL enabled and applies the
// idempotent schema migration.
func OpenDB(path string) (*DB, error) {
	dsn := path
	db, err := sql.Open("registry-sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(12)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA wal_autocheckpoint=1000`); err != nil {
		db.Close()
		return nil, err
	}
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{sql: db}, nil
}

func (d *DB) Close() error { return d.sql.Close() }

func (d *DB) SQL() *sql.DB { return d.sql }

func metaInt(ctx context.Context, q queryer, key string) (int64, error) {
	var raw string
	if err := q.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&raw); err != nil {
		return 0, err
	}
	return strconv.ParseInt(raw, 10, 64)
}

func setMeta(ctx context.Context, tx *sql.Tx, key string, value any) error {
	_, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key=?`, fmt.Sprint(value), key)
	return err
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type currentMeta struct {
	Version         int64
	DerivedRevision int64
}

func readCurrentMeta(ctx context.Context, q queryer) (currentMeta, error) {
	v, err := metaInt(ctx, q, "current_version")
	if err != nil {
		return currentMeta{}, err
	}
	r, err := metaInt(ctx, q, "current_derived_revision")
	if err != nil {
		return currentMeta{}, err
	}
	return currentMeta{Version: v, DerivedRevision: r}, nil
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func writeTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func readTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

func loadRuntimeInstances(ctx context.Context, q queryer) ([]RuntimeInstance, error) {
	rows, err := q.QueryContext(ctx, `SELECT service_name,instance_id,endpoint,weight,lease_ttl_ms,
		probe_kind,probe_path,expected_status,expected_marker,probe_timeout_ms,priority,
		success_threshold,failure_threshold,config_fingerprint,state,consecutive_success,
		consecutive_failure,last_heartbeat_ms,last_probe_at,last_probe_ok,last_probe_code,updated_revision
		FROM instances ORDER BY service_name,instance_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RuntimeInstance
	for rows.Next() {
		var in RuntimeInstance
		var kind string
		var lastOK int64
		if err := rows.Scan(&in.ServiceName, &in.InstanceID, &in.Endpoint, &in.Weight, &in.LeaseTTLMS,
			&kind, &in.Probe.Path, &in.Probe.ExpectedStatus, &in.Probe.ExpectedMarker, &in.Probe.TimeoutMS,
			&in.Priority, &in.SuccessThreshold, &in.FailureThreshold, &in.ConfigFingerprint, &in.State,
			&in.ConsecutiveSuccess, &in.ConsecutiveFailure, &in.LastHeartbeatMS, &in.LastProbeAt, &lastOK,
			&in.LastProbeCode, &in.UpdatedRevision); err != nil {
			return nil, err
		}
		in.Probe.Kind = ProbeKind(kind)
		if lastOK >= 0 {
			ok := lastOK == 1
			in.LastProbeOK = &ok
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

func getRuntimeInstance(ctx context.Context, q queryer, service, id string) (RuntimeInstance, error) {
	all, err := loadRuntimeInstances(ctx, q)
	if err != nil {
		return RuntimeInstance{}, err
	}
	for _, in := range all {
		if in.ServiceName == service && in.InstanceID == id {
			return in, nil
		}
	}
	return RuntimeInstance{}, newError(ErrNotFound, "instance not found", "serviceName", service, "instanceId", id)
}

func upsertRuntimeInstance(ctx context.Context, tx *sql.Tx, in RuntimeInstance) error {
	lastOK := int64(-1)
	if in.LastProbeOK != nil {
		if *in.LastProbeOK {
			lastOK = 1
		} else {
			lastOK = 0
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO instances(
		service_name,instance_id,endpoint,weight,lease_ttl_ms,probe_kind,probe_path,expected_status,
		expected_marker,probe_timeout_ms,priority,success_threshold,failure_threshold,config_fingerprint,
		state,consecutive_success,consecutive_failure,last_heartbeat_ms,last_probe_at,last_probe_ok,
		last_probe_code,updated_revision)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(service_name,instance_id) DO UPDATE SET
		endpoint=excluded.endpoint,weight=excluded.weight,lease_ttl_ms=excluded.lease_ttl_ms,
		probe_kind=excluded.probe_kind,probe_path=excluded.probe_path,
		expected_status=excluded.expected_status,expected_marker=excluded.expected_marker,
		probe_timeout_ms=excluded.probe_timeout_ms,priority=excluded.priority,
		success_threshold=excluded.success_threshold,failure_threshold=excluded.failure_threshold,
		config_fingerprint=excluded.config_fingerprint,state=excluded.state,
		consecutive_success=excluded.consecutive_success,consecutive_failure=excluded.consecutive_failure,
		last_heartbeat_ms=excluded.last_heartbeat_ms,last_probe_at=excluded.last_probe_at,
		last_probe_ok=excluded.last_probe_ok,last_probe_code=excluded.last_probe_code,
		updated_revision=excluded.updated_revision`,
		in.ServiceName, in.InstanceID, in.Endpoint, in.Weight, in.LeaseTTLMS, string(in.Probe.Kind),
		in.Probe.Path, in.Probe.ExpectedStatus, in.Probe.ExpectedMarker, in.Probe.TimeoutMS,
		in.Priority, in.SuccessThreshold, in.FailureThreshold, in.ConfigFingerprint, string(in.State),
		in.ConsecutiveSuccess, in.ConsecutiveFailure, in.LastHeartbeatMS, in.LastProbeAt, lastOK,
		in.LastProbeCode, in.UpdatedRevision)
	return err
}

func upsertLease(ctx context.Context, tx *sql.Tx, in RuntimeInstance, version int64) error {
	expires := int64(0)
	if in.LastHeartbeatMS > 0 {
		expires = in.LastHeartbeatMS + in.LeaseTTLMS
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO leases(service_name,instance_id,last_heartbeat_ms,ttl_ms,expires_at_ms,config_version)
		VALUES(?,?,?,?,?,?) ON CONFLICT(service_name,instance_id) DO UPDATE SET
		last_heartbeat_ms=excluded.last_heartbeat_ms,ttl_ms=excluded.ttl_ms,
		expires_at_ms=excluded.expires_at_ms,config_version=excluded.config_version`,
		in.ServiceName, in.InstanceID, in.LastHeartbeatMS, in.LeaseTTLMS, expires, version)
	return err
}

func deleteLease(ctx context.Context, tx *sql.Tx, service, id string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE service_name=? AND instance_id=?`, service, id)
	return err
}

func insertIntent(ctx context.Context, tx *sql.Tx, id, kind, requestID, payloadHash string, targetVersion, revision int64, roundID string, detail any) error {
	raw := []byte("{}")
	if detail != nil {
		raw, _ = json.Marshal(detail)
	}
	now := nowText()
	_, err := tx.ExecContext(ctx, `INSERT INTO intents(id,kind,request_id,payload_hash,status,target_version,derived_revision,round_id,detail_json,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id, kind, requestID, payloadHash, "RUNNING", targetVersion, revision, roundID, string(raw), now, now)
	return err
}

func completeIntent(ctx context.Context, tx *sql.Tx, id, status string) error {
	_, err := tx.ExecContext(ctx, `UPDATE intents SET status=?,updated_at=? WHERE id=?`, status, nowText(), id)
	return err
}

func putRequestRunning(ctx context.Context, tx *sql.Tx, requestID, operation, payloadHash string) error {
	now := nowText()
	_, err := tx.ExecContext(ctx, `INSERT INTO requests(request_id,operation,payload_hash,status,created_at,updated_at)
		VALUES(?,?,?,?,?,?) ON CONFLICT(request_id) DO UPDATE SET operation=excluded.operation,
		payload_hash=excluded.payload_hash,status='RUNNING',response_json='',http_status=0,
		error_code='',updated_at=excluded.updated_at`, requestID, operation, payloadHash, "RUNNING", now, now)
	return err
}

func finishRequest(ctx context.Context, tx *sql.Tx, requestID, status string, response []byte, httpStatus int, version int64, errCode string) error {
	_, err := tx.ExecContext(ctx, `UPDATE requests SET status=?,response_json=?,http_status=?,snapshot_version=?,error_code=?,updated_at=? WHERE request_id=?`,
		status, string(response), httpStatus, version, errCode, nowText(), requestID)
	return err
}

func getRequest(ctx context.Context, q queryer, requestID string) (operation, hash, status, response, errCode string, err error) {
	err = q.QueryRowContext(ctx, `SELECT operation,payload_hash,status,response_json,error_code FROM requests WHERE request_id=?`, requestID).
		Scan(&operation, &hash, &status, &response, &errCode)
	return
}

func snapshotMeta(ctx context.Context, q queryer, version int64) (string, string, int, error) {
	var generatedAt, digest string
	var count int
	err := q.QueryRowContext(ctx, `SELECT generated_at,digest,instance_count FROM snapshots WHERE version=? AND status='COMPLETE'`, version).
		Scan(&generatedAt, &digest, &count)
	return generatedAt, digest, count, err
}

func normalizeSQLErr(err error) error {
	if err == nil {
		return nil
	}
	if err == sql.ErrNoRows {
		return newError(ErrNotFound, "record not found")
	}
	return newError(ErrInternal, "database operation failed", "detail", err.Error())
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}
