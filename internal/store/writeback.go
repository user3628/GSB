package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"example.com/ntfsacl/internal/domain"
)

type NodeLock struct {
	Path      string
	Owner     string
	Subtree   string
	Waited    time.Duration
	Contended bool
}

func (s *DB) AcquireNodeLock(ctx context.Context, owner, subtree, nodePath string, timeout time.Duration) (NodeLock, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	var waitStart time.Time
	for {
		conn, err := beginImmediate(ctx, s.db)
		if err != nil {
			return NodeLock{}, err
		}
		now := time.Now()
		_, _ = conn.ExecContext(ctx, `DELETE FROM node_locks WHERE path=? AND acquired_unix_nano < ?`, nodePath, now.Add(-30*time.Second).UnixNano())
		_, err = conn.ExecContext(ctx, `INSERT INTO node_locks(path,owner,subtree,acquired_unix_nano) VALUES(?,?,?,?)`, nodePath, owner, subtree, now.UnixNano())
		if err == nil {
			if err := commitConn(conn); err != nil {
				return NodeLock{}, err
			}
			waited := time.Duration(0)
			if !waitStart.IsZero() {
				waited = time.Since(waitStart)
			}
			return NodeLock{Path: nodePath, Owner: owner, Subtree: subtree, Waited: waited, Contended: !waitStart.IsZero()}, nil
		}
		rollbackConn(conn)
		if !isBusyOrUnique(err) {
			return NodeLock{}, err
		}
		if waitStart.IsZero() {
			waitStart = time.Now()
		}
		if time.Now().After(deadline) {
			return NodeLock{}, fmt.Errorf("lock timeout for %s", nodePath)
		}
		select {
		case <-ctx.Done():
			return NodeLock{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (s *DB) ReleaseNodeLock(ctx context.Context, owner, nodePath string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM node_locks WHERE path=? AND owner=?`, nodePath, owner)
	return err
}

func (s *DB) ReleaseLocksForOwner(ctx context.Context, owner string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM node_locks WHERE owner=?`, owner)
	return err
}

func isBusyOrUnique(err error) bool {
	msg := err.Error()
	return containsAny(msg, []string{"database is locked", "UNIQUE constraint failed", "SQLITE_BUSY", "SQLITE_CONSTRAINT"})
}

func containsAny(s string, needles []string) bool {
	for _, n := range needles {
		if len(n) > 0 && len(s) >= len(n) && (s == n || indexOf(s, n) >= 0) {
			return true
		}
	}
	return false
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

type Session struct {
	ID                    string
	AuditVersion          int64
	PlanHash              string
	Status                string
	Code                  string
	BeforeSnapshotVersion int64
	AfterSnapshotVersion  int64
}

func (s *DB) StartWritebackSession(ctx context.Context, id string, auditVersion int64, planHash string) (Session, error) {
	seq, err := s.nextSeq(ctx, "writeback_sessions", "created_seq")
	if err != nil {
		return Session{}, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO writeback_sessions
		(id,audit_version,plan_hash,status,code,before_snapshot_version,after_snapshot_version,created_seq)
		VALUES(?,?,?,?,?,0,0,?) ON CONFLICT(id) DO NOTHING`, id, auditVersion, planHash, SessionPending, "", seq)
	if err != nil {
		return Session{}, err
	}
	return s.GetSession(ctx, id)
}

func (s *DB) nextSeq(ctx context.Context, table, column string) (int64, error) {
	var n sql.NullInt64
	q := fmt.Sprintf("SELECT MAX(%s) FROM %s", column, table)
	if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n.Int64 + 1, nil
}

func (s *DB) SetSessionStatus(ctx context.Context, id, status, code string, beforeVersion, afterVersion int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE writeback_sessions SET status=?, code=?,
		before_snapshot_version=CASE WHEN ? > 0 THEN ? ELSE before_snapshot_version END,
		after_snapshot_version=CASE WHEN ? > 0 THEN ? ELSE after_snapshot_version END
		WHERE id=?`, status, code, beforeVersion, beforeVersion, afterVersion, afterVersion, id)
	return err
}

func (s *DB) GetSession(ctx context.Context, id string) (Session, error) {
	var out Session
	err := s.db.QueryRowContext(ctx, `SELECT id,audit_version,plan_hash,status,code,before_snapshot_version,after_snapshot_version FROM writeback_sessions WHERE id=?`, id).
		Scan(&out.ID, &out.AuditVersion, &out.PlanHash, &out.Status, &out.Code, &out.BeforeSnapshotVersion, &out.AfterSnapshotVersion)
	return out, err
}

type Revision struct {
	Revision int64
	Payload  []byte
	SHA256   string
}

func (s *DB) BeginNode(ctx context.Context, sessionID, nodePath string, seq int, before, intendedAfter []byte) (Revision, error) {
	conn, err := beginImmediate(ctx, s.db)
	if err != nil {
		return Revision{}, err
	}
	ok := false
	defer func() {
		if !ok {
			rollbackConn(conn)
		}
	}()
	sha := domain.HashBytes(before)
	res, err := conn.ExecContext(ctx, `INSERT INTO acl_revisions(session_id,node_path,phase,payload,payload_sha256) VALUES(?,?,?,?,?)`, sessionID, nodePath, "before", before, sha)
	if err != nil {
		return Revision{}, err
	}
	rev, err := res.LastInsertId()
	if err != nil {
		return Revision{}, err
	}
	afterSHA := domain.HashBytes(intendedAfter)
	_, err = conn.ExecContext(ctx, `INSERT INTO writeback_ops
		(session_id,seq,node_path,phase,before_revision,before_payload,before_sha256,after_payload,after_sha256)
		VALUES(?,?,?,?,?,?,?,?,?)`, sessionID, seq, nodePath, OpPending, rev, before, sha, intendedAfter, afterSHA)
	if err != nil {
		return Revision{}, err
	}
	if _, err := conn.ExecContext(ctx, `UPDATE writeback_sessions SET status=? WHERE id=?`, SessionRunning, sessionID); err != nil {
		return Revision{}, err
	}
	if err := commitConn(conn); err != nil {
		return Revision{}, err
	}
	ok = true
	return Revision{Revision: rev, Payload: append([]byte(nil), before...), SHA256: sha}, nil
}

func (s *DB) CommitNode(ctx context.Context, sessionID, nodePath string, seq int, after []byte) (Revision, error) {
	conn, err := beginImmediate(ctx, s.db)
	if err != nil {
		return Revision{}, err
	}
	ok := false
	defer func() {
		if !ok {
			rollbackConn(conn)
		}
	}()
	sha := domain.HashBytes(after)
	res, err := conn.ExecContext(ctx, `INSERT INTO acl_revisions(session_id,node_path,phase,payload,payload_sha256) VALUES(?,?,?,?,?)`, sessionID, nodePath, "after", after, sha)
	if err != nil {
		return Revision{}, err
	}
	rev, err := res.LastInsertId()
	if err != nil {
		return Revision{}, err
	}
	res, err = conn.ExecContext(ctx, `UPDATE writeback_ops SET phase=?,after_revision=?,after_payload=?,after_sha256=?
		WHERE session_id=? AND seq=? AND node_path=? AND phase=?`,
		OpCommitted, rev, after, sha, sessionID, seq, nodePath, OpPending)
	if err != nil {
		return Revision{}, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return Revision{}, fmt.Errorf("pending writeback op not found for %s", nodePath)
	}
	if err := commitConn(conn); err != nil {
		return Revision{}, err
	}
	ok = true
	return Revision{Revision: rev, Payload: append([]byte(nil), after...), SHA256: sha}, nil
}

func (s *DB) MarkNodeRolledBack(ctx context.Context, sessionID, nodePath string, seq int, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE writeback_ops SET phase=?,error=? WHERE session_id=? AND seq=? AND node_path=?`,
		OpRolledBack, reason, sessionID, seq, nodePath)
	return err
}

type Op struct {
	ID             int64
	SessionID      string
	Seq            int
	NodePath       string
	Phase          string
	BeforeRevision int64
	BeforePayload  []byte
	BeforeSHA256   string
	AfterRevision  int64
	AfterPayload   []byte
	AfterSHA256    string
	Error          string
}

func (s *DB) Ops(ctx context.Context, sessionID string) ([]Op, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,session_id,seq,node_path,phase,before_revision,before_payload,before_sha256,
		after_revision,COALESCE(after_payload,X''),after_sha256,error FROM writeback_ops WHERE session_id=? ORDER BY seq`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Op
	for rows.Next() {
		var op Op
		if err := rows.Scan(&op.ID, &op.SessionID, &op.Seq, &op.NodePath, &op.Phase, &op.BeforeRevision, &op.BeforePayload, &op.BeforeSHA256, &op.AfterRevision, &op.AfterPayload, &op.AfterSHA256, &op.Error); err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

func (s *DB) UnfinishedSessions(ctx context.Context) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,audit_version,plan_hash,status,code,before_snapshot_version,after_snapshot_version
		FROM writeback_sessions WHERE status NOT IN (?,?,?) ORDER BY created_seq`, SessionCompleted, SessionRolledBack, SessionIncomplete)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		var sess Session
		if err := rows.Scan(&sess.ID, &sess.AuditVersion, &sess.PlanHash, &sess.Status, &sess.Code, &sess.BeforeSnapshotVersion, &sess.AfterSnapshotVersion); err != nil {
			return nil, err
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

func (s *DB) ClearStaleLocks(ctx context.Context, olderThan time.Duration) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM node_locks WHERE acquired_unix_nano < ?`, time.Now().Add(-olderThan).UnixNano())
	return err
}

var ErrNotFound = errors.New("not found")
