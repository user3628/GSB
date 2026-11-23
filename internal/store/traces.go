package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"localinfer/workbench/internal/domain"
)

type TraceMeta struct {
	TraceID      string `json:"trace_id"`
	Version      string `json:"version"`
	ContentHash  string `json:"content_hash"`
	CreatedAt    string `json:"created_at"`
	RequestCount int    `json:"request_count"`
}

func (s *Store) SaveTrace(trace *domain.Trace) (bool, error) {
	if trace == nil {
		return false, domain.NewBizError(domain.CodeTraceInvalid, "trace is required", nil)
	}
	var existingHash string
	err := s.db.QueryRow(`SELECT content_hash FROM traces WHERE trace_id=? AND version=?`, trace.TraceID, trace.Version).Scan(&existingHash)
	if err == nil {
		if existingHash == trace.ContentHash {
			return false, nil
		}
		return false, domain.NewBizError(domain.CodeConflicting, "trace_id/version already exists with different content", map[string]any{
			"trace_id": trace.TraceID, "version": trace.Version,
			"existing_content_hash": existingHash, "submitted_content_hash": trace.ContentHash,
		})
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	created := now()
	canonical := trace.CanonicalJSON()
	err = s.withIntent("COMMIT_TRACE", trace.TraceID, "", "", map[string]any{
		"version": trace.Version, "content_hash": trace.ContentHash, "request_count": len(trace.Requests),
	}, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO traces(trace_id,version,content_hash,trace_json,created_at) VALUES(?,?,?,?,?)`,
			trace.TraceID, trace.Version, trace.ContentHash, canonical, created); err != nil {
			return err
		}
		for _, r := range trace.Requests {
			if _, err := tx.Exec(`INSERT INTO trace_requests(trace_id,version,seq,request_id,arrival_ms,model_id,samples,latency_limit_us,priority)
				VALUES(?,?,?,?,?,?,?,?,?)`, trace.TraceID, trace.Version, r.Sequence, r.RequestID, r.ArrivalMS, r.ModelID, r.Samples, r.LatencyLimitUS, r.Priority); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		return true, nil
	}
	// A concurrent submission may have won the unique key while the intent was
	// running. Resolve it as an idempotent duplicate only when the facts match.
	if strings.Contains(strings.ToLower(err.Error()), "unique") {
		if e2 := s.db.QueryRow(`SELECT content_hash FROM traces WHERE trace_id=? AND version=?`, trace.TraceID, trace.Version).Scan(&existingHash); e2 == nil {
			if existingHash == trace.ContentHash {
				return false, nil
			}
			return false, domain.NewBizError(domain.CodeConflicting, "trace_id/version already exists with different content", map[string]any{
				"trace_id": trace.TraceID, "version": trace.Version,
				"existing_content_hash": existingHash, "submitted_content_hash": trace.ContentHash,
			})
		}
	}
	return false, err
}

func (s *Store) GetTrace(traceID, version string) (*domain.Trace, error) {
	var raw []byte
	err := s.db.QueryRow(`SELECT trace_json FROM traces WHERE trace_id=? AND version=?`, traceID, version).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NewBizError(domain.CodeNotFound, "trace not found", map[string]any{"trace_id": traceID, "version": version})
	}
	if err != nil {
		return nil, err
	}
	var trace domain.Trace
	if err := json.Unmarshal(raw, &trace); err != nil {
		return nil, err
	}
	return &trace, nil
}

func (s *Store) ListTraces() ([]TraceMeta, error) {
	rows, err := s.db.Query(`SELECT t.trace_id,t.version,t.content_hash,t.created_at,COUNT(r.request_id)
		FROM traces t LEFT JOIN trace_requests r ON r.trace_id=t.trace_id AND r.version=t.version
		GROUP BY t.trace_id,t.version,t.content_hash,t.created_at ORDER BY t.created_at,t.trace_id,t.version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]TraceMeta, 0)
	for rows.Next() {
		var m TraceMeta
		if err := rows.Scan(&m.TraceID, &m.Version, &m.ContentHash, &m.CreatedAt, &m.RequestCount); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
