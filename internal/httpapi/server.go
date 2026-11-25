package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"mvccengine/internal/engine"
	"mvccengine/pkg/mvcc"
)

type Server struct {
	Engine *engine.Engine
}

func NewServer(engine *engine.Engine) *Server {
	return &Server{Engine: engine}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /", s.index)

	mux.HandleFunc("POST /txns/begin", s.beginTxn)
	mux.HandleFunc("POST /txns/commit", s.commitTxn)
	mux.HandleFunc("POST /txns/rollback", s.rollbackTxn)

	mux.HandleFunc("POST /versions", s.writeVersion)
	mux.HandleFunc("POST /rows/versions", s.writeVersion)
	mux.HandleFunc("POST /rows/read", s.readRow)
	mux.HandleFunc("GET /rows/read", s.readRowQuery)
	mux.HandleFunc("GET /rows", s.readRowQuery)

	mux.HandleFunc("POST /snapshots/switch", s.switchSnapshot)
	mux.HandleFunc("GET /snapshots/current", s.currentSnapshot)

	mux.HandleFunc("POST /chains/validate", s.validateChain)
	mux.HandleFunc("GET /chains/validate", s.validateChainQuery)
	mux.HandleFunc("POST /chains/repair", s.repairChain)

	mux.HandleFunc("POST /reclaim", s.reclaim)
	mux.HandleFunc("GET /state", s.state)
	mux.HandleFunc("GET /recovery", s.recovery)
	mux.HandleFunc("POST /recovery/recover", s.recover)
	mux.HandleFunc("POST /samples/import", s.importSample)
	mux.HandleFunc("POST /samples/validate", s.validateSample)

	return recoverMiddleware(jsonMiddleware(mux))
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "mvccengine", "time_ms": s.Engine.State().CurrentSnapshot.GeneratedAtMS})
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeError(w, mvcc.NewError(mvcc.CodeNotFound, "endpoint not found", map[string]any{"path": r.URL.Path}))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true,
		"endpoints": []string{
			"POST /txns/begin", "POST /txns/commit", "POST /txns/rollback",
			"POST /versions", "POST /rows/read", "GET /rows?txn_seq=1&row_key=k",
			"POST /snapshots/switch", "GET /snapshots/current",
			"POST /chains/validate", "POST /chains/repair", "POST /reclaim",
			"GET /state", "GET /recovery", "POST /recovery/recover",
			"POST /samples/import", "POST /samples/validate",
		},
	})
}

func (s *Server) beginTxn(w http.ResponseWriter, r *http.Request) {
	var req engine.BeginTxnRequest
	requestID, ok := decodeBody(w, r, &req)
	if !ok {
		return
	}
	req.RequestID = requestID
	result, err := s.Engine.BeginTxn(req)
	respond(w, result, err)
}

func (s *Server) commitTxn(w http.ResponseWriter, r *http.Request) {
	var req engine.CommitTxnRequest
	id, ok := decodeBody(w, r, &req)
	if !ok {
		return
	}
	req.RequestID = id
	result, err := s.Engine.CommitTxn(req)
	respond(w, result, err)
}

func (s *Server) rollbackTxn(w http.ResponseWriter, r *http.Request) {
	var req engine.RollbackTxnRequest
	id, ok := decodeBody(w, r, &req)
	if !ok {
		return
	}
	req.RequestID = id
	result, err := s.Engine.RollbackTxn(req)
	respond(w, result, err)
}

func (s *Server) writeVersion(w http.ResponseWriter, r *http.Request) {
	var req engine.WriteVersionRequest
	id, ok := decodeBody(w, r, &req)
	if !ok {
		return
	}
	req.RequestID = id
	result, err := s.Engine.WriteVersion(req)
	respond(w, result, err)
}

func (s *Server) readRow(w http.ResponseWriter, r *http.Request) {
	var req engine.ReadRequest
	if _, ok := decodeBody(w, r, &req); !ok {
		return
	}
	result, err := s.Engine.Read(req)
	respond(w, result, err)
}

func (s *Server) readRowQuery(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	var txnSeq, snapshotSeq int64
	_, _ = parseInt64(query.Get("txn_seq"), &txnSeq)
	_, _ = parseInt64(query.Get("snapshot_seq"), &snapshotSeq)
	req := engine.ReadRequest{TxnSeq: txnSeq, SnapshotSeq: snapshotSeq, RowKey: query.Get("row_key")}
	result, err := s.Engine.Read(req)
	respond(w, result, err)
}

func (s *Server) switchSnapshot(w http.ResponseWriter, r *http.Request) {
	var req engine.SwitchSnapshotRequest
	id, ok := decodeBody(w, r, &req)
	if !ok {
		return
	}
	req.RequestID = id
	result, err := s.Engine.SwitchSnapshot(req)
	respond(w, result, err)
}

func (s *Server) currentSnapshot(w http.ResponseWriter, r *http.Request) {
	state := s.Engine.State()
	respond(w, map[string]any{"ok": true, "snapshot": state.CurrentSnapshot}, nil)
}

func (s *Server) validateChain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RowKey string `json:"row_key"`
	}
	if _, ok := decodeBody(w, r, &req); !ok {
		return
	}
	result, err := s.Engine.ValidateChain(req.RowKey)
	if err != nil {
		respond(w, result, err)
		return
	}
	respond(w, result, nil)
}

func (s *Server) validateChainQuery(w http.ResponseWriter, r *http.Request) {
	result, err := s.Engine.ValidateChain(r.URL.Query().Get("row_key"))
	respond(w, result, err)
}

func (s *Server) repairChain(w http.ResponseWriter, r *http.Request) {
	var req engine.RepairChainRequest
	id, ok := decodeBody(w, r, &req)
	if !ok {
		return
	}
	req.RequestID = id
	result, err := s.Engine.RepairChain(req)
	respond(w, result, err)
}

func (s *Server) reclaim(w http.ResponseWriter, r *http.Request) {
	var req engine.ReclaimRequest
	id, ok := decodeBody(w, r, &req)
	if !ok {
		return
	}
	req.RequestID = id
	result, err := s.Engine.Reclaim(req)
	respond(w, result, err)
}

func (s *Server) state(w http.ResponseWriter, r *http.Request) {
	respond(w, s.Engine.State(), nil)
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	report := s.Engine.LastRecoveryReport()
	if report == nil {
		report = &engine.RecoveryReport{FactsOnly: true, PendingIntentsScanned: 0}
	}
	b, err := json.Marshal(report)
	if err != nil {
		writeError(w, err)
		return
	}
	payload := map[string]any{"ok": true, "recovery": report}
	var fields map[string]any
	if err := json.Unmarshal(b, &fields); err == nil {
		for key, value := range fields {
			payload[key] = value
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	report, err := s.Engine.RecoverPending()
	respond(w, report, err)
}

func (s *Server) importSample(w http.ResponseWriter, r *http.Request) {
	var sample mvcc.Sample
	if _, ok := decodeBody(w, r, &sample); !ok {
		return
	}
	result, err := s.Engine.ImportSample(sample)
	respond(w, result, err)
}

func (s *Server) validateSample(w http.ResponseWriter, r *http.Request) {
	var sample mvcc.Sample
	if _, ok := decodeBody(w, r, &sample); !ok {
		return
	}
	if err := sample.Validate(); err != nil {
		respond(w, nil, err)
		return
	}
	respond(w, map[string]any{"ok": true, "valid": true, "name": sample.Name}, nil)
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) (string, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeError(w, mvcc.NewError(mvcc.CodeVersionInvalid, "cannot read request body", map[string]any{"cause": err.Error()}))
		return "", false
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return firstRequestID(r), true
	}
	if err := json.Unmarshal(body, dst); err != nil {
		writeError(w, mvcc.NewError(mvcc.CodeVersionInvalid, "request body is not valid JSON", map[string]any{"cause": err.Error()}))
		return "", false
	}
	var meta struct {
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(body, &meta)
	if meta.RequestID != "" {
		return meta.RequestID, true
	}
	return firstRequestID(r), true
}

func firstRequestID(r *http.Request) string {
	if v := r.Header.Get("X-Request-ID"); v != "" {
		return v
	}
	return r.Header.Get("Idempotency-Key")
}

func respond(w http.ResponseWriter, result any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeSuccess(w, result)
}

func writeSuccess(w http.ResponseWriter, result any) {
	if result == nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	b, err := json.Marshal(result)
	if err != nil {
		writeError(w, mvcc.NewError(mvcc.CodeInternal, "cannot encode response", map[string]any{"cause": err.Error()}))
		return
	}
	var fields map[string]any
	if err := json.Unmarshal(b, &fields); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": result})
		return
	}
	fields["ok"] = true
	writeJSON(w, http.StatusOK, fields)
}

func writeError(w http.ResponseWriter, err error) {
	me := mvcc.AsError(err)
	if me.Context == nil {
		me.Context = map[string]any{}
	}
	status := http.StatusBadRequest
	switch me.Code {
	case mvcc.CodeNotFound:
		status = http.StatusNotFound
	case mvcc.CodeConflictingRequest, mvcc.CodeWriteConflict:
		status = http.StatusConflict
	case mvcc.CodeChainBroken, mvcc.CodeReclaimUnsafe:
		status = http.StatusUnprocessableEntity
	case mvcc.CodeInterrupted, mvcc.CodeInternal:
		status = http.StatusInternalServerError
	}
	payload := map[string]any{
		"ok":      false,
		"code":    me.Code,
		"message": me.Message,
		"error":   me,
	}
	payload["context"] = me.Context
	writeJSON(w, status, payload)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
}

func jsonMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "http://127.0.0.1")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID, Idempotency-Key")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(w, mvcc.NewError(mvcc.CodeInternal, "internal server panic", map[string]any{"panic": recovered}))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func parseInt64(s string, target *int64) (bool, error) {
	if s == "" {
		return false, nil
	}
	var value int64
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return false, errors.New("invalid integer")
		}
		value = value*10 + int64(ch-'0')
	}
	*target = value
	return true, nil
}
