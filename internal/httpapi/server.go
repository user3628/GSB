package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"local/sqlitepool/internal/domain"
	"local/sqlitepool/internal/pool"
	"local/sqlitepool/internal/store"
)

type Options struct {
	AllowDebug bool
	MaxBody    int64
}

type Server struct {
	pool    *pool.Pool
	store   *store.Store
	opts    Options
	idem    *idempotencyCoordinator
	injMu   sync.Mutex
	debugMu sync.Mutex
	mux     *http.ServeMux
}

func New(p *pool.Pool, st *store.Store, opts Options) *Server {
	if opts.MaxBody <= 0 {
		opts.MaxBody = 1 << 20
	}
	s := &Server{
		pool:  p,
		store: st,
		opts:  opts,
		idem:  newIdempotencyCoordinator(),
		mux:   http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /v1/config", s.handleGetConfig)
	s.mux.HandleFunc("PUT /v1/config", s.handlePutConfig)
	s.mux.HandleFunc("POST /v1/acquire", s.handleAcquire)
	s.mux.HandleFunc("POST /v1/release", s.handleRelease)
	s.mux.HandleFunc("POST /v1/query", s.handleQuery)
	s.mux.HandleFunc("GET /v1/pool/stats", s.handleStats)
	s.mux.HandleFunc("GET /v1/pool/connections", s.handleConnections)
	s.mux.HandleFunc("POST /v1/injections", s.handleCreateInjection)
	s.mux.HandleFunc("GET /v1/injections", s.handleListInjections)
	s.mux.HandleFunc("GET /v1/injections/{id}", s.handleGetInjection)
	s.mux.HandleFunc("GET /v1/recovery", s.handleRecovery)
	if s.opts.AllowDebug {
		s.mux.HandleFunc("POST /v1/debug/crash-fixture", s.handleCrashFixture)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeResult(w, successResult(http.StatusOK, "", map[string]any{"status": "ok", "time": time.Now()}))
}

func (s *Server) mutation(w http.ResponseWriter, r *http.Request, route string, fn func(context.Context, string, []byte) apiResult) {
	body, requestID, canonical, err := s.readMutation(r)
	if err != nil {
		writeResult(w, resultFromError(err, requestID))
		return
	}
	result := s.idem.execute(r.Context(), requestID, digestFor(route, canonical), func() apiResult {
		return fn(r.Context(), requestID, body)
	})
	writeResult(w, result)
}

func (s *Server) readMutation(r *http.Request) ([]byte, string, string, error) {
	limited := io.LimitReader(r.Body, s.opts.MaxBody+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, "", "", domain.WrapError(domain.CodeBadRequest, "failed to read request body",
			map[string]any{"max_bytes": s.opts.MaxBody}, err)
	}
	if int64(len(body)) > s.opts.MaxBody {
		return nil, "", "", domain.NewError(domain.CodeBadRequest, "request body exceeds size limit",
			map[string]any{"max_bytes": s.opts.MaxBody, "received_bytes": len(body)})
	}
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}
	var generic map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&generic); err != nil {
		return nil, "", "", domain.WrapError(domain.CodeBadRequest, "request body must be a JSON object", nil, err)
	}
	bodyID, _ := generic["request_id"].(string)
	headerID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
	if headerID == "" {
		headerID = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if headerID != "" && bodyID != "" && headerID != bodyID {
		return nil, bodyID, digestFor(r.Method+" "+r.URL.Path, "conflicting-request-id"), domain.NewError(
			domain.CodeConflictingRequest, "header and body request IDs differ", map[string]any{
				"header_request_id": headerID,
				"body_request_id":   bodyID,
			})
	}
	requestID := headerID
	if requestID == "" {
		requestID = bodyID
	}
	if requestID == "" {
		requestID = "srv-" + randomID()
	}
	delete(generic, "request_id")
	canonicalBytes, err := json.Marshal(generic)
	if err != nil {
		return nil, requestID, "", domain.WrapError(domain.CodeBadRequest, "failed to canonicalize request", nil, err)
	}
	return body, requestID, string(canonicalBytes), nil
}

func decodeInto(body []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	return nil
}

func randomID() string {
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(buf))
}
