package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"blockpool"
)

const maxRequestBody = 64 << 20

type Server struct {
	pool *blockpool.Pool
	mux  *http.ServeMux
}

func New(pool *blockpool.Pool) *Server {
	s := &Server{pool: pool, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Mux exposes the local handler tree so a demo program can add deterministic
// race/scenario endpoints without bypassing the standard API behavior.
func (s *Server) Mux() *http.ServeMux { return s.mux }

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(w, blockpool.Errorf(blockpool.CodeInternal, "panic while handling request",
					"cause", fmt.Sprint(recovered)))
			}
		}()
		s.mux.ServeHTTP(w, r)
	})
}

func (s *Server) routes() {
	s.mux.HandleFunc("/v1/health", s.health)
	s.mux.HandleFunc("/v1/namespaces", s.namespaces)
	s.mux.HandleFunc("/v1/nodes", s.nodes)
	s.mux.HandleFunc("/v1/nodes/replace", s.replace)
	s.mux.HandleFunc("/v1/blocks", s.blocks)
	s.mux.HandleFunc("/v1/blocks/", s.blockPath)
	s.mux.HandleFunc("/v1/ingest", s.blocks)
	s.mux.HandleFunc("/v1/mount", s.mount)
	s.mux.HandleFunc("/v1/renew", s.renew)
	s.mux.HandleFunc("/v1/unmount", s.unmount)
	s.mux.HandleFunc("/v1/reclaim", s.reclaim)
	s.mux.HandleFunc("/v1/recovery", s.recovery)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, blockpool.Errorf(blockpool.CodeNotFound, "route not found",
			"method", r.Method, "path", r.URL.Path))
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time_ms": time.Now().UnixMilli()})
}

func (s *Server) namespaces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, r)
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if !decode(w, r, &body) {
		return
	}
	result, err := s.pool.CreateNamespace(r.Context(), body.Name)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) nodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, r)
		return
	}
	var body blockpool.NodeRef
	if !decode(w, r, &body) {
		return
	}
	result, err := s.pool.CreateNode(r.Context(), body)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

type ingestDTO struct {
	RequestID       string `json:"request_id,omitempty"`
	Digest          string `json:"digest"`
	DataBase64      string `json:"data_base64"`
	DataBase64Camel string `json:"dataBase64,omitempty"`
	SizeBytes       *int64 `json:"size_bytes,omitempty"`
	RetentionMS     int64  `json:"retention_ms,omitempty"`
}

func (s *Server) blocks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var body ingestDTO
		if !decode(w, r, &body) {
			return
		}
		encoded := body.DataBase64
		if encoded == "" {
			encoded = body.DataBase64Camel
		}
		content, decodeErr := base64.StdEncoding.DecodeString(encoded)
		if decodeErr != nil {
			writeError(w, blockpool.Errorf(blockpool.CodeSpecInvalid, "data_base64 is not valid base64", "cause", decodeErr.Error()))
			return
		}
		result, err := s.pool.Ingest(r.Context(), blockpool.IngestRequest{
			RequestID: withRequestID(r, body.RequestID), Digest: body.Digest, Data: content,
			SizeBytes: body.SizeBytes, RetentionMS: body.RetentionMS,
		})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, result)
	default:
		methodNotAllowed(w, r)
	}
}

type mountDTO struct {
	RequestID  string `json:"request_id,omitempty"`
	Namespace  string `json:"namespace"`
	Path       string `json:"path"`
	Digest     string `json:"digest"`
	LeaseTTLMS int64  `json:"lease_ms,omitempty"`
}

func (s *Server) mount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, r)
		return
	}
	var body mountDTO
	if !decode(w, r, &body) {
		return
	}
	result, err := s.pool.Mount(r.Context(), blockpool.MountRequest{
		RequestID: withRequestID(r, body.RequestID), Namespace: body.Namespace,
		Path: body.Path, Digest: body.Digest, LeaseTTLMS: body.LeaseTTLMS,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type renewDTO struct {
	RequestID  string `json:"request_id,omitempty"`
	Namespace  string `json:"namespace"`
	Path       string `json:"path"`
	Digest     string `json:"digest"`
	Version    int64  `json:"version"`
	LeaseTTLMS int64  `json:"lease_ms,omitempty"`
}

func (s *Server) renew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, r)
		return
	}
	var body renewDTO
	if !decode(w, r, &body) {
		return
	}
	result, err := s.pool.Renew(r.Context(), blockpool.RenewRequest{
		RequestID: withRequestID(r, body.RequestID), Namespace: body.Namespace,
		Path: body.Path, Digest: body.Digest, Version: body.Version, LeaseTTLMS: body.LeaseTTLMS,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type unmountDTO struct {
	RequestID string `json:"request_id,omitempty"`
	Namespace string `json:"namespace"`
	Path      string `json:"path"`
	Digest    string `json:"digest"`
}

func (s *Server) unmount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, r)
		return
	}
	var body unmountDTO
	if !decode(w, r, &body) {
		return
	}
	result, err := s.pool.Unmount(r.Context(), blockpool.UnmountRequest{
		RequestID: withRequestID(r, body.RequestID), Namespace: body.Namespace,
		Path: body.Path, Digest: body.Digest,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type batchBlockDTO struct {
	Digest      string  `json:"digest"`
	DataBase64  *string `json:"data_base64,omitempty"`
	SizeBytes   *int64  `json:"size_bytes,omitempty"`
	RetentionMS int64   `json:"retention_ms,omitempty"`
}

type replaceDTO struct {
	RequestID  string          `json:"request_id,omitempty"`
	Namespace  string          `json:"namespace"`
	Path       string          `json:"path"`
	Blocks     []batchBlockDTO `json:"blocks"`
	LeaseTTLMS int64           `json:"lease_ms,omitempty"`
}

func (s *Server) replace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, r)
		return
	}
	var body replaceDTO
	if !decode(w, r, &body) {
		return
	}
	blocks := make([]blockpool.BatchBlock, 0, len(body.Blocks))
	for _, item := range body.Blocks {
		var content []byte
		if item.DataBase64 != nil {
			decoded, err := base64.StdEncoding.DecodeString(*item.DataBase64)
			if err != nil {
				writeError(w, blockpool.Errorf(blockpool.CodeSpecInvalid, "batch data_base64 is not valid base64",
					"digest", item.Digest, "cause", err.Error()))
				return
			}
			content = decoded
			if content == nil {
				content = []byte{}
			}
		}
		blocks = append(blocks, blockpool.BatchBlock{
			Digest: item.Digest, Data: content, SizeBytes: item.SizeBytes, RetentionMS: item.RetentionMS,
		})
	}
	result, err := s.pool.ReplaceNodeRefs(r.Context(), blockpool.ReplaceRequest{
		RequestID: withRequestID(r, body.RequestID), Namespace: body.Namespace,
		Path: body.Path, Blocks: blocks, LeaseTTLMS: body.LeaseTTLMS,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type reclaimDTO struct {
	RequestID     string `json:"request_id,omitempty"`
	Digest        string `json:"digest,omitempty"`
	CapacityBytes *int64 `json:"capacity_bytes,omitempty"`
}

func (s *Server) reclaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, r)
		return
	}
	var body reclaimDTO
	if !decode(w, r, &body) {
		return
	}
	result, err := s.pool.Reclaim(r.Context(), blockpool.ReclaimRequest{
		RequestID: withRequestID(r, body.RequestID), Digest: body.Digest, CapacityBytes: body.CapacityBytes,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) blockPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/blocks/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, blockpool.Errorf(blockpool.CodeSpecInvalid, "digest path segment is required"))
		return
	}
	digest := parts[0]
	if len(parts) == 1 {
		result, err := s.pool.GetBlock(r.Context(), digest)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if len(parts) == 2 && parts[1] == "leases" {
		includeHistory, _ := strconv.ParseBool(r.URL.Query().Get("history"))
		details, history, err := s.pool.LeaseDetails(r.Context(), digest, includeHistory)
		if err != nil {
			writeError(w, err)
			return
		}
		if includeHistory {
			writeJSON(w, http.StatusOK, map[string]any{"block": details, "leases": history})
		} else {
			writeJSON(w, http.StatusOK, map[string]any{"block": details, "leases": details.Leases})
		}
		return
	}
	if len(parts) == 2 && parts[1] == "content" {
		content, err := s.pool.GetContent(r.Context(), digest)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Content-SHA256", digest)
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
		return
	}
	writeError(w, blockpool.Errorf(blockpool.CodeNotFound, "block subresource not found", "path", r.URL.Path))
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, r)
		return
	}
	report, err := s.pool.RecoveryReport(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBody))
	if err := decoder.Decode(target); err != nil {
		writeError(w, blockpool.Errorf(blockpool.CodeSpecInvalid, "request body is not valid JSON", "cause", err.Error()))
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, blockpool.Errorf(blockpool.CodeSpecInvalid, "request body must contain exactly one JSON value"))
		return false
	}
	return true
}

func withRequestID(r *http.Request, bodyID string) string {
	if bodyID != "" {
		return bodyID
	}
	if value := r.Header.Get("Idempotency-Key"); value != "" {
		return value
	}
	return r.Header.Get("X-Request-ID")
}

func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeError(w, blockpool.Errorf(blockpool.CodeSpecInvalid, "method is not allowed for this route",
		"method", r.Method, "path", r.URL.Path))
}

func writeError(w http.ResponseWriter, err *blockpool.Error) {
	status := statusForCode(err.Code)
	writeJSON(w, status, err)
}

func statusForCode(code blockpool.Code) int {
	switch code {
	case blockpool.CodeSpecInvalid:
		return http.StatusBadRequest
	case blockpool.CodeNodeUnknown, blockpool.CodeBlockNotFound, blockpool.CodeNotFound:
		return http.StatusNotFound
	case blockpool.CodeContentCorrupt, blockpool.CodeLeaseStale, blockpool.CodeLeaseHeld,
		blockpool.CodeNodeBusy, blockpool.CodeReclaimUnsafe, blockpool.CodeConflictingRequest:
		return http.StatusConflict
	case blockpool.CodeContextCanceled:
		return http.StatusRequestTimeout
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

// ListenAndServe binds only to a loopback address. It is intentionally strict:
// wildcard and non-loopback addresses are rejected before listening.
func ListenAndServe(ctx context.Context, addr string, handler http.Handler) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parse listen address: %w", err)
	}
	if host == "" || (host != "localhost" && host != "127.0.0.1" && host != "::1") {
		return errors.New("HTTP endpoint must bind to loopback (127.0.0.1, ::1, or localhost)")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
