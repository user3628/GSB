package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"ampcheckpoint/internal/domain"
	"ampcheckpoint/internal/engine"
)

type API struct {
	engine *engine.Engine
}

func New(eng *engine.Engine) http.Handler {
	a := &API{engine: eng}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("GET /v1/status", a.status)
	mux.HandleFunc("GET /v1/sample", a.sample)
	mux.HandleFunc("GET /v1/policies/current", a.currentPolicy)
	mux.HandleFunc("GET /v1/policies/{version}", a.policyByVersion)
	mux.HandleFunc("POST /v1/policies", a.submitPolicy)
	mux.HandleFunc("POST /v1/policy-snapshots", a.submitPolicy)
	mux.HandleFunc("POST /v1/deltas", a.submitDelta)
	mux.HandleFunc("POST /v1/policy-deltas", a.submitDelta)
	mux.HandleFunc("POST /v1/partitions/validate", a.validatePartitions)
	mux.HandleFunc("POST /v1/checkpoints", a.commitCheckpoint)
	mux.HandleFunc("POST /v1/checkpoints/commit", a.commitCheckpoint)
	for _, base := range []string{"/v1/versions", "/v1/checkpoints"} {
		mux.HandleFunc("GET "+base+"/{version}", a.checkpointMeta)
		mux.HandleFunc("GET "+base+"/{version}/meta", a.checkpointMeta)
		mux.HandleFunc("GET "+base+"/{version}/shards", a.shards)
		mux.HandleFunc("GET "+base+"/{version}/pools", a.pools)
		mux.HandleFunc("GET "+base+"/{version}/precision", a.precision)
		mux.HandleFunc("GET "+base+"/{version}/recovery", a.recovery)
		mux.HandleFunc("GET "+base+"/{version}/derived", a.derived)
		mux.HandleFunc("GET "+base+"/{version}/writes", a.writes)
	}
	return withRecover(normalizeHTTPErrors(mux))
}

func withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(w, domain.BizError(domain.ErrInternal, "unhandled server panic", map[string]any{"panic": fmt.Sprint(recovered)}))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type errorNormalizingWriter struct {
	http.ResponseWriter
	requestMethod string
	requestPath   string
	status        int
	wroteHeader   bool
	capture       bool
	buffer        bytes.Buffer
}

func normalizeHTTPErrors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := &errorNormalizingWriter{ResponseWriter: w, requestMethod: r.Method, requestPath: r.URL.Path}
		next.ServeHTTP(writer, r)
		writer.finish()
	})
}

func (w *errorNormalizingWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	contentType := w.Header().Get("Content-Type")
	if (status == http.StatusNotFound || status == http.StatusMethodNotAllowed) && !strings.Contains(contentType, "json") {
		w.capture = true
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *errorNormalizingWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.capture {
		return w.buffer.Write(p)
	}
	return w.ResponseWriter.Write(p)
}

func (w *errorNormalizingWriter) finish() {
	if !w.capture {
		return
	}
	code := domain.ErrNotFound
	message := "HTTP route not found"
	if w.status == http.StatusMethodNotAllowed {
		code = domain.ErrInvalidRequest
		message = "HTTP method is not allowed for this route"
	}
	w.Header().Del("Content-Length")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.ResponseWriter.WriteHeader(w.status)
	encoder := json.NewEncoder(w.ResponseWriter)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(map[string]any{
		"error": domain.BizError(code, message, map[string]any{
			"method":        w.requestMethod,
			"path":          w.requestPath,
			"original_body": strings.TrimSpace(w.buffer.String()),
		}),
	})
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	status, err := a.engine.Status(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (a *API) sample(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.engine.SampleManifest())
}

func (a *API) submitPolicy(w http.ResponseWriter, r *http.Request) {
	var input domain.PolicySnapshot
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	policy, err := a.engine.SubmitPolicy(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"policy_version": policy.Version,
		"source":         policy.Source,
		"checksum":       policy.Checksum,
		"policy":         policy,
	})
}

func (a *API) submitDelta(w http.ResponseWriter, r *http.Request) {
	var input domain.DeltaFragment
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	policy, err := a.engine.SubmitDelta(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"policy_version": policy.Version,
		"base_version":   policy.BaseVersion,
		"source":         policy.Source,
		"checksum":       policy.Checksum,
		"policy":         policy,
	})
}

func (a *API) currentPolicy(w http.ResponseWriter, r *http.Request) {
	policy, err := a.engine.CurrentPolicy(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (a *API) policyByVersion(w http.ResponseWriter, r *http.Request) {
	version, err := parseVersion(r.PathValue("version"), false)
	if err != nil {
		writeError(w, err)
		return
	}
	policy, err := a.engine.Policy(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (a *API) validatePartitions(w http.ResponseWriter, r *http.Request) {
	var input domain.PartitionBatch
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	result, err := domain.ValidatePartitionBatch(input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) commitCheckpoint(w http.ResponseWriter, r *http.Request) {
	var input domain.CheckpointRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	result, err := a.engine.CommitCheckpoint(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if result.FaultInjected != "" {
		status = http.StatusAccepted
	}
	writeJSON(w, status, result)
}

func (a *API) checkpointMeta(w http.ResponseWriter, r *http.Request) {
	version, err := a.resolveVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	meta, err := a.engine.CheckpointMeta(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, meta)
}

func (a *API) shards(w http.ResponseWriter, r *http.Request) {
	version, err := a.resolveVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := a.engine.Shards(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) pools(w http.ResponseWriter, r *http.Request) {
	version, err := a.resolveVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	pools, err := a.engine.Pools(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	var vramUsed, vramLimit, persistentUsed int64
	for _, p := range pools {
		if p.Pool == domain.PoolVRAM {
			vramUsed, vramLimit = p.TotalBytes, p.LimitBytes
		}
		if p.Pool == domain.PoolPersistent {
			persistentUsed = p.TotalBytes
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":               version,
		"pools":                 pools,
		"vram_used_bytes":       vramUsed,
		"vram_limit_bytes":      vramLimit,
		"persistent_used_bytes": persistentUsed,
	})
}

func (a *API) precision(w http.ResponseWriter, r *http.Request) {
	version, err := a.resolveVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	report, err := a.engine.Precision(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) recovery(w http.ResponseWriter, r *http.Request) {
	version, err := a.resolveVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	report, err := a.engine.Recovery(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) derived(w http.ResponseWriter, r *http.Request) {
	version, err := a.resolveVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	derived, err := a.engine.Derived(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "derived": derived})
}

func (a *API) writes(w http.ResponseWriter, r *http.Request) {
	version, err := a.resolveVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := a.engine.CheckpointResult(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) resolveVersion(r *http.Request) (int64, error) {
	if strings.EqualFold(r.PathValue("version"), "latest") {
		meta, err := a.engine.LatestCheckpointMeta(r.Context())
		if err != nil {
			return 0, err
		}
		return meta.Version, nil
	}
	return parseVersion(r.PathValue("version"), true)
}

func parseVersion(raw string, required bool) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		if required {
			return 0, domain.BizError(domain.ErrInvalidRequest, "version path parameter is required", nil)
		}
		return 0, domain.BizError(domain.ErrInvalidRequest, "version is required", nil)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0, domain.BizError(domain.ErrInvalidRequest, "version must be a positive integer", map[string]any{"version": raw})
	}
	return v, nil
}

func decodeJSON(r *http.Request, target any) error {
	if r.Body == nil {
		return domain.BizError(domain.ErrInvalidRequest, "request body is required", nil)
	}
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return domain.BizError(domain.ErrInvalidRequest, "invalid JSON request body", map[string]any{"reason": err.Error()})
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return domain.BizError(domain.ErrInvalidRequest, "request body must contain exactly one JSON object", nil)
	}
	return nil
}

func writeError(w http.ResponseWriter, err error) {
	be := &domain.BusinessError{Code: domain.ErrInternal, Message: "internal server error"}
	if !errors.As(err, &be) || be.Code == "" {
		be = domain.BizError(domain.ErrInternal, "internal server error", map[string]any{"reason": err.Error()})
	}
	if be.Context == nil {
		be.Context = map[string]any{}
	}
	status := httpStatus(be.Code)
	writeJSON(w, status, map[string]any{"error": be})
}

func httpStatus(code string) int {
	switch code {
	case domain.ErrPartitionInvalid, domain.ErrChecksumMismatch:
		return http.StatusUnprocessableEntity
	case domain.ErrVRAMPoolFull:
		return http.StatusInsufficientStorage
	case domain.ErrLossScaleOverflow:
		return http.StatusUnprocessableEntity
	case domain.ErrStaleSnapshot, domain.ErrStaleBase:
		return http.StatusConflict
	case domain.ErrWriteQueueFull:
		return http.StatusTooManyRequests
	case domain.ErrInvalidRequest:
		return http.StatusBadRequest
	case domain.ErrNotFound:
		return http.StatusNotFound
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
