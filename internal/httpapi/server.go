package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"blocksnap/internal/apperr"
	"blocksnap/internal/metrics"
	"blocksnap/internal/store"
)

type Server struct {
	store          *store.Store
	faultInjection bool
	mux            *http.ServeMux
}

func New(st *store.Store, faultInjection bool) *Server {
	s := &Server{store: st, faultInjection: faultInjection, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /v1/volumes", s.handleCreateVolume)
	s.mux.HandleFunc("GET /v1/volumes/{volume}", s.handleGetVolume)
	s.mux.HandleFunc("PUT /v1/volumes/{volume}/blocks/{block}", s.handleWriteBlock)
	s.mux.HandleFunc("GET /v1/volumes/{volume}/blocks/{block}", s.handleGetBlock)
	s.mux.HandleFunc("POST /v1/volumes/{volume}/snapshots", s.handleCreateSnapshot)
	s.mux.HandleFunc("GET /v1/volumes/{volume}/snapshots", s.handleListSnapshots)
	s.mux.HandleFunc("GET /v1/volumes/{volume}/snapshots/{snapshot}/analysis", s.handleAnalysis)
	s.mux.HandleFunc("GET /v1/volumes/{volume}/versions/{snapshot}/analysis", s.handleAnalysis)
	s.mux.HandleFunc("DELETE /v1/volumes/{volume}/snapshots/{snapshot}", s.handleDeleteSnapshot)
	s.mux.HandleFunc("POST /v1/volumes/{volume}/snapshots/merge", s.handleMerge)
	s.mux.HandleFunc("POST /v1/volumes/{volume}/rollback/evaluate", s.handleEvaluateRollback)
	s.mux.HandleFunc("POST /v1/volumes/{volume}/rollback", s.handleRollback)
	s.mux.HandleFunc("GET /v1/diagnostics/arithmetic", s.handleArithmeticDiagnostics)
	s.mux.HandleFunc("GET /v1/recovery/reports/latest", s.handleRecoveryReport)
	s.mux.HandleFunc("POST /v1/debug/crash", s.handleSimulateCrash)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	buf := &bufferedResponse{header: make(http.Header)}
	defer func() {
		if recovered := recover(); recovered != nil {
			e := apperr.WithContext(apperr.Internal(fmt.Errorf("panic: %v", recovered)), "path", r.URL.Path)
			body, _ := json.Marshal(errorEnvelope{Error: e})
			buf.status = http.StatusInternalServerError
			buf.body.Reset()
			buf.body.Write(body)
			buf.header.Set("Content-Type", "application/json; charset=utf-8")
		}
		flushBufferedResponse(w, buf, r)
	}()
	s.mux.ServeHTTP(buf, r)
}

func flushBufferedResponse(w http.ResponseWriter, buf *bufferedResponse, r *http.Request) {
	if (buf.status == http.StatusNotFound || buf.status == http.StatusMethodNotAllowed) &&
		!strings.Contains(buf.header.Get("Content-Type"), "application/json") {
		code := apperr.CodeNotFound
		msg := "route not found"
		if buf.status == http.StatusMethodNotAllowed {
			code = apperr.CodeMethodNotAllowed
			msg = "method not allowed"
		}
		e := apperr.New(code, msg, buf.status)
		e.Context["method"] = r.Method
		e.Context["path"] = r.URL.Path
		body, _ := json.Marshal(errorEnvelope{Error: e})
		buf.status = e.HTTPStatus
		buf.body.Reset()
		buf.body.Write(body)
		buf.header.Set("Content-Type", "application/json; charset=utf-8")
	}
	for k, values := range buf.header {
		for _, value := range values {
			w.Header().Add(k, value)
		}
	}
	if buf.status == 0 {
		buf.status = http.StatusOK
	}
	w.WriteHeader(buf.status)
	_, _ = w.Write(buf.body.Bytes())
}

type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *bufferedResponse) Header() http.Header { return w.header }
func (w *bufferedResponse) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}
func (w *bufferedResponse) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(b)
}

type errorEnvelope struct {
	Error *apperr.Error `json:"error"`
}

func writeError(w http.ResponseWriter, err error) {
	ae := apperr.As(err)
	if len(ae.ReplayBody) > 0 {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		status := ae.ReplayStatus
		if status == 0 {
			status = ae.HTTPStatus
		}
		w.WriteHeader(status)
		_, _ = w.Write(ae.ReplayBody)
		return
	}
	status := ae.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{Error: ae})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type createVolumeBody struct {
	RequestID                      string `json:"request_id"`
	VolumeID                       string `json:"volume_id"`
	BlockCount                     int64  `json:"block_count"`
	BlockSize                      int64  `json:"block_size"`
	WritebackOverheadPerBlockBytes *int64 `json:"writeback_overhead_bytes_per_block,omitempty"`
}

func (s *Server) handleCreateVolume(w http.ResponseWriter, r *http.Request) {
	var body createVolumeBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	overhead := int64(4096)
	if body.WritebackOverheadPerBlockBytes != nil {
		overhead = *body.WritebackOverheadPerBlockBytes
	}
	hash := store.RequestHash("create_volume", body.VolumeID, struct {
		VolumeID   string `json:"volume_id"`
		BlockCount int64  `json:"block_count"`
		BlockSize  int64  `json:"block_size"`
		Overhead   int64  `json:"writeback_overhead_bytes_per_block"`
	}{body.VolumeID, body.BlockCount, body.BlockSize, overhead})
	result, err := s.store.CreateVolume(r.Context(), store.CreateVolumeRequest{
		VolumeID: body.VolumeID, RequestID: body.RequestID, RequestHash: hash,
		BlockCount: body.BlockCount, BlockSize: body.BlockSize, OverheadBytes: overhead,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeMutation(w, result)
}

func (s *Server) handleGetVolume(w http.ResponseWriter, r *http.Request) {
	view, err := s.store.GetVolume(r.Context(), r.PathValue("volume"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

type writeBlockBody struct {
	RequestID       string `json:"request_id"`
	ExpectedVersion int64  `json:"expected_version"`
	ContentBase64   string `json:"content_base64"`
	SHA256          string `json:"sha256"`
	WrittenAt       string `json:"written_at,omitempty"`
}

func (s *Server) handleWriteBlock(w http.ResponseWriter, r *http.Request) {
	blockNo, err := strconv.ParseInt(r.PathValue("block"), 10, 64)
	if err != nil || blockNo < 0 {
		writeError(w, apperr.Invalid("block number must be a non-negative integer"))
		return
	}
	var body writeBlockBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	content, decErr := decodeBase64(body.ContentBase64)
	if decErr != nil {
		writeError(w, apperr.WithContext(apperr.Invalid("content_base64 is invalid"), "detail", decErr.Error()))
		return
	}
	volumeID := r.PathValue("volume")
	hash := store.RequestHash("write_block", volumeID, struct {
		BlockNo         int64  `json:"block_no"`
		ExpectedVersion int64  `json:"expected_version"`
		ContentSHA256   string `json:"content_sha256"`
		ContentLength   int    `json:"content_length"`
		SHA256          string `json:"sha256"`
		WrittenAt       string `json:"written_at"`
	}{blockNo, body.ExpectedVersion, sha256Hex(content), len(content), strings.ToLower(strings.TrimPrefix(body.SHA256, "sha256:")), body.WrittenAt})
	result, err := s.store.WriteBlock(r.Context(), store.WriteBlockRequest{
		VolumeID: volumeID, BlockNo: blockNo, RequestID: body.RequestID, RequestHash: hash,
		ExpectedVersion: body.ExpectedVersion, Content: content, Digest: body.SHA256, WrittenAt: body.WrittenAt,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeMutation(w, result)
}

func (s *Server) handleGetBlock(w http.ResponseWriter, r *http.Request) {
	blockNo, err := strconv.ParseInt(r.PathValue("block"), 10, 64)
	if err != nil || blockNo < 0 {
		writeError(w, apperr.Invalid("block number must be a non-negative integer"))
		return
	}
	view, content, err := s.store.GetBlock(r.Context(), r.PathValue("volume"), blockNo)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"block": view, "content_base64": encodeBase64(content)})
}

type createSnapshotBody struct {
	RequestID  string `json:"request_id"`
	SnapshotID string `json:"snapshot_id"`
	Kind       string `json:"kind"`
	ParentID   string `json:"parent_id,omitempty"`
	Weight     int64  `json:"weight"`
	CreatedAt  string `json:"created_at,omitempty"`
}

func (s *Server) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	var body createSnapshotBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	volumeID := r.PathValue("volume")
	hash := store.RequestHash("create_snapshot", volumeID, struct {
		SnapshotID string `json:"snapshot_id"`
		Kind       string `json:"kind"`
		ParentID   string `json:"parent_id"`
		Weight     int64  `json:"weight"`
		CreatedAt  string `json:"created_at"`
	}{body.SnapshotID, body.Kind, body.ParentID, body.Weight, body.CreatedAt})
	result, err := s.store.CreateSnapshot(r.Context(), store.CreateSnapshotRequest{
		VolumeID: volumeID, SnapshotID: body.SnapshotID, RequestID: body.RequestID, RequestHash: hash,
		Kind: body.Kind, ParentID: body.ParentID, Weight: body.Weight, CreatedAt: body.CreatedAt,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeMutation(w, result)
}

func (s *Server) handleListSnapshots(w http.ResponseWriter, r *http.Request) {
	result, err := s.store.ListSnapshots(r.Context(), r.PathValue("volume"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleAnalysis(w http.ResponseWriter, r *http.Request) {
	analysis, err := s.store.GetSnapshotAnalysis(r.Context(), r.PathValue("volume"), r.PathValue("snapshot"))
	if err != nil {
		writeError(w, err)
		return
	}
	response := map[string]any{"analysis": analysis}
	decisionInput, present, decErr := decisionFromQuery(r)
	if decErr != nil {
		writeError(w, decErr)
		return
	}
	if present {
		decision, e := evaluateDecision(analysis, decisionInput)
		if e != nil {
			writeError(w, e)
			return
		}
		response["decision"] = decision
	}
	writeJSON(w, http.StatusOK, response)
}

type deleteSnapshotBody struct {
	RequestID string `json:"request_id"`
}

func (s *Server) handleDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	var body deleteSnapshotBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	volumeID := r.PathValue("volume")
	snapshotID := r.PathValue("snapshot")
	hash := store.RequestHash("delete_snapshot", volumeID, map[string]string{"snapshot_id": snapshotID})
	result, err := s.store.DeleteSnapshot(r.Context(), store.DeleteSnapshotRequest{
		VolumeID: volumeID, SnapshotID: snapshotID, RequestID: body.RequestID, RequestHash: hash,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeMutation(w, result)
}

type mergeBody struct {
	RequestID string `json:"request_id"`
	ParentID  string `json:"parent_id"`
	ChildID   string `json:"child_id"`
}

func (s *Server) handleMerge(w http.ResponseWriter, r *http.Request) {
	var body mergeBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	volumeID := r.PathValue("volume")
	hash := store.RequestHash("merge", volumeID, struct {
		ParentID string `json:"parent_id"`
		ChildID  string `json:"child_id"`
	}{body.ParentID, body.ChildID})
	result, err := s.store.MergeSnapshots(r.Context(), store.MergeRequest{
		VolumeID: volumeID, RequestID: body.RequestID, RequestHash: hash, ParentID: body.ParentID, ChildID: body.ChildID,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeMutation(w, result)
}

type decisionBody struct {
	MarginFloor       string `json:"margin_floor"`
	CostBudgetBytes   string `json:"cost_budget_bytes"`
	RelativeTolerance string `json:"relative_tolerance"`
}

func (s *Server) handleEvaluateRollback(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SnapshotIDs []string `json:"snapshot_ids"`
		decisionBody
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	input, err := parseDecisionInput(body.decisionBody)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := s.store.EvaluateRollback(r.Context(), store.EvaluateRollbackRequest{
		VolumeID: r.PathValue("volume"), SnapshotIDs: body.SnapshotIDs, Decision: input,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type rollbackBody struct {
	RequestID  string `json:"request_id"`
	SnapshotID string `json:"snapshot_id"`
	decisionBody
}

func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request) {
	var body rollbackBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	input, err := parseDecisionInput(body.decisionBody)
	if err != nil {
		writeError(w, err)
		return
	}
	volumeID := r.PathValue("volume")
	hash := store.RequestHash("rollback", volumeID, struct {
		SnapshotID        string `json:"snapshot_id"`
		MarginFloor       string `json:"margin_floor"`
		CostBudgetBytes   string `json:"cost_budget_bytes"`
		RelativeTolerance string `json:"relative_tolerance"`
	}{body.SnapshotID, body.MarginFloor, body.CostBudgetBytes, body.RelativeTolerance})
	result, err := s.store.Rollback(r.Context(), store.RollbackRequest{
		VolumeID: volumeID, SnapshotID: body.SnapshotID, RequestID: body.RequestID, RequestHash: hash, Decision: input,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeMutation(w, result)
}

func (s *Server) handleRecoveryReport(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.LatestRecoveryReport(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

type crashBody struct {
	VolumeID  string `json:"volume_id"`
	Operation string `json:"operation"`
	ParentID  string `json:"parent_id,omitempty"`
	ChildID   string `json:"child_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func (s *Server) handleSimulateCrash(w http.ResponseWriter, r *http.Request) {
	if !s.faultInjection {
		writeError(w, apperr.New(apperr.CodeNotFound, "fault injection is disabled", http.StatusNotFound))
		return
	}
	var body crashBody
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	err := s.store.SimulateCrash(r.Context(), store.SimulateCrashRequest{
		VolumeID: body.VolumeID, Operation: body.Operation, ParentID: body.ParentID, ChildID: body.ChildID, RequestID: body.RequestID,
	}, func(exitCode int) {
		log.Printf("fault injection: terminating process with exit code %d", exitCode)
		os.Exit(exitCode)
	})
	if err != nil {
		writeError(w, err)
	}
}

func (s *Server) handleArithmeticDiagnostics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, arithmeticDiagnostics())
}

func evaluateDecision(analysis metrics.Analysis, input store.DecisionInput) (metrics.Decision, error) {
	return metrics.Decide(analysis, metrics.DecisionRequest{
		MarginFloorUnits: input.MarginFloor, CostBudgetBytes: input.CostBudgetBytes, RelativeTolerance: input.RelativeTolerance,
	})
}

func writeMutation(w http.ResponseWriter, result store.MutationResult) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	status := result.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(result.Body)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 128<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return apperr.WithContext(apperr.Invalid("invalid JSON body"), "detail", err.Error())
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return apperr.Invalid("request body must contain exactly one JSON object")
	}
	return nil
}
