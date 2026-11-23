package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"h2diag/internal/engine"
	"h2diag/internal/model"
	"h2diag/internal/store"
)

const maxRequestBody = 128 << 20

type Server struct {
	store       *store.Store
	sampleLocks sync.Map
	writeMu     sync.Mutex
}

type batchRequest struct {
	BatchID  string                `json:"batch_id"`
	Final    bool                  `json:"final"`
	Metadata *model.SampleMetadata `json:"metadata,omitempty"`
	Frames   []model.Frame         `json:"frames"`
}

func New(store *store.Store) *Server {
	return &Server{store: store}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/v1/samples", s.handleSamples)
	mux.HandleFunc("/v1/samples/", s.handleSamplePath)
	mux.HandleFunc("/v1/recovery", s.handleRecovery)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeBizError(w, model.Biz(model.CodeNotFound, "route not found", map[string]any{"path": r.URL.Path}))
	})
	return s.recoverPanics(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339Nano)})
}

func (s *Server) handleSamples(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxRequestBody)
	defer body.Close()
	var raw json.RawMessage
	if err := decodeStrict(body, &raw); err != nil {
		writeBizError(w, model.Biz(model.CodeFrameInvalid, "request body is not valid JSON", map[string]any{"detail": err.Error()}))
		return
	}
	sample, biz := engine.ParseSample(raw)
	if biz != nil {
		writeBizError(w, biz)
		return
	}
	_, hash, err := model.CanonicalSample(sample)
	if err != nil {
		writeInternal(w, err)
		return
	}

	unlock := s.lockSample(sample.SampleID)
	defer unlock()
	ctx := r.Context()
	meta, exists, err := s.store.GetSampleMeta(ctx, sample.SampleID)
	if err != nil {
		writeInternal(w, err)
		return
	}
	if exists {
		if meta.CanonicalHash != hash {
			writeBizError(w, conflictingHash(sample.SampleID, meta.CanonicalHash, hash))
			return
		}
		if meta.Status == "DONE" {
			response, decodeErr := storedResponse(meta)
			if decodeErr != nil {
				writeInternal(w, decodeErr)
				return
			}
			response.Reused = true
			writeJSON(w, http.StatusOK, response)
			return
		}
		writeBizError(w, sampleNotRunnable(meta))
		return
	}
	if draft, draftErr := s.store.DraftSummary(ctx, sample.SampleID); draftErr != nil {
		writeInternal(w, draftErr)
		return
	} else if draft != nil {
		writeBizError(w, model.Biz(model.CodeConflictingSample, "sample id already has an unfinished batch draft", map[string]any{
			"sample_id": sample.SampleID, "draft": draft,
		}))
		return
	}

	plan, biz := engine.Compile(sample)
	if biz != nil {
		writeBizError(w, biz)
		return
	}
	response, biz, err := s.persistPlanLocked(ctx, plan)
	if err != nil {
		writeInternal(w, err)
		return
	}
	if biz != nil {
		writeBizError(w, biz)
		return
	}
	writeJSON(w, http.StatusCreated, response)
}

func (s *Server) handleSamplePath(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/samples/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeBizError(w, model.Biz(model.CodeNotFound, "sample id is required", nil))
		return
	}
	sampleID, err := url.PathUnescape(parts[0])
	if err != nil || strings.TrimSpace(sampleID) == "" {
		writeBizError(w, model.Biz(model.CodeFrameInvalid, "sample id is invalid", nil))
		return
	}
	action := ""
	if len(parts) > 1 {
		action = strings.Join(parts[1:], "/")
	}
	switch action {
	case "", "snapshot":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleGetSample(w, r, sampleID)
	case "batches":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleBatch(w, r, sampleID)
	case "rounds":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleRounds(w, r, sampleID)
	case "verify":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleVerify(w, r, sampleID)
	case "recovery":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleRecoveryForSample(w, r, sampleID)
	default:
		writeBizError(w, model.Biz(model.CodeNotFound, "sample sub-route not found", map[string]any{"sample_id": sampleID, "action": action}))
	}
}

func (s *Server) handleGetSample(w http.ResponseWriter, r *http.Request, sampleID string) {
	meta, exists, err := s.store.GetSampleMeta(r.Context(), sampleID)
	if err != nil {
		writeInternal(w, err)
		return
	}
	if !exists {
		draft, draftErr := s.store.DraftSummary(r.Context(), sampleID)
		if draftErr != nil {
			writeInternal(w, draftErr)
			return
		}
		if draft == nil {
			writeBizError(w, model.Biz(model.CodeNotFound, "sample not found", map[string]any{"sample_id": sampleID}))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"sample_id": sampleID,
			"status":    "COLLECTING",
			"draft":     draft,
		})
		return
	}
	if versionText := r.URL.Query().Get("version"); versionText != "" {
		version, parseErr := strconv.ParseInt(versionText, 10, 64)
		if parseErr != nil || version < 0 {
			writeBizError(w, model.Biz(model.CodeFrameInvalid, "version must be a non-negative integer", map[string]any{"version": versionText}))
			return
		}
		versionSnapshot, versionErr := s.store.LoadSnapshotAtVersion(r.Context(), sampleID, version)
		if versionErr != nil {
			writeStoreError(w, versionErr)
			return
		}
		writeJSON(w, http.StatusOK, model.SnapshotResponse{
			SampleID:      sampleID,
			Status:        meta.Status,
			CanonicalHash: meta.CanonicalHash,
			Snapshot:      *versionSnapshot,
			TraceURL:      "/v1/samples/" + url.PathEscape(sampleID) + "/rounds",
			RecoveryURL:   "/v1/samples/" + url.PathEscape(sampleID) + "/recovery",
		})
		return
	}
	snapshot, metaAfter, err := s.store.LoadSnapshot(r.Context(), sampleID, true)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	response := model.SnapshotResponse{
		SampleID:      sampleID,
		Status:        metaAfter.Status,
		CanonicalHash: metaAfter.CanonicalHash,
		Snapshot:      *snapshot,
		TraceURL:      "/v1/samples/" + url.PathEscape(sampleID) + "/rounds",
		RecoveryURL:   "/v1/samples/" + url.PathEscape(sampleID) + "/recovery",
	}
	if len(metaAfter.ResultJSON) > 0 {
		var stored model.SubmitResponse
		if jsonErr := json.Unmarshal(metaAfter.ResultJSON, &stored); jsonErr == nil {
			response.Summary = &stored.Summary
		}
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request, sampleID string) {
	body := http.MaxBytesReader(w, r.Body, maxRequestBody)
	defer body.Close()
	var request batchRequest
	if err := decodeStrict(body, &request); err != nil {
		writeBizError(w, model.Biz(model.CodeFrameInvalid, "batch body is not valid JSON", map[string]any{"detail": err.Error()}))
		return
	}
	request.BatchID = strings.TrimSpace(request.BatchID)
	if request.BatchID == "" {
		writeBizError(w, model.Biz(model.CodeFrameInvalid, "batch_id is required", map[string]any{"sample_id": sampleID}))
		return
	}

	unlock := s.lockSample(sampleID)
	defer unlock()
	ctx := r.Context()
	meta, exists, err := s.store.GetSampleMeta(ctx, sampleID)
	if err != nil {
		writeInternal(w, err)
		return
	}
	if exists {
		switch meta.Status {
		case "DONE":
			existingFrames, found, frameErr := s.store.BatchFrames(ctx, sampleID, request.BatchID)
			if frameErr != nil {
				writeInternal(w, frameErr)
				return
			}
			if !found {
				writeBizError(w, model.Biz(model.CodeConflictingSample, "sample is finalized; no new batches may be appended", map[string]any{
					"sample_id": sampleID, "existing_hash": meta.CanonicalHash,
				}))
				return
			}
			if !equalFrameSlices(existingFrames, request.Frames) {
				writeBizError(w, model.Biz(model.CodeConflictingSample, "batch content differs from the committed batch", map[string]any{
					"sample_id": sampleID, "batch_id": request.BatchID,
				}))
				return
			}
			response, decodeErr := storedResponse(meta)
			if decodeErr != nil {
				writeInternal(w, decodeErr)
				return
			}
			response.Reused = true
			writeJSON(w, http.StatusOK, response)
			return
		case "RUNNING", "ABORTED":
			writeBizError(w, sampleNotRunnable(meta))
			return
		}
	}

	draft, draftErr := s.store.DraftSummary(ctx, sampleID)
	if draftErr != nil {
		writeInternal(w, draftErr)
		return
	}
	if !exists && draft != nil && draft["status"] != "COLLECTING" {
		writeBizError(w, model.Biz(model.CodeConflictingSample, "sample draft is no longer collecting", map[string]any{"sample_id": sampleID, "draft": draft}))
		return
	}
	if request.Metadata != nil && draft != nil {
		storedMetadata, _, found, loadErr := s.store.LoadBatches(ctx, sampleID)
		if loadErr != nil {
			writeInternal(w, loadErr)
			return
		}
		if found && !equalMetadata(storedMetadata, request.Metadata) {
			writeBizError(w, model.Biz(model.CodeConflictingSample, "batch metadata differs from the draft metadata", map[string]any{
				"sample_id": sampleID,
			}))
			return
		}
	}
	reusedBatch, saveErr := s.store.SaveBatch(ctx, sampleID, request.BatchID, request.Metadata, request.Frames)
	if saveErr != nil {
		writeStoreError(w, saveErr)
		return
	}
	if !request.Final {
		batchCount := 0
		if summary, summaryErr := s.store.DraftSummary(ctx, sampleID); summaryErr == nil && summary != nil {
			if count, ok := summary["batch_count"].(int); ok {
				batchCount = count
			}
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"sample_id":    sampleID,
			"status":       "COLLECTING",
			"batch_id":     request.BatchID,
			"reused_batch": reusedBatch,
			"batch_count":  batchCount,
		})
		return
	}

	metadata, frames, found, loadErr := s.store.LoadBatches(ctx, sampleID)
	if loadErr != nil {
		writeInternal(w, loadErr)
		return
	}
	if !found {
		writeBizError(w, model.Biz(model.CodeFrameInvalid, "sample metadata is missing from batch draft", map[string]any{"sample_id": sampleID}))
		return
	}
	sample := &model.Sample{SampleID: sampleID, Config: metadata.Config, Connections: metadata.Connections, Streams: metadata.Streams, Frames: frames}
	plan, biz := engine.Compile(sample)
	if biz != nil {
		writeBizError(w, biz)
		return
	}
	response, persistBiz, persistErr := s.persistPlanLocked(ctx, plan)
	if persistErr != nil {
		writeInternal(w, persistErr)
		return
	}
	if persistBiz != nil {
		writeBizError(w, persistBiz)
		return
	}
	if err := s.store.MarkDraftConsumed(ctx, sampleID); err != nil {
		writeInternal(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, response)
}

func (s *Server) persistPlanLocked(ctx context.Context, plan *model.Plan) (*model.SubmitResponse, *model.BizError, error) {
	meta, exists, err := s.store.GetSampleMeta(ctx, plan.Sample.SampleID)
	if err != nil {
		return nil, nil, err
	}
	if exists {
		if meta.CanonicalHash != plan.Hash {
			return nil, conflictingHash(plan.Sample.SampleID, meta.CanonicalHash, plan.Hash), nil
		}
		if meta.Status == "DONE" {
			response, decodeErr := storedResponse(meta)
			if decodeErr != nil {
				return nil, nil, decodeErr
			}
			response.Reused = true
			return &response, nil, nil
		}
		return nil, sampleNotRunnable(meta), nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.store.CreateSample(ctx, plan); err != nil {
		return nil, nil, err
	}
	for _, commit := range plan.Commits {
		if err := s.store.ApplyCommit(ctx, plan.Sample.SampleID, commit); err != nil {
			_ = s.store.MarkSampleAborted(context.Background(), plan.Sample.SampleID, model.CodeInternal, err.Error())
			return nil, nil, err
		}
	}
	response := model.SubmitResponse{
		SampleID:      plan.Sample.SampleID,
		Status:        "DONE",
		CanonicalHash: plan.Hash,
		Reused:        false,
		Version:       plan.Final.Version,
		Summary:       plan.Summary,
		Snapshot:      plan.Final,
		TraceURL:      "/v1/samples/" + url.PathEscape(plan.Sample.SampleID) + "/rounds",
		RecoveryURL:   "/v1/samples/" + url.PathEscape(plan.Sample.SampleID) + "/recovery",
	}
	if err := s.store.FinalizeSample(ctx, plan, response); err != nil {
		_ = s.store.MarkSampleAborted(context.Background(), plan.Sample.SampleID, model.CodeInternal, err.Error())
		return nil, nil, err
	}
	return &response, nil, nil
}

func (s *Server) handleRounds(w http.ResponseWriter, r *http.Request, sampleID string) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if _, exists, err := s.store.GetSampleMeta(r.Context(), sampleID); err != nil {
		writeInternal(w, err)
		return
	} else if !exists {
		writeBizError(w, model.Biz(model.CodeNotFound, "sample not found", map[string]any{"sample_id": sampleID}))
		return
	}
	if biz := s.store.VerifyCheckpoint(r.Context(), sampleID); biz != nil {
		writeBizError(w, biz)
		return
	}
	rounds, err := s.store.ListRounds(r.Context(), sampleID, limit, offset)
	if err != nil {
		writeInternal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sample_id": sampleID,
		"count":     len(rounds),
		"offset":    offset,
		"rounds":    rounds,
	})
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request, sampleID string) {
	meta, exists, err := s.store.GetSampleMeta(r.Context(), sampleID)
	if err != nil {
		writeInternal(w, err)
		return
	}
	if !exists {
		writeBizError(w, model.Biz(model.CodeNotFound, "sample not found", map[string]any{"sample_id": sampleID}))
		return
	}
	if biz := s.store.VerifyCheckpoint(r.Context(), sampleID); biz != nil {
		writeBizError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sample_id":     sampleID,
		"status":        meta.Status,
		"version":       meta.Version,
		"state_digest":  meta.StateDigest,
		"checkpoint_ok": true,
	})
}

func (s *Server) handleRecovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	s.handleRecoveryForSample(w, r, r.URL.Query().Get("sample_id"))
}

func (s *Server) handleRecoveryForSample(w http.ResponseWriter, r *http.Request, sampleID string) {
	reports, err := s.store.ListRecoveryReports(r.Context(), sampleID, 100)
	if err != nil {
		writeInternal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sample_id": sampleID,
		"count":     len(reports),
		"reports":   reports,
	})
}

func (s *Server) lockSample(sampleID string) func() {
	value, _ := s.sampleLocks.LoadOrStore(sampleID, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

func storedResponse(meta *model.SampleMeta) (model.SubmitResponse, error) {
	var response model.SubmitResponse
	if len(meta.ResultJSON) == 0 {
		return response, fmt.Errorf("sample %s is DONE but result_json is empty", meta.SampleID)
	}
	if err := json.Unmarshal(meta.ResultJSON, &response); err != nil {
		return response, err
	}
	response.TraceURL = "/v1/samples/" + url.PathEscape(meta.SampleID) + "/rounds"
	response.RecoveryURL = "/v1/samples/" + url.PathEscape(meta.SampleID) + "/recovery"
	return response, nil
}

func sampleNotRunnable(meta *model.SampleMeta) *model.BizError {
	return model.Biz(model.CodeSampleAborted, "sample is not runnable in its current state", map[string]any{
		"sample_id":     meta.SampleID,
		"status":        meta.Status,
		"last_version":  meta.Version,
		"state_digest":  meta.StateDigest,
		"recovery_hint": "query the sample snapshot and recovery report; submit a new sample_id to retry",
	})
}

func conflictingHash(sampleID, existingHash, incomingHash string) *model.BizError {
	return model.Biz(model.CodeConflictingSample, "sample id already exists with different canonical content", map[string]any{
		"sample_id":     sampleID,
		"existing_hash": existingHash,
		"incoming_hash": incomingHash,
	})
}

func equalMetadata(left, right *model.SampleMetadata) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftCopy := *left
	rightCopy := *right
	leftCopy.Connections = append([]model.ConnectionSpec(nil), left.Connections...)
	rightCopy.Connections = append([]model.ConnectionSpec(nil), right.Connections...)
	leftCopy.Streams = append([]model.StreamSpec(nil), left.Streams...)
	rightCopy.Streams = append([]model.StreamSpec(nil), right.Streams...)
	sort.Slice(leftCopy.Connections, func(i, j int) bool { return leftCopy.Connections[i].ID < leftCopy.Connections[j].ID })
	sort.Slice(rightCopy.Connections, func(i, j int) bool { return rightCopy.Connections[i].ID < rightCopy.Connections[j].ID })
	sort.Slice(leftCopy.Streams, func(i, j int) bool {
		if leftCopy.Streams[i].ConnectionID == leftCopy.Streams[j].ConnectionID {
			return leftCopy.Streams[i].StreamID < leftCopy.Streams[j].StreamID
		}
		return leftCopy.Streams[i].ConnectionID < leftCopy.Streams[j].ConnectionID
	})
	sort.Slice(rightCopy.Streams, func(i, j int) bool {
		if rightCopy.Streams[i].ConnectionID == rightCopy.Streams[j].ConnectionID {
			return rightCopy.Streams[i].StreamID < rightCopy.Streams[j].StreamID
		}
		return rightCopy.Streams[i].ConnectionID < rightCopy.Streams[j].ConnectionID
	})
	return reflect.DeepEqual(leftCopy, rightCopy)
}

func equalFrameSlices(left, right []model.Frame) bool {
	if len(left) != len(right) {
		return false
	}
	return reflect.DeepEqual(normalizeFramesForCompare(left), normalizeFramesForCompare(right))
}

func normalizeFramesForCompare(frames []model.Frame) []model.Frame {
	out := make([]model.Frame, len(frames))
	for i, frame := range frames {
		out[i] = frame
		if out[i].Flags == nil {
			out[i].Flags = []string{}
		}
		sort.Strings(out[i].Flags)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence < out[j].Sequence
		}
		return out[i].Timestamp < out[j].Timestamp
	})
	return out
}

func decodeStrict(reader io.Reader, target any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeBizError(w, model.Biz(model.CodeMethodNotAllowed, "HTTP method is not allowed", map[string]any{"allowed": allowed}))
}

func writeStoreError(w http.ResponseWriter, err error) {
	var biz *model.BizError
	if errors.As(err, &biz) {
		writeBizError(w, biz)
		return
	}
	writeInternal(w, err)
}

func writeBizError(w http.ResponseWriter, biz *model.BizError) {
	writeJSON(w, biz.HTTPStatus(), biz)
}

func writeInternal(w http.ResponseWriter, err error) {
	log.Printf("internal error: %v", err)
	writeJSON(w, http.StatusInternalServerError, model.Biz(model.CodeInternal, "internal error", map[string]any{"detail": err.Error()}))
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}

func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeInternal(w, fmt.Errorf("panic: %v", recovered))
			}
		}()
		next.ServeHTTP(w, r)
	})
}
