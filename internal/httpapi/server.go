package httpapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"recoveryengine/internal/engine"
	"recoveryengine/internal/fixed"
	"recoveryengine/internal/graph"
	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
	"recoveryengine/internal/webui"
	"recoveryengine/samples"
)

type Server struct {
	engine    *engine.Engine
	testHooks bool
	handler   http.Handler
}

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func New(engine *engine.Engine, testHooks bool) *Server {
	server := &Server{engine: engine, testHooks: testHooks}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/healthz", server.health)
	mux.HandleFunc("GET /api/v1/meta", server.meta)
	mux.HandleFunc("GET /api/v1/state", server.state)
	mux.HandleFunc("GET /api/v1/versions", server.versions)
	mux.HandleFunc("GET /api/v1/impact", server.impactQuery)
	mux.HandleFunc("GET /api/v1/impact/{component}", server.impactPath)
	mux.HandleFunc("GET /api/v1/consistency", server.consistency)
	mux.HandleFunc("POST /api/v1/snapshots", server.commitSnapshot)
	mux.HandleFunc("POST /api/v1/increments", server.commitIncrement)
	mux.HandleFunc("POST /api/v1/drills", server.executeDrill)
	mux.HandleFunc("GET /api/v1/drills", server.listDrills)
	mux.HandleFunc("GET /api/v1/drills/{id}", server.getDrill)
	mux.HandleFunc("GET /api/v1/intents", server.listIntents)
	mux.HandleFunc("GET /api/v1/recovery-reports", server.recoveryReports)
	mux.HandleFunc("GET /api/v1/lineage", server.lineage)
	mux.HandleFunc("GET /api/v1/lineage/rebuilds", server.rebuild)
	mux.HandleFunc("GET /api/v1/runtime/reservations", server.reservations)
	mux.HandleFunc("GET /api/v1/samples", server.listSamples)
	mux.HandleFunc("GET /api/v1/samples/raw", server.rawSample)
	mux.HandleFunc("POST /api/v1/samples/validate", server.validateSample)
	mux.HandleFunc("POST /api/v1/samples/load", server.loadSample)
	mux.Handle("/", webui.Handler())
	server.handler = server.middleware(mux)
	return server
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	s.handler.ServeHTTP(writer, request)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		writer.Header().Set("Cache-Control", "no-store")
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(writer, model.NewError(model.CodeInternal, "unhandled server panic", 500).
					WithContext("panic", fmt.Sprint(recovered)))
			}
			log.Printf("%s %s %s", request.Method, request.URL.Path, time.Since(started).Round(time.Millisecond))
		}()
		next.ServeHTTP(writer, request)
	})
}

func (s *Server) health(writer http.ResponseWriter, _ *http.Request) {
	state := s.engine.State()
	writeSuccess(writer, http.StatusOK, map[string]any{
		"status": "ok", "version": state.Version, "derived_digest": state.Derivation.DerivedDigest,
	})
}

func (s *Server) meta(writer http.ResponseWriter, _ *http.Request) {
	writeSuccess(writer, http.StatusOK, map[string]any{
		"name": "Recovery Coordinator", "api_version": "v1",
		"business_codes": []string{
			model.CodeSpecInvalid, model.CodeDependencyCycle, model.CodeStaleBase, model.CodeStaleVersion,
			model.CodeConflictingRequest, model.CodeVersionMismatch, model.CodeProbeTimeout,
			model.CodeRecoverTimeout, model.CodeProbeConnectionRefused, model.CodeRecoverConnectionRef,
			model.CodeProbeRejected, model.CodeRecoverRejected, model.CodeProcessInterrupted,
		},
		"fixed_point_scale": fixed.Scale,
		"rounding_mode":     fixed.RoundingModeHalfEven,
		"sample_count":      len(samples.Catalog()),
	})
}

func (s *Server) state(writer http.ResponseWriter, _ *http.Request) {
	writeSuccess(writer, http.StatusOK, s.engine.State())
}

func (s *Server) versions(writer http.ResponseWriter, request *http.Request) {
	limit := intQuery(request, "limit", 50)
	items, err := s.engine.ListSnapshots(request.Context(), limit)
	if err != nil {
		writeError(writer, internalError(err))
		return
	}
	writeSuccess(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) impactQuery(writer http.ResponseWriter, request *http.Request) {
	var origins []string
	for _, value := range request.URL.Query()["component"] {
		origins = append(origins, splitCSV(value)...)
	}
	origins = append(origins, splitCSV(request.URL.Query().Get("components"))...)
	s.writeImpact(writer, origins)
}

func (s *Server) impactPath(writer http.ResponseWriter, request *http.Request) {
	s.writeImpact(writer, []string{request.PathValue("component")})
}

func (s *Server) writeImpact(writer http.ResponseWriter, origins []string) {
	if len(origins) == 0 {
		writeError(writer, model.NewError(model.CodeInvalidRequest, "at least one component is required", 422))
		return
	}
	state := s.engine.State()
	results := make([]graph.ImpactResult, 0)
	missing := make([]string, 0)
	for _, origin := range origins {
		impact, ok := state.Derivation.Impacts[origin]
		if !ok {
			missing = append(missing, origin)
			continue
		}
		results = append(results, impact)
	}
	if len(missing) > 0 {
		writeError(writer, model.NewError(model.CodeSpecInvalid, "impact query references unknown components", 422).
			WithContext("missing_components", missing))
		return
	}
	writeSuccess(writer, http.StatusOK, map[string]any{
		"version": state.Version, "impacts": results, "graph_digest": state.Derivation.GraphDigest,
	})
}

func (s *Server) consistency(writer http.ResponseWriter, request *http.Request) {
	state := s.engine.State()
	snapshot := model.Snapshot{
		Version: state.Version, GeneratedAt: state.GeneratedAt, ChecksumSHA256: state.ChecksumSHA256,
		Components: state.Components, Edges: state.Edges, Observations: state.Observations,
		ConsistencyConfig: state.ConsistencyConfig,
	}
	repeats := intQuery(request, "repeat", 1)
	if repeats < 1 || repeats > 10 {
		repeats = 1
	}
	digests := make([]string, 0, repeats)
	var result fixed.Result
	reproducible := true
	for i := 0; i < repeats; i++ {
		result = fixed.Compute(snapshot)
		digests = append(digests, result.Digest)
		if i > 0 && digests[i] != digests[0] {
			reproducible = false
		}
	}
	writeSuccess(writer, http.StatusOK, map[string]any{
		"version": state.Version, "result": result, "reproducible": reproducible, "digests": digests,
	})
}

func (s *Server) commitSnapshot(writer http.ResponseWriter, request *http.Request) {
	body, business := readBody(writer, request, 8<<20)
	if business != nil {
		writeError(writer, business)
		return
	}
	key := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	var snapshot model.Snapshot
	if key != "" {
		parsed, err := spec.DecodeSnapshot(body)
		if err != nil {
			writeError(writer, err)
			return
		}
		snapshot = parsed
	} else {
		var payload model.FullSnapshotRequest
		if err := decodeJSON(body, &payload); err != nil {
			writeError(writer, model.NewError(model.CodeInvalidRequest, "snapshot body is not valid", 422).WithContext("cause", err.Error()))
			return
		}
		key = strings.TrimSpace(payload.IdempotencyKey)
		snapshot = payload.Snapshot
	}
	result, business := s.engine.CommitSnapshot(request.Context(), key, snapshot, s.commitOptions(writer, request))
	if business != nil {
		writeError(writer, business)
		return
	}
	writeSuccess(writer, http.StatusOK, result)
}

func (s *Server) commitIncrement(writer http.ResponseWriter, request *http.Request) {
	body, business := readBody(writer, request, 4<<20)
	if business != nil {
		writeError(writer, business)
		return
	}
	increment, business := spec.DecodeIncrement(body)
	if business != nil {
		writeError(writer, business)
		return
	}
	if strings.TrimSpace(increment.IdempotencyKey) == "" {
		increment.IdempotencyKey = strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	}
	result, business := s.engine.CommitIncrement(request.Context(), increment, s.commitOptions(writer, request))
	if business != nil {
		writeError(writer, business)
		return
	}
	writeSuccess(writer, http.StatusOK, result)
}

func (s *Server) executeDrill(writer http.ResponseWriter, request *http.Request) {
	body, business := readBody(writer, request, 2<<20)
	if business != nil {
		writeError(writer, business)
		return
	}
	drill, business := spec.DecodeDrill(body)
	if business != nil {
		writeError(writer, business)
		return
	}
	if strings.TrimSpace(drill.IdempotencyKey) == "" {
		drill.IdempotencyKey = strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	}
	result, business := s.engine.ExecuteDrill(request.Context(), drill, s.commitOptions(writer, request))
	if business != nil {
		writeError(writer, business)
		return
	}
	writeSuccess(writer, http.StatusOK, result)
}

func (s *Server) listDrills(writer http.ResponseWriter, request *http.Request) {
	items, err := s.engine.ListDrills(request.Context(), intQuery(request, "limit", 50))
	if err != nil {
		writeError(writer, internalError(err))
		return
	}
	writeSuccess(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) getDrill(writer http.ResponseWriter, request *http.Request) {
	item, err := s.engine.DrillByID(request.Context(), request.PathValue("id"))
	if errors.Is(err, sql.ErrNoRows) {
		writeError(writer, model.NewError(model.CodeNotFound, "drill was not found", 404))
		return
	}
	if err != nil {
		writeError(writer, internalError(err))
		return
	}
	writeSuccess(writer, http.StatusOK, item)
}

func (s *Server) listIntents(writer http.ResponseWriter, request *http.Request) {
	items, err := s.engine.ListIntents(request.Context(), request.URL.Query().Get("state"), intQuery(request, "limit", 100))
	if err != nil {
		writeError(writer, internalError(err))
		return
	}
	writeSuccess(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) recoveryReports(writer http.ResponseWriter, request *http.Request) {
	items, err := s.engine.ListRecoveryReports(request.Context(), intQuery(request, "limit", 20))
	if err != nil {
		writeError(writer, internalError(err))
		return
	}
	writeSuccess(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) lineage(writer http.ResponseWriter, request *http.Request) {
	version := int64(intQuery(request, "version", 0))
	refs, err := s.engine.Lineage(request.Context(), version)
	if err != nil {
		writeError(writer, internalError(err))
		return
	}
	writeSuccess(writer, http.StatusOK, map[string]any{"version": version, "refs": refs, "count": len(refs)})
}

func (s *Server) rebuild(writer http.ResponseWriter, request *http.Request) {
	version := int64(intQuery(request, "version", 0))
	payload, err := s.engine.Rebuild(request.Context(), version)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(writer, model.NewError(model.CodeNotFound, "derived rebuild record was not found", 404))
		return
	}
	if err != nil {
		writeError(writer, internalError(err))
		return
	}
	writeSuccess(writer, http.StatusOK, payload)
}

func (s *Server) reservations(writer http.ResponseWriter, _ *http.Request) {
	writeSuccess(writer, http.StatusOK, s.engine.ReservationSummary())
}

func (s *Server) listSamples(writer http.ResponseWriter, _ *http.Request) {
	writeSuccess(writer, http.StatusOK, map[string]any{"items": samples.Catalog()})
}

func (s *Server) rawSample(writer http.ResponseWriter, request *http.Request) {
	data, err := samples.Read(request.URL.Query().Get("name"))
	if err != nil {
		writeError(writer, model.NewError(model.CodeNotFound, "sample was not found", 404))
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(data)
}

func (s *Server) validateSample(writer http.ResponseWriter, request *http.Request) {
	var payload struct {
		Name string `json:"name"`
	}
	body, business := readBody(writer, request, 1<<20)
	if business != nil {
		writeError(writer, business)
		return
	}
	if err := decodeJSON(body, &payload); err != nil {
		writeError(writer, model.NewError(model.CodeInvalidRequest, "sample validation body is invalid", 422))
		return
	}
	data, err := samples.Read(payload.Name)
	if err != nil {
		writeError(writer, model.NewError(model.CodeNotFound, "sample was not found", 404))
		return
	}
	info, found := samples.Find(payload.Name)
	if !found {
		writeError(writer, model.NewError(model.CodeNotFound, "sample metadata was not found", 404))
		return
	}
	switch info.Kind {
	case "snapshot", "invalid":
		s.validateSnapshotSample(writer, payload.Name, data)
	case "increment":
		s.validateIncrementSample(writer, payload.Name, data)
	case "drill":
		s.validateDrillSample(writer, payload.Name, data)
	case "component-list", "edge-list":
		var generic any
		if err := decodeJSON(data, &generic); err != nil {
			writeError(writer, model.NewError(model.CodeSpecInvalid, "sample JSON is not valid", 422).
				WithContext("sample", payload.Name).WithContext("cause", err.Error()))
			return
		}
		writeSuccess(writer, http.StatusOK, map[string]any{
			"name": payload.Name, "kind": info.Kind, "valid": true,
		})
	default:
		writeError(writer, model.NewError(model.CodeInternal, "sample metadata has an unsupported kind", 500).
			WithContext("kind", info.Kind))
	}
}

func (s *Server) validateSnapshotSample(writer http.ResponseWriter, name string, data []byte) {
	snapshot, business := spec.DecodeSnapshot(data)
	if business != nil {
		writeError(writer, business)
		return
	}
	normalized, business := spec.NormalizeSnapshot(snapshot, true)
	if business != nil {
		writeError(writer, business)
		return
	}
	analysis := graph.Analyze(normalized.Snapshot)
	consistency := fixed.Compute(normalized.Snapshot)
	dataOut := map[string]any{
		"name": name, "kind": "snapshot", "version": normalized.Snapshot.Version,
		"checksum_sha256":      normalized.Snapshot.ChecksumSHA256,
		"duplicate_components": normalized.Duplicates,
		"diagnostics":          analysis.Diagnostics, "has_cycle": analysis.HasCycle,
		"consistency": consistency, "graph_digest": analysis.GraphDigest,
	}
	if analysis.HasCycle {
		business := model.NewError(model.CodeDependencyCycle, "sample dependency graph contains a cycle", 422).
			WithContext("cycles", analysis.Cycles)
		business.Partial = dataOut
		writeError(writer, business)
		return
	}
	writeSuccess(writer, http.StatusOK, dataOut)
}

func (s *Server) validateIncrementSample(writer http.ResponseWriter, name string, data []byte) {
	increment, business := spec.DecodeIncrement(data)
	if business != nil {
		writeError(writer, business)
		return
	}
	state := s.engine.State()
	base := model.Snapshot{
		Version: state.Version, GeneratedAt: state.GeneratedAt, ChecksumSHA256: state.ChecksumSHA256,
		Components: state.Components, Edges: state.Edges, Observations: state.Observations,
		ConsistencyConfig: state.ConsistencyConfig,
	}
	if increment.BaseVersion != base.Version {
		writeError(writer, model.NewError(model.CodeStaleBase, "sample base_version is not the current committed version", 409).
			WithContext("sample", name).
			WithContext("base_version", increment.BaseVersion).
			WithContext("current_version", base.Version))
		return
	}
	candidate, changedRefs, business := spec.ApplyIncrement(base, increment, base.Version+1)
	if business != nil {
		writeError(writer, business)
		return
	}
	analysis := graph.Analyze(candidate)
	consistency := fixed.Compute(candidate)
	dataOut := map[string]any{
		"name": name, "kind": "increment", "version": candidate.Version,
		"base_version": base.Version, "changed_refs": changedRefs,
		"diagnostics": analysis.Diagnostics, "has_cycle": analysis.HasCycle,
		"consistency": consistency, "graph_digest": analysis.GraphDigest,
		"topology_batches": analysis.TopologyBatches, "batch_groups": analysis.BatchGroups,
		"blocked": analysis.Blocked, "cycles": analysis.Cycles,
	}
	if analysis.HasCycle {
		business := model.NewError(model.CodeDependencyCycle, "sample dependency graph contains a cycle", 422).
			WithContext("sample", name).WithContext("cycles", analysis.Cycles).
			WithContext("blocked_components", sortedBlocked(analysis.Blocked))
		business.Partial = dataOut
		writeError(writer, business)
		return
	}
	dataOut["candidate_checksum_sha256"] = candidate.ChecksumSHA256
	writeSuccess(writer, http.StatusOK, dataOut)
}

func (s *Server) validateDrillSample(writer http.ResponseWriter, name string, data []byte) {
	drill, business := spec.DecodeDrill(data)
	if business != nil {
		writeError(writer, business)
		return
	}
	state := s.engine.State()
	if drill.Version != state.Version {
		writeError(writer, model.NewError(model.CodeVersionMismatch, "sample drill version is not the current committed version", 409).
			WithContext("sample", name).WithContext("requested_version", drill.Version).
			WithContext("current_version", state.Version))
		return
	}
	impacts, missing := state.Derivation.Impacts, make([]string, 0)
	if len(drill.FailedComponents) == 0 {
		writeError(writer, model.NewError(model.CodeSpecInvalid, "failed_components must not be empty", 422))
		return
	}
	selected := make([]graph.ImpactResult, 0, len(drill.FailedComponents))
	for _, component := range drill.FailedComponents {
		impact, ok := impacts[component]
		if !ok {
			missing = append(missing, component)
			continue
		}
		selected = append(selected, impact)
	}
	if len(missing) > 0 {
		writeError(writer, model.NewError(model.CodeSpecInvalid, "sample drill references unknown components", 422).
			WithContext("sample", name).WithContext("missing_components", missing))
		return
	}
	writeSuccess(writer, http.StatusOK, map[string]any{
		"name": name, "kind": "drill", "version": state.Version,
		"failed_components": drill.FailedComponents, "impacts": selected,
		"max_concurrency": drill.MaxConcurrency,
	})
}

func sortedBlocked(blocked map[string]graph.Block) []string {
	out := make([]string, 0, len(blocked))
	for component := range blocked {
		out = append(out, component)
	}
	sort.Strings(out)
	return out
}

func (s *Server) loadSample(writer http.ResponseWriter, request *http.Request) {
	var payload struct {
		Name           string `json:"name"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	body, business := readBody(writer, request, 1<<20)
	if business != nil {
		writeError(writer, business)
		return
	}
	if err := decodeJSON(body, &payload); err != nil {
		writeError(writer, model.NewError(model.CodeInvalidRequest, "sample load body is invalid", 422))
		return
	}
	data, err := samples.Read(payload.Name)
	if err != nil {
		writeError(writer, model.NewError(model.CodeNotFound, "sample was not found", 404))
		return
	}
	snapshot, business := spec.DecodeSnapshot(data)
	if business != nil {
		writeError(writer, business)
		return
	}
	key := payload.IdempotencyKey
	if key == "" {
		key = "sample-load:" + payload.Name + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	result, business := s.engine.CommitSnapshot(request.Context(), key, snapshot, s.commitOptions(writer, request))
	if business != nil {
		writeError(writer, business)
		return
	}
	writeSuccess(writer, http.StatusOK, result)
}

func (s *Server) commitOptions(writer http.ResponseWriter, request *http.Request) engine.CommitOptions {
	if !s.testHooks {
		return engine.CommitOptions{}
	}
	raw := strings.TrimSpace(request.Header.Get("X-Test-Hold-After-Intent-Ms"))
	if raw == "" {
		return engine.CommitOptions{}
	}
	milliseconds, err := strconv.Atoi(raw)
	if err != nil || milliseconds < 0 || milliseconds > 60_000 {
		return engine.CommitOptions{}
	}
	return engine.CommitOptions{HoldAfterIntent: time.Duration(milliseconds) * time.Millisecond}
}

func readBody(writer http.ResponseWriter, request *http.Request, limit int64) ([]byte, *model.BusinessError) {
	reader := http.MaxBytesReader(writer, request.Body, limit)
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, model.NewError(model.CodeInvalidRequest, "request body could not be read", 413).WithContext("cause", err.Error())
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil, model.NewError(model.CodeInvalidRequest, "request body is required", 422)
	}
	return body, nil
}

func decodeJSON(data []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("unexpected trailing JSON value")
	}
	return nil
}

func writeSuccess(writer http.ResponseWriter, status int, data any) {
	writeJSON(writer, status, map[string]any{"ok": true, "data": data})
}

func writeError(writer http.ResponseWriter, business *model.BusinessError) {
	if business == nil {
		business = model.NewError(model.CodeInternal, "unknown internal error", 500)
	}
	status := business.Status
	if status == 0 {
		status = 500
	}
	payload := map[string]any{
		"ok":    false,
		"error": errorBody{Code: business.Code, Message: business.Message, Context: business.Context},
	}
	if business.Partial != nil {
		payload["partial_result"] = business.Partial
	}
	writeJSON(writer, status, payload)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func internalError(err error) *model.BusinessError {
	return model.NewError(model.CodeInternal, "internal operation failed", 500).WithContext("cause", err.Error())
}

func intQuery(request *http.Request, key string, fallback int) int {
	raw := request.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}
