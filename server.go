package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"
)

type HTTPServer struct {
	workbench *Workbench
	mux       *http.ServeMux
}

type okResponse struct {
	OK   bool `json:"ok"`
	Data any  `json:"data,omitempty"`
}

type errorResponse struct {
	OK    bool   `json:"ok"`
	Error *Error `json:"error"`
}

type CrashRequest struct {
	CommitID         string             `json:"commit_id,omitempty"`
	RouteKey         string             `json:"route_key,omitempty"`
	StagedEntryCount int                `json:"staged_entry_count,omitempty"`
	Migration        *MigrationFragment `json:"migration,omitempty"`
	KillDelayMS      int64              `json:"kill_delay_ms,omitempty"`
	NoKill           bool               `json:"no_kill,omitempty"`
}

type CrashPrimeResult struct {
	CommitID      string   `json:"commit_id"`
	IntentIDs     []string `json:"intent_ids"`
	StagedEntries int      `json:"staged_entries"`
	Message       string   `json:"message"`
	KillScheduled bool     `json:"kill_scheduled"`
}

func NewHTTPServer(workbench *Workbench) *HTTPServer {
	server := &HTTPServer{workbench: workbench, mux: http.NewServeMux()}
	server.routes()
	return server
}

func (s *HTTPServer) Handler() http.Handler { return s.mux }

func (s *HTTPServer) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /v1/routes", s.handleListRoutes)
	s.mux.HandleFunc("POST /v1/entries", s.handleEntries)
	s.mux.HandleFunc("POST /v1/snapshots", s.handleSnapshot)
	s.mux.HandleFunc("POST /v1/migrations", s.handleMigration)
	s.mux.HandleFunc("POST /v1/rebuild", s.handleRebuild)
	s.mux.HandleFunc("GET /v1/ledger/{route_key}", s.handleLedger)
	s.mux.HandleFunc("GET /v1/conservation/{route_key}", s.handleConservation)
	s.mux.HandleFunc("POST /v1/probes/single", s.handleProbeSingle)
	s.mux.HandleFunc("POST /v1/probes/round", s.handleProbeRound)
	s.mux.HandleFunc("GET /v1/probes/batches/{batch_id}", s.handleProbeBatch)
	s.mux.HandleFunc("GET /v1/recovery", s.handleRecovery)
	s.mux.HandleFunc("POST /v1/admin/crash", s.handleCrash)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeOK(w http.ResponseWriter, value any) {
	writeJSON(w, http.StatusOK, okResponse{OK: true, Data: value})
}

func writeBusinessError(w http.ResponseWriter, err *Error) {
	writeJSON(w, err.HTTPStatus(), errorResponse{OK: false, Error: err})
}

func decodeRequest(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20))
	if err := decoder.Decode(target); err != nil {
		bizErr := NewError(CodeLedgerInvalid, "invalid JSON request body: "+err.Error())
		writeBusinessError(w, bizErr)
		return false
	}
	return true
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	version, _ := s.workbench.store.ViewVersion(r.Context())
	snapshotVersion, _ := s.workbench.store.SnapshotVersion(r.Context())
	writeOK(w, map[string]any{
		"service": "weight-ledger-workbench", "time_ms": s.workbench.NowMS(),
		"view_version": version, "snapshot_version": snapshotVersion,
	})
}

func (s *HTTPServer) handleListRoutes(w http.ResponseWriter, r *http.Request) {
	routes, version, err := s.workbench.ListRoutes(r.Context())
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeOK(w, map[string]any{"routes": routes, "view_version": version})
}

func (s *HTTPServer) handleEntries(w http.ResponseWriter, r *http.Request) {
	var batch EntryBatch
	if !decodeRequest(w, r, &batch) {
		return
	}
	if batch.CommitID == "" {
		batch.CommitID = r.Header.Get("Idempotency-Key")
	}
	result, err := s.workbench.SubmitEntryBatch(r.Context(), batch)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeOK(w, result)
}

func (s *HTTPServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	var snapshot WeightSnapshot
	if !decodeRequest(w, r, &snapshot) {
		return
	}
	if snapshot.CommitID == "" {
		snapshot.CommitID = r.Header.Get("Idempotency-Key")
	}
	result, err := s.workbench.SubmitSnapshot(r.Context(), snapshot)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeOK(w, result)
}

func (s *HTTPServer) handleMigration(w http.ResponseWriter, r *http.Request) {
	var fragment MigrationFragment
	if !decodeRequest(w, r, &fragment) {
		return
	}
	if fragment.CommitID == "" {
		fragment.CommitID = r.Header.Get("Idempotency-Key")
	}
	result, err := s.workbench.SubmitMigration(r.Context(), fragment)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeOK(w, result)
}

func (s *HTTPServer) handleRebuild(w http.ResponseWriter, r *http.Request) {
	var request RebuildRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.CommitID == "" {
		request.CommitID = r.Header.Get("Idempotency-Key")
	}
	result, err := s.workbench.Rebuild(r.Context(), request)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeOK(w, result)
}

func parseInt64Query(r *http.Request, key string, fallback int64) (int64, *Error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, NewError(CodeLedgerInvalid, "invalid integer query parameter "+key).WithContext("value", value)
	}
	return parsed, nil
}

func (s *HTTPServer) handleLedger(w http.ResponseWriter, r *http.Request) {
	routeKey := r.PathValue("route_key")
	observed, err := parseInt64Query(r, "observe_version", 0)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	view, bizErr := s.workbench.GetLedger(r.Context(), routeKey, observed)
	if bizErr != nil {
		writeBusinessError(w, bizErr)
		return
	}
	writeOK(w, view)
}

func (s *HTTPServer) handleConservation(w http.ResponseWriter, r *http.Request) {
	routeKey := r.PathValue("route_key")
	observed, err := parseInt64Query(r, "observe_version", 0)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	asOf, err := parseInt64Query(r, "as_of_ms", 0)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	tolerance := r.URL.Query().Get("tolerance")
	strict := r.URL.Query().Get("strict") == "1" || r.URL.Query().Get("strict") == "true"
	var view *ConservationView
	var bizErr *Error
	if strict {
		view, bizErr = s.workbench.VerifyConservation(r.Context(), routeKey, observed, asOf, tolerance)
	} else {
		view, bizErr = s.workbench.GetConservation(r.Context(), routeKey, observed, asOf, tolerance)
	}
	if bizErr != nil {
		if view != nil {
			if bizErr.Context == nil {
				bizErr.Context = map[string]any{}
			}
			bizErr.Context["view"] = view
		}
		writeBusinessError(w, bizErr)
		return
	}
	writeOK(w, view)
}

func (s *HTTPServer) handleProbeSingle(w http.ResponseWriter, r *http.Request) {
	var request ProbeRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	roundRequest := ProbeRoundRequest{
		CommitID: request.BatchID, BatchID: request.BatchID, RouteKeys: []string{request.RouteKey},
		BaseURL: request.BaseURL, Endpoints: map[string]string{request.RouteKey: request.Endpoint},
		TimeoutMS: request.TimeoutMS, LateThresholdMS: request.LateThresholdMS,
		ExpectedOwners: map[string]string{request.RouteKey: request.ExpectedOwner},
		Injection:      request.Injection,
	}
	if request.Endpoint != "" {
		roundRequest.Endpoints[request.RouteKey] = request.Endpoint
	}
	round, bizErr := s.workbench.SubmitProbeRound(r.Context(), roundRequest)
	if bizErr != nil {
		writeBusinessError(w, bizErr)
		return
	}
	result := round.Results[0]
	if result.Code == ProbeOutcomeOK {
		writeOK(w, result)
		return
	}
	writeBusinessError(w, probeResultError(result).WithContext("probe_round", round))
}

func (s *HTTPServer) handleProbeRound(w http.ResponseWriter, r *http.Request) {
	var request ProbeRoundRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	result, err := s.workbench.SubmitProbeRound(r.Context(), request)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeOK(w, result)
}

func (s *HTTPServer) handleProbeBatch(w http.ResponseWriter, r *http.Request) {
	batchID := r.PathValue("batch_id")
	var status string
	var viewVersion, snapshotVersion, total, completed int64
	var configJSON string
	err := s.workbench.db.QueryRowContext(r.Context(), `
		SELECT status,view_version,snapshot_version,total_routes,completed_routes,config_json
		FROM probe_batches WHERE batch_id=?`, batchID).
		Scan(&status, &viewVersion, &snapshotVersion, &total, &completed, &configJSON)
	if errors.Is(err, http.ErrNoCookie) || isNoRows(err) {
		writeBusinessError(w, NewError(CodeNotFound, "probe batch not found").WithContext("batch_id", batchID))
		return
	}
	if err != nil {
		writeBusinessError(w, WrapError(CodeInternal, err))
		return
	}
	rows, err := s.workbench.db.QueryContext(r.Context(), `
		SELECT route_key,endpoint,observed_owner,outcome_code,http_status,elapsed_ms,timed_out,
			conn_closed,slot_released,detail_json,is_current
		FROM probe_results WHERE batch_id=? ORDER BY route_key`, batchID)
	if err != nil {
		writeBusinessError(w, WrapError(CodeInternal, err))
		return
	}
	defer rows.Close()
	results := []map[string]any{}
	for rows.Next() {
		var route, endpoint, observed, code, detail string
		var httpStatus, elapsed, timedOut, connClosed, slotReleased, current int
		if err := rows.Scan(&route, &endpoint, &observed, &code, &httpStatus, &elapsed, &timedOut,
			&connClosed, &slotReleased, &detail, &current); err != nil {
			writeBusinessError(w, WrapError(CodeInternal, err))
			return
		}
		var detailValue any
		_ = json.Unmarshal([]byte(detail), &detailValue)
		results = append(results, map[string]any{
			"route_key": route, "endpoint": endpoint, "observed_owner_version": observed,
			"code": code, "http_status": httpStatus, "elapsed_ms": elapsed,
			"timed_out": timedOut == 1, "connection_closed": connClosed == 1,
			"slot_released": slotReleased == 1, "is_current": current == 1, "detail": detailValue,
		})
	}
	writeOK(w, map[string]any{
		"batch_id": batchID, "status": status, "view_version": viewVersion,
		"snapshot_version": snapshotVersion, "total_routes": total,
		"completed_routes": completed, "results": results,
	})
}

func (s *HTTPServer) handleRecovery(w http.ResponseWriter, r *http.Request) {
	report, err := s.workbench.store.LatestRecoveryReport(r.Context())
	if err != nil {
		writeBusinessError(w, WrapError(CodeInternal, err))
		return
	}
	if report == nil {
		report = s.workbench.recovery
	}
	writeOK(w, report)
}

func (s *HTTPServer) handleCrash(w http.ResponseWriter, r *http.Request) {
	var request CrashRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	result, bizErr := s.workbench.PrimeCrash(r.Context(), request)
	if bizErr != nil {
		writeBusinessError(w, bizErr)
		return
	}
	writeOK(w, result)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	if !request.NoKill {
		delay := time.Duration(request.KillDelayMS) * time.Millisecond
		if request.KillDelayMS <= 0 {
			delay = 250 * time.Millisecond
		}
		go func() {
			time.Sleep(delay)
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		}()
	}
}

func (w *Workbench) PrimeCrash(ctx context.Context, request CrashRequest) (*CrashPrimeResult, *Error) {
	commitID := request.CommitID
	if commitID == "" {
		commitID = randomSuffix("crash-")
	}
	routeKey := request.RouteKey
	if routeKey == "" {
		routeKey = "route-crash"
	}
	count := request.StagedEntryCount
	if count <= 0 {
		count = 3
	}
	nowMS := w.NowMS()
	intentEntries := "intent:crash-entries:" + commitID
	intentMigration := "intent:crash-migration:" + commitID
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO intents(intent_id,commit_id,kind,attempt,payload_json,status,created_at_ms,updated_at_ms)
		VALUES(?,?, 'entries',1,?,'started',?,?)`,
		intentEntries, commitID+"-entries", `{"crash":"staged_entries"}`, nowMS, nowMS)
	if err != nil {
		_ = tx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	for i := 0; i < count; i++ {
		weightsJSON := `{"stable":1000,"canary":0}`
		candidatesJSON := `["canary","stable"]`
		_, err = tx.ExecContext(ctx, `
			INSERT INTO ledger_entries(
				intent_id,commit_id,seq,timestamp_ms,route_key,client_id,hit_version,
				candidates_json,weights_json,request_weight,state,view_version)
			VALUES(?,?,?,?,?,?,?,?,?,?, 'staged',0)`,
			intentEntries, commitID+"-entries", i, nowMS+int64(i), routeKey,
			fmt.Sprintf("crash-client-%d", i), "stable", candidatesJSON, weightsJSON, 100*(i+1))
		if err != nil {
			_ = tx.Rollback()
			return nil, WrapError(CodeInternal, err)
		}
	}
	migration := request.Migration
	if migration == nil {
		migration = &MigrationFragment{
			MigrationID: "mig-crash-" + commitID, BaseVersion: 0, RouteKey: routeKey,
			FromVersion: "stable", ToVersion: "canary", RatioPermille: 250,
			StartAtMS: nowMS, TransitionMS: 60_000,
		}
	}
	payload, _ := json.Marshal(migration)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO intents(intent_id,commit_id,kind,attempt,payload_json,status,created_at_ms,updated_at_ms)
		VALUES(?,?, 'migration',1,?,'started',?,?)`,
		intentMigration, commitID+"-migration", string(payload), nowMS, nowMS)
	if err != nil {
		_ = tx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	return &CrashPrimeResult{
		CommitID: commitID, IntentIDs: []string{intentEntries, intentMigration},
		StagedEntries: count, Message: "durable intents and staged entries created; process will be killed",
		KillScheduled: !request.NoKill,
	}, nil
}
