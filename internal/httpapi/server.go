package httpapi

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"local/windowengine/internal/domain"
	"local/windowengine/internal/engine"
	"local/windowengine/internal/sample"
	"local/windowengine/internal/store"
)

//go:embed assets/index.html
var assets embed.FS

type Server struct {
	engine            *engine.Engine
	store             *store.Store
	mux               *http.ServeMux
	startupRecoveryID string
}

type errorEnvelope struct {
	Error *domain.Error `json:"error"`
}

func New(engineValue *engine.Engine, database *store.Store, startupRecoveryID string) *Server {
	server := &Server{engine: engineValue, store: database, mux: http.NewServeMux(), startupRecoveryID: startupRecoveryID}
	server.mux.HandleFunc("/", server.index)
	server.mux.HandleFunc("GET /api/health", server.health)
	server.mux.HandleFunc("GET /api/sample", server.sample)
	server.mux.HandleFunc("POST /api/validate", server.validate)
	server.mux.HandleFunc("GET /api/snapshots", server.snapshots)
	server.mux.HandleFunc("GET /api/snapshot", server.snapshot)
	server.mux.HandleFunc("POST /api/snapshots", server.loadSnapshot)
	server.mux.HandleFunc("GET /api/tenants", server.tenants)
	server.mux.HandleFunc("POST /api/recalc", server.recalc)
	server.mux.HandleFunc("GET /api/aggregates", server.aggregates)
	server.mux.HandleFunc("GET /api/tenant-state", server.tenantState)
	server.mux.HandleFunc("GET /api/schedule", server.schedule)
	server.mux.HandleFunc("GET /api/rounds", server.rounds)
	server.mux.HandleFunc("GET /api/recovery", server.recovery)
	server.mux.HandleFunc("GET /api/recovery-reports", server.recoveryReports)
	server.mux.HandleFunc("GET /api/intents", server.intents)
	return server
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'")
	if request.Method == http.MethodOptions {
		writer.Header().Set("Allow", "GET, POST, OPTIONS")
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	s.mux.ServeHTTP(writer, request)
}

func (s *Server) index(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.Path != "/" {
		writeBusinessError(writer, domain.NewError(domain.CodeNotFound, "route not found", map[string]any{"path": request.URL.Path}))
		return
	}
	data, err := assets.ReadFile("assets/index.html")
	if err != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "embedded UI missing", map[string]any{"reason": err.Error()}))
		return
	}
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(data)
}

func (s *Server) health(writer http.ResponseWriter, request *http.Request) {
	counts, err := s.store.IntentCounts(request.Context())
	if err != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "intent counts failed", map[string]any{"reason": err.Error()}))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"status": "ok", "time_ms": time.Now().UnixMilli(), "sqlite": s.store.Path(),
		"startup_recovery_report_id": s.startupRecoveryID, "intent_counts": counts,
	})
}

func (s *Server) sample(writer http.ResponseWriter, request *http.Request) {
	seed := int64(242)
	if raw := request.URL.Query().Get("seed"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeBusinessError(writer, domain.NewError(domain.CodeBadRequest, "seed must be an int64", map[string]any{"seed": raw}))
			return
		}
		seed = parsed
	}
	data, err := sample.Generate(seed)
	if err != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "generate sample failed", map[string]any{"reason": err.Error()}))
		return
	}
	var snapshot any
	if err := json.Unmarshal(data, &snapshot); err != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "decode generated sample failed", map[string]any{"reason": err.Error()}))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"seed": seed, "snapshot": snapshot})
}

func (s *Server) validate(writer http.ResponseWriter, request *http.Request) {
	body, err := readBody(request)
	if err != nil {
		writeBusinessError(writer, err)
		return
	}
	snapshot, parseErr := domain.ParseSnapshot(body)
	if parseErr != nil {
		writeBusinessError(writer, parseErr)
		return
	}
	windows, planErr := domain.PlanWindows(snapshot)
	if planErr != nil {
		writeBusinessError(writer, planErr)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"valid": true, "snapshot_id": snapshot.SnapshotID, "content_hash": snapshot.ContentHash,
		"tenant_count": len(snapshot.Tenants), "spec_count": len(snapshot.WindowSpecs),
		"event_count": len(snapshot.Events), "planned_window_count": len(windows),
		"boundary":                  domain.BoundaryLeftClosedRightOpen,
		"missing_measure_policy":    "count includes event; sum/min/max and sample_size exclude missing measure",
		"duplicate_sequence_policy": "greatest timestamp, then present measure, then greatest scaled value, then greatest event_id",
	})
}

func (s *Server) snapshots(writer http.ResponseWriter, request *http.Request) {
	snapshots, err := s.store.ListSnapshots(request.Context())
	if err != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "list snapshots failed", map[string]any{"reason": err.Error()}))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"snapshots": snapshots})
}

func (s *Server) snapshot(writer http.ResponseWriter, request *http.Request) {
	snapshotID, err := s.resolveSnapshotID(request.Context(), request.URL.Query().Get("snapshot_id"))
	if err != nil {
		writeBusinessError(writer, err)
		return
	}
	record, recordErr := s.store.SnapshotRecord(request.Context(), snapshotID)
	if recordErr != nil {
		writeBusinessError(writer, recordErr)
		return
	}
	var raw json.RawMessage
	var rawText string
	if err := s.store.DB().QueryRowContext(request.Context(), `SELECT raw_json FROM snapshots WHERE snapshot_id = ?`, snapshotID).Scan(&rawText); err != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "read snapshot failed", map[string]any{"reason": err.Error()}))
		return
	}
	raw = json.RawMessage(rawText)
	writeJSON(writer, http.StatusOK, map[string]any{"record": record, "snapshot": raw})
}

func (s *Server) loadSnapshot(writer http.ResponseWriter, request *http.Request) {
	body, err := readBody(request)
	if err != nil {
		writeBusinessError(writer, err)
		return
	}
	result, snapshot, loadErr := s.engine.LoadSnapshot(request.Context(), body)
	if loadErr != nil {
		writeBusinessError(writer, loadErr)
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{
		"load": result,
		"summary": map[string]any{
			"snapshot_id": snapshot.SnapshotID, "content_hash": snapshot.ContentHash,
			"tenant_count": len(snapshot.Tenants), "spec_count": len(snapshot.WindowSpecs),
			"event_count": len(snapshot.Events),
		},
	})
}

func (s *Server) tenants(writer http.ResponseWriter, request *http.Request) {
	snapshotID, err := s.resolveSnapshotID(request.Context(), request.URL.Query().Get("snapshot_id"))
	if err != nil {
		writeBusinessError(writer, err)
		return
	}
	tenants, listErr := s.store.Tenants(request.Context(), snapshotID)
	if listErr != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "list tenants failed", map[string]any{"reason": listErr.Error()}))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"snapshot_id": snapshotID, "tenants": tenants})
}

func (s *Server) recalc(writer http.ResponseWriter, request *http.Request) {
	body, err := readBody(request)
	if err != nil {
		writeBusinessError(writer, err)
		return
	}
	var options engine.TriggerOptions
	if err := json.Unmarshal(body, &options); err != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeBadRequest, "invalid recalculation request JSON", map[string]any{"reason": err.Error()}))
		return
	}
	if options.SnapshotID != "" {
		if _, recordErr := s.store.SnapshotRecord(request.Context(), options.SnapshotID); recordErr != nil {
			writeBusinessError(writer, recordErr)
			return
		}
	}
	result, triggerErr := s.engine.Trigger(request.Context(), options)
	if triggerErr != nil {
		if result != nil {
			if business, ok := domain.BusinessError(triggerErr); ok {
				business.Context["round_id"] = result.Round.RoundID
				business.Context["round_status"] = result.Round.Status
			}
		}
		writeBusinessError(writer, triggerErr)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (s *Server) aggregates(writer http.ResponseWriter, request *http.Request) {
	snapshotID, err := s.resolveSnapshotID(request.Context(), request.URL.Query().Get("snapshot_id"))
	if err != nil {
		writeBusinessError(writer, err)
		return
	}
	tenantID := strings.TrimSpace(request.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		writeBusinessError(writer, domain.NewError(domain.CodeBadRequest, "tenant_id is required", nil))
		return
	}
	expectedVersion, versionErr := store.ParseExpectedVersion(request.URL.Query().Get("version"))
	if versionErr != nil {
		writeBusinessError(writer, versionErr)
		return
	}
	result, queryErr := s.store.QueryAggregates(
		request.Context(), snapshotID, tenantID, request.URL.Query().Get("window_id"),
		request.URL.Query().Get("spec_id"), request.URL.Query().Get("group_key"), expectedVersion,
	)
	if queryErr != nil {
		writeBusinessError(writer, queryErr)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (s *Server) tenantState(writer http.ResponseWriter, request *http.Request) {
	snapshotID, err := s.resolveSnapshotID(request.Context(), request.URL.Query().Get("snapshot_id"))
	if err != nil {
		writeBusinessError(writer, err)
		return
	}
	tenantID := strings.TrimSpace(request.URL.Query().Get("tenant_id"))
	if tenantID == "" {
		writeBusinessError(writer, domain.NewError(domain.CodeBadRequest, "tenant_id is required", nil))
		return
	}
	expectedVersion, versionErr := store.ParseExpectedVersion(request.URL.Query().Get("version"))
	if versionErr != nil {
		writeBusinessError(writer, versionErr)
		return
	}
	state, stateErr := s.store.TenantStateVersioned(request.Context(), snapshotID, tenantID, expectedVersion)
	if stateErr != nil {
		writeBusinessError(writer, stateErr)
		return
	}
	writeJSON(writer, http.StatusOK, state)
}

func (s *Server) schedule(writer http.ResponseWriter, request *http.Request) {
	roundID := request.URL.Query().Get("round_id")
	round, err := s.store.Round(request.Context(), roundID)
	if err != nil {
		writeBusinessError(writer, asBusiness(err))
		return
	}
	tasks, taskErr := s.store.TasksForRound(request.Context(), round.RoundID)
	if taskErr != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "load round tasks failed", map[string]any{"reason": taskErr.Error()}))
		return
	}
	trace, traceErr := s.store.Trace(request.Context(), round.RoundID)
	if traceErr != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "load scheduler trace failed", map[string]any{"reason": traceErr.Error()}))
		return
	}
	decisions, decisionErr := s.store.ShedDecisions(request.Context(), round.RoundID)
	if decisionErr != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "load shed decisions failed", map[string]any{"reason": decisionErr.Error()}))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"round": round, "tasks": tasks, "trace": trace, "shed_decisions": decisions,
	})
}

func (s *Server) rounds(writer http.ResponseWriter, request *http.Request) {
	limit, _ := strconv.Atoi(request.URL.Query().Get("limit"))
	rounds, err := s.store.Rounds(request.Context(), limit)
	if err != nil {
		writeBusinessError(writer, asBusiness(err))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"rounds": rounds})
}

func (s *Server) recovery(writer http.ResponseWriter, request *http.Request) {
	report, err := s.store.RecoveryReport(request.Context(), request.URL.Query().Get("report_id"))
	if err != nil {
		writeBusinessError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, report)
}

func (s *Server) recoveryReports(writer http.ResponseWriter, request *http.Request) {
	limit, _ := strconv.Atoi(request.URL.Query().Get("limit"))
	reports, err := s.store.RecoveryReports(request.Context(), limit)
	if err != nil {
		writeBusinessError(writer, asBusiness(err))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"reports": reports})
}

func (s *Server) intents(writer http.ResponseWriter, request *http.Request) {
	intents, err := s.store.ListIntents(request.Context(), request.URL.Query().Get("status"))
	if err != nil {
		writeBusinessError(writer, domain.NewError(domain.CodeInternal, "list intents failed", map[string]any{"reason": err.Error()}))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"intents": intents})
}

func (s *Server) resolveSnapshotID(ctx context.Context, requested string) (string, *domain.Error) {
	if strings.TrimSpace(requested) != "" {
		if record, err := s.store.SnapshotRecord(ctx, requested); err != nil {
			return "", err
		} else {
			return record.SnapshotID, nil
		}
	}
	return s.store.LatestSnapshotID(ctx)
}

func readBody(request *http.Request) ([]byte, *domain.Error) {
	limited := io.LimitReader(request.Body, 16<<20)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, domain.NewError(domain.CodeBadRequest, "read request body failed", map[string]any{"reason": err.Error()})
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, domain.NewError(domain.CodeBadRequest, "request body is required", nil)
	}
	return body, nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func writeBusinessError(writer http.ResponseWriter, err error) {
	business := asBusiness(err)
	writeJSON(writer, statusForCode(business.Code), errorEnvelope{Error: business})
}

func asBusiness(err error) *domain.Error {
	var business *domain.Error
	if errors.As(err, &business) {
		return business
	}
	return domain.NewError(domain.CodeInternal, err.Error(), nil)
}

func statusForCode(code domain.Code) int {
	switch code {
	case domain.CodeBadRequest:
		return http.StatusBadRequest
	case domain.CodeNotFound:
		return http.StatusNotFound
	case domain.CodeRecalcQueueFull:
		return http.StatusTooManyRequests
	case domain.CodeStaleVersion, domain.CodeInvalidTransition:
		return http.StatusConflict
	case domain.CodeSnapshotInvalid, domain.CodeNumericOverflow, domain.CodeZeroDurationWindow, domain.CodeWindowRange:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}
