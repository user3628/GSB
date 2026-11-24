package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"columnar-executor/internal/model"
	"columnar-executor/internal/service"
)

type API struct {
	service *service.Service
}

func New(svc *service.Service) http.Handler {
	api := &API{service: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", api.health)
	mux.HandleFunc("GET /api/v1/meta", api.meta)
	mux.HandleFunc("POST /api/v1/snapshots/generate", api.generateSnapshot)
	mux.HandleFunc("POST /api/v1/snapshots/load", api.loadSnapshot)
	mux.HandleFunc("GET /api/v1/snapshots/current", api.currentSnapshot)
	mux.HandleFunc("GET /api/v1/snapshots/{version}", api.getSnapshot)
	mux.HandleFunc("POST /api/v1/chains", api.submitChain)
	mux.HandleFunc("GET /api/v1/chains/{id}/versions/{version}", api.getChain)
	mux.HandleFunc("POST /api/v1/chains/{id}/execute", api.execute)
	mux.HandleFunc("GET /api/v1/chains/{id}/versions/{version}/executions", api.listExecutions)
	mux.HandleFunc("GET /api/v1/chains/{id}/versions/{version}/batches", api.batches)
	mux.HandleFunc("GET /api/v1/chains/{id}/versions/{version}/aggregates", api.aggregates)
	mux.HandleFunc("GET /api/v1/executions/{id}", api.getExecution)
	mux.HandleFunc("GET /api/v1/executions/{id}/compare", api.compare)
	mux.HandleFunc("GET /api/v1/recovery/reports", api.recoveryReports)
	mux.HandleFunc("GET /api/v1/faults", api.listFaults)
	mux.HandleFunc("POST /api/v1/faults/probe", api.probeFault)
	mux.HandleFunc("/", api.notFound)
	return mux
}

func (a *API) notFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, model.NewBizError(model.CodeNotFound, "route not found").
		With("method", r.Method).With("path", r.URL.Path))
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": model.NowUTC()})
}

func (a *API) meta(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"fixed_scale":           model.FixedScale,
		"bitmap_forms":          []string{model.FormAll, model.FormNone, model.FormSparse, model.FormDense},
		"paths":                 []string{model.PathSmall, model.PathLarge},
		"batch_statuses":        []string{model.BatchPending, model.BatchProvisional, model.BatchCommitted},
		"execution_statuses":    []string{model.StatusRunning, model.StatusCommitted, model.StatusFailed, model.StatusAborted, model.StatusRecovered},
		"error_codes":           []string{model.CodeSnapshotInvalid, model.CodeBitmapMismatch, model.CodeNumericOverflow, model.CodeDivideByZero, model.CodeDictIndex, model.CodeExecAborted, model.CodeStaleVersion},
		"fault_kinds":           []string{model.FaultBitmapMismatch, model.FaultFixedOverflow, model.FaultDictIndex, model.FaultDivideByZero, model.FaultExecAborted},
		"exactly_one_aggregate": true,
		"fixed_rounding":        "half_away_from_zero",
		"read_visibility":       "committed execution versions only",
	})
}

func (a *API) generateSnapshot(w http.ResponseWriter, r *http.Request) {
	var request model.SnapshotGenerateRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	snap, err := a.service.GenerateSnapshot(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, snap)
}

func (a *API) loadSnapshot(w http.ResponseWriter, r *http.Request) {
	var snap model.Snapshot
	if err := decodeJSON(r, &snap); err != nil {
		writeError(w, err)
		return
	}
	loaded, err := a.service.LoadSnapshot(r.Context(), snap)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, loaded)
}

func (a *API) currentSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := a.service.CurrentSnapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (a *API) getSnapshot(w http.ResponseWriter, r *http.Request) {
	version, err := pathInt64(r, "version")
	if err != nil {
		writeError(w, err)
		return
	}
	snap, err := a.service.GetSnapshot(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (a *API) submitChain(w http.ResponseWriter, r *http.Request) {
	var chain model.Chain
	if err := decodeJSON(r, &chain); err != nil {
		writeError(w, err)
		return
	}
	submitted, err := a.service.SubmitChain(r.Context(), chain)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, submitted)
}

func (a *API) getChain(w http.ResponseWriter, r *http.Request) {
	version, err := pathInt64(r, "version")
	if err != nil {
		writeError(w, err)
		return
	}
	chain, err := a.service.GetChain(r.Context(), r.PathValue("id"), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, chain)
}

func (a *API) execute(w http.ResponseWriter, r *http.Request) {
	var request model.ExecuteRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	view, err := a.service.ExecuteChain(r.Context(), r.PathValue("id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (a *API) listExecutions(w http.ResponseWriter, r *http.Request) {
	version, err := pathInt64(r, "version")
	if err != nil {
		writeError(w, err)
		return
	}
	executions, err := a.service.ListExecutions(r.Context(), r.PathValue("id"), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"executions": executions})
}

func (a *API) getExecution(w http.ResponseWriter, r *http.Request) {
	view, err := a.service.GetExecution(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (a *API) batches(w http.ResponseWriter, r *http.Request) {
	view, err := a.resolveVersionExecution(r)
	if err != nil {
		writeError(w, err)
		return
	}
	response := map[string]any{
		"execution_id":     view.ExecutionID,
		"chain_id":         view.ChainID,
		"chain_version":    view.ChainVersion,
		"snapshot_version": view.SnapshotVersion,
		"status":           view.Status,
		"published":        view.Published,
		"path":             view.Path,
		"batches":          []model.BatchTrace{},
	}
	if view.Published {
		response["batches"] = view.Batches
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) aggregates(w http.ResponseWriter, r *http.Request) {
	view, err := a.resolveVersionExecution(r)
	if err != nil {
		writeError(w, err)
		return
	}
	response := map[string]any{
		"execution_id":     view.ExecutionID,
		"chain_id":         view.ChainID,
		"chain_version":    view.ChainVersion,
		"snapshot_version": view.SnapshotVersion,
		"status":           view.Status,
		"published":        view.Published,
		"aggregates":       []model.AggregateValue{},
	}
	if view.Published {
		response["aggregates"] = view.Aggregates
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) resolveVersionExecution(r *http.Request) (model.ExecutionView, error) {
	version, err := pathInt64(r, "version")
	if err != nil {
		return model.ExecutionView{}, err
	}
	return a.service.ResolveExecution(r.Context(), r.PathValue("id"), version, r.URL.Query().Get("execution_id"))
}

func (a *API) compare(w http.ResponseWriter, r *http.Request) {
	right := strings.TrimSpace(r.URL.Query().Get("right"))
	if right == "" {
		writeError(w, model.NewBizError(model.CodeInvalidRequest, "right execution id is required"))
		return
	}
	tolerance := int64(0)
	if raw := r.URL.Query().Get("tolerance_ppm"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, model.NewBizError(model.CodeInvalidRequest, "tolerance_ppm must be a non-negative integer"))
			return
		}
		tolerance = parsed
	}
	report, err := a.service.CompareExecutions(r.Context(), r.PathValue("id"), right, tolerance)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) recoveryReports(w http.ResponseWriter, r *http.Request) {
	reports, err := a.service.RecoveryReports(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reports": reports})
}

func (a *API) listFaults(w http.ResponseWriter, r *http.Request) {
	kinds := []string{model.FaultBitmapMismatch, model.FaultFixedOverflow, model.FaultDictIndex, model.FaultDivideByZero, model.FaultExecAborted}
	out := make([]service.FaultProbeResult, 0, len(kinds))
	for _, kind := range kinds {
		probe, _ := service.ProbeFault(kind)
		out = append(out, probe)
	}
	writeJSON(w, http.StatusOK, map[string]any{"faults": out})
}

func (a *API) probeFault(w http.ResponseWriter, r *http.Request) {
	var probe model.FaultProbe
	if err := decodeJSON(r, &probe); err != nil {
		writeError(w, err)
		return
	}
	result, err := service.ProbeFault(probe.Kind)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func pathInt64(r *http.Request, name string) (int64, error) {
	value, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil || value <= 0 {
		return 0, model.NewBizError(model.CodeInvalidRequest, "path value must be a positive integer").
			With("path", name).With("value", r.PathValue(name))
	}
	return value, nil
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64<<20))
	if err := decoder.Decode(target); err != nil {
		return model.NewBizError(model.CodeInvalidRequest, "invalid JSON request body").With("cause", err.Error())
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return model.NewBizError(model.CodeInvalidRequest, "request body must contain exactly one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	if err == nil {
		err = errors.New("unknown error")
	}
	response := model.ToErrorResponse(err)
	status := model.HTTPStatusForCode(response.Error.Code)
	writeJSON(w, status, response)
}

func ErrorHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(w, model.NewBizError(model.CodeInternal, fmt.Sprintf("panic: %v", recovered)))
			}
		}()
		next.ServeHTTP(w, r)
	})
}
