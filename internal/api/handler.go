package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"gatewayworkbench/internal/engine"
	"gatewayworkbench/internal/model"
	"gatewayworkbench/internal/scheduler"
	"gatewayworkbench/internal/store"
)

type Handler struct {
	engine    *engine.Engine
	scheduler *scheduler.Scheduler
	store     *store.Store
}

func New(eng *engine.Engine, sch *scheduler.Scheduler, st *store.Store) *Handler {
	return &Handler{engine: eng, scheduler: sch, store: st}
}

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("POST /v1/samples/load", h.loadSample)
	mux.HandleFunc("POST /v1/probe-rounds", h.runProbe)
	mux.HandleFunc("POST /v1/dispatch-rounds", h.runDispatch)
	mux.HandleFunc("GET /v1/instances/{id}", h.getInstance)
	mux.HandleFunc("GET /v1/instances", h.listInstances)
	mux.HandleFunc("POST /v1/instances/{id}/drain", h.drain)
	mux.HandleFunc("POST /v1/instances/{id}/resume", h.resume)
	mux.HandleFunc("GET /v1/requests/{seq}", h.getRequest)
	mux.HandleFunc("GET /v1/recovery/latest", h.recovery)
	mux.HandleFunc("GET /v1/snapshots/latest", h.latestSnapshot)
	mux.HandleFunc("GET /v1/rounds/{id}/probes", h.roundProbes)
	mux.HandleFunc("GET /v1/rounds/{id}/dispatches", h.roundDispatches)
	return mux
}

type errorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Context   string `json:"context,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

func writeError(w http.ResponseWriter, err error) {
	var biz *model.BizError
	status := http.StatusInternalServerError
	if errors.As(err, &biz) {
		switch biz.Code {
		case model.CodeInstanceInvalid, model.CodeInvalidPayload:
			status = http.StatusBadRequest
		case model.CodeInvalidTransition:
			status = http.StatusConflict
		case model.CodeNotFound:
			status = http.StatusNotFound
		case model.CodeDispatchQueueFull:
			status = http.StatusTooManyRequests
		case model.CodeNoCandidate:
			status = http.StatusServiceUnavailable
		default:
			status = http.StatusBadGateway
		}
	} else {
		biz = model.NewBizError("E_INTERNAL", err.Error(), "")
	}
	writeJSON(w, status, errorBody{
		Code: biz.Code, Message: biz.Message, Context: biz.Context,
		Timestamp: time.Now().UnixMilli(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) loadSample(w http.ResponseWriter, r *http.Request) {
	var sample model.Sample
	if err := json.NewDecoder(r.Body).Decode(&sample); err != nil {
		writeError(w, model.NewBizError(model.CodeInvalidPayload, "invalid JSON sample", err.Error()))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := h.engine.LoadSample(ctx, &sample); err != nil {
		writeError(w, err)
		return
	}
	if err := h.engine.SaveAffinity(ctx, sample.Affinity.Ratios); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"loaded":         true,
		"instances":      len(sample.Instances),
		"trace_requests": len(sample.Trace),
		"affinity_zones": len(sample.Affinity.Ratios),
	})
}

type roundRequest struct {
	RoundID string `json:"round_id"`
}

func (h *Handler) runProbe(w http.ResponseWriter, r *http.Request) {
	var req roundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, model.NewBizError(model.CodeInvalidPayload, "invalid probe request", err.Error()))
		return
	}
	if req.RoundID == "" {
		req.RoundID = "probe-" + strconv.FormatInt(time.Now().UnixMilli(), 10)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	outcomes, err := h.engine.RunProbeRound(ctx, req.RoundID)
	if err != nil {
		writeError(w, err)
		return
	}
	snap, _ := h.store.LatestSnapshot(ctx)
	writeJSON(w, http.StatusOK, map[string]any{
		"round_id": req.RoundID, "outcomes": outcomes, "snapshot": snap,
	})
}

func (h *Handler) runDispatch(w http.ResponseWriter, r *http.Request) {
	var req roundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, model.NewBizError(model.CodeInvalidPayload, "invalid dispatch request", err.Error()))
		return
	}
	if req.RoundID == "" {
		req.RoundID = "dispatch-" + strconv.FormatInt(time.Now().UnixMilli(), 10)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	result, err := h.scheduler.RunRound(ctx, req.RoundID)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusOK
	if result.QueueFullError != nil {
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, result)
}

func (h *Handler) getInstance(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	state, err := h.store.GetState(ctx, id)
	if err != nil {
		writeError(w, err)
		return
	}
	snap, err := h.store.LatestSnapshot(ctx)
	if err != nil {
		writeError(w, err)
		return
	}
	weight := 0
	zone := ""
	if snap != nil {
		for z, weights := range snap.ZoneWeights {
			if w, ok := weights[id]; ok {
				zone, weight = z, w
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"instance_id":       state.InstanceID,
		"phase":             state.Phase,
		"reason":            state.Reason,
		"success_streak":    state.SuccessStreak,
		"failure_streak":    state.FailureStreak,
		"allocated_zone":    zone,
		"normalized_weight": weight,
		"dispatchable":      model.IsDispatchable(state.Phase),
		"snapshot_version":  snapshotVersion(snap),
	})
}

func (h *Handler) listInstances(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	instances, err := h.store.ListInstances(ctx)
	if err != nil {
		writeError(w, err)
		return
	}
	states, err := h.store.ListStates(ctx)
	if err != nil {
		writeError(w, err)
		return
	}
	snap, _ := h.store.LatestSnapshot(ctx)
	type item struct {
		ID               string      `json:"id"`
		Port             int         `json:"port"`
		Zone             string      `json:"zone"`
		RawWeight        int         `json:"raw_weight"`
		Phase            model.Phase `json:"phase"`
		Reason           string      `json:"reason"`
		NormalizedWeight int         `json:"normalized_weight"`
		Dispatchable     bool        `json:"dispatchable"`
	}
	weightByID := map[string]int{}
	if snap != nil {
		for _, weights := range snap.ZoneWeights {
			for id, w := range weights {
				weightByID[id] = w
			}
		}
	}
	phaseByID := map[string]store.InstanceStateRow{}
	for _, row := range states {
		phaseByID[row.InstanceID] = row
	}
	out := make([]item, 0, len(instances))
	for _, ins := range instances {
		row := phaseByID[ins.ID]
		out = append(out, item{
			ID: ins.ID, Port: ins.Port, Zone: ins.Zone, RawWeight: ins.Weight,
			Phase: row.Phase, Reason: row.Reason,
			NormalizedWeight: weightByID[ins.ID],
			Dispatchable:     model.IsDispatchable(row.Phase),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_version": snapshotVersion(snap), "instances": out})
}

type drainRequest struct {
	Reason string `json:"reason"`
}

func (h *Handler) drain(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req drainRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	snap, err := h.engine.Drain(r.Context(), id, req.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"drained": id, "snapshot": snap})
}

func (h *Handler) resume(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	snap, err := h.engine.Resume(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"resumed": id, "snapshot": snap})
}

func (h *Handler) getRequest(w http.ResponseWriter, r *http.Request) {
	seq, err := strconv.Atoi(r.PathValue("seq"))
	if err != nil {
		writeError(w, model.NewBizError(model.CodeInvalidPayload, "request seq must be integer", r.PathValue("seq")))
		return
	}
	out, err := h.store.RequestDecision(r.Context(), seq)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) recovery(w http.ResponseWriter, r *http.Request) {
	report, err := h.store.LatestRecoveryReport(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(report)
	_, _ = w.Write([]byte("\n"))
}

func (h *Handler) latestSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := h.store.LatestSnapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if snap == nil {
		writeError(w, model.NewBizError(model.CodeNotFound, "no committed snapshot", ""))
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (h *Handler) roundProbes(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.ListProbes(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"round_id": r.PathValue("id"), "probes": rows})
}

func (h *Handler) roundDispatches(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.ListDispatches(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"round_id": r.PathValue("id"), "dispatches": rows})
}

func snapshotVersion(snap *model.SnapshotVersion) int64 {
	if snap == nil {
		return 0
	}
	return snap.Version
}
