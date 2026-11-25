package httpapi

import (
	"encoding/json"
	"net/http"

	"gpusched/internal/domain"
	"gpusched/internal/gen"
	"gpusched/internal/service"
)

type Handler struct {
	app *service.App
}

func New(app *service.App) *Handler { return &Handler{app: app} }

// Routes registers every endpoint on the given mux.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.health)

	mux.HandleFunc("POST /v1/spec/load", h.loadSpec)
	mux.HandleFunc("POST /v1/spec/load-sample", h.loadSample)

	mux.HandleFunc("GET /v1/groups", h.listGroups)
	mux.HandleFunc("GET /v1/nodes", h.listNodes)
	mux.HandleFunc("GET /v1/requests", h.listRequests)
	mux.HandleFunc("POST /v1/requests", h.submitRequest)
	mux.HandleFunc("GET /v1/requests/{id}", h.getRequest)

	mux.HandleFunc("GET /v1/quota", h.quota)
	mux.HandleFunc("GET /v1/batches", h.batches)

	mux.HandleFunc("POST /v1/requests/{id}/admit", h.admit)
	mux.HandleFunc("POST /v1/requests/{id}/transition", h.transition)
	mux.HandleFunc("POST /v1/requests/{id}/preempt", h.preempt)

	mux.HandleFunc("POST /v1/scheduler/round", h.runRound)
	mux.HandleFunc("GET /v1/policy/latest", h.latestPolicy)
	mux.HandleFunc("GET /v1/recovery", h.recovery)

	mux.HandleFunc("POST /v1/debug/crash", h.debugCrash)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeOK(w, map[string]string{"status": "ok"})
}

func (h *Handler) loadSpec(w http.ResponseWriter, r *http.Request) {
	var spec domain.LoadSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeErr(w, domain.NewError(domain.ESpecInvalid, "invalid JSON body: "+err.Error(), nil))
		return
	}
	set, err := domain.Normalize(spec)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.app.Load(r.Context(), set); err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, map[string]any{
		"groups": len(set.Groups), "nodes": len(set.Nodes),
		"requests": len(set.Requests), "snapshots": len(set.Snapshots),
		"fragments": len(set.Fragments),
	})
}

func (h *Handler) loadSample(w http.ResponseWriter, r *http.Request) {
	spec := gen.Generate(gen.DefaultSeed)
	set, err := domain.Normalize(spec)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.app.Load(r.Context(), set); err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, map[string]any{
		"seed":   gen.DefaultSeed,
		"groups": len(set.Groups), "nodes": len(set.Nodes),
		"requests": len(set.Requests), "snapshots": len(set.Snapshots),
		"fragments": len(set.Fragments),
	})
}

func (h *Handler) listGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := h.app.ListGroups(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, groups)
}

func (h *Handler) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := h.app.ListNodes(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, nodes)
}

func (h *Handler) listRequests(w http.ResponseWriter, r *http.Request) {
	reqs, err := h.app.ListRequests(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, reqs)
}

func (h *Handler) getRequest(w http.ResponseWriter, r *http.Request) {
	req, err := h.app.GetRequest(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, req)
}

// submitRequest validates and stores one new request. Duplicate ids or unknown
// groups return E_SPEC_INVALID and nothing is persisted.
func (h *Handler) submitRequest(w http.ResponseWriter, r *http.Request) {
	var spec domain.RequestSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeErr(w, domain.NewError(domain.ESpecInvalid, "invalid JSON body: "+err.Error(), nil))
		return
	}
	set, err := domain.Normalize(domain.LoadSpec{Requests: []domain.RequestSpec{spec}})
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := h.app.SubmitRequest(r.Context(), set.Requests[0]); err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, map[string]string{"request_id": spec.ID, "state": domain.StatePending})
}

func (h *Handler) quota(w http.ResponseWriter, r *http.Request) {
	report, err := h.app.QueryQuota(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, report)
}

func (h *Handler) batches(w http.ResponseWriter, r *http.Request) {
	batches, err := h.app.ListBatches(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, batches)
}

func (h *Handler) admit(w http.ResponseWriter, r *http.Request) {
	res, err := h.app.AdmitOne(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}

func (h *Handler) transition(w http.ResponseWriter, r *http.Request) {
	var body struct {
		To string `json:"to"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, domain.NewError(domain.ESpecInvalid, "invalid JSON body", nil))
		return
	}
	res, err := h.app.Transition(r.Context(), r.PathValue("id"), body.To)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}

func (h *Handler) preempt(w http.ResponseWriter, r *http.Request) {
	res, err := h.app.Preempt(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, res)
}

func (h *Handler) runRound(w http.ResponseWriter, r *http.Request) {
	var body struct {
		VirtualBudgetConsumedMs int64 `json:"virtual_budget_consumed_ms"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	report, err := h.app.RunRound(r.Context(), body.VirtualBudgetConsumedMs)
	if err != nil {
		// Even when the queue fills, the committed report is useful; surface it.
		if be, ok := domain.AsBizError(err); ok && be.Code == domain.EExecQueueFull {
			writeErr(w, err)
			return
		}
		writeErr(w, err)
		return
	}
	writeOK(w, report)
}

func (h *Handler) latestPolicy(w http.ResponseWriter, r *http.Request) {
	snap, err := h.app.LatestPolicy(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, snap)
}

func (h *Handler) recovery(w http.ResponseWriter, r *http.Request) {
	report, err := h.app.Recover(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, report)
}

// debugCrash installs a one-shot forced termination. The process exits at the
// named durability point so the restart/recovery path can be verified.
func (h *Handler) debugCrash(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Point string `json:"point"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, domain.NewError(domain.ESpecInvalid, "invalid JSON body", nil))
		return
	}
	h.app.ArmCrash(body.Point)
	writeOK(w, map[string]string{"armed_at": body.Point})
}
