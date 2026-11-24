package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"motionlab/internal/engine"
	"motionlab/internal/model"
	"motionlab/internal/motion"
	"motionlab/internal/store"
	"motionlab/internal/webui"
)

type App struct {
	engine  *engine.Engine
	store   *store.Store
	dataDir string
}

type envelope struct {
	OK    bool            `json:"ok"`
	Data  any             `json:"data,omitempty"`
	Error *model.AppError `json:"error,omitempty"`
}

func New(eng *engine.Engine, dataDir string) *App {
	return &App{engine: eng, store: eng.Store(), dataDir: dataDir}
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", a.health)
	mux.HandleFunc("GET /api/bootstrap", a.bootstrap)
	mux.HandleFunc("GET /api/sequences", a.listSequences)
	mux.HandleFunc("GET /api/sequences/{id}", a.sequenceDetail)
	mux.HandleFunc("GET /api/sequences/{id}/initial.png", a.initialFrame)
	mux.HandleFunc("GET /api/sequences/{id}/background.png", a.backgroundFrame)
	mux.HandleFunc("GET /api/sequences/{id}/truth/{file}", a.truthFrame)
	mux.HandleFunc("GET /api/policy", a.policy)
	mux.HandleFunc("POST /api/import", a.importSequence)
	mux.HandleFunc("GET /api/runs", a.listRuns)
	mux.HandleFunc("POST /api/runs", a.createRun)
	mux.HandleFunc("GET /api/runs/{id}", a.runDetail)
	mux.HandleFunc("POST /api/runs/{id}/start", a.startRun)
	mux.HandleFunc("POST /api/runs/{id}/stop", a.stopRun)
	mux.HandleFunc("POST /api/runs/{id}/resume", a.resumeRun)
	mux.HandleFunc("GET /api/runs/{id}/report", a.runReport)
	mux.HandleFunc("GET /api/runs/{id}/events", a.runEvents)
	mux.HandleFunc("GET /api/runs/{id}/branches/{branch}/steps", a.branchSteps)
	mux.HandleFunc("GET /api/runs/{id}/branches/{branch}/frames/{file}", a.predictedFrame)
	mux.HandleFunc("PUT /api/runs/{id}/policy", a.replacePolicy)
	mux.Handle("GET /", webui.Handler())
	return a.recoverer(a.securityHeaders(a.logRequests(mux)))
}

func (a *App) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				log.Printf("panic serving %s %s: %v", r.Method, r.URL.Path, v)
				writeError(w, model.NewError(model.CodeInternal, "internal server error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (a *App) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data: blob:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'")
		next.ServeHTTP(w, r)
	})
}

func (a *App) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
		}
	})
}

func writeData(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(envelope{OK: true, Data: data})
}

func writeError(w http.ResponseWriter, err error) {
	appErr := model.AsAppError(err)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(appErr.HTTPStatus())
	_ = json.NewEncoder(w).Encode(envelope{OK: false, Error: appErr})
}

func readJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return model.NewError(model.CodeValidation, "invalid JSON body: "+err.Error())
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return model.NewError(model.CodeValidation, "request body must contain one JSON document")
	}
	return nil
}

func (a *App) health(w http.ResponseWriter, r *http.Request) {
	writeData(w, http.StatusOK, map[string]any{
		"status": "ok", "service": "motionlab", "time": time.Now().UTC().Format(time.RFC3339),
		"database": "sqlite", "virtual_time_budget": true,
	})
}

func (a *App) catalog() *motion.Catalog { return a.engine.Catalog() }

func (a *App) bootstrap(w http.ResponseWriter, r *http.Request) {
	cat := a.catalog()
	runs, err := a.store.ListRuns(r.Context(), 30)
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{
		"sequences":   cat.Summary(),
		"policy":      cat.Policy,
		"policy_hash": cat.Policy.Hash(),
		"runs":        summarizeRuns(runs),
		"mode":        "deterministic-virtual-time",
	})
}

func (a *App) listSequences(w http.ResponseWriter, r *http.Request) {
	writeData(w, http.StatusOK, map[string]any{"sequences": a.catalog().Summary()})
}

func (a *App) sequenceDetail(w http.ResponseWriter, r *http.Request) {
	seq, appErr := a.catalog().Sequence(r.PathValue("id"))
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	frames := make([]map[string]any, 0, len(seq.Truth))
	for i, f := range seq.Truth {
		frames = append(frames, map[string]any{"index": i, "sha256": f.SHA256, "url": fmt.Sprintf("/api/sequences/%s/truth/%d.png", seq.Spec.SequenceID, i)})
	}
	writeData(w, http.StatusOK, map[string]any{
		"spec":              seq.Spec,
		"initial_sha256":    seq.Shared.Initial.SHA256,
		"background_sha256": seq.Shared.Background.SHA256,
		"truth_frames":      frames,
		"initial_url":       fmt.Sprintf("/api/sequences/%s/initial.png", seq.Spec.SequenceID),
	})
}

func (a *App) initialFrame(w http.ResponseWriter, r *http.Request) {
	seq, appErr := a.catalog().Sequence(r.PathValue("id"))
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	writePNG(w, seq.Shared.Initial)
}

func (a *App) backgroundFrame(w http.ResponseWriter, r *http.Request) {
	seq, appErr := a.catalog().Sequence(r.PathValue("id"))
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	writePNG(w, seq.Shared.Background)
}

func (a *App) truthFrame(w http.ResponseWriter, r *http.Request) {
	seq, appErr := a.catalog().Sequence(r.PathValue("id"))
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	idx, err := strconv.Atoi(strings.TrimSuffix(r.PathValue("file"), ".png"))
	if err != nil || idx < 0 || idx >= len(seq.Truth) {
		writeError(w, model.NewError(model.CodeNotFound, "truth frame index out of range"))
		return
	}
	writePNG(w, seq.Truth[idx])
}

func writePNG(w http.ResponseWriter, frame *motion.Frame) {
	data, err := frame.PNG()
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "public, max-age=3600, immutable")
	w.Header().Set("X-Frame-Digest", frame.SHA256)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (a *App) policy(w http.ResponseWriter, r *http.Request) {
	p := a.catalog().Policy
	writeData(w, http.StatusOK, map[string]any{"policy": p, "hash": p.Hash()})
}

type runCreateRequest struct {
	SequenceID     string                `json:"sequence_id"`
	RequestedSteps int                   `json:"requested_steps"`
	Concurrency    int                   `json:"concurrency"`
	BranchQuota    int                   `json:"branch_quota"`
	RoundBudgetMS  int                   `json:"round_time_budget_ms"`
	PruneThreshold float64               `json:"prune_threshold"`
	DisablePrune   bool                  `json:"disable_prune"`
	StageMode      bool                  `json:"stage_mode"`
	BranchFilter   []int                 `json:"branch_filter,omitempty"`
	Policy         *model.PolicySnapshot `json:"policy,omitempty"`
	Fresh          *bool                 `json:"fresh,omitempty"`
	AutoStart      *bool                 `json:"auto_start,omitempty"`
}

func (a *App) createRun(w http.ResponseWriter, r *http.Request) {
	var req runCreateRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if req.SequenceID == "" {
		writeError(w, model.NewError(model.CodeValidation, "sequence_id is required"))
		return
	}
	policy := a.catalog().Policy
	if req.Policy != nil {
		policy = *req.Policy
	}
	config := model.RunConfig{
		SequenceID: req.SequenceID, RequestedSteps: req.RequestedSteps,
		Concurrency: req.Concurrency, BranchQuota: req.BranchQuota,
		RoundBudgetMS: req.RoundBudgetMS, PruneThreshold: req.PruneThreshold,
		DisablePrune: req.DisablePrune, StageMode: req.StageMode, BranchFilter: req.BranchFilter,
	}
	fresh := true
	if req.Fresh != nil {
		fresh = *req.Fresh
	}
	row, err := a.engine.EnsureRun(r.Context(), req.SequenceID, policy, config, fresh)
	if err != nil {
		writeError(w, err)
		return
	}
	autoStart := true
	if req.AutoStart != nil {
		autoStart = *req.AutoStart
	}
	if autoStart {
		if err := a.engine.Start(row.RunID); err != nil {
			writeError(w, err)
			return
		}
	}
	branches, _ := a.store.LoadBranches(r.Context(), row.RunID)
	writeData(w, http.StatusCreated, map[string]any{
		"run_id": row.RunID, "sequence_id": row.SequenceID, "status": "pending",
		"active": a.engine.IsActive(row.RunID), "policy_hash": row.PolicyHash,
		"branches": branchRowsSummary(branches),
	})
}

func (a *App) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := a.store.ListRuns(r.Context(), 100)
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{"runs": summarizeRuns(runs), "active": a.activeRuns(runs)})
}

func (a *App) activeRuns(runs []store.RunRow) []string {
	var out []string
	for _, run := range runs {
		if a.engine.IsActive(run.RunID) {
			out = append(out, run.RunID)
		}
	}
	return out
}

func summarizeRuns(runs []store.RunRow) []map[string]any {
	out := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		item := map[string]any{
			"run_id": run.RunID, "sequence_id": run.SequenceID, "status": run.Status,
			"policy_version": run.PolicyVersion, "policy_hash": run.PolicyHash,
			"checkpoint_seq": run.CheckpointSeq, "recovery_count": run.RecoveryCount,
			"created_at": run.CreatedAt, "updated_at": run.UpdatedAt,
			"report_sha": run.ReportSHA,
		}
		if run.ReportJSON != "" {
			var report model.RunReport
			if json.Unmarshal([]byte(run.ReportJSON), &report) == nil {
				item["summary"] = report.Summary
			}
		}
		out = append(out, item)
	}
	return out
}

func branchRowsSummary(branches []store.BranchRow) []map[string]any {
	out := make([]map[string]any, 0, len(branches))
	for _, b := range branches {
		out = append(out, map[string]any{
			"branch_id": b.BranchID, "status": b.Status, "reason": b.Reason, "wave": b.Wave,
			"requested_steps": b.RequestedSteps, "target_steps": b.TargetSteps,
			"next_step": b.NextStep, "quota_used": b.QuotaUsed, "reclaimed_quota": b.ReclaimedQuota,
			"final_mae": b.FinalMAE, "state_sha256": b.StateSHA, "frame_sha256": b.LastFrameSHA,
		})
	}
	return out
}

func (a *App) runDetail(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	report, err := a.engine.Snapshot(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	events, _ := a.store.LoadEventsStructured(r.Context(), runID)
	policyEvents, _ := a.store.PolicyEvents(r.Context(), runID)
	writeData(w, http.StatusOK, map[string]any{
		"report": report, "active": a.engine.IsActive(runID),
		"events": events, "policy_events": policyEvents,
	})
}

func (a *App) startRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if _, err := a.store.GetRun(r.Context(), runID); err != nil {
		writeError(w, err)
		return
	}
	if err := a.engine.Start(runID); err != nil {
		writeError(w, err)
		return
	}
	writeData(w, http.StatusAccepted, map[string]any{"run_id": runID, "active": true})
}

func (a *App) stopRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.engine.Stop(ctx, runID); err != nil {
		writeError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{"run_id": runID, "status": "paused", "message": "committed prefix retained"})
}

func (a *App) resumeRun(w http.ResponseWriter, r *http.Request) {
	a.startRun(w, r)
}

func (a *App) runReport(w http.ResponseWriter, r *http.Request) {
	report, err := a.engine.Snapshot(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, http.StatusOK, report)
}

func (a *App) runEvents(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	events, err := a.store.LoadEventsStructured(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	policyEvents, err := a.store.PolicyEvents(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, http.StatusOK, map[string]any{"events": events, "policy_events": policyEvents})
}

func (a *App) branchSteps(w http.ResponseWriter, r *http.Request) {
	branchID, err := strconv.Atoi(r.PathValue("branch"))
	if err != nil {
		writeError(w, model.NewError(model.CodeValidation, "branch must be an integer"))
		return
	}
	report, err := a.engine.Snapshot(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	for _, b := range report.Branches {
		if b.BranchID == branchID {
			writeData(w, http.StatusOK, b)
			return
		}
	}
	writeError(w, model.NewError(model.CodeNotFound, "branch not found"))
}

func (a *App) predictedFrame(w http.ResponseWriter, r *http.Request) {
	branchID, err := strconv.Atoi(r.PathValue("branch"))
	if err != nil {
		writeError(w, model.NewError(model.CodeValidation, "branch must be an integer"))
		return
	}
	step, err := strconv.Atoi(strings.TrimSuffix(r.PathValue("file"), ".png"))
	if err != nil {
		writeError(w, model.NewError(model.CodeValidation, "step must be an integer"))
		return
	}
	data, sha, err := a.store.LoadStepPNG(r.Context(), r.PathValue("id"), branchID, step)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "public, max-age=3600, immutable")
	w.Header().Set("X-Frame-Digest", sha)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (a *App) replacePolicy(w http.ResponseWriter, r *http.Request) {
	var p model.PolicySnapshot
	if err := readJSON(r, &p); err != nil {
		writeError(w, err)
		return
	}
	decision, err := a.engine.ApplyPolicy(r.Context(), r.PathValue("id"), p)
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, http.StatusOK, decision)
}

type importFrame struct {
	Index int    `json:"index"`
	PNG   string `json:"png_base64"`
}

type importRequest struct {
	Spec       model.SequenceSpec `json:"spec"`
	InitialPNG string             `json:"initial_png_base64,omitempty"`
	Background string             `json:"background_png_base64,omitempty"`
	Frames     []importFrame      `json:"frames"`
}

func (a *App) importSequence(w http.ResponseWriter, r *http.Request) {
	var req importRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	truth := make([]*motion.Frame, len(req.Frames))
	for _, item := range req.Frames {
		if item.Index < 0 || item.Index >= len(req.Frames) {
			writeError(w, model.NewError(model.CodeValidation, "frame index outside supplied frame array"))
			return
		}
		data, err := base64.StdEncoding.DecodeString(item.PNG)
		if err != nil {
			writeError(w, model.NewError(model.CodeValidation, "frame is not valid base64: "+err.Error()))
			return
		}
		frame, err := motion.DecodePNG(data)
		if err != nil {
			writeError(w, model.NewError(model.CodeValidation, "frame is not valid PNG: "+err.Error()))
			return
		}
		truth[item.Index] = frame
	}
	var initial, background *motion.Frame
	if req.InitialPNG != "" {
		data, err := base64.StdEncoding.DecodeString(req.InitialPNG)
		if err != nil {
			writeError(w, model.NewError(model.CodeValidation, "initial frame is not valid base64"))
			return
		}
		initial, err = motion.DecodePNG(data)
		if err != nil {
			writeError(w, model.NewError(model.CodeValidation, "initial frame is not valid PNG"))
			return
		}
	}
	if req.Background != "" {
		data, err := base64.StdEncoding.DecodeString(req.Background)
		if err != nil {
			writeError(w, model.NewError(model.CodeValidation, "background frame is not valid base64"))
			return
		}
		background, err = motion.DecodePNG(data)
		if err != nil {
			writeError(w, model.NewError(model.CodeValidation, "background frame is not valid PNG"))
			return
		}
	}
	for _, f := range truth {
		if f == nil {
			writeError(w, model.NewError(model.CodeValidation, "every truth frame index must be supplied exactly once"))
			return
		}
	}
	cat, err := motion.ImportSequence(a.dataDir, motion.ImportBundle{Spec: req.Spec, Initial: initial, Background: background, Truth: truth})
	if err != nil {
		writeError(w, err)
		return
	}
	a.engine.SetCatalog(cat)
	writeData(w, http.StatusCreated, map[string]any{"sequence_id": req.Spec.SequenceID, "sequences": cat.Summary()})
}
