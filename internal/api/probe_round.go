package api

import (
	"net/http"
	"time"

	"deployverify/internal/model"
	"deployverify/internal/probe"
	"deployverify/internal/store"
)

type probeRoundRequest struct {
	ID                string   `json:"id"`
	Environment       string   `json:"environment"`
	Services          []string `json:"services"`
	AllServices       *bool    `json:"allServices"`
	GlobalConcurrency int      `json:"globalConcurrency"`
	EnvConcurrency    int      `json:"envConcurrency"`
	QueueCapacity     int      `json:"queueCapacity"`
	BudgetMS          int64    `json:"budgetMs"`
	ProbeTimeoutMS    int64    `json:"probeTimeoutMs"`
	ProbePath         string   `json:"probePath"`
	Method            string   `json:"method"`
}

func (s *Server) createProbeRound(w http.ResponseWriter, r *http.Request) {
	var request probeRoundRequest
	if r.ContentLength != 0 {
		if err := decodeBody(r, &request); err != nil {
			writeError(w, model.NewError(model.CodeSpecInvalid, "invalid probe-round request", err.Error()))
			return
		}
	}
	snapshot, err := s.Store.LoadSnapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if snapshot.Version == 0 {
		writeError(w, model.NewError(model.CodeNotFound, "no committed snapshot; submit a snapshot first", nil))
		return
	}
	filter := map[string]bool{}
	for _, service := range request.Services {
		filter[service] = true
	}
	explicitNone := request.AllServices != nil && !*request.AllServices && len(request.Services) == 0
	tasks := []probe.Task{}
	for _, env := range snapshot.Environments {
		if request.Environment != "" && env.Name != request.Environment {
			continue
		}
		for _, unit := range env.Units {
			if explicitNone {
				continue
			}
			if len(filter) > 0 && !filter[unit.Service] {
				continue
			}
			path := request.ProbePath
			timeoutMS := request.ProbeTimeoutMS
			if unit.Probe != nil {
				if path == "" {
					path = unit.Probe.Path
				}
				if timeoutMS == 0 {
					timeoutMS = int64(unit.Probe.TimeoutMS)
				}
			}
			if timeoutMS <= 0 {
				timeoutMS = 1000
			}
			targetURL, err := resolveProbeURL(s.ProbeBaseURL, path)
			if err != nil {
				writeError(w, model.NewError(model.CodeSpecInvalid, "invalid probe path", map[string]any{"service": unit.Service, "error": err.Error()}))
				return
			}
			tasks = append(tasks, probe.Task{
				Ref: model.Ref{Environment: env.Name, Service: unit.Service}, Priority: env.Priority,
				URL: targetURL, Method: request.Method, Timeout: time.Duration(timeoutMS) * time.Millisecond,
			})
		}
	}

	budgetMS := request.BudgetMS
	if budgetMS <= 0 {
		budgetMS = 2000
	}
	cfg := probe.Config{
		GlobalConcurrency: request.GlobalConcurrency, EnvConcurrency: request.EnvConcurrency,
		QueueCapacity: request.QueueCapacity, Budget: time.Duration(budgetMS) * time.Millisecond,
		DefaultTimeout: time.Duration(request.ProbeTimeoutMS) * time.Millisecond,
	}
	runID := request.ID
	if runID == "" {
		runID = store.NewIntentID("run")
	}
	run := &store.ProbeRun{
		ID: runID, Version: snapshot.Version, Status: "running",
		GlobalConcurrency: probe.NormalizeConfig(cfg).GlobalConcurrency,
		EnvConcurrency:    probe.NormalizeConfig(cfg).EnvConcurrency,
		QueueCapacity:     request.QueueCapacity, BudgetMS: budgetMS,
		ProbeTimeoutMS: request.ProbeTimeoutMS, TotalTasks: len(tasks),
	}
	if err := s.Store.CreateProbeRun(r.Context(), run, map[string]any{"request": request, "taskCount": len(tasks)}); err != nil {
		writeError(w, err)
		return
	}
	scheduler := &probe.Scheduler{Executor: probe.ExecuteHTTP, Sink: storeSink{store: s.Store}}
	report := scheduler.Run(r.Context(), runID, tasks, cfg)

	run.Status = report.Status
	run.ElapsedMS = report.ElapsedMS
	run.ConsumedBudgetMS = report.ElapsedMS
	run.TotalTasks = report.TotalTasks
	run.StartedCount = report.StartedCount
	run.SucceededCount = report.SucceededCount
	run.FailedCount = report.FailedCount
	run.TimeoutCount = report.TimeoutCount
	run.SkippedCount = report.SkippedCount
	run.DuplicateCount = report.DuplicateCount
	run.Report = report
	if err := s.Store.FinishProbeRun(r.Context(), *run); err != nil {
		writeError(w, err)
		return
	}
	if report.Code == model.CodeProbeQueueFull {
		writeError(w, model.NewError(model.CodeProbeQueueFull, "probe bounded waiting queue is full", report))
		return
	}
	for _, result := range report.Results {
		if result.PersistError != "" {
			writeError(w, model.NewError(model.CodeInternal, "probe result could not be atomically committed", report))
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "runId": runID, "report": report})
}
