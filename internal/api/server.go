package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"deployverify/internal/demo"
	"deployverify/internal/derive"
	"deployverify/internal/diff"
	"deployverify/internal/model"
	"deployverify/internal/probe"
	"deployverify/internal/spec"
	"deployverify/internal/store"
)

type Server struct {
	Store        *store.Store
	ProbeBaseURL string
	Workdir      string
	Scheduler    *probe.Scheduler
}

func NewServer(st *store.Store, probeBaseURL, workdir string) *Server {
	return &Server{Store: st, ProbeBaseURL: strings.TrimRight(probeBaseURL, "/"), Workdir: workdir, Scheduler: &probe.Scheduler{}}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, model.NewError(model.CodeNotFound, "route not found", map[string]string{"path": r.URL.Path}))
	})
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /api/workspace/samples", s.writeSamples)
	mux.HandleFunc("GET /api/state", s.state)
	mux.HandleFunc("POST /api/snapshots", s.createSnapshot)
	mux.HandleFunc("GET /api/snapshots", s.listSnapshots)
	mux.HandleFunc("POST /api/increments", s.createIncrement)
	mux.HandleFunc("GET /api/plans", s.plans)
	mux.HandleFunc("GET /api/topology", s.topology)
	mux.HandleFunc("GET /api/diagnostics/dependency-cycle", s.dependencyCycle)
	mux.HandleFunc("GET /api/scores", s.scores)
	mux.HandleFunc("GET /api/refs", s.refs)
	mux.HandleFunc("GET /api/cascade/latest", s.latestCascade)
	mux.HandleFunc("GET /api/diffs", s.diffs)
	mux.HandleFunc("POST /api/probe-rounds", s.createProbeRound)
	mux.HandleFunc("GET /api/probe-runs", s.listProbeRuns)
	mux.HandleFunc("GET /api/probe-runs/{id}", s.getProbeRun)
	mux.HandleFunc("GET /api/intents", s.listIntents)
	mux.HandleFunc("POST /api/admin/recover", s.recover)
	RegisterTestProbeRoutes(mux)
	return jsonContentType(mux)
}

func jsonContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/probe/") {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	version, _ := s.Store.CurrentVersion(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "status": "ok", "currentVersion": version, "probeBaseUrl": s.ProbeBaseURL})
}

func (s *Server) writeSamples(w http.ResponseWriter, r *http.Request) {
	workdir := s.Workdir
	var body struct {
		Workdir string `json:"workdir"`
	}
	_ = decodeBody(r, &body)
	if body.Workdir != "" {
		workdir = body.Workdir
	}
	manifest, err := demo.WriteSamples(workdir)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "manifest": manifest})
}

func (s *Server) state(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.Store.LoadSnapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "snapshot": snapshot})
}

func (s *Server) listSnapshots(w http.ResponseWriter, r *http.Request) {
	items, err := s.Store.ListSnapshots(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "snapshots": items})
}

func (s *Server) createSnapshot(w http.ResponseWriter, r *http.Request) {
	raw, err := readLimited(r)
	if err != nil {
		writeError(w, model.NewError(model.CodeSpecInvalid, "cannot read request body", err.Error()))
		return
	}
	snapshot, err := spec.ParseSnapshot(raw)
	if err != nil {
		writeError(w, err)
		return
	}
	bundle, err := s.Store.CommitSnapshot(r.Context(), snapshot)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"code": "OK", "version": snapshot.Version, "bundle": bundle})
}

func (s *Server) createIncrement(w http.ResponseWriter, r *http.Request) {
	raw, err := readLimited(r)
	if err != nil {
		writeError(w, model.NewError(model.CodeSpecInvalid, "cannot read request body", err.Error()))
		return
	}
	current, err := s.Store.LoadSnapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	increment, err := spec.ParseIncrement(raw, current)
	if err != nil {
		writeError(w, err)
		return
	}
	candidate, err := spec.ApplyIncrement(current, increment)
	if err != nil {
		writeError(w, err)
		return
	}
	changes := make([]derive.Change, 0, len(increment.Changes))
	for _, change := range increment.Changes {
		var old model.Unit
		found := false
		for _, env := range current.Environments {
			if env.Name != change.Environment {
				continue
			}
			for _, unit := range env.Units {
				if unit.Service == change.Service {
					old = unit
					found = true
					break
				}
			}
		}
		fields := []string{}
		if found {
			fields = derive.ChangedFields(old, change.Unit)
		}
		changes = append(changes, derive.Change{Ref: model.Ref{Environment: change.Environment, Service: change.Service}, Fields: fields})
	}
	bundle, err := s.Store.CommitIncrement(r.Context(), store.IncrementCommit{
		Increment: increment, Candidate: candidate, Changes: changes,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"code": "OK", "version": candidate.Version, "baseVersion": increment.BaseVersion, "bundle": bundle})
}

func (s *Server) plans(w http.ResponseWriter, r *http.Request) {
	bundle, err := s.Store.LoadDerived(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	env := r.URL.Query().Get("environment")
	plans := bundle.Plans
	if env != "" {
		filtered := make([]any, 0)
		for _, plan := range plans {
			if plan.Environment == env {
				filtered = append(filtered, plan)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "version": bundle.Version, "plans": filtered})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "version": bundle.Version, "plans": plans})
}

func (s *Server) topology(w http.ResponseWriter, r *http.Request) {
	bundle, err := s.Store.LoadDerived(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	env := r.URL.Query().Get("environment")
	if env != "" {
		for _, topology := range bundle.Topologies {
			if topology.Environment == env {
				response := map[string]any{"code": "OK", "version": bundle.Version, "topology": topology}
				if topology.Status != "complete" {
					response["diagnostic"] = model.NewError(model.CodeDependencyCycle, "dependency cycle detected; partial topology is available", topology)
				}
				writeJSON(w, http.StatusOK, response)
				return
			}
		}
		writeError(w, model.NewError(model.CodeNotFound, "environment topology not found", map[string]string{"environment": env}))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "version": bundle.Version, "topologies": bundle.Topologies})
}

func (s *Server) dependencyCycle(w http.ResponseWriter, r *http.Request) {
	env := r.URL.Query().Get("environment")
	if env == "" {
		writeError(w, model.NewError(model.CodeSpecInvalid, "environment query parameter is required", nil))
		return
	}
	bundle, err := s.Store.LoadDerived(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	for _, topology := range bundle.Topologies {
		if topology.Environment != env {
			continue
		}
		if topology.Status == "complete" {
			writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "topology": topology})
			return
		}
		writeError(w, model.NewError(model.CodeDependencyCycle, "dependency cycle detected; unpolluted layers and blocked units are included", topology))
		return
	}
	writeError(w, model.NewError(model.CodeNotFound, "environment topology not found", map[string]string{"environment": env}))
}

func (s *Server) scores(w http.ResponseWriter, r *http.Request) {
	bundle, err := s.Store.LoadDerived(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "version": bundle.Version, "scores": bundle.Scores})
}

func (s *Server) refs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	refs, err := s.Store.ListRefs(r.Context(), store.RefFilter{
		Environment: query.Get("environment"), Service: query.Get("service"),
		ConfigKey: query.Get("configKey"), ArtifactKey: query.Get("artifactKey"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "count": len(refs), "refs": refs})
}

func (s *Server) latestCascade(w http.ResponseWriter, r *http.Request) {
	report, err := s.Store.LatestCascade(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if report == nil {
		writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "cascade": nil})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "cascade": report})
}

func (s *Server) diffs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	snapshot, err := s.Store.LoadSnapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	tolerance := 0
	if raw := query.Get("toleranceUnits"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeError(w, model.NewError(model.CodeSpecInvalid, "toleranceUnits must be a non-negative integer", nil))
			return
		}
		tolerance = parsed
	}
	report, err := diff.Compare(snapshot, diff.Options{Baseline: query.Get("baseline"), ToleranceUnits: tolerance})
	if err != nil {
		writeError(w, err)
		return
	}
	if report.Drift {
		writeError(w, model.NewError(model.CodeEnvDrift, "one or more environments exceed the declared drift tolerance", report))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "report": report})
}

func (s *Server) listIntents(w http.ResponseWriter, r *http.Request) {
	intents, err := s.Store.ListIntents(r.Context(), r.URL.Query().Get("status"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "count": len(intents), "intents": intents})
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	report, err := s.Store.Recover(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "recovery": report})
}

func (s *Server) listProbeRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.Store.ListProbeRuns(r.Context(), 50)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "runs": runs})
}

func (s *Server) getProbeRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.Store.GetProbeRun(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, model.NewError(model.CodeNotFound, "probe run not found", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": "OK", "run": run})
}

func resolveProbeURL(base, rawPath string) (string, error) {
	if rawPath == "" {
		rawPath = "/probe/ok"
	}
	if strings.HasPrefix(rawPath, "http://") || strings.HasPrefix(rawPath, "https://") {
		parsed, err := url.Parse(rawPath)
		if err != nil {
			return "", err
		}
		rawPath = parsed.RequestURI()
	}
	if !strings.HasPrefix(rawPath, "/") {
		rawPath = "/" + rawPath
	}
	return base + rawPath, nil
}

func decodeBody(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 8<<20))
	return decoder.Decode(target)
}

func readLimited(r *http.Request) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r.Body, 16<<20))
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	var apiErr *model.APIError
	if errors.As(err, &apiErr) {
		status := http.StatusInternalServerError
		switch apiErr.Code {
		case model.CodeSpecInvalid:
			status = http.StatusUnprocessableEntity
		case model.CodeStaleBase, model.CodeEnvDrift, model.CodeConflict:
			status = http.StatusConflict
		case model.CodeDependencyCycle:
			status = http.StatusUnprocessableEntity
		case model.CodeProbeQueueFull:
			status = http.StatusTooManyRequests
		case model.CodeNotFound:
			status = http.StatusNotFound
		}
		writeJSON(w, status, map[string]any{"error": apiErr})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": model.NewError(model.CodeInternal, "internal error", err.Error())})
}

type storeSink struct{ store *store.Store }

func (s storeSink) CommitCompletion(_ context.Context, completion probe.Completion) error {
	return s.store.CommitProbeResult(context.Background(), store.ProbeResultRecord{
		RunID: completion.RunID, Env: completion.Result.Ref.Environment, Service: completion.Result.Ref.Service,
		Status: completion.Result.Status, Code: completion.Result.Code, URL: completion.Result.URL,
		LatencyMS: completion.Result.LatencyMS, Detail: completion.Result.Detail,
	}, completion.ElapsedMS)
}
