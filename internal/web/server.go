package web

import (
	"context"
	"embed"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/batch"
	"driftconverge/internal/detect"
	"driftconverge/internal/engine"
	"driftconverge/internal/plan"
	"driftconverge/internal/simulator"
	"driftconverge/internal/store"
)

//go:embed static/index.html
var assets embed.FS

type App struct {
	Session           *engine.Session
	Simulator         *simulator.Server
	SimulatorEndpoint string
	SimulatorMode     string

	mu               sync.RWMutex
	lastExecution    *engine.ExecutionReport
	lastExecutionErr string
	lastRecovery     *store.RecoveryReport
}

type Status struct {
	Product           string                  `json:"product"`
	Version           string                  `json:"version"`
	SnapshotVersion   int64                   `json:"snapshotVersion"`
	Checksum          string                  `json:"checksum"`
	GeneratedAt       string                  `json:"generatedAt"`
	ObjectCount       int                     `json:"objectCount"`
	DriftObjectCount  int                     `json:"driftObjectCount"`
	DetectionPath     string                  `json:"detectionPath"`
	PathReconciled    bool                    `json:"pathReconciled"`
	ObjectsCompared   int                     `json:"objectsCompared"`
	ExactDurationUs   int64                   `json:"exactDurationMicros"`
	BucketDurationUs  int64                   `json:"bucketDurationMicros"`
	ExactBatchSig     string                  `json:"exactBatchSignature"`
	BucketedBatchSig  string                  `json:"bucketedBatchSignature"`
	BatchPathOK       bool                    `json:"batchPathReconciled"`
	BatchVersionID    int64                   `json:"batchVersionId"`
	BatchSignature    string                  `json:"batchSignature"`
	PlanSignature     string                  `json:"planSignature"`
	Drift             []detect.ObjectDrift    `json:"drift"`
	Buckets           []detect.BucketReport   `json:"buckets"`
	Batches           []batch.Batch           `json:"batches"`
	BatchStats        batch.Stats             `json:"batchStats"`
	Cycles            []batch.Cycle           `json:"cycles"`
	Plan              []plan.BatchPlan        `json:"plan"`
	TotalCost         string                  `json:"totalCost"`
	TotalOccupancy    string                  `json:"totalOccupancy"`
	TotalRounding     string                  `json:"totalRoundingError"`
	SimulatorEndpoint string                  `json:"simulatorEndpoint"`
	SimulatorMode     string                  `json:"simulatorMode"`
	LastExecution     *engine.ExecutionReport `json:"lastExecution,omitempty"`
	LastExecutionErr  string                  `json:"lastExecutionError,omitempty"`
	LastRecovery      *store.RecoveryReport   `json:"lastRecovery,omitempty"`
}

func New(session *engine.Session, sim *simulator.Server, endpoint, mode string) *App {
	return &App{Session: session, Simulator: sim, SimulatorEndpoint: endpoint, SimulatorMode: mode}
}

func (a *App) SetRecovery(report *store.RecoveryReport) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastRecovery = report
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.handleIndex)
	mux.HandleFunc("GET /api/status", a.handleStatus)
	mux.HandleFunc("POST /api/execute", a.handleExecute)
	mux.HandleFunc("POST /api/recover", a.handleRecover)
	return loopbackOnly(mux)
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	data, err := assets.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data)
}

func (a *App) handleStatus(w http.ResponseWriter, r *http.Request) {
	status, err := a.status()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (a *App) status() (*Status, error) {
	analysis := a.Session.Analysis
	if analysis == nil {
		return nil, apperr.New(apperr.InvalidArgument, "no analysis is loaded")
	}
	drift := []detect.ObjectDrift{}
	if analysis.Drift != nil {
		drift = analysis.Drift.Results
	}
	status := &Status{
		Product: "DriftLoom", Version: "1.0.0",
		SnapshotVersion: analysis.SnapshotVersion, Checksum: analysis.SnapshotChecksum, GeneratedAt: analysis.GeneratedAt,
		ObjectCount: analysis.ObjectCount, DriftObjectCount: analysis.DriftObjectCount, DetectionPath: analysis.DetectionPath,
		ObjectsCompared: analysis.Drift.ObjectsCompared, ExactDurationUs: analysis.Drift.ExactDurationUs, BucketDurationUs: analysis.Drift.BucketDurationUs,
		PathReconciled: analysis.Drift.Reconciled, ExactBatchSig: analysis.ExactBatchSignature,
		BucketedBatchSig: analysis.BucketedBatchSignature, BatchPathOK: analysis.BatchPathReconciled,
		BatchVersionID: analysis.BatchVersionID,
		BatchSignature: analysis.BatchSignature, PlanSignature: analysis.PlanSignature,
		Drift: drift, Buckets: analysis.Drift.Buckets,
		SimulatorEndpoint: a.SimulatorEndpoint, SimulatorMode: a.SimulatorMode,
	}
	if analysis.Batches != nil {
		status.Batches = analysis.Batches.Batches
		status.BatchStats = analysis.Batches.Stats
		status.Cycles = analysis.Batches.Cycles
	}
	if analysis.Plan != nil {
		status.Plan = analysis.Plan.Batches
		status.TotalCost = analysis.Plan.TotalEstimatedCost
		status.TotalOccupancy = analysis.Plan.TotalBudgetOccupancy
		status.TotalRounding = analysis.Plan.TotalRoundingError
	}
	a.mu.RLock()
	status.LastExecution = a.lastExecution
	status.LastExecutionErr = a.lastExecutionErr
	status.LastRecovery = a.lastRecovery
	a.mu.RUnlock()
	return status, nil
}

func (a *App) handleExecute(w http.ResponseWriter, r *http.Request) {
	var input struct {
		RoundID string `json:"roundId"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input)
	if strings.TrimSpace(input.RoundID) == "" {
		input.RoundID = "web-round-1"
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	report, err := a.Session.ExecuteRound(ctx, engine.ExecuteOptions{RoundID: input.RoundID, Endpoint: a.SimulatorEndpoint})
	a.mu.Lock()
	a.lastExecution = report
	if err != nil {
		a.lastExecutionErr = err.Error()
	} else {
		a.lastExecutionErr = ""
	}
	a.mu.Unlock()
	response := map[string]any{"ok": err == nil, "execution": report}
	if err != nil {
		response["error"] = errorValue(err)
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *App) handleRecover(w http.ResponseWriter, r *http.Request) {
	report, err := a.Session.Store.Recover(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	a.mu.Lock()
	a.lastRecovery = report
	a.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "recovery": report})
}

func loopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			writeAPIError(w, http.StatusForbidden, apperr.New(apperr.EndpointInvalid, "dashboard accepts loopback clients only"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAPIError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"ok": false, "error": errorValue(err)})
}

func errorValue(err error) any {
	if appErr, ok := apperr.As(err); ok {
		return appErr
	}
	return apperr.Error{Code: apperr.Internal, Message: err.Error()}
}
