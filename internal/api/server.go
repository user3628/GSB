package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"

	"localsearch/multirecall/internal/engine"
	"localsearch/multirecall/internal/model"
	"localsearch/multirecall/internal/sample"
	"localsearch/multirecall/internal/store"
)

const Version = "0.1.0"

type Server struct {
	engine *engine.Engine
	mu     sync.Mutex
	jobs   map[string]*job
	runDB  map[string]string
}

type job struct {
	ID       string
	Status   string
	Stop     chan struct{}
	StopOnce sync.Once
	Result   *model.RunOutput
	Error    *model.AppError
	DBPath   string
	DataDir  string
}

type APIError struct {
	OK    bool            `json:"ok"`
	Error *model.AppError `json:"error"`
}

type APIResult struct {
	OK     bool `json:"ok"`
	Result any  `json:"result,omitempty"`
}

type runRequest struct {
	DataDir                     string `json:"data_dir"`
	DBPath                      string `json:"db_path"`
	Workers                     int    `json:"workers,omitempty"`
	Concurrency                 int    `json:"concurrency,omitempty"`
	Mode                        string `json:"mode,omitempty"`
	VerifyIncremental           *bool  `json:"verify_incremental,omitempty"`
	SkipIncrementalVerification bool   `json:"skip_incremental_verification,omitempty"`
}

type generateRequest struct {
	OutDir    string `json:"out_dir"`
	Seed      int64  `json:"seed"`
	Items     int    `json:"items"`
	Queries   int    `json:"queries"`
	VectorDim int    `json:"vector_dim"`
	Strategy  string `json:"strategy"`
}

func NewServer(eng *engine.Engine) *Server {
	return &Server{engine: eng, jobs: make(map[string]*job), runDB: make(map[string]string)}
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serveHTTP)
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			writeError(w, model.NewError(model.CodeInternal, fmt.Sprintf("panic: %v", recovered), ""))
		}
	}()
	cleanPath := path.Clean(r.URL.Path)
	switch {
	case cleanPath == "/healthz" && r.Method == http.MethodGet:
		writeOK(w, http.StatusOK, map[string]any{"service": "multirecall", "version": Version})
	case cleanPath == "/v1/generate" && r.Method == http.MethodPost:
		s.handleGenerate(w, r)
	case cleanPath == "/v1/runs" && r.Method == http.MethodPost:
		s.handleStartRun(w, r)
	case strings.HasPrefix(cleanPath, "/v1/jobs/") && strings.HasSuffix(cleanPath, "/stop") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(cleanPath, "/v1/jobs/"), "/stop")
		s.handleStopJob(w, id)
	case strings.HasPrefix(cleanPath, "/v1/jobs/") && strings.HasSuffix(cleanPath, "/result") && r.Method == http.MethodGet:
		id := strings.TrimSuffix(strings.TrimPrefix(cleanPath, "/v1/jobs/"), "/result")
		s.handleJobResult(w, id)
	case strings.HasPrefix(cleanPath, "/v1/jobs/") && r.Method == http.MethodGet:
		s.handleGetJob(w, strings.TrimPrefix(cleanPath, "/v1/jobs/"))
	case strings.HasPrefix(cleanPath, "/v1/runs/") && strings.HasSuffix(cleanPath, "/summary") && r.Method == http.MethodGet:
		id := strings.TrimSuffix(strings.TrimPrefix(cleanPath, "/v1/runs/"), "/summary")
		s.handleRunSummary(w, id)
	case strings.HasPrefix(cleanPath, "/v1/runs/") && strings.HasSuffix(cleanPath, "/checkpoints") && r.Method == http.MethodGet:
		id := strings.TrimSuffix(strings.TrimPrefix(cleanPath, "/v1/runs/"), "/checkpoints")
		s.handleCheckpoints(w, r, id)
	default:
		writeError(w, model.NewError(model.CodeHTTP, "route not found", ""))
	}
}

func (s *Server) handleGenerate(w http.ResponseWriter, r *http.Request) {
	var request generateRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, model.NewError(model.CodeHTTP, err.Error(), ""))
		return
	}
	manifest, err := sample.Generate(sample.GenerateOptions{
		OutDir: request.OutDir, Seed: request.Seed, ItemCount: request.Items,
		QueryCount: request.Queries, VectorDim: request.VectorDim, Strategy: request.Strategy,
	})
	if err != nil {
		writeError(w, model.AsAppError(err))
		return
	}
	writeOK(w, http.StatusCreated, manifest)
}

func (s *Server) handleStartRun(w http.ResponseWriter, r *http.Request) {
	var request runRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, model.NewError(model.CodeHTTP, err.Error(), ""))
		return
	}
	if request.DataDir == "" || request.DBPath == "" {
		writeError(w, model.NewError(model.CodeInvalidConfig, "data_dir and db_path are required", ""))
		return
	}
	skipVerification := request.SkipIncrementalVerification
	if request.VerifyIncremental != nil && !*request.VerifyIncremental {
		skipVerification = true
	}
	jobID, err := randomID()
	if err != nil {
		writeError(w, model.NewError(model.CodeInternal, "create job id: "+err.Error(), ""))
		return
	}
	jobRecord := &job{ID: jobID, Status: "running", Stop: make(chan struct{}), DBPath: request.DBPath, DataDir: request.DataDir}
	s.mu.Lock()
	s.jobs[jobID] = jobRecord
	s.mu.Unlock()
	go func() {
		output, runErr := s.engine.Run(context.Background(), engine.Request{
			DataDir: request.DataDir, DBPath: request.DBPath, Workers: request.Workers,
			Concurrency: request.Concurrency, Mode: request.Mode,
			SkipIncrementalVerification: skipVerification, Stop: jobRecord.Stop,
		})
		s.mu.Lock()
		defer s.mu.Unlock()
		if runErr != nil {
			jobRecord.Status = "failed"
			jobRecord.Error = model.AsAppError(runErr)
			return
		}
		jobRecord.Result = output
		jobRecord.Status = "completed"
		if output.Stopped {
			jobRecord.Status = "interrupted"
		}
		s.runDB[output.RunID] = request.DBPath
	}()
	writeOK(w, http.StatusAccepted, map[string]any{"job_id": jobID, "status": jobRecord.Status})
}

func (s *Server) handleGetJob(w http.ResponseWriter, id string) {
	s.mu.Lock()
	jobEntry, ok := s.jobs[id]
	var snapshot struct {
		Status string
		Result *model.RunOutput
		Error  *model.AppError
	}
	if ok {
		snapshot.Status = jobEntry.Status
		snapshot.Result = jobEntry.Result
		snapshot.Error = jobEntry.Error
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, model.NewError(model.CodeDataLoad, "job not found", ""))
		return
	}
	response := map[string]any{"job_id": id, "status": snapshot.Status}
	if snapshot.Error != nil {
		response["error"] = snapshot.Error
	}
	if snapshot.Result != nil {
		response["run_id"] = snapshot.Result.RunID
		response["summary"] = snapshot.Result.Summary
	}
	writeOK(w, http.StatusOK, response)
}

func (s *Server) handleStopJob(w http.ResponseWriter, id string) {
	s.mu.Lock()
	job, ok := s.jobs[id]
	s.mu.Unlock()
	if !ok {
		writeError(w, model.NewError(model.CodeDataLoad, "job not found", ""))
		return
	}
	job.StopOnce.Do(func() { close(job.Stop) })
	writeOK(w, http.StatusAccepted, map[string]any{"job_id": id, "status": "stopping"})
}

func (s *Server) handleJobResult(w http.ResponseWriter, id string) {
	s.mu.Lock()
	jobEntry, ok := s.jobs[id]
	var output *model.RunOutput
	var appErr *model.AppError
	if ok {
		output = jobEntry.Result
		appErr = jobEntry.Error
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, model.NewError(model.CodeDataLoad, "job not found", ""))
		return
	}
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	if output == nil {
		writeError(w, model.NewError(model.CodeHTTP, "job has not completed", ""))
		return
	}
	writeOK(w, http.StatusOK, output)
}

func (s *Server) handleRunSummary(w http.ResponseWriter, runID string) {
	dbPath, ok := s.runPath(runID)
	if !ok {
		writeError(w, model.NewError(model.CodeDataLoad, "run is not associated with this HTTP server", runID))
		return
	}
	st, err := store.Open(dbPath)
	if err != nil {
		writeError(w, model.AsAppError(err))
		return
	}
	defer st.Close()
	record, err := st.GetRun(runID)
	if err != nil {
		writeError(w, model.AsAppError(err))
		return
	}
	var summary any
	if record.SummaryJSON != "" {
		_ = json.Unmarshal([]byte(record.SummaryJSON), &summary)
	}
	writeOK(w, http.StatusOK, map[string]any{
		"run_id": record.RunID, "status": record.Status, "summary": summary,
		"summary_digest": record.SummaryDigest,
	})
}

func (s *Server) handleCheckpoints(w http.ResponseWriter, r *http.Request, runID string) {
	queryID := r.URL.Query().Get("query_id")
	if queryID == "" {
		writeError(w, model.NewError(model.CodeHTTP, "query_id is required", ""))
		return
	}
	dbPath, ok := s.runPath(runID)
	if !ok {
		writeError(w, model.NewError(model.CodeDataLoad, "run is not associated with this HTTP server", runID))
		return
	}
	st, err := store.Open(dbPath)
	if err != nil {
		writeError(w, model.AsAppError(err))
		return
	}
	defer st.Close()
	checkpoints, err := st.ListCheckpoints(r.Context(), queryID)
	if err != nil {
		writeError(w, model.AsAppError(err))
		return
	}
	for i := range checkpoints {
		checkpoints[i].Payload = nil
	}
	writeOK(w, http.StatusOK, map[string]any{"run_id": runID, "query_id": queryID, "checkpoints": checkpoints})
}

func (s *Server) runPath(runID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dbPath, ok := s.runDB[runID]
	return dbPath, ok
}

func randomID() (string, error) {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func decodeJSON(r *http.Request, target any) error {
	if r.Body == nil {
		return fmt.Errorf("request body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

func writeOK(w http.ResponseWriter, status int, result any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(APIResult{OK: true, Result: result})
}

func writeError(w http.ResponseWriter, appErr *model.AppError) {
	if appErr == nil {
		appErr = model.NewError(model.CodeInternal, "unknown error", "")
	}
	status := http.StatusInternalServerError
	switch appErr.Code {
	case model.CodeHTTP, model.CodeInvalidConfig, model.CodeInvalidWeight, model.CodeEmptyInput, model.CodeDataLoad:
		status = http.StatusBadRequest
	case model.CodeMissingPrerequisite, model.CodeStaleCheckpoint, model.CodeIllegalStageTransition, model.CodeIncrementalMismatch:
		status = http.StatusConflict
	case model.CodeWorkerFailure:
		status = http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(APIError{OK: false, Error: appErr})
}
