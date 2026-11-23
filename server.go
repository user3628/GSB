package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type App struct {
	service    *Service
	scheduler  *Scheduler
	samplePath string
	allowCrash bool
}

func NewApp(service *Service, scheduler *Scheduler, samplePath string, allowCrash bool) *App {
	return &App{service: service, scheduler: scheduler, samplePath: samplePath, allowCrash: allowCrash}
}

func (a *App) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.handleIndex)
	mux.HandleFunc("GET /api/health", a.handleHealth)
	mux.HandleFunc("GET /api/dashboard", a.handleDashboard)
	mux.HandleFunc("GET /api/topics", a.handleListTopics)
	mux.HandleFunc("POST /api/topics", a.handleCreateTopic)
	mux.HandleFunc("GET /api/partitions", a.handleListPartitions)
	mux.HandleFunc("GET /api/partitions/{topic}/{partition}", a.handleGetPartition)
	mux.HandleFunc("POST /api/partitions/{topic}/{partition}/pull", a.handlePull)
	mux.HandleFunc("POST /api/partitions/{topic}/{partition}/commit", a.handleCommit)
	mux.HandleFunc("POST /api/partitions/{topic}/{partition}/replay", a.handleReplay)
	mux.HandleFunc("POST /api/partitions/{topic}/{partition}/retention", a.handleRetention)
	mux.HandleFunc("POST /api/partitions/{topic}/{partition}/priority", a.handlePriority)
	mux.HandleFunc("POST /api/partitions/{topic}/{partition}/force-expire", a.handleForceExpire)
	mux.HandleFunc("GET /api/messages", a.handleListMessages)
	mux.HandleFunc("POST /api/messages", a.handlePublish)
	mux.HandleFunc("POST /api/messages/{messageID}/dead-letter", a.handleDeadLetter)
	mux.HandleFunc("GET /api/txns", a.handleListTxns)
	mux.HandleFunc("GET /api/txns/{txnID}", a.handleGetTxn)
	mux.HandleFunc("POST /api/txns/{txnID}/resolve", a.handleResolveTxn)
	mux.HandleFunc("GET /api/injections", a.handleListInjections)
	mux.HandleFunc("POST /api/injections", a.handleCreateInjection)
	mux.HandleFunc("GET /api/observations", a.handleListObservations)
	mux.HandleFunc("GET /api/scheduler", a.handleSchedulerView)
	mux.HandleFunc("POST /api/scheduler/config", a.handleSchedulerConfig)
	mux.HandleFunc("POST /api/scheduler/tasks", a.handleSchedulerEnqueue)
	mux.HandleFunc("POST /api/scheduler/run", a.handleSchedulerRun)
	mux.HandleFunc("GET /api/state-transitions", a.handleStateTransitions)
	mux.HandleFunc("GET /api/recovery/reports", a.handleRecoveryReports)
	mux.HandleFunc("GET /api/recovery/latest", a.handleRecoveryLatest)
	mux.HandleFunc("POST /api/demo/seed", a.handleSeed)
	mux.HandleFunc("POST /api/debug/crash-after-intent", a.handleCrashAfterIntent)
	mux.HandleFunc("GET /test/txn/{txnID}", a.service.CoordinatorHandler)
	return a.recoverMiddleware(mux)
}

func (a *App) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeAPIError(w, newErrDetails(CodeInternal, "internal server panic",
					map[string]any{"panic": fmt.Sprint(recovered)}))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type apiEnvelope struct {
	OK        bool            `json:"ok"`
	Data      any             `json:"data,omitempty"`
	Error     *WorkbenchError `json:"error,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
}

func writeAPI(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiEnvelope{OK: true, Data: data})
}

func writeAPIRequest(w http.ResponseWriter, status int, data any, requestID string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiEnvelope{OK: true, Data: data, RequestID: requestID})
}

func writeAPIError(w http.ResponseWriter, err error) {
	we := asWorkbenchError(err)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(we.HTTP)
	_ = json.NewEncoder(w).Encode(apiEnvelope{OK: false, Error: we})
}

func decodeJSON(r *http.Request, target any) error {
	if r.Body == nil {
		return newErr(CodeInvalidArgument, "request body is required")
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		return newErr(CodeInvalidArgument, "could not read request body")
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return newErr(CodeInvalidArgument, "request body is required")
	}
	if err := json.Unmarshal(body, target); err != nil {
		return newErrDetails(CodeInvalidArgument, "invalid JSON request body",
			map[string]any{"error": err.Error()})
	}
	return nil
}

func parsePartition(r *http.Request) (string, int, error) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("partition"))
	if err != nil || p < 0 {
		return "", 0, newErr(CodeInvalidArgument, "partition must be a non-negative integer")
	}
	return topic, p, nil
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(webIndex)
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	recovery, _ := a.service.LatestRecoveryReport(r.Context())
	writeAPI(w, http.StatusOK, map[string]any{
		"status": "ok", "time": time.Now().UTC().Format(time.RFC3339Nano),
		"database": "sqlite", "recovery": recovery,
	})
}

func (a *App) handleDashboard(w http.ResponseWriter, r *http.Request) {
	dashboard, err := a.service.DashboardSnapshot(r.Context(), a.scheduler)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, dashboard)
}

func (a *App) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics, err := a.service.ListTopics(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, topics)
}

func (a *App) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var topic Topic
	if err := decodeJSON(r, &topic); err != nil {
		writeAPIError(w, err)
		return
	}
	created, err := a.service.CreateTopic(r.Context(), topic)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusCreated, created)
}

func (a *App) handleListPartitions(w http.ResponseWriter, r *http.Request) {
	partitions, err := a.service.ListPartitions(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, partitions)
}

func (a *App) handleGetPartition(w http.ResponseWriter, r *http.Request) {
	topic, partition, err := parsePartition(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	view, err := a.service.GetPartition(r.Context(), topic, partition)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, view)
}

func (a *App) handlePull(w http.ResponseWriter, r *http.Request) {
	topic, partition, err := parsePartition(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	var req PullRequest
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	out, err := a.service.Pull(r.Context(), topic, partition, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIRequest(w, http.StatusOK, out, out.RequestID)
}

func (a *App) handleCommit(w http.ResponseWriter, r *http.Request) {
	topic, partition, err := parsePartition(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	var req CommitRequest
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	out, err := a.service.Commit(r.Context(), topic, partition, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIRequest(w, http.StatusOK, out, out.RequestID)
}

func (a *App) handleReplay(w http.ResponseWriter, r *http.Request) {
	topic, partition, err := parsePartition(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	var req ReplayRequest
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	out, err := a.service.Replay(r.Context(), topic, partition, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIRequest(w, http.StatusOK, out, out.RequestID)
}

func (a *App) handleRetention(w http.ResponseWriter, r *http.Request) {
	topic, partition, err := parsePartition(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	var req RetainRequest
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	out, err := a.service.RetainFrom(r.Context(), topic, partition, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, out)
}

func (a *App) handlePriority(w http.ResponseWriter, r *http.Request) {
	topic, partition, err := parsePartition(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	var req struct {
		Priority int `json:"priority"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	out, err := a.service.SetPartitionPriority(r.Context(), topic, partition, req.Priority)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, out)
}

func (a *App) handleForceExpire(w http.ResponseWriter, r *http.Request) {
	topic, partition, err := parsePartition(r)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	expired, err := a.service.ForceExpireLease(r.Context(), topic, partition, "debug_force_expire")
	if err != nil {
		writeAPIError(w, err)
		return
	}
	view, err := a.service.GetPartition(r.Context(), topic, partition)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, map[string]any{"expired": expired, "partition": view})
}

func (a *App) handleListMessages(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	partition := -1
	if raw := r.URL.Query().Get("partition"); raw != "" {
		p, err := strconv.Atoi(raw)
		if err != nil {
			writeAPIError(w, newErr(CodeInvalidArgument, "invalid partition query"))
			return
		}
		partition = p
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	messages, err := a.service.ListMessages(r.Context(), topic, partition, limit)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, messages)
}

func (a *App) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req PublishRequest
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	out, err := a.service.Publish(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	status := http.StatusCreated
	if out.Duplicate {
		status = http.StatusOK
	}
	writeAPIRequest(w, status, out, out.RequestID)
}

func (a *App) handleDeadLetter(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RequestID string `json:"request_id"`
	}
	if r.ContentLength > 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeAPIError(w, err)
			return
		}
	}
	message, err := a.service.DeadLetter(r.Context(), r.PathValue("messageID"), req.RequestID)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, message)
}

func (a *App) handleListTxns(w http.ResponseWriter, r *http.Request) {
	pending := r.URL.Query().Get("pending") == "1" || r.URL.Query().Get("pending") == "true"
	txns, err := a.service.ListTransactions(r.Context(), pending)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, txns)
}

func (a *App) handleGetTxn(w http.ResponseWriter, r *http.Request) {
	txn, err := a.service.GetTransaction(r.Context(), r.PathValue("txnID"))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, txn)
}

func (a *App) handleResolveTxn(w http.ResponseWriter, r *http.Request) {
	var req ResolveRequest
	if r.ContentLength > 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeAPIError(w, err)
			return
		}
	}
	out, err := a.service.ResolveTransaction(r.Context(), r.PathValue("txnID"), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPIRequest(w, http.StatusOK, out, out.RequestID)
}

func (a *App) handleListInjections(w http.ResponseWriter, r *http.Request) {
	injections, err := a.service.ListInjections(r.Context(), r.URL.Query().Get("kind"))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, injections)
}

func (a *App) handleCreateInjection(w http.ResponseWriter, r *http.Request) {
	var req InjectionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	inj, err := a.service.CreateInjection(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusCreated, inj)
}

func (a *App) handleListObservations(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	observations, err := a.service.ListObservations(r.Context(), limit)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, observations)
}

func (a *App) handleSchedulerView(w http.ResponseWriter, r *http.Request) {
	writeAPI(w, http.StatusOK, a.scheduler.View())
}

func (a *App) handleSchedulerConfig(w http.ResponseWriter, r *http.Request) {
	var cfg SchedulerConfig
	if err := decodeJSON(r, &cfg); err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, a.scheduler.SetConfig(cfg))
}

func (a *App) handleSchedulerEnqueue(w http.ResponseWriter, r *http.Request) {
	var req SchedulerTaskRequest
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	task, err := a.scheduler.Enqueue(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusCreated, task)
}

func (a *App) handleSchedulerRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		BudgetMS int `json:"budget_ms"`
	}
	if r.ContentLength > 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeAPIError(w, err)
			return
		}
	}
	round, err := a.scheduler.RunRound(r.Context(), req.BudgetMS)
	if err != nil && !errors.Is(err, context.Canceled) {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, round)
}

func (a *App) handleStateTransitions(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	transitions, err := a.service.ListStateTransitions(r.Context(), limit)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, transitions)
}

func (a *App) handleRecoveryReports(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	reports, err := a.service.ListRecoveryReports(r.Context(), limit)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, reports)
}

func (a *App) handleRecoveryLatest(w http.ResponseWriter, r *http.Request) {
	report, err := a.service.LatestRecoveryReport(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, report)
}

func (a *App) handleSeed(w http.ResponseWriter, r *http.Request) {
	if a.samplePath == "" {
		writeAPIError(w, newErr(CodeInvalidArgument, "no sample path configured; start with -seed"))
		return
	}
	spec, err := loadSampleFile(a.samplePath)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := a.service.SeedSample(r.Context(), spec); err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusOK, map[string]any{"seeded": true, "path": a.samplePath,
		"topics": len(spec.Topics), "messages": len(spec.Messages)})
}

func (a *App) handleCrashAfterIntent(w http.ResponseWriter, r *http.Request) {
	if !a.allowCrash {
		writeAPIError(w, newErr(CodeInvalidArgument, "crash injection is disabled; start with -allow-crash"))
		return
	}
	var req struct {
		Operation string `json:"operation"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Owner     string `json:"owner"`
		TxnID     string `json:"txn_id"`
		RequestID string `json:"request_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeAPIError(w, err)
		return
	}
	if req.Owner == "" {
		req.Owner = "crash-consumer"
	}
	if req.RequestID == "" {
		req.RequestID = newID("crash")
	}
	target := fmt.Sprintf("%s/%d", req.Topic, req.Partition)
	var payload any
	switch req.Operation {
	case "pull":
		payload = PullRequest{Owner: req.Owner, Limit: 1}
	case "commit":
		payload = CommitRequest{Owner: req.Owner, ThroughSeq: 0}
	case "replay":
		payload = ReplayRequest{Owner: req.Owner, FromSeq: 0}
	case "txn_resolve":
		payload = ResolveRequest{QueryTimeoutMS: 150}
		target = req.TxnID
	default:
		writeAPIError(w, newErr(CodeInvalidArgument, "operation must be pull, commit, replay, or txn_resolve"))
		return
	}
	decision, err := a.service.prepareOperation(context.Background(), req.RequestID,
		req.Operation, target, payload, nil)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeAPI(w, http.StatusAccepted, map[string]any{"intent_id": decision.Op.IntentID,
		"request_id": decision.RequestID, "message": "process will exit after intent is durable"})
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	time.Sleep(80 * time.Millisecond)
	_, _ = fmt.Fprintln(os.Stderr, "fatal crash injection after durable intent", decision.Op.IntentID)
	os.Exit(75)
}
