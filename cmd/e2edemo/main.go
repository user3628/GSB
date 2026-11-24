package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"local/sqlitepool/internal/domain"
	"local/sqlitepool/internal/sample"
)

type envelope struct {
	OK        bool            `json:"ok"`
	RequestID string          `json:"request_id"`
	Data      json.RawMessage `json:"data"`
	Error     *struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Context map[string]any `json:"context"`
	} `json:"error"`
}

type apiCall struct {
	status int
	body   []byte
	env    envelope
}

type client struct {
	base string
	hc   *http.Client
}

func newClient(base string) *client {
	return &client{base: base, hc: &http.Client{Timeout: 30 * time.Second}}
}

func (c *client) do(method, path, requestID string, body any) (*apiCall, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	response, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	call := &apiCall{status: response.StatusCode, body: data}
	if err := json.Unmarshal(data, &call.env); err != nil {
		return nil, fmt.Errorf("decode %s %s response: %w body=%s", method, path, err, data)
	}
	return call, nil
}

func (c *client) post(path, requestID string, body any) (*apiCall, error) {
	return c.do(http.MethodPost, path, requestID, body)
}

func (c *client) get(path string) (*apiCall, error) {
	return c.do(http.MethodGet, path, "", nil)
}

type serverProcess struct {
	cmd     *exec.Cmd
	logPath string
	logFile *os.File
}

func startServer(bin, samplePath, dataDir, addr string, number int) (*serverProcess, error) {
	logPath := filepath.Join(dataDir, fmt.Sprintf("server-%d.log", number))
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin,
		"-sample", samplePath,
		"-data-dir", dataDir,
		"-addr", addr,
		"-allow-debug",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	sp := &serverProcess{cmd: cmd, logPath: logPath, logFile: logFile}
	if err := waitHealth(addr, sp); err != nil {
		_ = sp.kill()
		return nil, err
	}
	return sp, nil
}

func waitHealth(addr string, sp *serverProcess) error {
	url := "http://" + addr + "/healthz"
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	logData, _ := os.ReadFile(sp.logPath)
	return fmt.Errorf("server did not become healthy at %s; log:\n%s", addr, tail(string(logData), 4000))
}

func (s *serverProcess) kill() error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	err := s.cmd.Process.Kill()
	_ = s.cmd.Wait()
	if s.logFile != nil {
		_ = s.logFile.Close()
	}
	return err
}

func (s *serverProcess) stop() error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	err := s.cmd.Process.Signal(os.Interrupt)
	if waitErr := s.cmd.Wait(); waitErr != nil && err == nil {
		err = waitErr
	}
	if s.logFile != nil {
		_ = s.logFile.Close()
	}
	return err
}

type stepRecord struct {
	Step        string         `json:"step"`
	OK          bool           `json:"ok"`
	Observation map[string]any `json:"observation,omitempty"`
	Snapshot    map[string]any `json:"pool_snapshot,omitempty"`
	Error       string         `json:"error,omitempty"`
}

type runner struct {
	samplePath string
	dataDir    string
	addr       string
	serverBin  string
	sample     sample.Sample
	client     *client
	server     *serverProcess
	steps      []stepRecord
	serverSeq  int
}

func main() {
	var samplePath, dataDir, addr, serverBin, resultFile string
	flag.StringVar(&samplePath, "sample", "samples/pool_sample.json", "sample file")
	flag.StringVar(&dataDir, "data-dir", "", "fresh data directory")
	flag.StringVar(&addr, "addr", "127.0.0.1:19090", "loopback listen address")
	flag.StringVar(&serverBin, "server-bin", "", "prebuilt poolserver binary")
	flag.StringVar(&resultFile, "result-file", "", "JSON verification result file")
	flag.Parse()
	if dataDir == "" || serverBin == "" {
		fatalf("-data-dir and -server-bin are required")
	}
	s, err := sample.Load(samplePath)
	if err != nil {
		fatalf("load sample: %v", err)
	}
	if _, err := os.Stat(serverBin); err != nil {
		fatalf("server binary: %v", err)
	}
	r := &runner{
		samplePath: samplePath,
		dataDir:    dataDir,
		addr:       addr,
		serverBin:  serverBin,
		sample:     s,
		client:     newClient("http://" + addr),
	}
	runErr := r.run()
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "e2e failed: %v\n", runErr)
	}
	r.writeResult(resultFile, runErr)
	if runErr != nil {
		os.Exit(1)
	}
}

func (r *runner) run() (runErr error) {
	if err := checkPortFree(r.addr); err != nil {
		return err
	}
	server, err := r.start()
	if err != nil {
		return err
	}
	defer func() {
		if r.server != nil {
			_ = r.server.stop()
			r.server = nil
		}
	}()
	_ = server
	if err := r.stepConcurrentAndRelease(); err != nil {
		return err
	}
	if err := r.stepFIFOWake(); err != nil {
		return err
	}
	if err := r.stepInjectionsAndControlPlane(); err != nil {
		return err
	}
	if err := r.stepForcedKillRecovery(); err != nil {
		return err
	}
	return nil
}

func (r *runner) start() (*serverProcess, error) {
	r.serverSeq++
	sp, err := startServer(r.serverBin, r.samplePath, r.dataDir, r.addr, r.serverSeq)
	if err != nil {
		return nil, err
	}
	r.server = sp
	return sp, nil
}

func (r *runner) record(step string, err error, observation map[string]any) {
	rec := stepRecord{Step: step, OK: err == nil, Observation: observation}
	if err != nil {
		rec.Error = err.Error()
	}
	if snap, snapErr := r.snapshot(); snapErr == nil {
		rec.Snapshot = snap
	}
	r.steps = append(r.steps, rec)
	compact := map[string]any{
		"step":        step,
		"ok":          err == nil,
		"observation": compactObservation(step, observation),
	}
	if rec.Snapshot != nil {
		compact["pool_stats"] = rec.Snapshot["stats"]
	}
	if err != nil {
		compact["error"] = err.Error()
	}
	data, _ := json.Marshal(compact)
	fmt.Println(string(data))
}

func compactObservation(step string, observation map[string]any) map[string]any {
	switch step {
	case "exception_injections_and_control_plane":
		injections, _ := observation["injections"].([]map[string]any)
		var types []string
		var ids []string
		for _, item := range injections {
			if value, ok := item["type"].(string); ok {
				types = append(types, value)
			}
			if value, ok := item["injection_id"].(string); ok {
				ids = append(ids, value)
			}
		}
		return map[string]any{
			"distinct_codes": observation["distinct_codes"],
			"types":          types,
			"injection_ids":  ids,
		}
	case "sigkill_restart_recovery":
		report, _ := observation["recovery_report"].(domain.RecoveryReport)
		return map[string]any{
			"pending_intents_found":        report.PendingIntentsFound,
			"intents":                      report.Intents,
			"metadata_states_restored":     report.MetadataStatesRestored,
			"runtime_handles_preserved":    report.RuntimeHandlesPreserved,
			"idle_queue_order_recoverable": report.IdleQueueOrderRecoverable,
			"facts_only":                   report.FactsOnly,
			"recovery_report_id":           report.ReportID,
		}
	default:
		return observation
	}
}

func (r *runner) snapshot() (map[string]any, error) {
	call, err := r.client.get("/v1/pool/stats")
	if err != nil {
		return nil, err
	}
	if call.status != http.StatusOK {
		return nil, fmt.Errorf("stats status %d: %s", call.status, call.body)
	}
	var data map[string]any
	if err := json.Unmarshal(call.env.Data, &data); err != nil {
		return nil, err
	}
	return data, nil
}

type leaseData struct {
	ConnectionID   string    `json:"connection_id"`
	CreatedAt      time.Time `json:"created_at"`
	AcquiredAt     time.Time `json:"acquired_at"`
	UseCount       int64     `json:"use_count"`
	WaitDurationMS int64     `json:"wait_duration_ms"`
	WaiterID       string    `json:"waiter_id"`
}

func leaseFromCall(call *apiCall) (leaseData, error) {
	if call.status != http.StatusOK {
		return leaseData{}, fmt.Errorf("acquire failed status=%d body=%s", call.status, call.body)
	}
	var data struct {
		Lease leaseData `json:"lease"`
	}
	if err := json.Unmarshal(call.env.Data, &data); err != nil {
		return leaseData{}, err
	}
	if data.Lease.ConnectionID == "" {
		return leaseData{}, errors.New("lease has no connection_id")
	}
	return data.Lease, nil
}

type interval struct {
	conn        string
	start, end  time.Time
	worker, req int
}

func (r *runner) stepConcurrentAndRelease() error {
	cfg := r.sample.Concurrent
	if cfg.Workers <= 0 || cfg.RequestsPerWorker <= 0 {
		return errors.New("sample concurrent workers and requests_per_worker must be positive")
	}
	var mu sync.Mutex
	var intervals []interval
	var failures []string
	var wg sync.WaitGroup
	start := time.Now()
	for worker := 0; worker < cfg.Workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := 0; req < cfg.RequestsPerWorker; req++ {
				rid := fmt.Sprintf("e2e-conc-%d-%d", worker, req)
				leaseCall, err := r.client.post("/v1/acquire", rid+"-acq", map[string]any{
					"timeout_ms": cfg.AcquireTimeoutMS,
				})
				if err != nil {
					mu.Lock()
					failures = append(failures, err.Error())
					mu.Unlock()
					return
				}
				lease, err := leaseFromCall(leaseCall)
				if err != nil {
					mu.Lock()
					failures = append(failures, err.Error())
					mu.Unlock()
					return
				}
				queryCall, err := r.client.post("/v1/query", rid+"-query", map[string]any{
					"conn_id": lease.ConnectionID,
					"sql":     "SELECT 42 AS answer, sqlite_version() AS version",
				})
				if err != nil || queryCall.status != http.StatusOK {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("query %s: err=%v status=%d body=%s", rid, err, callStatus(queryCall), callBody(queryCall)))
					mu.Unlock()
					return
				}
				releaseCall, err := r.client.post("/v1/release", rid+"-rel", map[string]any{
					"conn_id": lease.ConnectionID,
				})
				if err != nil || releaseCall.status != http.StatusOK {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("release %s: err=%v status=%d body=%s", rid, err, callStatus(releaseCall), callBody(releaseCall)))
					mu.Unlock()
					return
				}
				var releaseData struct {
					Release struct {
						ReleasedAt time.Time `json:"released_at"`
					} `json:"release"`
				}
				if err := json.Unmarshal(releaseCall.env.Data, &releaseData); err != nil {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("decode release: %v", err))
					mu.Unlock()
					return
				}
				mu.Lock()
				intervals = append(intervals, interval{
					conn: lease.ConnectionID, start: lease.AcquiredAt, end: releaseData.Release.ReleasedAt,
					worker: worker, req: req,
				})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].conn == intervals[j].conn {
			return intervals[i].start.Before(intervals[j].start)
		}
		return intervals[i].conn < intervals[j].conn
	})
	overlapDetected := false
	for i := 1; i < len(intervals); i++ {
		if intervals[i].conn == intervals[i-1].conn && intervals[i].start.Before(intervals[i-1].end) {
			overlapDetected = true
		}
	}
	uniq := map[string]bool{}
	for _, item := range intervals {
		uniq[item.conn] = true
	}
	maxObservedConcurrency := maxConcurrency(intervals)
	obs := map[string]any{
		"workers":                 cfg.Workers,
		"requests_per_worker":     cfg.RequestsPerWorker,
		"completed_leases":        len(intervals),
		"distinct_connections":    len(uniq),
		"max_overlapping_leases":  maxObservedConcurrency,
		"configured_max_open":     r.sample.InitialPool.MaxOpen,
		"same_connection_overlap": overlapDetected,
		"elapsed_ms":              time.Since(start).Milliseconds(),
	}
	var err error
	if len(failures) > 0 {
		err = fmt.Errorf("concurrent failures: %s", strings.Join(failures, "; "))
	} else if overlapDetected {
		err = errors.New("same connection was observed under two overlapping leases")
	} else if maxObservedConcurrency > r.sample.InitialPool.MaxOpen {
		err = fmt.Errorf("observed concurrency %d exceeds max_open %d", maxObservedConcurrency, r.sample.InitialPool.MaxOpen)
	}
	r.record("concurrent_acquire_query_release", err, obs)
	return err
}

func (r *runner) stepFIFOWake() error {
	snap, err := r.snapshot()
	if err != nil {
		return err
	}
	var stats struct {
		Stats domain.PoolStats `json:"stats"`
	}
	raw, _ := json.Marshal(snap)
	if err := json.Unmarshal(raw, &stats); err != nil {
		return err
	}
	initialWakeEvents, _ := countWakeEvents(snap)
	cfg := stats.Stats.Config
	if cfg.MaxOpen < 2 || cfg.MaxWaiters < 2 {
		return fmt.Errorf("fifo sample needs max_open>=2 and max_waiters>=2, got %+v", cfg)
	}
	holds := make([]leaseData, 0, cfg.MaxOpen)
	for i := 0; i < cfg.MaxOpen; i++ {
		call, err := r.client.post("/v1/acquire", fmt.Sprintf("e2e-fifo-hold-%d", i), map[string]any{"timeout_ms": 5000})
		if err != nil {
			return err
		}
		lease, err := leaseFromCall(call)
		if err != nil {
			return err
		}
		holds = append(holds, lease)
	}
	waitOne := make(chan *apiCall, 1)
	waitTwo := make(chan *apiCall, 1)
	go func() {
		call, _ := r.client.post("/v1/acquire", "e2e-fifo-waiter-1", map[string]any{"timeout_ms": 5000})
		waitOne <- call
	}()
	if err := r.waitQueueDepth(1); err != nil {
		return err
	}
	go func() {
		call, _ := r.client.post("/v1/acquire", "e2e-fifo-waiter-2", map[string]any{"timeout_ms": 5000})
		waitTwo <- call
	}()
	if err := r.waitQueueDepth(2); err != nil {
		return err
	}
	releaseOne, err := r.client.post("/v1/release", "e2e-fifo-release-hold-0", map[string]any{"conn_id": holds[0].ConnectionID})
	if err != nil {
		return err
	}
	if releaseOne.status != http.StatusOK {
		return fmt.Errorf("release hold 0 failed: %s", releaseOne.body)
	}
	var releaseOneData struct {
		Release domain.ReleaseResult `json:"release"`
	}
	if err := json.Unmarshal(releaseOne.env.Data, &releaseOneData); err != nil {
		return err
	}
	if releaseOneData.Release.Disposition != domain.DispositionHandoff {
		return fmt.Errorf("release did not hand off to FIFO waiter: %+v", releaseOneData.Release)
	}
	var waiterOne *apiCall
	select {
	case waiterOne = <-waitOne:
	case <-time.After(6 * time.Second):
		return errors.New("first FIFO waiter was not woken")
	}
	leaseOne, err := leaseFromCall(waiterOne)
	if err != nil {
		return fmt.Errorf("first waiter: %w", err)
	}
	if leaseOne.ConnectionID != holds[0].ConnectionID {
		return fmt.Errorf("first waiter got %s, expected handed-off %s", leaseOne.ConnectionID, holds[0].ConnectionID)
	}
	releaseWaiterOne, err := r.client.post("/v1/release", "e2e-fifo-release-waiter-1", map[string]any{"conn_id": leaseOne.ConnectionID})
	if err != nil {
		return err
	}
	if releaseWaiterOne.status != http.StatusOK {
		return fmt.Errorf("release waiter one failed: %s", releaseWaiterOne.body)
	}
	var releaseWaiterOneData struct {
		Release domain.ReleaseResult `json:"release"`
	}
	if err := json.Unmarshal(releaseWaiterOne.env.Data, &releaseWaiterOneData); err != nil {
		return err
	}
	if releaseWaiterOneData.Release.Disposition != domain.DispositionHandoff {
		return fmt.Errorf("second wake was not a handoff: %+v", releaseWaiterOneData.Release)
	}
	var waiterTwo *apiCall
	select {
	case waiterTwo = <-waitTwo:
	case <-time.After(6 * time.Second):
		return errors.New("second FIFO waiter was not woken")
	}
	leaseTwo, err := leaseFromCall(waiterTwo)
	if err != nil {
		return fmt.Errorf("second waiter: %w", err)
	}
	if leaseTwo.ConnectionID != leaseOne.ConnectionID {
		return fmt.Errorf("second waiter got %s, expected %s", leaseTwo.ConnectionID, leaseOne.ConnectionID)
	}
	if _, err := r.client.post("/v1/release", "e2e-fifo-release-waiter-2", map[string]any{"conn_id": leaseTwo.ConnectionID}); err != nil {
		return err
	}
	if _, err := r.client.post("/v1/release", "e2e-fifo-release-hold-1", map[string]any{"conn_id": holds[1].ConnectionID}); err != nil {
		return err
	}
	snapAfter, err := r.snapshot()
	if err != nil {
		return err
	}
	events, _ := extractEvents(snapAfter)
	wakeEvents := 0
	for _, event := range events {
		if event["type"] == "acquire_wake" {
			wakeEvents++
		}
	}
	newWakeEvents := wakeEvents - initialWakeEvents
	obs := map[string]any{
		"held_connections":                     []string{holds[0].ConnectionID, holds[1].ConnectionID},
		"waiter_one_connection":                leaseOne.ConnectionID,
		"waiter_two_connection":                leaseTwo.ConnectionID,
		"first_handoff_waiter":                 releaseOneData.Release.HandoffTo,
		"second_handoff_waiter":                releaseWaiterOneData.Release.HandoffTo,
		"observable_acquire_wake_events_total": wakeEvents,
		"new_acquire_wake_events":              newWakeEvents,
		"final_waiting":                        queueWaiters(snapAfter),
	}
	var stepErr error
	if newWakeEvents < 2 {
		stepErr = fmt.Errorf("expected at least 2 new acquire_wake events, got %d", newWakeEvents)
	} else if queueWaiters(snapAfter) != 0 {
		stepErr = fmt.Errorf("residual waiters after FIFO wake: %d", queueWaiters(snapAfter))
	}
	r.record("fifo_wait_and_real_wakeup", stepErr, obs)
	return stepErr
}

func (r *runner) stepInjectionsAndControlPlane() error {
	type spec struct {
		name   string
		code   string
		status int
		params map[string]any
	}
	specs := []spec{
		{name: "dial_failed", code: domain.CodeDialFailed, status: http.StatusServiceUnavailable, params: r.sample.Injections["dial_failed"]},
		{name: "query_timeout", code: domain.CodeQueryTimeout, status: http.StatusGatewayTimeout, params: r.sample.Injections["query_timeout"]},
		{name: "conn_reset", code: domain.CodeConnReset, status: http.StatusBadGateway, params: r.sample.Injections["conn_reset"]},
		{name: "wait_queue_overflow", code: domain.CodePoolExhausted, status: http.StatusTooManyRequests, params: r.sample.Injections["wait_queue_overflow"]},
		{name: "acquire_timeout", code: domain.CodeAcquireTimeout, status: http.StatusRequestTimeout, params: r.sample.Injections["acquire_timeout"]},
	}
	seen := map[string]bool{}
	var observations []map[string]any
	var firstInjection *apiCall
	var firstInjectionID, firstRequestID string
	var firstBody []byte
	var stepErr error
	for _, sp := range specs {
		if sp.params == nil {
			sp.params = map[string]any{}
		}
		requestID := "e2e-inj-" + sp.name
		before, _ := r.snapshot()
		call, err := r.client.post("/v1/injections", requestID, map[string]any{
			"type": sp.name, "params": sp.params,
		})
		if err != nil {
			stepErr = err
			break
		}
		after, _ := r.snapshot()
		obs := map[string]any{
			"type": sp.name, "request_id": requestID, "status": call.status,
			"raw":    json.RawMessage(call.body),
			"before": before, "after": after,
		}
		if call.status != sp.status || call.env.Error == nil || call.env.Error.Code != sp.code {
			stepErr = fmt.Errorf("%s expected %s/%d, got code=%s status=%d body=%s", sp.name, sp.code, sp.status,
				errorCode(call), call.status, call.body)
			observations = append(observations, obs)
			break
		}
		if call.env.Error.Context["observation"] == nil {
			stepErr = fmt.Errorf("%s did not bind observation context", sp.name)
			observations = append(observations, obs)
			break
		}
		injectionID, _ := call.env.Error.Context["injection_id"].(string)
		obs["injection_id"] = injectionID
		recordCall, err := r.client.get("/v1/injections/" + injectionID)
		if err != nil || recordCall.status != http.StatusOK {
			stepErr = fmt.Errorf("injection record %s unavailable: err=%v status=%d body=%s", injectionID, err, callStatus(recordCall), callBody(recordCall))
			observations = append(observations, obs)
			break
		}
		var recordData struct {
			Injection domain.InjectionRecord `json:"injection"`
		}
		if err := json.Unmarshal(recordCall.env.Data, &recordData); err != nil {
			stepErr = err
			observations = append(observations, obs)
			break
		}
		obs["record"] = recordData.Injection
		if recordData.Injection.BusinessCode != sp.code || recordData.Injection.Status != "completed" {
			stepErr = fmt.Errorf("injection record mismatch for %s: %+v", sp.name, recordData.Injection)
			observations = append(observations, obs)
			break
		}
		obs["residual_waiters"] = queueWaiters(after)
		if sp.name == "wait_queue_overflow" || sp.name == "acquire_timeout" {
			if queueWaiters(after) != 0 {
				stepErr = fmt.Errorf("%s left residual waiters: %d", sp.name, queueWaiters(after))
				observations = append(observations, obs)
				break
			}
		}
		if sp.name == "conn_reset" {
			detail := injectionObservation(call)
			oldID, _ := detail["connection_id"].(string)
			replacement, _ := detail["replacement_connection_id"].(string)
			if oldID == "" || replacement == "" || oldID == replacement {
				stepErr = fmt.Errorf("conn reset did not remove/replace deterministically: %+v", detail)
				observations = append(observations, obs)
				break
			}
			connectionsCall, err := r.client.get("/v1/pool/connections")
			if err != nil || connectionsCall.status != http.StatusOK {
				stepErr = fmt.Errorf("connection details unavailable: %v", err)
				observations = append(observations, obs)
				break
			}
			var connectionsData struct {
				Connections []domain.ConnectionRecord `json:"connections"`
			}
			if err := json.Unmarshal(connectionsCall.env.Data, &connectionsData); err != nil {
				stepErr = err
				observations = append(observations, obs)
				break
			}
			var oldRecord *domain.ConnectionRecord
			var replacementRecord *domain.ConnectionRecord
			for i := range connectionsData.Connections {
				if connectionsData.Connections[i].ConnectionID == oldID {
					oldRecord = &connectionsData.Connections[i]
				}
				if connectionsData.Connections[i].ConnectionID == replacement {
					replacementRecord = &connectionsData.Connections[i]
				}
			}
			if oldRecord == nil || oldRecord.State != domain.StateInvalid {
				stepErr = fmt.Errorf("force-closed connection is still reusable: %+v", oldRecord)
				observations = append(observations, obs)
				break
			}
			if oldRecord.ReplacedBy != replacement || oldRecord.ReplacementReason == "" {
				stepErr = fmt.Errorf("force-closed connection lacks replacement link: %+v", oldRecord)
				observations = append(observations, obs)
				break
			}
			if replacementRecord == nil || replacementRecord.ReplacedConnID != oldID ||
				replacementRecord.ReplacementReason != oldRecord.ReplacementReason {
				stepErr = fmt.Errorf("replacement connection lacks matching reason: old=%+v new=%+v", oldRecord, replacementRecord)
				observations = append(observations, obs)
				break
			}
			obs["old_connection"] = oldRecord
			obs["replacement_connection"] = replacementRecord
			obs["replacement_connection_id"] = replacement
		}
		seen[call.env.Error.Code] = true
		if firstInjection == nil {
			firstInjection = call
			firstInjectionID = injectionID
			firstRequestID = requestID
			firstBody = append([]byte(nil), call.body...)
		}
		observations = append(observations, obs)
	}
	if stepErr == nil && len(seen) != len(specs) {
		stepErr = fmt.Errorf("injection business codes are not distinct: %+v", seen)
	}
	if stepErr == nil {
		replay, err := r.client.post("/v1/injections", firstRequestID, map[string]any{
			"type": "dial_failed", "params": map[string]any{},
		})
		if err != nil {
			stepErr = err
		} else if replay.status != firstInjection.status || !bytes.Equal(replay.body, firstBody) {
			stepErr = fmt.Errorf("idempotent injection replay was not reused: status=%d body=%s", replay.status, replay.body)
		} else if replay.env.Error == nil || replay.env.Error.Context["injection_id"] != firstInjectionID {
			stepErr = fmt.Errorf("idempotent replay changed injection identity: %s", replay.body)
		}
	}
	if stepErr == nil {
		configCall, err := r.client.get("/v1/config")
		if err != nil {
			stepErr = err
		} else {
			var configData struct {
				Config domain.Config `json:"config"`
			}
			if err := json.Unmarshal(configCall.env.Data, &configData); err != nil {
				stepErr = err
			} else {
				stale, err := r.client.do(http.MethodPut, "/v1/config", "e2e-stale-config", configData.Config)
				if err != nil {
					stepErr = err
				} else if stale.status != http.StatusConflict || errorCode(stale) != domain.CodeStaleConfig {
					stepErr = fmt.Errorf("stale config expected E_STALE_CONFIG, got %d %s", stale.status, stale.body)
				}
			}
		}
	}
	if stepErr == nil {
		conflict, err := r.client.post("/v1/injections", firstRequestID, map[string]any{
			"type": "query_timeout", "params": map[string]any{},
		})
		if err != nil {
			stepErr = err
		} else if conflict.status != http.StatusConflict || errorCode(conflict) != domain.CodeConflictingRequest {
			stepErr = fmt.Errorf("request conflict expected E_CONFLICTING_REQUEST, got %d %s", conflict.status, conflict.body)
		}
	}
	if stepErr != nil {
		stepErr = fmt.Errorf("%w", stepErr)
	}
	r.record("exception_injections_and_control_plane", stepErr, map[string]any{
		"injections":     observations,
		"distinct_codes": mapKeys(seen),
	})
	return stepErr
}

func (r *runner) stepForcedKillRecovery() error {
	snap, err := r.snapshot()
	if err != nil {
		return err
	}
	var stats struct {
		Stats domain.PoolStats `json:"stats"`
	}
	raw, _ := json.Marshal(snap)
	if err := json.Unmarshal(raw, &stats); err != nil {
		return err
	}
	if stats.Stats.Config.MaxOpen < 2 {
		return fmt.Errorf("crash recovery sample needs max_open >= 2, got %d", stats.Stats.Config.MaxOpen)
	}
	leaseA, err := r.acquireLease("e2e-crash-hold-a", 5000)
	if err != nil {
		return err
	}
	leaseB, err := r.acquireLease("e2e-crash-hold-b", 5000)
	if err != nil {
		return err
	}
	releaseA, err := r.client.post("/v1/release", "e2e-crash-release-a", map[string]any{"conn_id": leaseA.ConnectionID})
	if err != nil || releaseA.status != http.StatusOK {
		return fmt.Errorf("release crash fixture A: err=%v status=%d body=%s", err, callStatus(releaseA), callBody(releaseA))
	}
	if err := r.waitPoolShape(1, 1); err != nil {
		return err
	}
	fixtureCall, err := r.client.post("/v1/debug/crash-fixture", "e2e-crash-fixture", map[string]any{})
	if err != nil {
		return err
	}
	if fixtureCall.status != http.StatusOK || !fixtureCall.env.OK {
		return fmt.Errorf("crash fixture failed: %d %s", fixtureCall.status, fixtureCall.body)
	}
	var fixture map[string]any
	if err := json.Unmarshal(fixtureCall.env.Data, &fixture); err != nil {
		return err
	}
	pendingAcquire, _ := fixture["pending_acquire"].(map[string]any)
	pendingRelease, _ := fixture["pending_release"].(map[string]any)
	if pendingAcquire == nil || pendingRelease == nil {
		return fmt.Errorf("crash fixture missing pending intent details: %+v", fixture)
	}
	preKillSnapshot, _ := r.snapshot()
	killErr := r.server.cmd.Process.Kill()
	waitErr := r.server.cmd.Wait()
	if r.server.logFile != nil {
		_ = r.server.logFile.Close()
	}
	r.server = nil
	if killErr != nil {
		return fmt.Errorf("SIGKILL: %w", killErr)
	}
	if waitErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			return fmt.Errorf("wait after SIGKILL: %w", waitErr)
		}
	}
	if _, err := r.start(); err != nil {
		return err
	}
	recoveryCall, err := r.client.get("/v1/recovery")
	if err != nil {
		return err
	}
	if recoveryCall.status != http.StatusOK {
		return fmt.Errorf("recovery report unavailable: %d %s", recoveryCall.status, recoveryCall.body)
	}
	var recoveryData struct {
		Recovery domain.RecoveryReport `json:"recovery"`
	}
	if err := json.Unmarshal(recoveryCall.env.Data, &recoveryData); err != nil {
		return err
	}
	report := recoveryData.Recovery
	if report.PendingIntentsFound < 2 {
		return fmt.Errorf("expected at least 2 pending intents, got %d", report.PendingIntentsFound)
	}
	if report.MetadataStatesRestored != true || report.RuntimeHandlesPreserved != false ||
		report.IdleQueueOrderRecoverable != false || report.FactsOnly != true {
		return fmt.Errorf("recovery report capability flags incorrect: %+v", report)
	}
	acquireRollback := false
	releaseRollback := false
	for _, intent := range report.Intents {
		if intent.Kind == domain.IntentAcquire && intent.FromState == domain.StateInUse && intent.ToState == domain.StateAvailable {
			acquireRollback = true
		}
		if intent.Kind == domain.IntentRelease && intent.FromState == domain.StateAvailable && intent.ToState == domain.StateInUse {
			releaseRollback = true
		}
	}
	if !acquireRollback || !releaseRollback {
		return fmt.Errorf("recovery did not show both acquire and release rollbacks: %+v", report.Intents)
	}
	recoveryFile := filepath.Join(r.dataDir, "recovery-latest.json")
	if _, err := os.Stat(recoveryFile); err != nil {
		return fmt.Errorf("recovery file missing: %w", err)
	}
	var fileReport domain.RecoveryReport
	fileData, err := os.ReadFile(recoveryFile)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(fileData, &fileReport); err != nil {
		return err
	}
	if fileReport.ReportID != report.ReportID {
		return fmt.Errorf("API and file recovery reports differ: %s vs %s", report.ReportID, fileReport.ReportID)
	}
	postSnap, _ := r.snapshot()
	obs := map[string]any{
		"pre_kill_held_connection": leaseB.ConnectionID,
		"pre_kill_idle_connection": leaseA.ConnectionID,
		"pending_acquire":          pendingAcquire,
		"pending_release":          pendingRelease,
		"pre_kill_snapshot":        preKillSnapshot,
		"recovery_report":          report,
		"recovery_file":            recoveryFile,
		"post_restart_snapshot":    postSnap,
	}
	r.record("sigkill_restart_recovery", nil, obs)
	return nil
}

func (r *runner) acquireLease(requestID string, timeoutMS int64) (leaseData, error) {
	call, err := r.client.post("/v1/acquire", requestID, map[string]any{"timeout_ms": timeoutMS})
	if err != nil {
		return leaseData{}, err
	}
	return leaseFromCall(call)
}

func (r *runner) waitQueueDepth(want int) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := r.snapshot()
		if err != nil {
			return err
		}
		if queueWaiters(snap) >= want {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("queue depth never reached %d", want)
}

func (r *runner) waitPoolShape(inUse, idle int) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := r.snapshot()
		if err != nil {
			return err
		}
		stats, err := snapshotStats(snap)
		if err != nil {
			return err
		}
		if stats.InUse == inUse && stats.Idle == idle && stats.Waiting == 0 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("pool shape did not reach in_use=%d idle=%d", inUse, idle)
}

func (r *runner) writeResult(resultFile string, runErr error) {
	if resultFile == "" {
		return
	}
	result := map[string]any{
		"success":     runErr == nil,
		"sample":      r.samplePath,
		"addr":        r.addr,
		"data_dir":    r.dataDir,
		"finished_at": time.Now(),
	}
	if runErr != nil {
		result["error"] = runErr.Error()
	}
	steps := make([]map[string]any, 0, len(r.steps))
	for _, step := range r.steps {
		raw, _ := json.Marshal(step)
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		steps = append(steps, m)
	}
	result["steps"] = steps
	data, _ := json.MarshalIndent(result, "", "  ")
	_ = os.MkdirAll(filepath.Dir(resultFile), 0o755)
	if err := os.WriteFile(resultFile, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write result: %v\n", err)
	}
}

func snapshotStats(snap map[string]any) (domain.PoolStats, error) {
	raw, err := json.Marshal(snap["stats"])
	if err != nil {
		return domain.PoolStats{}, err
	}
	var stats domain.PoolStats
	if err := json.Unmarshal(raw, &stats); err != nil {
		return stats, err
	}
	return stats, nil
}

func queueWaiters(snap map[string]any) int {
	stats, err := snapshotStats(snap)
	if err != nil {
		return -1
	}
	return stats.Waiting
}

func extractEvents(snap map[string]any) ([]map[string]any, error) {
	raw, err := json.Marshal(snap["events"])
	if err != nil {
		return nil, err
	}
	var events []map[string]any
	if err := json.Unmarshal(raw, &events); err != nil {
		return nil, err
	}
	return events, nil
}

func countWakeEvents(snap map[string]any) (int, error) {
	events, err := extractEvents(snap)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, event := range events {
		if event["type"] == "acquire_wake" {
			count++
		}
	}
	return count, nil
}

func errorCode(call *apiCall) string {
	if call == nil || call.env.Error == nil {
		return ""
	}
	return call.env.Error.Code
}

func injectionObservation(call *apiCall) map[string]any {
	if call == nil || call.env.Error == nil {
		return nil
	}
	obs, _ := call.env.Error.Context["observation"].(map[string]any)
	return obs
}

func callStatus(call *apiCall) int {
	if call == nil {
		return 0
	}
	return call.status
}

func callBody(call *apiCall) string {
	if call == nil {
		return "<nil>"
	}
	return string(call.body)
}

func maxConcurrency(intervals []interval) int {
	type point struct {
		at    time.Time
		delta int
	}
	var points []point
	for _, item := range intervals {
		points = append(points, point{at: item.start, delta: 1}, point{at: item.end, delta: -1})
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].at.Equal(points[j].at) {
			return points[i].delta < points[j].delta
		}
		return points[i].at.Before(points[j].at)
	})
	current, max := 0, 0
	for _, p := range points {
		current += p.delta
		if current > max {
			max = current
		}
	}
	return max
}

func mapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func checkPortFree(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("loopback port %s is already in use", addr)
	}
	return nil
}

func tail(value string, n int) string {
	if len(value) <= n {
		return value
	}
	return value[len(value)-n:]
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
