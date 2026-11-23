package main

import (
	"bytes"
	"context"
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
	"runtime"
	"strings"
	"sync"
	"time"
)

type messageView struct {
	MessageID string `json:"message_id"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Seq       int64  `json:"seq"`
	Status    string `json:"status"`
	TxnID     string `json:"txn_id"`
}

type partitionView struct {
	Topic             string `json:"topic"`
	Partition         int    `json:"partition"`
	NextSeq           int64  `json:"next_seq"`
	VisibleFrontier   int64  `json:"visible_frontier"`
	EffectiveFrontier int64  `json:"effective_frontier"`
	ConsumeCursor     int64  `json:"consume_cursor"`
	LeaseState        string `json:"lease_state"`
	LeaseOwner        string `json:"lease_owner"`
	LeaseVersion      int64  `json:"lease_version"`
	ReplayActive      bool   `json:"replay_active"`
	ReplayFrom        *int64 `json:"replay_from"`
	ReplayLimit       *int64 `json:"replay_limit"`
	PendingTxnCount   int64  `json:"pending_txn_count"`
}

type pullView struct {
	Messages []messageView `json:"messages"`
	Lease    partitionView `json:"lease"`
}

type commitView struct {
	Partition    partitionView `json:"partition"`
	ConsumedFrom int64         `json:"consumed_from"`
	ConsumedTo   int64         `json:"consumed_to"`
}

type replayView struct {
	Partition   partitionView  `json:"partition"`
	ReplayFrom  int64          `json:"replay_from"`
	ReplayLimit int64          `json:"replay_limit"`
	Injection   map[string]any `json:"injection"`
}

type txnView struct {
	TxnID    string `json:"txn_id"`
	Status   string `json:"status"`
	Decision string `json:"decision"`
}

type resolveView struct {
	Transaction txnView       `json:"transaction"`
	Message     messageView   `json:"message"`
	Partition   partitionView `json:"partition"`
	Idempotent  bool          `json:"idempotent"`
}

type schedulerTaskView struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Deduped bool   `json:"deduped"`
}

type budgetStep struct {
	Result string `json:"result"`
	Reason string `json:"reason"`
	Code   string `json:"code"`
}

type budgetRound struct {
	ID          int64        `json:"id"`
	Status      string       `json:"status"`
	ConsumedMS  int64        `json:"consumed_ms"`
	RemainingMS int64        `json:"remaining_ms"`
	Steps       []budgetStep `json:"steps"`
}

type recoveryDetail struct {
	Operation string `json:"operation"`
	Action    string `json:"action"`
	Evidence  string `json:"evidence"`
}

type recoveryReport struct {
	RecoveredIntents int64            `json:"recovered_intents"`
	Details          []recoveryDetail `json:"details"`
}

type check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail any    `json:"detail,omitempty"`
}

type verification struct {
	StartedAt     string         `json:"started_at"`
	FinishedAt    string         `json:"finished_at"`
	Port          int            `json:"port"`
	Database      string         `json:"database"`
	Checks        []check        `json:"checks"`
	Recovery      recoveryReport `json:"recovery"`
	DemoServerLog string         `json:"demo_server_log,omitempty"`
}

type apiEnvelope struct {
	OK        bool            `json:"ok"`
	Data      json.RawMessage `json:"data"`
	Error     *apiError       `json:"error"`
	RequestID string          `json:"request_id"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type failure struct {
	status  int
	code    string
	message string
}

func (f *failure) Error() string {
	return fmt.Sprintf("HTTP %d %s: %s", f.status, f.code, f.message)
}

type apiClient struct {
	base string
	http *http.Client
}

func newClient(base string) *apiClient {
	return &apiClient{base: base, http: &http.Client{Timeout: 10 * time.Second}}
}

func (c *apiClient) call(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	var env apiEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode HTTP %d: %w (%s)", resp.StatusCode, err, string(raw))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !env.OK {
		code, message := "", ""
		if env.Error != nil {
			code, message = env.Error.Code, env.Error.Message
		}
		return &failure{status: resp.StatusCode, code: code, message: message}
	}
	if out != nil && len(env.Data) > 0 {
		if err := json.Unmarshal(env.Data, out); err != nil {
			return fmt.Errorf("decode data: %w", err)
		}
	}
	return nil
}

type serverRun struct {
	cmd  *exec.Cmd
	done chan error
	log  *os.File
	path string
}

func buildServer(root, temp string) (string, error) {
	bin := filepath.Join(temp, "workbench")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", bin, ".")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build server: %w", err)
	}
	return bin, nil
}

func startServer(bin, root, dbPath, logPath string, port int) (*serverRun, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, "-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"-db", dbPath, "-seed", "samples/demo.json", "-allow-crash",
		"-lease-ttl", "10s", "-round-budget-ms", "250")
	cmd.Dir = root
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	run := &serverRun{cmd: cmd, done: make(chan error, 1), log: logFile, path: logPath}
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	go func() { run.done <- cmd.Wait() }()
	return run, nil
}

func (s *serverRun) stop() {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return
	}
	select {
	case <-s.done:
	default:
		_ = s.cmd.Process.Kill()
		select {
		case <-s.done:
		case <-time.After(5 * time.Second):
		}
	}
	_ = s.log.Close()
}

func waitHealth(ctx context.Context, client *apiClient) error {
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if err := client.call(ctx, http.MethodGet, "/api/health", nil, nil); err == nil {
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("server did not become healthy: %w", last)
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found")
		}
		dir = parent
	}
}

func main() {
	port := flag.Int("port", 18080, "local demo port")
	keepTemp := flag.Bool("keep-temp", false, "keep temporary database and server logs")
	serverBin := flag.String("server-bin", "", "optional prebuilt workbench binary")
	flag.Parse()
	if err := run(*port, *keepTemp, *serverBin); err != nil {
		fmt.Fprintln(os.Stderr, "DEMO FAILED:", err)
		os.Exit(1)
	}
}

func run(port int, keepTemp bool, prebuilt string) error {
	if !portFree(port) {
		return fmt.Errorf("port %d is already in use", port)
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	temp, err := os.MkdirTemp("", "workbench-demo-*")
	if err != nil {
		return err
	}
	if !keepTemp {
		defer os.RemoveAll(temp)
	}
	dbPath := filepath.Join(temp, "demo.db")
	logPath := filepath.Join(temp, "demo-server.log")
	bin := prebuilt
	if bin == "" {
		fmt.Println("building local workbench binary...")
		bin, err = buildServer(root, temp)
		if err != nil {
			return err
		}
	}
	bin, _ = filepath.Abs(bin)
	report := verification{StartedAt: time.Now().Format(time.RFC3339), Port: port,
		Database: dbPath, DemoServerLog: logPath}
	pass := func(name string, detail any) {
		report.Checks = append(report.Checks, check{Name: name, OK: true, Detail: detail})
	}

	runServer := func() (*serverRun, *apiClient, error) {
		proc, err := startServer(bin, root, dbPath, logPath, port)
		if err != nil {
			return nil, nil, err
		}
		client := newClient(fmt.Sprintf("http://127.0.0.1:%d", port))
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := waitHealth(ctx, client); err != nil {
			proc.stop()
			return nil, nil, err
		}
		return proc, client, nil
	}
	proc, client, err := runServer()
	if err != nil {
		return err
	}
	defer func() { proc.stop() }()
	ctx := context.Background()

	var part partitionView
	if err := client.call(ctx, http.MethodGet, "/api/partitions/orders/0", nil, &part); err != nil {
		return err
	}
	if part.NextSeq != 3 || part.PendingTxnCount != 1 {
		return fmt.Errorf("unexpected sample partition: %+v", part)
	}
	pass("sample partitions and pending transaction visible", part)

	pull, err := pullOnce(ctx, client, "orders", 0, "ordered-consumer", nil, 1, "demo-ordered-pull-0")
	if err != nil {
		return err
	}
	if len(pull.Messages) != 1 || pull.Messages[0].Seq != 0 {
		return fmt.Errorf("ordered pull expected seq 0: %+v", pull)
	}
	if _, err := commitOnce(ctx, client, "orders", 0, "ordered-consumer", pull.Lease.LeaseVersion, 0, "demo-ordered-commit-0"); err != nil {
		return err
	}
	pass("strict ordered consumption starts at partition sequence 0", pull.Messages)

	var resolved resolveView
	if err := client.call(ctx, http.MethodPost, "/api/txns/tx-commit-1/resolve", map[string]any{
		"request_id": "demo-resolve-commit", "injection_mode": "commit"}, &resolved); err != nil {
		return err
	}
	if resolved.Transaction.Status != "COMMITTED" || resolved.Message.Status != "VISIBLE" || resolved.Partition.VisibleFrontier != 3 {
		return fmt.Errorf("pending commit result: %+v", resolved)
	}
	pass("pending transaction commit atomically exposes message and advances frontier", resolved)

	type concurrentPull struct {
		status int
		code   string
		data   pullView
	}
	results := make([]concurrentPull, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := pullOnce(ctx, client, "orders", 0, fmt.Sprintf("concurrent-%d", i), nil, 1,
				fmt.Sprintf("demo-concurrent-pull-%d", i))
			if err == nil {
				results[i] = concurrentPull{status: 200, data: out}
				return
			}
			var f *failure
			if errors.As(err, &f) {
				results[i] = concurrentPull{status: f.status, code: f.code}
				return
			}
			results[i] = concurrentPull{status: 0, code: err.Error()}
		}(i)
	}
	wg.Wait()
	winner := -1
	held := false
	for i, result := range results {
		if result.status == 200 {
			if winner != -1 {
				return fmt.Errorf("both concurrent pulls acquired the lease: %+v", results)
			}
			winner = i
		}
		if result.code == "E_LEASE_HELD" {
			held = true
		}
	}
	if winner == -1 || !held {
		return fmt.Errorf("concurrent lease conflict not observed: %+v", results)
	}
	winnerResult := results[winner]
	if len(winnerResult.data.Messages) != 1 || winnerResult.data.Messages[0].Seq != 1 {
		return fmt.Errorf("concurrent winner did not receive seq 1: %+v", winnerResult.data)
	}
	if _, err := commitOnce(ctx, client, "orders", 0, fmt.Sprintf("concurrent-%d", winner),
		winnerResult.data.Lease.LeaseVersion, 1, "demo-concurrent-commit"); err != nil {
		return err
	}
	pass("same-partition concurrent pulls conflict with E_LEASE_HELD", []map[string]any{
		{"status": results[0].status, "code": results[0].code},
		{"status": results[1].status, "code": results[1].code},
	})

	next, err := pullOnce(ctx, client, "orders", 0, "ordered-consumer", nil, 1, "demo-ordered-pull-2")
	if err != nil {
		return err
	}
	if len(next.Messages) != 1 || next.Messages[0].Seq != 2 {
		return fmt.Errorf("expected seq 2 after concurrent commit: %+v", next)
	}
	if _, err := commitOnce(ctx, client, "orders", 0, "ordered-consumer", next.Lease.LeaseVersion,
		2, "demo-ordered-commit-2"); err != nil {
		return err
	}
	pass("order resumes after lease release without skipping", next.Messages)

	var rollback resolveView
	if err := client.call(ctx, http.MethodPost, "/api/txns/tx-rollback-1/resolve", map[string]any{
		"request_id": "demo-resolve-rollback", "injection_mode": "rollback"}, &rollback); err != nil {
		return err
	}
	if rollback.Transaction.Status != "ROLLED_BACK" || rollback.Message.Status != "ROLLED_BACK" ||
		rollback.Partition.VisibleFrontier != 2 {
		return fmt.Errorf("rollback result: %+v", rollback)
	}
	pass("pending transaction rollback stays out of consumable sequence", rollback)

	err = client.call(ctx, http.MethodPost, "/api/txns/tx-timeout-1/resolve", map[string]any{
		"request_id": "demo-timeout-1", "injection_mode": "hang", "query_timeout_ms": 120}, nil)
	var timeoutFailure *failure
	if !errors.As(err, &timeoutFailure) || timeoutFailure.code != "E_TXN_TIMEOUT" {
		return fmt.Errorf("expected E_TXN_TIMEOUT, got %v", err)
	}
	var timeoutRetry resolveView
	if err := client.call(ctx, http.MethodPost, "/api/txns/tx-timeout-1/resolve", map[string]any{
		"request_id": "demo-timeout-retry", "injection_mode": "commit"}, &timeoutRetry); err != nil {
		return err
	}
	if timeoutRetry.Transaction.Status != "COMMITTED" {
		return fmt.Errorf("timeout retry did not commit: %+v", timeoutRetry)
	}
	pass("coordinator timeout closes the query and a new request retries to commit", map[string]any{
		"timeout_code": timeoutFailure.code, "retry": timeoutRetry.Transaction})

	var conflictFirst resolveView
	if err := client.call(ctx, http.MethodPost, "/api/txns/tx-conflict-1/resolve", map[string]any{
		"request_id": "demo-conflict-1", "injection_mode": "conflict"}, &conflictFirst); err != nil {
		return err
	}
	err = client.call(ctx, http.MethodPost, "/api/txns/tx-conflict-1/resolve", map[string]any{
		"request_id": "demo-conflict-2", "injection_mode": "conflict", "force_recheck": true}, nil)
	var conflictFailure *failure
	if !errors.As(err, &conflictFailure) || conflictFailure.code != "E_TXN_CONFLICT" {
		return fmt.Errorf("expected E_TXN_CONFLICT, got %v", err)
	}
	pass("contradictory coordinator conclusion returns E_TXN_CONFLICT without overwrite", map[string]any{
		"first_decision": conflictFirst.Transaction.Decision, "recheck_code": conflictFailure.code})

	empty, err := pullOnce(ctx, client, "orders", 0, "replayer", nil, 1, "demo-replay-lease")
	if err != nil {
		return err
	}
	var replay replayView
	if err := client.call(ctx, http.MethodPost, "/api/partitions/orders/0/replay", map[string]any{
		"owner": "replayer", "lease_version": empty.Lease.LeaseVersion, "from_seq": 1,
		"lease_injection": "new_message_on_replay", "request_id": "demo-replay-start"}, &replay); err != nil {
		return err
	}
	if !replay.Partition.ReplayActive || replay.Partition.VisibleFrontier != 3 || replay.Partition.NextSeq != 4 {
		return fmt.Errorf("replay state: %+v", replay)
	}
	replayVersion := replay.Partition.LeaseVersion
	replayVersionPtr := &replayVersion
	for seq := int64(1); seq <= 2; seq++ {
		out, err := pullOnce(ctx, client, "orders", 0, "replayer", replayVersionPtr, 1,
			fmt.Sprintf("demo-replay-pull-%d", seq))
		if err != nil {
			return err
		}
		if len(out.Messages) != 1 || out.Messages[0].Seq != seq {
			return fmt.Errorf("replay expected seq %d: %+v", seq, out)
		}
		if _, err := commitOnce(ctx, client, "orders", 0, "replayer", out.Lease.LeaseVersion, seq,
			fmt.Sprintf("demo-replay-commit-%d", seq)); err != nil {
			return err
		}
		replayVersionPtr = nil // next pull reacquires a FREE lease
	}
	var afterReplay partitionView
	if err := client.call(ctx, http.MethodGet, "/api/partitions/orders/0", nil, &afterReplay); err != nil {
		return err
	}
	if afterReplay.ReplayActive || afterReplay.VisibleFrontier != 4 {
		return fmt.Errorf("replay did not finish cleanly: %+v", afterReplay)
	}
	pass("replay blocks newly arrived messages until the replay limit, then advances frontier", replay)

	oldLease, err := pullOnce(ctx, client, "orders", 0, "preempt-old", nil, 1, "demo-preempt-old")
	if err != nil {
		return err
	}
	var preempted pullView
	preemptedRaw := map[string]any{"owner": "preempt-new", "limit": 1,
		"lease_injection": "timeout_preempt", "request_id": "demo-preempt-new"}
	if err := client.call(ctx, http.MethodPost, "/api/partitions/orders/0/pull", preemptedRaw, &preempted); err != nil {
		return err
	}
	if len(preempted.Messages) != 1 || preempted.Messages[0].Seq != 3 || preempted.Lease.LeaseOwner != "preempt-new" {
		return fmt.Errorf("lease preemption failed: %+v", preempted)
	}
	err = client.call(ctx, http.MethodPost, "/api/partitions/orders/0/commit", map[string]any{
		"owner": "preempt-old", "lease_version": oldLease.Lease.LeaseVersion,
		"through_seq": 3, "request_id": "demo-preempt-stale"}, nil)
	var preemptStale *failure
	if !errors.As(err, &preemptStale) || preemptStale.code != "E_LEASE_STALE" {
		return fmt.Errorf("expected E_LEASE_STALE after preemption, got %v", err)
	}
	if _, err := commitOnce(ctx, client, "orders", 0, "preempt-new",
		preempted.Lease.LeaseVersion, 3, "demo-preempt-commit"); err != nil {
		return err
	}
	pass("lease timeout preemption invalidates the old lease with E_LEASE_STALE", map[string]any{
		"old_version": oldLease.Lease.LeaseVersion, "new_version": preempted.Lease.LeaseVersion,
		"stale_code": preemptStale.code})

	config := map[string]any{"global_concurrency": 2, "per_partition_lease_limit": 1,
		"queue_capacity": 2, "round_budget_ms": 5, "query_timeout_ms": 100, "min_dispatch_ms": 2}
	if err := client.call(ctx, http.MethodPost, "/api/scheduler/config", config, nil); err != nil {
		return err
	}
	for i := 0; i < 2; i++ {
		var task schedulerTaskView
		if err := client.call(ctx, http.MethodPost, "/api/scheduler/tasks", map[string]any{
			"kind": "txn", "txn_id": fmt.Sprintf("tx-sched-%d", i)}, &task); err != nil {
			return err
		}
	}
	err = client.call(ctx, http.MethodPost, "/api/scheduler/tasks", map[string]any{
		"kind": "txn", "txn_id": "tx-sched-2"}, nil)
	var queueFailure *failure
	if !errors.As(err, &queueFailure) || queueFailure.code != "E_QUEUE_FULL" {
		return fmt.Errorf("expected E_QUEUE_FULL, got %v", err)
	}
	var degraded budgetRound
	if err := client.call(ctx, http.MethodPost, "/api/scheduler/run", map[string]any{"budget_ms": 1}, &degraded); err != nil {
		return err
	}
	if degraded.Status != "degraded_budget" || len(degraded.Steps) < 2 {
		return fmt.Errorf("budget degradation missing: %+v", degraded)
	}
	config["queue_capacity"] = 10
	config["round_budget_ms"] = 1000
	if err := client.call(ctx, http.MethodPost, "/api/scheduler/config", config, nil); err != nil {
		return err
	}
	var completed budgetRound
	if err := client.call(ctx, http.MethodPost, "/api/scheduler/run", map[string]any{"budget_ms": 1000}, &completed); err != nil {
		return err
	}
	if completed.Status != "completed" {
		return fmt.Errorf("scheduler completion missing: %+v", completed)
	}
	pass("bounded queue, deterministic budget skips, and recovery on a later round", map[string]any{
		"queue_full": queueFailure.code, "degraded": degraded, "completed": completed})

	crashBody := map[string]any{"operation": "pull", "topic": "orders", "partition": 1,
		"owner": "crash-consumer", "request_id": "demo-crash-intent"}
	_ = client.call(ctx, http.MethodPost, "/api/debug/crash-after-intent", crashBody, nil)
	select {
	case <-proc.done:
	case <-time.After(10 * time.Second):
		return errors.New("crash-injected process did not terminate")
	}
	_ = proc.log.Close()
	time.Sleep(300 * time.Millisecond)

	proc2, client2, err := runServer()
	if err != nil {
		return fmt.Errorf("restart after crash: %w", err)
	}
	defer func() { proc2.stop() }()
	var recovery recoveryReport
	if err := client2.call(ctx, http.MethodGet, "/api/recovery/latest", nil, &recovery); err != nil {
		return err
	}
	if recovery.RecoveredIntents < 1 {
		return fmt.Errorf("recovery report did not include prepared intent: %+v", recovery)
	}
	foundRecovery := false
	for _, detail := range recovery.Details {
		if detail.Operation == "pull" && detail.Action == "pull_rolled_back_to_consumable" {
			foundRecovery = true
		}
	}
	if !foundRecovery {
		return fmt.Errorf("pull recovery action missing: %+v", recovery)
	}
	var recoveredPartition partitionView
	if err := client2.call(ctx, http.MethodGet, "/api/partitions/orders/1", nil, &recoveredPartition); err != nil {
		return err
	}
	var dashboard struct {
		Recovery recoveryReport `json:"recovery"`
	}
	if err := client2.call(ctx, http.MethodGet, "/api/dashboard", nil, &dashboard); err != nil {
		return err
	}
	if dashboard.Recovery.RecoveredIntents != recovery.RecoveredIntents {
		return fmt.Errorf("dashboard recovery report differs: %+v vs %+v", dashboard.Recovery, recovery)
	}
	report.Recovery = recovery
	pass("forced termination recovers prepared pull intent and dashboard report matches", map[string]any{
		"recovery": recovery, "partition": recoveredPartition})

	report.FinishedAt = time.Now().Format(time.RFC3339)
	if err := os.MkdirAll(filepath.Join(root, "artifacts"), 0o755); err != nil {
		return err
	}
	logBytes, readErr := os.ReadFile(logPath)
	if readErr == nil {
		_ = os.WriteFile(filepath.Join(root, "artifacts", "demo-server.log"), logBytes, 0o644)
		report.DemoServerLog = "artifacts/demo-server.log"
	}
	raw, _ := json.MarshalIndent(report, "", "  ")
	outPath := filepath.Join(root, "artifacts", "demo-verification.json")
	if err := os.WriteFile(outPath, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("\nVERIFIED %d checks\n", len(report.Checks))
	for _, c := range report.Checks {
		fmt.Printf("  OK  %s\n", c.Name)
	}
	fmt.Printf("report: %s\n", outPath)
	if keepTemp {
		fmt.Printf("temp database: %s\nserver log: %s\n", dbPath, logPath)
	}
	return nil
}

func portFree(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

func pullOnce(ctx context.Context, client *apiClient, topic string, partition int, owner string,
	version *int64, limit int, requestID string) (pullView, error) {
	body := map[string]any{"owner": owner, "limit": limit, "request_id": requestID}
	if version != nil {
		body["lease_version"] = *version
	}
	var out pullView
	err := client.call(ctx, http.MethodPost,
		fmt.Sprintf("/api/partitions/%s/%d/pull", topic, partition), body, &out)
	return out, err
}

func commitOnce(ctx context.Context, client *apiClient, topic string, partition int, owner string,
	version int64, through int64, requestID string) (commitView, error) {
	var out commitView
	err := client.call(ctx, http.MethodPost,
		fmt.Sprintf("/api/partitions/%s/%d/commit", topic, partition), map[string]any{
			"owner": owner, "lease_version": version, "through_seq": through, "request_id": requestID}, &out)
	return out, err
}

var _ = strings.TrimSpace
