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

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/engine"
	"local/configmiddleware/internal/pipeline"
	"local/configmiddleware/internal/sim"
)

type verification struct {
	Name      string   `json:"name"`
	Passed    bool     `json:"passed"`
	Checks    []string `json:"checks"`
	Artifacts []string `json:"artifacts"`
}

func (v *verification) check(ok bool, message string) {
	prefix := "PASS "
	if !ok {
		prefix = "FAIL "
		v.Passed = false
	}
	v.Checks = append(v.Checks, prefix+message)
	if !ok {
		fmt.Printf("  FAIL %s\n", message)
	}
}

type lab struct {
	root        string
	outDir      string
	binPath     string
	dbPath      string
	httpClient  *http.Client
	apiBase     string
	simHandler  *sim.Handler
	simServer   *http.Server
	simPort     int
	refusedPort int
	mw          *middlewareProcess
	artifacts   map[string]string
}

type middlewareProcess struct {
	cmd     *exec.Cmd
	base    string
	stdout  *os.File
	stderr  *os.File
	once    sync.Once
	waitErr error
}

type apiErrorEnvelope struct {
	Error core.BizError `json:"error"`
}

type triggerResponse struct {
	Batch  core.Batch        `json:"batch"`
	Pushes []engine.PushView `json:"pushes"`
	Errors []*core.BizError  `json:"errors"`
}

type statsResponse struct {
	ByPush map[string]sim.Stats `json:"by_push"`
}

func main() {
	rootFlag := flag.String("root", ".", "repository root")
	outFlag := flag.String("out", "artifacts/experiment", "experiment output directory")
	flag.Parse()
	root, err := findRoot(*rootFlag)
	must(err)
	outDir, _ := filepath.Abs(filepath.Join(root, *outFlag))
	l := &lab{root: root, outDir: outDir, artifacts: map[string]string{}, httpClient: &http.Client{Timeout: 15 * time.Second}}
	must(l.prepare())
	defer l.close()

	var results []*verification
	run := func(name string, fn func(*verification) error) {
		v := &verification{Name: name, Passed: true}
		fmt.Printf("[scenario] %s\n", name)
		err := fn(v)
		if err != nil {
			v.Passed = false
			v.Checks = append(v.Checks, "FAIL scenario error: "+err.Error())
			fmt.Printf("  FAIL scenario error: %v\n", err)
		}
		results = append(results, v)
	}

	run("push timeout closes connection and releases capacity", l.scenarioTimeout)
	run("connection refused", l.scenarioRefused)
	run("confirmation/version mismatch", l.scenarioVersionMismatch)
	run("duplicate push and conflicting content", l.scenarioDuplicate)
	run("late ack and concurrent versions", l.scenarioLateAck)
	run("snapshot guards", l.scenarioSnapshotGuards)
	run("forced termination and recovery", l.scenarioRecovery)

	allPassed := true
	for _, r := range results {
		if !r.Passed {
			allPassed = false
		}
	}
	summary := map[string]any{
		"generated_at": core.Now(), "all_passed": allPassed, "database": l.dbPath,
		"artifacts": l.artifacts, "scenarios": results,
	}
	path := l.save("summary.json", summary)
	fmt.Printf("\nsummary: %s\nall_passed=%v\n", path, allPassed)
	if !allPassed {
		os.Exit(1)
	}
}

func findRoot(start string) (string, error) {
	abs, err := filepath.Abs(start)
	if err != nil {
		return "", err
	}
	for {
		if data, err := os.ReadFile(filepath.Join(abs, "go.mod")); err == nil && bytes.Contains(data, []byte("module local/configmiddleware")) {
			return abs, nil
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", errors.New("repository root containing go.mod not found")
		}
		abs = parent
	}
}

func (l *lab) prepare() error {
	if err := os.RemoveAll(l.outDir); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(l.outDir, "logs"), 0o755); err != nil {
		return err
	}
	binName := "middleware"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	l.binPath = filepath.Join(l.outDir, "bin", binName)
	if err := os.MkdirAll(filepath.Dir(l.binPath), 0o755); err != nil {
		return err
	}
	build := exec.Command("go", "build", "-o", l.binPath, "./cmd/middleware")
	build.Dir = l.root
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("build middleware: %w", err)
	}
	l.dbPath = filepath.Join(l.outDir, "run", "middleware.db")
	if err := os.MkdirAll(filepath.Dir(l.dbPath), 0o755); err != nil {
		return err
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(l.dbPath + suffix)
	}
	doc, err := pipeline.Load(filepath.Join(l.root, "samples", "samples.json"))
	if err != nil {
		return err
	}
	l.simHandler = sim.New(doc)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	l.simPort = listener.Addr().(*net.TCPAddr).Port
	l.simServer = &http.Server{Handler: l.simHandler.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = l.simServer.Serve(listener) }()
	l.refusedPort, err = freePort()
	if err != nil {
		return err
	}
	if err := l.startMiddleware(nil); err != nil {
		return err
	}
	if err := l.submitSnapshot("snapshot.v1.json"); err != nil {
		return err
	}
	return l.registerClients()
}

func (l *lab) close() {
	if l.mw != nil {
		_ = l.mw.stop()
	}
	if l.simServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = l.simServer.Shutdown(ctx)
		cancel()
	}
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port, nil
}

func (l *lab) startMiddleware(extraEnv []string) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	logName := fmt.Sprintf("middleware-%d", time.Now().UnixNano())
	stdout, err := os.Create(filepath.Join(l.outDir, "logs", logName+".out.log"))
	if err != nil {
		return err
	}
	stderr, err := os.Create(filepath.Join(l.outDir, "logs", logName+".err.log"))
	if err != nil {
		_ = stdout.Close()
		return err
	}
	args := []string{
		"-listen", fmt.Sprintf("127.0.0.1:%d", port),
		"-db", l.dbPath,
		"-samples", filepath.Join(l.root, "samples", "samples.json"),
		"-concurrency", "2",
		"-queue", "8",
		"-timeout", "400ms",
		"-budget", "5s",
	}
	cmd := exec.Command(l.binPath, args...)
	cmd.Dir = l.root
	cmd.Env = append(os.Environ(), extraEnv...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return err
	}
	l.mw = &middlewareProcess{cmd: cmd, base: base, stdout: stdout, stderr: stderr}
	l.apiBase = base
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		status, _, err := l.call(http.MethodGet, "/api/v1/health", nil)
		if err == nil && status == http.StatusOK {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = l.mw.stop()
	return fmt.Errorf("middleware did not become healthy; logs in %s", filepath.Join(l.outDir, "logs"))
}

func (m *middlewareProcess) stop() error {
	if m == nil || m.cmd == nil || m.cmd.Process == nil {
		return nil
	}
	_ = m.cmd.Process.Kill()
	m.once.Do(func() {
		m.waitErr = m.cmd.Wait()
		if m.stdout != nil {
			_ = m.stdout.Close()
		}
		if m.stderr != nil {
			_ = m.stderr.Close()
		}
	})
	return nil
}

func (m *middlewareProcess) wait(timeout time.Duration) error {
	if m == nil {
		return errors.New("nil process")
	}
	done := make(chan error, 1)
	go func() {
		m.once.Do(func() {
			m.waitErr = m.cmd.Wait()
			if m.stdout != nil {
				_ = m.stdout.Close()
			}
			if m.stderr != nil {
				_ = m.stderr.Close()
			}
		})
		done <- m.waitErr
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return errors.New("process did not exit")
	}
}

func (l *lab) submitSnapshot(name string) error {
	raw, err := os.ReadFile(filepath.Join(l.root, "samples", name))
	if err != nil {
		return err
	}
	status, body, err := l.call(http.MethodPost, "/api/v1/snapshots", json.RawMessage(raw))
	if err != nil {
		return err
	}
	if status != http.StatusCreated {
		return fmt.Errorf("submit %s: HTTP %d %s", name, status, body)
	}
	return nil
}

func (l *lab) registerClients() error {
	raw, err := os.ReadFile(filepath.Join(l.root, "samples", "clients.json"))
	if err != nil {
		return err
	}
	text := strings.ReplaceAll(string(raw), "{PORT}", fmt.Sprintf("%d", l.simPort))
	text = strings.ReplaceAll(text, "{REFUSED_PORT}", fmt.Sprintf("%d", l.refusedPort))
	var clients []core.Client
	if err := json.Unmarshal([]byte(text), &clients); err != nil {
		return err
	}
	for _, client := range clients {
		status, body, err := l.call(http.MethodPost, "/api/v1/clients", client)
		if err != nil {
			return err
		}
		if status != http.StatusCreated {
			return fmt.Errorf("register %s: HTTP %d %s", client.ClientID, status, body)
		}
	}
	return nil
}

func (l *lab) call(method, path string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, l.apiBase+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := l.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return resp.StatusCode, raw, err
}

func (l *lab) getJSON(path string, target any) error {
	status, body, err := l.call(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d %s", path, status, body)
	}
	return json.Unmarshal(body, target)
}

func (l *lab) trigger(req engine.TriggerRequest) (triggerResponse, int, []byte, error) {
	status, body, err := l.call(http.MethodPost, "/api/v1/pushes", req)
	if err != nil {
		return triggerResponse{}, status, body, err
	}
	var result triggerResponse
	if status == http.StatusOK {
		if err := json.Unmarshal(body, &result); err != nil {
			return result, status, body, err
		}
	}
	return result, status, body, nil
}

func (l *lab) mustTrigger(req engine.TriggerRequest) (triggerResponse, error) {
	result, status, body, err := l.trigger(req)
	if err != nil {
		return result, err
	}
	if status != http.StatusOK {
		return result, fmt.Errorf("trigger HTTP %d: %s", status, body)
	}
	return result, nil
}

func (l *lab) state() (engine.State, error) {
	var state engine.State
	err := l.getJSON("/api/v1/state", &state)
	return state, err
}

func (l *lab) stats() (statsResponse, error) {
	resp, err := l.httpClient.Get(fmt.Sprintf("http://127.0.0.1:%d/stats", l.simPort))
	if err != nil {
		return statsResponse{}, err
	}
	defer resp.Body.Close()
	var result statsResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result, err
}

func (l *lab) save(name string, value any) string {
	raw, _ := json.MarshalIndent(value, "", "  ")
	path := filepath.Join(l.outDir, name)
	_ = os.WriteFile(path, append(raw, '\n'), 0o644)
	l.artifacts[name] = path
	return path
}

func findPush(state engine.State, clientID string, version int64) (core.Push, bool) {
	for _, push := range state.Pushes {
		if push.ClientID == clientID && push.Version == version {
			return push, true
		}
	}
	return core.Push{}, false
}

func findPushView(resp triggerResponse, clientID string, version int64) (engine.PushView, bool) {
	for _, push := range resp.Pushes {
		if push.ClientID == clientID && push.Version == version {
			return push, true
		}
	}
	return engine.PushView{}, false
}

func resultFromPush(push engine.PushView) (core.ResultReport, error) {
	if push.Ack == nil || push.Ack.Result == nil {
		return core.ResultReport{}, fmt.Errorf("push %s has no client result", push.PushID)
	}
	raw, err := json.Marshal(push.Ack.Result)
	if err != nil {
		return core.ResultReport{}, err
	}
	var report core.ResultReport
	if err := json.Unmarshal(raw, &report); err != nil {
		return report, err
	}
	report.ClientID = push.ClientID
	report.PushID = push.PushID
	report.PickedVersion = push.Version
	return report, nil
}

func (l *lab) submitResult(report core.ResultReport) (core.Result, int, []byte, error) {
	status, body, err := l.call(http.MethodPost, "/api/v1/results", report)
	if err != nil {
		return core.Result{}, status, body, err
	}
	var result core.Result
	if status == http.StatusOK {
		if err := json.Unmarshal(body, &result); err != nil {
			return result, status, body, err
		}
	}
	return result, status, body, nil
}

func errorCode(body []byte) string {
	var envelope apiErrorEnvelope
	if json.Unmarshal(body, &envelope) == nil {
		return envelope.Error.Code
	}
	return ""
}

func (l *lab) scenarioTimeout(v *verification) error {
	extra := core.Client{
		ClientID:     "client-delay-2",
		Keys:         []string{"weight_x", "weight_y", "bias", "normalization_ratio", "decision_threshold"},
		Endpoint:     fmt.Sprintf("http://127.0.0.1:%d/push?behavior=delay&delay_ms=2000", l.simPort),
		PickupPolicy: "LATEST",
	}
	status, _, err := l.call(http.MethodPost, "/api/v1/clients", extra)
	if err != nil {
		return err
	}
	v.check(status == http.StatusCreated, "register second delayed client (global concurrency=2)")

	started := time.Now()
	resp, err := l.mustTrigger(engine.TriggerRequest{Version: 1, ClientIDs: []string{"client-delay", "client-delay-2"}, TimeoutMS: 250, RoundBudgetMS: 2000})
	elapsed := time.Since(started)
	if err != nil {
		return err
	}
	v.check(elapsed < 900*time.Millisecond, fmt.Sprintf("both pushes timed out at their own 250ms deadline (elapsed=%s)", elapsed.Round(time.Millisecond)))
	v.check(resp.Batch.Status == "COMPLETED_WITH_ERRORS", "batch marked COMPLETED_WITH_ERRORS")
	for _, clientID := range []string{"client-delay", "client-delay-2"} {
		push, ok := findPushView(resp, clientID, 1)
		v.check(ok && push.Status == core.StatusAborted && push.ErrorCode == "E_PUSH_TIMEOUT", clientID+" persisted ABORTED/E_PUSH_TIMEOUT")
		v.check(ok && numberContext(push.ErrorContext, "connections_opened") >= 1, clientID+" opened a real TCP connection")
		v.check(ok && numberContext(push.ErrorContext, "connections_closed") >= 1, clientID+" closed the underlying TCP connection after timeout")
	}
	stats, err := l.stats()
	if err != nil {
		return err
	}
	canceled := 0
	for _, value := range stats.ByPush {
		canceled += value.Canceled
	}
	v.check(canceled >= 2, fmt.Sprintf("client-sim observed request-context cancellation (%d)", canceled))

	started = time.Now()
	follow, err := l.mustTrigger(engine.TriggerRequest{Version: 1, ClientIDs: []string{"client-success"}, TimeoutMS: 1000, RoundBudgetMS: 1200})
	if err != nil {
		return err
	}
	followElapsed := time.Since(started)
	success, ok := findPushView(follow, "client-success", 1)
	v.check(ok && success.Status == core.StatusAcked, "follow-up push acquired released capacity and reached ACKED")
	v.check(followElapsed < 800*time.Millisecond, fmt.Sprintf("follow-up push was not blocked by timed-out slots (elapsed=%s)", followElapsed.Round(time.Millisecond)))
	if ok {
		report, err := resultFromPush(success)
		if err != nil {
			return err
		}
		result, status, body, err := l.submitResult(report)
		if err != nil {
			return err
		}
		v.check(status == http.StatusOK && result.Status == "APPLIED", "version 1 deterministic result applied: "+string(body))
	}
	state, _ := l.state()
	v.Artifacts = append(v.Artifacts, l.save("01-timeout-trigger.json", resp), l.save("01-timeout-state.json", state))
	return nil
}

func (l *lab) scenarioRefused(v *verification) error {
	resp, err := l.mustTrigger(engine.TriggerRequest{Version: 1, ClientIDs: []string{"client-refused"}, TimeoutMS: 1000, RoundBudgetMS: 2000})
	if err != nil {
		return err
	}
	push, ok := findPushView(resp, "client-refused", 1)
	v.check(ok && push.Status == core.StatusAborted && push.ErrorCode == "E_PUSH_REFUSED", "closed loopback port produced E_PUSH_REFUSED")
	v.check(resp.Batch.Status == "COMPLETED_WITH_ERRORS", "refused batch marked COMPLETED_WITH_ERRORS")
	state, _ := l.state()
	v.Artifacts = append(v.Artifacts, l.save("02-refused-trigger.json", resp), l.save("02-refused-state.json", state))
	return nil
}

func (l *lab) scenarioVersionMismatch(v *verification) error {
	client := core.Client{
		ClientID:     "client-wrong-version",
		Keys:         []string{"weight_x", "weight_y", "bias", "normalization_ratio", "decision_threshold"},
		Endpoint:     fmt.Sprintf("http://127.0.0.1:%d/push?behavior=wrong_version", l.simPort),
		PickupPolicy: "LATEST",
	}
	status, _, err := l.call(http.MethodPost, "/api/v1/clients", client)
	if err != nil {
		return err
	}
	v.check(status == http.StatusCreated, "registered client that returns an ACK for the wrong version")
	resp, err := l.mustTrigger(engine.TriggerRequest{Version: 1, ClientIDs: []string{"client-wrong-version"}, TimeoutMS: 1000, RoundBudgetMS: 2000})
	if err != nil {
		return err
	}
	push, ok := findPushView(resp, "client-wrong-version", 1)
	v.check(ok && push.Status == core.StatusAborted && push.ErrorCode == "E_ACK_VERSION_MISMATCH", "wrong confirmation version produced E_ACK_VERSION_MISMATCH and ABORTED")
	state, _ := l.state()
	foundRejected := false
	for _, event := range state.AckEvents {
		if ok && event.PushID == push.PushID && event.Disposition == core.DispositionBad {
			foundRejected = true
		}
	}
	v.check(foundRejected, "mismatched confirmation is persisted as REJECTED history without an ACK row")
	ackRows := 0
	for _, ack := range state.Acks {
		if ok && ack.PushID == push.PushID {
			ackRows++
		}
	}
	v.check(ackRows == 0, "mismatched confirmation did not create a current ACK")
	v.Artifacts = append(v.Artifacts, l.save("02b-ack-mismatch-trigger.json", resp), l.save("02b-ack-mismatch-state.json", state))
	return nil
}

func (l *lab) scenarioDuplicate(v *verification) error {
	first, err := l.mustTrigger(engine.TriggerRequest{Version: 1, ClientIDs: []string{"client-duplicate"}, TimeoutMS: 1000, RoundBudgetMS: 2000})
	if err != nil {
		return err
	}
	second, err := l.mustTrigger(engine.TriggerRequest{Version: 1, ClientIDs: []string{"client-duplicate"}, TimeoutMS: 1000, RoundBudgetMS: 2000})
	if err != nil {
		return err
	}
	p1, ok1 := findPushView(first, "client-duplicate", 1)
	p2, ok2 := findPushView(second, "client-duplicate", 1)
	v.check(ok1 && ok2 && p1.PushID == p2.PushID, "repeat trigger reused the same push id")
	v.check(ok2 && p2.Reused && !p1.Reused, "second trigger is marked reused")
	v.check(ok2 && p2.Ack != nil && p2.Ack.IngressCount == 2, "target duplicate response was deduplicated to one persisted ACK with ingress_count=2")
	stats, err := l.stats()
	if err != nil {
		return err
	}
	if ok1 {
		s := stats.ByPush[p1.PushID]
		v.check(s.Received == 1 && s.Completed == 1 && s.DuplicateAcks == 1, "client-sim received exactly one push and emitted one duplicate ACK response")
	}
	state, _ := l.state()
	ackCount := 0
	for _, ack := range state.Acks {
		if ok1 && ack.PushID == p1.PushID {
			ackCount++
		}
	}
	v.check(ackCount == 1, "exactly one current ACK row exists for the duplicate push")

	client := core.Client{
		ClientID:     "client-duplicate",
		Keys:         []string{"weight_x", "weight_y", "bias", "normalization_ratio"},
		Endpoint:     fmt.Sprintf("http://127.0.0.1:%d/push?behavior=duplicate", l.simPort),
		PickupPolicy: "LATEST",
	}
	status, _, err := l.call(http.MethodPut, "/api/v1/clients/client-duplicate", client)
	if err != nil {
		return err
	}
	v.check(status == http.StatusOK, "client subscription changed to create a real content-digest conflict")
	_, status, conflictBody, err := l.trigger(engine.TriggerRequest{Version: 1, ClientIDs: []string{"client-duplicate"}})
	if err != nil {
		return err
	}
	v.check(status == http.StatusConflict && errorCode(conflictBody) == "E_CONFLICTING_PUSH", "canonical content mismatch returned E_CONFLICTING_PUSH")
	client.Keys = []string{"weight_x", "weight_y", "bias", "normalization_ratio", "decision_threshold"}
	_, _, _ = l.call(http.MethodPut, "/api/v1/clients/client-duplicate", client)
	state, _ = l.state()
	v.Artifacts = append(v.Artifacts, l.save("03-duplicate-first.json", first), l.save("03-duplicate-second.json", second), l.save("03-duplicate-conflict.json", json.RawMessage(conflictBody)), l.save("03-duplicate-state.json", state))
	return nil
}

func (l *lab) scenarioLateAck(v *verification) error {
	stateBeforeV2, _ := l.state()
	var v1push core.Push
	var v1ack core.Ack
	for _, push := range stateBeforeV2.Pushes {
		if push.ClientID == "client-success" && push.Version == 1 {
			v1push = push
		}
	}
	for _, ack := range stateBeforeV2.Acks {
		if ack.PushID == v1push.PushID {
			v1ack = ack
		}
	}
	v.check(v1push.Status == core.StatusApplied, "client-success v1 is APPLIED before v2")
	if err := l.submitSnapshot("snapshot.v2.json"); err != nil {
		return err
	}
	resp2, err := l.mustTrigger(engine.TriggerRequest{Version: 2, ClientIDs: []string{"client-success"}, TimeoutMS: 1000, RoundBudgetMS: 2000})
	if err != nil {
		return err
	}
	p2, ok := findPushView(resp2, "client-success", 2)
	if !ok {
		return errors.New("v2 push missing")
	}
	report2, err := resultFromPush(p2)
	if err != nil {
		return err
	}
	result2, status, body, err := l.submitResult(report2)
	if err != nil {
		return err
	}
	v.check(status == http.StatusOK && result2.Status == "APPLIED", "client-success v2 applied: "+string(body))
	beforeLate, _ := l.state()
	lateAck := v1ack
	lateAck.ReceivedAt = ""
	status, lateBody, err := l.call(http.MethodPost, "/api/v1/acks", lateAck)
	if err != nil {
		return err
	}
	v.check(status == http.StatusConflict && errorCode(lateBody) == "E_STALE_ACK", "late v1 confirmation returned E_STALE_ACK")
	afterLate, _ := l.state()
	v.check(pushVersionsEqual(beforeLate.Pushes, afterLate.Pushes), "late confirmation did not alter any current push state")
	v.check(len(afterLate.AckEvents) > len(beforeLate.AckEvents), "late confirmation is recorded as durable history")
	foundStale := false
	for _, event := range afterLate.AckEvents {
		if event.ClientID == "client-success" && event.Version == 1 && event.Disposition == core.DispositionOld {
			foundStale = true
		}
	}
	v.check(foundStale, "historical ACK event disposition is STALE")

	type concurrentResult struct {
		Resp   triggerResponse
		Status int
		Body   string
		Err    string
	}
	run := func(version int64) concurrentResult {
		resp, status, body, err := l.trigger(engine.TriggerRequest{Version: version, ClientIDs: []string{"client-concurrent"}, TimeoutMS: 1500, RoundBudgetMS: 3000})
		result := concurrentResult{Resp: resp, Status: status, Body: string(body)}
		if err != nil {
			result.Err = err.Error()
		}
		return result
	}
	ch1 := make(chan concurrentResult, 1)
	ch2 := make(chan concurrentResult, 1)
	go func() { ch1 <- run(1) }()
	go func() { ch2 <- run(2) }()
	c1, c2 := <-ch1, <-ch2
	if c1.Err != "" || c2.Err != "" {
		return fmt.Errorf("concurrent triggers: v1=%s v2=%s", c1.Err, c2.Err)
	}
	pv1, ok1 := findPushView(c1.Resp, "client-concurrent", 1)
	pv2, ok2 := findPushView(c2.Resp, "client-concurrent", 2)
	v.check(ok1 && pv1.Status == core.StatusAborted && pv1.ErrorCode == "E_STALE_ACK", "older concurrent version was judged E_STALE_ACK and ABORTED")
	v.check(ok2 && pv2.Status == core.StatusAcked, "newer concurrent version reached ACKED")
	if ok2 {
		r2, err := resultFromPush(pv2)
		if err != nil {
			return err
		}
		applied, status, body, err := l.submitResult(r2)
		if err != nil {
			return err
		}
		v.check(status == http.StatusOK && applied.Status == "APPLIED", "only newer concurrent version reached APPLIED: "+string(body))
	}
	state, _ := l.state()
	for _, push := range state.Pushes {
		if push.ClientID == "client-concurrent" && push.Version == 1 {
			v.check(push.Status != core.StatusApplied, "older concurrent version never entered APPLIED")
		}
	}
	badReport := report2
	badReport.Normalized += 0.5
	badResult, badStatus, badBody, err := l.submitResult(badReport)
	if err != nil {
		return err
	}
	v.check(badStatus == http.StatusOK && badResult.Status == "MISMATCH" && len(badResult.Mismatches) >= 2, "tampered result produced mismatch details without applying")
	var incons engine.InconsistencyReport
	if err := l.getJSON("/api/v1/inconsistencies", &incons); err != nil {
		return err
	}
	v.check(len(incons.RecomputeRequired) > 0, "old/late result classified RECOMPUTE_REQUIRED")
	v.check(len(incons.Mergeable) > 0, "latest confirmed+validated result classified mergeable")
	foundMismatchDetail := false
	for _, item := range incons.Items {
		if item.ClientID == "client-success" && item.PickedVersion == 2 && item.Status == "MISMATCH" && len(item.Mismatches) > 0 && item.AckID != "" {
			foundMismatchDetail = true
		}
	}
	v.check(foundMismatchDetail, "inconsistency item includes client, version, result summary, ACK id and field differences")
	v.Artifacts = append(v.Artifacts, l.save("04-late-ack-error.json", json.RawMessage(lateBody)), l.save("04-late-ack-state.json", afterLate), l.save("04-concurrent-v1.json", c1), l.save("04-concurrent-v2.json", c2), l.save("04-result-mismatch.json", map[string]any{"http_status": badStatus, "body": json.RawMessage(badBody)}), l.save("04-inconsistencies.json", incons), l.save("04-state.json", state))
	return nil
}

func pushVersionsEqual(a, b []core.Push) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].PushID != b[i].PushID || a[i].Status != b[i].Status || a[i].Version != b[i].Version || a[i].ErrorCode != b[i].ErrorCode {
			return false
		}
	}
	return true
}

func (l *lab) scenarioSnapshotGuards(v *verification) error {
	status, body, err := l.call(http.MethodPost, "/api/v1/snapshots", json.RawMessage(`{"version":1,"generated_at":"2026-11-24T00:00:00Z","config":{"weight_x":0.75,"weight_y":0.25,"bias":0.05,"normalization_ratio":0.8,"decision_threshold":0.3},"digest":"sha256:7e1b0dd6194ec243e3a06d051b95a84cf205ce1ca7d383d1f2ca8559689fa0fb"}`))
	if err != nil {
		return err
	}
	v.check(status == http.StatusConflict && errorCode(body) == "E_STALE_SNAPSHOT", "non-increasing snapshot rejected with E_STALE_SNAPSHOT")
	status, body, err = l.call(http.MethodPost, "/api/v1/snapshots", json.RawMessage(`{"version":3,"generated_at":"2026-11-24T00:02:00Z","config":{"weight_x":0.4,"weight_y":0.6,"bias":0.0,"normalization_ratio":1.0,"decision_threshold":0.1},"digest":"sha256:invalid"}`))
	if err != nil {
		return err
	}
	v.check(status == http.StatusUnprocessableEntity && errorCode(body) == "E_SNAPSHOT_INVALID", "bad checksum rejected with E_SNAPSHOT_INVALID")
	l.save("05-stale-snapshot.json", json.RawMessage(body))
	return nil
}

func (l *lab) scenarioRecovery(v *verification) error {
	// First crash: process is terminated while a real delayed push is in PUSHED.
	ch := make(chan error, 1)
	go func() {
		_, err := l.mustTrigger(engine.TriggerRequest{Version: 2, ClientIDs: []string{"client-delay"}, TimeoutMS: 5000, RoundBudgetMS: 8000})
		ch <- err
	}()
	var observed core.Push
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, err := l.state()
		if err == nil {
			if push, ok := findPush(state, "client-delay", 2); ok && push.Status == core.StatusPushed {
				observed = push
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	v.check(observed.Status == core.StatusPushed, "forced-kill point observed PUSHED with a prepared attempt intent")
	_ = l.mw.stop()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
	}
	if err := l.startMiddleware(nil); err != nil {
		return err
	}
	var recovery core.RecoveryReport
	if err := l.getJSON("/api/v1/recovery", &recovery); err != nil {
		return err
	}
	v.check(recovery.ScannedIntents >= 1, fmt.Sprintf("restart scanned persisted PREPARED intents (%d)", recovery.ScannedIntents))
	v.check(hasAction(recovery, "push_aborted"), "recovery report contains push_aborted action")
	v.check(hasAction(recovery, "batch_aborted"), "recovery report contains batch_aborted action")
	state, err := l.state()
	if err != nil {
		return err
	}
	push, ok := findPush(state, "client-delay", 2)
	v.check(ok && push.Status == core.StatusAborted && push.ErrorCode == "E_RECOVERED_ABORT", "crashed push restored to ABORTED/E_RECOVERED_ABORT")
	v.check(len(state.PendingIntents) == 0, "no PREPARED intent remains after recovery")
	v.Artifacts = append(v.Artifacts, l.save("06-recovery-during-push.json", recovery), l.save("06-state-after-push-recovery.json", state))

	// Second crash: RESULT_APPLY intent is fsynced, then the process exits before mutation commit.
	resp, err := l.mustTrigger(engine.TriggerRequest{Version: 2, ClientIDs: []string{"client-duplicate"}, TimeoutMS: 1000, RoundBudgetMS: 2000})
	if err != nil {
		return err
	}
	pdup, ok := findPushView(resp, "client-duplicate", 2)
	if !ok || pdup.Ack == nil {
		return errors.New("client-duplicate v2 did not acknowledge")
	}
	report, err := resultFromPush(pdup)
	if err != nil {
		return err
	}
	_ = l.mw.stop()
	if err := l.startMiddleware([]string{"MW_FAULT_CRASH=RESULT_APPLY_AFTER_INTENT"}); err != nil {
		return err
	}
	_, _, _, _ = l.submitResult(report)
	waitErr := l.mw.wait(4 * time.Second)
	v.check(waitErr == nil || exitStatus(waitErr) == 137, fmt.Sprintf("fault-injected process exited after PREPARED intent (observed=%v)", waitErr))
	if err := l.startMiddleware(nil); err != nil {
		return err
	}
	var recovery2 core.RecoveryReport
	if err := l.getJSON("/api/v1/recovery", &recovery2); err != nil {
		return err
	}
	v.check(recovery2.ScannedIntents >= 1, "restart scanned the RESULT_APPLY PREPARED intent")
	v.check(hasIntentKind(recovery2, "RESULT_APPLY"), "recovery facts include the persisted RESULT_APPLY intent")
	state2, err := l.state()
	if err != nil {
		return err
	}
	push2, ok2 := findPush(state2, "client-duplicate", 2)
	v.check(ok2 && push2.Status == core.StatusAborted && push2.ErrorCode == "E_RECOVERED_ABORT", "mid-result push restored to ABORTED, not a mixed ACKED/APPLIED state")
	v.check(len(state2.PendingIntents) == 0, "result recovery leaves no unfinished intent")
	v.Artifacts = append(v.Artifacts, l.save("06-recovery-during-result.json", recovery2), l.save("06-state-after-result-recovery.json", state2), l.save("06-final-state.json", state2))
	return nil
}

func hasAction(report core.RecoveryReport, actionType string) bool {
	for _, action := range report.Actions {
		if value, ok := action["type"].(string); ok && value == actionType {
			return true
		}
	}
	return false
}

func hasIntentKind(report core.RecoveryReport, kind string) bool {
	items, _ := report.Facts["prepared_intents"].([]any)
	for _, item := range items {
		if value, ok := item.(map[string]any); ok {
			if itemKind, _ := value["kind"].(string); itemKind == kind {
				return true
			}
		}
	}
	return false
}

func numberContext(context map[string]any, key string) float64 {
	if context == nil {
		return 0
	}
	switch value := context[key].(type) {
	case float64:
		return value
	case int:
		return float64(value)
	case int64:
		return float64(value)
	default:
		return 0
	}
}

func exitStatus(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "experiment:", err)
		os.Exit(1)
	}
}
