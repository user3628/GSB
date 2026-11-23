package engine_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/engine"
	"local/configmiddleware/internal/pipeline"
	"local/configmiddleware/internal/sim"
	"local/configmiddleware/internal/store"
)

type testEnv struct {
	t      *testing.T
	store  *store.Store
	engine *engine.Engine
	sim    *httptest.Server
	stats  *sim.Handler
}

func newTestEnv(t *testing.T, concurrency int, timeout time.Duration) *testEnv {
	t.Helper()
	doc, err := pipeline.Load(filepath.Join("..", "..", "samples", "samples.json"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	handler := sim.New(doc)
	server := httptest.NewServer(handler.Routes())
	eng := engine.New(st, doc, engine.Config{
		Concurrency: concurrency, QueueCapacity: 8,
		DefaultTimeout: timeout, DefaultRoundBudget: 3 * time.Second,
	})
	t.Cleanup(func() {
		server.Close()
		_ = st.Close()
	})
	return &testEnv{t: t, store: st, engine: eng, sim: server, stats: handler}
}

func (e *testEnv) submitSnapshot(name string) core.Snapshot {
	e.t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "samples", name))
	if err != nil {
		e.t.Fatal(err)
	}
	var snap core.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		e.t.Fatal(err)
	}
	stored, err := e.engine.SubmitSnapshot(context.Background(), snap)
	if err != nil {
		e.t.Fatal(err)
	}
	return stored
}

func (e *testEnv) register(clientID, behavior string) core.Client {
	e.t.Helper()
	client := core.Client{
		ClientID:     clientID,
		Keys:         []string{"weight_x", "weight_y", "bias", "normalization_ratio", "decision_threshold"},
		Endpoint:     fmt.Sprintf("%s/push?behavior=%s", e.sim.URL, behavior),
		PickupPolicy: core.PickupLatest,
	}
	stored, err := e.engine.RegisterClient(context.Background(), client)
	if err != nil {
		e.t.Fatal(err)
	}
	return stored
}

func (e *testEnv) trigger(clientID string, version int64, timeoutMS int64) engine.TriggerResult {
	e.t.Helper()
	result, err := e.engine.TriggerPush(context.Background(), engine.TriggerRequest{
		Version: version, ClientIDs: []string{clientID}, TimeoutMS: timeoutMS, RoundBudgetMS: 2000,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return *result
}

func firstPush(t *testing.T, result engine.TriggerResult) engine.PushView {
	t.Helper()
	if len(result.Pushes) != 1 {
		t.Fatalf("pushes = %d, want 1", len(result.Pushes))
	}
	return result.Pushes[0]
}

func reportFromView(t *testing.T, push engine.PushView) core.ResultReport {
	t.Helper()
	if push.Ack == nil || push.Ack.Result == nil {
		t.Fatalf("push %s has no acknowledgement result", push.PushID)
	}
	raw, err := json.Marshal(push.Ack.Result)
	if err != nil {
		t.Fatal(err)
	}
	var report core.ResultReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	report.ClientID = push.ClientID
	report.PushID = push.PushID
	report.PickedVersion = push.Version
	return report
}

func TestPushAckResultDuplicateAndLateAck(t *testing.T) {
	env := newTestEnv(t, 2, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-success", "success")

	v1 := firstPush(t, env.trigger("client-success", 1, 1000))
	if v1.Status != core.StatusAcked || v1.Ack == nil {
		t.Fatalf("v1 push = %s, want ACKED with ack", v1.Status)
	}
	applied, err := env.engine.SubmitResult(context.Background(), reportFromView(t, v1))
	if err != nil {
		t.Fatal(err)
	}
	if applied.Status != "APPLIED" {
		t.Fatalf("v1 result = %s (%s), want APPLIED", applied.Status, applied.Reason)
	}

	duplicate := firstPush(t, env.trigger("client-success", 1, 1000))
	if !duplicate.Reused || duplicate.PushID != v1.PushID {
		t.Fatalf("duplicate push was not reused: %+v", duplicate)
	}
	resp, err := http.Get(env.sim.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var stats struct {
		ByPush map[string]sim.Stats `json:"by_push"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := stats.ByPush[v1.PushID].Received; got != 1 {
		t.Fatalf("client received %d pushes, want exactly 1", got)
	}

	env.submitSnapshot("snapshot.v2.json")
	v2 := firstPush(t, env.trigger("client-success", 2, 1000))
	var payloadRaw string
	if err := env.store.DB().QueryRow(`SELECT payload_json FROM pushes WHERE push_id=?`, v2.PushID).Scan(&payloadRaw); err != nil {
		t.Fatal(err)
	}
	var envelope core.PushEnvelope
	if err := json.Unmarshal([]byte(payloadRaw), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.BaseVersion != 1 || len(envelope.Changes) != 5 {
		t.Fatalf("v2 increment base=%d changes=%v, want base=1 and 5 changed keys", envelope.BaseVersion, envelope.Changes)
	}
	applied2, err := env.engine.SubmitResult(context.Background(), reportFromView(t, v2))
	if err != nil {
		t.Fatal(err)
	}
	if applied2.Status != "APPLIED" {
		t.Fatalf("v2 result = %s, want APPLIED", applied2.Status)
	}
	late := *v1.Ack
	late.AckID = "ack-late-v1"
	_, err = env.engine.Acknowledge(context.Background(), late)
	if err == nil || core.BizErrorOf(err).Code != "E_STALE_ACK" {
		t.Fatalf("late ack error = %v, want E_STALE_ACK", err)
	}
	state, err := env.engine.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var oldStatus string
	for _, push := range state.Pushes {
		if push.PushID == v1.PushID {
			oldStatus = push.Status
		}
	}
	if oldStatus != core.StatusApplied {
		t.Fatalf("late ack changed current v1 state to %s", oldStatus)
	}
	foundStale := false
	for _, event := range state.AckEvents {
		if event.PushID == v1.PushID && event.Disposition == core.DispositionOld {
			foundStale = true
		}
	}
	if !foundStale {
		t.Fatal("late ack was not persisted as STALE history")
	}
}

func TestRealTimeoutRefusalAndCapacityRelease(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-delay", "delay&delay_ms=500")
	env.register("client-success", "success")

	started := time.Now()
	timedOut := firstPush(t, env.trigger("client-delay", 1, 80))
	elapsed := time.Since(started)
	if timedOut.Status != core.StatusAborted || timedOut.ErrorCode != "E_PUSH_TIMEOUT" {
		t.Fatalf("timed out push = %s/%s", timedOut.Status, timedOut.ErrorCode)
	}
	if elapsed > 700*time.Millisecond {
		t.Fatalf("timeout took %s, far above configured 80ms", elapsed)
	}
	opened := contextNumber(timedOut.ErrorContext, "connections_opened")
	closed := contextNumber(timedOut.ErrorContext, "connections_closed")
	if opened < 1 || closed < 1 {
		t.Fatalf("connection accounting opened=%v closed=%v", timedOut.ErrorContext["connections_opened"], timedOut.ErrorContext["connections_closed"])
	}

	started = time.Now()
	follow := firstPush(t, env.trigger("client-success", 1, 1000))
	if follow.Status != core.StatusAcked {
		t.Fatalf("follow-up status = %s", follow.Status)
	}
	if time.Since(started) > 700*time.Millisecond {
		t.Fatal("timed-out push did not release its concurrency slot")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refusedPort := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	_, err = env.engine.RegisterClient(context.Background(), core.Client{
		ClientID:     "client-refused",
		Keys:         []string{"weight_x", "weight_y", "bias", "normalization_ratio", "decision_threshold"},
		Endpoint:     fmt.Sprintf("http://127.0.0.1:%d/push", refusedPort),
		PickupPolicy: core.PickupLatest,
	})
	if err != nil {
		t.Fatal(err)
	}
	refused := firstPush(t, env.trigger("client-refused", 1, 1000))
	if refused.Status != core.StatusAborted || refused.ErrorCode != "E_PUSH_REFUSED" {
		t.Fatalf("refused push = %s/%s", refused.Status, refused.ErrorCode)
	}
}

func TestTimeoutWhileReadingResponseBodyClosesConnection(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	release := make(chan struct{})
	stalled := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() {
		close(release)
		stalled.Close()
	}()
	_, err := env.engine.RegisterClient(context.Background(), core.Client{
		ClientID:     "client-stalled-body",
		Keys:         []string{"weight_x", "weight_y", "bias", "normalization_ratio", "decision_threshold"},
		Endpoint:     stalled.URL,
		PickupPolicy: core.PickupLatest,
	})
	if err != nil {
		t.Fatal(err)
	}

	timedOut := firstPush(t, env.trigger("client-stalled-body", 1, 80))
	if timedOut.Status != core.StatusAborted || timedOut.ErrorCode != "E_PUSH_TIMEOUT" {
		t.Fatalf("stalled-body push = %s/%s, want ABORTED/E_PUSH_TIMEOUT", timedOut.Status, timedOut.ErrorCode)
	}
	if contextNumber(timedOut.ErrorContext, "connections_opened") < 1 || contextNumber(timedOut.ErrorContext, "connections_closed") < 1 {
		t.Fatalf("stalled-body connection accounting = %v", timedOut.ErrorContext)
	}
}

func TestPushDoesNotFollowRedirects(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	var redirected int
	var redirectedMu sync.Mutex
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedMu.Lock()
		redirected++
		redirectedMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/push", http.StatusFound)
	}))
	defer source.Close()
	_, err := env.engine.RegisterClient(context.Background(), core.Client{
		ClientID:     "client-redirect",
		Keys:         []string{"weight_x", "weight_y", "bias", "normalization_ratio", "decision_threshold"},
		Endpoint:     source.URL + "/push",
		PickupPolicy: core.PickupLatest,
	})
	if err != nil {
		t.Fatal(err)
	}

	push := firstPush(t, env.trigger("client-redirect", 1, 1000))
	if push.Status != core.StatusAborted || push.ErrorCode != "E_PUSH_UPSTREAM" {
		t.Fatalf("redirected push = %s/%s, want ABORTED/E_PUSH_UPSTREAM", push.Status, push.ErrorCode)
	}
	redirectedMu.Lock()
	defer redirectedMu.Unlock()
	if redirected != 0 {
		t.Fatalf("redirect target received %d requests, want 0", redirected)
	}
}

func TestConcurrentDuplicatePushWaitsAndReuses(t *testing.T) {
	env := newTestEnv(t, 2, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-delay", "delay&delay_ms=250")

	type result struct {
		value engine.TriggerResult
		err   error
	}
	run := func() result {
		value, err := env.engine.TriggerPush(context.Background(), engine.TriggerRequest{
			Version: 1, ClientIDs: []string{"client-delay"}, TimeoutMS: 1000, RoundBudgetMS: 2000,
		})
		if err != nil {
			return result{err: err}
		}
		return result{value: *value}
	}
	start := time.Now()
	results := make(chan result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			results <- run()
		}()
	}
	wg.Wait()
	close(results)
	elapsed := time.Since(start)
	var pushIDs []string
	reused := 0
	for item := range results {
		if item.err != nil {
			t.Fatal(item.err)
		}
		push := firstPush(t, item.value)
		pushIDs = append(pushIDs, push.PushID)
		if push.Reused {
			reused++
		}
	}
	if len(pushIDs) != 2 || pushIDs[0] != pushIDs[1] {
		t.Fatalf("concurrent duplicates used different pushes: %v", pushIDs)
	}
	if reused != 1 {
		t.Fatalf("reused count = %d, want 1", reused)
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("duplicate trigger did not wait for leader result: %s", elapsed)
	}
	resp, err := http.Get(env.sim.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var stats struct {
		ByPush map[string]sim.Stats `json:"by_push"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.ByPush[pushIDs[0]].Received != 1 {
		t.Fatalf("client received %d pushes, want 1", stats.ByPush[pushIDs[0]].Received)
	}
}

func TestRoundBudgetBoundsWaitingQueue(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-delay-a", "delay&delay_ms=1000")
	env.register("client-delay-b", "delay&delay_ms=1000")
	started := time.Now()
	result, err := env.engine.TriggerPush(context.Background(), engine.TriggerRequest{
		Version: 1, ClientIDs: []string{"client-delay-a", "client-delay-b"},
		TimeoutMS: 1000, RoundBudgetMS: 120,
	})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 800*time.Millisecond {
		t.Fatalf("round budget did not bound queue execution: %s", elapsed)
	}
	if result.Batch.Status != "COMPLETED_WITH_ERRORS" {
		t.Fatalf("batch status = %s", result.Batch.Status)
	}
	for _, push := range result.Pushes {
		if push.Status != core.StatusAborted || push.ErrorCode != "E_PUSH_BUDGET_EXCEEDED" {
			t.Fatalf("push %s = %s/%s, want ABORTED/E_PUSH_BUDGET_EXCEEDED", push.ClientID, push.Status, push.ErrorCode)
		}
	}
}

func contextNumber(values map[string]any, key string) int {
	switch value := values[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}

func TestResultMismatchDoesNotApply(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-success", "success")
	push := firstPush(t, env.trigger("client-success", 1, 1000))
	report := reportFromView(t, push)
	report.Normalized += 0.25
	mismatch, err := env.engine.SubmitResult(context.Background(), report)
	if err != nil {
		t.Fatal(err)
	}
	if mismatch.Status != "MISMATCH" || len(mismatch.Mismatches) == 0 {
		t.Fatalf("result status = %s mismatches=%v", mismatch.Status, mismatch.Mismatches)
	}
	state, err := env.engine.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range state.Pushes {
		if p.PushID == push.PushID && p.Status != core.StatusAcked {
			t.Fatalf("mismatching result changed push to %s", p.Status)
		}
	}
	valid, err := env.engine.SubmitResult(context.Background(), reportFromView(t, push))
	if err != nil {
		t.Fatal(err)
	}
	if valid.Status != "APPLIED" {
		t.Fatalf("valid re-submission = %s (%s)", valid.Status, valid.Reason)
	}
}

func TestStartupRecoveryAbortsPreparedPush(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-recovery", "success")
	pushID := core.NewID("push")
	batchID := core.NewID("batch")
	intentID := core.NewID("intent")
	now := core.Now()
	err := env.store.RawWrite(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO batches(batch_id,version,status,created_at,time_budget_ms) VALUES(?,?,?,?,?)`, batchID, 1, "DISPATCHING", now, 1000); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO intents(intent_id,kind,aggregate_id,payload_json,status,created_at,detail_json) VALUES(?,?,?,?,?,?,?)`, intentID, "PUSH_ATTEMPT", pushID, "{}", "PREPARED", now, "{}"); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO pushes(push_id,batch_id,client_id,version,status,content_digest,attempt_token,attempt_intent_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			pushID, batchID, "client-recovery", 1, core.StatusPushed, "sha256:test", "attempt-test", intentID, now, now); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO push_transitions(push_id,from_status,to_status,reason,created_at) VALUES(?,?,?,?,?)`, pushID, core.StatusPending, core.StatusPushed, "test", now)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := env.engine.RecoverStartup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ScannedIntents != 1 {
		t.Fatalf("scanned intents = %d, want 1", report.ScannedIntents)
	}
	recoveryIntentID, ok := report.Facts["recovery_intent_id"].(string)
	if !ok || recoveryIntentID == "" {
		t.Fatalf("recovery intent id missing from facts: %v", report.Facts)
	}
	var recoveryIntentStatus string
	if err := env.store.DB().QueryRow(`SELECT status FROM intents WHERE intent_id=?`, recoveryIntentID).Scan(&recoveryIntentStatus); err != nil {
		t.Fatal(err)
	}
	if recoveryIntentStatus != "COMMITTED" {
		t.Fatalf("recovery intent status = %s, want COMMITTED", recoveryIntentStatus)
	}
	state, err := env.engine.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, push := range state.Pushes {
		if push.PushID == pushID && (push.Status != core.StatusAborted || push.ErrorCode != "E_RECOVERED_ABORT") {
			t.Fatalf("recovered push = %s/%s", push.Status, push.ErrorCode)
		}
	}
	if len(state.PendingIntents) != 0 {
		t.Fatalf("pending intents after recovery = %d", len(state.PendingIntents))
	}
}

func TestStartupRecoveryCompletesFullyAppliedOpenBatch(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-complete-recovery", "success")
	push := firstPush(t, env.trigger("client-complete-recovery", 1, 1000))
	applied, err := env.engine.SubmitResult(context.Background(), reportFromView(t, push))
	if err != nil {
		t.Fatal(err)
	}
	if applied.Status != "APPLIED" {
		t.Fatalf("result status = %s, want APPLIED", applied.Status)
	}
	if err := env.store.RawWrite(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE batches SET status='DISPATCHING',finished_at=NULL,error_code='' WHERE batch_id=?`, push.BatchID)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	report, err := env.engine.RecoverStartup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	foundCompleted := false
	for _, action := range report.Actions {
		if action["type"] == "batch_completed" && action["batch_id"] == push.BatchID {
			foundCompleted = true
		}
	}
	if !foundCompleted {
		t.Fatalf("recovery actions = %v, want batch_completed", report.Actions)
	}
	state, err := env.engine.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range state.Batches {
		if batch.BatchID == push.BatchID && batch.Status != "COMPLETED" {
			t.Fatalf("batch status = %s, want COMPLETED", batch.Status)
		}
	}
	for _, persisted := range state.Pushes {
		if persisted.PushID == push.PushID && persisted.Status != core.StatusApplied {
			t.Fatalf("push status = %s, want APPLIED", persisted.Status)
		}
	}
}

func TestRecoveryScanIsIntentFirstAcrossCrash(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-recovery-crash", "success")
	pushID := core.NewID("push")
	batchID := core.NewID("batch")
	intentID := core.NewID("intent")
	now := core.Now()
	if err := env.store.RawWrite(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO batches(batch_id,version,status,created_at,time_budget_ms) VALUES(?,?,?,?,?)`, batchID, 1, "DISPATCHING", now, 1000); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO intents(intent_id,kind,aggregate_id,payload_json,status,created_at,detail_json) VALUES(?,?,?,?,?,?,?)`, intentID, "PUSH_ATTEMPT", pushID, "{}", "PREPARED", now, "{}"); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO pushes(push_id,batch_id,client_id,version,status,content_digest,attempt_token,attempt_intent_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			pushID, batchID, "client-recovery-crash", 1, core.StatusPushed, "sha256:test", "attempt-test", intentID, now, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	dbPath := env.store.Path()
	if err := env.store.Close(); err != nil {
		t.Fatal(err)
	}

	samplesPath, err := filepath.Abs(filepath.Join("..", "..", "samples", "samples.json"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRecoveryCrashHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(),
		"GO_WANT_RECOVERY_CRASH_HELPER=1",
		"RECOVERY_TEST_DB="+dbPath,
		"RECOVERY_TEST_SAMPLES="+samplesPath,
		"MW_FAULT_CRASH=RECOVERY_SCAN_BEFORE_COMMIT",
	)
	output, err := cmd.CombinedOutput()
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 137 || !strings.Contains(string(output), "FAULT_INJECTION") {
		t.Fatalf("recovery crash helper err=%v output=%s", err, output)
	}

	reopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	var attemptStatus, pushStatus string
	if err := reopened.DB().QueryRow(`SELECT status FROM intents WHERE intent_id=?`, intentID).Scan(&attemptStatus); err != nil {
		t.Fatal(err)
	}
	if err := reopened.DB().QueryRow(`SELECT status FROM pushes WHERE push_id=?`, pushID).Scan(&pushStatus); err != nil {
		t.Fatal(err)
	}
	var reportCount, preparedRecovery int
	if err := reopened.DB().QueryRow(`SELECT COUNT(*) FROM recovery_reports`).Scan(&reportCount); err != nil {
		t.Fatal(err)
	}
	if err := reopened.DB().QueryRow(`SELECT COUNT(*) FROM intents WHERE kind='RECOVERY_SCAN' AND status='PREPARED'`).Scan(&preparedRecovery); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "PREPARED" || pushStatus != core.StatusPushed || reportCount != 0 || preparedRecovery != 1 {
		t.Fatalf("post-crash facts attempt=%s push=%s reports=%d prepared_recovery=%d", attemptStatus, pushStatus, reportCount, preparedRecovery)
	}

	doc, err := pipeline.Load(samplesPath)
	if err != nil {
		t.Fatal(err)
	}
	recoveryEngine := engine.New(reopened, doc, engine.Config{})
	report, err := recoveryEngine.RecoverStartup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ScannedIntents != 2 {
		t.Fatalf("scanned intents = %d, want PUSH_ATTEMPT and crashed RECOVERY_SCAN", report.ScannedIntents)
	}
	var oldRecoveryStatus string
	if err := reopened.DB().QueryRow(`SELECT status FROM intents WHERE kind='RECOVERY_SCAN' AND intent_id<>? ORDER BY created_at LIMIT 1`, report.Facts["recovery_intent_id"]).Scan(&oldRecoveryStatus); err != nil {
		t.Fatal(err)
	}
	if oldRecoveryStatus != "RECOVERED" {
		t.Fatalf("crashed recovery intent status = %s, want RECOVERED", oldRecoveryStatus)
	}
	var pending int
	if err := reopened.DB().QueryRow(`SELECT COUNT(*) FROM intents WHERE status='PREPARED'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("pending intents after second recovery = %d", pending)
	}
}

func TestRecoveryCrashHelper(t *testing.T) {
	if os.Getenv("GO_WANT_RECOVERY_CRASH_HELPER") != "1" {
		return
	}
	doc, err := pipeline.Load(os.Getenv("RECOVERY_TEST_SAMPLES"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(os.Getenv("RECOVERY_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	eng := engine.New(st, doc, engine.Config{})
	if _, err := eng.RecoverStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Fatal("RECOVERY_SCAN_BEFORE_COMMIT did not terminate the helper process")
}

func TestIllegalAckTransitionDoesNotMutatePersistedState(t *testing.T) {
	env := newTestEnv(t, 1, time.Second)
	env.submitSnapshot("snapshot.v1.json")
	env.register("client-illegal", "success")
	pushID := core.NewID("push")
	batchID := core.NewID("batch")
	now := core.Now()
	err := env.store.RawWrite(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO batches(batch_id,version,status,created_at,time_budget_ms) VALUES(?,?,?,?,?)`, batchID, 1, "DISPATCHING", now, 1000); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO pushes(push_id,batch_id,client_id,version,status,content_digest,attempt_token,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`,
			pushID, batchID, "client-illegal", 1, core.StatusPending, "sha256:test", "attempt-test", now, now)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = env.engine.Acknowledge(context.Background(), core.Ack{
		AckID: "ack-illegal", PushID: pushID, ClientID: "client-illegal", Version: 1,
		ContentDigest: "sha256:test", Source: "test",
	})
	if err == nil || core.BizErrorOf(err).Code != "E_INVALID_TRANSITION" {
		t.Fatalf("ack error = %v, want E_INVALID_TRANSITION", err)
	}
	state, err := env.engine.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, push := range state.Pushes {
		if push.PushID == pushID && push.Status != core.StatusPending {
			t.Fatalf("illegal transition changed push to %s", push.Status)
		}
	}
	if len(state.Acks) != 0 || len(state.AckEvents) != 0 {
		t.Fatalf("illegal transition persisted ACK state: acks=%d events=%d", len(state.Acks), len(state.AckEvents))
	}
}
