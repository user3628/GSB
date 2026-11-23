package engine

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
	"recoveryengine/internal/store"
	"recoveryengine/samples"
)

func sampleSnapshot(t *testing.T) model.Snapshot {
	t.Helper()
	data, err := samples.Read("full-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, business := spec.DecodeSnapshot(data)
	if business != nil {
		t.Fatal(business)
	}
	normalized, business := spec.NormalizeSnapshot(raw, true)
	if business != nil {
		t.Fatal(business)
	}
	return normalized.Snapshot
}

func sampleIncrement(t *testing.T, name, key string, baseVersion int64) model.IncrementRequest {
	t.Helper()
	data, err := samples.Read(name)
	if err != nil {
		t.Fatal(err)
	}
	request, business := spec.DecodeIncrement(data)
	if business != nil {
		t.Fatal(business)
	}
	request.IdempotencyKey = key
	request.BaseVersion = baseVersion
	return request
}

func bootstrapEngine(t *testing.T, path string, snapshot model.Snapshot) *Engine {
	t.Helper()
	app, err := New(context.Background(), path, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, business := app.CommitSnapshot(context.Background(), "bootstrap-"+filepath.Base(path), snapshot, CommitOptions{}); business != nil {
		app.Close()
		t.Fatal(business)
	}
	return app
}

func TestIncrementCascadeIdempotencyAndStaleBase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "engine.db")
	app := bootstrapEngine(t, path, sampleSnapshot(t))
	defer app.Close()

	request := sampleIncrement(t, "increments/resource-adjust.json", "test-resource", 1)
	first, business := app.CommitIncrement(ctx, request, CommitOptions{})
	if business != nil {
		t.Fatal(business)
	}
	if first.Version != 2 || first.BaseVersion != 1 {
		t.Fatalf("unexpected versions: %#v", first)
	}
	if len(first.Cascade.Invalidated) == 0 || len(first.Cascade.Rebuilt) == 0 {
		t.Fatalf("expected non-empty cascade: %#v", first.Cascade)
	}

	replay, business := app.CommitIncrement(ctx, request, CommitOptions{})
	if business != nil {
		t.Fatal(business)
	}
	if !replay.Replayed || replay.Version != first.Version || replay.IntentID != first.IntentID {
		t.Fatalf("idempotent replay did not reuse the result: first=%#v replay=%#v", first, replay)
	}

	stale := sampleIncrement(t, "increments/stale-base.json", "test-stale", 1)
	if _, business := app.CommitIncrement(ctx, stale, CommitOptions{}); business == nil || business.Code != model.CodeStaleBase {
		t.Fatalf("expected E_STALE_BASE, got %#v", business)
	}
	if app.State().Version != 2 {
		t.Fatalf("stale submission changed state: %d", app.State().Version)
	}
}

func TestConcurrentIncrementsSingleWinner(t *testing.T) {
	ctx := context.Background()
	app := bootstrapEngine(t, filepath.Join(t.TempDir(), "race.db"), sampleSnapshot(t))
	defer app.Close()

	first := sampleIncrement(t, "increments/resource-adjust.json", "race-a", 1)
	second := sampleIncrement(t, "increments/probe-adjust.json", "race-b", 1)
	type outcome struct {
		result *CommitResult
		err    *model.BusinessError
	}
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, request := range []model.IncrementRequest{first, second} {
		request := request
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, business := app.CommitIncrement(ctx, request, CommitOptions{})
			outcomes <- outcome{result: result, err: business}
		}()
	}
	wg.Wait()
	close(outcomes)
	successes, stale := 0, 0
	for item := range outcomes {
		switch {
		case item.err == nil && item.result != nil:
			successes++
		case item.err != nil && item.err.Code == model.CodeStaleBase:
			stale++
		default:
			t.Fatalf("unexpected concurrent outcome: %#v / %#v", item.result, item.err)
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("expected one winner and one stale loser, got success=%d stale=%d", successes, stale)
	}
	if app.State().Version != 2 {
		t.Fatalf("unexpected final version: %d", app.State().Version)
	}
}

func TestConcurrentSameIdempotencyKeyExecutesOnce(t *testing.T) {
	ctx := context.Background()
	app := bootstrapEngine(t, filepath.Join(t.TempDir(), "idempotent.db"), sampleSnapshot(t))
	defer app.Close()
	request := sampleIncrement(t, "increments/edge-add.json", "same-key", 1)
	type outcome struct {
		result *CommitResult
		err    *model.BusinessError
	}
	results := make(chan outcome, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, business := app.CommitIncrement(ctx, request, CommitOptions{})
			results <- outcome{result: result, err: business}
		}()
	}
	wg.Wait()
	close(results)
	original, replays := 0, 0
	for item := range results {
		if item.err != nil {
			t.Fatal(item.err)
		}
		if item.result.Replayed {
			replays++
		} else {
			original++
		}
	}
	if original != 1 || replays != 7 {
		t.Fatalf("expected one execution and seven replays, got original=%d replays=%d", original, replays)
	}
	intents, err := app.ListIntents(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, intent := range intents {
		if intent.SubmissionKey == "same-key" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("idempotent key created %d intents, want 1", count)
	}
}

func TestCycleRejectionKeepsCommittedVersion(t *testing.T) {
	ctx := context.Background()
	app := bootstrapEngine(t, filepath.Join(t.TempDir(), "cycle.db"), sampleSnapshot(t))
	defer app.Close()
	request := sampleIncrement(t, "increments/cycle-add.json", "cycle-test", 1)
	result, business := app.CommitIncrement(ctx, request, CommitOptions{})
	if result != nil || business == nil || business.Code != model.CodeDependencyCycle {
		t.Fatalf("expected cycle rejection, got result=%#v error=%#v", result, business)
	}
	if business.Partial == nil {
		t.Fatal("cycle rejection must include partial ordering result")
	}
	if app.State().Version != 1 {
		t.Fatalf("cycle changed committed version to %d", app.State().Version)
	}
	if app.State().Derivation.Cycles != nil && len(app.State().Derivation.Cycles) != 0 {
		t.Fatalf("committed state is cycle-polluted: %#v", app.State().Derivation.Cycles)
	}
	count, err := app.SnapshotCount(ctx)
	if err != nil || count != 1 {
		t.Fatalf("snapshot count = %d, err=%v", count, err)
	}
}

func TestReadersSeePreviousVersionDuringHeldIntent(t *testing.T) {
	ctx := context.Background()
	app := bootstrapEngine(t, filepath.Join(t.TempDir(), "held.db"), sampleSnapshot(t))
	defer app.Close()
	before := app.State()
	request := sampleIncrement(t, "increments/resource-adjust.json", "held-reader", 1)
	done := make(chan *model.BusinessError, 1)
	go func() {
		_, business := app.CommitIncrement(ctx, request, CommitOptions{HoldAfterIntent: 450 * time.Millisecond})
		done <- business
	}()
	deadline := time.Now().Add(2 * time.Second)
	observedPending := false
	for time.Now().Before(deadline) {
		intents, err := app.ListIntents(ctx, "pending", 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, intent := range intents {
			if intent.SubmissionKey == "held-reader" {
				observedPending = true
			}
		}
		if observedPending {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !observedPending {
		t.Fatal("did not observe pending intent")
	}
	during := app.State()
	if during.Version != before.Version || during.Derivation.DerivedDigest != before.Derivation.DerivedDigest {
		t.Fatalf("reader observed partial state: before=%d/%s during=%d/%s", before.Version, before.Derivation.DerivedDigest, during.Version, during.Derivation.DerivedDigest)
	}
	if business := <-done; business != nil {
		t.Fatal(business)
	}
	after := app.State()
	if after.Version != 2 || after.Derivation.DerivedDigest == before.Derivation.DerivedDigest {
		t.Fatalf("new version was not published atomically: before=%d after=%d", before.Version, after.Version)
	}
}

func TestDrillRealFailureModesAndReservationRelease(t *testing.T) {
	closedAddress := closedLoopbackAddress(t)
	testServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mode := request.URL.Query().Get("mode")
		writer.Header().Set("Connection", "close")
		switch mode {
		case "timeout":
			select {
			case <-time.After(2 * time.Second):
				http.Error(writer, "late", http.StatusGatewayTimeout)
			case <-request.Context().Done():
			}
		case "reject":
			http.Error(writer, "rejected", http.StatusServiceUnavailable)
		default:
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{"ok":true}`))
		}
	}))
	defer testServer.Close()

	snapshot := sampleSnapshot(t)
	for index := range snapshot.Components {
		component := &snapshot.Components[index]
		switch component.ID {
		case "gateway":
			component.HealthProbe.URL = testServer.URL + "/health?mode=ok&component=gateway"
			component.RecoveryRequest.URL = testServer.URL + "/recover?mode=ok&component=gateway"
		case "telemetry":
			component.HealthProbe.URL = "http://" + closedAddress + "/health"
			component.RecoveryRequest.URL = "http://" + closedAddress + "/recover"
		case "reporter":
			component.HealthProbe.URL = testServer.URL + "/health?mode=timeout"
			component.RecoveryRequest.URL = testServer.URL + "/recover?mode=timeout"
			component.HealthProbe.TimeoutMs = 100
			component.RecoveryRequest.TimeoutMs = 100
		case "notifier":
			component.HealthProbe.URL = testServer.URL + "/health?mode=reject"
			component.RecoveryRequest.URL = testServer.URL + "/recover?mode=reject"
		}
	}
	snapshot.ChecksumSHA256 = spec.ComputeChecksum(snapshot)
	app := bootstrapEngine(t, filepath.Join(t.TempDir(), "drill.db"), snapshot)
	defer app.Close()
	request := model.DrillRequest{
		IdempotencyKey: "drill-test", Version: 1, FailedComponents: []string{"gateway"}, MaxConcurrency: 2,
	}
	result, business := app.ExecuteDrill(context.Background(), request, CommitOptions{})
	if business != nil {
		t.Fatal(business)
	}
	wanted := []string{
		model.CodeProbeConnectionRefused, model.CodeRecoverConnectionRef,
		model.CodeProbeTimeout, model.CodeRecoverTimeout,
		model.CodeProbeRejected, model.CodeRecoverRejected,
	}
	for _, code := range wanted {
		if result.CodeCounts[code] == 0 {
			t.Fatalf("drill was missing code %s: %#v", code, result.CodeCounts)
		}
	}
	if result.AllOperationsSucceeded {
		t.Fatal("drill with timeout/refusal/rejection should not report all operations successful")
	}
	if !result.ReservationsReleased || len(app.PollReservations()) != 0 {
		t.Fatalf("reservations were not released: result=%t active=%#v", result.ReservationsReleased, app.PollReservations())
	}
	if app.ReservationSummary()["released_total"].(uint64) < 4 {
		t.Fatalf("expected at least four released reservations: %#v", app.ReservationSummary())
	}
	replay, business := app.ExecuteDrill(context.Background(), request, CommitOptions{})
	if business != nil {
		t.Fatal(business)
	}
	if !replay.Replayed || replay.ID != result.ID {
		t.Fatalf("drill idempotency failed: original=%s replay=%s replayed=%t", result.ID, replay.ID, replay.Replayed)
	}
	if _, err := app.DrillByID(context.Background(), result.ID); err != nil {
		t.Fatalf("persisted drill cannot be read: %v", err)
	}
}

func TestCrashRecoveryReplayMatchesUninterruptedRun(t *testing.T) {
	ctx := context.Background()
	request := sampleIncrement(t, "increments/resource-adjust.json", "crash-replay", 1)
	baseSnapshot := sampleSnapshot(t)

	baselinePath := filepath.Join(t.TempDir(), "baseline.db")
	baseline := bootstrapEngine(t, baselinePath, baseSnapshot)
	baselineResult, business := baseline.CommitIncrement(ctx, request, CommitOptions{})
	if business != nil {
		t.Fatal(business)
	}
	baselineState := baseline.State()
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}

	crashPath := filepath.Join(t.TempDir(), "crash.db")
	crashApp := bootstrapEngine(t, crashPath, baseSnapshot)
	if err := crashApp.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := store.Open(ctx, crashPath)
	if err != nil {
		t.Fatal(err)
	}
	now := store.Now()
	hash, err := hashPayload("increment", request)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.PrepareSubmission(ctx, request.IdempotencyKey, "increment", hash, now); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(request)
	if err := database.InsertIntent(ctx, store.Intent{
		ID: "intent-crash-fixture", SubmissionKey: request.IdempotencyKey, Kind: "increment",
		BaseVersion: 1, TargetVersion: 2, PayloadJSON: payload, Phase: "awaiting_atomic_commit",
		State: "pending", CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := New(ctx, crashPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.State().Version != 1 {
		t.Fatalf("recovered version advanced unexpectedly: %d", recovered.State().Version)
	}
	reports, err := recovered.ListRecoveryReports(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].ScannedIntents < 1 || !strings.Contains(string(reports[0].Aborted), request.IdempotencyKey) {
		t.Fatalf("startup recovery report did not record aborted intent: %#v", reports)
	}
	intents, err := recovered.ListIntents(ctx, "aborted", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, intent := range intents {
		if intent.SubmissionKey == request.IdempotencyKey && intent.Phase == "recovered_on_startup" {
			found = true
		}
	}
	if !found {
		t.Fatalf("pending intent was not marked aborted: %#v", intents)
	}

	retried, business := recovered.CommitIncrement(ctx, request, CommitOptions{})
	if business != nil {
		t.Fatal(business)
	}
	recoveredState := recovered.State()
	if retried.Version != baselineResult.Version ||
		retried.DerivedDigest != baselineResult.DerivedDigest ||
		recoveredState.Derivation.DerivedDigest != baselineState.Derivation.DerivedDigest ||
		recoveredState.ChecksumSHA256 != baselineState.ChecksumSHA256 {
		t.Fatalf("replay differs from uninterrupted execution:\nbaseline version=%d derived=%s checksum=%s\nrecovered version=%d derived=%s checksum=%s",
			baselineResult.Version, baselineResult.DerivedDigest, baselineState.ChecksumSHA256,
			retried.Version, retried.DerivedDigest, recoveredState.ChecksumSHA256)
	}
}

func closedLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
