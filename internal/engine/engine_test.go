package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"meshmigrate/internal/compat"
	"meshmigrate/internal/samples"
	"meshmigrate/internal/store"
)

func testEngine(t *testing.T, opt Options) (*Engine, *store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	if _, err := samples.Generate(dir); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "mesh.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	matrix, err := compat.LoadMatrix()
	if err != nil {
		t.Fatal(err)
	}
	return New(st, matrix, opt), st, dir
}

func readSample(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestConcurrentDuplicateImportAndRecovery(t *testing.T) {
	eng, st, dir := testEngine(t, Options{})
	legacy := readSample(t, dir, samples.LegacyBinary)
	current := readSample(t, dir, samples.CurrentJSONL)
	ctx := context.Background()
	if got := eng.Import(ctx, ImportOptions{RequestID: "v1", Body: legacy, FormatHint: "auto"}); got.Status != 201 {
		t.Fatalf("v1 import failed: %d %s", got.Status, got.Body)
	}

	type result struct {
		resp ImportResponse
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			results <- result{resp: eng.Import(ctx, ImportOptions{RequestID: "same-v2", Body: current, FormatHint: "auto"})}
		}()
	}
	close(start)
	a, b := <-results, <-results
	if a.resp.Status != 201 || b.resp.Status != 201 {
		t.Fatalf("concurrent duplicate imports: %d %s / %d %s", a.resp.Status, a.resp.Body, b.resp.Status, b.resp.Body)
	}
	if !bytes.Equal(a.resp.Body, b.resp.Body) || a.resp.ReportID != b.resp.ReportID {
		t.Fatalf("duplicate request was not executed once:\n%s\n%s", a.resp.Body, b.resp.Body)
	}

	now := time.Now().UTC().UnixMilli()
	intentID := "pending-crash-intent"
	requestID := "pending-crash-request"
	if err := st.CreateIntent(ctx, store.IntentRecord{
		IntentID: intentID, RequestID: requestID, Kind: "import", SnapshotVersion: 99,
		Status: "pending", CreatedAtMs: now, Facts: map[string]any{"test": "pending intent"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRequestPending(ctx, store.RequestRecord{RequestID: requestID, RequestHash: "sha256:pending", CreatedAtMs: now}); err != nil {
		t.Fatal(err)
	}
	recovery, err := eng.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery == nil || recovery.PendingCount != 1 {
		t.Fatalf("expected one recovered intent: %+v", recovery)
	}
	active, err := st.ActiveVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if active != 2 {
		t.Fatalf("recovery changed active version to %d", active)
	}
	if pending, _ := st.ListPendingIntents(ctx); len(pending) != 0 {
		t.Fatalf("pending intents remain: %+v", pending)
	}
	req, err := st.GetRequest(ctx, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if req.Status != "aborted" || req.HTTPStatus != 503 {
		t.Fatalf("request not marked aborted: %+v", req)
	}
	reportBody, ae := eng.RecoveryReport(ctx)
	if ae != nil || !bytes.Contains(reportBody, []byte(`"activeSnapshotVersion":2`)) || !bytes.Contains(reportBody, []byte(`"pendingIntentCount":1`)) {
		t.Fatalf("unexpected recovery report: %s %v", reportBody, ae)
	}
}

func TestReadersSeeOnlyCompleteVersionDuringDerivedRebuild(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "mid-rebuild.marker")
	eng, _, dir := testEngine(t, Options{EnableTestHooks: true, MarkerPath: marker, PauseDuration: 500 * time.Millisecond})
	legacy := readSample(t, dir, samples.LegacyBinary)
	current := readSample(t, dir, samples.CurrentJSONL)
	ctx := context.Background()
	if got := eng.Import(ctx, ImportOptions{RequestID: "v1", Body: legacy, FormatHint: "auto"}); got.Status != 201 {
		t.Fatalf("v1 import: %d %s", got.Status, got.Body)
	}
	done := make(chan ImportResponse, 1)
	go func() {
		done <- eng.Import(ctx, ImportOptions{RequestID: "paused-v2", Body: current, FormatHint: "auto", PauseMidRebuild: true})
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mid-rebuild marker was not written")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The writer holds an uncommitted transaction with version-2 derived rows.
	// Independent reads must still see version 1, never a mixture.
	policiesBody, ae := eng.Policies(ctx, 0, "")
	if ae != nil || !bytes.Contains(policiesBody, []byte(`"snapshotVersion":1`)) || bytes.Contains(policiesBody, []byte(`"snapshotVersion":2`)) {
		t.Fatalf("policy read crossed transaction boundary: %s %v", policiesBody, ae)
	}
	derivedBody, ae := eng.Derived(ctx, 0)
	if ae != nil || !bytes.Contains(derivedBody, []byte(`"snapshotVersion":1`)) {
		t.Fatalf("derived read crossed transaction boundary: %s %v", derivedBody, ae)
	}
	versionsBody, ae := eng.VersionList(ctx)
	if ae != nil || !bytes.Contains(versionsBody, []byte(`"activeVersion":1`)) {
		t.Fatalf("version read crossed transaction boundary: %s %v", versionsBody, ae)
	}
	var final ImportResponse
	select {
	case final = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("paused import did not finish")
	}
	if final.Status != 201 {
		t.Fatalf("paused import failed: %d %s", final.Status, final.Body)
	}
	policiesBody, ae = eng.Policies(ctx, 0, "")
	if ae != nil || !bytes.Contains(policiesBody, []byte(`"snapshotVersion":2`)) {
		t.Fatalf("new version not visible after commit: %s %v", policiesBody, ae)
	}
}
