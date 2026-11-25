package qos

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestStartupRecoveryMarksPendingIntentAborted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "qos.db")
	store, err := OpenStore(StoreOptions{DBPath: dbPath, DataDir: filepath.Join(dir, "data"), SampleSeed: DefaultSampleSeed})
	if err != nil {
		t.Fatal(err)
	}
	snapshotPolicy(t, store, 1, []Queue{testQueue("root", "", 10_000, 20_000, 100, 200, 1)})
	now := nowRFC3339()
	_, err = store.DB().Exec(`INSERT INTO intents(request_id,kind,status,base_version,target_version,phase,payload_json,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, "crashed", "increment", "pending", 1, 2, "increment_rebuild", "{}", now, now)
	if err != nil {
		t.Fatal(err)
	}
	// Defensive future-version rows simulate remnants that must not be visible.
	_, _ = store.DB().Exec(`INSERT INTO queue_view_history(version,queue_id,view_json) VALUES(2,'root','{}')`)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(StoreOptions{DBPath: dbPath, DataDir: filepath.Join(dir, "data"), SampleSeed: DefaultSampleSeed})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, be := reopened.GetRecoveryReport(context.Background())
	if be != nil {
		t.Fatal(be)
	}
	if report.CurrentVersion != 1 {
		t.Fatalf("current=%d", report.CurrentVersion)
	}
	if len(report.Reports) != 1 || report.Reports[0].RequestID != "crashed" || report.Reports[0].Status != "ABORTED" {
		t.Fatalf("reports=%+v", report.Reports)
	}
	status, be := reopened.GetIntent(context.Background(), "crashed")
	if be != nil {
		t.Fatal(be)
	}
	if status.Status != "aborted" || status.Phase != "aborted" {
		t.Fatalf("intent=%+v", status)
	}
	var leftovers int
	if err := reopened.DB().QueryRow(`SELECT COUNT(*) FROM queue_view_history WHERE version > 1`).Scan(&leftovers); err != nil {
		t.Fatal(err)
	}
	if leftovers != 0 {
		t.Fatalf("future derived rows remain: %d", leftovers)
	}
	if v, _ := reopened.CurrentVersion(context.Background()); v != 1 {
		t.Fatalf("recovery changed version: %d", v)
	}
	raw, _ := json.Marshal(report)
	t.Logf("recovery report: %s", raw)
}
