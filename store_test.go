package cockpitaudio

import (
	"path/filepath"
	"sync"
	"testing"
)

func TestStoreSequenceCASAndTamperClassification(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "checkpoints.db")
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	artifact := map[string]string{"stage": "vad", "value": "stable"}
	out1, serr := store.Publish(PublishInput{ShardID: "shard-001", Stage: StageVAD, Sequence: 1, Artifact: artifact, ConfigVersion: "cfg1"})
	if serr != nil || out1.Classification != "normal_advance" || out1.Checkpoint.Sequence != 1 {
		t.Fatalf("publish1=%+v err=%v", out1, serr)
	}

	_, dup := store.Publish(PublishInput{ShardID: "shard-001", Stage: StageVAD, Sequence: 1, Artifact: artifact, ConfigVersion: "cfg1"})
	ae, ok := AsError(dup)
	if !ok || ae.Code != CodeStaleCheckpoint || ae.Diagnostics["classification"] != "duplicate_or_behind" {
		t.Fatalf("duplicate error=%#v", dup)
	}

	if _, err := store.Publish(PublishInput{ShardID: "shard-001", Stage: StageAttribution, Sequence: 2, Artifact: artifact, ConfigVersion: "cfg1"}); err != nil {
		t.Fatalf("publish2: %v", err)
	}
	_, gap := store.Publish(PublishInput{ShardID: "shard-001", Stage: StageTimeline, Sequence: 4, Artifact: artifact, ConfigVersion: "cfg1"})
	if ae, ok := AsError(gap); !ok || ae.Code != CodeStaleCheckpoint || ae.Diagnostics["classification"] != "future_gap" {
		t.Fatalf("gap error=%#v", gap)
	}
	_, cross := store.Publish(PublishInput{ShardID: "shard-002", ExpectedShardID: "shard-001", Stage: StageVAD, Sequence: 1, Artifact: artifact})
	if ae, ok := AsError(cross); !ok || ae.Code != CodeStaleCheckpoint || ae.Diagnostics["classification"] != "cross_shard" {
		t.Fatalf("cross shard error=%#v", cross)
	}

	// A different database file has its own counter. Reopen same path for a real second worker.
	worker2, oerr := OpenStore(dbPath)
	if oerr != nil {
		t.Fatal(oerr)
	}
	defer worker2.Close()
	out3, perr := worker2.Publish(PublishInput{ShardID: "shard-001", Stage: StageTimeline, Sequence: 0, Artifact: artifact, PublisherID: "worker2"})
	if perr != nil || out3.Checkpoint.Sequence != 3 {
		t.Fatalf("second worker publish seq3: %+v err=%v", out3, perr)
	}

	if ver, verr := store.VerifySnapshot("shard-001", StageVAD); verr != nil || !ver.Healthy {
		t.Fatalf("verify healthy: %+v %v", ver, verr)
	}

	if _, err := store.db.Exec(`UPDATE checkpoints SET artifact_digest='deadbeef' WHERE shard_id=? AND stage=? AND is_valid=1`, "shard-001", StageVAD); err != nil {
		t.Fatal(err)
	}
	ver, verr := store.VerifySnapshot("shard-001", StageVAD)
	ae, ok = AsError(verr)
	if !ok || ae.Code != CodeStaleCheckpoint || ver.Classification != "tamper" || ae.Diagnostics["classification"] != "tamper" {
		t.Fatalf("digest tamper ver=%+v err=%#v", ver, verr)
	}
}

func TestSnapshotRollbackClassification(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "rollback.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	artifact := map[string]int{"v": 1}
	if _, err := store.Publish(PublishInput{ShardID: "s1", Stage: StageVAD, Sequence: 1, Artifact: artifact}); err != nil {
		t.Fatal(err)
	}
	artifact["v"] = 2
	if _, err := store.Publish(PublishInput{ShardID: "s1", Stage: StageVAD, Sequence: 2, Artifact: artifact}); err != nil {
		t.Fatal(err)
	}

	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE checkpoints SET is_valid=0 WHERE shard_id='s1' AND sequence=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE checkpoints SET is_valid=1 WHERE shard_id='s1' AND sequence=1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ver, verr := store.VerifySnapshot("s1", StageVAD)
	ae, ok := AsError(verr)
	if !ok || ae.Code != CodeStaleCheckpoint || ver == nil || ver.Classification != "rollback" {
		t.Fatalf("rollback ver=%+v err=%#v", ver, verr)
	}
}

func TestConcurrentPublishSameSequenceOnlyOneWins(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const workers = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, stale := 0, 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, perr := store.Publish(PublishInput{
				ShardID: "shard-concurrent", Stage: StageVAD, Sequence: 1,
				Artifact: map[string]int{"worker": i}, PublisherID: "worker",
			})
			mu.Lock()
			defer mu.Unlock()
			if perr == nil {
				success++
				return
			}
			if ae, ok := AsError(perr); ok && ae.Code == CodeStaleCheckpoint {
				stale++
			} else {
				t.Errorf("unexpected publish error: %v", perr)
			}
		}(i)
	}
	wg.Wait()
	if success != 1 || stale != workers-1 {
		t.Fatalf("success=%d stale=%d, want 1/%d", success, stale, workers-1)
	}
}
