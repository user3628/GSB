package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"searchlab/internal/apperr"
	"searchlab/internal/stage"
)

func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	db := filepath.Join(dir, "state.db")
	lock := filepath.Join(dir, "state.lock")
	store, err := Open(db, lock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store, db
}

func artifactPayload(label string) []byte {
	return []byte(`{"artifact":"` + label + `"}`)
}

func publish(t *testing.T, store *Store, seq int64, stageName, pipeline, shard string, payload []byte) Record {
	t.Helper()
	record, err := store.Publish(context.Background(), PublishRequest{
		Seq: seq, Stage: stageName, ShardDigest: shard, InputDigest: shard,
		ConfigVersion: "config", PipelineID: pipeline, Payload: payload,
	})
	if err != nil {
		t.Fatalf("publish seq=%d stage=%s: %v", seq, stageName, err)
	}
	return record
}

func TestPublishSequencePrerequisitesAndUniqueActive(t *testing.T) {
	store, _ := testStore(t)
	publish(t, store, 1, stage.Tokenize, "p1", "shard", artifactPayload("tokenize"))
	_, err := store.Publish(context.Background(), PublishRequest{Seq: 1, Stage: stage.Tokenize, ShardDigest: "shard", InputDigest: "i", ConfigVersion: "c", PipelineID: "p1", Payload: artifactPayload("duplicate")})
	if !apperr.IsCode(err, apperr.CodeStaleCheckpoint) {
		t.Fatalf("duplicate seq error = %v, want E_STALE_CHECKPOINT", err)
	}
	_, err = store.Publish(context.Background(), PublishRequest{Seq: 2, Stage: stage.Stats, ShardDigest: "shard", InputDigest: "i", ConfigVersion: "c", PipelineID: "wrong-pipeline", Payload: artifactPayload("stats")})
	if !apperr.IsCode(err, apperr.CodeStaleCheckpoint) {
		t.Fatalf("cross pipeline error = %v, want E_STALE_CHECKPOINT", err)
	}
	publish(t, store, 2, stage.Stats, "p1", "shard", artifactPayload("stats"))
	publish(t, store, 3, stage.Tokenize, "p1", "shard", artifactPayload("tokenize-v2"))
	publish(t, store, 4, stage.Stats, "p1", "shard", artifactPayload("stats-v2"))
	var active int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM checkpoints WHERE stage=? AND shard_digest=? AND active=1`, stage.Tokenize, "shard").Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("active tokenize checkpoints = %d, want 1", active)
	}
	current, err := store.CurrentSeq(context.Background())
	if err != nil || current != 4 {
		t.Fatalf("current seq = %d err=%v, want 4", current, err)
	}
}

func TestMissingPrerequisiteIsBusinessError(t *testing.T) {
	store, _ := testStore(t)
	_, err := store.Publish(context.Background(), PublishRequest{Seq: 1, Stage: stage.Stats, ShardDigest: "shard", InputDigest: "i", ConfigVersion: "c", PipelineID: "p", Payload: artifactPayload("stats")})
	if !apperr.IsCode(err, apperr.CodePrereqMissing) {
		t.Fatalf("missing prerequisite error = %v, want E_PREREQ_MISSING", err)
	}
	business := apperr.As(err)
	if business.Predecessor != stage.Tokenize {
		t.Fatalf("predecessor = %q, want tokenize", business.Predecessor)
	}
}

func TestPublishRollbackOnLateWriteFailure(t *testing.T) {
	store, _ := testStore(t)
	publish(t, store, 1, stage.Tokenize, "p", "shard", artifactPayload("tokenize"))
	if _, err := store.db.Exec(`CREATE TRIGGER reject_stats BEFORE INSERT ON checkpoints WHEN NEW.stage='stats' BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END;`); err != nil {
		t.Fatal(err)
	}
	_, err := store.Publish(context.Background(), PublishRequest{Seq: 2, Stage: stage.Stats, ShardDigest: "shard", InputDigest: "i", ConfigVersion: "c", PipelineID: "p", Payload: artifactPayload("stats")})
	if err == nil {
		t.Fatal("expected injected checkpoint insert failure")
	}
	current, _ := store.CurrentSeq(context.Background())
	if current != 1 {
		t.Fatalf("current seq after rollback = %d, want 1", current)
	}
	var artifacts int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM artifacts`).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 {
		t.Fatalf("artifacts after rollback = %d, want 1 (stats artifact must be rolled back)", artifacts)
	}
	active, err := store.Active(context.Background(), stage.Tokenize, "shard")
	if err != nil || active == nil || active.Record.Seq != 1 {
		t.Fatalf("tokenize active row was lost after rollback: %+v err=%v", active, err)
	}
}

func TestConcurrentPublishWithSeparateProcesses(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "concurrency.db")
	lock := filepath.Join(dir, "concurrency.lock")
	payloadPath := filepath.Join(dir, "artifact.json")
	if err := os.WriteFile(payloadPath, []byte(`{"concurrent":true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	const workers = 5
	var wg sync.WaitGroup
	outputs := make([]string, workers)
	errors := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			resultPath := filepath.Join(dir, fmt.Sprintf("result-%d.json", index))
			cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointHelperProcess$")
			cmd.Env = append(os.Environ(),
				"SEARCHLAB_CHECKPOINT_HELPER=1",
				"SEARCHLAB_HELPER_DB="+db,
				"SEARCHLAB_HELPER_LOCK="+lock,
				"SEARCHLAB_HELPER_PAYLOAD="+payloadPath,
				"SEARCHLAB_HELPER_RESULT="+resultPath,
			)
			outputs[index] = resultPath
			combined, err := cmd.CombinedOutput()
			if err != nil {
				errors[index] = fmt.Errorf("%v: %s", err, string(combined))
			}
		}(i)
	}
	wg.Wait()
	successes, stale := 0, 0
	for i := 0; i < workers; i++ {
		if errors[i] != nil {
			t.Fatalf("helper process %d failed: %v", i, errors[i])
		}
		raw, err := os.ReadFile(outputs[i])
		if err != nil {
			t.Fatalf("read helper result %d: %v", i, err)
		}
		var result struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatalf("parse helper result %d: %v", i, err)
		}
		switch result.Code {
		case "OK":
			successes++
		case apperr.CodeStaleCheckpoint:
			stale++
		default:
			t.Fatalf("helper %d returned %q", i, result.Code)
		}
	}
	if successes != 1 || stale != workers-1 {
		t.Fatalf("concurrent publish successes=%d stale=%d, want 1/%d", successes, stale, workers-1)
	}
}

func TestCheckpointHelperProcess(t *testing.T) {
	if os.Getenv("SEARCHLAB_CHECKPOINT_HELPER") != "1" {
		return
	}
	code := "OK"
	store, err := Open(os.Getenv("SEARCHLAB_HELPER_DB"), os.Getenv("SEARCHLAB_HELPER_LOCK"))
	if err != nil {
		code = apperr.As(err).Code
	} else {
		payload, readErr := os.ReadFile(os.Getenv("SEARCHLAB_HELPER_PAYLOAD"))
		if readErr != nil {
			code = apperr.CodeInput
		} else {
			_, err = store.Publish(context.Background(), PublishRequest{
				Seq: 1, Stage: stage.Tokenize, ShardDigest: "concurrency", InputDigest: "concurrency",
				ConfigVersion: "config", PipelineID: "concurrent", Payload: payload,
			})
			if err != nil {
				code = apperr.As(err).Code
			}
		}
		store.Close()
	}
	result := []byte(`{"code":"` + strings.ReplaceAll(code, `"`, `\"`) + `"}`)
	if err := os.WriteFile(os.Getenv("SEARCHLAB_HELPER_RESULT"), result, 0o644); err != nil {
		t.Fatal(err)
	}
}
