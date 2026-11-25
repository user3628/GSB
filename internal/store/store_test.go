package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"imretrace/internal/model"
)

func testStore(t *testing.T) (*Store, model.Paths) {
	t.Helper()
	dir := t.TempDir()
	paths := model.DefaultPaths(filepath.Join(dir, "ws"))
	s, err := Open(context.Background(), paths)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, paths
}

func TestConcurrentFirstCheckpointOnlyOneWinner(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	item := model.ManifestItem{ItemID: "item-1", ShardID: "shard-1", ImageFile: "x.png", Caption: "c", Lang: "zh-en", Source: "test"}
	if err := s.UpsertItems(ctx, []model.ManifestItem{item}, map[string]string{"item-1": "hash"}); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, stale := 0, 0
	currentSeen := map[int]int{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.PublishCheckpoint(ctx, PublishInput{
				ShardID: "shard-1", ItemID: "item-1", Stage: model.StageFeature, Seq: 1,
				PolicyVersion: 1, PolicyHash: "p", InputHash: "h", ArtifactDigest: "a", PayloadJSON: "{}",
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
				return
			}
			biz, ok := model.AsBiz(err)
			if !ok || biz.Code != model.CodeStaleCheckpoint {
				t.Errorf("expected stale checkpoint, got %v", err)
				return
			}
			stale++
			currentSeen[biz.CurrentSeq]++
		}()
	}
	wg.Wait()
	if success != 1 || stale != workers-1 {
		t.Fatalf("success=%d stale=%d, want 1/%d", success, stale, workers-1)
	}
	if currentSeen[1] != workers-1 {
		t.Fatalf("all losers should observe current_seq=1, got %v", currentSeen)
	}

	var activeCount, seqCount int
	if err := s.DB().QueryRow(`SELECT count(*) FROM checkpoints WHERE shard_id='shard-1' AND stage=? AND status='active'`, model.StageFeature).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT count(*) FROM checkpoints WHERE shard_id='shard-1' AND seq=1 AND status='active'`, model.StageFeature).Scan(&seqCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 1 || seqCount != 1 {
		t.Fatalf("active stage count=%d seq count=%d, want 1/1", activeCount, seqCount)
	}
}

func TestSequenceMustAdvanceAndCrossShardRejected(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	item := model.ManifestItem{ItemID: "item-2", ShardID: "shard-2", Caption: "c"}
	if err := s.UpsertItems(ctx, []model.ManifestItem{item}, map[string]string{"item-2": "h"}); err != nil {
		t.Fatal(err)
	}
	in := PublishInput{ShardID: "shard-2", ItemID: "item-2", PolicyVersion: 1, PolicyHash: "p", InputHash: "h", ArtifactDigest: "a", PayloadJSON: "{}"}
	for seq := 1; seq <= 4; seq++ {
		in.Seq = seq
		in.Stage = model.StageNames[seq-1]
		if _, err := s.PublishCheckpoint(ctx, in); err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
	}
	in.Seq, in.Stage = 2, model.StageProjectQuant
	_, err := s.PublishCheckpoint(ctx, in)
	if biz, ok := model.AsBiz(err); !ok || biz.Code != model.CodeStaleCheckpoint || biz.CurrentSeq != 4 {
		t.Fatalf("duplicate publish error=%#v", err)
	}

	in.ShardID = "shard-wrong"
	in.Seq, in.Stage = 5, model.StageRecallVerify
	_, err = s.PublishCheckpoint(ctx, in)
	if biz, ok := model.AsBiz(err); !ok || biz.Code != model.CodeStaleCheckpoint || biz.Reason != "cross_shard_mismatch" {
		t.Fatalf("cross-shard error=%#v", err)
	}
}
