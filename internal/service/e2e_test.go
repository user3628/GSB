package service_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"tm-sync/internal/service"
	"tm-sync/internal/store"
	"tm-sync/internal/tm"
)

func newTestService(t *testing.T) (*service.Service, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "tm.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return service.New(st, tm.DefaultAlignConfig()), dbPath
}

func readDataset(t *testing.T) (*tm.GeneratedDataset, []byte) {
	t.Helper()
	dir := t.TempDir()
	dataset, err := tm.GenerateDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	var source []byte
	for _, file := range dataset.SourceFiles {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		source = append(source, b...)
	}
	return dataset, source
}

func TestEndToEndIncrementalInvalidationAndStableErrors(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	dataset, source := readDataset(t)

	if _, err := svc.ImportSources(ctx, source, 1); err != nil {
		t.Fatal(err)
	}
	sync1, err := svc.Sync(ctx, service.SyncRequest{Mode: "full"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sync1.FailedSegments) != 2 {
		t.Fatalf("expected two E_SEGMENT_EMPTY reports, got %#v", sync1.FailedSegments)
	}

	_, actual1, err := svc.ExportBundle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, full1, err := svc.FullRebuildExport(ctx)
	if err != nil || string(actual1) != string(full1) {
		t.Fatalf("v1 export mismatch: %v", err)
	}

	inc1, err := os.ReadFile(dataset.IncrementFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	invalid1, err := svc.ApplyIncrement(ctx, inc1)
	if err != nil {
		t.Fatal(err)
	}
	if len(invalid1.InvalidUnits) == 0 || len(invalid1.InvalidSuggestions) == 0 {
		t.Fatalf("expected shared unit/suggestion invalidation: %#v", invalid1)
	}
	wantSegments := map[string]bool{"seg-001": true, "seg-004": true, "seg-005": true}
	for _, id := range invalid1.InvalidSegments {
		delete(wantSegments, id)
	}
	if len(wantSegments) != 0 {
		t.Fatalf("missing invalid peer segments: %v", wantSegments)
	}

	if _, err := svc.Sync(ctx, service.SyncRequest{Mode: "incremental"}); err != nil {
		t.Fatal(err)
	}
	assertExportsMatchFull(t, ctx, svc)

	if _, err := svc.ApplyIncrement(ctx, inc1); err == nil {
		t.Fatal("expected E_STALE_BASE")
	} else if biz, ok := tm.AsBizError(err); !ok || biz.Code != "E_STALE_BASE" || biz.Details["current_version"] != int64(2) {
		t.Fatalf("unexpected stale base error: %#v", err)
	}

	inc2, err := os.ReadFile(dataset.IncrementFiles[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ApplyIncrement(ctx, inc2); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Sync(ctx, service.SyncRequest{Mode: "incremental"}); err != nil {
		t.Fatal(err)
	}
	assertExportsMatchFull(t, ctx, svc)

	search, err := svc.SearchSuggestions(ctx, "提交", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(search.Active) == 0 {
		t.Fatal("expected active suggestion")
	}
	var foundCommit bool
	for _, sug := range search.Active {
		if sug.SourceText == "请立即提交。" && sug.TargetText == "Commit." && sug.OccurrenceCount == 3 {
			foundCommit = true
		}
	}
	if !foundCommit {
		t.Fatalf("incremental winner not found: %#v", search.Active)
	}
	if len(search.Invalid) == 0 || len(search.Invalid[0].ReasonChain) < 2 {
		t.Fatalf("expected invalid suggestion reason chain: %#v", search.Invalid)
	}

	pub, err := svc.Publish(ctx, service.PublishRequest{SegmentID: "seg-001", Seq: 1, Kind: "unit"})
	if err != nil || pub.CurrentSeq != 1 {
		t.Fatalf("publish seq1 failed: %#v %v", pub, err)
	}
	if _, err := svc.Publish(ctx, service.PublishRequest{SegmentID: "seg-001", Seq: 1}); err == nil {
		t.Fatal("expected duplicate E_STALE_CHECKPOINT")
	} else if biz, ok := tm.AsBizError(err); !ok || biz.Code != "E_STALE_CHECKPOINT" || biz.Details["current_seq"] != int64(1) {
		t.Fatalf("unexpected checkpoint error: %#v", err)
	}

	if _, err := svc.RecordVersion(ctx, service.VersionRecordRequest{Stream: "test", Version: 0, ContentDigest: "x"}); err == nil {
		t.Fatal("expected rollback rejection")
	} else if biz, ok := tm.AsBizError(err); !ok || biz.Code != "E_VERSION_ROLLBACK" {
		t.Fatalf("unexpected rollback error: %#v", err)
	}

	if _, err := svc.RecordVersion(ctx, service.VersionRecordRequest{Stream: "replace-test", Version: 3, ContentDigest: "digest-a"}); err != nil {
		t.Fatal(err)
	}
	replaced, err := svc.RecordVersion(ctx, service.VersionRecordRequest{Stream: "replace-test", Version: 3, ContentDigest: "digest-b"})
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Classification != "whole_replacement" || replaced.PreviousDigest != "digest-a" {
		t.Fatalf("unexpected replacement classification: %#v", replaced)
	}
	if _, err := svc.Sync(ctx, service.SyncRequest{Mode: "incremental"}); err != nil {
		t.Fatal(err)
	}
	assertExportsMatchFull(t, ctx, svc)

	if _, err := svc.RunStage(ctx, service.StageRequest{SegmentID: "seg-003-empty-target", Stage: "alignment"}); err == nil {
		t.Fatal("expected missing prerequisite")
	} else if biz, ok := tm.AsBizError(err); !ok || biz.Code != "E_MISSING_PREREQUISITE" || biz.SegmentID != "seg-003-empty-target" {
		t.Fatalf("unexpected prerequisite error: %#v", err)
	}
	if _, err := svc.RunStage(ctx, service.StageRequest{SegmentID: "seg-002", Stage: "merge", FromStage: "segmentation"}); err == nil {
		t.Fatal("expected invalid stage transition")
	} else if biz, ok := tm.AsBizError(err); !ok || biz.Code != "E_INVALID_STAGE_TRANSITION" || biz.SegmentID != "seg-002" {
		t.Fatalf("unexpected stage jump error: %#v", err)
	}
}

func assertExportsMatchFull(t *testing.T, ctx context.Context, svc *service.Service) {
	t.Helper()
	_, actual, err := svc.ExportBundle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fullBundle, full, err := svc.FullRebuildExport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	actualBundle, _, err := svc.ExportBundle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tm.CompareExports(fullBundle, actualBundle); err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(full) {
		t.Fatalf("export bytes differ:\nactual=%s\nfull=%s", actual, full)
	}
}
