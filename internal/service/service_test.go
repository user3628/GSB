package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"upstreampool/internal/config"
	"upstreampool/internal/model"
	"upstreampool/internal/service"
	"upstreampool/internal/sim"
	"upstreampool/internal/store"
	"upstreampool/internal/upstream"
)

func TestBatchSegmentationIdempotencyAndConflicts(t *testing.T) {
	up, cfg, st, svc := newTestService(t)
	defer up.Close()
	defer st.Close()
	defer svc.Close()

	full := loadSubmit(t, "../../samples/track-full.json")
	full.TrackID = "it-full"
	fullResp, err := svc.Submit(context.Background(), full)
	if err != nil {
		t.Fatal(err)
	}
	seg1 := loadSubmit(t, "../../samples/track-segment-1.json")
	seg1.TrackID = "it-seg"
	if _, err := svc.Submit(context.Background(), seg1); err != nil {
		t.Fatal(err)
	}
	seg2 := loadSubmit(t, "../../samples/track-segment-2.json")
	seg2.TrackID = "it-seg"
	segResp, err := svc.Submit(context.Background(), seg2)
	if err != nil {
		t.Fatal(err)
	}
	stripSnapshot(fullResp.Snapshot)
	stripSnapshot(segResp.Snapshot)
	if !snapshotsEqual(fullResp.Snapshot, segResp.Snapshot) {
		left, _ := json.Marshal(fullResp.Snapshot)
		right, _ := json.Marshal(segResp.Snapshot)
		t.Fatalf("whole/segmented snapshots differ\n%s\n%s", left, right)
	}
	for _, at := range []int64{0, 300, 401, 500, 602} {
		left, _, _, err := svc.Snapshot(context.Background(), "it-full", &at)
		if err != nil {
			t.Fatal(err)
		}
		right, _, _, err := svc.Snapshot(context.Background(), "it-seg", &at)
		if err != nil {
			t.Fatal(err)
		}
		stripSnapshot(left)
		stripSnapshot(right)
		if !snapshotsEqual(left, right) {
			t.Fatalf("snapshot at %d differs", at)
		}
	}
	dup, err := svc.Submit(context.Background(), full)
	if err != nil {
		t.Fatal(err)
	}
	if dup.Advanced {
		t.Fatal("duplicate batch must not advance")
	}
	conflict := full
	conflict.BatchID = full.BatchID
	conflict.Requests = append([]model.RequestInput(nil), full.Requests...)
	conflict.Requests[0].RequestBytes++
	_, err = svc.Submit(context.Background(), conflict)
	var be *model.BusinessError
	if !errors.As(err, &be) || be.Body.Code != model.ErrConflictingBatch {
		t.Fatalf("expected E_CONFLICTING_BATCH, got %v", err)
	}
	outcomes := make(map[string]service.Outcome)
	for _, outcome := range fullResp.Requests {
		outcomes[outcome.RequestID] = outcome
	}
	if outcomes["j2"].Error == nil || outcomes["j2"].Error.Code != model.ErrPoolExhausted {
		t.Fatalf("j2 outcome: %+v", outcomes["j2"])
	}
	tr, err := svc.Trace(context.Background(), "it-full", "edge-a-c000001")
	if err != nil {
		t.Fatal(err)
	}
	if len(tr) < 9 || tr[5].From != "IDLE" || tr[5].To != "HANDSHAKING" || tr[6].To != "SEND_BODY" {
		t.Fatalf("unexpected trace: %+v", tr)
	}
	_ = cfg
}

func TestDialFailureIsReturned(t *testing.T) {
	up, cfg, st, svc := newTestService(t)
	defer up.Close()
	defer st.Close()
	defer svc.Close()
	refusedPort := unusedPort(t)
	for i := range cfg.Endpoints {
		if cfg.Endpoints[i].ID == "edge-refused" {
			cfg.Endpoints[i].Address = refusedPort
		}
	}
	cfg.DBPath = filepath.Join(t.TempDir(), "dial.db")
	st2, svc2 := newServiceWithConfig(t, cfg)
	defer st2.Close()
	defer svc2.Close()
	req := loadSubmit(t, "../../samples/track-dial-failure.json")
	resp, err := svc2.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Requests) != 1 || resp.Requests[0].Error == nil || resp.Requests[0].Error.Code != model.ErrDialFailed {
		t.Fatalf("expected E_DIAL_FAILED, got %+v", resp.Requests)
	}
}

func TestSpecInvalidDoesNotIngestBatch(t *testing.T) {
	up, _, st, svc := newTestService(t)
	defer up.Close()
	defer st.Close()
	defer svc.Close()
	ctx := context.Background()
	in := model.SubmitRequest{TrackID: "it-invalid", BatchID: "invalid-batch", Requests: []model.RequestInput{{
		RequestID: "bad", TimestampMS: 0, ClientID: "c", EndpointID: "missing", RequestBytes: 1, UploadRateBPS: 1, ResponseBytes: 1,
	}}}
	_, err := svc.Submit(ctx, in)
	var be *model.BusinessError
	if !errors.As(err, &be) || be.Body.Code != model.ErrSpecInvalid {
		t.Fatalf("expected E_SPEC_INVALID, got %v", err)
	}
	batch, err := st.FindBatch(ctx, "it-invalid", "invalid-batch")
	if err != nil {
		t.Fatal(err)
	}
	if batch != nil {
		t.Fatalf("invalid batch must not be persisted: %+v", batch)
	}
}

func TestRecoveryAbortsIntentAndResumesBatch(t *testing.T) {
	up, cfg, st, _ := newTestService(t)
	defer up.Close()
	defer st.Close()
	ctx := context.Background()
	trackID := "it-recovery"
	batchID := "batch-recovery"
	digest, err := store.NormalizeBatchDigest(trackID, batchID, []model.RequestInput{{RequestID: "rec-1", TimestampMS: 0, ClientID: "c", EndpointID: "edge-a", RequestBytes: 10, UploadRateBPS: 10000, ResponseBytes: 10}})
	if err != nil {
		t.Fatal(err)
	}
	ingestIntent, err := st.CreateIntent(ctx, trackID, "INGEST_BATCH", map[string]any{"batch": batchID})
	if err != nil {
		t.Fatal(err)
	}
	rows := []model.Request{{TrackID: trackID, BatchID: batchID, RequestKey: "rec-1", RequestID: "rec-1", TimestampMS: 0, ClientID: "c", EndpointID: "edge-a", RequestBytes: 10, UploadRateBPS: 10000, ResponseBytes: 10}}
	if err := st.InsertBatch(ctx, trackID, batchID, digest, rows, ingestIntent); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateIntent(ctx, trackID, "APPLY_PLAN", map[string]any{"simulated_crash": true}); err != nil {
		t.Fatal(err)
	}

	st2, svc2 := newServiceWithConfig(t, cfg)
	defer st2.Close()
	defer svc2.Close()
	report, err := svc2.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.AbortedIntents) != 1 || len(report.ResumedBatches) != 1 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	snap, _, _, err := svc2.Snapshot(ctx, trackID, nil)
	if err != nil || snap == nil {
		t.Fatalf("snapshot after recovery: %v, %v", snap, err)
	}
}

func TestCheckpointCorruptionIsReported(t *testing.T) {
	up, cfg, st, svc := newTestService(t)
	defer up.Close()
	ctx := context.Background()
	req := loadSubmit(t, "../../samples/track-full.json")
	req.TrackID = "it-corrupt"
	if _, err := svc.Submit(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, `UPDATE checkpoints SET state_json='{"corrupt":true}' WHERE track_id='it-corrupt' AND seq=2`); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	st2, svc2 := newServiceWithConfig(t, cfg)
	defer st2.Close()
	defer svc2.Close()
	report, err := svc2.Recover(ctx)
	var be *model.BusinessError
	if !errors.As(err, &be) || be.Body.Code != model.ErrCheckpointCorrupt {
		t.Fatalf("expected E_CHECKPOINT_CORRUPT, got report=%+v err=%v", report, err)
	}
	_, _, _, err = svc2.Snapshot(ctx, "it-corrupt", nil)
	if !errors.As(err, &be) || be.Body.Code != model.ErrCheckpointCorrupt {
		t.Fatalf("expected corrupt snapshot error, got %v", err)
	}
}

func TestConcurrentReadersSeeCompleteSnapshots(t *testing.T) {
	up, _, st, svc := newTestService(t)
	defer up.Close()
	defer st.Close()
	defer svc.Close()
	ctx := context.Background()
	seg1 := loadSubmit(t, "../../samples/track-segment-1.json")
	seg1.TrackID = "it-concurrent"
	if _, err := svc.Submit(ctx, seg1); err != nil {
		t.Fatal(err)
	}
	seg2 := loadSubmit(t, "../../samples/track-segment-2.json")
	seg2.TrackID = "it-concurrent"
	errCh := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 80; j++ {
				at := int64([]int{0, 150, 300, 401, 500, 602}[j%6])
				snap, _, _, err := svc.Snapshot(ctx, "it-concurrent", &at)
				if err != nil {
					errCh <- err
					return
				}
				if err := validateSnapshot(snap); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := svc.Submit(ctx, seg2); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func validateSnapshot(snap *sim.Snapshot) error {
	if snap == nil {
		return errors.New("nil snapshot")
	}
	for _, ep := range snap.Endpoints {
		active := 0
		idle := make(map[string]bool)
		for _, conn := range snap.Connections {
			if conn.EndpointID != ep.EndpointID || conn.Stage == "CLOSED" {
				continue
			}
			active++
			if conn.Stage == "IDLE" {
				idle[conn.ID] = true
			}
		}
		if active != ep.ActiveConnections || ep.AvailableSlots != ep.MaxConnections-active {
			return errors.New("endpoint counters do not match connection states")
		}
		if len(idle) != len(ep.IdleConnections) {
			return errors.New("idle queue does not match IDLE connection states")
		}
		for _, id := range ep.IdleConnections {
			if !idle[id] {
				return errors.New("idle queue references a non-IDLE connection")
			}
		}
	}
	return nil
}

func newTestService(t *testing.T) (*upstream.Server, model.Config, *store.Store, *service.Service) {
	t.Helper()
	up, err := upstream.Start("127.0.0.1:0", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := model.LoadConfig("../../samples/lab.json")
	if err != nil {
		up.Close()
		t.Fatal(err)
	}
	for i := range cfg.Endpoints {
		if cfg.Endpoints[i].Health == "UP" && cfg.Endpoints[i].ID != "edge-refused" {
			cfg.Endpoints[i].Address = up.Addr()
		}
	}
	cfg.DBPath = filepath.Join(t.TempDir(), "workbench.db")
	if err := config.Validate(&cfg); err != nil {
		up.Close()
		t.Fatal(err)
	}
	st, svc := newServiceWithConfig(t, cfg)
	return up, cfg, st, svc
}

func newServiceWithConfig(t *testing.T, cfg model.Config) (*store.Store, *service.Service) {
	t.Helper()
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(context.Background(), st, cfg)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	return st, svc
}

func loadSubmit(t *testing.T, path string) model.SubmitRequest {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out model.SubmitRequest
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func stripSnapshot(s *sim.Snapshot) {
	if s == nil {
		return
	}
	s.TrackID = ""
}

func snapshotsEqual(a, b *sim.Snapshot) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}

func unusedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
