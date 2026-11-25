package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"profilehotspot/internal/apperr"
	"profilehotspot/internal/engine"
	"profilehotspot/internal/generator"
	"profilehotspot/internal/model"
	"profilehotspot/internal/storage"
)

func fixedClock() time.Time { return time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC) }

func openTestService(t *testing.T, path string) (*Service, *storage.Store) {
	t.Helper()
	store, err := storage.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc := New(store)
	svc.SetClock(fixedClock)
	t.Cleanup(func() {
		_ = svc.Close()
	})
	return svc, store
}

func tempDB(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "profile.db")
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	return apperr.From(err).Code
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if got := errorCode(err); got != code {
		t.Fatalf("error code = %q, want %q (err=%v)", got, code, err)
	}
}

func mustCommitPolicy(t *testing.T, svc *Service, policy model.Policy) *PolicyCommitResponse {
	t.Helper()
	response, err := svc.CommitPolicy(context.Background(), policy)
	if err != nil {
		t.Fatalf("commit policy: %v", err)
	}
	return response
}

func manualSample(id, service, channel string, timestamp, weight int64, frames ...string) model.SampleInput {
	out := make([]model.Frame, 0, len(frames))
	for _, frame := range frames {
		out = append(out, model.Frame{ID: frame, Name: frame})
	}
	return model.SampleInput{
		ID:          id,
		TimestampMS: timestamp,
		Service:     service,
		Frames:      out,
		Weight:      weight,
		Channel:     channel,
	}
}

func ingestChunk(t *testing.T, svc *Service, batchID string, policyVersion uint64, samples []model.SampleInput) *IngestResponse {
	t.Helper()
	response, err := svc.Ingest(context.Background(), model.SampleChunk{
		BatchID:       batchID,
		PolicyVersion: &policyVersion,
		Samples:       samples,
	})
	if err != nil {
		t.Fatalf("ingest %s: %v", batchID, err)
	}
	return response
}

func findServiceHotspot(t *testing.T, report engine.HotspotReport, service string) engine.ServiceHotspot {
	t.Helper()
	for _, item := range report.Services {
		if item.Service == service {
			return item
		}
	}
	t.Fatalf("service %s not found in hotspot report", service)
	return engine.ServiceHotspot{}
}

func TestValidationFailureLeavesNoState(t *testing.T) {
	svc, store := openTestService(t, tempDB(t))
	policy := generator.DemoPolicy(1)
	mustCommitPolicy(t, svc, policy)
	before, err := store.CurrentState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	invalid := model.SampleChunk{
		BatchID:       "invalid-weight",
		PolicyVersion: &before.CurrentPolicyVersion,
		Samples: []model.SampleInput{
			manualSample("bad", "checkout", "cpu", 1, 0, "root"),
		},
	}
	_, err = svc.Ingest(context.Background(), invalid)
	requireCode(t, err, apperr.CodeSampleInvalid)
	after, err := store.CurrentState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("state changed after invalid ingest: before=%+v after=%+v", before, after)
	}
	_, found, err := store.BatchByID(context.Background(), "invalid-weight")
	if err != nil || found {
		t.Fatalf("invalid batch was persisted: found=%v err=%v", found, err)
	}

	badPolicy := generator.DemoPolicy(2)
	badPolicy.Channels["cpu"] = model.ChannelPolicy{TargetPermille: 0, BiasCorrection: 1}
	_, err = svc.CommitPolicy(context.Background(), badPolicy)
	requireCode(t, err, apperr.CodeSampleInvalid)
	afterPolicyError, _ := store.CurrentState(context.Background())
	if afterPolicyError != before {
		t.Fatalf("invalid policy changed state: %+v", afterPolicyError)
	}
}

func TestDeduplicationConflictAndBoundedReservoir(t *testing.T) {
	svc, store := openTestService(t, tempDB(t))
	policy := generator.DemoPolicy(1)
	policy.Services["checkout"] = model.ServicePolicy{ReservoirCapacity: 2, ExactLimit: 0, BiasCorrection: 1}
	policy.Stratification = model.Stratification{DepthPrefixes: []int{1}, Separator: ">"}
	mustCommitPolicy(t, svc, policy)

	samples := make([]model.SampleInput, 0, 5)
	for i := 0; i < 5; i++ {
		samples = append(samples, manualSample(fmt.Sprintf("s%d", i), "checkout", "wall", int64(i+1), int64(i+2), "root", "leaf"))
	}
	response := ingestChunk(t, svc, "b1", 1, samples)
	if response.NewSamples != 5 || response.Modes.ApproximateServices != 1 {
		t.Fatalf("unexpected initial response: %+v", response)
	}
	reservoirSnapshot, err := svc.Reservoirs(context.Background(), 1, nil, "checkout", "wall")
	if err != nil {
		t.Fatal(err)
	}
	if len(reservoirSnapshot.Reservoirs) != 1 {
		t.Fatalf("reservoirs = %d", len(reservoirSnapshot.Reservoirs))
	}
	reservoir := reservoirSnapshot.Reservoirs[0]
	if reservoir.SeenCount != 5 || reservoir.SelectedCount != 2 || reservoir.Capacity != 2 {
		t.Fatalf("unexpected reservoir bounds: %+v", reservoir)
	}
	if reservoir.ReplacedTotal+reservoir.RejectedTotal != 3 {
		t.Fatalf("replacement accounting: %+v", reservoir)
	}

	duplicate := samples[0]
	duplicate.Channel = "cpu"
	response = ingestChunk(t, svc, "b2", 1, []model.SampleInput{duplicate})
	if response.DuplicateExistingSamples != 1 || response.DuplicateObservations != 1 || response.NewSamples != 0 {
		t.Fatalf("duplicate accounting response: %+v", response)
	}
	var duplicateCount, receivedCount int64
	if err := store.DB().QueryRow(`SELECT duplicate_count, received_count FROM samples WHERE sample_id='s0'`).Scan(&duplicateCount, &receivedCount); err != nil {
		t.Fatal(err)
	}
	if duplicateCount != 1 || receivedCount != 2 {
		t.Fatalf("persisted duplicate counters = %d/%d", duplicateCount, receivedCount)
	}
	hotspots, err := svc.Hotspots(context.Background(), 1, nil, "checkout", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := findServiceHotspot(t, hotspots.Report, "checkout").DuplicateObservations; got != 1 {
		t.Fatalf("hotspot duplicate observations = %d", got)
	}

	beforeConflict, _ := store.CurrentState(context.Background())
	conflict := samples[0]
	conflict.Weight = 999
	newSample := manualSample("should-not-exist", "checkout", "wall", 77, 2, "root", "leaf")
	_, err = svc.Ingest(context.Background(), model.SampleChunk{
		BatchID:       "b3",
		PolicyVersion: &policy.Version,
		Samples:       []model.SampleInput{conflict, newSample},
	})
	requireCode(t, err, apperr.CodeSampleConflict)
	afterConflict, _ := store.CurrentState(context.Background())
	if afterConflict != beforeConflict {
		t.Fatalf("conflicting batch changed commit state: before=%+v after=%+v", beforeConflict, afterConflict)
	}
	var count int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM samples WHERE sample_id='should-not-exist'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("conflicting batch partially inserted a new sample")
	}

	withinBatch := manualSample("within", "checkout", "wall", 88, 5, "root", "leaf")
	withinBatchCopy := withinBatch
	withinBatchCopy.Channel = "cpu"
	response = ingestChunk(t, svc, "b4", 1, []model.SampleInput{withinBatch, withinBatchCopy})
	if response.AcceptedUniqueSamples != 1 || response.DuplicateObservations != 1 {
		t.Fatalf("within-batch duplicate accounting: %+v", response)
	}
}

func TestAdaptiveExactToApproximateWithAuditableBounds(t *testing.T) {
	svc, _ := openTestService(t, tempDB(t))
	policy := generator.DemoPolicy(1)
	mustCommitPolicy(t, svc, policy)
	version := uint64(1)
	small := generator.Chunk(policy, generator.ChunkOptions{Seed: 7, BatchID: "small", PolicyVersion: &version, Count: 3, Services: []string{"checkout"}})
	if _, err := svc.Ingest(context.Background(), small); err != nil {
		t.Fatal(err)
	}
	hotspots, err := svc.Hotspots(context.Background(), 1, nil, "checkout", 0)
	if err != nil {
		t.Fatal(err)
	}
	exact := findServiceHotspot(t, hotspots.Report, "checkout")
	if exact.Path.Mode != engine.ModeExact || exact.BiasUpperBound != 0 {
		t.Fatalf("small service did not use exact path: %+v", exact.Path)
	}

	large := generator.Chunk(policy, generator.ChunkOptions{Seed: 7, BatchID: "large", PolicyVersion: &version, Count: 40, StartIndex: 3, Services: []string{"checkout"}, DuplicateEvery: 11})
	response, err := svc.Ingest(context.Background(), large)
	if err != nil {
		t.Fatal(err)
	}
	if response.ModeByService["checkout"] != engine.ModeApproximate {
		t.Fatalf("large service mode = %q", response.ModeByService["checkout"])
	}
	hotspots, err = svc.Hotspots(context.Background(), 1, nil, "checkout", 0)
	if err != nil {
		t.Fatal(err)
	}
	approx := findServiceHotspot(t, hotspots.Report, "checkout")
	if approx.Path.Mode != engine.ModeApproximate || !approx.Path.UsedReservoir || approx.ApproximationBasis == nil {
		t.Fatalf("approximate path not explicitly labelled: %+v", approx.Path)
	}
	if approx.BiasUpperBound <= 0 || approx.BiasUpperBound > 1 || len(approx.Strata) == 0 || len(approx.Channels) == 0 {
		t.Fatalf("missing approximation metadata: %+v", approx)
	}
	previousGroup := -1
	previousID := ""
	for _, frame := range approx.Frames {
		if frame.AbsoluteError > frame.BiasUpperBound+1e-12 || !frame.WithinBiasBound {
			t.Fatalf("frame %s violates auditable bound: %+v", frame.FrameID, frame)
		}
		if frame.ExactShare < frame.ConfidenceInterval.Lower-1e-12 || frame.ExactShare > frame.ConfidenceInterval.Upper+1e-12 {
			t.Fatalf("frame %s exact share outside confidence interval", frame.FrameID)
		}
		if frame.TieGroup < previousGroup {
			t.Fatal("tie groups are not ordered")
		}
		if frame.TieGroup == previousGroup && frame.FrameID < previousID {
			t.Fatal("frame ids are not stably ordered inside a tie group")
		}
		previousGroup = frame.TieGroup
		previousID = frame.FrameID
	}
	differences, err := svc.Differences(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(differences.Services) != 1 || differences.Services[0].Mode != engine.ModeApproximate || len(differences.Services[0].Frames) == 0 {
		t.Fatalf("unexpected difference report: %+v", differences)
	}
}

func TestPolicyCascadeRebuildAndStaleCodes(t *testing.T) {
	svc, _ := openTestService(t, tempDB(t))
	policy := generator.DemoPolicy(1)
	first := mustCommitPolicy(t, svc, policy)
	version := uint64(1)
	chunk := generator.Chunk(policy, generator.ChunkOptions{Seed: 11, BatchID: "cascade-1", PolicyVersion: &version, Count: 6, Services: []string{"checkout"}})
	if _, err := svc.Ingest(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	oldHotspot, err := svc.Hotspots(context.Background(), 1, nil, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	capacity := 5
	exactLimit := 2
	increment := model.PolicyIncrement{
		BaseVersion: 1,
		Services: map[string]model.ServicePatch{
			"checkout": {ReservoirCapacity: &capacity, ExactLimit: &exactLimit},
		},
	}
	second, err := svc.CommitIncrement(context.Background(), increment)
	if err != nil {
		t.Fatalf("increment: %v", err)
	}
	if second.PolicyVersion != 2 || !second.Rebuild.Atomic || !second.Rebuild.RebuiltHotspot {
		t.Fatalf("unexpected rebuild response: %+v", second)
	}
	if second.CommitSeq <= oldHotspot.CommitSeq {
		t.Fatal("policy rebuild did not advance commit sequence")
	}
	newReservoirs, err := svc.Reservoirs(context.Background(), 2, nil, "checkout", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(newReservoirs.Reservoirs) == 0 {
		t.Fatal("new version has no rebuilt reservoirs")
	}
	for _, reservoir := range newReservoirs.Reservoirs {
		if reservoir.Capacity != 5 || reservoir.References.PolicyVersion != 2 {
			t.Fatalf("reservoir not rebuilt under new policy: %+v", reservoir)
		}
	}
	newHotspot, err := svc.Hotspots(context.Background(), 2, nil, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if newHotspot.CommitSeq != newReservoirs.CommitSeq || newHotspot.Report.CommitSeq != newHotspot.CommitSeq {
		t.Fatalf("hotspot/reservoir commit boundary mismatch: %+v vs %+v", newHotspot, newReservoirs)
	}
	oldAgain, err := svc.Hotspots(context.Background(), 1, nil, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if oldAgain.CommitSeq != oldHotspot.CommitSeq {
		t.Fatal("historical policy snapshot was mutated by cascade rebuild")
	}

	beforeStale, _ := svc.CurrentState(context.Background())
	_, err = svc.CommitIncrement(context.Background(), increment)
	requireCode(t, err, apperr.CodeStaleBase)
	afterStale, _ := svc.CurrentState(context.Background())
	if afterStale != beforeStale {
		t.Fatal("stale increment changed committed state")
	}
	fullStale := generator.DemoPolicy(2)
	_, err = svc.CommitPolicy(context.Background(), fullStale)
	requireCode(t, err, apperr.CodeStaleSnapshot)

	stratification := model.Stratification{DepthPrefixes: []int{1}, Separator: ">"}
	third, err := svc.CommitIncrement(context.Background(), model.PolicyIncrement{
		BaseVersion:    2,
		Stratification: &stratification,
	})
	if err != nil {
		t.Fatalf("stratification increment: %v", err)
	}
	if third.PolicyVersion != 3 || !third.Rebuild.Changes.StratificationChanged {
		t.Fatalf("stratification cascade missing: %+v", third)
	}
	thirdReservoirs, err := svc.Reservoirs(context.Background(), 3, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(thirdReservoirs.Reservoirs) == 0 {
		t.Fatal("stratification rebuild produced no reservoirs")
	}
	for _, reservoir := range thirdReservoirs.Reservoirs {
		if len(reservoir.DepthPrefixes) != 1 || reservoir.DepthPrefixes[0] != 1 {
			t.Fatalf("reservoir references were not replaced: %+v", reservoir.DepthPrefixes)
		}
		if reservoir.References.StratificationFingerprint == "" || reservoir.References.HotspotPolicyFingerprint == "" {
			t.Fatal("reservoir is missing aggregate references")
		}
	}
	if first.PolicyVersion != 1 {
		t.Fatal("initial policy response is inconsistent")
	}
}

func TestConcurrentIncrementsOnSameBaseOnlyOneWins(t *testing.T) {
	svc, store := openTestService(t, tempDB(t))
	mustCommitPolicy(t, svc, generator.DemoPolicy(1))
	capacityA, capacityB := 5, 6
	increments := []model.PolicyIncrement{
		{BaseVersion: 1, Services: map[string]model.ServicePatch{"checkout": {ReservoirCapacity: &capacityA}}},
		{BaseVersion: 1, Services: map[string]model.ServicePatch{"checkout": {ReservoirCapacity: &capacityB}}},
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, increment := range increments {
		increment := increment
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.CommitIncrement(context.Background(), increment)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	successes, stale := 0, 0
	for err := range results {
		switch errorCode(err) {
		case "":
			successes++
		case apperr.CodeStaleBase:
			stale++
		default:
			t.Fatalf("unexpected concurrent increment error: %v", err)
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("concurrent increments successes=%d stale=%d", successes, stale)
	}
	state, _ := store.CurrentState(context.Background())
	if state.CurrentPolicyVersion != 2 || state.CommitSeq != 2 {
		t.Fatalf("unexpected final state: %+v", state)
	}
	var policyCount int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM policy_snapshots`).Scan(&policyCount); err != nil {
		t.Fatal(err)
	}
	if policyCount != 2 {
		t.Fatalf("policy snapshot count = %d, want 2", policyCount)
	}
}

func TestBatchIdempotencyAndConflictingReuse(t *testing.T) {
	svc, store := openTestService(t, tempDB(t))
	mustCommitPolicy(t, svc, generator.DemoPolicy(1))
	version := uint64(1)
	chunk := generator.Chunk(generator.DemoPolicy(1), generator.ChunkOptions{Seed: 9, BatchID: "same", PolicyVersion: &version, Count: 5, Services: []string{"checkout"}})
	first, err := svc.Ingest(context.Background(), chunk)
	if err != nil {
		t.Fatal(err)
	}
	var firstReceived int64
	if err := store.DB().QueryRow(`SELECT SUM(received_count) FROM samples`).Scan(&firstReceived); err != nil {
		t.Fatal(err)
	}
	second, err := svc.Ingest(context.Background(), chunk)
	if err != nil {
		t.Fatal(err)
	}
	if !second.IdempotentReplay || second.CommitSeq != first.CommitSeq {
		t.Fatalf("batch replay was not idempotent: first=%+v second=%+v", first, second)
	}
	var secondReceived int64
	if err := store.DB().QueryRow(`SELECT SUM(received_count) FROM samples`).Scan(&secondReceived); err != nil {
		t.Fatal(err)
	}
	if secondReceived != firstReceived {
		t.Fatal("idempotent replay changed duplicate counters")
	}
	before, _ := store.CurrentState(context.Background())
	conflicting := chunk
	conflicting.Samples = append([]model.SampleInput(nil), chunk.Samples...)
	conflicting.Samples[0].Weight++
	_, err = svc.Ingest(context.Background(), conflicting)
	requireCode(t, err, apperr.CodeConflictingRequest)
	after, _ := store.CurrentState(context.Background())
	if before != after {
		t.Fatal("batch id reuse with different payload changed state")
	}
}

func TestConcurrentReadersSeeCompleteCommittedSnapshots(t *testing.T) {
	svc, _ := openTestService(t, tempDB(t))
	policy := generator.DemoPolicy(1)
	mustCommitPolicy(t, svc, policy)
	version := uint64(1)
	chunk := generator.Chunk(policy, generator.ChunkOptions{Seed: 13, BatchID: "reader-base", PolicyVersion: &version, Count: 40, DuplicateEvery: 7})
	if _, err := svc.Ingest(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 15; j++ {
				snapshot, err := svc.VersionSnapshot(context.Background(), 1, nil, "", "", 0)
				if err != nil {
					errs <- err
					return
				}
				if snapshot.CommitSeq != snapshot.Hotspots.CommitSeq {
					errs <- fmt.Errorf("mixed snapshot boundary: envelope=%d report=%d", snapshot.CommitSeq, snapshot.Hotspots.CommitSeq)
					return
				}
				for _, reservoir := range snapshot.Reservoirs {
					if reservoir.PolicyVersion != snapshot.PolicyVersion {
						errs <- fmt.Errorf("reservoir version %d in snapshot %d", reservoir.PolicyVersion, snapshot.PolicyVersion)
						return
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		next := generator.Chunk(policy, generator.ChunkOptions{Seed: 17, BatchID: "reader-write", PolicyVersion: &version, Count: 20, StartIndex: 100})
		_, _ = svc.Ingest(context.Background(), next)
		capacity := 7
		_, _ = svc.CommitIncrement(context.Background(), model.PolicyIncrement{BaseVersion: 1, Services: map[string]model.ServicePatch{"checkout": {ReservoirCapacity: &capacity}}})
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestForcedTerminationRecovery(t *testing.T) {
	if os.Getenv("PROFILE_CRASH_HELPER") == "1" {
		path := os.Getenv("PROFILE_CRASH_DB")
		store, err := storage.Open(context.Background(), path)
		if err != nil {
			t.Fatalf("helper open: %v", err)
		}
		svc := New(store)
		version := uint64(1)
		chunk := generator.Chunk(generator.DemoPolicy(1), generator.ChunkOptions{Seed: 99, BatchID: "crash-batch", PolicyVersion: &version, Count: 8, Services: []string{"checkout"}})
		if _, err := svc.Ingest(context.Background(), chunk); err == nil {
			t.Fatal("crash helper returned instead of being terminated")
		}
		return
	}

	for _, stage := range []string{"during_reservoir_replace", "during_hotspot_aggregation"} {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			runForcedTerminationScenario(t, stage)
		})
	}
}

func runForcedTerminationScenario(t *testing.T, stage string) {
	t.Helper()
	path := tempDB(t)
	store, err := storage.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	svc := New(store)
	svc.SetClock(fixedClock)
	mustCommitPolicy(t, svc, generator.DemoPolicy(1))
	version := uint64(1)
	stable := generator.Chunk(generator.DemoPolicy(1), generator.ChunkOptions{Seed: 3, BatchID: "stable", PolicyVersion: &version, Count: 4, Services: []string{"checkout"}})
	if _, err := svc.Ingest(context.Background(), stable); err != nil {
		t.Fatal(err)
	}
	before, err := store.CurrentState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	beforeSnapshot, err := svc.VersionSnapshot(context.Background(), 1, nil, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestForcedTerminationRecovery$")
	command.Env = append(os.Environ(),
		"PROFILE_CRASH_HELPER=1",
		"PROFILE_CRASH_STAGE="+stage,
		"PROFILE_CRASH_DB="+path,
	)
	output, err := command.CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 137 {
		t.Fatalf("crash helper did not terminate with 137: err=%v output=%s", err, output)
	}

	reopened, err := storage.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen after forced termination: %v", err)
	}
	defer reopened.Close()
	recoveredService := New(reopened)
	recoveredService.SetClock(fixedClock)
	after, err := reopened.CurrentState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("recovery did not roll back to last commit: before=%+v after=%+v", before, after)
	}
	afterSnapshot, err := recoveredService.VersionSnapshot(context.Background(), 1, nil, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeSnapshot.Reservoirs, afterSnapshot.Reservoirs) || !reflect.DeepEqual(beforeSnapshot.Hotspots, afterSnapshot.Hotspots) {
		t.Fatal("recovery did not restore the last complete reservoir/hotspot state")
	}
	report, err := reopened.LatestRecoveryReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Complete || report.ScannedIntents < 1 || report.RolledBackIntents < 1 || report.LastCompleteCommitSeq != before.CommitSeq {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	if _, found, err := reopened.BatchByID(context.Background(), "crash-batch"); err != nil || found {
		t.Fatalf("uncommitted batch visible after recovery: found=%v err=%v", found, err)
	}

	retry := generator.Chunk(generator.DemoPolicy(1), generator.ChunkOptions{Seed: 99, BatchID: "crash-batch", PolicyVersion: &version, Count: 8, Services: []string{"checkout"}})
	retryResponse, err := recoveredService.Ingest(context.Background(), retry)
	if err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	if retryResponse.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("retry commit seq = %d, want %d", retryResponse.CommitSeq, before.CommitSeq+1)
	}
}
