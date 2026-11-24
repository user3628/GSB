package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"replicationlag/internal/apperr"
	"replicationlag/internal/compat"
	"replicationlag/internal/domain"
	"replicationlag/internal/samplegen"
	"replicationlag/internal/store"
)

const engineSource = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func TestSampleJSONLImportBinaryRoundTripAndQueries(t *testing.T) {
	ctx := context.Background()
	sampleDir := t.TempDir()
	if err := samplegen.Generate(sampleDir); err != nil {
		t.Fatalf("generate samples: %v", err)
	}
	matrix, err := compat.Load(filepath.Join(sampleDir, "compat.json"))
	if err != nil {
		t.Fatal(err)
	}
	input, err := os.ReadFile(filepath.Join(sampleDir, "observations.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	first := openTestEngine(t, matrix)
	result, err := first.Import(ctx, ImportRequest{BatchID: "seed-jsonl", Data: input})
	if err != nil {
		t.Fatalf("import JSONL: %v", err)
	}
	if !result.Success {
		t.Fatalf("import failed: %s", result.Body)
	}
	payload := decodeBody(t, result.Body)
	if payload["observation_count"].(float64) != 13 {
		t.Fatalf("observation count = %v", payload["observation_count"])
	}
	sequences, version, err := first.ListSequences(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || len(sequences) != 2 {
		t.Fatalf("version=%d sequences=%d", version, len(sequences))
	}
	states := map[string]domain.ReplicaState{}
	for _, sequence := range sequences {
		states[sequence.ReplicaID] = sequence.CurrentState
	}
	if states["replica-a"] != domain.StateHealthy || states["replica-b"] != domain.StateHealthy {
		t.Fatalf("states = %#v", states)
	}

	binaryData, exportedVersion, exportedFormat, err := first.Export(ctx, "binary")
	if err != nil || exportedVersion != 1 || exportedFormat != "binary" {
		t.Fatalf("export binary = %d/%s err=%v", exportedVersion, exportedFormat, err)
	}
	second := openTestEngine(t, matrix)
	binaryResult, err := second.Import(ctx, ImportRequest{BatchID: "seed-binary", Data: binaryData, Format: "binary"})
	if err != nil || !binaryResult.Success {
		t.Fatalf("import binary: %v body=%s", err, resultBody(binaryResult))
	}
	firstObservations, _, err := first.ListObservations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secondObservations, _, err := second.ListObservations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertObservationSetEqual(t, firstObservations, secondObservations)

	jsonlData, _, _, err := first.Export(ctx, "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	third := openTestEngine(t, matrix)
	jsonlResult, err := third.Import(ctx, ImportRequest{BatchID: "seed-jsonl-roundtrip", Data: jsonlData})
	if err != nil || !jsonlResult.Success {
		t.Fatalf("round-trip JSONL: %v body=%s", err, resultBody(jsonlResult))
	}
	thirdObservations, _, _ := third.ListObservations(ctx)
	assertObservationSetEqual(t, firstObservations, thirdObservations)
}

func TestLegacyImportDegrades(t *testing.T) {
	ctx := context.Background()
	sampleDir := t.TempDir()
	if err := samplegen.Generate(sampleDir); err != nil {
		t.Fatal(err)
	}
	matrix, _ := compat.Load(filepath.Join(sampleDir, "compat.json"))
	data, _ := os.ReadFile(filepath.Join(sampleDir, "legacy.jsonl"))
	engineInstance := openTestEngine(t, matrix)
	result, err := engineInstance.Import(ctx, ImportRequest{BatchID: "legacy", Data: data})
	if err != nil || !result.Success {
		t.Fatalf("legacy import: %v body=%s", err, resultBody(result))
	}
	payload := decodeBody(t, result.Body)
	if payload["compatibility"] != "DEGRADE" || len(payload["degradations"].([]any)) < 5 {
		t.Fatalf("compatibility report = %s", result.Body)
	}
	sequence, _, err := engineInstance.GetSequence(ctx, "legacy-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(sequence.Steps) != 2 || sequence.Steps[1].PositionRelation != domain.PositionAdvance {
		t.Fatalf("legacy sequence = %+v", sequence)
	}
	if sequence.Steps[0].Observation.ReportedVersion != "5.7.0" || sequence.Steps[0].Observation.SampledAtMS != 1700100000000 {
		t.Fatalf("legacy normalization = %+v", sequence.Steps[0].Observation)
	}
}

func TestInvalidBatchIsAtomicAndLeavesPriorVersionReadable(t *testing.T) {
	ctx := context.Background()
	engineInstance := openTestEngine(t, compat.Default())
	seed := observationLine(t, "replica-x", 1000, engineSource+":1-5", 10, 1, "2026-01-01T00:00:00Z")
	seedDoc := jsonlDocument(t, seed)
	seedResult, err := engineInstance.Import(ctx, ImportRequest{BatchID: "seed", Data: seedDoc})
	if err != nil || !seedResult.Success {
		t.Fatalf("seed: %v %s", err, resultBody(seedResult))
	}
	before, beforeVersion, _ := engineInstance.ListObservations(ctx)
	invalid := `{"replica_id":"replica-x","sampled_at_ms":2000,"gtid_segments":["` + engineSource + `:1-8"],"position_name":"` + engineSource + `:1-8","lag_ms":-5,"reported_version":"8.0.0"}`
	valid := observationLine(t, "replica-y", 1000, engineSource+":1-3", 20, 1, "2026-01-01T00:00:01Z")
	result, err := engineInstance.Import(ctx, ImportRequest{BatchID: "bad", Data: jsonlDocument(t, valid, invalid)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Success {
		t.Fatal("invalid batch unexpectedly succeeded")
	}
	if result.Code != apperr.CodeObservationInvalid {
		t.Fatalf("code = %s body=%s", result.Code, result.Body)
	}
	after, afterVersion, _ := engineInstance.ListObservations(ctx)
	if afterVersion != beforeVersion {
		t.Fatalf("version changed from %d to %d", beforeVersion, afterVersion)
	}
	assertObservationSetEqual(t, before, after)
	sequence, _, err := engineInstance.GetSequence(ctx, "replica-x")
	if err != nil || len(sequence.Steps) != 1 {
		t.Fatalf("prior evolution unavailable: %v %+v", err, sequence)
	}
}

func TestIdempotentBatchConflictAndStaleBase(t *testing.T) {
	ctx := context.Background()
	engineInstance := openTestEngine(t, compat.Default())
	input := ObservationInput{
		ReplicaID: "replica-x", SampledAtMS: 1000, GTID: engineSource + ":1-5",
		PositionName: engineSource + ":1-5", LagMS: 10, ReportedVersion: "8.0.36",
	}
	first, err := engineInstance.UpsertObservation(ctx, UpsertRequest{BatchID: "same", Observation: input})
	if err != nil || !first.Success || first.Cached {
		t.Fatalf("first submit: %v cached=%v body=%s", err, first.Cached, first.Body)
	}
	second, err := engineInstance.UpsertObservation(ctx, UpsertRequest{BatchID: "same", Observation: input})
	if err != nil || !second.Success || !second.Cached || second.Version != first.Version {
		t.Fatalf("replay: %v cached=%v version=%d body=%s", err, second.Cached, second.Version, second.Body)
	}
	conflicting := input
	conflicting.LagMS = 20
	third, err := engineInstance.UpsertObservation(ctx, UpsertRequest{BatchID: "same", Observation: conflicting})
	if err != nil {
		t.Fatal(err)
	}
	if third.Success || third.Code != apperr.CodeConflictingBatch {
		t.Fatalf("conflicting batch result = success=%v code=%s body=%s", third.Success, third.Code, third.Body)
	}

	zero := int64(0)
	stale, err := engineInstance.UpsertObservation(ctx, UpsertRequest{
		BatchID: "stale", BaseVersion: &zero,
		Observation: ObservationInput{ReplicaID: "replica-y", SampledAtMS: 1000, GTID: engineSource + ":1", PositionName: engineSource + ":1", LagMS: 1, ReportedVersion: "8.0.0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stale.Success || stale.Code != apperr.CodeStaleBase {
		t.Fatalf("stale result success=%v code=%s body=%s", stale.Success, stale.Code, stale.Body)
	}
	observations, version, _ := engineInstance.ListObservations(ctx)
	if version != first.Version || len(observations) != 1 {
		t.Fatalf("stale write modified state: version=%d observations=%d", version, len(observations))
	}
}

func TestConcurrentSameBatchExecutesOnce(t *testing.T) {
	ctx := context.Background()
	engineInstance := openTestEngine(t, compat.Default())
	input := ObservationInput{
		ReplicaID: "replica-x", SampledAtMS: 1000, GTID: engineSource + ":1-5",
		PositionName: engineSource + ":1-5", LagMS: 10, ReportedVersion: "8.0.36",
	}
	const workers = 12
	results := make(chan store.Result, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := engineInstance.UpsertObservation(ctx, UpsertRequest{BatchID: "concurrent-same", Observation: input})
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	executed := 0
	for result := range results {
		if !result.Success || result.Version != 1 {
			t.Fatalf("batch result success=%v version=%d body=%s", result.Success, result.Version, result.Body)
		}
		if !result.Cached {
			executed++
		}
	}
	if executed != 1 {
		t.Fatalf("real executions = %d, want 1", executed)
	}
	observations, version, err := engineInstance.ListObservations(ctx)
	if err != nil || version != 1 || len(observations) != 1 {
		t.Fatalf("final state: version=%d observations=%d err=%v", version, len(observations), err)
	}
}

func TestImportOutcomesOverwriteSkipReject(t *testing.T) {
	ctx := context.Background()
	engineInstance := openTestEngine(t, compat.Default())
	base := observationLine(t, "replica-x", 1000, engineSource+":1-5", 10, 1, "2026-01-01T00:00:00Z")
	if result, _ := engineInstance.Import(ctx, ImportRequest{BatchID: "base", Data: jsonlDocument(t, base)}); !result.Success {
		t.Fatalf("base import: %s", resultBody(result))
	}

	overwrite := `{"replica_id":"replica-x","sampled_at_ms":1000,"gtid_segments":["` + engineSource + `:1-6"],"position_name":"` + engineSource + `:1-6","lag_ms":12,"reported_version":"8.0.0","last_write_ms":200,"format_version":1}`
	overwriteResult, err := engineInstance.Import(ctx, ImportRequest{BatchID: "overwrite", Data: jsonlDocument(t, overwrite)})
	if err != nil || !overwriteResult.Success {
		t.Fatalf("overwrite import: %v %s", err, resultBody(overwriteResult))
	}
	outcomes := decodeBody(t, overwriteResult.Body)["outcomes"].([]any)
	if outcomes[0].(map[string]any)["outcome"] != "OVERWRITE" {
		t.Fatalf("overwrite outcomes = %#v", outcomes)
	}

	skipResult, err := engineInstance.Import(ctx, ImportRequest{BatchID: "skip", Data: jsonlDocument(t, overwrite)})
	if err != nil || !skipResult.Success {
		t.Fatalf("skip import: %v %s", err, resultBody(skipResult))
	}
	if decodeBody(t, skipResult.Body)["outcomes"].([]any)[0].(map[string]any)["outcome"] != "SKIP" {
		t.Fatalf("skip body = %s", skipResult.Body)
	}

	rejectCandidate := `{"replica_id":"replica-x","sampled_at_ms":1000,"gtid_segments":["` + engineSource + `:1-5:7"],"position_name":"` + engineSource + `:1-5:7","lag_ms":99,"reported_version":"8.0.0","last_write_ms":200,"format_version":1}`
	rejectResult, err := engineInstance.Import(ctx, ImportRequest{BatchID: "reject", Data: jsonlDocument(t, rejectCandidate)})
	if err != nil {
		t.Fatal(err)
	}
	if rejectResult.Success || rejectResult.Code != apperr.CodeObservationConflict {
		t.Fatalf("reject result success=%v code=%s body=%s", rejectResult.Success, rejectResult.Code, rejectResult.Body)
	}
	observations, _, _ := engineInstance.ListObservations(ctx)
	if len(observations) != 1 || observations[0].GTID.String() != engineSource+":1-6" || observations[0].LagMS != 12 {
		t.Fatalf("rejected batch changed data: %+v", observations)
	}
}

func TestConcurrentReadersSeeOnlyCompleteVersions(t *testing.T) {
	ctx := context.Background()
	engineInstance := openTestEngine(t, compat.Default())
	seed := observationLine(t, "replica-x", 1000, engineSource+":1-5", 10, 1, "2026-01-01T00:00:00Z")
	if result, _ := engineInstance.Import(ctx, ImportRequest{BatchID: "concurrent-seed", Data: jsonlDocument(t, seed)}); !result.Success {
		t.Fatal(resultBody(result))
	}

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for reader := 0; reader < 8; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 12; i++ {
				observations, version, err := engineInstance.ListObservations(ctx)
				if err != nil {
					errs <- err
					return
				}
				if version != int64(len(observations)) {
					errs <- fmt.Errorf("partial snapshot: version=%d observations=%d", version, len(observations))
					return
				}
			}
		}()
	}
	for writer := 0; writer < 6; writer++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			line := observationLine(t, "replica-x", int64(1000+index+1), fmt.Sprintf("%s:1-%d", engineSource, 6+index), int64(index), 1, "2026-01-01T00:00:01Z")
			result, err := engineInstance.Import(ctx, ImportRequest{Data: jsonlDocument(t, line)})
			if err != nil || !result.Success {
				errs <- fmt.Errorf("writer %d: %v %s", index, err, resultBody(result))
			}
		}(writer)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestRecoveryAbortsPendingIntentAndPreservesVersion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recovery.db")
	database, err := store.Open(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	engineInstance, _ := New(database, compat.Default(), domain.DefaultPolicy())
	seed := observationLine(t, "replica-x", 1000, engineSource+":1-5", 10, 1, "2026-01-01T00:00:00Z")
	if result, _ := engineInstance.Import(ctx, ImportRequest{BatchID: "seed", Data: jsonlDocument(t, seed)}); !result.Success {
		t.Fatal(resultBody(result))
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO batches
		(batch_id, request_hash, operation, status, http_status, response_json, version, created_ms)
		VALUES ('crashed', 'hash', 'import', 'PENDING', 0, '', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO intents
		(batch_id, operation, request_hash, base_version, status, detail_json, created_ms)
		VALUES ('crashed', 'import', 'hash', 1, 'PENDING', '{}', 1)`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("reopen/recover: %v", err)
	}
	defer reopened.Close()
	report, version, err := reopened.RecoveryReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || report.LastCompleteVersion != 1 || report.PendingIntentsFound != 1 || len(report.Events) != 1 {
		t.Fatalf("recovery report = %+v version=%d", report, version)
	}
	if report.Events[0].Action != "ABORT_PENDING_INTENT" || report.Events[0].BaseVersion != 1 {
		t.Fatalf("recovery event = %+v", report.Events[0])
	}
	observations, _, err := reopened.ObservationSet(ctx)
	if err != nil || len(observations) != 1 {
		t.Fatalf("observations after recovery: %v count=%d", err, len(observations))
	}
}

func openTestEngine(t *testing.T, matrix compat.Matrix) *Engine {
	t.Helper()
	path := filepath.Join(t.TempDir(), "engine.db")
	database, err := store.Open(context.Background(), path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	instance, err := New(database, matrix, domain.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func observationLine(t *testing.T, replica string, sampledAt int64, gtid string, lag int64, formatVersion int, lastWrite string) string {
	t.Helper()
	var lastWriteMS int64
	if formatVersion == 1 {
		lastWriteMS = 1700200000000
	}
	return fmt.Sprintf(`{"replica_id":%q,"sampled_at_ms":%d,"gtid_segments":[%q],"position_name":%q,"lag_ms":%d,"reported_version":"8.0.36","last_write_ms":%d,"format_version":%d}`,
		replica, sampledAt, gtid, gtid, lag, lastWriteMS, formatVersion)
}

func jsonlDocument(t *testing.T, lines ...string) []byte {
	t.Helper()
	header := `{"format":"replication-observation-jsonl","format_version":1,"required_fields":["replica_id","sampled_at_ms","gtid_segments","position_name","lag_ms","reported_version"]}`
	result := header + "\n"
	for _, line := range lines {
		result += line + "\n"
	}
	return []byte(result)
}

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode body %s: %v", body, err)
	}
	return result
}

func resultBody(result store.Result) string {
	return string(result.Body)
}

func assertObservationSetEqual(t *testing.T, a, b []domain.Observation) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("observation count %d != %d", len(a), len(b))
	}
	left := domain.SortObservations(a)
	right := domain.SortObservations(b)
	for i := range left {
		if left[i].ReplicaID != right[i].ReplicaID || left[i].SampledAtMS != right[i].SampledAtMS ||
			!left[i].GTID.Equal(right[i].GTID) || left[i].PositionName != right[i].PositionName ||
			left[i].LagMS != right[i].LagMS || left[i].ReportedVersion.String() != right[i].ReportedVersion.String() {
			t.Fatalf("observation %d differs: %+v vs %+v", i, left[i], right[i])
		}
	}
}
